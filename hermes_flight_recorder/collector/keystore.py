"""Operator key storage in the Flight Recorder home.

This is the file-custody layer over :mod:`content_crypto`. It decides where
the operator keypair lives on disk and which half a given install holds:

- **Solo install** (a laptop, one agent): both halves live here. The public
  key seals content; the private key (``operator.secret``, ``0600``) is
  present so the same machine can decrypt its own outbox.
- **Fleet agent**: only ``operator.pub`` is written. No private key ever
  touches the host, so a compromise cannot read the fleet's history. The
  operator keeps the private key on their console/keystore.

The distinction is made by what ``install`` is given, not by a mode flag:
nothing → solo (auto-keygen); ``--operator-pubkey`` → fleet agent.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from . import content_crypto as cc
from .content_crypto import CryptoError, OperatorKeyPair, OperatorPublicKey

__all__ = [
    "KeystoreError",
    "PUBLIC_FILENAME",
    "SECRET_FILENAME",
    "RETIRED_DIR_NAME",
    "public_path",
    "secret_path",
    "has_public",
    "has_secret",
    "write_public_key",
    "write_keypair",
    "ensure_solo_keypair",
    "mint_operator_keypair",
    "retired_keypairs",
    "load_public_key",
    "load_keypair",
]

PUBLIC_FILENAME = "operator.pub"
SECRET_FILENAME = "operator.secret"
# Rotated-out keypairs are moved here (never deleted), so content sealed to an
# earlier operator key stays decryptable with the key that sealed it.
RETIRED_DIR_NAME = "retired-keys"


class KeystoreError(RuntimeError):
    """The operator key on disk is missing, malformed, or conflicting."""


def public_path(fr_home: str | os.PathLike[str]) -> Path:
    return Path(fr_home) / PUBLIC_FILENAME


def secret_path(fr_home: str | os.PathLike[str]) -> Path:
    return Path(fr_home) / SECRET_FILENAME


def has_public(fr_home: str | os.PathLike[str]) -> bool:
    return public_path(fr_home).exists()


def has_secret(fr_home: str | os.PathLike[str]) -> bool:
    return secret_path(fr_home).exists()


def _write_private(path: Path, text: str) -> None:
    """Write a secret file with owner-only permissions from creation."""
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, (text + "\n").encode("ascii"))
    finally:
        os.close(fd)
    os.chmod(path, 0o600)


def _write_public(path: Path, text: str) -> None:
    """Write the public key. It is not secret, but keep it tidy at 0644."""
    path.write_text(text + "\n", encoding="ascii")
    try:
        os.chmod(path, 0o644)
    except OSError:
        pass


def write_public_key(
    fr_home: str | os.PathLike[str], public: OperatorPublicKey
) -> Path:
    """Install a fleet agent's public key. Refuses if a private key is present.

    A host that already holds a private key is a solo/operator machine;
    quietly overwriting it with a public-only key would strand its data, so
    that is an error the caller must resolve.
    """
    if has_secret(fr_home):
        raise KeystoreError(
            f"{secret_path(fr_home)} exists; this host already holds an operator "
            f"private key. Remove it deliberately before switching to a "
            f"public-only fleet agent."
        )
    path = public_path(fr_home)
    _write_public(path, public.to_text())
    return path


def write_keypair(fr_home: str | os.PathLike[str], keypair: OperatorKeyPair) -> None:
    """Write both halves of an operator keypair (solo/operator machine)."""
    _write_public(public_path(fr_home), keypair.public.to_text())
    _write_private(secret_path(fr_home), keypair.secret_to_text())


def ensure_solo_keypair(fr_home: str | os.PathLike[str]) -> OperatorKeyPair:
    """Return this host's operator keypair, minting one on first call.

    Solo path: if a keypair already exists it is loaded and returned
    unchanged (idempotent, like the old content key). If only a public key
    exists, this host is a fleet agent with no private key — refuse rather
    than mint a second, conflicting operator identity.
    """
    if has_secret(fr_home):
        return load_keypair(fr_home)
    if has_public(fr_home):
        raise KeystoreError(
            f"{public_path(fr_home)} exists without a private key; this is a "
            f"fleet agent. It cannot mint its own operator key. Decrypt from "
            f"the operator console that holds the private key."
        )
    keypair = cc.generate_operator_keypair()
    write_keypair(fr_home, keypair)
    return keypair


def mint_operator_keypair(
    fr_home: str | os.PathLike[str], *, rotate: bool = False
) -> OperatorKeyPair:
    """Mint an operator keypair on this (operator/solo) machine.

    With ``rotate=False`` this refuses when any key half already exists, so a
    stray ``keygen`` cannot silently replace a live key. With ``rotate=True`` it
    first retires the current keypair — both halves are moved under
    ``retired-keys/<timestamp>/`` and never deleted, so content sealed to the
    old key stays decryptable — then writes a fresh keypair as current
    (forward-only rotation: new content seals to the new key).
    """
    if has_public(fr_home) or has_secret(fr_home):
        if not rotate:
            raise KeystoreError(
                f"an operator key already exists at {public_path(fr_home)}; pass "
                f"rotate=True to mint a new one (the old key is retained under "
                f"'{RETIRED_DIR_NAME}/' so existing history stays readable)"
            )
        _retire_current(fr_home)
    keypair = cc.generate_operator_keypair()
    write_keypair(fr_home, keypair)
    return keypair


def _retire_current(fr_home: str | os.PathLike[str]) -> Path:
    """Move the current keypair into a fresh ``retired-keys/<timestamp>/`` dir."""
    home = Path(fr_home)
    retired_root = home / RETIRED_DIR_NAME
    retired_root.mkdir(mode=0o700, exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    dest = retired_root / stamp
    suffix = 1
    while dest.exists():
        dest = retired_root / f"{stamp}-{suffix}"
        suffix += 1
    dest.mkdir(mode=0o700)
    for filename in (PUBLIC_FILENAME, SECRET_FILENAME):
        source = home / filename
        if source.exists():
            source.rename(dest / filename)
    return dest


def retired_keypairs(fr_home: str | os.PathLike[str]) -> list[OperatorKeyPair]:
    """Load every retired operator keypair, newest retirement first.

    Reading pre-rotation history needs the key that sealed it; tooling matches a
    record's ``operator_key_id`` against these to pick the right one.
    """
    retired_root = Path(fr_home) / RETIRED_DIR_NAME
    if not retired_root.is_dir():
        return []
    keypairs: list[OperatorKeyPair] = []
    for entry in sorted(retired_root.iterdir(), reverse=True):
        secret = entry / SECRET_FILENAME
        if secret.is_file():
            try:
                keypairs.append(cc.load_keypair(secret.read_text(encoding="ascii")))
            except (OSError, CryptoError):
                continue
    return keypairs


def load_public_key(fr_home: str | os.PathLike[str]) -> OperatorPublicKey:
    """Load the operator public key used to seal content."""
    path = public_path(fr_home)
    if not path.exists():
        raise KeystoreError(f"operator public key missing at {path}")
    try:
        return cc.load_public_key(path.read_text(encoding="ascii"))
    except (OSError, CryptoError) as exc:
        raise KeystoreError(f"cannot read operator public key at {path}: {exc}") from exc


def load_keypair(fr_home: str | os.PathLike[str]) -> OperatorKeyPair:
    """Load the operator keypair. Only present on a solo/operator machine."""
    path = secret_path(fr_home)
    if not path.exists():
        raise KeystoreError(
            f"operator private key missing at {path}; this host cannot decrypt "
            f"(it is a fleet agent, or the key lives on your operator console)"
        )
    try:
        return cc.load_keypair(path.read_text(encoding="ascii"))
    except (OSError, CryptoError) as exc:
        raise KeystoreError(f"cannot read operator private key at {path}: {exc}") from exc
