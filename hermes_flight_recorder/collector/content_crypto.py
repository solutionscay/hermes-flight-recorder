"""Operator-keyed content encryption.

This module is the cryptographic core of the fleet key model. It is
deliberately self-contained: it imports only the standard library and
``cryptography`` (already the sole runtime dependency), and nothing from
``collector``, so the crypto contract has no dependency on the outbox,
config, or sync code that uses it.

The model has three key kinds:

- **Operator keypair (KEK).** One asymmetric X25519 keypair for a whole
  fleet. The *public* half is distributed to every agent (safe to hand out,
  even by an untrusted backend). The *private* half stays with the operator
  and never touches an agent host or the server. It is the one key that
  decrypts the fleet.
- **Data key (DEK).** A random AES-256-GCM key that actually encrypts
  content. Disposable, minted per ``(installation, key epoch)``.
- **Wrapped DEK.** The DEK sealed to the operator public key. Stored
  alongside the ciphertext; only the operator private key can open it.

An agent holds only the operator *public* key and its current DEK. If the
host is compromised, an attacker can write new content but cannot read the
fleet's history, because unwrapping any DEK needs the operator private key.

The seal is an HPKE-style sealed box built from primitives in
``cryptography``: an ephemeral X25519 key agreement, HKDF-SHA256 to derive a
one-time AES key bound to both public keys, then AES-256-GCM. A fresh
ephemeral key per seal makes each derived key unique, so the construction is
safe and needs no external nonce management for the wrap.

See ``docs/schema/envelope-v1.md`` for how the sealed DEK and ciphertext
travel in the envelope.
"""

from __future__ import annotations

import base64
import hashlib
import os
from dataclasses import dataclass

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey,
    X25519PublicKey,
)
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    PublicFormat,
    PrivateFormat,
    NoEncryption,
)

__all__ = [
    "CryptoError",
    "OperatorKeyPair",
    "OperatorPublicKey",
    "generate_operator_keypair",
    "load_public_key",
    "load_keypair",
    "generate_dek",
    "wrap_dek",
    "unwrap_dek",
    "seal",
    "unseal",
    "encrypt_content",
    "decrypt_content",
]

# Text serialization prefixes. A prefix makes a key self-describing and lets
# tooling reject a private key handed where a public one belongs.
_PUBLIC_PREFIX = "hfr-operator-public-v1:"
_SECRET_PREFIX = "hfr-operator-secret-v1:"

# Domain-separation label bound into the sealed-box key derivation. Bumping
# it (a new scheme) makes old and new seals cryptographically distinct.
_SEAL_INFO = b"hfr-sealed-box-v1"
_SEAL_VERSION = 1

_DEK_BYTES = 32  # AES-256
_NONCE_BYTES = 12  # AES-GCM standard nonce


class CryptoError(RuntimeError):
    """Raised on any malformed key, ciphertext, or failed authentication."""


# --- key identity -------------------------------------------------------
def _public_bytes(public: X25519PublicKey) -> bytes:
    return public.public_bytes(Encoding.Raw, PublicFormat.Raw)


def _key_id(public: X25519PublicKey) -> str:
    """A short, stable fingerprint of a public key.

    Records stamp this as their recipient so a reader knows which operator
    key epoch sealed a DEK, and rotation can tell old wraps from new.
    """
    digest = hashlib.sha256(_public_bytes(public)).hexdigest()
    return "opk1:" + digest[:16]


@dataclass(frozen=True)
class OperatorPublicKey:
    """The distributable half of an operator keypair."""

    _public: X25519PublicKey

    @property
    def key_id(self) -> str:
        return _key_id(self._public)

    def to_text(self) -> str:
        """Serialize to a single self-describing line safe to distribute."""
        raw = _public_bytes(self._public)
        return _PUBLIC_PREFIX + base64.b64encode(raw).decode("ascii")


@dataclass(frozen=True)
class OperatorKeyPair:
    """A full operator keypair. The private half must never leave the operator."""

    _private: X25519PrivateKey

    @property
    def public(self) -> OperatorPublicKey:
        return OperatorPublicKey(self._private.public_key())

    @property
    def key_id(self) -> str:
        return _key_id(self._private.public_key())

    def secret_to_text(self) -> str:
        """Serialize the private key. Handle the result as a secret (0600)."""
        raw = self._private.private_bytes(
            Encoding.Raw, PrivateFormat.Raw, NoEncryption()
        )
        return _SECRET_PREFIX + base64.b64encode(raw).decode("ascii")


def generate_operator_keypair() -> OperatorKeyPair:
    """Mint a fresh operator keypair on this machine."""
    return OperatorKeyPair(X25519PrivateKey.generate())


def load_public_key(text: str) -> OperatorPublicKey:
    """Parse a serialized operator public key.

    Rejects a private key passed by mistake, so a caller cannot accidentally
    distribute the decrypting half.
    """
    text = text.strip()
    if text.startswith(_SECRET_PREFIX):
        raise CryptoError(
            "expected an operator public key but got a private key; "
            "distribute the public half only"
        )
    if not text.startswith(_PUBLIC_PREFIX):
        raise CryptoError("not an operator public key (bad prefix)")
    raw = _b64_raw(text[len(_PUBLIC_PREFIX):], "public key")
    try:
        return OperatorPublicKey(X25519PublicKey.from_public_bytes(raw))
    except ValueError as exc:
        raise CryptoError(f"invalid operator public key: {exc}") from exc


def load_keypair(text: str) -> OperatorKeyPair:
    """Parse a serialized operator private key into a full keypair."""
    text = text.strip()
    if not text.startswith(_SECRET_PREFIX):
        raise CryptoError("not an operator private key (bad prefix)")
    raw = _b64_raw(text[len(_SECRET_PREFIX):], "private key")
    try:
        return OperatorKeyPair(X25519PrivateKey.from_private_bytes(raw))
    except ValueError as exc:
        raise CryptoError(f"invalid operator private key: {exc}") from exc


def _b64_raw(encoded: str, what: str) -> bytes:
    try:
        return base64.b64decode(encoded, validate=True)
    except (ValueError, base64.binascii.Error) as exc:  # type: ignore[attr-defined]
        raise CryptoError(f"malformed {what} encoding") from exc


# --- data keys ----------------------------------------------------------
def generate_dek() -> bytes:
    """Mint a fresh random AES-256 data key."""
    return AESGCM.generate_key(bit_length=256)


# --- sealed box ---------------------------------------------------------
def seal(recipient: OperatorPublicKey, plaintext: bytes) -> bytes:
    """Seal ``plaintext`` to a public key. Only its private half opens it.

    Layout: ``version(1) || ephemeral_public(32) || nonce(12) || ciphertext``.
    A fresh ephemeral key per call makes the derived AES key unique, so the
    12-byte nonce never repeats under a given derived key.
    """
    recipient_public = recipient._public
    ephemeral = X25519PrivateKey.generate()
    ephemeral_public = ephemeral.public_key()
    shared = ephemeral.exchange(recipient_public)
    key = _derive_key(shared, _public_bytes(ephemeral_public), _public_bytes(recipient_public))
    nonce = os.urandom(_NONCE_BYTES)
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, None)
    return bytes([_SEAL_VERSION]) + _public_bytes(ephemeral_public) + nonce + ciphertext


def unseal(keypair: OperatorKeyPair, sealed: bytes) -> bytes:
    """Open a sealed box with the operator private key."""
    header = 1 + 32 + _NONCE_BYTES
    if len(sealed) < header:
        raise CryptoError("sealed box too short")
    if sealed[0] != _SEAL_VERSION:
        raise CryptoError(f"unsupported sealed-box version {sealed[0]}")
    ephemeral_public_raw = sealed[1:33]
    nonce = sealed[33:header]
    ciphertext = sealed[header:]
    private = keypair._private
    try:
        ephemeral_public = X25519PublicKey.from_public_bytes(ephemeral_public_raw)
    except ValueError as exc:
        raise CryptoError(f"invalid ephemeral public key: {exc}") from exc
    shared = private.exchange(ephemeral_public)
    recipient_public_raw = _public_bytes(private.public_key())
    key = _derive_key(shared, ephemeral_public_raw, recipient_public_raw)
    try:
        return AESGCM(key).decrypt(nonce, ciphertext, None)
    except Exception as exc:  # cryptography raises InvalidTag on tamper/wrong key
        raise CryptoError("sealed box failed to authenticate (wrong key or tampered)") from exc


def _derive_key(shared: bytes, ephemeral_public: bytes, recipient_public: bytes) -> bytes:
    """Derive the one-time AES key, bound to both public keys."""
    return HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=None,
        info=_SEAL_INFO + ephemeral_public + recipient_public,
    ).derive(shared)


def wrap_dek(recipient: OperatorPublicKey, dek: bytes) -> bytes:
    """Seal a data key to the operator public key."""
    if len(dek) != _DEK_BYTES:
        raise CryptoError(f"data key must be {_DEK_BYTES} bytes, got {len(dek)}")
    return seal(recipient, dek)


def unwrap_dek(keypair: OperatorKeyPair, wrapped: bytes) -> bytes:
    """Recover a data key from its wrapped form with the operator private key."""
    dek = unseal(keypair, wrapped)
    if len(dek) != _DEK_BYTES:
        raise CryptoError("unwrapped data key has the wrong length")
    return dek


# --- content ------------------------------------------------------------
def encrypt_content(dek: bytes, content: str | bytes) -> tuple[bytes, bytes, str]:
    """Encrypt content with a data key.

    Returns ``(ciphertext, nonce, content_hash)``. The hash is over the
    plaintext, so a reader can verify a decrypt and the outbox can dedupe.
    """
    raw = content.encode("utf-8") if isinstance(content, str) else content
    nonce = os.urandom(_NONCE_BYTES)
    ciphertext = AESGCM(dek).encrypt(nonce, raw, None)
    content_hash = "sha256:" + hashlib.sha256(raw).hexdigest()
    return ciphertext, nonce, content_hash


def decrypt_content(dek: bytes, ciphertext: bytes, nonce: bytes) -> bytes:
    """Decrypt content with its data key."""
    try:
        return AESGCM(dek).decrypt(nonce, ciphertext, None)
    except Exception as exc:
        raise CryptoError("content failed to authenticate (wrong key or tampered)") from exc
