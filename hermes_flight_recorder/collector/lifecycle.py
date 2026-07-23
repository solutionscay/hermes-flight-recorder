"""Installation lifecycle for a Flight Recorder companion.

``install`` makes one Hermes home into one Flight Recorder installation: it
creates ``$HERMES_HOME/flight-recorder``, initializes the outbox identity and
encryption key, writes configuration with restrictive permissions, installs (or
repoints) the in-gateway hook, and verifies the result. It is idempotent and
never registers an OS service — native service registration wraps ``serve``
separately.

Legacy ``~/.hermes-flight-recorder`` data is never moved silently: ``install``
detects it and stops with an actionable message. (A ``migrate`` command that
performs the move is a separately scoped follow-up.)
"""

from __future__ import annotations

import os
from pathlib import Path

from . import recorder_config
from ._common import (
    LEGACY_FLIGHT_RECORDER_HOME,
    resolve_flight_recorder_home,
    resolve_hermes_home,
)
from .hook import HOOK_DIR_NAME, baked_flight_recorder_home, install_hook
from .outbox import Outbox, OutboxError


class InstallError(RuntimeError):
    """The installation could not be completed or verified."""


def _legacy_home() -> Path:
    """The pre-#101 default home, honoring an explicit override.

    An operator who set ``SC_HERMES_FLIGHT_RECORDER_HOME`` already chose their
    location, so there is nothing legacy to detect; return that path (the
    equality check against the target then never fires).
    """
    env = os.environ.get("SC_HERMES_FLIGHT_RECORDER_HOME")
    if env:
        return Path(env).expanduser()
    return Path.home() / LEGACY_FLIGHT_RECORDER_HOME


def install(
    flight_recorder_home: str | os.PathLike[str] | None,
    hermes_home: str | os.PathLike[str] | None,
    *,
    log=print,
) -> Path:
    """Install (or update) the Flight Recorder into ``hermes_home``.

    Returns the resolved Flight Recorder home. Raises :class:`InstallError` on a
    validation or verification failure. ``log`` receives human-readable progress
    lines (default ``print``).
    """
    hermes = resolve_hermes_home(hermes_home)
    if not hermes.is_dir():
        raise InstallError(
            f"Hermes home {hermes} does not exist; create it or pass "
            f"--hermes-home to point at your Hermes installation"
        )
    if not (hermes / "config.yaml").exists() and not (hermes / "state.db").exists():
        log(
            f"warning: {hermes} has no config.yaml or state.db; it may not be a "
            f"Hermes home. Continuing."
        )

    fr_home = resolve_flight_recorder_home(flight_recorder_home, hermes_home)
    if fr_home.resolve() == hermes.resolve():
        raise InstallError(
            f"refusing to install into the Hermes home root ({hermes}); use its "
            f"namespaced 'flight-recorder' child"
        )

    # Legacy detection guards the default-path user. An explicit
    # --flight-recorder-home (or SC_HERMES_FLIGHT_RECORDER_HOME) is a deliberate
    # location choice, so there is nothing to warn about.
    using_default = not flight_recorder_home and not os.environ.get(
        "SC_HERMES_FLIGHT_RECORDER_HOME"
    )
    if using_default:
        _stop_if_legacy_present(fr_home, log=log)

    # Outbox.open creates fr_home (mode 0700) and mints identity + key.
    outbox = Outbox.open(fr_home, hermes_home=hermes_home)
    try:
        installation_id = outbox.initialize()
    finally:
        outbox.close()
    log(f"flight recorder home: {fr_home}")
    log(f"installation id:      {installation_id}")

    _write_default_config(fr_home, log=log)

    hook_dir = install_hook(hermes, fr_home, force=True)
    log(f"hook installed:       {hook_dir}")

    _verify(fr_home, hook_dir)
    log("verified outbox, encryption key, config, and hook.")
    log("restart the Hermes gateway to load the hook, then run "
        "`hermes-flight-recorder serve`.")
    return fr_home


def _stop_if_legacy_present(fr_home: Path, *, log) -> None:
    """Refuse to proceed when legacy data exists at a different location."""
    legacy = _legacy_home()
    legacy_outbox = legacy / "outbox.sqlite"
    if legacy_outbox.exists() and legacy.resolve() != fr_home.resolve():
        raise InstallError(
            f"legacy Flight Recorder data found at {legacy}.\n"
            f"Automatic migration is not available yet. Move its contents "
            f"(outbox.sqlite, content-dev.key, recorder-config.json, "
            f"sync-config.json) to {fr_home} while `serve` is stopped, then "
            f"re-run install; or set SC_HERMES_FLIGHT_RECORDER_HOME to keep "
            f"using {legacy}."
        )


def _write_default_config(fr_home: Path, *, log) -> None:
    """Write recorder-config.json only when absent, preserving operator edits."""
    path = recorder_config.config_path(fr_home)
    if path.exists():
        log(f"config preserved:     {path}")
        return
    recorder_config.save(recorder_config.RecorderConfig(), fr_home)
    log(f"config written:       {path}")


def _verify(fr_home: Path, hook_dir: Path) -> None:
    """Confirm the installation is usable, or raise :class:`InstallError`."""
    # Outbox opens and reports an identity.
    try:
        outbox = Outbox.open(fr_home)
        try:
            _ = outbox.installation_id
        finally:
            outbox.close()
    except OutboxError as exc:
        raise InstallError(f"outbox verification failed: {exc}") from exc

    # Encryption key exists with owner-only permissions (where supported).
    key = fr_home / "content-dev.key"
    if not key.exists():
        raise InstallError(f"encryption key missing at {key}")
    _require_owner_only(key, "encryption key")
    _require_owner_only(recorder_config.config_path(fr_home), "recorder config")

    # Hook files exist and target this recorder root.
    for name in ("HOOK.yaml", "handler.py"):
        if not (hook_dir / name).is_file():
            raise InstallError(f"hook file missing: {hook_dir / name}")
    baked = baked_flight_recorder_home(hook_dir)
    if baked is None or Path(baked).resolve() != fr_home.resolve():
        raise InstallError(
            f"hook targets {baked!r}, expected {fr_home.resolve()}"
        )


def _require_owner_only(path: Path, label: str) -> None:
    """Raise if a file is group/other-accessible on a POSIX filesystem."""
    if os.name != "posix":
        return
    try:
        mode = path.stat().st_mode & 0o077
    except OSError:
        return
    if mode:
        raise InstallError(
            f"{label} at {path} has permissive mode; expected owner-only (0600)"
        )


__all__ = ["InstallError", "install", "HOOK_DIR_NAME"]
