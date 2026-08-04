"""Installation lifecycle for a Flight Recorder companion.

``install`` makes one Hermes home into one Flight Recorder installation: it
creates ``$HERMES_HOME/flight-recorder``, initializes the outbox identity,
establishes the operator key (solo: auto-mint both halves; fleet:
``--operator-pubkey`` writes only the public half), writes configuration with
restrictive permissions, installs (or repoints) the in-gateway hook, verifies
the result, and (unless ``--no-service``) registers the native ``serve`` service
so capture and transmit run continuously without a manual step. It is
idempotent.

Legacy ``~/.hermes-flight-recorder`` data is never moved silently: ``install``
detects it and stops with an actionable message. (A ``migrate`` command that
performs the move is a separately scoped follow-up.)
"""

from __future__ import annotations

import os
import shutil
import time
from pathlib import Path

from . import content_crypto as cc
from . import keystore
from . import recorder_config
from . import security_scan
from . import sync_config
from ._common import (
    CAPTURE_BACKFILL_META_KEY,
    INSTALLED_AT_META_KEY,
    LEGACY_FLIGHT_RECORDER_HOME,
    SERVICE_MANAGED_META_KEY,
    resolve_flight_recorder_home,
    resolve_hermes_home,
)
from ..version import build_identity
from .hook import (
    HOOK_DIR_NAME,
    baked_flight_recorder_build,
    baked_flight_recorder_home,
    install_hook,
)
from .outbox import Outbox, OutboxError
from .runtime_lock import LOCK_FILENAME, RuntimeLock, RuntimeLockError


class InstallError(RuntimeError):
    """The installation could not be completed or verified."""


class UninstallError(RuntimeError):
    """The uninstall could not be completed safely."""


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
    backfill: bool = True,
    operator_pubkey: str | os.PathLike[str] | None = None,
    manage_service: bool = False,
    log=print,
) -> Path:
    """Install (or update) the Flight Recorder into ``hermes_home``.

    Returns the resolved Flight Recorder home. Raises :class:`InstallError` on a
    validation or verification failure. ``log`` receives human-readable progress
    lines (default ``print``). With ``backfill=False`` capture starts from the
    install moment instead of ingesting the whole Hermes history.

    ``operator_pubkey`` selects the key model. Omitted → **solo**: an operator
    keypair is minted here (both halves local), so this box can decrypt its own
    outbox. A path → **fleet agent**: only that operator public key is written;
    no private key ever lands on the host, so a compromise can write but not
    read history.
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

    # The legacy path belongs only to the default Hermes installation. Do not
    # let data in one user's default home block another Hermes installation.
    default_hermes = Path.home() / ".hermes"
    if (
        not flight_recorder_home
        and not os.environ.get("SC_HERMES_FLIGHT_RECORDER_HOME")
        and hermes.resolve() == default_hermes.resolve()
    ):
        _stop_if_legacy_present(fr_home, log=log)

    fr_home.mkdir(mode=0o700, parents=True, exist_ok=True)
    runtime_lock = RuntimeLock(fr_home / LOCK_FILENAME)
    try:
        runtime_lock.acquire()
    except RuntimeLockError:
        raise InstallError(
            f"a Flight Recorder process is running against {fr_home}; "
            "stop `serve` before installing or refreshing the hook"
        ) from None

    try:
        # Outbox.open mints the identity/key and applies registered migrations.
        outbox = Outbox.open(fr_home, hermes_home=hermes_home)
        try:
            installation_id = outbox.initialize()
            # Stamp the reconcile horizon once, so the reconciler never judges
            # Hermes history that predates this installation (see reconcile).
            if outbox.get_meta(INSTALLED_AT_META_KEY) is None:
                outbox.set_meta(INSTALLED_AT_META_KEY, repr(time.time()))
            # Record the backfill choice so capture honors it on every pass. Only
            # written when disabled, so a re-install never silently flips it on.
            if not backfill:
                outbox.set_meta(CAPTURE_BACKFILL_META_KEY, "false")
            # Record whether this install manages the native service, so a later
            # `update` keeps the operator's choice.
            outbox.set_meta(
                SERVICE_MANAGED_META_KEY, "true" if manage_service else "false"
            )
            from .update import write_installed_version

            version = write_installed_version(fr_home)
            outbox.set_meta("installed_build", version.build)
        finally:
            outbox.close()
        log(f"flight recorder home: {fr_home}")
        log(f"installation id:      {installation_id}")
        log(f"installed build:      {version.build}")

        _establish_operator_key(fr_home, operator_pubkey, log=log)

        try:
            fingerprint_key = security_scan.ensure_fingerprint_key(fr_home)
        except security_scan.SecurityScanError as exc:
            raise InstallError(str(exc)) from exc
        log(f"secret scan key:       {security_scan.fingerprint_key_path(fr_home)}")

        _write_default_config(fr_home, log=log)

        hook_dir = install_hook(hermes, fr_home, force=True, build=version.build)
        log(f"hook installed:       {hook_dir}")

        _verify(fr_home, hook_dir)
        log("verified outbox, operator key, config, and hook.")
    finally:
        # Release the runtime lock before enabling the service: `enable --now`
        # starts `serve`, which acquires the same lock.
        runtime_lock.release()

    if manage_service:
        from .service import register_service

        register_service(fr_home, hermes, log=log)
        log("restart the Hermes gateway to load the hook. The recorder service "
            "captures and transmits automatically.")
    else:
        log("restart the Hermes gateway to load the hook, then run "
            "`hermes-flight-recorder serve` (or a service that wraps it).")
    return fr_home


def _stop_if_legacy_present(fr_home: Path, *, log) -> None:
    """Refuse to proceed when legacy data exists at a different location."""
    legacy = _legacy_home()
    legacy_outbox = legacy / "outbox.sqlite"
    if legacy_outbox.exists() and legacy.resolve() != fr_home.resolve():
        raise InstallError(
            f"legacy Flight Recorder data found at {legacy}.\n"
            f"Automatic migration is not available yet. Move its contents "
            f"(outbox.sqlite, operator.pub, operator.secret, secret-scan.key, "
            f"recorder-config.json, sync-config.json) to {fr_home} while "
            f"`serve` is stopped, then re-run install; or set "
            f"SC_HERMES_FLIGHT_RECORDER_HOME to keep using {legacy}."
        )


def _establish_operator_key(
    fr_home: Path, operator_pubkey: str | os.PathLike[str] | None, *, log
) -> None:
    """Set up the operator key this install seals content to.

    Fleet agent (``operator_pubkey`` given): write only the public key, so no
    private key touches the host. Solo (omitted): mint a keypair locally if one
    is not already present, and warn if a sync config is present — a host-held
    private key defeats the compromised-host property, and the fleet flow keeps
    the private half on the operator console instead. Idempotent: an existing
    key is preserved.
    """
    if operator_pubkey is not None:
        source = Path(operator_pubkey).expanduser()
        try:
            public = cc.load_public_key(source.read_text(encoding="ascii"))
        except OSError as exc:
            raise InstallError(
                f"cannot read operator public key at {source}: {exc}"
            ) from exc
        except cc.CryptoError as exc:
            raise InstallError(f"invalid operator public key at {source}: {exc}") from exc
        try:
            path = keystore.write_public_key(fr_home, public)
        except keystore.KeystoreError as exc:
            raise InstallError(str(exc)) from exc
        log(f"operator key:         fleet agent, public only ({public.key_id})")
        log(f"                      {path}")
        return

    if keystore.has_public(fr_home) and not keystore.has_secret(fr_home):
        raise InstallError(
            f"{keystore.public_path(fr_home)} exists without a private key; this "
            f"host is a fleet agent. Re-run with --operator-pubkey to refresh it, "
            f"or remove it deliberately to convert this host to solo."
        )
    try:
        keypair = keystore.ensure_solo_keypair(fr_home)
    except keystore.KeystoreError as exc:
        raise InstallError(str(exc)) from exc
    log(f"operator key:         solo, both halves local ({keypair.key_id})")
    if sync_config.config_path(fr_home).exists():
        log(
            "warning: a sync config is present and this host now holds the "
            "operator private key. A host-held private key defeats the "
            "compromised-host property; for a fleet, install agents with "
            "--operator-pubkey and keep the private key on your operator console."
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

    # Operator public key exists (both solo and fleet installs have it). A
    # private key is optional (solo has it, a fleet agent must not); when it is
    # present it must be owner-only.
    public = keystore.public_path(fr_home)
    if not public.exists():
        raise InstallError(f"operator public key missing at {public}")
    if keystore.has_secret(fr_home):
        _require_owner_only(keystore.secret_path(fr_home), "operator private key")
    secret_scan_key = security_scan.fingerprint_key_path(fr_home)
    if not secret_scan_key.exists():
        raise InstallError(f"secret scan key missing at {secret_scan_key}")
    _require_owner_only(secret_scan_key, "secret scan fingerprint key")
    _require_owner_only(recorder_config.config_path(fr_home), "recorder config")
    try:
        security_config = recorder_config.load(fr_home).security
    except recorder_config.RecorderConfigError as exc:
        raise InstallError(str(exc)) from exc
    baseline = security_scan.baseline_path(fr_home, security_config)
    if baseline.exists():
        _require_owner_only(baseline, "secret scan baseline")

    # Hook files exist and target this recorder root.
    for name in ("HOOK.yaml", "handler.py"):
        if not (hook_dir / name).is_file():
            raise InstallError(f"hook file missing: {hook_dir / name}")
    baked = baked_flight_recorder_home(hook_dir)
    if baked is None or Path(baked).resolve() != fr_home.resolve():
        raise InstallError(
            f"hook targets {baked!r}, expected {fr_home.resolve()}"
        )
    hook_build = baked_flight_recorder_build(hook_dir)
    package_build = build_identity()
    if hook_build != package_build:
        raise InstallError(
            f"hook build {hook_build!r} does not match package build "
            f"{package_build!r}; rerun `hermes-flight-recorder install`"
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


def uninstall(
    flight_recorder_home: str | os.PathLike[str] | None,
    hermes_home: str | os.PathLike[str] | None,
    *,
    purge_data: bool = False,
    log=print,
) -> None:
    """Remove the Hermes hook and, with ``purge_data``, the recorder home.

    Preserves all recorder data by default (only the hook and the runtime lock
    go); ``purge_data`` also deletes the recorder home (outbox, key, config).
    Refuses while a ``serve`` process holds the runtime lock. Idempotent and
    never touches any other Hermes state. Raises :class:`UninstallError` when
    it is unsafe to proceed or a requested path remains after removal.
    """
    hermes = resolve_hermes_home(hermes_home)
    fr_home = resolve_flight_recorder_home(flight_recorder_home, hermes_home)

    # Stop and remove the native service first: `disable --now` stops `serve`,
    # which frees the runtime lock the refuse-if-serving check needs. Idempotent
    # and a no-op when no service was ever registered.
    from .service import unregister_service

    unregister_service(log=log)

    _refuse_if_serving(fr_home)

    remaining: list[tuple[Path, OSError | None]] = []
    hook_dir = hermes / "hooks" / HOOK_DIR_NAME
    if hook_dir.exists():
        error = _remove_tree(hook_dir)
        remains, check_error = _path_remains(hook_dir)
        if remains:
            remaining.append((hook_dir, error or check_error))
        else:
            log(f"hook removed:     {hook_dir}")
    else:
        log(f"hook absent:      {hook_dir}")

    if purge_data:
        if fr_home.exists():
            error = _remove_tree(fr_home)
            remains, check_error = _path_remains(fr_home)
            if remains:
                remaining.append((fr_home, error or check_error))
            else:
                log(f"recorder purged:  {fr_home}")
        else:
            log(f"recorder absent:  {fr_home}")
    else:
        # Drop only the runtime lock; keep the outbox, key, and configuration.
        lock = fr_home / LOCK_FILENAME
        if lock.exists():
            error = _remove_file(lock)
            remains, check_error = _path_remains(lock)
            if remains:
                remaining.append((lock, error or check_error))
        log(f"recorder data preserved at {fr_home} (use --purge-data to remove)")

    if remaining:
        details = []
        for path, error in remaining:
            suffix = f": {error}" if error is not None else ""
            details.append(f"  {path}{suffix}")
        raise UninstallError("requested paths remain:\n" + "\n".join(details))


def _remove_tree(path: Path) -> OSError | None:
    """Try to remove a directory tree and return its deletion error."""
    try:
        shutil.rmtree(path)
    except OSError as exc:
        return exc
    return None


def _remove_file(path: Path) -> OSError | None:
    """Try to remove a file and return its deletion error."""
    try:
        path.unlink()
    except OSError as exc:
        return exc
    return None


def _path_remains(path: Path) -> tuple[bool, OSError | None]:
    """Check a removal target without treating a stat error as absence."""
    try:
        path.lstat()
    except FileNotFoundError:
        return False, None
    except OSError as exc:
        return True, exc
    return True, None


def _refuse_if_serving(fr_home: Path) -> None:
    """Raise :class:`UninstallError` when a ``serve`` process holds the lock."""
    if not fr_home.exists():
        return  # nothing installed here; cannot be serving
    lock = RuntimeLock(fr_home / LOCK_FILENAME)
    try:
        lock.acquire()
    except RuntimeLockError:
        raise UninstallError(
            f"a Flight Recorder process is running against {fr_home}; "
            f"stop `serve` first"
        ) from None
    lock.release()


__all__ = [
    "InstallError",
    "UninstallError",
    "install",
    "uninstall",
    "HOOK_DIR_NAME",
    "INSTALLED_AT_META_KEY",
]
