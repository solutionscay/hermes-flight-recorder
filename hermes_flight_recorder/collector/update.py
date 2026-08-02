"""Two-stage package and installation update workflow.

The first stage runs from the old package: refuse a live recorder, create a
recoverable backup, and ask pip to install the selected source. A new Python
process then imports the newly installed package, applies outbox migrations,
regenerates the Hermes hook, stamps the installed build, and verifies it.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from ..version import VersionInfo, build_identity, current_version
from . import atomic_file
from . import recorder_config
from . import security_scan
from ._common import resolve_flight_recorder_home, resolve_hermes_home
from .hook import HOOK_DIR_NAME, install_hook
from .outbox import Outbox
from .runtime_lock import LOCK_FILENAME, RuntimeLock, RuntimeLockError
from .update_target import UpdateError, build_update_target, validate_git_commit

DEFAULT_SOURCE = "git+https://github.com/solutionscay/hermes-flight-recorder.git"
PENDING_UPDATE_FILENAME = "pending-update.json"
INSTALLED_VERSION_FILENAME = "installed-version.json"
LAST_UPDATE_FILENAME = "last-update.json"
BACKUP_DIRNAME = "update-backups"


def _write_json(path: Path, value: dict[str, Any]) -> None:
    atomic_file.write_json_object(path, value, sort_keys=True)


def _read_json(path: Path) -> dict[str, Any]:
    return atomic_file.read_json_object(
        path, error=UpdateError, description="update state"
    )


def _refuse_if_serving(fr_home: Path) -> RuntimeLock:
    lock = RuntimeLock(fr_home / LOCK_FILENAME)
    try:
        lock.acquire()
    except RuntimeLockError:
        raise UpdateError(
            f"a Flight Recorder process is running against {fr_home}; "
            "stop `serve` or its service manager before updating"
        ) from None
    return lock


def _backup_sqlite(source: Path, destination: Path) -> None:
    source_conn = sqlite3.connect(str(source))
    destination_conn = sqlite3.connect(str(destination))
    try:
        source_conn.backup(destination_conn)
    finally:
        destination_conn.close()
        source_conn.close()
    try:
        os.chmod(destination, 0o600)
    except OSError:
        pass


def _unique_backup_dir(fr_home: Path) -> Path:
    root = fr_home / BACKUP_DIRNAME
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    candidate = root / stamp
    suffix = 1
    while candidate.exists():
        candidate = root / f"{stamp}-{suffix}"
        suffix += 1
    candidate.mkdir(mode=0o700)
    return candidate


def _backup_installation(fr_home: Path, hermes_home: Path) -> Path:
    backup = _unique_backup_dir(fr_home)
    database = fr_home / "outbox.sqlite"
    if not database.is_file():
        raise UpdateError(
            f"Flight Recorder is not installed at {fr_home}; run `install` first"
        )
    _backup_sqlite(database, backup / "outbox.sqlite")
    names = [
        "operator.pub",
        "operator.secret",
        "secret-scan.key",
        "recorder-config.json",
        "sync-config.json",
        INSTALLED_VERSION_FILENAME,
    ]
    try:
        baseline_name = recorder_config.load(fr_home).security.secret_scan_baseline
    except recorder_config.RecorderConfigError:
        baseline_name = recorder_config.DEFAULT_SECRET_SCAN_BASELINE
    names.append(baseline_name)
    for name in dict.fromkeys(names):
        source = fr_home / name
        if source.is_file():
            shutil.copy2(source, backup / name)
    hook = hermes_home / "hooks" / HOOK_DIR_NAME
    if hook.is_dir():
        shutil.copytree(hook, backup / "hook")
    return backup


def _completion_command(
    *,
    flight_recorder_home: str | os.PathLike[str] | None,
    hermes_home: str | os.PathLike[str] | None,
    guard_owner_pid: int | None = None,
) -> list[str]:
    command = [
        sys.executable,
        "-m",
        "hermes_flight_recorder.cli",
        "update",
        "--complete",
    ]
    if flight_recorder_home is not None:
        command.extend(["--flight-recorder-home", str(flight_recorder_home)])
    if hermes_home is not None:
        command.extend(["--hermes-home", str(hermes_home)])
    if guard_owner_pid is not None:
        command.extend(["--guard-owner-pid", str(guard_owner_pid)])
    return command


def _prepare_update_locked(
    fr_home: Path,
    hermes: Path,
    *,
    source: str = DEFAULT_SOURCE,
    commit: str | None = None,
    editable: bool = False,
    guard_owner_pid: int | None = None,
) -> tuple[Path, list[str], list[str]]:
    """Back up an installation while the caller holds its runtime lock."""
    if not hermes.is_dir():
        raise UpdateError(f"Hermes home {hermes} does not exist")
    fr_home.mkdir(mode=0o700, parents=True, exist_ok=True)
    target = build_update_target(
        source, commit, editable=editable, validator=validate_git_commit
    )
    backup = _backup_installation(fr_home, hermes)
    pending = {
        "state": "prepared",
        "prepared_at": time.time(),
        "previous": current_version().to_dict(),
        "target": target,
        "backup": str(backup),
    }
    if guard_owner_pid is not None:
        pending["guard_owner_pid"] = guard_owner_pid
    _write_json(backup / "update.json", pending)
    _write_json(fr_home / PENDING_UPDATE_FILENAME, pending)

    pip_command = [sys.executable, "-m", "pip", "install", "--upgrade"]
    if editable:
        pip_command.append("--editable")
    pip_command.append(target["requirement"])
    return (
        backup,
        pip_command,
        _completion_command(
            flight_recorder_home=fr_home,
            hermes_home=hermes,
            guard_owner_pid=guard_owner_pid,
        ),
    )


def prepare_update(
    flight_recorder_home: str | os.PathLike[str] | None,
    hermes_home: str | os.PathLike[str] | None,
    *,
    source: str = DEFAULT_SOURCE,
    commit: str | None = None,
    editable: bool = False,
) -> tuple[Path, list[str], list[str]]:
    """Back up an idle installation and return pip/completion commands."""
    fr_home = resolve_flight_recorder_home(
        flight_recorder_home, hermes_home
    ).resolve()
    hermes = resolve_hermes_home(hermes_home).resolve()
    fr_home.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock = _refuse_if_serving(fr_home)
    try:
        return _prepare_update_locked(
            fr_home,
            hermes,
            source=source,
            commit=commit,
            editable=editable,
        )
    finally:
        lock.release()


def _run(
    command: Sequence[str],
    *,
    runner: Callable[..., subprocess.CompletedProcess[Any]],
) -> None:
    try:
        result = runner(list(command), check=False)
    except OSError as exc:
        raise UpdateError(f"could not run {command[0]}: {exc}") from exc
    if result.returncode:
        raise UpdateError(
            f"command failed with exit code {result.returncode}: "
            + " ".join(command)
        )


def update(
    flight_recorder_home: str | os.PathLike[str] | None,
    hermes_home: str | os.PathLike[str] | None,
    *,
    source: str = DEFAULT_SOURCE,
    commit: str | None = None,
    editable: bool = False,
    runner: Callable[..., subprocess.CompletedProcess[Any]] = subprocess.run,
    log=print,
) -> Path:
    """Install selected package source, then complete with the new package."""
    fr_home = resolve_flight_recorder_home(
        flight_recorder_home, hermes_home
    ).resolve()
    hermes = resolve_hermes_home(hermes_home).resolve()
    fr_home.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock = _refuse_if_serving(fr_home)
    try:
        backup, pip_command, completion_command = _prepare_update_locked(
            fr_home,
            hermes,
            source=source,
            commit=commit,
            editable=editable,
            guard_owner_pid=os.getpid(),
        )
        pending = _read_json(fr_home / PENDING_UPDATE_FILENAME)
        target = pending["target"]
        log(f"backup created:       {backup}")
        resolved = target.get("commit") or "unavailable (local checkout)"
        dirty = "-dirty" if target.get("dirty") else ""
        log(f"requested commit:     {resolved}{dirty}")
        log("updating package...")
        _set_pending_state(fr_home, "package-replacement", "package_started_at")
        _run(pip_command, runner=runner)
        log("completing installation with the updated package...")
        _run(completion_command, runner=runner)
    except UpdateError:
        _mark_update_failed(fr_home)
        raise
    finally:
        lock.release()
    return backup


def _set_pending_state(fr_home: Path, state: str, timestamp_key: str) -> None:
    pending_path = fr_home / PENDING_UPDATE_FILENAME
    pending = _read_json(pending_path)
    pending["state"] = state
    pending[timestamp_key] = time.time()
    _write_json(pending_path, pending)


def _mark_update_failed(fr_home: Path, exc: Exception | None = None) -> None:
    pending_path = fr_home / PENDING_UPDATE_FILENAME
    if not pending_path.exists():
        return
    pending = _read_json(pending_path)
    pending["failed_stage"] = pending.get("state")
    pending["state"] = "failed"
    pending["failed_at"] = time.time()
    if exc is not None:
        pending["failure"] = str(exc)
    _write_json(pending_path, pending)


def _check_parent_guard(fr_home: Path, pending: dict[str, Any], owner_pid: int) -> None:
    expected_pid = pending.get("guard_owner_pid")
    if expected_pid != owner_pid:
        raise UpdateError("update completion guard does not match pending state")

    probe = RuntimeLock(fr_home / LOCK_FILENAME)
    try:
        probe.acquire()
    except RuntimeLockError:
        holder = probe.holder_info
        if holder is None or holder.split(maxsplit=1)[0] != str(owner_pid):
            raise UpdateError("the update guard is held by an unexpected process") from None
        return
    else:
        probe.release()
        raise UpdateError("the parent update guard is no longer active")


def write_installed_version(
    fr_home: Path,
    *,
    source: str | None = None,
    selected_commit: str | None = None,
) -> VersionInfo:
    info = current_version()
    value = info.to_dict() | {
        "installed_at": time.time(),
        "selected_source": source,
        "selected_commit": selected_commit,
        "installed_revision": info.revision,
    }
    _write_json(fr_home / INSTALLED_VERSION_FILENAME, value)
    return info


def complete_update(
    flight_recorder_home: str | os.PathLike[str] | None,
    hermes_home: str | os.PathLike[str] | None,
    *,
    guard_owner_pid: int | None = None,
    log=print,
) -> Path:
    """Migrate, refresh, stamp, and verify using the newly installed package."""
    fr_home = resolve_flight_recorder_home(
        flight_recorder_home, hermes_home
    ).resolve()
    hermes = resolve_hermes_home(hermes_home).resolve()
    pending_path = fr_home / PENDING_UPDATE_FILENAME
    pending = _read_json(pending_path)
    target = pending.get("target")
    if not isinstance(target, dict):
        raise UpdateError(f"invalid target in {pending_path}")

    # Accept the old pending-state field during a self-update from a release
    # that prepared the update before this package was installed.
    expected_revision = target.get("commit", target.get("resolved_revision"))
    if expected_revision is not None and not isinstance(expected_revision, str):
        raise UpdateError(f"invalid commit in {pending_path}")
    installed_info = current_version()
    if expected_revision and installed_info.revision != expected_revision:
        raise UpdateError(
            f"installed revision {installed_info.revision!r} does not match "
            f"requested commit {expected_revision!r}"
        )

    lock: RuntimeLock | None = None
    if guard_owner_pid is None:
        lock = _refuse_if_serving(fr_home)
    else:
        _check_parent_guard(fr_home, pending, guard_owner_pid)
    try:
        _set_pending_state(fr_home, "completing", "completion_started_at")
        outbox = Outbox.open(fr_home, hermes_home=hermes)
        try:
            installation_id = outbox.initialize()
            security_scan.ensure_fingerprint_key(fr_home)
            info = write_installed_version(
                fr_home,
                source=(
                    target.get("source")
                    if isinstance(target.get("source"), str)
                    else None
                ),
                selected_commit=expected_revision,
            )
            outbox.set_meta("installed_build", info.build)
        finally:
            outbox.close()

        hook_dir = install_hook(
            hermes,
            fr_home,
            force=True,
            build=build_identity(),
        )
        from .lifecycle import _verify

        _verify(fr_home, hook_dir)
        completed = pending | {
            "state": "complete",
            "completed_at": time.time(),
            "installed": current_version().to_dict(),
            "installation_id": installation_id,
        }
        _write_json(fr_home / LAST_UPDATE_FILENAME, completed)
        pending_path.unlink()
    except Exception as exc:
        _mark_update_failed(fr_home, exc)
        if isinstance(exc, UpdateError):
            raise
        raise UpdateError(f"update completion failed: {exc}") from exc
    finally:
        if lock is not None:
            lock.release()

    log(f"updated build:        {build_identity()}")
    requested = expected_revision or "local checkout"
    installed = installed_info.revision or "unknown"
    log(f"requested commit:     {requested}")
    log(f"installed revision:   {installed}")
    log(f"installation id:      {installation_id}")
    log(f"hook refreshed:       {hook_dir}")
    log("restart the Hermes gateway, then restart `hermes-flight-recorder serve`.")
    return fr_home


__all__ = [
    "BACKUP_DIRNAME",
    "DEFAULT_SOURCE",
    "INSTALLED_VERSION_FILENAME",
    "LAST_UPDATE_FILENAME",
    "PENDING_UPDATE_FILENAME",
    "UpdateError",
    "complete_update",
    "prepare_update",
    "validate_git_commit",
    "update",
    "write_installed_version",
]
