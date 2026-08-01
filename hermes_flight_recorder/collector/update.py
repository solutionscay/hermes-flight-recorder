"""Two-stage package and installation update workflow.

The first stage runs from the old package: refuse a live recorder, create a
recoverable backup, and ask pip to install the selected source. A new Python
process then imports the newly installed package, applies outbox migrations,
regenerates the Hermes hook, stamps the installed build, and verifies it.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Sequence

from ..version import VersionInfo, build_identity, current_version
from . import atomic_file
from ._common import resolve_flight_recorder_home, resolve_hermes_home
from .hook import HOOK_DIR_NAME, install_hook
from .outbox import Outbox
from .runtime_lock import LOCK_FILENAME, RuntimeLock, RuntimeLockError

DEFAULT_SOURCE = "git+https://github.com/solutionscay/hermes-flight-recorder.git"
PENDING_UPDATE_FILENAME = "pending-update.json"
INSTALLED_VERSION_FILENAME = "installed-version.json"
LAST_UPDATE_FILENAME = "last-update.json"
BACKUP_DIRNAME = "update-backups"


class UpdateError(RuntimeError):
    """The package or local installation could not be updated safely."""


def _write_json(path: Path, value: dict[str, Any]) -> None:
    data = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    atomic_file.atomic_write(path, data, mode=0o600)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise UpdateError(f"cannot read update state at {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise UpdateError(f"invalid update state at {path}")
    return value


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
    for name in (
        "operator.pub",
        "operator.secret",
        "recorder-config.json",
        "sync-config.json",
        INSTALLED_VERSION_FILENAME,
    ):
        source = fr_home / name
        if source.is_file():
            shutil.copy2(source, backup / name)
    hook = hermes_home / "hooks" / HOOK_DIR_NAME
    if hook.is_dir():
        shutil.copytree(hook, backup / "hook")
    return backup


def _requirement(source: str, ref: str | None) -> tuple[str, bool]:
    local = Path(source).expanduser()
    if local.exists():
        if ref:
            raise UpdateError(
                "--ref cannot be combined with a local --source; check out the "
                "desired revision in that source directory"
            )
        return str(local.resolve()), True
    if ref:
        if not source.startswith("git+"):
            raise UpdateError("--ref requires a git+ URL source")
        return f"{source}@{ref}", False
    return source, False


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
    ref: str | None = None,
    editable: bool = False,
    guard_owner_pid: int | None = None,
) -> tuple[Path, list[str], list[str]]:
    """Back up an installation while the caller holds its runtime lock."""
    if not hermes.is_dir():
        raise UpdateError(f"Hermes home {hermes} does not exist")
    fr_home.mkdir(mode=0o700, parents=True, exist_ok=True)
    requirement, is_local = _requirement(source, ref)
    if editable and not is_local:
        raise UpdateError("--editable requires a local --source directory")
    backup = _backup_installation(fr_home, hermes)
    pending = {
        "state": "prepared",
        "prepared_at": time.time(),
        "previous": current_version().to_dict(),
        "target": {
            "source": requirement,
            "requested_ref": ref,
            "editable": editable,
        },
        "backup": str(backup),
    }
    if guard_owner_pid is not None:
        pending["guard_owner_pid"] = guard_owner_pid
    _write_json(backup / "update.json", pending)
    _write_json(fr_home / PENDING_UPDATE_FILENAME, pending)

    pip_command = [sys.executable, "-m", "pip", "install", "--upgrade"]
    if editable:
        pip_command.append("--editable")
    pip_command.append(requirement)
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
    ref: str | None = None,
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
            ref=ref,
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
    ref: str | None = None,
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
            ref=ref,
            editable=editable,
            guard_owner_pid=os.getpid(),
        )
        log(f"backup created:       {backup}")
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
    requested_ref: str | None = None,
) -> VersionInfo:
    info = current_version()
    value = info.to_dict() | {
        "installed_at": time.time(),
        "selected_source": source,
        "selected_ref": requested_ref,
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
            info = write_installed_version(
                fr_home,
                source=(
                    target.get("source")
                    if isinstance(target.get("source"), str)
                    else None
                ),
                requested_ref=(
                    target.get("requested_ref")
                    if isinstance(target.get("requested_ref"), str)
                    else None
                ),
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
    "update",
    "write_installed_version",
]
