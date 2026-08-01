"""Two-stage package and installation update workflow.

The first stage runs from the old package: refuse a live recorder, create a
recoverable backup, and ask pip to install the selected source. A new Python
process then imports the newly installed package, applies outbox migrations,
regenerates the Hermes hook, stamps the installed build, and verifies it.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

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
_FULL_GIT_REVISION = re.compile(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}")


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


def _git_command(
    command: Sequence[str],
    *,
    runner: Callable[..., subprocess.CompletedProcess[Any]] = subprocess.run,
) -> str:
    try:
        result = runner(
            list(command), check=False, capture_output=True, text=True
        )
    except OSError as exc:
        raise UpdateError(f"could not run {command[0]}: {exc}") from exc
    if result.returncode:
        detail = (result.stderr or result.stdout or "").strip()
        suffix = f": {detail}" if detail else ""
        raise UpdateError(
            f"revision check failed with exit code {result.returncode}{suffix}"
        )
    return result.stdout


def _fetch_commit(
    git_url: str,
    revision: str,
    *,
    runner: Callable[..., subprocess.CompletedProcess[Any]] = subprocess.run,
) -> str:
    """Check an unadvertised full commit with a temporary shallow fetch."""
    with tempfile.TemporaryDirectory(prefix="hfr-revision-") as temporary:
        _git_command(["git", "init", "--bare", temporary], runner=runner)
        _git_command(
            [
                "git",
                "-C",
                temporary,
                "fetch",
                "--depth=1",
                "--no-tags",
                git_url,
                revision,
            ],
            runner=runner,
        )
        resolved = _git_command(
            ["git", "-C", temporary, "rev-parse", "FETCH_HEAD^{commit}"],
            runner=runner,
        ).strip()
    if not _FULL_GIT_REVISION.fullmatch(resolved):
        raise UpdateError(f"git returned an invalid revision for {revision!r}")
    return resolved.lower()


def resolve_git_revision(
    source: str,
    ref: str,
    *,
    runner: Callable[..., subprocess.CompletedProcess[Any]] = subprocess.run,
) -> str:
    """Resolve and check one remote branch, tag, or full commit."""
    git_url = source.removeprefix("git+")
    if _FULL_GIT_REVISION.fullmatch(ref):
        return _fetch_commit(git_url, ref.lower(), runner=runner)

    output = _git_command(["git", "ls-remote", git_url], runner=runner)
    advertised: dict[str, str] = {}
    for line in output.splitlines():
        fields = line.split(maxsplit=1)
        if len(fields) == 2 and _FULL_GIT_REVISION.fullmatch(fields[0]):
            advertised[fields[1]] = fields[0].lower()

    if ref.startswith("refs/tags/"):
        revision = advertised.get(ref + "^{}") or advertised.get(ref)
        if revision is None:
            raise UpdateError(f"git revision {ref!r} does not exist at {git_url}")
        return revision
    if ref.startswith("refs/"):
        revision = advertised.get(ref)
        if revision is None:
            raise UpdateError(f"git revision {ref!r} does not exist at {git_url}")
        return revision

    tag_revision = advertised.get(f"refs/tags/{ref}^{{}}") or advertised.get(
        f"refs/tags/{ref}"
    )
    branch_revision = advertised.get(f"refs/heads/{ref}")
    if tag_revision is None and branch_revision is None:
        if re.fullmatch(r"[0-9a-fA-F]{7,39}", ref):
            raise UpdateError("use the full commit hash, not an abbreviated hash")
        raise UpdateError(f"git revision {ref!r} does not exist at {git_url}")
    if (
        tag_revision is not None
        and branch_revision is not None
        and tag_revision != branch_revision
    ):
        raise UpdateError(
            f"git revision {ref!r} is ambiguous; use refs/heads/... or refs/tags/..."
        )
    if tag_revision is not None:
        return tag_revision
    assert branch_revision is not None
    return branch_revision


def _local_revision(source: Path) -> tuple[str | None, bool]:
    try:
        revision = subprocess.run(
            ["git", "-C", str(source), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        dirty = bool(
            subprocess.run(
                ["git", "-C", str(source), "status", "--porcelain"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
        )
    except (OSError, subprocess.CalledProcessError):
        return None, False
    return (revision.lower() if _FULL_GIT_REVISION.fullmatch(revision) else None), dirty


def _target(source: str, ref: str | None, *, editable: bool) -> dict[str, Any]:
    local = Path(source).expanduser()
    if local.exists():
        if ref:
            raise UpdateError(
                "--ref cannot be combined with a local --source; check out the "
                "desired revision in that source directory"
            )
        if not editable:
            raise UpdateError(
                "a local --source requires --editable and is for development only"
            )
        resolved, dirty = _local_revision(local.resolve())
        return {
            "source": str(local.resolve()),
            "requirement": str(local.resolve()),
            "requested_revision": "local checkout",
            "resolved_revision": resolved,
            "dirty": dirty,
            "editable": True,
        }
    if editable:
        raise UpdateError("--editable requires a local --source directory")
    if not source.startswith("git+"):
        raise UpdateError("--source must be a git+ URL or a local editable checkout")
    if not ref:
        raise UpdateError("a remote Git update requires --ref with a release, tag, or commit")
    resolved = resolve_git_revision(source, ref)
    return {
        "source": source,
        "requirement": f"{source}@{resolved}",
        "requested_revision": ref,
        "resolved_revision": resolved,
        "dirty": False,
        "editable": False,
    }


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
    target = _target(source, ref, editable=editable)
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
        pending = _read_json(fr_home / PENDING_UPDATE_FILENAME)
        target = pending["target"]
        log(f"backup created:       {backup}")
        log(f"requested revision:   {target['requested_revision']}")
        resolved = target.get("resolved_revision") or "unavailable (local checkout)"
        dirty = "-dirty" if target.get("dirty") else ""
        log(f"resolved revision:    {resolved}{dirty}")
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
    resolved_revision: str | None = None,
) -> VersionInfo:
    info = current_version()
    value = info.to_dict() | {
        "installed_at": time.time(),
        "selected_source": source,
        "selected_ref": requested_ref,
        "resolved_revision": resolved_revision,
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

    expected_revision = target.get("resolved_revision")
    if expected_revision is not None and not isinstance(expected_revision, str):
        raise UpdateError(f"invalid resolved revision in {pending_path}")
    installed_info = current_version()
    if expected_revision and installed_info.revision != expected_revision:
        raise UpdateError(
            f"installed revision {installed_info.revision!r} does not match "
            f"resolved revision {expected_revision!r}"
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
            info = write_installed_version(
                fr_home,
                source=(
                    target.get("source")
                    if isinstance(target.get("source"), str)
                    else None
                ),
                requested_ref=(
                    target.get("requested_revision")
                    if isinstance(target.get("requested_revision"), str)
                    else None
                ),
                resolved_revision=expected_revision,
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
    requested = target.get("requested_revision") or "unspecified"
    installed = installed_info.revision or "unknown"
    log(f"requested revision:   {requested}")
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
    "resolve_git_revision",
    "update",
    "write_installed_version",
]
