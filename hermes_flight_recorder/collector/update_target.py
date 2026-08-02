"""Validate and build an immutable package update target."""

from __future__ import annotations

import re
import subprocess
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

_FULL_GIT_COMMIT = re.compile(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}")


class UpdateError(RuntimeError):
    """The package or local installation could not be updated safely."""


def _git_command(
    command: Sequence[str],
    *,
    runner: Callable[..., subprocess.CompletedProcess[Any]] = subprocess.run,
) -> str:
    try:
        result = runner(list(command), check=False, capture_output=True, text=True)
    except OSError as exc:
        raise UpdateError(f"could not run {command[0]}: {exc}") from exc
    if result.returncode:
        detail = (result.stderr or result.stdout or "").strip()
        suffix = f": {detail}" if detail else ""
        raise UpdateError(
            f"commit check failed with exit code {result.returncode}{suffix}"
        )
    return result.stdout


def _fetch_commit(
    git_url: str,
    commit: str,
    *,
    runner: Callable[..., subprocess.CompletedProcess[Any]] = subprocess.run,
) -> str:
    with tempfile.TemporaryDirectory(prefix="hfr-commit-") as temporary:
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
                commit,
            ],
            runner=runner,
        )
        resolved = _git_command(
            ["git", "-C", temporary, "rev-parse", "FETCH_HEAD^{commit}"],
            runner=runner,
        ).strip()
    if not _FULL_GIT_COMMIT.fullmatch(resolved):
        raise UpdateError(f"git returned an invalid commit for {commit!r}")
    return resolved.lower()


def validate_git_commit(
    source: str,
    commit: str,
    *,
    runner: Callable[..., subprocess.CompletedProcess[Any]] = subprocess.run,
) -> str:
    """Check that a full commit hash exists in the remote repository."""
    if not _FULL_GIT_COMMIT.fullmatch(commit):
        raise UpdateError("--commit must be a full 40-character or 64-character hash")
    return _fetch_commit(source.removeprefix("git+"), commit.lower(), runner=runner)


def _local_commit(source: Path) -> tuple[str | None, bool]:
    try:
        commit = subprocess.run(
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
    return (commit.lower() if _FULL_GIT_COMMIT.fullmatch(commit) else None), dirty


def build_update_target(
    source: str,
    commit: str | None,
    *,
    editable: bool,
    validator: Callable[[str, str], str] = validate_git_commit,
) -> dict[str, Any]:
    local = Path(source).expanduser()
    if local.exists():
        if commit:
            raise UpdateError(
                "--commit cannot be combined with a local --source; check out the "
                "desired revision in that source directory"
            )
        if not editable:
            raise UpdateError(
                "a local --source requires --editable and is for development only"
            )
        resolved, dirty = _local_commit(local.resolve())
        return {
            "source": str(local.resolve()),
            "requirement": str(local.resolve()),
            "commit": resolved,
            "dirty": dirty,
            "editable": True,
        }
    if editable:
        raise UpdateError("--editable requires a local --source directory")
    if not source.startswith("git+"):
        raise UpdateError("--source must be a git+ URL or a local editable checkout")
    if not commit:
        raise UpdateError("a remote Git update requires --commit with a full hash")
    resolved = validator(source, commit)
    return {
        "source": source,
        "requirement": f"{source}@{resolved}",
        "commit": resolved,
        "dirty": False,
        "editable": False,
    }


__all__ = ["UpdateError", "build_update_target", "validate_git_commit"]
