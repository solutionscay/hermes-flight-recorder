"""Durable atomic file writes for recorder state."""

from __future__ import annotations

import errno
import json
import os
import tempfile
from pathlib import Path
from typing import Any


def atomic_write(path: Path, data: bytes, *, mode: int = 0o600) -> Path:
    """Durably replace one file after a complete private temporary write."""
    path = Path(path)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.chmod(temporary, mode)
        remaining = memoryview(data)
        while remaining:
            written = os.write(fd, remaining)
            if written <= 0:
                raise OSError(errno.EIO, "write returned no progress", temporary)
            remaining = remaining[written:]
        os.fsync(fd)
        os.close(fd)
        fd = -1
        os.replace(temporary, path)
        _sync_directory(path.parent)
        return path
    finally:
        if fd >= 0:
            os.close(fd)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def read_json_object(
    path: Path,
    *,
    error: type[Exception],
    description: str,
    missing_ok: bool = False,
) -> dict[str, Any]:
    """Read a private JSON file that must contain an object.

    Read and parse failures raise ``error``; a non-object document raises
    ``error`` too. With ``missing_ok`` an absent file is an empty object.
    """
    if missing_ok and not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise error(f"cannot read {description} at {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise error(f"{description} at {path} is not a JSON object")
    return value


def write_json_object(
    path: Path,
    value: dict[str, Any],
    *,
    error: type[Exception] | None = None,
    description: str = "JSON file",
    sort_keys: bool = False,
) -> Path:
    """Write a JSON object durably and atomically with mode ``0600``.

    ``OSError`` is wrapped in ``error`` when given, else it propagates.
    """
    text = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=sort_keys) + "\n"
    try:
        return atomic_write(path, text.encode("utf-8"), mode=0o600)
    except OSError as exc:
        if error is None:
            raise
        raise error(f"cannot write {description} at {path}: {exc}") from exc


def _sync_directory(directory: Path) -> None:
    """Sync a replacement into its parent directory on POSIX systems."""
    if os.name != "posix":
        return
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    fd = os.open(directory, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


__all__ = ["atomic_write", "read_json_object", "write_json_object"]
