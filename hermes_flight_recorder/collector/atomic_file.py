"""Durable atomic file writes for recorder state."""

from __future__ import annotations

import errno
import os
import tempfile
from pathlib import Path


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


__all__ = ["atomic_write"]
