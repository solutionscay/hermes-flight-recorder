"""A cross-platform, single-instance runtime lock.

``serve`` runs one foreground process per Flight Recorder installation. This
guards that invariant with an OS advisory file lock on ``runtime.lock`` inside
the Flight Recorder home. The kernel releases the lock automatically when the
holding process exits or crashes, so there is no stale-PID file to reason about
and no PID-reuse hazard: liveness is the lock itself, never the file contents.

Standard library only (``fcntl`` on POSIX, ``msvcrt`` on Windows) so the core
package keeps its single ``cryptography`` runtime dependency. The lock is per
Flight Recorder home, so distinct installations never contend.
"""

from __future__ import annotations

import os
from pathlib import Path

LOCK_FILENAME = "runtime.lock"

# Import the platform lock primitive once. Guarded so importing this module
# never fails on either OS; the missing branch simply won't be exercised there.
try:  # POSIX
    import fcntl
except ImportError:  # pragma: no cover - Windows
    fcntl = None  # type: ignore[assignment]

try:  # Windows
    import msvcrt
except ImportError:  # pragma: no cover - POSIX
    msvcrt = None  # type: ignore[assignment]


class RuntimeLockError(RuntimeError):
    """Another process already holds the runtime lock."""


class RuntimeLock:
    """A non-reentrant, single-holder advisory lock backed by a lock file.

    Use as a context manager or call :meth:`acquire` / :meth:`release`. The
    lock is advisory (it only excludes other ``RuntimeLock`` holders of the
    same file), which is exactly the single-instance guarantee ``serve`` needs.
    """

    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)
        self._fd: int | None = None

    # --- lifecycle ------------------------------------------------------
    def acquire(self) -> None:
        """Take the lock without blocking.

        Raises :class:`RuntimeLockError` when another process holds it. The
        lock file's directory must already exist.
        """
        if self._fd is not None:
            return  # already held by this instance
        fd = os.open(str(self.path), os.O_RDWR | os.O_CREAT, 0o600)
        try:
            self._lock(fd)
        except OSError:
            holder = self._read_holder(fd)
            os.close(fd)
            detail = f" ({holder})" if holder else ""
            raise RuntimeLockError(
                f"another Flight Recorder process holds {self.path}{detail}"
            ) from None
        self._fd = fd
        self._write_holder(fd)

    def release(self) -> None:
        """Release the lock and close the descriptor. Idempotent."""
        if self._fd is None:
            return
        fd, self._fd = self._fd, None
        try:
            self._unlock(fd)
        finally:
            os.close(fd)

    def __enter__(self) -> "RuntimeLock":
        self.acquire()
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()

    # --- diagnostics ----------------------------------------------------
    @property
    def holder_info(self) -> str | None:
        """Best-effort ``"<pid> <start-epoch>"`` recorded by the current holder.

        For human diagnostics only — never parsed to decide liveness.
        """
        try:
            fd = os.open(str(self.path), os.O_RDONLY)
        except OSError:
            return None
        try:
            return self._read_holder(fd)
        finally:
            os.close(fd)

    # --- platform primitives -------------------------------------------
    @staticmethod
    def _lock(fd: int) -> None:
        if fcntl is not None:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        elif msvcrt is not None:  # pragma: no cover - Windows
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:  # pragma: no cover - no lock primitive
            raise RuntimeLockError("no file-locking primitive available")

    @staticmethod
    def _unlock(fd: int) -> None:
        if fcntl is not None:
            fcntl.flock(fd, fcntl.LOCK_UN)
        elif msvcrt is not None:  # pragma: no cover - Windows
            try:
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            except OSError:
                pass

    @staticmethod
    def _write_holder(fd: int) -> None:
        # `time` is imported lazily so the module has no import-time clock read.
        import time

        try:
            os.ftruncate(fd, 0)
            os.lseek(fd, 0, os.SEEK_SET)
            os.write(fd, f"{os.getpid()} {time.time():.0f}\n".encode("ascii"))
            # Windows locks a byte region from the current offset; keep the
            # descriptor pointing back at the locked byte after writing.
            os.lseek(fd, 0, os.SEEK_SET)
        except OSError:
            pass

    @staticmethod
    def _read_holder(fd: int) -> str | None:
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            data = os.read(fd, 256).decode("ascii", "replace").strip()
        except OSError:
            return None
        return data or None


__all__ = ["LOCK_FILENAME", "RuntimeLock", "RuntimeLockError"]
