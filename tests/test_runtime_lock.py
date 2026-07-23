"""Single-instance runtime lock (issue #101).

The lock enforces one ``serve`` process per Flight Recorder home. It is an OS
advisory file lock, so the kernel releases it when the holder exits — there is
no stale-PID reasoning.
"""

from __future__ import annotations

import os
import sys

import pytest

from hermes_flight_recorder.collector.runtime_lock import RuntimeLock, RuntimeLockError


def _lock_path(tmp_path):
    return tmp_path / "runtime.lock"


def test_acquire_then_second_acquire_raises(tmp_path):
    a = RuntimeLock(_lock_path(tmp_path))
    a.acquire()
    try:
        b = RuntimeLock(_lock_path(tmp_path))
        with pytest.raises(RuntimeLockError):
            b.acquire()
    finally:
        a.release()


def test_release_allows_reacquire(tmp_path):
    a = RuntimeLock(_lock_path(tmp_path))
    a.acquire()
    a.release()
    b = RuntimeLock(_lock_path(tmp_path))
    b.acquire()  # must not raise
    b.release()


def test_release_is_idempotent(tmp_path):
    a = RuntimeLock(_lock_path(tmp_path))
    a.acquire()
    a.release()
    a.release()  # no error on a second release


def test_context_manager_releases(tmp_path):
    with RuntimeLock(_lock_path(tmp_path)):
        pass
    # After the block, the lock is free.
    b = RuntimeLock(_lock_path(tmp_path))
    b.acquire()
    b.release()


def test_holder_info_records_pid(tmp_path):
    a = RuntimeLock(_lock_path(tmp_path))
    a.acquire()
    try:
        info = a.holder_info
        assert info is not None and info.split()[0] == str(os.getpid())
    finally:
        a.release()


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX flock semantics")
def test_lock_auto_released_when_fd_closes(tmp_path):
    # Simulate a crashed holder: close the descriptor without release(); the
    # kernel drops the advisory lock, so a fresh acquire succeeds.
    a = RuntimeLock(_lock_path(tmp_path))
    a.acquire()
    os.close(a._fd)  # holder "crashes" — no orderly release
    a._fd = None
    b = RuntimeLock(_lock_path(tmp_path))
    b.acquire()  # must not raise
    b.release()
