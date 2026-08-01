"""Fault-injection tests for durable atomic recorder-state writes."""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from hermes_flight_recorder.collector import atomic_file


def test_completes_short_writes_and_syncs_file_and_directory(tmp_path, monkeypatch):
    path = tmp_path / "private.json"
    payload = b"private recorder state"
    real_write = os.write
    real_fsync = os.fsync
    real_replace = os.replace
    synced_modes: list[int] = []
    replacement_modes: list[int] = []

    def short_write(fd, data):
        return real_write(fd, data[:3])

    def record_sync(fd):
        synced_modes.append(os.fstat(fd).st_mode)
        return real_fsync(fd)

    def record_replace(source, destination):
        replacement_modes.append(stat.S_IMODE(Path(source).stat().st_mode))
        return real_replace(source, destination)

    monkeypatch.setattr(atomic_file.os, "write", short_write)
    monkeypatch.setattr(atomic_file.os, "fsync", record_sync)
    monkeypatch.setattr(atomic_file.os, "replace", record_replace)

    atomic_file.atomic_write(path, payload, mode=0o600)

    assert path.read_bytes() == payload
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert replacement_modes == [0o600]
    assert any(stat.S_ISREG(mode) for mode in synced_modes)
    if os.name == "posix":
        assert any(stat.S_ISDIR(mode) for mode in synced_modes)


def test_stop_before_replacement_preserves_old_file(tmp_path, monkeypatch):
    path = tmp_path / "private.json"
    path.write_bytes(b"old")

    def stop_before_replace(source, destination):
        raise OSError("stop before replacement")

    monkeypatch.setattr(atomic_file.os, "replace", stop_before_replace)

    with pytest.raises(OSError, match="before replacement"):
        atomic_file.atomic_write(path, b"new")

    assert path.read_bytes() == b"old"
    assert list(tmp_path.glob(".private.json.*.tmp")) == []


def test_stop_after_replacement_leaves_complete_new_file(tmp_path, monkeypatch):
    path = tmp_path / "private.json"
    path.write_bytes(b"old")
    real_replace = os.replace

    def stop_after_replace(source, destination):
        real_replace(source, destination)
        raise OSError("stop after replacement")

    monkeypatch.setattr(atomic_file.os, "replace", stop_after_replace)

    with pytest.raises(OSError, match="after replacement"):
        atomic_file.atomic_write(path, b"complete new value")

    assert path.read_bytes() == b"complete new value"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
