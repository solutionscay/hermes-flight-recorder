"""Supported package/install update workflow (issue #115)."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
from pathlib import Path

import pytest

from hermes_flight_recorder import cli
from hermes_flight_recorder.collector import update as update_module
from hermes_flight_recorder.collector._common import build_record
from hermes_flight_recorder.collector.hook import baked_flight_recorder_build
from hermes_flight_recorder.collector.outbox import (
    OUTBOX_SCHEMA_VERSION,
    Outbox,
    OutboxError,
)
from hermes_flight_recorder.collector.runtime_lock import RuntimeLock, RuntimeLockError
from hermes_flight_recorder.collector.update import (
    BACKUP_DIRNAME,
    INSTALLED_VERSION_FILENAME,
    LAST_UPDATE_FILENAME,
    PENDING_UPDATE_FILENAME,
    UpdateError,
    complete_update,
    prepare_update,
    update,
)
from hermes_flight_recorder.version import VersionInfo, build_identity


def _hermes(tmp_path: Path) -> tuple[Path, Path]:
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    (hermes / "config.yaml").write_text("terminal:\n  home_mode: auto\n")
    assert cli.main(["install", "--hermes-home", str(hermes)]) == 0
    return hermes, hermes / "flight-recorder"


def _record() -> dict:
    return build_record(
        event_type="session.created",
        occurred_at=1784415000.0,
        source="test",
        capture_method="test",
        runtime={"kind": "cli", "engine": "standard"},
        correlation_id="corr",
        payload={},
    )


def test_prepare_update_creates_consistent_recovery_backup(tmp_path):
    hermes, fr_home = _hermes(tmp_path)
    checkout = tmp_path / "checkout"
    checkout.mkdir()

    outbox = Outbox.open(fr_home, hermes_home=hermes)
    event = outbox.append(_record(), content=b"sealed", dedup_key="update:event")
    installation_id = outbox.installation_id
    outbox.set_meta("cursor:delivery", str(event["producer_sequence"]))
    outbox.set_meta("capture:test", "cursor-value")
    outbox.close()
    (fr_home / "sync-config.json").write_text('{"secret":"preserve"}')

    backup, pip_command, completion_command = prepare_update(
        fr_home,
        hermes,
        source=str(checkout),
        editable=True,
    )

    copied = sqlite3.connect(backup / "outbox.sqlite")
    try:
        assert copied.execute(
            "SELECT value FROM meta WHERE key='installation_id'"
        ).fetchone()[0] == installation_id
        assert copied.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
        assert copied.execute(
            "SELECT value FROM meta WHERE key='cursor:delivery'"
        ).fetchone()[0] == "1"
    finally:
        copied.close()
    assert (backup / "operator.pub").read_bytes() == (
        fr_home / "operator.pub"
    ).read_bytes()
    assert (backup / "operator.secret").read_bytes() == (
        fr_home / "operator.secret"
    ).read_bytes()
    assert (backup / "recorder-config.json").read_bytes() == (
        fr_home / "recorder-config.json"
    ).read_bytes()
    assert (backup / "sync-config.json").read_bytes() == (
        fr_home / "sync-config.json"
    ).read_bytes()
    assert (backup / "hook" / "handler.py").is_file()
    assert pip_command[-2:] == ["--editable", str(checkout.resolve())]
    assert completion_command[-4:] == [
        "--flight-recorder-home",
        str(fr_home),
        "--hermes-home",
        str(hermes),
    ]
    pending = json.loads((fr_home / PENDING_UPDATE_FILENAME).read_text())
    assert pending["state"] == "prepared"
    assert pending["backup"] == str(backup)
    assert json.loads((backup / "update.json").read_text()) == pending


def test_prepare_update_refuses_while_serve_lock_is_held(tmp_path):
    hermes, fr_home = _hermes(tmp_path)
    lock = RuntimeLock(fr_home / "runtime.lock")
    lock.acquire()
    try:
        with pytest.raises(UpdateError, match="stop `serve`"):
            prepare_update(fr_home, hermes)
    finally:
        lock.release()
    assert not (fr_home / PENDING_UPDATE_FILENAME).exists()


def test_git_commit_produces_an_exact_pip_requirement(tmp_path, monkeypatch):
    hermes, fr_home = _hermes(tmp_path)
    revision = "a" * 40
    monkeypatch.setattr(
        update_module, "validate_git_commit", lambda source, commit: revision
    )
    _backup, pip_command, _completion = prepare_update(
        fr_home,
        hermes,
        source="git+https://example.test/flight-recorder.git",
        commit=revision,
    )
    assert pip_command[-1] == f"git+https://example.test/flight-recorder.git@{revision}"
    target = json.loads((fr_home / PENDING_UPDATE_FILENAME).read_text())["target"]
    assert target["commit"] == revision


def test_remote_update_requires_a_revision(tmp_path):
    hermes, fr_home = _hermes(tmp_path)

    with pytest.raises(UpdateError, match="requires --commit"):
        prepare_update(fr_home, hermes)

    assert not (fr_home / BACKUP_DIRNAME).exists()


def test_revision_check_runs_before_backup(tmp_path, monkeypatch):
    hermes, fr_home = _hermes(tmp_path)

    def missing_revision(source, commit):
        raise UpdateError("revision does not exist")

    monkeypatch.setattr(update_module, "validate_git_commit", missing_revision)
    with pytest.raises(UpdateError, match="does not exist"):
        prepare_update(
            fr_home,
            hermes,
            source="git+https://example.test/flight-recorder.git",
            commit="a" * 40,
        )

    assert not (fr_home / BACKUP_DIRNAME).exists()


@pytest.mark.parametrize("commit", ["v1.2.3", "main", "abc1234"])
def test_remote_update_rejects_any_non_full_commit(tmp_path, commit):
    hermes, fr_home = _hermes(tmp_path)

    with pytest.raises(UpdateError, match="full 40-character or 64-character hash"):
        prepare_update(
            fr_home,
            hermes,
            source="git+https://example.test/flight-recorder.git",
            commit=commit,
        )

    assert not (fr_home / BACKUP_DIRNAME).exists()


def test_full_commit_is_checked_with_a_shallow_fetch():
    commit = "2" * 40
    commands = []

    def runner(command, **_kwargs):
        commands.append(command)
        output = f"{commit}\n" if command[-2:] == ["rev-parse", "FETCH_HEAD^{commit}"] else ""
        return subprocess.CompletedProcess(command, 0, stdout=output, stderr="")

    assert (
        update_module.validate_git_commit(
            "git+https://example.test/flight-recorder.git",
            commit,
            runner=runner,
        )
        == commit
    )
    assert commands[1][-2:] == ["https://example.test/flight-recorder.git", commit]


def test_update_runs_pip_then_new_package_completion(tmp_path):
    hermes, fr_home = _hermes(tmp_path)
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    commands: list[list[str]] = []

    def runner(command, *, check):
        assert check is False
        restart_lock = RuntimeLock(fr_home / "runtime.lock")
        with pytest.raises(RuntimeLockError):
            restart_lock.acquire()
        commands.append(command)
        return subprocess.CompletedProcess(command, 0)

    update(
        fr_home,
        hermes,
        source=str(checkout),
        editable=True,
        runner=runner,
        log=lambda _message: None,
    )

    assert commands[0][1:5] == ["-m", "pip", "install", "--upgrade"]
    assert commands[1][1:5] == [
        "-m",
        "hermes_flight_recorder.cli",
        "update",
        "--complete",
    ]
    assert commands[1][-2:] == ["--guard-owner-pid", str(os.getpid())]


def test_guarded_completion_uses_the_parent_update_lock(tmp_path):
    hermes, fr_home = _hermes(tmp_path)
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    holder = RuntimeLock(fr_home / "runtime.lock")
    holder.acquire()
    try:
        from hermes_flight_recorder.collector import update as update_module

        update_module._prepare_update_locked(
            fr_home,
            hermes,
            source=str(checkout),
            editable=True,
            guard_owner_pid=os.getpid(),
        )
        complete_update(
            fr_home,
            hermes,
            guard_owner_pid=os.getpid(),
            log=lambda _message: None,
        )
    finally:
        holder.release()

    assert not (fr_home / PENDING_UPDATE_FILENAME).exists()
    assert json.loads((fr_home / LAST_UPDATE_FILENAME).read_text())["state"] == (
        "complete"
    )


def test_completion_rejects_an_unexpected_installed_revision(tmp_path, monkeypatch):
    hermes, fr_home = _hermes(tmp_path)
    expected = "a" * 40
    installed = "b" * 40
    monkeypatch.setattr(
        update_module, "validate_git_commit", lambda source, commit: expected
    )
    prepare_update(
        fr_home,
        hermes,
        source="git+https://example.test/flight-recorder.git",
        commit=expected,
    )
    monkeypatch.setattr(
        update_module,
        "current_version",
        lambda: VersionInfo("1.2.3", installed, "v1.2.3", "git+https://example.test"),
    )

    with pytest.raises(UpdateError, match="does not match requested commit"):
        complete_update(fr_home, hermes, log=lambda _message: None)

    assert not (fr_home / LAST_UPDATE_FILENAME).exists()


def test_completion_reports_requested_and_installed_revisions(tmp_path, monkeypatch):
    hermes, fr_home = _hermes(tmp_path)
    installed = update_module.current_version().revision
    assert installed is not None
    monkeypatch.setattr(
        update_module, "validate_git_commit", lambda source, commit: installed
    )
    prepare_update(
        fr_home,
        hermes,
        source="git+https://example.test/flight-recorder.git",
        commit=installed,
    )
    messages: list[str] = []

    complete_update(fr_home, hermes, log=messages.append)

    assert f"requested commit:     {installed}" in messages
    assert f"installed revision:   {installed}" in messages
    record = json.loads((fr_home / INSTALLED_VERSION_FILENAME).read_text())
    assert record["selected_commit"] == installed
    assert record["installed_revision"] == installed


def test_failed_package_update_keeps_backup_and_marks_pending_state(tmp_path):
    hermes, fr_home = _hermes(tmp_path)
    checkout = tmp_path / "checkout"
    checkout.mkdir()

    def runner(command, *, check):
        assert check is False
        return subprocess.CompletedProcess(command, 9)

    with pytest.raises(UpdateError, match="exit code 9"):
        update(
            fr_home,
            hermes,
            source=str(checkout),
            editable=True,
            runner=runner,
            log=lambda _message: None,
        )

    pending = json.loads((fr_home / PENDING_UPDATE_FILENAME).read_text())
    assert pending["state"] == "failed"
    assert pending["failed_stage"] == "package-replacement"
    backup = Path(pending["backup"])
    assert (backup / "outbox.sqlite").is_file()
    assert json.loads((backup / "update.json").read_text())["previous"]["build"]


def test_failed_completion_records_the_recovery_state(tmp_path, monkeypatch):
    hermes, fr_home = _hermes(tmp_path)
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    prepare_update(fr_home, hermes, source=str(checkout), editable=True)

    def fail_hook(*_args, **_kwargs):
        raise OSError("hook refresh failed")

    monkeypatch.setattr(update_module, "install_hook", fail_hook)
    with pytest.raises(UpdateError, match="hook refresh failed"):
        complete_update(fr_home, hermes, log=lambda _message: None)

    pending = json.loads((fr_home / PENDING_UPDATE_FILENAME).read_text())
    assert pending["state"] == "failed"
    assert pending["failed_stage"] == "completing"
    assert pending["failure"] == "hook refresh failed"
    assert pending["completion_started_at"] <= pending["failed_at"]
    assert Path(pending["backup"]).is_dir()


def test_complete_update_preserves_state_and_refreshes_hook(tmp_path):
    hermes, fr_home = _hermes(tmp_path)
    checkout = tmp_path / "checkout"
    checkout.mkdir()

    outbox = Outbox.open(fr_home, hermes_home=hermes)
    outbox.append(_record(), content=b"sealed", dedup_key="update:preserved")
    installation_id = outbox.installation_id
    outbox.set_meta("cursor:delivery", "1")
    outbox.set_meta("capture:test", "42")
    outbox.close()

    config_before = (fr_home / "recorder-config.json").read_bytes()
    public_before = (fr_home / "operator.pub").read_bytes()
    secret_before = (fr_home / "operator.secret").read_bytes()
    prepare_update(fr_home, hermes, source=str(checkout), editable=True)
    complete_update(fr_home, hermes, log=lambda _message: None)

    updated = Outbox.open(fr_home, hermes_home=hermes)
    try:
        assert updated.installation_id == installation_id
        assert updated.count() == 1
        assert updated.get_meta("cursor:delivery") == "1"
        assert updated.get_meta("capture:test") == "42"
        assert updated.get_meta("outbox_schema_version") == OUTBOX_SCHEMA_VERSION
        assert updated.get_meta("installed_build") == build_identity()
    finally:
        updated.close()
    assert (fr_home / "recorder-config.json").read_bytes() == config_before
    assert (fr_home / "operator.pub").read_bytes() == public_before
    assert (fr_home / "operator.secret").read_bytes() == secret_before
    assert not (fr_home / PENDING_UPDATE_FILENAME).exists()
    assert json.loads((fr_home / LAST_UPDATE_FILENAME).read_text())["state"] == (
        "complete"
    )
    hook = hermes / "hooks" / "hermes-flight-recorder"
    assert baked_flight_recorder_build(hook) == build_identity()

    prepare_update(fr_home, hermes, source=str(checkout), editable=True)
    complete_update(fr_home, hermes, log=lambda _message: None)
    reopened = Outbox.open(fr_home, hermes_home=hermes)
    try:
        assert reopened.installation_id == installation_id
    finally:
        reopened.close()


def test_registered_migration_failure_rolls_back_schema_and_version(
    tmp_path, monkeypatch
):
    import hermes_flight_recorder.collector.outbox as outbox_module

    hermes, fr_home = _hermes(tmp_path)
    conn = sqlite3.connect(fr_home / "outbox.sqlite")
    conn.execute("UPDATE meta SET value='0' WHERE key='outbox_schema_version'")
    conn.commit()
    conn.close()

    def fail_migration(self):
        self._conn.execute("CREATE TABLE migration_probe(value TEXT)")
        raise RuntimeError("migration failed")

    monkeypatch.setitem(outbox_module._MIGRATIONS, "0", ("1", "_fail_migration"))
    monkeypatch.setattr(Outbox, "_fail_migration", fail_migration, raising=False)

    with pytest.raises(RuntimeError, match="migration failed"):
        Outbox.open(fr_home, hermes_home=hermes)

    conn = sqlite3.connect(fr_home / "outbox.sqlite")
    try:
        assert conn.execute(
            "SELECT value FROM meta WHERE key='outbox_schema_version'"
        ).fetchone()[0] == "0"
        assert conn.execute(
            "SELECT name FROM sqlite_master WHERE name='migration_probe'"
        ).fetchone() is None
    finally:
        conn.close()


def test_schema_v1_migrates_knowledge_omissions(tmp_path):
    hermes, fr_home = _hermes(tmp_path)
    database = fr_home / "outbox.sqlite"
    conn = sqlite3.connect(database)
    conn.executescript(
        """
        ALTER TABLE knowledge_version RENAME TO knowledge_version_v2;
        CREATE TABLE knowledge_version (
            artifact_id TEXT NOT NULL,
            seq INTEGER NOT NULL,
            manifest_json TEXT NOT NULL,
            manifest_hash TEXT NOT NULL,
            occurred_at REAL NOT NULL,
            origin TEXT NOT NULL,
            linked_event_id TEXT,
            is_tombstone INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (artifact_id, seq)
        );
        INSERT INTO knowledge_version
        SELECT artifact_id, seq, manifest_json, manifest_hash, occurred_at,
               origin, linked_event_id, is_tombstone
        FROM knowledge_version_v2;
        DROP TABLE knowledge_version_v2;
        UPDATE meta SET value='1' WHERE key='outbox_schema_version';
        """
    )
    conn.commit()
    conn.close()

    migrated = Outbox.open(fr_home, hermes_home=hermes)
    try:
        columns = {
            row[1]
            for row in migrated._conn.execute("PRAGMA table_info(knowledge_version)")
        }
        assert "skipped_json" in columns
        assert migrated.get_meta("outbox_schema_version") == OUTBOX_SCHEMA_VERSION
    finally:
        migrated.close()


def test_unknown_schema_version_is_rejected(tmp_path):
    hermes, fr_home = _hermes(tmp_path)
    conn = sqlite3.connect(fr_home / "outbox.sqlite")
    conn.execute("UPDATE meta SET value='999' WHERE key='outbox_schema_version'")
    conn.commit()
    conn.close()
    with pytest.raises(OutboxError, match="unsupported outbox schema"):
        Outbox.open(fr_home, hermes_home=hermes)


def test_status_detects_hook_package_mismatch(tmp_path, capsys):
    hermes, fr_home = _hermes(tmp_path)
    outbox = Outbox.open(fr_home, hermes_home=hermes)
    outbox.set_meta("capture:last_success_at", "9999999999")
    outbox.close()
    handler = hermes / "hooks" / "hermes-flight-recorder" / "handler.py"
    handler.write_text(
        handler.read_text().replace(
            f"_FLIGHT_RECORDER_BUILD = {json.dumps(build_identity())}",
            '_FLIGHT_RECORDER_BUILD = "older-build"',
        )
    )

    code = cli.main(["status", "--hermes-home", str(hermes)])
    output = capsys.readouterr().out
    assert code == 1
    assert "MISMATCH" in output
    assert "older-build" in output


def test_install_records_meaningful_version_and_build(tmp_path):
    hermes, fr_home = _hermes(tmp_path)
    installed = json.loads((fr_home / INSTALLED_VERSION_FILENAME).read_text())
    assert installed["version"] == "0.1.0.dev0"
    assert installed["build"].startswith("0.1.0.dev0 (")
    hook = hermes / "hooks" / "hermes-flight-recorder"
    assert baked_flight_recorder_build(hook) == installed["build"]
