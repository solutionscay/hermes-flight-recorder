"""Supported package/install update workflow (issue #115)."""

from __future__ import annotations

import json
import sqlite3
import subprocess
from pathlib import Path

import pytest

from hermes_flight_recorder import cli
from hermes_flight_recorder.collector._common import build_record
from hermes_flight_recorder.collector.hook import baked_flight_recorder_build
from hermes_flight_recorder.collector.outbox import Outbox, OutboxError
from hermes_flight_recorder.collector.runtime_lock import RuntimeLock
from hermes_flight_recorder.collector.update import (
    INSTALLED_VERSION_FILENAME,
    LAST_UPDATE_FILENAME,
    PENDING_UPDATE_FILENAME,
    UpdateError,
    complete_update,
    prepare_update,
    update,
)
from hermes_flight_recorder.version import build_identity


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
    event = outbox.append(_record(), dedup_key="update:event")
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
    assert (backup / "content-dev.key").read_bytes() == (
        fr_home / "content-dev.key"
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


def test_git_ref_builds_an_exact_pip_requirement(tmp_path):
    hermes, fr_home = _hermes(tmp_path)
    _backup, pip_command, _completion = prepare_update(
        fr_home,
        hermes,
        source="git+https://example.test/flight-recorder.git",
        ref="feature/update-test",
    )
    assert pip_command[-1] == (
        "git+https://example.test/flight-recorder.git@feature/update-test"
    )


def test_update_runs_pip_then_new_package_completion(tmp_path):
    hermes, fr_home = _hermes(tmp_path)
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    commands: list[list[str]] = []

    def runner(command, *, check):
        assert check is False
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


def test_failed_package_update_keeps_backup_and_marks_pending_state(tmp_path):
    hermes, fr_home = _hermes(tmp_path)

    def runner(command, *, check):
        assert check is False
        return subprocess.CompletedProcess(command, 9)

    with pytest.raises(UpdateError, match="exit code 9"):
        update(
            fr_home,
            hermes,
            runner=runner,
            log=lambda _message: None,
        )

    pending = json.loads((fr_home / PENDING_UPDATE_FILENAME).read_text())
    assert pending["state"] == "failed"
    backup = Path(pending["backup"])
    assert (backup / "outbox.sqlite").is_file()
    assert json.loads((backup / "update.json").read_text())["previous"]["build"]


def test_complete_update_preserves_state_and_refreshes_hook(tmp_path):
    hermes, fr_home = _hermes(tmp_path)
    checkout = tmp_path / "checkout"
    checkout.mkdir()

    outbox = Outbox.open(fr_home, hermes_home=hermes)
    outbox.append(_record(), dedup_key="update:preserved")
    installation_id = outbox.installation_id
    outbox.set_meta("cursor:delivery", "1")
    outbox.set_meta("capture:test", "42")
    outbox.close()

    config_before = (fr_home / "recorder-config.json").read_bytes()
    key_before = (fr_home / "content-dev.key").read_bytes()
    prepare_update(fr_home, hermes, source=str(checkout))
    complete_update(fr_home, hermes, log=lambda _message: None)

    updated = Outbox.open(fr_home, hermes_home=hermes)
    try:
        assert updated.installation_id == installation_id
        assert updated.count() == 1
        assert updated.get_meta("cursor:delivery") == "1"
        assert updated.get_meta("capture:test") == "42"
        assert updated.get_meta("outbox_schema_version") == "1"
        assert updated.get_meta("installed_build") == build_identity()
    finally:
        updated.close()
    assert (fr_home / "recorder-config.json").read_bytes() == config_before
    assert (fr_home / "content-dev.key").read_bytes() == key_before
    assert not (fr_home / PENDING_UPDATE_FILENAME).exists()
    assert json.loads((fr_home / LAST_UPDATE_FILENAME).read_text())["state"] == (
        "complete"
    )
    hook = hermes / "hooks" / "hermes-flight-recorder"
    assert baked_flight_recorder_build(hook) == build_identity()

    prepare_update(fr_home, hermes, source=str(checkout))
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
