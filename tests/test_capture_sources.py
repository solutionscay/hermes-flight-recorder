"""Capture source selection and reconciliation exclusions (issue #98)."""

from __future__ import annotations

import sqlite3

import pytest

from hermes_flight_recorder import collector
from hermes_flight_recorder.collector import (
    cron_db,
    gateway_log,
    hook,
    kanban_db,
    knowledge_store,
    state_db,
)
from hermes_flight_recorder.collector import reconcile as reconcile_module
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.recorder_config import (
    CAPTURE_SOURCE_NAMES,
    CaptureConfig,
)


def new_outbox(tmp_path) -> Outbox:
    outbox = Outbox.open(tmp_path / "bridge")
    outbox.initialize()
    return outbox


@pytest.mark.parametrize("disabled", CAPTURE_SOURCE_NAMES)
def test_false_source_skips_only_that_collector(tmp_path, monkeypatch, disabled):
    calls: list[str] = []

    def poll(name):
        def fake(*args, **kwargs):
            calls.append(name)
            return {}

        return fake

    monkeypatch.setattr(hook, "drain", poll("hook"))
    monkeypatch.setattr(state_db, "poll", poll("state_db"))
    monkeypatch.setattr(cron_db, "poll", poll("cron"))
    monkeypatch.setattr(kanban_db, "poll", poll("kanban"))
    monkeypatch.setattr(gateway_log, "poll", poll("gateway_log"))
    monkeypatch.setattr(knowledge_store, "poll", poll("knowledge"))

    collector.run_pass(
        new_outbox(tmp_path),
        tmp_path / "hermes",
        capture_config=CaptureConfig(sources={disabled: False}),
    )

    assert set(calls) == set(CAPTURE_SOURCE_NAMES) - {disabled}


def test_missing_source_keys_enable_all_collectors(tmp_path, monkeypatch):
    calls: list[str] = []

    def poll(name):
        def fake(*args, **kwargs):
            calls.append(name)
            return {}

        return fake

    monkeypatch.setattr(hook, "drain", poll("hook"))
    monkeypatch.setattr(state_db, "poll", poll("state_db"))
    monkeypatch.setattr(cron_db, "poll", poll("cron"))
    monkeypatch.setattr(kanban_db, "poll", poll("kanban"))
    monkeypatch.setattr(gateway_log, "poll", poll("gateway_log"))
    monkeypatch.setattr(knowledge_store, "poll", poll("knowledge"))

    collector.run_pass(new_outbox(tmp_path), tmp_path / "hermes")

    assert set(calls) == set(CAPTURE_SOURCE_NAMES)


def test_disabled_state_db_does_not_report_coverage_gap(tmp_path):
    home = tmp_path / "hermes"
    home.mkdir()
    db = sqlite3.connect(home / "state.db")
    db.executescript(
        """
        CREATE TABLE sessions (
            id TEXT, source TEXT, parent_session_id TEXT, started_at REAL,
            ended_at REAL, expiry_finalized INT, profile_name TEXT
        );
        INSERT INTO sessions VALUES ('S', 'cli', NULL, 100, NULL, 0, NULL);
        """
    )
    db.close()

    counts = reconcile_module.reconcile(
        new_outbox(tmp_path),
        home,
        now=200,
        config=reconcile_module.ReconcileConfig(coverage_grace=0),
        capture_config=CaptureConfig(sources={"state_db": False}),
    )

    assert counts.get("reconcile.gap_detected", 0) == 0


def test_disabled_sources_skip_related_reconciliation(tmp_path, monkeypatch):
    def unexpected(*args, **kwargs):
        raise AssertionError("disabled source reached reconciliation")

    monkeypatch.setattr(reconcile_module, "_load_execution_rows", unexpected)
    monkeypatch.setattr(reconcile_module, "_detect_missing_terminals", unexpected)
    monkeypatch.setattr(reconcile_module, "_detect_missed_cron", unexpected)
    monkeypatch.setattr(reconcile_module, "_detect_gateway_start_failed", unexpected)
    monkeypatch.setattr(reconcile_module, "_detect_stale_task_leases", unexpected)
    monkeypatch.setattr(reconcile_module, "_detect_knowledge_gaps", unexpected)

    counts = reconcile_module.reconcile(
        new_outbox(tmp_path),
        tmp_path / "missing-hermes",
        now=200,
        capture_config=CaptureConfig(
            sources={name: False for name in CAPTURE_SOURCE_NAMES}
        ),
    )

    assert counts == {}


def test_frequent_reconcile_skips_complete_audit_scans(tmp_path, monkeypatch):
    def unexpected(*args, **kwargs):
        raise AssertionError("complete audit ran on the frequent path")

    monkeypatch.setattr(reconcile_module, "_load_execution_rows", unexpected)
    monkeypatch.setattr(reconcile_module, "_detect_sequence_gaps", unexpected)
    monkeypatch.setattr(reconcile_module, "_detect_coverage_gaps", unexpected)
    monkeypatch.setattr(reconcile_module, "_detect_missing_terminals", unexpected)
    monkeypatch.setattr(reconcile_module, "_detect_missed_cron", unexpected)
    monkeypatch.setattr(reconcile_module, "_detect_knowledge_gaps", unexpected)

    counts = reconcile_module.reconcile(
        new_outbox(tmp_path),
        tmp_path / "missing-hermes",
        now=200,
        full_audit=False,
    )

    assert counts == {}
