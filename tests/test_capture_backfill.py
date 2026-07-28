"""The ``install --no-backfill`` capture horizon (issue #111).

By default capture backfills the whole Hermes history. ``--no-backfill`` records
only activity at or after the install moment, so a fresh install over a
long-lived Hermes home does not ingest the entire past.
"""

from __future__ import annotations

import sqlite3

import pytest

from hermes_flight_recorder.collector import lifecycle, run_pass
from hermes_flight_recorder.collector._common import (
    CAPTURE_BACKFILL_META_KEY,
    INSTALLED_AT_META_KEY,
)
from hermes_flight_recorder.collector.outbox import Outbox


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("SC_HERMES_FLIGHT_RECORDER_HOME", raising=False)
    monkeypatch.delenv("HERMES_HOME", raising=False)


def _make_state(hermes, sessions, messages) -> None:
    db = sqlite3.connect(hermes / "state.db")
    db.executescript(
        """
        CREATE TABLE sessions (id TEXT, source TEXT, parent_session_id TEXT, model TEXT,
            message_count INT, tool_call_count INT, input_tokens INT, output_tokens INT,
            estimated_cost_usd REAL, started_at REAL, ended_at REAL, end_reason TEXT,
            profile_name TEXT, expiry_finalized INT);
        CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT,
            tool_name TEXT, tool_call_id TEXT, effect_disposition TEXT, content TEXT,
            timestamp REAL, finish_reason TEXT);
        CREATE TABLE session_model_usage (session_id TEXT, model TEXT, task TEXT);
        """
    )
    for sid, started in sessions:
        db.execute(
            "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (sid, "cli", None, "m", 0, 0, 0, 0, 0.0, started, None, None, "default", 1),
        )
    for i, (sid, role, ts, content) in enumerate(messages, 1):
        db.execute(
            "INSERT INTO messages VALUES (?,?,?,?,?,?,?,?,?)",
            (i, sid, role, None, None, None, content, ts, None),
        )
    db.commit()
    db.close()


def _install(tmp_path, *, backfill: bool):
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    (hermes / "config.yaml").write_text("terminal:\n  home_mode: auto\n")
    fr = lifecycle.install(None, str(hermes), backfill=backfill, log=lambda *a: None)
    return hermes, fr


def _counts(ob):
    sessions = sum(
        1 for e in ob.iter_events() if e["payload"]["event_type"] == "session.created"
    )
    messages = sum(1 for e in ob.iter_events() if "message_row_id" in e["payload"])
    return sessions, messages


def test_default_install_backfills_history(tmp_path):
    hermes, fr = _install(tmp_path, backfill=True)
    ob = Outbox.open(fr)
    horizon = float(ob.get_meta(INSTALLED_AT_META_KEY))
    _make_state(
        hermes,
        [("OLD", horizon - 1000), ("NEW", horizon + 1000)],
        [("OLD", "user", horizon - 1000, "old"), ("NEW", "user", horizon + 1000, "new")],
    )
    run_pass(ob, str(hermes))
    assert _counts(ob) == (2, 2)  # both historical and new captured
    assert ob.get_meta(CAPTURE_BACKFILL_META_KEY) is None  # flag not set
    ob.close()


def test_no_backfill_skips_history_keeps_new(tmp_path):
    hermes, fr = _install(tmp_path, backfill=False)
    ob = Outbox.open(fr)
    assert ob.get_meta(CAPTURE_BACKFILL_META_KEY) == "false"
    horizon = float(ob.get_meta(INSTALLED_AT_META_KEY))
    _make_state(
        hermes,
        [("OLD", horizon - 1000), ("NEW", horizon + 1000)],
        [("OLD", "user", horizon - 1000, "old"), ("NEW", "user", horizon + 1000, "new")],
    )
    run_pass(ob, str(hermes))
    assert _counts(ob) == (1, 1)  # only the post-install session and message
    types = {e["payload"]["event_type"] for e in ob.iter_events()}
    assert "session.created" in types
    ob.close()


def test_choice_persists_across_reopen(tmp_path):
    # The flag lives in the outbox, so a later run_pass (new process) honors it
    # without any CLI argument.
    hermes, fr = _install(tmp_path, backfill=False)
    ob = Outbox.open(fr)
    horizon = float(ob.get_meta(INSTALLED_AT_META_KEY))
    ob.close()

    _make_state(
        hermes,
        [("OLD", horizon - 1000)],
        [("OLD", "user", horizon - 1000, "old")],
    )
    reopened = Outbox.open(fr)  # fresh handle, as serve/run would open
    run_pass(reopened, str(hermes))
    assert _counts(reopened) == (0, 0)  # nothing historical, even after reopen
    reopened.close()
