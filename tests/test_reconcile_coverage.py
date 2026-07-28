"""Focused tests for the reconciler's coverage-gap detector (issue #6).

``_detect_coverage_gaps`` diffs the durable stores (``state.db``,
``cron/executions.db``) against the captured outbox stream. A durable row
with no matching captured event proves a dropped capture and surfaces as
``reconcile.gap_detected`` / ``gap_kind='uncaptured_row'``.

This module is self-contained: it does not import anything from
``tests/test_reconcile.py``. Everything is driven by a fixed epoch anchor
and an explicit ``ReconcileConfig`` with small windows, so wall-clock never
enters and terminal-missing detection never interferes with the assertions
made here.
"""

from __future__ import annotations

import datetime
import sqlite3

from hermes_flight_recorder.collector._common import INSTALLED_AT_META_KEY, build_record
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile
from hermes_flight_recorder.envelope import validate

# A fixed epoch anchor and a US-Central-like offset, mirroring the cron store.
B = 1784415000.0
TZ = datetime.timezone(datetime.timedelta(hours=-5))


def iso(epoch: float) -> str:
    return datetime.datetime.fromtimestamp(epoch, TZ).isoformat()


def new_outbox(tmp_path) -> Outbox:
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


def append_event(ob, event_type, **over):
    """Append a minimal valid producer event straight to the outbox."""
    rec = build_record(
        event_type=event_type,
        occurred_at=over.pop("occurred_at", B),
        source=over.pop("source", "hook:test"),
        capture_method=over.pop("capture_method", "hook:test"),
        runtime={"kind": "cli", "engine": "standard"},
        correlation_id=over.pop("correlation_id", "corr"),
        payload=over.pop("payload", {}),
        **over,
    )
    return ob.append(rec)


def coverage_gaps(ob, subject_type: str | None = None):
    """All reconciler-emitted ``uncaptured_row`` findings, optionally filtered."""
    out = []
    for e in ob.iter_events():
        if e.get("source") != "reconciler":
            continue
        pl = e["payload"]
        if pl.get("event_type") != "reconcile.gap_detected":
            continue
        if pl.get("gap_kind") != "uncaptured_row":
            continue
        if subject_type is not None and pl.get("subject_type") != subject_type:
            continue
        out.append(e)
    return out


# --- durable-store builders ----------------------------------------------
_SESSIONS_SCHEMA = """
CREATE TABLE sessions (id TEXT, source TEXT, parent_session_id TEXT,
    started_at REAL, ended_at REAL, expiry_finalized INT, profile_name TEXT);
CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT,
    content TEXT);
CREATE TABLE session_model_usage (session_id TEXT, model TEXT, task TEXT);
"""


def make_state_db(hh, *, sessions=(), messages=(), model_usage=()):
    """Build a minimal state.db. Each arg is a list of column-value tuples
    matching the CREATE TABLE column order above.
    """
    db = sqlite3.connect(hh / "state.db")
    db.executescript(_SESSIONS_SCHEMA)
    db.executemany("INSERT INTO sessions VALUES (?,?,?,?,?,?,?)", sessions)
    db.executemany("INSERT INTO messages VALUES (?,?,?,?)", messages)
    db.executemany("INSERT INTO session_model_usage VALUES (?,?,?)", model_usage)
    db.commit()
    db.close()


def make_executions_db(cron_dir, rows):
    """rows: list of (id, job_id, status, claimed_at_iso, started_at_iso, finished_at_iso)."""
    db = sqlite3.connect(cron_dir / "executions.db")
    db.execute(
        "CREATE TABLE executions (id TEXT, job_id TEXT, source TEXT, pid INT, status TEXT, "
        "claimed_at TEXT, started_at TEXT, finished_at TEXT, error TEXT)"
    )
    db.executemany(
        "INSERT INTO executions VALUES (?,?,'builtin',1,?,?,?,?,NULL)", rows
    )
    db.commit()
    db.close()


# Small, explicit, deterministic windows — never wall-clock defaults.
CFG = ReconcileConfig(
    coverage_grace=0.0,
    session_terminal_timeout=100.0,
    subagent_terminal_timeout=100.0,
    invocation_terminal_timeout=100.0,
    cron_run_terminal_timeout=100.0,
    ticker_stale_after=100.0,
    cron_match_slack=45.0,
    once_match_slack=300.0,
    cron_lookback=3600.0,
)


# --- session coverage -----------------------------------------------------
def test_uncaptured_session_surfaces_once_as_coverage_gap(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    # started_at == now, so this is NOT also terminal-missing (age 0).
    make_state_db(hh, sessions=[("S1", "cli", None, B, None, 0, None)])
    ob = new_outbox(tmp_path)

    reconcile(ob, hh, now=B, config=CFG)

    gaps = coverage_gaps(ob, "session")
    assert len(gaps) == 1
    g = gaps[0]
    assert g["payload"]["subject_id"] == "S1"
    assert g["payload"]["source_table"] == "state.db:sessions"
    assert g["correlation_id"] == "S1"
    assert g["session_id"] == "S1"
    assert g["partial"] is True
    validate(g)


def test_captured_session_does_not_surface(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_state_db(hh, sessions=[("S1", "cli", None, B, B + 1, 1, None)])
    ob = new_outbox(tmp_path)
    append_event(ob, "session.created", session_id="S1", correlation_id="S1")

    reconcile(ob, hh, now=B, config=CFG)

    assert coverage_gaps(ob, "session") == []


def test_uncaptured_subagent_correlates_to_root_session(tmp_path):
    """A coverage gap for a subagent's own row correlates to the root ancestor,
    not to itself, per root_session() walking parent_session_id.
    """
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_state_db(
        hh,
        sessions=[
            ("P", "cli", None, B, B + 1, 1, None),  # closed root, no gap expected
            ("C", "subagent", "P", B, None, 0, None),  # open child, uncaptured
        ],
    )
    ob = new_outbox(tmp_path)
    # Only the parent is captured; the child row is left uncaptured.
    append_event(ob, "session.created", session_id="P", correlation_id="P")

    reconcile(ob, hh, now=B, config=CFG)

    gaps = coverage_gaps(ob, "session")
    assert len(gaps) == 1
    g = gaps[0]
    assert g["payload"]["subject_id"] == "C"
    assert g["correlation_id"] == "P"  # root, not "C"
    assert g["session_id"] == "C"
    assert g["parent_session_id"] == "P"


# --- tool-message coverage --------------------------------------------------
def test_uncaptured_tool_message_surfaces_once_as_coverage_gap(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_state_db(
        hh,
        sessions=[("P", "cli", None, B, B + 1, 1, None)],
        messages=[(10, "P", "tool", "{}")],
    )
    ob = new_outbox(tmp_path)
    append_event(ob, "session.created", session_id="P", correlation_id="P")

    reconcile(ob, hh, now=B, config=CFG)

    gaps = coverage_gaps(ob, "message")
    assert len(gaps) == 1
    g = gaps[0]
    assert g["payload"]["subject_id"] == "10"  # str(message id)
    assert g["payload"]["source_table"] == "state.db:messages"
    assert g["session_id"] == "P"
    assert g["correlation_id"] == "P"


def test_captured_tool_message_does_not_surface(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_state_db(
        hh,
        sessions=[("P", "cli", None, B, B + 1, 1, None)],
        messages=[(10, "P", "tool", "{}")],
    )
    ob = new_outbox(tmp_path)
    append_event(ob, "session.created", session_id="P", correlation_id="P")
    append_event(
        ob, "tool.call_completed",
        session_id="P", correlation_id="P",
        payload={"message_row_id": 10},
    )

    reconcile(ob, hh, now=B, config=CFG)

    assert coverage_gaps(ob, "message") == []


def test_uncaptured_user_and_assistant_messages_surface_as_coverage_gaps(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_state_db(
        hh,
        sessions=[("P", "cli", None, B, B + 1, 1, None)],
        messages=[
            (11, "P", "user", "prompt"),
            (12, "P", "assistant", "response"),
            (13, "P", "assistant", ""),  # tool-call scaffold is out of scope
        ],
    )
    ob = new_outbox(tmp_path)
    append_event(ob, "session.created", session_id="P", correlation_id="P")

    reconcile(ob, hh, now=B, config=CFG)

    assert {
        gap["payload"]["subject_id"] for gap in coverage_gaps(ob, "message")
    } == {"11", "12"}


def test_captured_user_and_assistant_messages_do_not_surface(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_state_db(
        hh,
        sessions=[("P", "cli", None, B, B + 1, 1, None)],
        messages=[
            (11, "P", "user", "prompt"),
            (12, "P", "assistant", "response"),
        ],
    )
    ob = new_outbox(tmp_path)
    append_event(ob, "session.created", session_id="P", correlation_id="P")
    append_event(
        ob,
        "invocation.started",
        session_id="P",
        correlation_id="P",
        payload={"message_row_id": 11},
    )
    append_event(
        ob,
        "invocation.completed",
        session_id="P",
        correlation_id="P",
        payload={"message_row_id": 12},
    )

    reconcile(ob, hh, now=B, config=CFG)

    assert coverage_gaps(ob, "message") == []


# --- model-usage coverage ---------------------------------------------------
def test_uncaptured_model_usage_surfaces_once_as_coverage_gap(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_state_db(
        hh,
        sessions=[("P", "cli", None, B, B + 1, 1, None)],
        model_usage=[("P", "claude-x", "chat")],
    )
    ob = new_outbox(tmp_path)
    append_event(ob, "session.created", session_id="P", correlation_id="P")

    reconcile(ob, hh, now=B, config=CFG)

    gaps = coverage_gaps(ob, "model_usage")
    assert len(gaps) == 1
    g = gaps[0]
    assert g["payload"]["subject_id"] == "P:claude-x:chat"
    assert g["payload"]["source_table"] == "state.db:session_model_usage"
    assert g["session_id"] == "P"
    assert g["correlation_id"] == "P"


def test_captured_model_usage_does_not_surface(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_state_db(
        hh,
        sessions=[("P", "cli", None, B, B + 1, 1, None)],
        model_usage=[("P", "claude-x", "chat")],
    )
    ob = new_outbox(tmp_path)
    append_event(ob, "session.created", session_id="P", correlation_id="P")
    append_event(
        ob, "model.usage_recorded",
        session_id="P", correlation_id="P",
        payload={"model": "claude-x", "task": "chat"},
    )

    reconcile(ob, hh, now=B, config=CFG)

    assert coverage_gaps(ob, "model_usage") == []


def test_missing_model_usage_table_is_tolerated(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    db = sqlite3.connect(hh / "state.db")
    db.executescript(
        """
        CREATE TABLE sessions (id TEXT, source TEXT, parent_session_id TEXT,
            started_at REAL, ended_at REAL);
        CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT,
            content TEXT);
        """
    )
    db.execute("INSERT INTO sessions VALUES ('P','cli',NULL,?,NULL)", (B,))
    db.commit(); db.close()
    ob = new_outbox(tmp_path)

    reconcile(ob, hh, now=B, config=CFG)

    assert coverage_gaps(ob, "model_usage") == []


# --- execution coverage ------------------------------------------------------
def test_uncaptured_execution_surfaces_once_as_coverage_gap(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    cron = hh / "cron"
    cron.mkdir()
    # finished_at set, so no terminal-missing interference.
    make_executions_db(cron, [("e1", "j1", "completed", iso(B), iso(B), iso(B + 2))])
    ob = new_outbox(tmp_path)

    reconcile(ob, hh, now=B, config=CFG)

    gaps = coverage_gaps(ob, "execution")
    assert len(gaps) == 1
    g = gaps[0]
    assert g["payload"]["subject_id"] == "e1"
    assert g["payload"]["source_table"] == "cron:executions.db"
    assert g["correlation_id"] == "j1"
    assert g.get("session_id") is None  # execution gaps carry no session_id


def test_new_execution_is_not_a_gap_if_capture_gets_it_on_next_tick(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    cron = hh / "cron"
    cron.mkdir()
    make_executions_db(
        cron, [("e1", "j1", "completed", iso(B), iso(B), iso(B + 2))]
    )
    ob = new_outbox(tmp_path)
    cfg = ReconcileConfig(coverage_grace=30.0)

    first = reconcile(ob, hh, now=B, config=cfg)
    assert "reconcile.gap_detected" not in first

    append_event(
        ob, "cron.run_claimed", correlation_id="j1",
        payload={"execution_id": "e1"},
    )
    second = reconcile(ob, hh, now=B + 60, config=cfg)

    assert "reconcile.gap_detected" not in second
    assert coverage_gaps(ob, "execution") == []


def test_execution_still_absent_after_grace_surfaces(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    cron = hh / "cron"
    cron.mkdir()
    make_executions_db(
        cron, [("e1", "j1", "completed", iso(B), iso(B), iso(B + 2))]
    )
    ob = new_outbox(tmp_path)
    cfg = ReconcileConfig(coverage_grace=30.0)

    reconcile(ob, hh, now=B, config=cfg)
    reconcile(ob, hh, now=B + 31, config=cfg)

    assert len(coverage_gaps(ob, "execution")) == 1


def test_captured_execution_does_not_surface(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    cron = hh / "cron"
    cron.mkdir()
    make_executions_db(cron, [("e1", "j1", "completed", iso(B), iso(B), iso(B + 2))])
    ob = new_outbox(tmp_path)
    append_event(
        ob, "cron.run_claimed", correlation_id="j1",
        payload={"execution_id": "e1"},
    )

    reconcile(ob, hh, now=B, config=CFG)

    assert coverage_gaps(ob, "execution") == []


def test_no_backfill_install_horizon_suppresses_historic_state_rows(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    cron = hh / "cron"
    cron.mkdir()
    make_state_db(
        hh,
        sessions=[("old", "cli", None, B - 100, B - 50, 1, None)],
        messages=[(30, "old", "user", "prompt")],
        model_usage=[("old", "claude-x", "chat")],
    )
    make_executions_db(
        cron,
        [("old-ex", "job1", "completed", iso(B - 100), iso(B - 100), iso(B - 50))],
    )
    ob = new_outbox(tmp_path)
    ob.set_meta(INSTALLED_AT_META_KEY, str(B))

    reconcile(ob, hh, now=B + 60, config=CFG)

    assert coverage_gaps(ob) == []


def test_no_backfill_install_horizon_keeps_new_uncaptured_rows(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    cron = hh / "cron"
    cron.mkdir()
    make_state_db(
        hh,
        sessions=[("new", "cli", None, B + 10, None, 0, None)],
        messages=[(31, "new", "assistant", "response")],
        model_usage=[("new", "claude-x", "chat")],
    )
    make_executions_db(
        cron,
        [("new-ex", "job1", "completed", iso(B + 10), iso(B + 10), iso(B + 12))],
    )
    ob = new_outbox(tmp_path)
    ob.set_meta(INSTALLED_AT_META_KEY, str(B))

    reconcile(ob, hh, now=B + 60, config=CFG)

    surfaced = {(g["payload"]["subject_type"], g["payload"]["subject_id"]) for g in coverage_gaps(ob)}
    assert surfaced == {
        ("session", "new"),
        ("message", "31"),
        ("model_usage", "new:claude-x:chat"),
        ("execution", "new-ex"),
    }


# --- idempotency & combined scenario -----------------------------------------
def test_coverage_gap_is_idempotent_across_reconcile_runs(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_state_db(hh, sessions=[("S1", "cli", None, B, None, 0, None)])
    ob = new_outbox(tmp_path)

    reconcile(ob, hh, now=B, config=CFG)
    assert len(coverage_gaps(ob, "session")) == 1
    n = ob.count()

    reconcile(ob, hh, now=B, config=CFG)
    assert ob.count() == n  # dedup_key stops a second identical finding
    assert len(coverage_gaps(ob, "session")) == 1


def test_mixed_captured_and_uncaptured_rows_across_all_subject_kinds(tmp_path):
    """One row of each kind is captured, one of each kind is not. Exactly the
    uncaptured rows surface, each exactly once, with the right subject_type.
    """
    hh = tmp_path / "hermes"
    hh.mkdir()
    cron = hh / "cron"
    cron.mkdir()
    make_state_db(
        hh,
        sessions=[
            ("cap-sess", "cli", None, B, B + 1, 1, None),
            ("gap-sess", "cli", None, B, None, 0, None),
        ],
        messages=[
            (20, "cap-sess", "tool", "{}"),
            (21, "cap-sess", "tool", "{}"),
        ],
        model_usage=[
            ("cap-sess", "m1", "chat"),
            ("cap-sess", "m1", "code"),
        ],
    )
    make_executions_db(
        cron,
        [
            ("cap-ex", "job1", "completed", iso(B), iso(B), iso(B + 2)),
            ("gap-ex", "job1", "completed", iso(B), iso(B), iso(B + 2)),
        ],
    )
    ob = new_outbox(tmp_path)
    append_event(ob, "session.created", session_id="cap-sess", correlation_id="cap-sess")
    append_event(
        ob, "tool.call_completed", session_id="cap-sess", correlation_id="cap-sess",
        payload={"message_row_id": 20},
    )
    append_event(
        ob, "model.usage_recorded", session_id="cap-sess", correlation_id="cap-sess",
        payload={"model": "m1", "task": "chat"},
    )
    append_event(
        ob, "cron.run_claimed", correlation_id="job1",
        payload={"execution_id": "cap-ex"},
    )

    reconcile(ob, hh, now=B, config=CFG)

    all_gaps = coverage_gaps(ob)
    surfaced = {(g["payload"]["subject_type"], g["payload"]["subject_id"]) for g in all_gaps}
    assert surfaced == {
        ("session", "gap-sess"),
        ("message", "21"),
        ("model_usage", "cap-sess:m1:code"),
        ("execution", "gap-ex"),
    }
    # Exactly one of each kind — no duplicates.
    kinds = [g["payload"]["subject_type"] for g in all_gaps]
    assert sorted(kinds) == ["execution", "message", "model_usage", "session"]
