"""Tests for the reconciler (issue #6).

Cover the four detectors — sequence gap, coverage gap, missing terminal,
missed cron — plus the stale-ticker signal and idempotency. Fixtures use a
fixed ``now`` and small windows so wall-clock never enters.
"""

from __future__ import annotations

import datetime
import json
import sqlite3
from collections import Counter

from hermes_flight_recorder.collector import state_db
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile
from hermes_flight_recorder.collector._common import build_record
from hermes_flight_recorder.envelope import validate

# A fixed epoch anchor and a US-Central offset like the real cron store.
B = 1784415000.0
TZ = datetime.timezone(datetime.timedelta(hours=-5))


def iso(epoch: float) -> str:
    return datetime.datetime.fromtimestamp(epoch, TZ).isoformat()


def new_outbox(tmp_path) -> Outbox:
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


def types(outbox) -> Counter:
    return Counter(e["payload"]["event_type"] for e in outbox.iter_events())


def findings(outbox, event_type):
    return [
        e for e in outbox.iter_events()
        if e["payload"]["event_type"] == event_type and e["source"] == "reconciler"
    ]


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


# --- sequence gaps ------------------------------------------------------
def test_dropped_sequence_surfaces_as_gap_detected(tmp_path):
    ob = new_outbox(tmp_path)
    for _ in range(5):
        append_event(ob, "session.created")
    # Simulate a dropped capture: remove sequence 3 from the store.
    ob._conn.execute("DELETE FROM events WHERE producer_sequence=3")

    reconcile(ob, tmp_path / "hermes-missing", now=B)  # no hermes home -> gaps only

    gaps = [
        e for e in findings(ob, "reconcile.gap_detected")
        if e["payload"]["gap_kind"] == "sequence"
    ]
    assert len(gaps) == 1
    g = gaps[0]
    assert g["payload"]["missing_sequence"] == 3
    assert g["payload"]["prev_sequence"] == 2 and g["payload"]["next_sequence"] == 4
    assert g["partial"] is False  # a lost sequence is a fact
    for e in ob.iter_events():
        validate(e)


# --- coverage gaps ------------------------------------------------------
def test_durable_row_with_no_captured_event_is_uncaptured(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    db = sqlite3.connect(hh / "state.db")
    db.executescript(
        """
        CREATE TABLE sessions (id TEXT, source TEXT, parent_session_id TEXT,
            started_at REAL, ended_at REAL, expiry_finalized INT, profile_name TEXT);
        CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT);
        CREATE TABLE session_model_usage (session_id TEXT, model TEXT, task TEXT);
        """
    )
    # started_at = now, so the open session is NOT terminal-missing yet.
    db.execute("INSERT INTO sessions VALUES ('S','cli',NULL,?,NULL,0,NULL)", (B,))
    db.commit(); db.close()
    ob = new_outbox(tmp_path)

    reconcile(ob, hh, now=B)

    cover = [
        e for e in findings(ob, "reconcile.gap_detected")
        if e["payload"]["gap_kind"] == "uncaptured_row"
    ]
    assert len(cover) == 1
    assert cover[0]["payload"]["subject_type"] == "session"
    assert cover[0]["payload"]["subject_id"] == "S"
    assert types(ob)["reconcile.terminal_missing"] == 0  # within lifetime


def test_polled_rows_are_not_flagged_as_uncaptured(tmp_path):
    """After a poll captures the rows, coverage detection finds nothing."""
    hh = tmp_path / "hermes"; hh.mkdir()
    _full_state_db(hh)
    ob = new_outbox(tmp_path)
    state_db.poll(ob, hh)  # capture everything first

    counts = reconcile(ob, hh, now=B)
    assert "reconcile.gap_detected" not in counts


# --- missing terminals --------------------------------------------------
def test_open_session_past_timeout_is_terminal_missing(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    db = sqlite3.connect(hh / "state.db")
    db.executescript(
        """
        CREATE TABLE sessions (id TEXT, source TEXT, parent_session_id TEXT,
            started_at REAL, ended_at REAL, expiry_finalized INT, profile_name TEXT);
        CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT);
        CREATE TABLE session_model_usage (session_id TEXT, model TEXT, task TEXT);
        """
    )
    db.execute("INSERT INTO sessions VALUES ('S','cli',NULL,?,NULL,0,NULL)", (B,))
    db.commit(); db.close()
    ob = new_outbox(tmp_path)

    cfg = ReconcileConfig(session_terminal_timeout=100.0)
    reconcile(ob, hh, now=B + 500, config=cfg)  # 500s > 100s window

    term = findings(ob, "reconcile.terminal_missing")
    assert len(term) == 1
    assert term[0]["payload"]["subject_type"] == "session"
    assert term[0]["payload"]["subject_id"] == "S"
    assert term[0]["session_id"] == "S"
    assert term[0]["partial"] is True


def test_invocation_started_without_completed_is_terminal_missing(tmp_path):
    ob = new_outbox(tmp_path)
    append_event(
        ob, "invocation.started",
        occurred_at=B, invocation_id="S:turn:3", session_id="S", correlation_id="S",
    )
    # A different invocation that DID complete — must not be flagged.
    append_event(ob, "invocation.started", occurred_at=B, invocation_id="S:turn:4", correlation_id="S")
    append_event(ob, "invocation.completed", occurred_at=B + 1, invocation_id="S:turn:4", correlation_id="S")

    cfg = ReconcileConfig(invocation_terminal_timeout=100.0)
    reconcile(ob, tmp_path / "no-hermes", now=B + 500, config=cfg)

    term = findings(ob, "reconcile.terminal_missing")
    assert len(term) == 1
    assert term[0]["payload"]["subject_type"] == "invocation"
    assert term[0]["payload"]["subject_id"] == "S:turn:3"
    assert term[0]["invocation_id"] == "S:turn:3"


def test_unfinished_cron_execution_is_terminal_missing(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    cron = hh / "cron"; cron.mkdir()
    _executions_db(cron, [("e1", "j1", "running", iso(B), None, None)])
    ob = new_outbox(tmp_path)

    cfg = ReconcileConfig(cron_run_terminal_timeout=100.0)
    reconcile(ob, hh, now=B + 500, config=cfg)

    term = [e for e in findings(ob, "reconcile.terminal_missing")
            if e["payload"]["subject_type"] == "cron_run"]
    assert len(term) == 1
    assert term[0]["payload"]["subject_id"] == "e1"


# --- missed cron --------------------------------------------------------
def test_missed_interval_fire_surfaces_as_run_missed(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    cron = hh / "cron"; cron.mkdir()
    # A 1-minute job. Fired at B+60 and B+240 — the B+120 and B+180 slots
    # are missing. Fresh heartbeat so per-job detection runs.
    _executions_db(cron, [
        ("e1", "j1", "completed", iso(B + 60), iso(B + 60), iso(B + 61)),
        ("e2", "j1", "completed", iso(B + 240), iso(B + 240), iso(B + 241)),
    ])
    _jobs_json(cron, [_interval_job("j1", minutes=1, created=B)])
    (cron / "ticker_heartbeat").write_text(str(B + 250))
    ob = new_outbox(tmp_path)

    reconcile(ob, hh, now=B + 250)

    missed = findings(ob, "cron.run_missed")
    assert len(missed) == 1
    assert missed[0]["payload"]["expected_fire_at"] == B + 120
    assert missed[0]["payload"]["missed_count"] == 2  # both slots collapsed to one row
    assert missed[0]["correlation_id"] == "j1"


def test_paused_and_exhausted_jobs_give_no_false_positive(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    cron = hh / "cron"; cron.mkdir()
    _executions_db(cron, [])  # nothing ever fired
    paused = _interval_job("paused", minutes=1, created=B)
    paused["state"] = "paused"; paused["paused_at"] = iso(B)
    exhausted = _interval_job("done", minutes=1, created=B)
    exhausted["repeat"] = {"times": 2, "completed": 2}
    _jobs_json(cron, [paused, exhausted])
    (cron / "ticker_heartbeat").write_text(str(B + 600))
    ob = new_outbox(tmp_path)

    counts = reconcile(ob, hh, now=B + 600)
    assert "cron.run_missed" not in counts


def test_stale_ticker_is_one_signal_and_suppresses_per_job_tail(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    cron = hh / "cron"; cron.mkdir()
    _executions_db(cron, [("e1", "j1", "completed", iso(B + 60), iso(B + 60), iso(B + 61))])
    _jobs_json(cron, [_interval_job("j1", minutes=1, created=B)])
    # Heartbeat is ~1h stale relative to now.
    (cron / "ticker_heartbeat").write_text(str(B + 100))
    ob = new_outbox(tmp_path)

    reconcile(ob, hh, now=B + 3700)  # ~1h later

    ticker = [e for e in findings(ob, "reconcile.terminal_missing")
              if e["payload"]["subject_type"] == "cron_ticker"]
    assert len(ticker) == 1  # one installation-wide signal
    # The dead-ticker tail is not re-reported per job.
    assert "cron.run_missed" not in reconcile(ob, hh, now=B + 3700)


def test_missed_once_job(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    cron = hh / "cron"; cron.mkdir()
    _executions_db(cron, [])
    job = {
        "id": "once1", "enabled": True, "state": "scheduled",
        "created_at": iso(B), "schedule": {"kind": "once", "run_at": iso(B + 100)},
    }
    _jobs_json(cron, [job])
    (cron / "ticker_heartbeat").write_text(str(B + 600))
    ob = new_outbox(tmp_path)

    reconcile(ob, hh, now=B + 600)
    missed = findings(ob, "cron.run_missed")
    assert len(missed) == 1
    assert missed[0]["payload"]["expected_fire_at"] == B + 100


# --- idempotency & robustness -------------------------------------------
def test_reconcile_is_idempotent(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    _full_state_db(hh)
    cron = hh / "cron"; cron.mkdir()
    _executions_db(cron, [
        ("e1", "j1", "completed", iso(B + 60), iso(B + 60), iso(B + 61)),
        ("e2", "j1", "completed", iso(B + 240), iso(B + 240), iso(B + 241)),
    ])
    _jobs_json(cron, [_interval_job("j1", minutes=1, created=B)])
    (cron / "ticker_heartbeat").write_text(str(B + 250))
    ob = new_outbox(tmp_path)

    first = reconcile(ob, hh, now=B + 250)
    n = ob.count()
    second = reconcile(ob, hh, now=B + 250)
    assert ob.count() == n  # no new rows on the second pass
    assert second == {}  # nothing new
    assert sum(first.values()) > 0  # the first pass did find something
    for e in ob.iter_events():
        validate(e)


def test_missing_stores_are_tolerated(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()  # no state.db, no cron dir
    ob = new_outbox(tmp_path)
    append_event(ob, "session.created")
    assert reconcile(ob, hh, now=B) == {}  # nothing to diff, no crash


def test_cron_expression_missed_fire(tmp_path):
    """A '*/1 * * * *' job behaves like a 1-minute interval via the parser."""
    hh = tmp_path / "hermes"; hh.mkdir()
    cron = hh / "cron"; cron.mkdir()
    # No executions at all across a 3-minute window -> collapsed miss run.
    _executions_db(cron, [])
    job = {
        "id": "c1", "enabled": True, "state": "scheduled", "created_at": iso(B),
        "schedule": {"kind": "cron", "expression": "*/1 * * * *"},
    }
    _jobs_json(cron, [job])
    (cron / "ticker_heartbeat").write_text(str(B + 180))
    ob = new_outbox(tmp_path)

    reconcile(ob, hh, now=B + 180)
    missed = findings(ob, "cron.run_missed")
    assert len(missed) == 1
    assert missed[0]["payload"]["missed_count"] >= 2


# --- fixtures -----------------------------------------------------------
def _full_state_db(hh) -> None:
    db = sqlite3.connect(hh / "state.db")
    db.executescript(
        """
        CREATE TABLE sessions (id TEXT, source TEXT, parent_session_id TEXT, model TEXT,
            message_count INT, tool_call_count INT, input_tokens INT, output_tokens INT,
            estimated_cost_usd REAL, started_at REAL, ended_at REAL, end_reason TEXT,
            profile_name TEXT, expiry_finalized INT);
        CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT,
            tool_name TEXT, tool_call_id TEXT, effect_disposition TEXT, content TEXT, timestamp REAL);
        CREATE TABLE session_model_usage (session_id TEXT, model TEXT, task TEXT,
            api_call_count INT, input_tokens INT, output_tokens INT, cache_read_tokens INT,
            reasoning_tokens INT, estimated_cost_usd REAL, cost_status TEXT, last_seen REAL);
        CREATE TABLE async_delegations (delegation_id TEXT, origin_session TEXT,
            parent_session_id TEXT, state TEXT, delivery_state TEXT,
            owner_pid INT, dispatched_at REAL, event_json TEXT, result_json TEXT);
        """
    )
    # A parent that ended (so no terminal-missing) plus one tool message and usage.
    db.execute(
        "INSERT INTO sessions VALUES ('P','cli',NULL,'m',1,1,10,1,0.0,?,?,'done','default',1)",
        (B, B + 10),
    )
    db.execute(
        "INSERT INTO messages VALUES (5,'P','tool','read','tc',NULL,'{\"exit_code\":0}',?)",
        (B + 2,),
    )
    db.execute(
        "INSERT INTO session_model_usage VALUES ('P','m','',1,10,1,0,0,0.0,'estimated',?)",
        (B + 5,),
    )
    db.commit(); db.close()


def _executions_db(cron, rows) -> None:
    db = sqlite3.connect(cron / "executions.db")
    db.execute(
        "CREATE TABLE executions (id TEXT, job_id TEXT, source TEXT, pid INT, status TEXT, "
        "claimed_at TEXT, started_at TEXT, finished_at TEXT, error TEXT)"
    )
    db.executemany(
        "INSERT INTO executions VALUES (?,?,'builtin',1,?,?,?,?,NULL)",
        [(exid, job, status, claimed, started, finished)
         for (exid, job, status, claimed, started, finished) in rows],
    )
    db.commit(); db.close()


def _jobs_json(cron, jobs) -> None:
    (cron / "jobs.json").write_text(json.dumps({"jobs": jobs}))


def _interval_job(job_id, *, minutes, created) -> dict:
    return {
        "id": job_id,
        "enabled": True,
        "state": "scheduled",
        "created_at": iso(created),
        "schedule": {"kind": "interval", "minutes": minutes},
        "repeat": {"times": None, "completed": 0},
    }
