"""Tests for the reconciler (issue #6).

Cover the four detectors — sequence gap, coverage gap, missing terminal,
missed cron — plus the stale-ticker signal and idempotency. Fixtures use a
fixed ``now`` and small windows so wall-clock never enters.
"""

from __future__ import annotations

from collections import Counter

from helpers import (
    ASYNC_DELEGATIONS_DDL,
    B,
    MESSAGES_FULL,
    SESSIONS_FULL,
    USAGE_FULL,
    _executions_db,
    _interval_job,
    _jobs_json,
    append_event,
    iso,
    make_state_db,
    new_outbox,
)

from hermes_flight_recorder.collector import state_db
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile
from hermes_flight_recorder.envelope import validate


def types(outbox) -> Counter:
    return Counter(e["payload"]["event_type"] for e in outbox.iter_events())


def findings(outbox, event_type):
    return [
        e for e in outbox.iter_events()
        if e["payload"]["event_type"] == event_type and e["source"] == "reconciler"
    ]


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
    # started_at = now, so the open session is NOT terminal-missing yet.
    make_state_db(hh, sessions=[("S", "cli", None, B, None, 0, None)])
    ob = new_outbox(tmp_path)

    reconcile(ob, hh, now=B, config=ReconcileConfig(coverage_grace=0.0))

    cover = [
        e for e in findings(ob, "reconcile.gap_detected")
        if e["payload"]["gap_kind"] == "uncaptured_row"
    ]
    assert len(cover) == 1
    assert cover[0]["payload"]["subject_type"] == "session"
    assert cover[0]["payload"]["subject_id"] == "S"
    assert types(ob)["reconcile.terminal_missing"] == 0  # sessions are not terminal-judged


def test_polled_rows_are_not_flagged_as_uncaptured(tmp_path):
    """After a poll captures the rows, coverage detection finds nothing."""
    hh = tmp_path / "hermes"; hh.mkdir()
    _full_state_db(hh)
    ob = new_outbox(tmp_path)
    state_db.poll(ob, hh)  # capture everything first

    counts = reconcile(ob, hh, now=B)
    assert "reconcile.gap_detected" not in counts


# --- missing terminals --------------------------------------------------
# Only invocations are terminal-judged. Session/subagent/cron-run terminal
# detection was removed: it trusted the durable ended_at/finished_at column,
# which disagrees with the captured terminal (issues #94, #95).
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
    # A parent that ended (so no terminal-missing) plus one tool message and usage.
    make_state_db(
        hh,
        sessions=[("P", "cli", None, "m", 1, 1, 10, 1, 0.0, B, B + 10, "done", "default", 1)],
        messages=[(5, "P", "tool", "read", "tc", None, '{"exit_code":0}', B + 2, None)],
        model_usage=[("P", "m", "", 1, 10, 1, 0, 0, 0.0, "estimated", B + 5)],
        sessions_columns=SESSIONS_FULL,
        messages_columns=MESSAGES_FULL,
        usage_columns=USAGE_FULL,
        extra_ddl=ASYNC_DELEGATIONS_DDL,
    )
