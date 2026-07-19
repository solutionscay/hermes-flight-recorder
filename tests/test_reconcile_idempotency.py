"""Idempotency + envelope-validity tests for the reconciler (issue #6).

Self-contained: defines its own fixtures/helpers (does not import from
tests/test_reconcile.py). Mirrors that file's style (fixed epoch anchor,
iso() with a fixed tz offset, new_outbox(tmp_path)) but is independent.

Focus:
  - Every reconcile.* / cron.run_missed finding validates against
    envelope.validate.
  - A second reconcile pass with ``now`` advanced (+300s) appends ZERO new
    rows and returns {} --- dedup keys must be stable across a changing
    ``now``, especially cron.run_missed (keyed by int(expected_fire_at))
    and terminal_missing (keyed by subject_id).
  - Findings carry source='reconciler' and capture_method='derive:reconciler'.
"""

from __future__ import annotations

import datetime
import json
import sqlite3

from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile
from hermes_flight_recorder.collector._common import build_record
from hermes_flight_recorder.envelope import validate

# A fixed epoch anchor and a fixed tz offset, like the real cron store.
B = 1784415000.0
TZ = datetime.timezone(datetime.timedelta(hours=-5))

# Small explicit thresholds so every detector fires deterministically and
# stays fired across a +300s "now" advance (see module docstring above).
CFG = ReconcileConfig(
    session_terminal_timeout=100.0,
    subagent_terminal_timeout=100.0,
    invocation_terminal_timeout=100.0,
    cron_run_terminal_timeout=100.0,
    ticker_stale_after=100.0,
    cron_match_slack=45.0,
    once_match_slack=300.0,
    cron_lookback=24 * 3600.0,
)

_RECONCILER_TYPES = {"reconcile.gap_detected", "reconcile.terminal_missing", "cron.run_missed"}


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


def findings(ob, event_type):
    return [
        e for e in ob.iter_events()
        if e["payload"]["event_type"] == event_type and e["source"] == "reconciler"
    ]


def dedup_keys(ob, like: str) -> list[str]:
    """Read the dedup_key column directly (it is not part of the envelope)."""
    rows = ob._conn.execute(
        "SELECT dedup_key FROM events WHERE dedup_key LIKE ? "
        "ORDER BY producer_sequence",
        (like,),
    ).fetchall()
    return [r[0] for r in rows]


# --- durable-store fixtures ----------------------------------------------
def _state_db(hh, sessions) -> None:
    db = sqlite3.connect(hh / "state.db")
    db.executescript(
        """
        CREATE TABLE sessions (id TEXT, source TEXT, parent_session_id TEXT,
            started_at REAL, ended_at REAL, expiry_finalized INT, profile_name TEXT);
        CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT);
        CREATE TABLE session_model_usage (session_id TEXT, model TEXT, task TEXT);
        """
    )
    for sid, parent, started, ended in sessions:
        db.execute(
            "INSERT INTO sessions VALUES (?,'cli',?,?,?,0,NULL)",
            (sid, parent, started, ended),
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


def _build_scenario(tmp_path):
    """One store that triggers every reconcile.* / cron.run_missed finding.

    - S1: an open session (ended_at NULL) with no captured session.created
      event -> both reconcile.gap_detected(uncaptured_row) AND, once past
      its window, reconcile.terminal_missing(session).
    - INV1: invocation.started with no invocation.completed -> terminal_missing(invocation).
    - ex_open: a cron execution with finished_at NULL -> terminal_missing(cron_run).
    - j1: an interval job that fired at B+60 and B+240 but missed the B+120
      slot -> cron.run_missed (missed_count=2, not a tail: e2 closes the run).
    - A ticker heartbeat already stale at B+250 -> terminal_missing(cron_ticker),
      and it stays stale (same heartbeat value) at B+550 too, so any tail
      that opens up as 'now' advances is suppressed both times.
    - Five outbox events with producer_sequence 3 deleted -> reconcile.gap_detected(sequence).

    Note: the three executions (ex_open, e1, e2) also each surface as
    reconcile.gap_detected(uncaptured_row) because nothing captured them.
    That is incidental; tests only assert the properties they name.
    """
    hh = tmp_path / "hermes"; hh.mkdir()
    cron = hh / "cron"; cron.mkdir()

    _state_db(hh, sessions=[("S1", None, B, None)])
    _executions_db(cron, [
        ("ex_open", "jX", "running", iso(B), iso(B), None),
        ("e1", "j1", "completed", iso(B + 60), iso(B + 60), iso(B + 61)),
        ("e2", "j1", "completed", iso(B + 240), iso(B + 240), iso(B + 241)),
    ])
    _jobs_json(cron, [_interval_job("j1", minutes=1, created=B)])
    (cron / "ticker_heartbeat").write_text(str(B + 100))

    ob = new_outbox(tmp_path)
    for _ in range(5):
        append_event(ob, "session.created")  # seq 1..5, no session_id -> no coverage
    ob._conn.execute("DELETE FROM events WHERE producer_sequence=3")
    append_event(
        ob, "invocation.started",
        occurred_at=B, invocation_id="INV1", session_id="S1", correlation_id="S1",
    )  # seq 6
    return ob, hh


# --- one-of-each-finding-type scenario ------------------------------------
def test_full_scenario_covers_every_finding_type(tmp_path):
    ob, hh = _build_scenario(tmp_path)

    counts = reconcile(ob, hh, now=B + 250, config=CFG)

    assert counts.get("reconcile.gap_detected", 0) >= 2
    assert counts.get("reconcile.terminal_missing", 0) >= 4
    assert counts.get("cron.run_missed", 0) == 1

    gap_kinds = {e["payload"]["gap_kind"] for e in findings(ob, "reconcile.gap_detected")}
    assert gap_kinds == {"sequence", "uncaptured_row"}

    term_subjects = {e["payload"]["subject_type"] for e in findings(ob, "reconcile.terminal_missing")}
    assert term_subjects == {"session", "invocation", "cron_run", "cron_ticker"}

    missed = findings(ob, "cron.run_missed")
    assert len(missed) == 1
    assert missed[0]["payload"]["expected_fire_at"] == B + 120
    assert missed[0]["payload"]["missed_count"] == 2
    assert missed[0]["correlation_id"] == "j1"


def test_every_finding_validates_against_envelope(tmp_path):
    ob, hh = _build_scenario(tmp_path)
    counts = reconcile(ob, hh, now=B + 250, config=CFG)
    assert sum(counts.values()) > 0

    n_checked = 0
    for e in ob.iter_events():
        validate(e)  # raises EnvelopeValidationError on any defect
        n_checked += 1
    assert n_checked == ob.count()


def test_findings_carry_reconciler_source_and_capture_method(tmp_path):
    ob, hh = _build_scenario(tmp_path)
    reconcile(ob, hh, now=B + 250, config=CFG)

    seen_reconciler_type = False
    for e in ob.iter_events():
        if e["payload"]["event_type"] in _RECONCILER_TYPES:
            seen_reconciler_type = True
            assert e["source"] == "reconciler"
            assert e["capture_method"] == "derive:reconciler"
        else:
            # the hand-appended hook events must NOT look like reconciler output
            assert e["source"] != "reconciler"
    assert seen_reconciler_type


# --- the idempotency proof itself -----------------------------------------
def test_second_pass_with_advanced_now_is_full_no_op(tmp_path):
    ob, hh = _build_scenario(tmp_path)

    first = reconcile(ob, hh, now=B + 250, config=CFG)
    assert sum(first.values()) > 0  # the first pass did find something
    n = ob.count()

    second = reconcile(ob, hh, now=B + 550, config=CFG)  # +300s later

    assert second == {}
    assert ob.count() == n  # no new rows appended

    for e in ob.iter_events():
        validate(e)


def test_missing_stores_still_noop_across_advancing_now(tmp_path):
    """No durable stores at all: nothing to diff, both passes are no-ops."""
    hh = tmp_path / "hermes"; hh.mkdir()  # no state.db, no cron dir
    ob = new_outbox(tmp_path)
    append_event(ob, "session.created")

    assert reconcile(ob, hh, now=B, config=CFG) == {}
    n = ob.count()
    assert reconcile(ob, hh, now=B + 300, config=CFG) == {}
    assert ob.count() == n


# --- dedup-key stability, examined directly --------------------------------
def test_cron_run_missed_dedup_key_is_job_and_int_expected_fire_at(tmp_path):
    ob, hh = _build_scenario(tmp_path)
    reconcile(ob, hh, now=B + 250, config=CFG)

    keys = dedup_keys(ob, "reconcile:missed:%")
    assert keys == [f"reconcile:missed:j1:{int(B + 120)}"]

    # 'now' advances by 300s: the same closed run must dedup to the SAME
    # key, and any newly-open tail must be suppressed by the (still-stale)
    # ticker heartbeat rather than minted under a new key.
    reconcile(ob, hh, now=B + 550, config=CFG)
    keys_after = dedup_keys(ob, "reconcile:missed:%")
    assert keys_after == keys


def test_terminal_missing_dedup_key_is_subject_id_and_stable(tmp_path):
    ob, hh = _build_scenario(tmp_path)
    reconcile(ob, hh, now=B + 250, config=CFG)

    session_keys = dedup_keys(ob, "reconcile:terminal:session:%")
    assert session_keys == ["reconcile:terminal:session:S1"]
    invocation_keys = dedup_keys(ob, "reconcile:terminal:invocation:%")
    assert invocation_keys == ["reconcile:terminal:invocation:INV1"]

    reconcile(ob, hh, now=B + 550, config=CFG)  # age grows, subject_id doesn't

    assert dedup_keys(ob, "reconcile:terminal:session:%") == session_keys
    assert dedup_keys(ob, "reconcile:terminal:invocation:%") == invocation_keys


def test_sequence_gap_dedup_key_stable_across_now(tmp_path):
    ob, hh = _build_scenario(tmp_path)
    reconcile(ob, hh, now=B + 250, config=CFG)

    keys = dedup_keys(ob, "reconcile:seq:%")
    assert len(keys) == 1
    assert keys[0].endswith(":3")  # missing_sequence == 3

    reconcile(ob, hh, now=B + 550, config=CFG)
    assert dedup_keys(ob, "reconcile:seq:%") == keys


def test_coverage_gap_dedup_key_stable_across_now(tmp_path):
    ob, hh = _build_scenario(tmp_path)
    reconcile(ob, hh, now=B + 250, config=CFG)

    keys = dedup_keys(ob, "reconcile:cover:session:%")
    assert keys == ["reconcile:cover:session:S1"]

    reconcile(ob, hh, now=B + 550, config=CFG)
    assert dedup_keys(ob, "reconcile:cover:session:%") == keys
