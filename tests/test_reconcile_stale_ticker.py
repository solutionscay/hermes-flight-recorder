"""Tests for stale-ticker semantics in the reconciler (``_ticker_is_stale``
and its interaction with missed-cron detection).

Self-contained: builds its own outbox, ``cron/jobs.json``,
``cron/executions.db``, and ``cron/ticker_heartbeat`` fixtures directly,
mirroring (but not importing from) ``tests/test_reconcile.py``. Every
call to ``reconcile`` passes an explicit ``now`` and a ``ReconcileConfig``
with small explicit windows, so nothing here depends on wall-clock.

Covered:

- A heartbeat older than ``ticker_stale_after`` emits exactly ONE
  ``reconcile.terminal_missing`` with ``subject_type='cron_ticker'`` for
  the installation, no matter how many jobs exist (not one per job).
- A fresh heartbeat emits no ticker finding.
- When the ticker is stale, a per-job OPEN-ENDED tail miss is suppressed,
  but an INTERNAL gap (bounded by two real executions on both sides) is
  still reported.
- A missing or malformed ``ticker_heartbeat`` file is tolerated: treated
  as not-stale, no crash, and per-job tails are NOT suppressed.
- The ticker finding's ``dedup_key`` is deterministic, so a second pass
  appends nothing new (idempotent).
"""

from __future__ import annotations

import datetime
import json
import sqlite3
from collections import Counter

from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile
from hermes_flight_recorder.envelope import validate

# A fixed epoch anchor and a fixed UTC-5 offset, same convention as
# tests/test_reconcile.py, so timestamps never touch wall-clock.
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


def ticker_findings(outbox):
    return [
        e for e in findings(outbox, "reconcile.terminal_missing")
        if e["payload"]["subject_type"] == "cron_ticker"
    ]


def dedup_keys(outbox):
    return [r[0] for r in outbox._conn.execute("SELECT dedup_key FROM events").fetchall()]


# --- fixture builders -----------------------------------------------------
def _executions_db(cron, rows) -> None:
    """rows: (exec_id, job_id, status, claimed_at_iso, started_at_iso, finished_at_iso)."""
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


def _interval_job(job_id, *, minutes, created, extra=None) -> dict:
    job = {
        "id": job_id,
        "enabled": True,
        "state": "scheduled",
        "created_at": iso(created),
        "schedule": {"kind": "interval", "minutes": minutes},
        "repeat": {"times": None, "completed": 0},
    }
    if extra:
        job.update(extra)
    return job


def _heartbeat(cron, epoch: float) -> None:
    (cron / "ticker_heartbeat").write_text(str(epoch))


# --- one signal, not one per job -------------------------------------------
def test_stale_heartbeat_emits_single_ticker_signal_across_multiple_jobs(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    cron = hh / "cron"; cron.mkdir()
    # Three active jobs, none due yet (so no cron.run_missed noise), all
    # sharing the one installation-wide scheduler.
    jobs = [_interval_job(f"j{i}", minutes=5, created=B) for i in range(3)]
    _jobs_json(cron, jobs)
    _heartbeat(cron, B)  # stale relative to `now` below

    ob = new_outbox(tmp_path)
    cfg = ReconcileConfig(ticker_stale_after=60.0)
    reconcile(ob, hh, now=B + 120, config=cfg)  # staleness = 120 > 60

    ticker = ticker_findings(ob)
    assert len(ticker) == 1
    assert ticker[0]["correlation_id"] == "cron:ticker"
    assert ticker[0]["payload"]["subject_id"] == "cron:ticker"
    for e in ob.iter_events():
        validate(e)


def test_stale_ticker_signal_independent_of_job_activity(tmp_path):
    """The scheduler-health signal fires even when every job is inactive —
    it is about the ticker, not any particular job's schedule."""
    hh = tmp_path / "hermes"; hh.mkdir()
    cron = hh / "cron"; cron.mkdir()
    paused = _interval_job("paused", minutes=1, created=B, extra={"state": "paused", "paused_at": iso(B)})
    exhausted = _interval_job("done", minutes=1, created=B, extra={"repeat": {"times": 1, "completed": 1}})
    _jobs_json(cron, [paused, exhausted])
    _heartbeat(cron, B)

    ob = new_outbox(tmp_path)
    cfg = ReconcileConfig(ticker_stale_after=60.0)
    reconcile(ob, hh, now=B + 120, config=cfg)

    assert len(ticker_findings(ob)) == 1
    assert "cron.run_missed" not in types(ob)


# --- fresh heartbeat --------------------------------------------------------
def test_fresh_heartbeat_emits_no_ticker_signal(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    cron = hh / "cron"; cron.mkdir()
    job = _interval_job("j1", minutes=1, created=B, extra={"state": "paused", "paused_at": iso(B)})
    _jobs_json(cron, [job])
    _heartbeat(cron, B + 55)  # fresh: only 5s stale

    ob = new_outbox(tmp_path)
    cfg = ReconcileConfig(ticker_stale_after=60.0)
    counts = reconcile(ob, hh, now=B + 60, config=cfg)  # staleness = 5 <= 60

    assert ticker_findings(ob) == []
    assert counts.get("reconcile.terminal_missing", 0) == 0


# --- stale ticker: tail suppressed, internal gap still reported ------------
def test_stale_ticker_suppresses_open_ended_tail_but_not_internal_gap(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    cron = hh / "cron"; cron.mkdir()
    # 1-minute job. Real fires at B+60 and B+240 bracket an internal gap at
    # B+120 (both slots missing, collapsed to one run). After B+240 there is
    # no further real fire before `now`, so the B+300 slot is an open-ended
    # tail — that one must be suppressed once the ticker is judged dead.
    _executions_db(cron, [
        ("e1", "j1", "completed", iso(B + 60), iso(B + 60), iso(B + 61)),
        ("e2", "j1", "completed", iso(B + 240), iso(B + 240), iso(B + 241)),
    ])
    _jobs_json(cron, [_interval_job("j1", minutes=1, created=B)])
    _heartbeat(cron, B + 60)  # last heartbeat coincides with the last real fire

    ob = new_outbox(tmp_path)
    cfg = ReconcileConfig(ticker_stale_after=100.0, cron_match_slack=10.0)
    reconcile(ob, hh, now=B + 310, config=cfg)  # staleness = 250 > 100 -> dead

    assert len(ticker_findings(ob)) == 1

    missed = [e for e in findings(ob, "cron.run_missed") if e["correlation_id"] == "j1"]
    fire_ats = {m["payload"]["expected_fire_at"] for m in missed}
    assert B + 120 in fire_ats  # internal gap: still reported
    assert B + 300 not in fire_ats  # open-ended tail: suppressed by dead ticker
    internal = next(m for m in missed if m["payload"]["expected_fire_at"] == B + 120)
    assert internal["payload"]["missed_count"] == 2


def test_fresh_ticker_does_not_suppress_open_ended_tail(tmp_path):
    """Contrast case: with a healthy ticker the same open-ended tail IS
    reported, proving suppression is conditioned on ticker deadness."""
    hh = tmp_path / "hermes"; hh.mkdir()
    cron = hh / "cron"; cron.mkdir()
    _executions_db(cron, [
        ("e1", "j1", "completed", iso(B + 60), iso(B + 60), iso(B + 61)),
        ("e2", "j1", "completed", iso(B + 240), iso(B + 240), iso(B + 241)),
    ])
    _jobs_json(cron, [_interval_job("j1", minutes=1, created=B)])
    _heartbeat(cron, B + 305)  # fresh relative to now

    ob = new_outbox(tmp_path)
    cfg = ReconcileConfig(ticker_stale_after=100.0, cron_match_slack=10.0)
    reconcile(ob, hh, now=B + 310, config=cfg)  # staleness = 5 <= 100 -> alive

    assert ticker_findings(ob) == []
    missed = [e for e in findings(ob, "cron.run_missed") if e["correlation_id"] == "j1"]
    fire_ats = {m["payload"]["expected_fire_at"] for m in missed}
    assert B + 120 in fire_ats
    assert B + 300 in fire_ats  # tail reported: ticker is alive


# --- missing / malformed heartbeat file tolerated ---------------------------
def test_missing_heartbeat_file_treated_as_not_stale(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    cron = hh / "cron"; cron.mkdir()
    _executions_db(cron, [])  # nothing ever fired
    _jobs_json(cron, [_interval_job("j1", minutes=5, created=B)])
    # No ticker_heartbeat file written at all.

    ob = new_outbox(tmp_path)
    cfg = ReconcileConfig(ticker_stale_after=60.0)
    counts = reconcile(ob, hh, now=B + 900, config=cfg)  # no crash

    assert ticker_findings(ob) == []
    assert counts.get("reconcile.terminal_missing", 0) == 0
    # Not-stale means the tail is NOT suppressed.
    missed = [e for e in findings(ob, "cron.run_missed") if e["correlation_id"] == "j1"]
    assert len(missed) == 1


def test_malformed_heartbeat_treated_as_not_stale(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    cron = hh / "cron"; cron.mkdir()
    _executions_db(cron, [])
    _jobs_json(cron, [_interval_job("j1", minutes=5, created=B)])
    (cron / "ticker_heartbeat").write_text("not-a-number\n")

    ob = new_outbox(tmp_path)
    cfg = ReconcileConfig(ticker_stale_after=60.0)
    counts = reconcile(ob, hh, now=B + 900, config=cfg)  # tolerated, no crash

    assert ticker_findings(ob) == []
    assert counts.get("reconcile.terminal_missing", 0) == 0
    missed = [e for e in findings(ob, "cron.run_missed") if e["correlation_id"] == "j1"]
    assert len(missed) == 1  # tail not suppressed: malformed heartbeat != stale


def test_empty_heartbeat_file_treated_as_not_stale(tmp_path):
    """An empty (zero-byte / whitespace-only) heartbeat file must not be
    parsed as 0.0 and must not crash the parse."""
    hh = tmp_path / "hermes"; hh.mkdir()
    cron = hh / "cron"; cron.mkdir()
    _executions_db(cron, [])
    _jobs_json(cron, [_interval_job("j1", minutes=5, created=B)])
    (cron / "ticker_heartbeat").write_text("   \n")

    ob = new_outbox(tmp_path)
    cfg = ReconcileConfig(ticker_stale_after=60.0)
    counts = reconcile(ob, hh, now=B + 900, config=cfg)

    assert ticker_findings(ob) == []
    assert counts.get("reconcile.terminal_missing", 0) == 0


# --- dedup / idempotency -----------------------------------------------------
def test_ticker_dedup_key_is_deterministic_and_idempotent(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    cron = hh / "cron"; cron.mkdir()
    _jobs_json(cron, [_interval_job("j1", minutes=5, created=B)])
    hb = B + 10
    _heartbeat(cron, hb)

    ob = new_outbox(tmp_path)
    cfg = ReconcileConfig(ticker_stale_after=60.0)

    first_counts = reconcile(ob, hh, now=B + 500, config=cfg)  # staleness 490 > 60
    assert first_counts.get("reconcile.terminal_missing", 0) == 1

    keys = dedup_keys(ob)
    expected_key = f"reconcile:ticker_stale:{int(hb)}"
    assert keys.count(expected_key) == 1

    n = ob.count()
    second_counts = reconcile(ob, hh, now=B + 500, config=cfg)  # same pass again
    assert ob.count() == n  # no new rows appended
    assert second_counts.get("reconcile.terminal_missing", 0) == 0
    assert dedup_keys(ob).count(expected_key) == 1  # still exactly one
    assert len(ticker_findings(ob)) == 1  # still exactly one finding, not two
