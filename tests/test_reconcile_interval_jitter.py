"""Interval missed-cron robustness (``_interval_missed``), exercised only
through the public ``reconcile()`` entry point.

Focus: a realistic ticker that fires an "every minute" job ~78s apart must
never read as a missed run (detection re-anchors on each real fire, so
jitter never accumulates into a false gap); an internal gap bounded by two
real executions collapses to one non-tail row that a stale ticker must NOT
suppress (only an open-ended tail is a ticker-explained artifact); a
never-fired job is judged from its first due instant; and the startup gap
before a job's very first fire must never itself read as a miss.

Self-contained: no imports from tests/test_reconcile.py. Mirrors its style
(fixed epoch anchor, iso() helper, new_outbox()) but defines everything
locally.
"""

from __future__ import annotations

import datetime
import json
import sqlite3

from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile

# A fixed epoch anchor and a US-Central-like offset, same convention as the
# reconciler's own test suite.
B = 1784415000.0
TZ = datetime.timezone(datetime.timedelta(hours=-5))


def iso(epoch: float) -> str:
    return datetime.datetime.fromtimestamp(epoch, TZ).isoformat()


def new_outbox(tmp_path) -> Outbox:
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


def findings(outbox, event_type):
    return [
        e for e in outbox.iter_events()
        if e["payload"]["event_type"] == event_type and e["source"] == "reconciler"
    ]


def _executions_db(cron, rows) -> None:
    """rows: list of (id, job_id, status, claimed_at_iso, started_at_iso, finished_at_iso)."""
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


def _heartbeat(cron, epoch: float) -> None:
    (cron / "ticker_heartbeat").write_text(str(epoch))


def _exec_row(idx: int, job_id: str, at: float) -> tuple:
    """A completed execution claimed (and finished a second later) at `at`."""
    return (f"e{idx}", job_id, "completed", iso(at), iso(at), iso(at + 1))


def _setup(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    cron = hh / "cron"; cron.mkdir()
    return hh, cron


# --- realistic ticker jitter: zero false misses -------------------------
def test_realistic_ticker_jitter_yields_zero_false_misses(tmp_path):
    """A '1m' job whose ticker actually fires ~78s apart (not a clean 60s
    cadence) must not be flagged as missing anything: each real fire
    re-anchors the expected-next instant, so jitter never accumulates.
    """
    hh, cron = _setup(tmp_path)
    offsets = [78, 156, 234, 312, 390]  # ~78s apart, drifting off a clean 60s grid
    execs = [B + o for o in offsets]
    _executions_db(cron, [_exec_row(i, "j1", t) for i, t in enumerate(execs)])
    _jobs_json(cron, [_interval_job("j1", minutes=1, created=B)])
    now = B + 400  # shortly after the last real fire, before the next is due
    _heartbeat(cron, now)
    ob = new_outbox(tmp_path)

    cfg = ReconcileConfig(cron_match_slack=45.0, ticker_stale_after=300.0)
    counts = reconcile(ob, hh, now=now, config=cfg)

    assert "cron.run_missed" not in counts
    assert findings(ob, "cron.run_missed") == []


# --- internal gap: collapsed row, immune to stale ticker -----------------
def test_internal_gap_collapses_and_is_not_suppressed_by_stale_ticker(tmp_path):
    """A gap bounded by two real executions (not open-ended) is a fact, not
    an artifact the dead scheduler could explain — a stale ticker suppresses
    only the trailing (is_tail) catch-up, never a bounded internal miss.
    """
    hh, cron = _setup(tmp_path)
    execs = [B + 60, B + 240]  # B+120 and B+180 slots are missing, in between
    _executions_db(cron, [_exec_row(1, "j1", execs[0]), _exec_row(2, "j1", execs[1])])
    _jobs_json(cron, [_interval_job("j1", minutes=1, created=B)])
    now = B + 250
    # Heartbeat is old enough to be stale under this test's own threshold.
    _heartbeat(cron, B)
    ob = new_outbox(tmp_path)

    cfg = ReconcileConfig(cron_match_slack=45.0, ticker_stale_after=100.0)
    reconcile(ob, hh, now=now, config=cfg)

    ticker = [e for e in findings(ob, "reconcile.terminal_missing")
              if e["payload"]["subject_type"] == "cron_ticker"]
    assert len(ticker) == 1  # the ticker is indeed judged stale this pass

    missed = findings(ob, "cron.run_missed")
    assert len(missed) == 1
    m = missed[0]
    assert m["payload"]["expected_fire_at"] == B + 120
    assert m["payload"]["missed_count"] == 2
    assert m["correlation_id"] == "j1"


# --- open-ended trailing gap: reported only while the ticker is alive ----
def test_trailing_gap_reported_when_ticker_is_fresh(tmp_path):
    """An open-ended gap to `now` (is_tail=True) is a normal miss when the
    ticker is alive — the scheduler had no excuse not to fire.
    """
    hh, cron = _setup(tmp_path)
    _executions_db(cron, [_exec_row(1, "j1", B + 60)])  # one fire, then silence
    _jobs_json(cron, [_interval_job("j1", minutes=1, created=B)])
    now = B + 600
    _heartbeat(cron, now)  # fresh: staleness == 0
    ob = new_outbox(tmp_path)

    cfg = ReconcileConfig(cron_match_slack=45.0, ticker_stale_after=300.0)
    reconcile(ob, hh, now=now, config=cfg)

    ticker = [e for e in findings(ob, "reconcile.terminal_missing")
              if e["payload"]["subject_type"] == "cron_ticker"]
    assert ticker == []  # ticker is alive, no installation-wide signal

    missed = findings(ob, "cron.run_missed")
    assert len(missed) == 1
    m = missed[0]
    assert m["payload"]["expected_fire_at"] == B + 120
    assert m["payload"]["missed_count"] == 9
    assert m["payload"]["catch_up"] is True


def test_trailing_gap_suppressed_when_ticker_is_stale(tmp_path):
    """The exact same open-ended gap, but the ticker is dead: the single
    installation-wide ticker signal already explains the trailing catch-up,
    so the per-job cron.run_missed for that tail must NOT also fire.
    """
    hh, cron = _setup(tmp_path)
    _executions_db(cron, [_exec_row(1, "j1", B + 60)])
    _jobs_json(cron, [_interval_job("j1", minutes=1, created=B)])
    now = B + 600
    _heartbeat(cron, B)  # stale relative to this test's threshold
    ob = new_outbox(tmp_path)

    cfg = ReconcileConfig(cron_match_slack=45.0, ticker_stale_after=200.0)
    counts = reconcile(ob, hh, now=now, config=cfg)

    ticker = [e for e in findings(ob, "reconcile.terminal_missing")
              if e["payload"]["subject_type"] == "cron_ticker"]
    assert len(ticker) == 1  # the one installation-wide signal

    assert "cron.run_missed" not in counts
    assert findings(ob, "cron.run_missed") == []


# --- never fired ----------------------------------------------------------
def test_never_fired_job_past_first_due_instant(tmp_path):
    """A job that has never once executed is judged from its first due
    instant (created_at + step), with a catch-up count derived from how
    far `now` has drifted past created_at.
    """
    hh, cron = _setup(tmp_path)
    _executions_db(cron, [])  # never fired
    _jobs_json(cron, [_interval_job("j1", minutes=1, created=B)])
    now = B + 200
    _heartbeat(cron, now)  # fresh: no ticker-dead suppression in play
    ob = new_outbox(tmp_path)

    cfg = ReconcileConfig(cron_match_slack=45.0, ticker_stale_after=300.0)
    reconcile(ob, hh, now=now, config=cfg)

    missed = findings(ob, "cron.run_missed")
    assert len(missed) == 1
    m = missed[0]
    assert m["payload"]["expected_fire_at"] == B + 60
    assert m["payload"]["missed_count"] == 3


# --- startup gap is not a miss --------------------------------------------
def test_startup_gap_before_first_fire_is_not_a_miss(tmp_path):
    """Before a freshly created job's very first due instant, there is no
    miss to report at all — the gap between created_at and the first fire
    is expected latency, not lost work.
    """
    hh, cron = _setup(tmp_path)
    _executions_db(cron, [])  # never fired yet — and shouldn't have
    _jobs_json(cron, [_interval_job("j1", minutes=1, created=B)])
    now = B + 10  # well before created_at + 60s (minus slack)
    _heartbeat(cron, now)
    ob = new_outbox(tmp_path)

    cfg = ReconcileConfig(cron_match_slack=45.0, ticker_stale_after=300.0)
    counts = reconcile(ob, hh, now=now, config=cfg)

    assert "cron.run_missed" not in counts
    assert findings(ob, "cron.run_missed") == []
