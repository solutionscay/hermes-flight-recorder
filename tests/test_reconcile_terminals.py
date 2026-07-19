"""Self-contained tests for the reconciler's MISSING-TERMINAL detector.

Covers the four subject_types the terminal detector judges:

- session       — state.db row, source != 'subagent', ended_at NULL past
                  ``session_terminal_timeout``.
- subagent      — state.db row, source == 'subagent', judged against the
                  shorter ``subagent_terminal_timeout``.
- cron_run      — cron executions.db row, finished_at NULL past
                  ``cron_run_terminal_timeout``.
- invocation    — outbox ``invocation.started`` with no matching
                  ``invocation.completed`` past ``invocation_terminal_timeout``.

Every case is driven by an explicit ``now=`` float and a ``ReconcileConfig``
with small explicit windows, so no wall-clock is ever consulted. Boundary
cases assert that exactly-at-timeout does NOT flag (the reconciler uses
``age <= timeout`` / ``when - occurred <= timeout`` to skip) and one tick
past DOES.
"""

from __future__ import annotations

import datetime
import sqlite3

from hermes_flight_recorder.collector._common import build_record
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile

# A fixed epoch anchor and a fixed tz offset (mirrors tests/test_reconcile.py)
# so ISO timestamps round-trip deterministically through to_epoch().
B = 1784415000.0
TZ = datetime.timezone(datetime.timedelta(hours=-5))


def iso(epoch: float) -> str:
    return datetime.datetime.fromtimestamp(epoch, TZ).isoformat()


def new_outbox(tmp_path) -> Outbox:
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


def terminal_missing(outbox, subject_type=None):
    """All reconcile.terminal_missing findings, optionally filtered by subject_type."""
    out = []
    for e in outbox.iter_events():
        pl = e["payload"]
        if pl.get("event_type") != "reconcile.terminal_missing":
            continue
        if subject_type is not None and pl.get("subject_type") != subject_type:
            continue
        out.append(e)
    return out


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


# --- state.db fixture ----------------------------------------------------
_STATE_DDL = """
CREATE TABLE sessions (id TEXT, source TEXT, parent_session_id TEXT,
    started_at REAL, ended_at REAL, expiry_finalized INT, profile_name TEXT);
CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT);
CREATE TABLE session_model_usage (session_id TEXT, model TEXT, task TEXT);
"""


def make_state_db(hh, sessions):
    """``sessions``: dicts with id, source, started_at; optional parent_session_id,
    ended_at, expiry_finalized, profile_name."""
    db = sqlite3.connect(hh / "state.db")
    db.executescript(_STATE_DDL)
    for s in sessions:
        db.execute(
            "INSERT INTO sessions VALUES (?,?,?,?,?,?,?)",
            (
                s["id"],
                s["source"],
                s.get("parent_session_id"),
                s["started_at"],
                s.get("ended_at"),
                s.get("expiry_finalized", 0),
                s.get("profile_name"),
            ),
        )
    db.commit()
    db.close()


def make_executions_db(cron_dir, rows):
    """``rows``: (id, job_id, status, claimed_at_iso, started_at_iso, finished_at_iso)."""
    db = sqlite3.connect(cron_dir / "executions.db")
    db.execute(
        "CREATE TABLE executions (id TEXT, job_id TEXT, source TEXT, pid INT, status TEXT, "
        "claimed_at TEXT, started_at TEXT, finished_at TEXT, error TEXT)"
    )
    db.executemany(
        "INSERT INTO executions VALUES (?,?,'builtin',1,?,?,?,?,NULL)",
        rows,
    )
    db.commit()
    db.close()


# --- session -------------------------------------------------------------
def test_session_boundary_exact_timeout_not_flagged(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_state_db(hh, [{"id": "S", "source": "cli", "started_at": B}])
    ob = new_outbox(tmp_path)

    cfg = ReconcileConfig(session_terminal_timeout=200.0)
    reconcile(ob, hh, now=B + 200.0, config=cfg)  # age == timeout exactly

    assert terminal_missing(ob, "session") == []


def test_session_past_timeout_flagged(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_state_db(hh, [{"id": "S", "source": "cli", "started_at": B}])
    ob = new_outbox(tmp_path)

    cfg = ReconcileConfig(session_terminal_timeout=200.0)
    reconcile(ob, hh, now=B + 201.0, config=cfg)  # one tick past

    term = terminal_missing(ob, "session")
    assert len(term) == 1
    t = term[0]
    assert t["payload"]["subject_id"] == "S"
    assert t["payload"]["expected_terminal_event_type"] == "session.ended"
    assert t["partial"] is True
    assert t["session_id"] == "S"
    assert t["correlation_id"] == "S"  # no parent -> correlation root is itself


# --- subagent (shorter timeout) -------------------------------------------
def test_subagent_shorter_timeout_differs_from_session_same_age(tmp_path):
    """A same-age cli root session and a subagent child: the subagent's
    shorter window trips while the root's longer window does not."""
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_state_db(
        hh,
        [
            {"id": "ROOT", "source": "cli", "started_at": B},
            {"id": "SUB", "source": "subagent", "parent_session_id": "ROOT", "started_at": B},
        ],
    )
    ob = new_outbox(tmp_path)

    cfg = ReconcileConfig(session_terminal_timeout=1000.0, subagent_terminal_timeout=100.0)
    reconcile(ob, hh, now=B + 500.0, config=cfg)  # age 500: > 100, but <= 1000

    sub_term = terminal_missing(ob, "subagent")
    sess_term = terminal_missing(ob, "session")
    assert len(sub_term) == 1
    assert sess_term == []  # ROOT still within its (longer) lifetime

    t = sub_term[0]
    assert t["payload"]["subject_id"] == "SUB"
    assert t["payload"]["expected_terminal_event_type"] == "subagent.completed"
    assert t["partial"] is True
    assert t["session_id"] == "SUB"
    assert t["parent_session_id"] == "ROOT"
    assert t["correlation_id"] == "ROOT"  # correlation walks to the root session


def test_subagent_boundary_exact_timeout_not_flagged(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_state_db(
        hh,
        [
            {"id": "ROOT", "source": "cli", "started_at": B},
            {"id": "SUB", "source": "subagent", "parent_session_id": "ROOT", "started_at": B},
        ],
    )
    ob = new_outbox(tmp_path)

    cfg = ReconcileConfig(session_terminal_timeout=1000.0, subagent_terminal_timeout=50.0)
    reconcile(ob, hh, now=B + 50.0, config=cfg)  # age == subagent timeout exactly

    assert terminal_missing(ob, "subagent") == []


# --- cron_run --------------------------------------------------------------
def test_cron_run_boundary_exact_timeout_not_flagged(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    cron = hh / "cron"
    cron.mkdir()
    make_executions_db(cron, [("e1", "j1", "running", iso(B), iso(B), None)])
    ob = new_outbox(tmp_path)

    cfg = ReconcileConfig(cron_run_terminal_timeout=90.0)
    reconcile(ob, hh, now=B + 90.0, config=cfg)  # age == timeout exactly

    assert terminal_missing(ob, "cron_run") == []


def test_cron_run_past_timeout_flagged(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    cron = hh / "cron"
    cron.mkdir()
    make_executions_db(cron, [("e1", "j1", "running", iso(B), iso(B), None)])
    ob = new_outbox(tmp_path)

    cfg = ReconcileConfig(cron_run_terminal_timeout=90.0)
    reconcile(ob, hh, now=B + 91.0, config=cfg)  # one tick past

    term = terminal_missing(ob, "cron_run")
    assert len(term) == 1
    t = term[0]
    assert t["payload"]["subject_id"] == "e1"
    assert t["payload"]["job_id"] == "j1"
    assert t["payload"]["expected_terminal_event_type"] == "cron.run_finished"
    assert t["partial"] is True
    assert t["correlation_id"] == "j1"


# --- invocation ------------------------------------------------------------
def test_invocation_boundary_exact_timeout_not_flagged(tmp_path):
    ob = new_outbox(tmp_path)
    append_event(
        ob, "invocation.started",
        occurred_at=B, invocation_id="S:turn:1", session_id="S", correlation_id="S",
    )
    cfg = ReconcileConfig(invocation_terminal_timeout=50.0)

    reconcile(ob, tmp_path / "no-hermes", now=B + 50.0, config=cfg)  # age == timeout exactly

    assert terminal_missing(ob, "invocation") == []


def test_invocation_past_timeout_flagged(tmp_path):
    ob = new_outbox(tmp_path)
    append_event(
        ob, "invocation.started",
        occurred_at=B, invocation_id="S:turn:1", session_id="S", correlation_id="S",
    )
    cfg = ReconcileConfig(invocation_terminal_timeout=50.0)

    reconcile(ob, tmp_path / "no-hermes", now=B + 51.0, config=cfg)  # one tick past

    term = terminal_missing(ob, "invocation")
    assert len(term) == 1
    t = term[0]
    assert t["payload"]["subject_id"] == "S:turn:1"
    assert t["payload"]["expected_terminal_event_type"] == "invocation.completed"
    assert t["partial"] is True
    assert t["invocation_id"] == "S:turn:1"
    assert t["correlation_id"] == "S"
    assert t["session_id"] == "S"


def test_invocation_completed_pair_not_flagged(tmp_path):
    """A started+completed pair must never be flagged, however old it is."""
    ob = new_outbox(tmp_path)
    append_event(
        ob, "invocation.started",
        occurred_at=B, invocation_id="S:turn:2", session_id="S", correlation_id="S",
    )
    append_event(
        ob, "invocation.completed",
        occurred_at=B + 1.0, invocation_id="S:turn:2", session_id="S", correlation_id="S",
    )
    # An unrelated started-without-completed invocation confirms the detector
    # is actually running (and would flag) while the paired one stays silent.
    append_event(
        ob, "invocation.started",
        occurred_at=B, invocation_id="S:turn:3", session_id="S", correlation_id="S",
    )
    cfg = ReconcileConfig(invocation_terminal_timeout=50.0)

    reconcile(ob, tmp_path / "no-hermes", now=B + 1000.0, config=cfg)

    term = terminal_missing(ob, "invocation")
    ids = {t["payload"]["subject_id"] for t in term}
    assert "S:turn:2" not in ids  # completed — never flagged
    assert "S:turn:3" in ids  # never completed, well past timeout — flagged
