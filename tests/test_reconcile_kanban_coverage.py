"""Focused tests for the reconciler's Kanban coverage-gap detector (issue #54).

``_coverage_kanban`` diffs each board's durable ``tasks`` and ``task_runs``
rows against the captured ``task.*`` stream. A durable row with no matching
captured event proves a dropped capture and surfaces as
``reconcile.gap_detected`` / ``gap_kind='uncaptured_row'`` with
``subject_type`` ``task`` or ``task_run``.

Self-contained: builds its own ``kanban.db`` fixtures and passes a fixed
``now`` plus a ``ReconcileConfig`` with small windows, so wall-clock never
enters. Every run row carries an ``outcome`` so the stale-lease detector
(issue #53) never fires and the coverage assertions stand alone.
"""

from __future__ import annotations

import sqlite3

from hermes_flight_recorder.collector._common import build_record
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile
from hermes_flight_recorder.envelope import validate

B = 1784415000.0

# task_runs carries the columns the stale-lease loader (#53) also reads, so a
# reconcile pass over the same board never errors on a missing column.
_KANBAN_SCHEMA = """
CREATE TABLE tasks (id TEXT, session_id TEXT, status TEXT);
CREATE TABLE task_runs (id INTEGER PRIMARY KEY, task_id TEXT, claim_lock TEXT,
    claim_expires INT, worker_pid INT, last_heartbeat_at INT, started_at INT,
    outcome TEXT);
"""


def make_kanban_db(path, *, tasks=(), runs=()):
    """Build a minimal board kanban.db.

    tasks: (id, session_id, status). runs: (id, task_id, claim_lock,
    claim_expires, worker_pid, last_heartbeat_at, started_at, outcome).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.executescript(_KANBAN_SCHEMA)
    db.executemany("INSERT INTO tasks VALUES (?,?,?)", tasks)
    db.executemany("INSERT INTO task_runs VALUES (?,?,?,?,?,?,?,?)", runs)
    db.commit()
    db.close()


def new_outbox(tmp_path) -> Outbox:
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


def append_event(ob, event_type, **over):
    """Append a minimal valid producer event straight to the outbox."""
    rec = build_record(
        event_type=event_type,
        occurred_at=over.pop("occurred_at", B),
        source=over.pop("source", "kanban:test"),
        capture_method=over.pop("capture_method", "poll:test"),
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


# Small, explicit windows — never wall-clock defaults.
CFG = ReconcileConfig(
    coverage_grace=0.0,
    session_terminal_timeout=100.0,
    subagent_terminal_timeout=100.0,
    invocation_terminal_timeout=100.0,
    cron_run_terminal_timeout=100.0,
    ticker_stale_after=100.0,
    task_lease_grace=10.0,
    task_heartbeat_stale_after=60.0,
)

# A closed run: outcome set, so the stale-lease detector never touches it.
_CLOSED = (int(B + 100), 200, int(B + 100), int(B + 50), "completed")


# --- task coverage --------------------------------------------------------
def test_uncaptured_task_surfaces_once_as_coverage_gap(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_kanban_db(hh / "kanban.db", tasks=[("t1", "S1", "queued")])
    ob = new_outbox(tmp_path)

    reconcile(ob, hh, now=B, config=CFG)

    gaps = coverage_gaps(ob, "task")
    assert len(gaps) == 1
    g = gaps[0]
    assert g["payload"]["subject_id"] == "default:t1"
    assert g["payload"]["source_table"] == "kanban:default:tasks"
    assert g["correlation_id"] == "t1"
    assert g["session_id"] == "S1"
    assert g["partial"] is True
    validate(g)


def test_captured_task_created_suppresses_task_gap(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_kanban_db(hh / "kanban.db", tasks=[("t1", "S1", "queued")])
    ob = new_outbox(tmp_path)
    append_event(
        ob, "task.created", session_id="S1", correlation_id="t1",
        payload={"board": "default", "task_id": "t1"},
    )

    reconcile(ob, hh, now=B, config=CFG)

    assert coverage_gaps(ob, "task") == []


# --- run coverage ---------------------------------------------------------
def test_uncaptured_run_surfaces_once_as_coverage_gap(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_kanban_db(
        hh / "kanban.db",
        tasks=[("t1", "S1", "running")],
        runs=[(1, "t1", "host:200", *_CLOSED)],
    )
    ob = new_outbox(tmp_path)
    # Capture the task itself so only the run gap is left to surface.
    append_event(
        ob, "task.created", session_id="S1", correlation_id="t1",
        payload={"board": "default", "task_id": "t1"},
    )

    reconcile(ob, hh, now=B, config=CFG)

    gaps = coverage_gaps(ob, "task_run")
    assert len(gaps) == 1
    g = gaps[0]
    assert g["payload"]["subject_id"] == "default:1"
    assert g["payload"]["source_table"] == "kanban:default:task_runs"
    assert g["correlation_id"] == "t1"
    validate(g)


def test_captured_task_claimed_suppresses_run_gap(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_kanban_db(
        hh / "kanban.db",
        tasks=[("t1", "S1", "running")],
        runs=[(1, "t1", "host:200", *_CLOSED)],
    )
    ob = new_outbox(tmp_path)
    append_event(
        ob, "task.created", session_id="S1", correlation_id="t1",
        payload={"board": "default", "task_id": "t1"},
    )
    append_event(
        ob, "task.claimed", session_id="S1", correlation_id="t1",
        payload={"board": "default", "task_id": "t1", "run_id": 1},
    )

    reconcile(ob, hh, now=B, config=CFG)

    assert coverage_gaps(ob, "task_run") == []


def test_captured_attempt_ended_suppresses_run_gap(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_kanban_db(
        hh / "kanban.db",
        tasks=[("t1", "S1", "done")],
        runs=[(1, "t1", "host:200", *_CLOSED)],
    )
    ob = new_outbox(tmp_path)
    append_event(
        ob, "task.created", session_id="S1", correlation_id="t1",
        payload={"board": "default", "task_id": "t1"},
    )
    append_event(
        ob, "task.attempt_ended", session_id="S1", correlation_id="t1",
        payload={"board": "default", "task_id": "t1", "run_id": 1},
    )

    reconcile(ob, hh, now=B, config=CFG)

    assert coverage_gaps(ob, "task_run") == []


# --- board scoping --------------------------------------------------------
def test_equal_ids_across_boards_do_not_collide(tmp_path):
    """Two boards each hold a task id "t1" and a run id 1. A captured event on
    the default board must not suppress the alpha board's identical ids.
    """
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_kanban_db(
        hh / "kanban.db",
        tasks=[("t1", "S1", "running")],
        runs=[(1, "t1", "host:200", *_CLOSED)],
    )
    make_kanban_db(
        hh / "kanban" / "boards" / "alpha" / "kanban.db",
        tasks=[("t1", "S2", "running")],
        runs=[(1, "t1", "host:200", *_CLOSED)],
    )
    ob = new_outbox(tmp_path)
    # Only the default board's task + run are captured.
    append_event(
        ob, "task.created", session_id="S1", correlation_id="t1",
        payload={"board": "default", "task_id": "t1"},
    )
    append_event(
        ob, "task.claimed", session_id="S1", correlation_id="t1",
        payload={"board": "default", "task_id": "t1", "run_id": 1},
    )

    reconcile(ob, hh, now=B, config=CFG)

    surfaced = {
        (g["payload"]["subject_type"], g["payload"]["subject_id"])
        for g in coverage_gaps(ob)
    }
    # The alpha board's identical ids are board-scoped and still surface.
    assert surfaced == {("task", "alpha:t1"), ("task_run", "alpha:1")}
    for g in coverage_gaps(ob):
        validate(g)


# --- idempotency ----------------------------------------------------------
def test_kanban_coverage_is_idempotent_across_reconcile_runs(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_kanban_db(
        hh / "kanban.db",
        tasks=[("t1", "S1", "running")],
        runs=[(1, "t1", "host:200", *_CLOSED)],
    )
    ob = new_outbox(tmp_path)

    reconcile(ob, hh, now=B, config=CFG)
    assert len(coverage_gaps(ob, "task")) == 1
    assert len(coverage_gaps(ob, "task_run")) == 1
    n = ob.count()

    reconcile(ob, hh, now=B, config=CFG)
    assert ob.count() == n  # dedup_key stops a second identical finding
    assert len(coverage_gaps(ob, "task")) == 1
    assert len(coverage_gaps(ob, "task_run")) == 1
