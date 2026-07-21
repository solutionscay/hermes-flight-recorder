"""Tests for the stale task-lease detector (Phase 2, issue #53).

The Kanban analog of the stale-ticker signal. An open ``task_runs`` row
(``outcome`` NULL) whose ``claim_expires`` has lapsed past a grace, with a
heartbeat stale beyond the window, is a worker that died mid-attempt and gets
one ``reconcile.terminal_missing`` (``subject_type='task_run'``). A live/renewing
worker, a still-valid lease, and a closed run each raise nothing.

Self-contained: builds its own ``kanban.db`` and passes an explicit ``now`` and
a ``ReconcileConfig`` with small windows, so nothing depends on wall-clock. Only
``kanban.db`` is written; the other durable stores are absent and tolerated.
"""

from __future__ import annotations

import sqlite3
from collections import Counter

from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile
from hermes_flight_recorder.envelope import validate

B = 1784415000.0

_RUNS_DDL = (
    "CREATE TABLE task_runs (id INTEGER PRIMARY KEY, task_id TEXT, claim_lock TEXT, "
    "claim_expires INT, worker_pid INT, last_heartbeat_at INT, started_at INT, "
    "outcome TEXT)"
)


def kanban_db(path, runs) -> None:
    """runs: (id, task_id, claim_lock, claim_expires, worker_pid, hb, started_at, outcome)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    db.execute(_RUNS_DDL)
    db.executemany("INSERT INTO task_runs VALUES (?,?,?,?,?,?,?,?)", runs)
    db.commit()
    db.close()


def new_outbox(tmp_path) -> Outbox:
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


def lease_findings(outbox):
    return [
        e
        for e in outbox.iter_events()
        if e["payload"]["event_type"] == "reconcile.terminal_missing"
        and e["payload"]["subject_type"] == "task_run"
    ]


def dedup_keys(outbox):
    return [r[0] for r in outbox._conn.execute("SELECT dedup_key FROM events").fetchall()]


def small_cfg(**over) -> ReconcileConfig:
    base = dict(task_lease_grace=10.0, task_heartbeat_stale_after=60.0)
    base.update(over)
    return ReconcileConfig(**base)


# --- fires -----------------------------------------------------------------
def test_lapsed_lease_and_stale_heartbeat_fires_once(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    kanban_db(hh / "kanban.db", [
        (1, "t_aaaaaaaa", "host-1:200", int(B + 100), 200, int(B + 100), int(B + 50), None),
    ])
    ob = new_outbox(tmp_path)
    counts = reconcile(ob, hh, now=B + 200, config=small_cfg())  # lapsed 100>10, hb 100>60

    found = lease_findings(ob)
    assert len(found) == 1
    p = found[0]["payload"]
    assert p["subject_id"] == "1"
    assert p["run_id"] == 1
    assert p["board"] == "default"
    assert p["holder"] == "host-1:200"
    assert p["claim_expires"] == int(B + 100)
    assert p["expected_terminal_event_type"] == "task.attempt_ended"
    assert found[0]["correlation_id"] == "t_aaaaaaaa"
    assert found[0]["partial"] is True
    assert counts.get("reconcile.terminal_missing", 0) == 1
    for e in ob.iter_events():
        validate(e)


def test_missing_heartbeat_with_lapsed_lease_fires(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    kanban_db(hh / "kanban.db", [
        (1, "t_aaaaaaaa", "host-1:200", int(B + 100), None, None, int(B + 50), None),
    ])
    ob = new_outbox(tmp_path)
    reconcile(ob, hh, now=B + 200, config=small_cfg())
    assert len(lease_findings(ob)) == 1  # no heartbeat at all counts as stale


# --- does not fire ---------------------------------------------------------
def test_fresh_heartbeat_does_not_fire(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    kanban_db(hh / "kanban.db", [
        (1, "t_aaaaaaaa", "host-1:200", int(B + 100), 200, int(B + 190), int(B + 50), None),
    ])
    ob = new_outbox(tmp_path)
    # Lease is lapsed (100 > grace) but the worker heartbeated 10s ago (<= 60):
    # it is alive and Hermes will renew the lease, so no false positive.
    reconcile(ob, hh, now=B + 200, config=small_cfg())
    assert lease_findings(ob) == []


def test_valid_lease_does_not_fire(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    kanban_db(hh / "kanban.db", [
        (1, "t_aaaaaaaa", "host-1:200", int(B + 300), 200, None, int(B + 50), None),
    ])
    ob = new_outbox(tmp_path)
    reconcile(ob, hh, now=B + 200, config=small_cfg())  # claim_expires in the future
    assert lease_findings(ob) == []


def test_within_grace_does_not_fire(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    kanban_db(hh / "kanban.db", [
        (1, "t_aaaaaaaa", "host-1:200", int(B + 195), 200, None, int(B + 50), None),
    ])
    ob = new_outbox(tmp_path)
    reconcile(ob, hh, now=B + 200, config=small_cfg())  # lapsed only 5s <= grace 10
    assert lease_findings(ob) == []


def test_closed_run_does_not_fire(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    kanban_db(hh / "kanban.db", [
        # A long-finished attempt whose claim_expires is ancient — but it ended,
        # so there is no missing terminal.
        (1, "t_aaaaaaaa", "host-1:200", int(B + 100), 200, int(B + 100), int(B + 50), "completed"),
    ])
    ob = new_outbox(tmp_path)
    reconcile(ob, hh, now=B + 5000, config=small_cfg())
    assert lease_findings(ob) == []


def test_no_kanban_is_tolerated(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()  # no kanban.db, no other stores
    ob = new_outbox(tmp_path)
    counts = reconcile(ob, hh, now=B + 200, config=small_cfg())  # no crash
    assert lease_findings(ob) == []
    assert counts.get("reconcile.terminal_missing", 0) == 0


# --- multi-board + idempotency ---------------------------------------------
def test_multiple_boards_each_fire_with_scoped_keys(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    run = (1, "t_aaaaaaaa", "host-1:200", int(B + 100), 200, None, int(B + 50), None)
    kanban_db(hh / "kanban.db", [run])  # board "default", run id 1
    kanban_db(hh / "kanban" / "boards" / "alpha" / "kanban.db", [run])  # board "alpha", run id 1
    ob = new_outbox(tmp_path)
    reconcile(ob, hh, now=B + 200, config=small_cfg())

    found = lease_findings(ob)
    assert len(found) == 2  # equal run ids across boards do not collide
    assert {f["payload"]["board"] for f in found} == {"default", "alpha"}


def test_dedup_key_is_deterministic_and_idempotent(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    kanban_db(hh / "kanban.db", [
        (7, "t_aaaaaaaa", "host-1:200", int(B + 100), 200, None, int(B + 50), None),
    ])
    ob = new_outbox(tmp_path)
    cfg = small_cfg()

    first = reconcile(ob, hh, now=B + 200, config=cfg)
    assert first.get("reconcile.terminal_missing", 0) == 1
    expected_key = "reconcile:terminal:task_run:default:7"
    assert dedup_keys(ob).count(expected_key) == 1

    n = ob.count()
    second = reconcile(ob, hh, now=B + 200, config=cfg)  # same pass again
    assert ob.count() == n  # nothing new appended
    assert second.get("reconcile.terminal_missing", 0) == 0
    assert len(lease_findings(ob)) == 1
