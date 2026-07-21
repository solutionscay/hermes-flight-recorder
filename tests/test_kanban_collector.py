"""Tests for the Kanban board adapter (Phase 2, issue #51).

Fixtures build a real Hermes-shaped ``kanban.db`` and drive the five reserved
``task.*`` lifecycle events through it. The scenario exercises the one rule the
contract (#50) turns on: state is read from ``task_events.kind`` +
``task_runs.outcome``, never the overloaded ``tasks.status`` column.
"""

from __future__ import annotations

import sqlite3
from collections import Counter

from hermes_flight_recorder.collector import kanban_db
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.envelope import validate

# The kanban.db columns this adapter reads. Real Hermes boards have many more;
# the fixture carries only what the SELECTs touch, plus enough shape to be
# faithful.
_TASKS_DDL = (
    "CREATE TABLE tasks (id TEXT, status TEXT, session_id TEXT, priority INT, "
    "assignee TEXT, project_id TEXT, idempotency_key TEXT, block_kind TEXT, "
    "consecutive_failures INT)"
)
_RUNS_DDL = (
    "CREATE TABLE task_runs (id INTEGER PRIMARY KEY, task_id TEXT, claim_lock TEXT, "
    "claim_expires INT, worker_pid INT, last_heartbeat_at INT, outcome TEXT)"
)
_EVENTS_DDL = (
    "CREATE TABLE task_events (id INTEGER PRIMARY KEY, task_id TEXT, run_id INT, "
    "kind TEXT, payload TEXT, created_at INT)"
)


def make_board(db_path) -> None:
    """A board with three tasks: a completed one, a breaker give-up, a block.

    The give-up task deliberately sits in ``status='blocked'`` to prove the
    adapter classifies it ``task.failed_terminal`` from the event kind, not the
    status column.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(db_path)
    db.executescript(_TASKS_DDL + ";" + _RUNS_DDL + ";" + _EVENTS_DDL)
    db.executemany(
        "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?)",
        [
            ("t_aaaaaaaa", "done", "sess-1", 5, "worker", "proj", "idem-a", None, 0),
            ("t_bbbbbbbb", "blocked", None, 3, None, None, None, None, 2),
            ("t_cccccccc", "blocked", None, 1, None, None, None, "needs_input", 0),
        ],
    )
    db.executemany(
        "INSERT INTO task_runs VALUES (?,?,?,?,?,?,?)",
        [
            (1, "t_aaaaaaaa", "host-1:100", 2000, 100, 1900, "completed"),
            (2, "t_bbbbbbbb", "host-1:200", 1500, 200, None, "gave_up"),
        ],
    )
    db.executemany(
        "INSERT INTO task_events VALUES (?,?,?,?,?,?)",
        [
            (1, "t_aaaaaaaa", None, "created", None, 1000),
            (2, "t_aaaaaaaa", 1, "claimed", None, 1010),
            (3, "t_aaaaaaaa", 1, "completed", "{}", 1050),
            (4, "t_bbbbbbbb", None, "created", None, 1005),
            (5, "t_bbbbbbbb", 2, "claimed", None, 1015),
            (6, "t_bbbbbbbb", 2, "gave_up", "{}", 1060),
            (7, "t_cccccccc", None, "created", None, 1008),
            (8, "t_cccccccc", None, "blocked", "{}", 1020),
            # Non-lifecycle kinds the five-event contract skips.
            (9, "t_aaaaaaaa", None, "assigned", None, 1009),
            (10, "t_aaaaaaaa", 1, "heartbeat", None, 1030),
        ],
    )
    db.commit()
    db.close()


def new_outbox(tmp_path):
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


def types(outbox) -> Counter:
    return Counter(e["payload"]["event_type"] for e in outbox.iter_events())


def test_kanban_event_mapping(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_board(hh / "kanban.db")
    ob = new_outbox(tmp_path)

    counts = kanban_db.poll(ob, hh)
    assert counts == {
        "task.created": 3,
        "task.claimed": 2,
        "task.completed": 1,
        "task.failed_terminal": 1,  # from gave_up, not the blocked status
        "task.blocked": 1,
    }
    for e in ob.iter_events():
        validate(e)


def test_terminal_from_kind_not_status(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_board(hh / "kanban.db")
    ob = new_outbox(tmp_path)
    kanban_db.poll(ob, hh)

    ev = next(
        e for e in ob.iter_events() if e["payload"]["event_type"] == "task.failed_terminal"
    )
    assert ev["payload"]["task_id"] == "t_bbbbbbbb"
    assert ev["payload"]["hermes_event_kind"] == "gave_up"
    assert ev["payload"]["status"] == "blocked"  # status says blocked; kind says terminal


def test_claim_carries_lease_from_run(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_board(hh / "kanban.db")
    ob = new_outbox(tmp_path)
    kanban_db.poll(ob, hh)

    claimed = next(
        e
        for e in ob.iter_events()
        if e["payload"]["event_type"] == "task.claimed"
        and e["payload"]["task_id"] == "t_aaaaaaaa"
    )
    p = claimed["payload"]
    assert p["run_id"] == 1
    assert p["holder"] == "host-1:100"
    assert p["claim_expires"] == 2000
    assert p["worker_pid"] == 100
    assert p["last_heartbeat_at"] == 1900


def test_created_correlates_and_links_session(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_board(hh / "kanban.db")
    ob = new_outbox(tmp_path)
    kanban_db.poll(ob, hh)

    created = next(
        e
        for e in ob.iter_events()
        if e["payload"]["event_type"] == "task.created"
        and e["payload"]["task_id"] == "t_aaaaaaaa"
    )
    assert created["correlation_id"] == "t_aaaaaaaa"  # every event of a task ties together
    assert created["session_id"] == "sess-1"  # links back to the originating session
    assert created["payload"]["board"] == "default"


def test_blocked_carries_block_kind(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_board(hh / "kanban.db")
    ob = new_outbox(tmp_path)
    kanban_db.poll(ob, hh)

    blocked = next(
        e for e in ob.iter_events() if e["payload"]["event_type"] == "task.blocked"
    )
    assert blocked["payload"]["block_kind"] == "needs_input"


def test_repoll_is_idempotent(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_board(hh / "kanban.db")
    ob = new_outbox(tmp_path)
    kanban_db.poll(ob, hh)
    n = ob.count()
    second = kanban_db.poll(ob, hh)
    assert ob.count() == n  # dedup on task_events id: no duplicates
    assert second == {}


def test_multiple_boards_are_namespaced(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_board(hh / "kanban.db")  # legacy top-level -> board "default"
    make_board(hh / "kanban" / "boards" / "alpha" / "kanban.db")
    ob = new_outbox(tmp_path)

    counts = kanban_db.poll(ob, hh)
    # Both boards contribute; identical task_events ids do not collide because
    # the dedup key is board-scoped.
    assert counts["task.created"] == 6
    boards = {e["payload"]["board"] for e in ob.iter_events()}
    assert boards == {"default", "alpha"}


def test_adapter_never_writes_board(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    db = hh / "kanban.db"
    make_board(db)
    ob = new_outbox(tmp_path)
    before = db.read_bytes()
    kanban_db.poll(ob, hh)
    assert db.read_bytes() == before  # byte-for-byte unchanged


def test_no_kanban_is_tolerated(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()  # no kanban.db, no boards dir
    ob = new_outbox(tmp_path)
    assert kanban_db.poll(ob, hh) == {}  # nothing to poll, no crash
