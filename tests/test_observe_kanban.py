"""Tests for the observe kanban view (Phase 2, issue #55).

The kanban view renders the reserved ``task.*`` events into a per-task board:
latest status, current holder + lease state, the per-attempt timeline (each
``task.claimed`` paired with its ``task.attempt_ended`` by ``run_id``), and the
task terminals. Records are produced by ``kanban_db.poll`` over a real
Hermes-shaped fixture board — the same path the collector tests drive — so the
view is exercised on genuine envelopes, and directly through ``build_record``
where a hand-built event makes an assertion sharper.
"""

from __future__ import annotations

import sqlite3

from hermes_flight_recorder import observe
from hermes_flight_recorder.cli import main
from hermes_flight_recorder.collector import kanban_db
from hermes_flight_recorder.collector._common import build_record
from hermes_flight_recorder.collector.outbox import Outbox

# Reuse the collector test's board builder so the two stay in lock-step on the
# real kanban.db shape.
from tests.test_kanban_collector import make_board

B = 1784415000.0


def new_outbox(tmp_path) -> Outbox:
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


def add(ob, event_type, *, occurred_at=B, correlation_id="corr", session_id=None,
        payload=None):
    rec = build_record(
        event_type=event_type,
        occurred_at=occurred_at,
        source="test",
        capture_method="test",
        runtime={"kind": "kanban", "engine": "standard"},
        correlation_id=correlation_id,
        session_id=session_id,
        payload=payload or {},
    )
    return ob.append(rec)


def seed_board(tmp_path) -> Outbox:
    """Poll the shared fixture board into a fresh outbox."""
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_board(hh / "kanban.db")
    ob = new_outbox(tmp_path)
    kanban_db.poll(ob, hh)
    return ob


# --- grouping & status --------------------------------------------------
def test_kanban_groups_each_task_with_its_latest_status(tmp_path):
    ob = seed_board(tmp_path)
    lines = observe.render_kanban(observe.load(ob))
    text = "\n".join(lines)
    # Three tasks, each a header keyed on task_id + board, carrying the
    # tasks.status snapshot.
    assert "▣ task t_aaaaaaaa  [done]  board=default" in text
    assert "▣ task t_bbbbbbbb  [blocked]  board=default" in text
    assert "▣ task t_cccccccc  [blocked]  board=default" in text


def test_kanban_empty_records_renders_placeholder(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="P", payload={"kind": "cli"})  # not a task
    lines = observe.render_kanban(observe.load(ob))
    assert lines == ["(no kanban tasks captured)"]


# --- holder + lease -----------------------------------------------------
def test_kanban_shows_current_holder_and_lease_state(tmp_path):
    ob = seed_board(tmp_path)
    task = _task_block(observe.render_kanban(observe.load(ob)), "t_aaaaaaaa")
    holder = next(l for l in task if "holder" in l)
    # The completing attempt (run 1) is the newest holder-bearing event and its
    # run ended, so the lease reads released, with the claim's expiry.
    assert "host-1:100" in holder
    assert "[released]" in holder
    assert "expires=2000" in holder


def test_kanban_open_claim_reads_held(tmp_path):
    """A claim whose run has no attempt_ended yet still holds its lease."""
    ob = new_outbox(tmp_path)
    add(ob, "task.claimed", correlation_id="t_open",
        payload={"board": "default", "task_id": "t_open", "status": "in_progress",
                 "run_id": 9, "holder": "host-9:900", "claim_expires": 5000,
                 "hermes_event_kind": "claimed"})
    task = _task_block(observe.render_kanban(observe.load(ob)), "t_open")
    holder = next(l for l in task if "holder" in l)
    assert "host-9:900" in holder
    assert "[held]" in holder
    assert "expires=5000" in holder


# --- attempt timeline ---------------------------------------------------
def test_kanban_attempt_timeline_pairs_claim_and_end_by_run(tmp_path):
    ob = seed_board(tmp_path)
    task = _task_block(observe.render_kanban(observe.load(ob)), "t_aaaaaaaa")
    text = "\n".join(task)
    assert "attempts:" in text
    # run 1 completed (success), run 3 was reclaimed (released) — each end paired
    # to its run by id, with the holder and outcome/disposition.
    assert "run 1  host-1:100  completed/success" in text
    assert "run 3  host-1:101  reclaimed/released" in text
    # Ordered oldest-first by attempt time, so the reclaimed early attempt (run
    # 3, ended 1005) is listed before the completing one (run 1, ended 1050),
    # even though its run id is higher.
    assert text.index("run 3") < text.index("run 1")


def test_kanban_running_attempt_reads_running(tmp_path):
    """A claim with no captured attempt_ended shows as still running."""
    ob = new_outbox(tmp_path)
    add(ob, "task.claimed", correlation_id="t_run",
        payload={"board": "default", "task_id": "t_run", "status": "in_progress",
                 "run_id": 7, "holder": "host-7:700", "claim_expires": 4000,
                 "hermes_event_kind": "claimed"})
    task = _task_block(observe.render_kanban(observe.load(ob)), "t_run")
    assert any("run 7  host-7:700  running" in l for l in task)


# --- terminals ----------------------------------------------------------
def test_kanban_terminals_explain_the_transition(tmp_path):
    ob = seed_board(tmp_path)
    lines = observe.render_kanban(observe.load(ob))
    completed = _task_block(lines, "t_aaaaaaaa")
    assert any("task.completed  (completed)" in l for l in completed)
    # The give-up terminal names the raw kind, distinguishing it from the
    # tasks.status column (which still reads "blocked").
    failed = _task_block(lines, "t_bbbbbbbb")
    assert any("task.failed_terminal  (gave_up)" in l for l in failed)
    # A recoverable block surfaces its block_kind for the operator.
    blocked = _task_block(lines, "t_cccccccc")
    assert any("task.blocked  (blocked, block_kind=needs_input)" in l for l in blocked)


# --- CLI wiring ---------------------------------------------------------
def test_kanban_cli_view_prints_header_and_tasks(tmp_path, capsys):
    bridge = str(tmp_path / "bridge")
    hh = tmp_path / "hermes"
    hh.mkdir()
    make_board(hh / "kanban.db")
    ob = Outbox.open(bridge)
    ob.initialize()
    kanban_db.poll(ob, hh)
    ob.close()

    code = main(["observe", "--kanban", "--bridge-home", bridge])
    out = capsys.readouterr().out
    assert code == 0
    assert "── kanban ──" in out
    assert "▣ task t_aaaaaaaa  [done]  board=default" in out
    assert "run 1  host-1:100  completed/success" in out


def test_kanban_cli_view_order_after_report(tmp_path, capsys):
    """The fixed view order stays stream -> tree -> report -> kanban."""
    bridge = str(tmp_path / "bridge")
    ob = Outbox.open(bridge)
    ob.initialize()
    ob.close()

    code = main(["observe", "--kanban", "--report", "--stream", "--tree",
                 "--bridge-home", bridge])
    out = capsys.readouterr().out
    assert code == 0
    assert (out.index("── stream") < out.index("── tree ──")
            < out.index("── report ──") < out.index("── kanban ──"))


def _task_block(lines, task_id):
    """The header line for ``task_id`` plus its indented body, up to the next
    task header or a blank separator.
    """
    block = []
    inside = False
    for line in lines:
        if line.startswith("▣ task "):
            if inside:
                break
            inside = f"task {task_id} " in line
            if inside:
                block.append(line)
            continue
        if inside:
            if line == "":
                break
            block.append(line)
    assert block, f"no block for {task_id}"
    return block
