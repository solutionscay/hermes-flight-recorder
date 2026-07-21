#!/usr/bin/env python3
"""Phase 2 POC Kanban gate — capture, reconcile, detect a dead attempt (issue #56).

Runs the Kanban slice of the Bridge pipeline against a throwaway, synthetic-
but-schema-accurate ``kanban.db`` and proves the Phase 2 claim: **a claimed-
then-abandoned attempt is detectable.** Three scenarios, each on its own
disposable outbox:

1. Healthy board    — one live claim (an unexpired lease) reconciles clean; the
                      reconciler raises no ``reconcile.terminal_missing``.
2. Abandoned claim  — an open ``task_runs`` row whose ``claim_expires`` lapsed
                      past the grace with a dead heartbeat yields exactly one
                      ``reconcile.terminal_missing`` with ``subject_type='task_run'``.
3. Bridge restart   — reopen the outbox: the ``producer_sequence`` high-water
                      mark survives, and a second reconcile over the same durable
                      state appends nothing new (idempotent, one finding total).

Determinism: a fixed ``now`` (no wall clock) and a fixed synthetic board, so the
result is identical on any host at any time. This is the automated gate; for a
run against the *real* Hermes kanban CLI, see ``scripts/kanban_live_check.py``.

Usage:  python scripts/poc_kanban_gate.py [-v]
Exit:   0 if every scenario passes its assertions, 1 otherwise.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

# The CI wrappers spec-load this file, so put the sibling _gate module on the
# path; the repo root goes first so a shared venv (e.g. a git worktree) imports
# the co-located hermes_flight_recorder, not an editable install elsewhere.
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _gate import run_gate
from hermes_flight_recorder.collector import kanban_db
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile

# A fixed clock. Everything is anchored to it so the gate never depends on when
# it runs. NOW sits shortly after the demo claim activity.
NOW = 1_800_000_000.0
CFG = ReconcileConfig()
VERBOSE = "-v" in sys.argv[1:]

# The board slug for the legacy top-level kanban.db.
BOARD = "default"


# --- synthetic Kanban board ----------------------------------------------
def build_kanban(home: Path, *, abandoned: bool) -> tuple[str, int]:
    """A one-task board with a single open claim (a ``task_runs`` row, outcome
    NULL). ``abandoned=True`` lapses its lease past the grace with a dead
    heartbeat (a worker that died mid-attempt); otherwise the lease is live.

    The columns mirror the real Hermes schema (``hermes_cli/kanban_db.py``):
    ``claim_expires`` / ``last_heartbeat_at`` are epoch integers written at
    claim time and preserved per run. Returns ``(task_id, run_id)``.
    """
    home.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(home / "kanban.db")
    db.executescript(
        """
        CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT, body TEXT,
            assignee TEXT, status TEXT, priority INTEGER, session_id TEXT,
            project_id TEXT, idempotency_key TEXT, block_kind TEXT,
            consecutive_failures INTEGER DEFAULT 0);
        CREATE TABLE task_runs (id INTEGER PRIMARY KEY, task_id TEXT, profile TEXT,
            step_key TEXT, status TEXT, claim_lock TEXT, claim_expires INTEGER,
            worker_pid INTEGER, last_heartbeat_at INTEGER, started_at INTEGER,
            ended_at INTEGER, outcome TEXT);
        CREATE TABLE task_events (id INTEGER PRIMARY KEY, task_id TEXT,
            run_id INTEGER, kind TEXT, created_at INTEGER);
        """
    )
    task_id, run_id = "t_demo01", 1
    if abandoned:
        # Claimed 4000 s ago; its 900 s lease lapsed 3100 s ago; no heartbeat
        # ever arrived — the worker is gone and no terminal is coming.
        created, claimed = NOW - 4100, NOW - 4000
        claim_expires, heartbeat = NOW - 3100, None
    else:
        # A fresh claim: the lease is still open and a recent heartbeat proves
        # the worker is alive, so the reconciler must leave it be.
        created, claimed = NOW - 120, NOW - 100
        claim_expires, heartbeat = NOW + 800, NOW - 20
    db.execute(
        "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (task_id, "demo task", None, "probe", "running", 0, "sess1", None, None, None, 0),
    )
    db.execute(
        "INSERT INTO task_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (run_id, task_id, "probe", None, "running", "host:4242",
         int(claim_expires), 4242, None if heartbeat is None else int(heartbeat),
         int(claimed), None, None),
    )
    db.executemany(
        "INSERT INTO task_events VALUES (?,?,?,?,?)",
        [
            (1, task_id, None, "created", int(created)),
            (2, task_id, run_id, "claimed", int(claimed)),
        ],
    )
    db.commit()
    db.close()
    return task_id, run_id


def terminal_missing(ob: Outbox) -> list[dict]:
    """Every ``reconcile.terminal_missing`` for a ``task_run`` in the stream."""
    return [
        e
        for e in ob.iter_events()
        if e["payload"]["event_type"] == "reconcile.terminal_missing"
        and e["payload"].get("subject_type") == "task_run"
    ]


def show(ob: Outbox, title: str, findings: dict) -> None:
    if not VERBOSE:
        return
    print(f"\n    --- {title}: reconcile findings ---")
    print(f"    {dict(findings)}")
    for e in terminal_missing(ob):
        p = e["payload"]
        print(f"    terminal_missing task_run {p.get('subject_id')} on board {p.get('board')}")


# --- scenarios ------------------------------------------------------------
def scenario_healthy(tmp: Path) -> list[str]:
    fails: list[str] = []
    home = tmp / "h"
    build_kanban(home, abandoned=False)

    ob = Outbox.open(tmp / "b")
    ob.initialize()
    poll_counts = kanban_db.poll(ob, home)
    findings = reconcile(ob, home, now=NOW, config=CFG)
    show(ob, "healthy", findings)

    if findings.get("reconcile.terminal_missing"):
        fails.append(f"healthy: a live claim raised {findings} (want no terminal_missing)")
    if terminal_missing(ob):
        fails.append("healthy: an active board produced a false task_run terminal_missing")
    if poll_counts.get("task.claimed") != 1:
        fails.append(f"healthy: expected the claim to be captured, got {poll_counts}")
    ob.close()
    return fails


def scenario_abandoned(tmp: Path) -> list[str]:
    fails: list[str] = []
    home = tmp / "h"
    _, run_id = build_kanban(home, abandoned=True)

    ob = Outbox.open(tmp / "b")
    ob.initialize()
    poll_counts = kanban_db.poll(ob, home)
    findings = reconcile(ob, home, now=NOW, config=CFG)
    show(ob, "abandoned", findings)

    if findings != {"reconcile.terminal_missing": 1}:
        fails.append(f"abandoned: expected exactly one terminal_missing, got {dict(findings)}")
    tm = terminal_missing(ob)
    if len(tm) != 1:
        fails.append(f"abandoned: expected one task_run terminal_missing, got {len(tm)}")
    else:
        p = tm[0]["payload"]
        if p.get("subject_id") != str(run_id):
            fails.append(f"abandoned: finding points at run {p.get('subject_id')}, not {run_id}")
        if p.get("board") != BOARD:
            fails.append(f"abandoned: finding board {p.get('board')!r} != {BOARD!r}")
        if p.get("expected_terminal_event_type") != "task.attempt_ended":
            fails.append("abandoned: expected_terminal_event_type is not task.attempt_ended")
    if poll_counts.get("task.claimed") != 1:
        fails.append(f"abandoned: expected the claim to be captured, got {poll_counts}")
    ob.close()
    return fails


def scenario_restart(tmp: Path) -> list[str]:
    fails: list[str] = []
    home = tmp / "h"
    build_kanban(home, abandoned=True)
    bridge = tmp / "b"

    ob = Outbox.open(bridge)
    ob.initialize()
    kanban_db.poll(ob, home)
    reconcile(ob, home, now=NOW, config=CFG)
    hw, n, inst = ob.high_water(), ob.count(), ob.installation_id
    before = len(terminal_missing(ob))
    ob.close()  # simulate the Bridge process stopping

    if before != 1:
        fails.append(f"restart: pre-restart stream has {before} terminal_missing, want 1")

    # Reopen — a fresh process/handle onto the same durable outbox.
    ob = Outbox.open(bridge)
    if ob.high_water() != hw:
        fails.append(f"restart: high-water {ob.high_water()} != pre-restart {hw}")
    if ob.installation_id != inst:
        fails.append("restart: installation_id changed across restart")
    if ob.count() != n:
        fails.append(f"restart: event count {ob.count()} != {n}")

    # A second capture + reconcile over the same durable state must append
    # nothing new: the poll dedups on run/event id and the finding dedups on
    # the (board, run) key, so the stream stays idempotent.
    kanban_db.poll(ob, home)
    findings = reconcile(ob, home, now=NOW, config=CFG)
    show(ob, "restart (2nd pass)", findings)
    if findings:
        fails.append(f"restart: second pass appended {dict(findings)} (want nothing)")
    if ob.count() != n:
        fails.append(f"restart: second pass changed the count {ob.count()} != {n}")
    if len(terminal_missing(ob)) != 1:
        fails.append(f"restart: stream now has {len(terminal_missing(ob))} terminal_missing, want 1")
    ob.close()
    return fails


SCENARIOS = [
    ("healthy board", scenario_healthy),
    ("abandoned claim", scenario_abandoned),
    ("bridge restart", scenario_restart),
]


def main() -> int:
    return run_gate(
        ["Phase 2 POC Kanban gate (issue #56)"],
        SCENARIOS,
        passed="GATE PASSED — a claimed-then-abandoned attempt is loss-detectable across restarts",
        failed="GATE FAILED",
    )


if __name__ == "__main__":
    raise SystemExit(main())
