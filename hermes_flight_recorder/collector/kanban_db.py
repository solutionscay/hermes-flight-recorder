"""Durable-state adapter for the Hermes Kanban kernel (Phase 2, issue #51).

Poll each board's ``kanban.db`` read-only and emit the five reserved ``task.*``
lifecycle events. Grounded in the frozen capture contract
(``docs/schema/envelope-v1.md``, issue #50), confirmed against Hermes source
(``hermes_cli/kanban_db.py``):

- ``task_events`` is Hermes's append-only transition log. Its ``kind`` — not the
  overloaded ``tasks.status`` column — is the authoritative lifecycle signal:
  ``status='blocked'`` covers both a recoverable block and a terminal
  circuit-breaker give-up, so the two are told apart by kind
  (``gave_up`` / ``block_loop_detected`` are terminal, the rest recoverable).
- The claiming attempt's lease lives on the ``task_runs`` row
  (``claim_lock`` = ``host:pid`` holder, ``claim_expires``, ``last_heartbeat_at``),
  written at claim time and preserved per run; the reconciler reads those off
  the emitted ``task.claimed`` event to judge a stale lease.
- Boards live at ``<home>/kanban/boards/<slug>/kanban.db`` plus a legacy
  top-level ``<home>/kanban.db`` (board slug ``"default"``).

Each ended ``task_runs`` row is also captured as one ``task.attempt_ended``
event (issue #52) carrying the run's outcome and disposition — attempt history
that the task-level events alone do not show, since a ``crashed`` / ``timed_out``
/ ``reclaimed`` attempt ends a run without ending the task. Sensitive text
(title, body, result, error, summaries, comments, event payloads) is never read
into the plaintext payload. This adapter never writes to a board.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

from ._common import (
    append_and_count,
    build_record,
    kanban_board_dbs,
    open_sqlite_read_only,
    read_home_mode,
    resolve_hermes_home,
    runtime_stamp,
    sqlite_column_or_default,
    sqlite_table_columns,
)

# Hermes task_events.kind -> the task-level task.* event it maps to. Kinds absent
# here (assigned, commented, heartbeat, claim_extended, spawned, promoted,
# unblocked, archived, …) are not task-level lifecycle transitions and are
# skipped. Attempt terminals (crashed, timed_out, reclaimed, …) are captured
# instead from task_runs as task.attempt_ended, keyed on the run, not the event.
_KIND_EVENT = {
    "created": "task.created",
    "claimed": "task.claimed",
    "completed": "task.completed",
    "blocked": "task.blocked",
    "dependency_wait": "task.blocked",
    "scheduled": "task.blocked",
    "gave_up": "task.failed_terminal",
    "block_loop_detected": "task.failed_terminal",
}

# task_runs.outcome -> the coarse disposition of one ended attempt. success ends
# the task; failure feeds the circuit breaker and the task retries; released
# hands the task back to the queue or a waiting state. An unlisted outcome is
# reported verbatim as run_outcome with disposition "unknown".
_RUN_DISPOSITION = {
    "completed": "success",
    "crashed": "failure",
    "timed_out": "failure",
    "spawn_failed": "failure",
    "gave_up": "failure",
    "reclaimed": "released",
    "stale": "released",
    "rate_limited": "released",
    "blocked": "released",
    "scheduled": "released",
}

# Current-snapshot task columns that are plaintext metadata (never free text).
_TASK_META = (
    "priority",
    "assignee",
    "project_id",
    "idempotency_key",
    "block_kind",
    "consecutive_failures",
)


def poll(outbox: Any, hermes_home: str | Path | None = None) -> dict[str, int]:
    """One read-only poll pass over every Kanban board. Returns per-type counts."""
    home = resolve_hermes_home(hermes_home)
    home_mode = read_home_mode(hermes_home)
    counts: dict[str, int] = defaultdict(int)
    for board, db_path in kanban_board_dbs(home):
        _poll_board(outbox, board, db_path, counts, home_mode)
    return dict(counts)


def _poll_board(outbox, board: str, db_path: Path, counts, home_mode) -> None:
    conn = open_sqlite_read_only(db_path)
    try:
        task_cols = sqlite_table_columns(conn, "tasks")
        task_select = ", ".join(
            sqlite_column_or_default(task_cols, name)
            for name in (
                "id",
                "status",
                "session_id",
                "priority",
                "assignee",
                "project_id",
                "idempotency_key",
                "block_kind",
                "consecutive_failures",
            )
        )
        tasks = {
            r["id"]: r
            for r in conn.execute(f"SELECT {task_select} FROM tasks")
        }
        runs = {
            r["id"]: r
            for r in conn.execute(
                "SELECT id, task_id, claim_lock, claim_expires, worker_pid, "
                "last_heartbeat_at, started_at, ended_at, outcome, profile, "
                "step_key FROM task_runs"
            )
        }
        events = conn.execute(
            "SELECT id, task_id, run_id, kind, created_at FROM task_events ORDER BY id"
        ).fetchall()
    finally:
        conn.close()

    rt = runtime_stamp("kanban", home_mode=home_mode)
    for ev in events:
        event_type = _KIND_EVENT.get(ev["kind"])
        if event_type is None:
            continue
        task = tasks.get(ev["task_id"])
        payload = _payload(board, ev, task, runs.get(ev["run_id"]))
        record = build_record(
            event_type=event_type,
            occurred_at=float(ev["created_at"] or 0.0),
            source="kanban:task_events",
            capture_method="poll:kanban:task_events",
            runtime=rt,
            correlation_id=ev["task_id"],
            session_id=task["session_id"] if task else None,
            payload=payload,
        )
        append_and_count(
            outbox, counts, record, dedup_key=f"kanban:{board}:event:{ev['id']}"
        )

    # Attempt history: one task.attempt_ended per ended run (outcome set). A
    # still-running run (outcome NULL) is skipped until a later poll sees it
    # closed; a closed run's outcome is final, so dedup on run_id is idempotent.
    for run_id in sorted(runs):
        run = runs[run_id]
        if run["outcome"] is None:
            continue
        task = tasks.get(run["task_id"])
        record = build_record(
            event_type="task.attempt_ended",
            occurred_at=float(run["ended_at"] or run["started_at"] or 0.0),
            source="kanban:task_runs",
            capture_method="poll:kanban:task_runs",
            runtime=rt,
            correlation_id=run["task_id"],
            session_id=task["session_id"] if task else None,
            payload=_attempt_payload(board, run),
        )
        append_and_count(
            outbox, counts, record, dedup_key=f"kanban:{board}:run:{run_id}"
        )


def _payload(board: str, ev, task, run) -> dict[str, Any]:
    """Plaintext coordination metadata for one task lifecycle event."""
    payload: dict[str, Any] = {
        "board": board,
        "task_id": ev["task_id"],
        # Keep the raw Hermes transition kind so observe can show the exact
        # lifecycle edge behind an overloaded event type (blocked vs scheduled,
        # gave_up vs block_loop_detected).
        "hermes_event_kind": ev["kind"],
    }
    if ev["run_id"] is not None:
        payload["run_id"] = ev["run_id"]
    _add_lease_fields(payload, run)
    if task is not None:
        payload["status"] = task["status"]
        for col in _TASK_META:
            value = task[col]
            if value is not None:
                payload[col] = value
    return payload


def _attempt_payload(board: str, run) -> dict[str, Any]:
    """Plaintext record of one ended attempt (a closed ``task_runs`` row)."""
    outcome = run["outcome"]
    payload: dict[str, Any] = {
        "board": board,
        "task_id": run["task_id"],
        "run_id": run["id"],
        "run_outcome": outcome,
        "attempt_disposition": _RUN_DISPOSITION.get(outcome, "unknown"),
    }
    for col in ("profile", "step_key"):
        if run[col] is not None:
            payload[col] = run[col]
    _add_lease_fields(payload, run)
    return payload


def _add_lease_fields(payload: dict[str, Any], run) -> None:
    """Copy the attempt's lease/holder fields off its ``task_runs`` row.

    These live on the run (written at claim, preserved per attempt), not the
    ``tasks`` row, which clears ``claim_lock``/``claim_expires`` between
    attempts. ``run`` is None for a task-level event with no owning run.
    """
    if run is None:
        return
    if run["claim_lock"] is not None:
        payload["holder"] = run["claim_lock"]
    if run["claim_expires"] is not None:
        payload["claim_expires"] = run["claim_expires"]
    if run["worker_pid"] is not None:
        payload["worker_pid"] = run["worker_pid"]
    if run["last_heartbeat_at"] is not None:
        payload["last_heartbeat_at"] = run["last_heartbeat_at"]
