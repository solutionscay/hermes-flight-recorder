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

Attempt-level run outcomes (``crashed`` / ``timed_out`` / ``reclaimed`` …) and
the full ``task_runs`` history belong to a later adapter (#52). Sensitive text
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
)

# Hermes task_events.kind -> the reserved task.* event it maps to. Kinds absent
# here (assigned, commented, heartbeat, claim_extended, spawned, promoted,
# reclaimed, stale, timed_out, crashed, unblocked, archived, …) are not task
# lifecycle transitions in the five-event contract and are skipped: run-level
# outcomes are #52's job, and disposal/no-op transitions have no reserved event.
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
        tasks = {
            r["id"]: r
            for r in conn.execute(
                "SELECT id, status, session_id, priority, assignee, project_id, "
                "idempotency_key, block_kind, consecutive_failures FROM tasks"
            )
        }
        runs = {
            r["id"]: r
            for r in conn.execute(
                "SELECT id, claim_lock, claim_expires, worker_pid, "
                "last_heartbeat_at FROM task_runs"
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
    # Lease fields come from the claiming attempt's run row (immutable per run),
    # not the tasks row (which clears claim_lock/claim_expires between attempts).
    if run is not None:
        if run["claim_lock"] is not None:
            payload["holder"] = run["claim_lock"]
        if run["claim_expires"] is not None:
            payload["claim_expires"] = run["claim_expires"]
        if run["worker_pid"] is not None:
            payload["worker_pid"] = run["worker_pid"]
        if run["last_heartbeat_at"] is not None:
            payload["last_heartbeat_at"] = run["last_heartbeat_at"]
    if task is not None:
        payload["status"] = task["status"]
        for col in _TASK_META:
            value = task[col]
            if value is not None:
                payload[col] = value
    return payload
