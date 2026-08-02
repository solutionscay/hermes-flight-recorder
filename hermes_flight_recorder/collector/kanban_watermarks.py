"""Bounded source reads for the Kanban collector."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ._common import open_sqlite_read_only, sqlite_select_list, sqlite_table_exists
from .watermark import Watermark

BOARD_OVERLAP = 64
TASK_META_COLUMNS = (
    "priority",
    "assignee",
    "project_id",
    "idempotency_key",
    "block_kind",
    "consecutive_failures",
)
_EVENT_COLUMNS = ("id", "task_id", "run_id", "kind", "created_at")
_RUN_COLUMNS = (
    "id",
    "task_id",
    "claim_lock",
    "claim_expires",
    "worker_pid",
    "last_heartbeat_at",
    "started_at",
    "ended_at",
    "outcome",
    "profile",
    "step_key",
)
_TASK_COLUMNS = ("id", "status", "session_id", *TASK_META_COLUMNS)


@dataclass(frozen=True)
class BoardBatch:
    events: list[Any]
    runs: dict[Any, Any]
    tasks: dict[Any, Any]
    event_watermark: Watermark
    event_high_water: int
    run_watermark: Watermark
    run_high_water: int
    open_run_ids: tuple[Any, ...]
    meta_store: Any
    board: str

    def advance(self) -> None:
        self.event_watermark.advance(self.event_high_water)
        self.run_watermark.advance(self.run_high_water)
        self.meta_store.set_meta(
            _open_runs_meta_key(self.board),
            json.dumps(self.open_run_ids, separators=(",", ":")),
        )


def read_board(outbox, board: str, db_path: Path) -> BoardBatch | None:
    conn = open_sqlite_read_only(db_path)
    try:
        if not sqlite_table_exists(conn, "task_events"):
            return None
        event_watermark = Watermark(
            outbox, f"kanban:{board}:task-events:v1", overlap=BOARD_OVERLAP
        )
        run_watermark = Watermark(
            outbox, f"kanban:{board}:task-runs:v1", overlap=BOARD_OVERLAP
        )
        conn.execute("BEGIN")
        try:
            events, event_high_water = _read_events(conn, event_watermark)
            runs, changed_runs, run_high_water = _read_runs(
                conn, run_watermark, events, open_run_ids(outbox, board)
            )
            tasks = _read_tasks(conn, events, changed_runs)
        finally:
            conn.rollback()
    finally:
        conn.close()
    return BoardBatch(
        events,
        runs,
        tasks,
        event_watermark,
        event_high_water,
        run_watermark,
        run_high_water,
        tuple(sorted(run_id for run_id, run in runs.items() if run["outcome"] is None)),
        outbox,
        board,
    )


def _read_events(conn, watermark: Watermark) -> tuple[list[Any], int]:
    select = sqlite_select_list(conn, "task_events", _EVENT_COLUMNS)
    high_water = max(
        watermark.read(),
        int(conn.execute("SELECT COALESCE(MAX(id), 0) FROM task_events").fetchone()[0]),
    )
    rows = conn.execute(
        f"SELECT {select} FROM task_events WHERE id > ? AND id <= ? ORDER BY id",
        (watermark.lower_bound(), high_water),
    ).fetchall()
    return rows, high_water


def _read_runs(
    conn, watermark: Watermark, events: list[Any], prior_open: set[Any]
) -> tuple[dict[Any, Any], list[Any], int]:
    if not sqlite_table_exists(conn, "task_runs"):
        return {}, [], watermark.read()
    select = sqlite_select_list(conn, "task_runs", _RUN_COLUMNS)
    high_water = max(
        watermark.read(),
        int(conn.execute("SELECT COALESCE(MAX(id), 0) FROM task_runs").fetchone()[0]),
    )
    changed = conn.execute(
        f"SELECT {select} FROM task_runs WHERE id > ? AND id <= ? ORDER BY id",
        (watermark.lower_bound(), high_water),
    ).fetchall()
    runs = {row["id"]: row for row in changed}
    referenced = {
        event["run_id"]
        for event in events
        if event["run_id"] is not None and event["run_id"] not in runs
    }
    referenced.update(prior_open - runs.keys())
    for row in _select_ids(conn, "task_runs", select, referenced):
        runs[row["id"]] = row
    return runs, changed, high_water


def open_run_ids(outbox, board: str) -> set[Any]:
    raw = outbox.get_meta(_open_runs_meta_key(board))
    if raw is None:
        return set()
    try:
        values = json.loads(raw)
    except (TypeError, ValueError):
        return set()
    return set(values) if isinstance(values, list) else set()


def _open_runs_meta_key(board: str) -> str:
    return f"kanban:{board}:open-runs:v1"


def _read_tasks(conn, events: list[Any], changed_runs: list[Any]) -> dict[Any, Any]:
    if not sqlite_table_exists(conn, "tasks"):
        return {}
    task_ids = {event["task_id"] for event in events}
    task_ids.update(run["task_id"] for run in changed_runs)
    select = sqlite_select_list(conn, "tasks", _TASK_COLUMNS)
    return {row["id"]: row for row in _select_ids(conn, "tasks", select, task_ids)}


def _select_ids(conn, table: str, select: str, ids: set[Any]) -> list[Any]:
    values = list(ids)
    rows = []
    for start in range(0, len(values), 500):
        group = values[start : start + 500]
        placeholders = ",".join("?" for _ in group)
        rows.extend(
            conn.execute(
                f"SELECT {select} FROM {table} WHERE id IN ({placeholders})",
                group,
            ).fetchall()
        )
    return rows


__all__ = ["BoardBatch", "TASK_META_COLUMNS", "open_run_ids", "read_board"]
