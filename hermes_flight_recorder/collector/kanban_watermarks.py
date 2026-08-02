"""Bounded source reads for the Kanban collector."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ._common import (
    open_sqlite_read_only,
    sqlite_select_chunked,
    sqlite_select_list,
    sqlite_table_exists,
)
from .watermark import Watermark, load_meta_json, save_meta_json

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
        save_meta_json(
            self.meta_store, _open_runs_meta_key(self.board), self.open_run_ids
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
    return watermark.bounded_rows(conn, "task_events", select, "id")


def _read_runs(
    conn, watermark: Watermark, events: list[Any], prior_open: set[Any]
) -> tuple[dict[Any, Any], list[Any], int]:
    if not sqlite_table_exists(conn, "task_runs"):
        return {}, [], watermark.read()
    select = sqlite_select_list(conn, "task_runs", _RUN_COLUMNS)
    changed, high_water = watermark.bounded_rows(conn, "task_runs", select, "id")
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
    return set(load_meta_json(outbox, _open_runs_meta_key(board), []))


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
    return sqlite_select_chunked(
        conn, f"SELECT {select} FROM {table} WHERE id IN ({{placeholders}})", ids
    )


__all__ = ["BoardBatch", "TASK_META_COLUMNS", "open_run_ids", "read_board"]
