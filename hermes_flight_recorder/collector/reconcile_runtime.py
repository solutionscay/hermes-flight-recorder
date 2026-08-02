"""Bounded runtime-health checks for the frequent reconcile pass."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from . import CAPTURE_HEARTBEAT_KEY
from ._common import (
    gateway_starts_log_path,
    gateway_state_path,
    kanban_board_dbs,
    load_json_dict,
    open_sqlite_read_only,
    read_float,
    sqlite_select_chunked,
    sqlite_select_list,
    sqlite_table_columns,
    ticker_heartbeat_path,
    to_epoch,
)
from .reconcile_common import emit_finding, emit_terminal_missing
from .kanban_watermarks import open_run_ids
from .watermark import meta_float

_PID_RE = re.compile(r"PID (\d+)")


def detect_stale_task_leases(
    outbox, home, counts, when, config, *, bounded: bool = True
) -> None:
    for run in load_open_task_runs(home, outbox=outbox if bounded else None):
        if not lease_is_dead(run, when, config):
            continue
        board, run_id = run["board"], run["id"]
        emit_terminal_missing(
            outbox,
            counts,
            occurred_at=when,
            correlation_id=run["task_id"],
            subject_type="task_run",
            subject_id=str(run_id),
            start_event_type="task.claimed",
            expected_terminal_event_type="task.attempt_ended",
            details={
                "board": board,
                "task_id": run["task_id"],
                "run_id": run_id,
                "holder": run["claim_lock"],
                "claim_expires": run["claim_expires"],
                "last_heartbeat_at": run["last_heartbeat_at"],
                "start_occurred_at": run["started_at"],
                "age_seconds": when - run["claim_expires"],
            },
            dedup_key=f"reconcile:terminal:task_run:{board}:{run_id}",
        )


def lease_is_dead(run: dict[str, Any], when: float, config: Any) -> bool:
    expires = run["claim_expires"]
    if expires is None or when - expires <= config.task_lease_grace:
        return False
    heartbeat = run["last_heartbeat_at"]
    return not (
        heartbeat is not None and when - heartbeat <= config.task_heartbeat_stale_after
    )


def load_open_task_runs(home: Path, *, outbox=None) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    for board, db_path in kanban_board_dbs(home):
        conn = open_sqlite_read_only(db_path)
        try:
            columns = sqlite_table_columns(conn, "task_runs")
            if "outcome" not in columns:
                rows = []
            else:
                select = sqlite_select_list(
                    conn,
                    "task_runs",
                    (
                        "id",
                        "task_id",
                        "claim_lock",
                        "claim_expires",
                        "worker_pid",
                        "last_heartbeat_at",
                        "started_at",
                    ),
                )
                ids = open_run_ids(outbox, board) if outbox is not None else None
                if ids is not None:
                    rows = _select_open_run_ids(conn, select, ids)
                else:
                    rows = conn.execute(
                        f"SELECT {select} FROM task_runs WHERE outcome IS NULL"
                    ).fetchall()
        finally:
            conn.close()
        runs.extend(
            {
                "board": board,
                "id": row["id"],
                "task_id": row["task_id"],
                "claim_lock": row["claim_lock"],
                "claim_expires": row["claim_expires"],
                "worker_pid": row["worker_pid"],
                "last_heartbeat_at": row["last_heartbeat_at"],
                "started_at": row["started_at"],
            }
            for row in rows
        )
    return runs


def _select_open_run_ids(conn, select: str, ids: set[Any]) -> list[Any]:
    return sqlite_select_chunked(
        conn,
        f"SELECT {select} FROM task_runs "
        "WHERE outcome IS NULL AND id IN ({placeholders})",
        ids,
    )


def detect_gateway_start_failed(outbox, home, counts, when) -> None:
    state_path = gateway_state_path(home)
    if state_path.exists():
        data = load_json_dict(state_path)
        state = data.get("gateway_state")
        updated_at = to_epoch(data.get("updated_at")) or 0.0
        if state == "startup_failed":
            reason = data.get("exit_reason") or ""
            emit_finding(
                outbox,
                counts,
                event_type="runtime.gateway_start_failed",
                occurred_at=updated_at or when,
                correlation_id="gateway",
                partial=True,
                payload={
                    "reason_class": classify_gateway_reason(reason),
                    "gateway_state": state,
                },
                content=reason or None,
                dedup_key=(
                    f"reconcile:gateway_start_failed:startup_failed:{int(updated_at)}"
                ),
            )
        platforms = data.get("platforms")
        if isinstance(platforms, dict):
            for name, info in platforms.items():
                if not isinstance(info, dict):
                    continue
                code = info.get("error_code") or ""
                message = info.get("error_message") or ""
                if not (code.endswith("_lock") or "already in use" in message):
                    continue
                pid = parse_pid(message)
                platform_time = to_epoch(info.get("updated_at")) or updated_at or when
                emit_finding(
                    outbox,
                    counts,
                    event_type="runtime.gateway_start_failed",
                    occurred_at=platform_time,
                    correlation_id=f"gateway:{name}",
                    partial=True,
                    payload={
                        "reason_class": "token_conflict",
                        "gateway_state": state,
                        "platform": name,
                        "error_code": code or None,
                        "conflicting_pid": pid,
                    },
                    content=message or None,
                    dedup_key=(
                        "reconcile:gateway_start_failed:token_conflict:"
                        f"{name}:{pid if pid is not None else 'unknown'}"
                    ),
                )
        return

    last_start = last_start_epoch(gateway_starts_log_path(home))
    if last_start is not None:
        emit_finding(
            outbox,
            counts,
            event_type="runtime.gateway_start_failed",
            occurred_at=last_start,
            correlation_id="gateway",
            partial=True,
            payload={"reason_class": "absent", "last_start_at": last_start},
            dedup_key=f"reconcile:gateway_start_failed:absent:{int(last_start)}",
        )


def classify_gateway_reason(text: str) -> str:
    value = (text or "").lower()
    if "already in use" in value or "_lock" in value or "conflict" in value:
        return "token_conflict"
    if "policy" in value:
        return "policy_open"
    if "config" in value or "invalid" in value or "not found" in value:
        return "config_invalid"
    return "unknown"


def parse_pid(text: str) -> int | None:
    match = _PID_RE.search(text or "")
    return int(match.group(1)) if match else None


def last_start_epoch(path: Path) -> float | None:
    if not path.exists():
        return None
    last: float | None = None
    try:
        for line in path.read_text().splitlines():
            try:
                last = float(line.strip())
            except ValueError:
                continue
    except OSError:
        return None
    return last


def ticker_is_stale(outbox, home, counts, when, config) -> bool:
    heartbeat = read_float(ticker_heartbeat_path(home))
    if heartbeat is None or when - heartbeat <= config.ticker_stale_after:
        return False
    emit_terminal_missing(
        outbox,
        counts,
        occurred_at=when,
        correlation_id="cron:ticker",
        subject_type="cron_ticker",
        subject_id="cron:ticker",
        start_event_type="cron.ticker_heartbeat",
        expected_terminal_event_type="cron.ticker_heartbeat",
        details={
            "heartbeat": heartbeat,
            "staleness_seconds": when - heartbeat,
        },
        dedup_key=f"reconcile:ticker_stale:{int(heartbeat)}",
    )
    return True


def detect_capture_stale(outbox, counts, when, config) -> None:
    last = meta_float(outbox, CAPTURE_HEARTBEAT_KEY)
    if last is None:
        return
    staleness = when - last
    if staleness <= config.capture_stale_after:
        return
    emit_finding(
        outbox,
        counts,
        event_type="reconcile.capture_stale",
        occurred_at=when,
        correlation_id=outbox.installation_id,
        partial=True,
        payload={
            "last_success_at": last,
            "staleness_seconds": staleness,
            "threshold_seconds": config.capture_stale_after,
        },
        dedup_key=f"reconcile:capture_stale:{int(last)}",
    )


__all__ = [
    "classify_gateway_reason",
    "detect_capture_stale",
    "detect_gateway_start_failed",
    "detect_stale_task_leases",
    "last_start_epoch",
    "lease_is_dead",
    "load_open_task_runs",
    "parse_pid",
    "ticker_is_stale",
]
