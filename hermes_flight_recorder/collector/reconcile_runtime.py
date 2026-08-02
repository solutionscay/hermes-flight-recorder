"""Bounded runtime-health checks for the frequent reconcile pass."""

from __future__ import annotations

import os
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
from .reconcile_common import ReconcilePass, emit_finding, emit_terminal_missing
from .kanban_watermarks import open_run_ids
from .watermark import meta_float

_PID_RE = re.compile(r"PID (\d+)")

# gateway-starts.log grows one epoch line per gateway start, forever. The
# frequent reconcile pass only needs the last parseable line, so it reads a
# bounded tail instead of the whole file.
_GATEWAY_LOG_TAIL_BYTES = 8192


def detect_stale_task_leases(
    ctx: ReconcilePass, *, full: bool = False, boards=None
) -> None:
    for run in load_open_task_runs(ctx.home, ctx.outbox, full=full, boards=boards):
        if not lease_is_dead(run, ctx.when, ctx.config):
            continue
        board, run_id = run["board"], run["id"]
        emit_terminal_missing(
            ctx,
            occurred_at=ctx.when,
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
                "age_seconds": ctx.when - run["claim_expires"],
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


def load_open_task_runs(
    home: Path, outbox, *, full: bool = False, boards=None
) -> list[dict[str, Any]]:
    """Open task attempts per board. ``boards`` reuses a caller's board list.

    This layer owns the audit-scope decision: ``full=True`` (the audit) scans
    every open ``task_runs`` row; ``full=False`` (the frequent pass) reads
    only the runs the durable open-run watermark tracks, so a board whose
    open-run id set is empty — the common steady state — is skipped without
    opening its ``kanban.db`` at all.
    """
    runs: list[dict[str, Any]] = []
    if boards is None:
        boards = kanban_board_dbs(home)
    for board, db_path in boards:
        ids = None if full else open_run_ids(outbox, board)
        if ids is not None and not ids:
            continue  # no open attempts recorded — nothing to read
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


def detect_gateway_start_failed(ctx: ReconcilePass) -> None:
    state_path = gateway_state_path(ctx.home)
    if state_path.exists():
        data = load_json_dict(state_path)
        state = data.get("gateway_state")
        updated_at = to_epoch(data.get("updated_at")) or 0.0
        if state == "startup_failed":
            reason = data.get("exit_reason") or ""
            emit_finding(
                ctx,
                event_type="runtime.gateway_start_failed",
                occurred_at=updated_at or ctx.when,
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
                platform_time = (
                    to_epoch(info.get("updated_at")) or updated_at or ctx.when
                )
                emit_finding(
                    ctx,
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

    last_start = last_start_epoch(gateway_starts_log_path(ctx.home))
    if last_start is not None:
        emit_finding(
            ctx,
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
    """The last parseable epoch in the tail of ``gateway-starts.log``.

    Reads at most :data:`_GATEWAY_LOG_TAIL_BYTES` from the end of the file.
    When the read starts mid-file, the first line of the tail may be partial,
    so it is dropped; the "last parseable line" semantics hold within the
    tail. Returns None when the file is absent or unreadable.
    """
    try:
        with path.open("rb") as fh:
            size = fh.seek(0, os.SEEK_END)
            start = max(0, size - _GATEWAY_LOG_TAIL_BYTES)
            fh.seek(start)
            tail = fh.read()
    except OSError:
        return None
    lines = tail.decode("utf-8", "replace").splitlines()
    if start > 0:
        lines = lines[1:]  # a mid-file read starts on a partial line
    last: float | None = None
    for line in lines:
        try:
            last = float(line.strip())
        except ValueError:
            continue
    return last


def ticker_is_stale(ctx: ReconcilePass) -> bool:
    heartbeat = read_float(ticker_heartbeat_path(ctx.home))
    if heartbeat is None or ctx.when - heartbeat <= ctx.config.ticker_stale_after:
        return False
    emit_terminal_missing(
        ctx,
        occurred_at=ctx.when,
        correlation_id="cron:ticker",
        subject_type="cron_ticker",
        subject_id="cron:ticker",
        start_event_type="cron.ticker_heartbeat",
        expected_terminal_event_type="cron.ticker_heartbeat",
        details={
            "heartbeat": heartbeat,
            "staleness_seconds": ctx.when - heartbeat,
        },
        dedup_key=f"reconcile:ticker_stale:{int(heartbeat)}",
    )
    return True


def detect_capture_stale(ctx: ReconcilePass) -> None:
    last = meta_float(ctx.outbox, CAPTURE_HEARTBEAT_KEY)
    if last is None:
        return
    staleness = ctx.when - last
    if staleness <= ctx.config.capture_stale_after:
        return
    emit_finding(
        ctx,
        event_type="reconcile.capture_stale",
        occurred_at=ctx.when,
        correlation_id=ctx.outbox.installation_id,
        partial=True,
        payload={
            "last_success_at": last,
            "staleness_seconds": staleness,
            "threshold_seconds": ctx.config.capture_stale_after,
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
