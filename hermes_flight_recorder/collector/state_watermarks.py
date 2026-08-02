"""Bounded source reads for the state database collector."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ._common import (
    sqlite_column_or_default,
    sqlite_select_chunked,
    sqlite_table_columns,
    sqlite_table_exists,
)
from .watermark import Watermark, load_meta_json, save_meta_json

MESSAGE_WATERMARK = "state.db:messages:v2"
MESSAGE_OVERLAP = 32
SESSION_WATERMARK = "state.db:sessions:v1"
SESSION_OVERLAP = 64
USAGE_STATE_VERSION = "delta-v1"
DELEGATION_WATERMARK = "state.db:delegations:v1"
DELEGATION_OVERLAP = 32
_OPEN_SESSIONS_META = "state.db:open-sessions:v1"

_SESSION_COLUMNS = (
    ("id", "NULL"),
    ("source", "NULL"),
    ("parent_session_id", "NULL"),
    ("model", "NULL"),
    ("message_count", "0"),
    ("tool_call_count", "0"),
    ("api_call_count", "NULL"),
    ("input_tokens", "NULL"),
    ("output_tokens", "NULL"),
    ("cache_read_tokens", "NULL"),
    ("cache_write_tokens", "NULL"),
    ("reasoning_tokens", "NULL"),
    ("estimated_cost_usd", "NULL"),
    ("actual_cost_usd", "NULL"),
    ("cost_status", "NULL"),
    ("cost_source", "NULL"),
    ("started_at", "NULL"),
    ("ended_at", "NULL"),
    ("end_reason", "NULL"),
    ("profile_name", "NULL"),
    ("expiry_finalized", "1"),
)


@dataclass(frozen=True)
class RowBatch:
    rows: list[Any]
    watermark: Watermark | None = None
    high_water: int = 0

    def advance(self) -> None:
        if self.watermark is not None:
            self.watermark.advance(self.high_water)


@dataclass(frozen=True)
class SessionBatch:
    emit_rows: list[Any]
    context_rows: list[Any]
    watermark: Watermark
    high_water: int
    open_ids: tuple[str, ...]

    def advance(self) -> None:
        self.watermark.advance(self.high_water)
        save_meta_json(self.watermark.store, _OPEN_SESSIONS_META, self.open_ids)


def read_messages(outbox, conn, roles: tuple[str, ...]) -> RowBatch:
    watermark = Watermark(outbox, MESSAGE_WATERMARK, overlap=MESSAGE_OVERLAP)
    columns = sqlite_table_columns(conn, "messages")
    select = ", ".join(
        sqlite_column_or_default(columns, name)
        for name in (
            "id",
            "session_id",
            "role",
            "tool_name",
            "tool_call_id",
            "effect_disposition",
            "content",
            "timestamp",
            "finish_reason",
            "tool_calls",
        )
    )
    conn.execute("BEGIN")
    try:
        high_water = watermark.high_water(conn, "messages", "id")
        if roles:
            placeholders = ",".join("?" for _ in roles)
            rows = conn.execute(
                f"SELECT {select} FROM messages "
                f"WHERE id > ? AND id <= ? AND role IN ({placeholders}) ORDER BY id",
                (watermark.lower_bound(), high_water, *roles),
            ).fetchall()
        else:
            rows = []
    finally:
        conn.rollback()
    return RowBatch(rows, watermark, high_water)


def read_model_usage(outbox, conn, session_ids: set[str]) -> RowBatch:
    if not sqlite_table_exists(conn, "session_model_usage"):
        return RowBatch([])
    columns = sqlite_table_columns(conn, "session_model_usage")
    select = ", ".join(
        sqlite_column_or_default(columns, name, default)
        for name, default in (
            ("session_id", "NULL"),
            ("model", "NULL"),
            ("task", "NULL"),
            ("api_call_count", "0"),
            ("input_tokens", "0"),
            ("output_tokens", "0"),
            ("cache_read_tokens", "0"),
            ("reasoning_tokens", "0"),
            ("estimated_cost_usd", "0"),
            ("cost_status", "NULL"),
            ("last_seen", "0"),
        )
    )
    if outbox.get_meta("state.db:model-usage-state-version") != USAGE_STATE_VERSION:
        return RowBatch(
            conn.execute(f"SELECT {select} FROM session_model_usage").fetchall()
        )
    rows = sqlite_select_chunked(
        conn,
        f"SELECT {select} FROM session_model_usage "
        "WHERE session_id IN ({placeholders}) ORDER BY session_id, last_seen",
        session_ids,
    )
    return RowBatch(rows)


def read_delegations(outbox, conn) -> RowBatch:
    if not sqlite_table_exists(conn, "async_delegations"):
        return RowBatch([])
    watermark = Watermark(outbox, DELEGATION_WATERMARK, overlap=DELEGATION_OVERLAP)
    rows, high_water = watermark.bounded_rows(
        conn,
        "async_delegations",
        "delegation_id, origin_session, parent_session_id, state, "
        "delivery_state, owner_pid, dispatched_at, event_json, result_json",
        "rowid",
    )
    return RowBatch(rows, watermark, high_water)


def read_sessions(outbox, conn, subject_ids: set[str]) -> SessionBatch:
    columns = sqlite_table_columns(conn, "sessions")
    select = ", ".join(
        sqlite_column_or_default(columns, name, default)
        for name, default in _SESSION_COLUMNS
    )
    watermark = Watermark(outbox, SESSION_WATERMARK, overlap=SESSION_OVERLAP)
    recent, high_water = watermark.bounded_rows(conn, "sessions", select, "rowid")
    open_ids = _read_open_session_ids(outbox)
    context = {row["id"]: row for row in recent if row["id"] is not None}
    needed = set(subject_ids) | open_ids
    _read_session_ids(conn, select, needed - context.keys(), context)

    parents = {
        row["parent_session_id"]
        for row in context.values()
        if row["parent_session_id"] is not None
    }
    while parents - context.keys():
        missing = parents - context.keys()
        _read_session_ids(conn, select, missing, context)
        next_parents = {
            context[sid]["parent_session_id"]
            for sid in missing
            if sid in context and context[sid]["parent_session_id"] is not None
        }
        if not next_parents - context.keys():
            break
        parents.update(next_parents)

    emit = {row["id"]: row for row in recent if row["id"] is not None}
    emit.update((sid, context[sid]) for sid in open_ids if sid in context)
    next_open = tuple(
        sorted(
            sid
            for sid, row in emit.items()
            if row["ended_at"] is None or row["expiry_finalized"] == 0
        )
    )
    return SessionBatch(
        list(emit.values()),
        list(context.values()),
        watermark,
        high_water,
        next_open,
    )


def _read_open_session_ids(outbox) -> set[str]:
    value = load_meta_json(outbox, _OPEN_SESSIONS_META, [])
    return {item for item in value if isinstance(item, str) and item}


def _read_session_ids(conn, select: str, ids: set[str], rows: dict[str, Any]) -> None:
    for row in sqlite_select_chunked(
        conn, f"SELECT {select} FROM sessions WHERE id IN ({{placeholders}})", ids
    ):
        if row["id"] is not None:
            rows[row["id"]] = row


__all__ = [
    "DELEGATION_WATERMARK",
    "MESSAGE_WATERMARK",
    "SESSION_WATERMARK",
    "USAGE_STATE_VERSION",
    "RowBatch",
    "SessionBatch",
    "read_delegations",
    "read_messages",
    "read_model_usage",
    "read_sessions",
]
