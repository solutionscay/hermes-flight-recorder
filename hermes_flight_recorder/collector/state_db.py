"""Durable-state adapter for Hermes ``state.db``.

Poll the durable local store read-only and emit canonical envelope v1
records into the outbox. This is the authoritative reconstruction of what
happened, and the stream the reconciler diffs the lossy live hook against.

Grounded in a real probe session (see issue #5):

- A subagent is a ``sessions`` row with ``source='subagent'`` and
  ``parent_session_id`` -> ``subagent.child_spawned`` / ``subagent.completed``.
- ``messages.id`` is a global autoincrement, so an ``id > cursor`` poll is
  incremental.
- Tool status is inside the (encrypted) ``role='tool'`` result body; parse
  it best-effort before encrypting.
- Tokens and cost live in ``session_model_usage`` per (session, model, task).
- ``async_delegations`` has no ``child_session_id``; it stands on its own as
  ``delegation.dispatched``.

The adapter never writes to ``state.db``.
"""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any

from ._common import (
    build_record,
    read_home_mode,
    resolve_hermes_home,
    root_session,
    runtime_stamp,
)

_SESSION_COLS = (
    "id, source, parent_session_id, model, message_count, tool_call_count, "
    "input_tokens, output_tokens, estimated_cost_usd, started_at, ended_at, "
    "end_reason, profile_name, expiry_finalized"
)


def poll(outbox: Any, hermes_home: str | Path | None = None) -> dict[str, int]:
    """One read-only poll pass over ``state.db``. Returns per-type counts."""
    db_path = resolve_hermes_home(hermes_home) / "state.db"
    if not db_path.exists():
        raise FileNotFoundError(f"state.db not found at {db_path}")

    # Resolve the terminal home-mode policy once per poll, not per record.
    home_mode = read_home_mode(hermes_home)

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        sessions = conn.execute(f"SELECT {_SESSION_COLS} FROM sessions").fetchall()
        parent_map = {r["id"]: r["parent_session_id"] for r in sessions}
        profile_of = {r["id"]: (r["profile_name"] or "default") for r in sessions}

        counts: dict[str, int] = defaultdict(int)
        _poll_sessions(outbox, sessions, parent_map, counts, home_mode)
        _poll_messages(outbox, conn, parent_map, profile_of, counts, home_mode)
        _poll_model_usage(outbox, conn, parent_map, profile_of, counts, home_mode)
        _poll_delegations(outbox, conn, parent_map, profile_of, counts, home_mode)
        return dict(counts)
    finally:
        conn.close()


def _poll_sessions(outbox, sessions, parent_map, counts, home_mode) -> None:
    for r in sessions:
        sid = r["id"]
        is_sub = r["source"] == "subagent"
        kind = r["source"] or "unknown"
        profile = r["profile_name"] or "default"
        corr = root_session(sid, parent_map) or sid

        created = "subagent.child_spawned" if is_sub else "session.created"
        outbox.append(
            build_record(
                event_type=created,
                occurred_at=r["started_at"],
                source="state.db:sessions",
                capture_method="poll:state.db:sessions",
                runtime=runtime_stamp(kind, home_mode=home_mode),
                correlation_id=corr,
                session_id=sid,
                parent_session_id=r["parent_session_id"],
                profile=profile,
                payload={
                    "kind": kind,
                    # The originating surface: the verbatim sessions.source
                    # (cli | desktop | cron | subagent | a gateway platform
                    # name like telegram/discord ...). Open-ended by design —
                    # plugin platforms extend it — so it is not enum-validated.
                    "surface": kind,
                    "model": r["model"],
                    "message_count": r["message_count"],
                    "tool_call_count": r["tool_call_count"],
                },
            ),
            dedup_key=f"state.db:{created}:{sid}",
        )
        if outbox.last_append_created:
            counts[created] += 1

        # A NULL ended_at is a live session, not a crash. Emit no terminal;
        # the reconciler decides terminal-missing after a lifetime window.
        if r["ended_at"] is None:
            continue

        ended = "subagent.completed" if is_sub else "session.ended"
        # end_reason is not stable until expiry_finalized flips from 0.
        partial = r["expiry_finalized"] == 0
        outbox.append(
            build_record(
                event_type=ended,
                occurred_at=r["ended_at"],
                source="state.db:sessions",
                capture_method="poll:state.db:sessions",
                runtime=runtime_stamp(kind, home_mode=home_mode),
                correlation_id=corr,
                session_id=sid,
                parent_session_id=r["parent_session_id"],
                profile=profile,
                partial=partial,
                payload={
                    "kind": kind,
                    "end_reason": r["end_reason"],
                    "message_count": r["message_count"],
                    "tool_call_count": r["tool_call_count"],
                    "input_tokens": r["input_tokens"],
                    "output_tokens": r["output_tokens"],
                    "estimated_cost_usd": r["estimated_cost_usd"],
                },
            ),
            dedup_key=f"state.db:{ended}:{sid}",
        )
        if outbox.last_append_created:
            counts[ended] += 1


def _poll_messages(outbox, conn, parent_map, profile_of, counts, home_mode) -> None:
    cursor = int(outbox.get_cursor("state.db:messages") or 0)
    rows = conn.execute(
        "SELECT id, session_id, tool_name, tool_call_id, effect_disposition, "
        "content, timestamp FROM messages WHERE id > ? AND role='tool' ORDER BY id",
        (cursor,),
    ).fetchall()
    for r in rows:
        sid = r["session_id"]
        corr = root_session(sid, parent_map) or sid
        outbox.append(
            build_record(
                event_type="tool.call_completed",
                occurred_at=r["timestamp"] or 0.0,
                source="state.db:messages",
                capture_method="poll:state.db:messages",
                runtime=runtime_stamp("tool", home_mode=home_mode),
                correlation_id=corr,
                session_id=sid,
                profile=profile_of.get(sid, "default"),
                payload={
                    "tool_name": r["tool_name"],
                    "tool_call_id": r["tool_call_id"],
                    "effect_disposition": r["effect_disposition"],
                    "status": _derive_tool_status(r["content"]),
                    "message_row_id": r["id"],
                },
            ),
            content=r["content"],
            dedup_key=f"state.db:tool:{r['id']}",
        )
        if outbox.last_append_created:
            counts["tool.call_completed"] += 1

    max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM messages").fetchone()[0]
    outbox.set_cursor("state.db:messages", max_id)


def _poll_model_usage(outbox, conn, parent_map, profile_of, counts, home_mode) -> None:
    rows = conn.execute(
        "SELECT session_id, model, task, api_call_count, input_tokens, output_tokens, "
        "cache_read_tokens, reasoning_tokens, estimated_cost_usd, cost_status, last_seen "
        "FROM session_model_usage"
    ).fetchall()
    for r in rows:
        sid = r["session_id"]
        corr = root_session(sid, parent_map) or sid
        outbox.append(
            build_record(
                event_type="model.usage_recorded",
                occurred_at=r["last_seen"] or 0.0,
                source="state.db:session_model_usage",
                capture_method="poll:state.db:session_model_usage",
                runtime=runtime_stamp("model", home_mode=home_mode),
                correlation_id=corr,
                session_id=sid,
                profile=profile_of.get(sid, "default"),
                payload={
                    k: r[k]
                    for k in (
                        "model",
                        "task",
                        "api_call_count",
                        "input_tokens",
                        "output_tokens",
                        "cache_read_tokens",
                        "reasoning_tokens",
                        "estimated_cost_usd",
                        "cost_status",
                    )
                },
            ),
            dedup_key=f"state.db:usage:{sid}:{r['model']}:{r['task']}",
        )
        if outbox.last_append_created:
            counts["model.usage_recorded"] += 1


def _poll_delegations(outbox, conn, parent_map, profile_of, counts, home_mode) -> None:
    rows = conn.execute(
        "SELECT delegation_id, origin_session, parent_session_id, state, delivery_state, "
        "owner_pid, dispatched_at, event_json, result_json FROM async_delegations"
    ).fetchall()
    for r in rows:
        parent = r["parent_session_id"] or r["origin_session"]
        corr = root_session(parent, parent_map) or parent
        event = _safe_json(r["event_json"])  # is_batch lives here, not as a column
        outbox.append(
            build_record(
                event_type="delegation.dispatched",
                occurred_at=r["dispatched_at"] or 0.0,
                source="state.db:async_delegations",
                capture_method="poll:state.db:async_delegations",
                runtime=runtime_stamp("subagent", home_mode=home_mode),
                correlation_id=corr,
                session_id=parent,
                parent_session_id=r["parent_session_id"],
                profile=profile_of.get(parent, "default"),
                payload={
                    "delegation_id": r["delegation_id"],
                    "state": r["state"],
                    "delivery_state": r["delivery_state"],
                    "is_batch": bool(event.get("is_batch")),
                    "owner_pid": r["owner_pid"],
                },
            ),
            content=_delegation_content(event, r["result_json"]),
            dedup_key=f"state.db:deleg:{r['delegation_id']}",
        )
        if outbox.last_append_created:
            counts["delegation.dispatched"] += 1


def _derive_tool_status(content: str | None) -> str:
    """Best-effort status from the tool result body (before encryption)."""
    if not content:
        return "unknown"
    try:
        obj = json.loads(content)
    except (ValueError, TypeError):
        return "unknown"
    if not isinstance(obj, dict):
        return "unknown"
    if "exit_code" in obj:
        return "ok" if obj["exit_code"] == 0 else "error"
    if obj.get("error"):
        return "error"
    return str(obj.get("status") or "ok")


def _safe_json(text: str | None) -> dict[str, Any]:
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else {}
    except (ValueError, TypeError):
        return {}


def _delegation_content(event: dict[str, Any], result_json: str | None) -> str | None:
    parts: dict[str, Any] = {}
    if event.get("goal"):
        parts["goal"] = event["goal"]
    results = _safe_json(result_json).get("results")
    if isinstance(results, list):
        parts["summaries"] = [x.get("summary") for x in results if isinstance(x, dict)]
    return json.dumps(parts) if parts else None
