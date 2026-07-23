"""Durable-state adapter for Hermes ``state.db``.

Poll the durable local store read-only and emit canonical envelope v1
records into the outbox. This is the authoritative reconstruction of what
happened, and the stream the reconciler diffs the lossy live hook against.

Grounded in a real probe session (see issue #5):

- A subagent is a ``sessions`` row with ``source='subagent'`` and
  ``parent_session_id`` -> ``subagent.child_spawned`` / ``subagent.completed``.
- ``messages.id`` is a global autoincrement, so an ``id > cursor`` poll is
  incremental.
- Content-bearing ``user`` / ``assistant`` rows are the durable message and
  response bodies for hook-derived invocation windows.
- Tool status is inside the (encrypted) ``role='tool'`` result body; parse
  it best-effort before encrypting.
- Tokens and cost live in ``session_model_usage`` per (session, model, task).
- ``async_delegations`` has no ``child_session_id``; it stands on its own as
  ``delegation.dispatched``.

The adapter never writes to ``state.db``.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..envelope import SESSION_LIFECYCLE
from ._common import (
    append_and_count,
    build_record,
    open_sqlite_read_only,
    read_home_mode,
    resolve_hermes_home,
    root_session,
    runtime_stamp,
    safe_json_dict,
    occurred_before,
    sqlite_column_or_default,
    sqlite_table_columns,
    sqlite_table_exists,
    state_db_path,
)
from .recorder_config import CaptureConfig

_SESSION_COL_DEFAULTS = (
    ("id", "NULL"),
    ("source", "NULL"),
    ("parent_session_id", "NULL"),
    ("model", "NULL"),
    ("message_count", "0"),
    ("tool_call_count", "0"),
    ("input_tokens", "0"),
    ("output_tokens", "0"),
    ("estimated_cost_usd", "0"),
    ("started_at", "NULL"),
    ("ended_at", "NULL"),
    ("end_reason", "NULL"),
    ("profile_name", "NULL"),
    # Missing expiry_finalized means the Hermes schema predates that nuance;
    # treat ended sessions as stable instead of permanently partial.
    ("expiry_finalized", "1"),
)


def _session_select(columns: set[str]) -> str:
    return ", ".join(
        sqlite_column_or_default(columns, name, default)
        for name, default in _SESSION_COL_DEFAULTS
    )

_USAGE_COUNTERS = (
    "api_call_count",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "reasoning_tokens",
    "estimated_cost_usd",
)
_USAGE_STATE_VERSION = "delta-v1"
# v1 advanced one global cursor while selecting only role='tool'. A new
# cursor intentionally starts at zero once so upgrades backfill durable
# user/assistant content; existing tool rows hit their stable dedup keys.
_MESSAGE_CURSOR = "state.db:messages:v2"
# Hermes persists an incoming user message shortly before firing agent:start.
# Keep the skew narrow so an old, unrelated row cannot attach to a later turn.
_USER_START_SKEW_SECONDS = 30.0


@dataclass(frozen=True)
class _InvocationWindow:
    """One hook-derived turn window for one exact session."""

    invocation_id: str
    started_at: float
    ended_at: float | None


def poll(
    outbox: Any,
    hermes_home: str | Path | None = None,
    *,
    capture_config: CaptureConfig | None = None,
    since: float | None = None,
) -> dict[str, int]:
    """One read-only poll pass over ``state.db``. Returns per-type counts.

    ``since`` is the capture horizon: when set (``install --no-backfill``), rows
    whose activity predates it are skipped, so history is not backfilled. The
    session parent/profile maps are still built from every row so post-horizon
    activity in an older session keeps its attribution.
    """
    db_path = state_db_path(resolve_hermes_home(hermes_home))
    if not db_path.exists():
        raise FileNotFoundError(f"state.db not found at {db_path}")

    # Resolve configuration and the terminal home-mode policy once per poll,
    # not per record.
    capture = capture_config or CaptureConfig()
    home_mode = read_home_mode(hermes_home)

    conn = open_sqlite_read_only(db_path)
    try:
        session_cols = sqlite_table_columns(conn, "sessions")
        sessions = conn.execute(f"SELECT {_session_select(session_cols)} FROM sessions").fetchall()
        parent_map = {r["id"]: r["parent_session_id"] for r in sessions}
        profile_of = {r["id"]: (r["profile_name"] or "default") for r in sessions}
        invocation_windows = _invocation_windows(outbox)

        counts: dict[str, int] = defaultdict(int)
        _poll_sessions(outbox, sessions, parent_map, counts, home_mode, since)
        _poll_messages(
            outbox,
            conn,
            parent_map,
            profile_of,
            invocation_windows,
            counts,
            home_mode,
            capture,
            since,
        )
        _poll_model_usage(
            outbox, conn, parent_map, profile_of, invocation_windows, counts, home_mode, since
        )
        _poll_delegations(
            outbox, conn, parent_map, profile_of, invocation_windows, counts, home_mode, since
        )
        return dict(counts)
    finally:
        conn.close()


def _poll_sessions(outbox, sessions, parent_map, counts, home_mode, since=None) -> None:
    for r in sessions:
        if occurred_before(since, r["started_at"]):
            continue  # started before the capture horizon (no backfill)
        sid = r["id"]
        is_sub = r["source"] == "subagent"
        kind = r["source"] or "unknown"
        profile = r["profile_name"] or "default"
        corr = root_session(sid, parent_map) or sid

        created, ended = SESSION_LIFECYCLE["subagent" if is_sub else "session"]
        record = build_record(
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
        )
        append_and_count(outbox, counts, record, dedup_key=f"state.db:{created}:{sid}")

        # A NULL ended_at is a live session, not a crash. Emit no terminal;
        # the reconciler decides terminal-missing after a lifetime window.
        if r["ended_at"] is None:
            continue

        # end_reason is not stable until expiry_finalized flips from 0.
        partial = r["expiry_finalized"] == 0
        record = build_record(
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
        )
        append_and_count(outbox, counts, record, dedup_key=f"state.db:{ended}:{sid}")


def _poll_messages(
    outbox,
    conn,
    parent_map,
    profile_of,
    invocation_windows,
    counts,
    home_mode,
    capture_config,
    since=None,
) -> None:
    cursor = int(outbox.get_cursor(_MESSAGE_CURSOR) or 0)
    supported_roles = tuple(
        role
        for role in ("user", "assistant", "tool")
        if role in capture_config.message_roles
    )
    if supported_roles:
        placeholders = ",".join("?" for _ in supported_roles)
        columns = sqlite_table_columns(conn, "messages")
        select_cols = ", ".join(
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
            )
        )
        rows = conn.execute(
            f"SELECT {select_cols} FROM messages "
            f"WHERE id > ? AND role IN ({placeholders}) ORDER BY id",
            (cursor, *supported_roles),
        ).fetchall()
    else:
        rows = []

    last_seen_id = cursor
    for r in rows:
        # Advance only through this query's snapshot. A separate MAX(id)
        # query can see a row inserted after the snapshot and skip it forever.
        last_seen_id = max(last_seen_id, int(r["id"]))
        if occurred_before(since, r["timestamp"]):
            continue  # predates the capture horizon; cursor still advances
        sid = r["session_id"]
        corr = root_session(sid, parent_map) or sid
        role = r["role"]
        content = r["content"]
        # Hermes writes empty assistant rows to carry tool-call structure.
        # They are not assistant response content and already surface through
        # the corresponding role='tool' result rows.
        if role in ("user", "assistant") and not content:
            continue

        invocation_id = _infer_message_invocation(
            invocation_windows, sid, r["timestamp"], role
        )
        limited_content, content_metadata = _limit_content(
            content, capture_config.max_content_bytes
        )

        if role in ("user", "assistant"):
            is_intermediate = (
                role == "assistant" and r["finish_reason"] == "tool_calls"
            )
            payload = {
                "message_row_id": r["id"],
                "message_role": role,
                **content_metadata,
            }
            if role == "assistant":
                payload["message_phase"] = (
                    "intermediate" if is_intermediate else "final"
                )
                if r["finish_reason"] is not None:
                    payload["finish_reason"] = r["finish_reason"]
            if invocation_id is not None:
                payload["invocation_attribution"] = "inferred_from_session_window"
            record = build_record(
                event_type=(
                    "invocation.started"
                    if role == "user"
                    else "model.call_succeeded"
                    if is_intermediate
                    else "invocation.completed"
                ),
                occurred_at=r["timestamp"] or 0.0,
                source="state.db:messages",
                capture_method="poll:state.db:messages",
                runtime=runtime_stamp(role, home_mode=home_mode),
                correlation_id=corr,
                session_id=sid,
                invocation_id=invocation_id,
                profile=profile_of.get(sid, "default"),
                partial=content_metadata["content_truncated"],
                payload=payload,
            )
            append_and_count(
                outbox,
                counts,
                record,
                content=limited_content,
                dedup_key=f"state.db:{role}:{r['id']}",
            )
            continue

        payload = {
            "tool_name": r["tool_name"],
            "tool_call_id": r["tool_call_id"],
            "effect_disposition": r["effect_disposition"],
            "status": _derive_tool_status(content),
            "message_row_id": r["id"],
            **content_metadata,
        }
        if invocation_id is not None:
            payload["invocation_attribution"] = "inferred_from_session_window"
        record = build_record(
            event_type="tool.call_completed",
            occurred_at=r["timestamp"] or 0.0,
            source="state.db:messages",
            capture_method="poll:state.db:messages",
            runtime=runtime_stamp("tool", home_mode=home_mode),
            correlation_id=corr,
            session_id=sid,
            invocation_id=invocation_id,
            profile=profile_of.get(sid, "default"),
            partial=content_metadata["content_truncated"],
            payload=payload,
        )
        append_and_count(
            outbox,
            counts,
            record,
            content=limited_content,
            dedup_key=f"state.db:tool:{r['id']}",
        )

    outbox.set_cursor(_MESSAGE_CURSOR, last_seen_id)


def _poll_model_usage(
    outbox, conn, parent_map, profile_of, invocation_windows, counts, home_mode, since=None
) -> None:
    if not sqlite_table_exists(conn, "session_model_usage"):
        return
    columns = sqlite_table_columns(conn, "session_model_usage")
    select_cols = ", ".join(
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
    rows = conn.execute(
        f"SELECT {select_cols} FROM session_model_usage"
    ).fetchall()
    identities = [
        (str(row["session_id"]), str(row["model"] or ""), str(row["task"] or ""))
        for row in rows
    ]
    previous_states = _usage_states(outbox, identities)
    for r in rows:
        if occurred_before(since, r["last_seen"]):
            continue  # last touched before the capture horizon (no backfill)
        sid = str(r["session_id"])
        corr = root_session(sid, parent_map) or sid
        identity = (sid, str(r["model"] or ""), str(r["task"] or ""))
        current = {key: _number(r[key]) for key in _USAGE_COUNTERS}
        current["last_seen"] = _number(r["last_seen"])
        previous = previous_states.get(identity)
        if previous == current:
            outbox.set_meta(_usage_meta_key(identity), _serialize_usage_state(current))
            continue

        deltas: dict[str, int | float] = {}
        reset_fields: list[str] = []
        for key in _USAGE_COUNTERS:
            before = _number(previous.get(key)) if previous is not None else 0
            after = current[key]
            if after < before:
                # Hermes recreated/reset this cumulative row. Treat the new
                # absolute value as the first delta of the new counter epoch.
                deltas[key] = after
                reset_fields.append(key)
            else:
                deltas[key] = after - before

        invocation_id = _infer_invocation(invocation_windows, sid, r["last_seen"])
        payload = {
            "model": identity[1],
            "task": identity[2],
            "usage_semantics": "monotonic_delta",
            "cost_status": r["cost_status"],
            **deltas,
            **{f"cumulative_{key}": value for key, value in current.items() if key != "last_seen"},
        }
        if reset_fields:
            payload["counter_reset_fields"] = reset_fields
        if invocation_id is not None:
            payload["invocation_attribution"] = "inferred_from_session_window"
        record = build_record(
            event_type="model.usage_recorded",
            occurred_at=r["last_seen"] or 0.0,
            source="state.db:session_model_usage",
            capture_method="poll:state.db:session_model_usage",
            runtime=runtime_stamp("model", home_mode=home_mode),
            correlation_id=corr,
            session_id=sid,
            invocation_id=invocation_id,
            profile=profile_of.get(sid, "default"),
            payload=payload,
        )
        snapshot = json.dumps(current, sort_keys=True, separators=(",", ":"))
        snapshot_id = hashlib.sha256(snapshot.encode()).hexdigest()
        append_and_count(
            outbox,
            counts,
            record,
            dedup_key=f"state.db:usage:{_usage_key(identity)}:{snapshot_id}",
        )
        previous_states[identity] = current
        outbox.set_meta(_usage_meta_key(identity), _serialize_usage_state(current))

    outbox.set_meta("state.db:model-usage-state-version", _USAGE_STATE_VERSION)


def _poll_delegations(
    outbox, conn, parent_map, profile_of, invocation_windows, counts, home_mode, since=None
) -> None:
    if not sqlite_table_exists(conn, "async_delegations"):
        return
    rows = conn.execute(
        "SELECT delegation_id, origin_session, parent_session_id, state, delivery_state, "
        "owner_pid, dispatched_at, event_json, result_json FROM async_delegations"
    ).fetchall()
    for r in rows:
        if occurred_before(since, r["dispatched_at"]):
            continue  # dispatched before the capture horizon (no backfill)
        parent = r["parent_session_id"] or r["origin_session"]
        corr = root_session(parent, parent_map) or parent
        event = safe_json_dict(r["event_json"])  # is_batch lives here, not as a column
        invocation_id = _infer_invocation(invocation_windows, parent, r["dispatched_at"])
        payload = {
            "delegation_id": r["delegation_id"],
            "state": r["state"],
            "delivery_state": r["delivery_state"],
            "is_batch": bool(event.get("is_batch")),
            "owner_pid": r["owner_pid"],
        }
        if invocation_id is not None:
            payload["invocation_attribution"] = "inferred_from_session_window"
        record = build_record(
            event_type="delegation.dispatched",
            occurred_at=r["dispatched_at"] or 0.0,
            source="state.db:async_delegations",
            capture_method="poll:state.db:async_delegations",
            runtime=runtime_stamp("subagent", home_mode=home_mode),
            correlation_id=corr,
            session_id=parent,
            parent_session_id=r["parent_session_id"],
            invocation_id=invocation_id,
            profile=profile_of.get(parent, "default"),
            payload=payload,
        )
        append_and_count(
            outbox,
            counts,
            record,
            content=_delegation_content(event, r["result_json"]),
            dedup_key=f"state.db:deleg:{r['delegation_id']}",
        )


def _invocation_windows(outbox: Any) -> dict[str, list[_InvocationWindow]]:
    """Reconstruct exact-session invocation windows from durable hook events.

    Windows come from the outbox instead of transient drain state, so a tool
    poll in a later process or after the terminal hook was drained produces
    the same attribution. A later start caps an incomplete earlier turn.
    """
    starts: dict[str, list[tuple[float, str]]] = defaultdict(list)
    terminals: dict[str, float] = {}
    for event in outbox.iter_events():
        event_type = event.get("payload", {}).get("event_type")
        if event_type not in ("invocation.started", "invocation.completed"):
            continue
        # Durable message rows reuse invocation.* as their content carrier.
        # They must not recursively redefine the hook-derived time windows.
        if not str(event.get("capture_method", "")).startswith("hook:agent:"):
            continue
        sid = event.get("session_id")
        invocation_id = event.get("invocation_id")
        if not sid or not invocation_id:
            continue
        occurred_at = _number(event.get("occurred_at"))
        if event_type == "invocation.started":
            starts[sid].append((occurred_at, invocation_id))
        else:
            current = terminals.get(invocation_id)
            if current is None or occurred_at < current:
                terminals[invocation_id] = occurred_at

    result: dict[str, list[_InvocationWindow]] = {}
    for sid, session_starts in starts.items():
        ordered = sorted(session_starts)
        windows: list[_InvocationWindow] = []
        for index, (started_at, invocation_id) in enumerate(ordered):
            ended_at = terminals.get(invocation_id)
            next_start = ordered[index + 1][0] if index + 1 < len(ordered) else None
            if next_start is not None and (ended_at is None or ended_at >= next_start):
                ended_at = next_start
            windows.append(_InvocationWindow(invocation_id, started_at, ended_at))
        result[sid] = windows
    return result


def _infer_invocation(
    windows: dict[str, list[_InvocationWindow]], sid: str | None, occurred_at: Any
) -> str | None:
    """Return the containing invocation for this exact session and timestamp."""
    if not sid:
        return None
    timestamp = _number(occurred_at)
    if timestamp <= 0:
        return None
    candidate: _InvocationWindow | None = None
    for window in windows.get(sid, ()):
        if window.started_at > timestamp:
            break
        candidate = window
    if candidate is None:
        return None
    if candidate.ended_at is not None and timestamp > candidate.ended_at:
        return None
    return candidate.invocation_id


def _infer_message_invocation(
    windows: dict[str, list[_InvocationWindow]],
    sid: str | None,
    occurred_at: Any,
    role: str,
) -> str | None:
    """Attribute one durable message row to its hook-derived invocation.

    Assistant responses and tools land inside the start/end window. Hermes
    persists an incoming user row just before it emits ``agent:start``, so a
    user row may also bind to the nearest following start within a small,
    measured skew allowance.
    """
    timestamp = _number(occurred_at)
    if role == "user" and sid and timestamp > 0:
        for window in windows.get(sid, ()):
            skew = window.started_at - timestamp
            if skew < 0:
                continue
            if skew <= _USER_START_SKEW_SECONDS:
                return window.invocation_id
            break
    return _infer_invocation(windows, sid, timestamp)


def _limit_content(
    content: str | bytes | None, max_bytes: int
) -> tuple[str | bytes | None, dict[str, int | bool]]:
    """Bound encrypted content by UTF-8 bytes and report any truncation.

    Metadata makes the cap visible; plaintext is never decorated with a
    marker that could be mistaken for source content. String truncation
    stops before a partial UTF-8 code point.
    """
    if content is None:
        return None, {
            "content_original_bytes": 0,
            "content_captured_bytes": 0,
            "content_truncated": False,
        }
    raw = content.encode("utf-8") if isinstance(content, str) else content
    if len(raw) <= max_bytes:
        return content, {
            "content_original_bytes": len(raw),
            "content_captured_bytes": len(raw),
            "content_truncated": False,
        }

    limited_raw = raw[:max_bytes]
    if isinstance(content, str):
        limited: str | bytes = limited_raw.decode("utf-8", "ignore")
        captured_bytes = len(limited.encode("utf-8"))
    else:
        limited = limited_raw
        captured_bytes = len(limited_raw)
    return limited, {
        "content_original_bytes": len(raw),
        "content_captured_bytes": captured_bytes,
        "content_truncated": True,
    }


def _number(value: Any) -> int | float:
    """Normalize SQLite numeric values while keeping integral counters tidy."""
    if value is None:
        return 0
    number = float(value)
    return int(number) if number.is_integer() else number


def _usage_key(identity: tuple[str, str, str]) -> str:
    encoded = json.dumps(identity, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


def _usage_meta_key(identity: tuple[str, str, str]) -> str:
    return f"state.db:model-usage:{_usage_key(identity)}"


def _serialize_usage_state(state: dict[str, int | float]) -> str:
    return json.dumps(state, sort_keys=True, separators=(",", ":"))


def _usage_states(
    outbox: Any, identities: list[tuple[str, str, str]]
) -> dict[tuple[str, str, str], dict[str, int | float]]:
    """Load the last absolute model counters, including pre-#48 records.

    Older events stored cumulative fields directly. New delta events retain
    explicit ``cumulative_*`` companions, allowing this reconstruction to be
    crash-safe without mutating the append-only outbox.
    """
    if outbox.get_meta("state.db:model-usage-state-version") == _USAGE_STATE_VERSION:
        states: dict[tuple[str, str, str], dict[str, int | float]] = {}
        for identity in identities:
            raw = outbox.get_meta(_usage_meta_key(identity))
            if raw is None:
                continue
            try:
                parsed = json.loads(raw)
            except (TypeError, ValueError):
                continue
            if isinstance(parsed, dict):
                states[identity] = {
                    key: _number(parsed.get(key)) for key in (*_USAGE_COUNTERS, "last_seen")
                }
        return states

    states: dict[tuple[str, str, str], dict[str, int | float]] = {}
    for event in outbox.iter_events():
        payload = event.get("payload", {})
        if (
            payload.get("event_type") != "model.usage_recorded"
            or event.get("source") != "state.db:session_model_usage"
        ):
            continue
        sid = event.get("session_id")
        model = payload.get("model")
        task = payload.get("task")
        if not isinstance(sid, str) or not isinstance(model, str) or not isinstance(task, str):
            continue
        is_delta = payload.get("usage_semantics") == "monotonic_delta"
        state = {
            key: _number(payload.get(f"cumulative_{key}" if is_delta else key))
            for key in _USAGE_COUNTERS
        }
        state["last_seen"] = _number(event.get("occurred_at"))
        states[(sid, model, task)] = state
    return states


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


def _delegation_content(event: dict[str, Any], result_json: str | None) -> str | None:
    parts: dict[str, Any] = {}
    if event.get("goal"):
        parts["goal"] = event["goal"]
    results = safe_json_dict(result_json).get("results")
    if isinstance(results, list):
        parts["summaries"] = [x.get("summary") for x in results if isinstance(x, dict)]
    return json.dumps(parts) if parts else None
