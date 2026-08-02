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
    state_db_path,
)
from .recorder_config import CaptureConfig
from . import state_watermarks
from .invocation_watermarks import InvocationWindow
from .invocation_watermarks import read_invocation_windows

_TERMINAL_USAGE_FIELDS = (
    "api_call_count",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "reasoning_tokens",
    "estimated_cost_usd",
    "actual_cost_usd",
    "cost_status",
    "cost_source",
)

_USAGE_COUNTERS = (
    "api_call_count",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "reasoning_tokens",
    "estimated_cost_usd",
)
_USAGE_STATE_VERSION = state_watermarks.USAGE_STATE_VERSION
# Hermes persists an incoming user message shortly before firing agent:start.
# Keep the skew narrow so an old, unrelated row cannot attach to a later turn.
_USER_START_SKEW_SECONDS = 30.0
# Message rows appended per outbox transaction. Messages are the highest-volume
# source and a first backfill scans the whole history, so commit in bounded
# slices (matching the hook drain's chunk cadence) instead of building one
# giant transaction. A crash between slices re-scans into dedup hits — the
# same tolerance the per-row commits had.
_MESSAGE_BATCH_ROWS = 500


def poll(
    outbox: Any,
    hermes_home: str | Path | None = None,
    *,
    capture_config: CaptureConfig | None = None,
    knowledge_config: Any = None,
    since: float | None = None,
    home_mode: str | None = None,
) -> dict[str, int]:
    """One read-only poll pass over ``state.db``. Returns per-type counts.

    ``since`` is the capture horizon: when set (``install --no-backfill``), rows
    whose activity predates it are skipped, so history is not backfilled. The
    session parent/profile maps are still built from every row so post-horizon
    activity in an older session keeps its attribution.

    ``home_mode`` is the terminal home-mode policy resolved by the caller
    (``run_pass`` resolves it once per capture pass, issue #164); when None,
    a standalone call resolves it itself.
    """
    home = resolve_hermes_home(hermes_home)
    db_path = state_db_path(home)
    if not db_path.exists():
        raise FileNotFoundError(f"state.db not found at {db_path}")

    # Resolve configuration and the terminal home-mode policy once per poll,
    # not per record.
    capture = capture_config or CaptureConfig()
    if home_mode is None:
        home_mode = read_home_mode(hermes_home)

    conn = open_sqlite_read_only(db_path)
    try:
        roles = tuple(
            role
            for role in ("user", "assistant", "tool")
            if role in capture.message_roles
        )
        messages = state_watermarks.read_messages(outbox, conn, roles)
        delegations = state_watermarks.read_delegations(outbox, conn)
        subject_ids = {
            row["session_id"]
            for row in messages.rows
            if isinstance(row["session_id"], str)
        }
        subject_ids.update(
            value
            for row in delegations.rows
            for value in (row["origin_session"], row["parent_session_id"])
            if isinstance(value, str)
        )
        sessions = state_watermarks.read_sessions(
            outbox, conn, subject_ids
        )
        usage_ids = subject_ids | {
            row["id"]
            for row in sessions.emit_rows
            if isinstance(row["id"], str)
        }
        usage = state_watermarks.read_model_usage(outbox, conn, usage_ids)
        parent_map = {
            row["id"]: row["parent_session_id"] for row in sessions.context_rows
        }
        profile_of = {
            row["id"]: (row["profile_name"] or "default")
            for row in sessions.context_rows
        }
        invocation_windows = read_invocation_windows(outbox, usage_ids)

        # Each source's append loop plus its watermark advance runs in one
        # outbox transaction (issue #160): a crash rolls back the rows and the
        # cursor together, so a re-poll re-reads exactly the same range. The
        # source rows were all gathered above, so no batch holds the write
        # lock across a read of a Hermes store.
        counts: dict[str, int] = defaultdict(int)
        with outbox.batch():
            _poll_sessions(
                outbox, sessions.emit_rows, parent_map, counts, home_mode, since
            )
            sessions.advance()
        knowledge_rows: list[tuple[Any, str, str | None, str]] = []
        for start in range(0, len(messages.rows), _MESSAGE_BATCH_ROWS):
            with outbox.batch():
                _poll_messages(
                    outbox,
                    messages.rows[start : start + _MESSAGE_BATCH_ROWS],
                    parent_map,
                    profile_of,
                    invocation_windows,
                    counts,
                    home_mode,
                    capture,
                    knowledge_rows,
                    since,
                )
        # Knowledge mutation capture reads and hashes artifact files, so it
        # runs after the message batch commits instead of holding the write
        # lock across that work. The messages watermark advances only after
        # it finishes: a crash here re-scans the same rows next pass, the
        # message appends dedup, and the knowledge capture retries.
        for row, corr, invocation_id, profile in knowledge_rows:
            _capture_knowledge_mutation(
                outbox,
                conn,
                row,
                corr,
                invocation_id,
                profile,
                home_mode,
                counts,
                knowledge_config,
                home,
            )
        messages.advance()
        with outbox.batch():
            _poll_model_usage(
                outbox,
                usage.rows,
                parent_map,
                profile_of,
                invocation_windows,
                counts,
                home_mode,
                since,
            )
            usage.advance()
        with outbox.batch():
            _poll_delegations(
                outbox,
                delegations.rows,
                parent_map,
                profile_of,
                invocation_windows,
                counts,
                home_mode,
                since,
            )
            delegations.advance()
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

        # end_reason and the cumulative counters are not stable until
        # expiry_finalized flips from 0.  The live hook already supplies a
        # provisional terminal, so wait rather than consuming the durable
        # terminal's stable dedup key with a partial record.
        if r["expiry_finalized"] == 0:
            continue

        payload = {
            "kind": kind,
            "end_reason": r["end_reason"],
            "message_count": r["message_count"],
            "tool_call_count": r["tool_call_count"],
            "usage_semantics": "cumulative_total",
        }
        # These are the authoritative totals on a finalized durable session
        # row. Omit unavailable/NULL values so consumers can distinguish
        # unknown cost from an explicit zero.
        payload.update(
            {
                field: r[field]
                for field in _TERMINAL_USAGE_FIELDS
                if r[field] is not None
            }
        )

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
            partial=False,
            payload=payload,
        )
        append_and_count(outbox, counts, record, dedup_key=f"state.db:{ended}:{sid}")


def _poll_messages(
    outbox,
    rows,
    parent_map,
    profile_of,
    invocation_windows,
    counts,
    home_mode,
    capture_config,
    knowledge_rows,
    since=None,
) -> None:
    for r in rows:
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
        # Defer knowledge candidates: their capture reads and hashes artifact
        # files, which must not run while the batch holds the write lock.
        if r["tool_name"] in ("skill_manage", "memory"):
            knowledge_rows.append(
                (r, corr, invocation_id, profile_of.get(sid, "default"))
            )

def _poll_model_usage(
    outbox, rows, parent_map, profile_of, invocation_windows, counts, home_mode, since=None
) -> None:
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
    outbox, rows, parent_map, profile_of, invocation_windows, counts, home_mode, since=None
) -> None:
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


def _infer_invocation(
    windows: dict[str, list[InvocationWindow]], sid: str | None, occurred_at: Any
) -> str | None:
    """Return the containing invocation for this exact session and timestamp."""
    if not sid:
        return None
    timestamp = _number(occurred_at)
    if timestamp <= 0:
        return None
    candidate: InvocationWindow | None = None
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
    windows: dict[str, list[InvocationWindow]],
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


def _capture_knowledge_mutation(
    outbox: Any,
    conn: Any,
    tool_row: Any,
    correlation_id: str,
    invocation_id: str | None,
    profile: str,
    home_mode: str,
    counts: dict[str, int],
    knowledge_config: Any,
    hermes_home: Path,
) -> None:
    """Pair one knowledge tool result with its assistant tool-call arguments."""
    tool_name = tool_row["tool_name"]
    tool_call_id = tool_row["tool_call_id"]
    if (
        tool_name not in {"skill_manage", "memory"}
        or not isinstance(tool_call_id, str)
        or not tool_call_id
        or not _table_has_column(conn, "messages", "tool_calls")
    ):
        return
    result = safe_json_dict(tool_row["content"])
    if result.get("success") is not True or result.get("staged") is True:
        return

    paired = _find_tool_call(
        conn,
        session_id=tool_row["session_id"],
        before_row_id=int(tool_row["id"]),
        tool_call_id=tool_call_id,
    )
    if paired is None:
        return
    assistant_row_id, paired_name, arguments_text, arguments = paired
    if paired_name != tool_name:
        return

    from . import knowledge_mutation

    emitted = knowledge_mutation.capture(
        outbox,
        tool_name=tool_name,
        arguments_text=arguments_text,
        arguments=arguments,
        result=result,
        assistant_row_id=assistant_row_id,
        tool_result_row_id=int(tool_row["id"]),
        tool_call_id=tool_call_id,
        occurred_at=float(tool_row["timestamp"] or 0.0),
        home_mode=home_mode,
        correlation_id=correlation_id,
        session_id=tool_row["session_id"],
        invocation_id=invocation_id,
        profile=profile,
        knowledge_config=knowledge_config,
        hermes_home=hermes_home,
    )
    for event_type, count in emitted.items():
        counts[event_type] += count


def _find_tool_call(
    conn: Any,
    *,
    session_id: str,
    before_row_id: int,
    tool_call_id: str,
) -> tuple[int, str, str, dict[str, Any]] | None:
    """Find a tool call by its durable ID in earlier assistant rows."""
    rows = conn.execute(
        "SELECT id, tool_calls FROM messages "
        "WHERE session_id=? AND role='assistant' AND id < ? "
        "AND tool_calls IS NOT NULL ORDER BY id DESC",
        (session_id, before_row_id),
    ).fetchall()
    for row in rows:
        try:
            calls = json.loads(row["tool_calls"])
        except (TypeError, ValueError):
            continue
        if not isinstance(calls, list):
            continue
        for call in calls:
            if not isinstance(call, dict):
                continue
            call_id = call.get("id") or call.get("call_id")
            if call_id != tool_call_id:
                continue
            function = call.get("function")
            if not isinstance(function, dict):
                return None
            name = function.get("name")
            raw_arguments = function.get("arguments")
            if not isinstance(name, str):
                return None
            if isinstance(raw_arguments, str):
                arguments_text = raw_arguments
                try:
                    arguments = json.loads(raw_arguments)
                except (TypeError, ValueError):
                    return None
            elif isinstance(raw_arguments, dict):
                arguments = raw_arguments
                arguments_text = json.dumps(
                    raw_arguments, ensure_ascii=False, separators=(",", ":")
                )
            else:
                return None
            if not isinstance(arguments, dict):
                return None
            return int(row["id"]), name, arguments_text, arguments
    return None


def _table_has_column(conn: Any, table: str, column: str) -> bool:
    return any(row[1] == column for row in conn.execute(f"PRAGMA table_info({table})"))


def _delegation_content(event: dict[str, Any], result_json: str | None) -> str | None:
    parts: dict[str, Any] = {}
    if event.get("goal"):
        parts["goal"] = event["goal"]
    results = safe_json_dict(result_json).get("results")
    if isinstance(results, list):
        parts["summaries"] = [x.get("summary") for x in results if isinstance(x, dict)]
    return json.dumps(parts) if parts else None
