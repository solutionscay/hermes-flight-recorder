"""Flight Recorder-side drain for the live hook spool.

The in-gateway spooler appends one JSON line per Hermes lifecycle event to
``hook-spool.jsonl``. This module runs in the Flight Recorder environment and turns
those raw lines into canonical envelope v1 records: it maps each event,
assigns the ``producer_sequence`` via the outbox, and appends with a dedup
key. Invocation hooks are metadata-only; complete user/assistant content is
captured later from ``state.db``.

Durability model (issue #4): at-least-once with dedup at the drain. The outbox
stores the read offset, file identity, and generation. The generation makes
an offset unique after replacement, truncation, or compaction. On a Flight
Recorder stop between an append and the cursor commit, the next drain re-reads
the same generation and deduplicates it. A partial trailing line stays for the
next drain.

Fields the hook context does not carry are synthesized here, best-effort:
``invocation_id`` (minted on ``agent:start`` from the line offset, then
paired to the matching ``agent:end`` via a per-session id stashed in outbox
meta — see ``_pair_invocation_id``, issue #23), ``session_id`` on
session-end (recovered from a ``session_key`` -> ``session_id`` map built
from session-start within this drain), ``correlation_id``, and defaulted
``profile``/``tenant``. Such records are marked ``partial`` where the issue
requires it; the state adapter and reconciler supply the authoritative form.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .._common import append_and_count, build_record, gateway_runtime_stamp, runtime_stamp
from . import CURSOR_NAME, MAX_SPOOL_BYTES, SPOOL_FILENAME

_GENERATION_META = "hook-spool:generation"
_IDENTITY_META = "hook-spool:identity"
_SEGMENT_PREFIX = f"{SPOOL_FILENAME}.segment."
# Spool lines per outbox transaction. One commit per chunk instead of one per
# line, while a huge backlog never builds one giant transaction (or memory
# buffer). A crash between chunk commits re-reads from the stored cursor and
# the per-line dedup keys absorb the replay.
_DRAIN_CHUNK_LINES = 500


def drain(outbox: Any, flight_recorder_home: str | Path | None = None) -> dict[str, int]:
    """Drain new spool lines into the outbox. Returns per-type counts.

    ``flight_recorder_home`` defaults to the outbox's own home, so the spool and the
    outbox always align. Only newly-created rows are counted (a dedup hit on
    re-drain does not count).
    """
    home = Path(flight_recorder_home) if flight_recorder_home else Path(outbox.path).parent
    spool = home / SPOOL_FILENAME
    counts: dict[str, int] = defaultdict(int)
    session_ids: dict[str, str] = {}
    _drain_segments(outbox, home, counts, session_ids)
    if not spool.exists():
        return dict(counts)

    stat = spool.stat()
    identity = f"{stat.st_dev}:{stat.st_ino}"
    cursor = int(outbox.get_cursor(CURSOR_NAME) or 0)
    saved_identity = outbox.get_meta(_IDENTITY_META)
    generation = outbox.get_meta(_GENERATION_META)

    if saved_identity is None:
        if stat.st_size < cursor:
            cursor = 0
            generation = uuid.uuid4().hex
            outbox.set_cursor(CURSOR_NAME, cursor)
        else:
            # Preserve the old offset-only key during the first pass after an
            # upgrade. New installations start with a generation key.
            generation = generation or ("legacy" if cursor else uuid.uuid4().hex)
        outbox.set_meta(_IDENTITY_META, identity)
        outbox.set_meta(_GENERATION_META, generation)
    elif saved_identity != identity or stat.st_size < cursor:
        # Replacement changes the inode. Truncation can keep the inode but
        # moves the end before the cursor. Both cases start a new generation.
        cursor = 0
        generation = uuid.uuid4().hex
        outbox.set_cursor(CURSOR_NAME, cursor)
        outbox.set_meta(_IDENTITY_META, identity)
        outbox.set_meta(_GENERATION_META, generation)
    elif generation is None:
        generation = uuid.uuid4().hex
        outbox.set_meta(_GENERATION_META, generation)

    if stat.st_size == cursor:
        if cursor >= MAX_SPOOL_BYTES:
            _rotate_consumed_spool(outbox, spool, cursor, generation)
        return dict(counts)

    consumed, _complete = _drain_path(
        outbox, spool, cursor, generation, counts, session_ids
    )
    cursor += consumed
    outbox.set_cursor(CURSOR_NAME, cursor)
    # Rotate after a size-limited generation is drained. This condition does
    # not need a quiet pass, so continuous writes cannot pin the active spool
    # above the cap.
    if cursor >= MAX_SPOOL_BYTES:
        _rotate_consumed_spool(outbox, spool, cursor, generation)
    return dict(counts)


def _drain_path(
    outbox: Any,
    path: Path,
    cursor: int,
    generation: str,
    counts: dict[str, int],
    session_ids: dict[str, str],
) -> tuple[int, bool]:
    """Drain complete lines from one stable spool generation."""
    consumed = 0
    complete = True
    # Read one bounded chunk of complete lines at a time and commit it as one
    # outbox transaction, so a large backlog neither sits in memory whole nor
    # pays one write transaction per line. The file read stays outside the
    # transaction; only the append loop holds the write lock. A line without
    # a trailing newline (only possible at EOF) is a partial write; leave it
    # for the next drain.
    with open(path, "rb") as fh:
        fh.seek(cursor)
        while complete:
            chunk: list[tuple[int, bytes]] = []
            for raw in fh:
                if not raw.endswith(b"\n"):
                    complete = False
                    break
                chunk.append((cursor + consumed, raw))
                consumed += len(raw)
                if len(chunk) >= _DRAIN_CHUNK_LINES:
                    break
            if not chunk:
                break
            with outbox.batch():
                for line_offset, raw in chunk:
                    _drain_line(
                        outbox, raw, line_offset, generation, counts, session_ids
                    )
    return consumed, complete


def _drain_line(
    outbox: Any,
    raw: bytes,
    line_offset: int,
    generation: str,
    counts: dict[str, int],
    session_ids: dict[str, str],
) -> None:
    """Map one complete spool line and append it with its stable dedup key."""
    text = raw.decode("utf-8", "replace").strip()
    if not text:
        return
    try:
        obj = json.loads(text)
    except ValueError:
        return  # skip an undecodable line rather than fail the drain
    mapped = _map_event(obj, line_offset, session_ids, outbox)
    if mapped is None:
        return
    record, content = mapped
    dedup_key = (
        f"hook-spool:{line_offset}"
        if generation == "legacy"
        else f"hook-spool:{generation}:{line_offset}"
    )
    append_and_count(
        outbox, counts, record, content=content,
        dedup_key=dedup_key,
    )


def _rotate_consumed_spool(
    outbox: Any, spool: Path, cursor: int, generation: str
) -> None:
    """Move a consumed spool aside and start a new offset generation."""
    segment = spool.with_name(
        f"{_SEGMENT_PREFIX}{generation}.{cursor}.{uuid.uuid4().hex}"
    )
    spool.replace(segment)
    outbox.set_cursor(CURSOR_NAME, 0)
    outbox.delete_meta(_IDENTITY_META)
    outbox.delete_meta(_GENERATION_META)


def _drain_segments(
    outbox: Any,
    home: Path,
    counts: dict[str, int],
    session_ids: dict[str, str],
) -> None:
    """Finish and remove spool generations that compaction moved aside."""
    for segment in sorted(home.glob(f"{_SEGMENT_PREFIX}*")):
        suffix = segment.name[len(_SEGMENT_PREFIX):]
        try:
            generation, cursor_text, _nonce = suffix.split(".", 2)
            cursor = int(cursor_text)
        except (TypeError, ValueError):
            continue
        _consumed, complete = _drain_path(
            outbox, segment, cursor, generation, counts, session_ids
        )
        if complete:
            segment.unlink()


def _clean(payload: dict[str, Any]) -> dict[str, Any]:
    """Drop None-valued keys so payloads stay tidy."""
    return {k: v for k, v in payload.items() if v is not None}


def _gateway_id(outbox: Any, occurred_at: float, offset: Any) -> str:
    """A stable, token-free per-boot gateway id.

    Hermes' authoritative process identity (pid, /proc start-time) is not
    carried in the gateway:startup hook context, and reading gateway.pid at
    drain time is racy for historical spool lines. So derive a deterministic
    id from stable line inputs: the installation, the event time, and the
    line offset. Re-draining the same line reproduces the id (idempotent);
    the offset guards against a same-second restart or a missing timestamp
    collapsing distinct boots to one id.
    """
    seed = f"{outbox.installation_id}:{int(occurred_at)}:{offset}"
    return "gw-" + hashlib.sha256(seed.encode()).hexdigest()[:16]


def _pair_invocation_id(outbox: Any, sid: str | None, offset: Any, is_start: bool) -> str:
    """Pair an ``agent:start`` with its ``agent:end`` via outbox meta.

    A new id is minted on start and stashed under a session-scoped meta key;
    the matching end reuses and clears it, so start and end share one
    ``invocation_id`` even when they land in separate drains (issue #23).
    Without a session id, or when no pending start is on record, an id is
    synthesized fresh from the line offset and stays unpaired — for a truly
    lost end, that is exactly the reconciler's signal to fire.
    """
    if sid is None:
        return f"unknown:hook:{offset}"
    key = f"hook-invocation:{sid}"
    if is_start:
        invocation_id = f"{sid}:hook:{offset}"
        outbox.set_meta(key, invocation_id)
        return invocation_id
    pending = outbox.get_meta(key)
    if pending is not None:
        outbox.delete_meta(key)
        return pending
    return f"{sid}:hook:{offset}"


@dataclass(frozen=True)
class _MapInput:
    event_type: str
    context: dict[str, Any]
    occurred_at: float
    source: str
    capture_method: str
    runtime: dict[str, Any]
    offset: Any
    session_ids: dict[str, str]
    outbox: Any


def _map_event(
    obj: dict[str, Any], offset: Any, session_ids: dict[str, str], outbox: Any
) -> tuple[dict[str, Any], str | None] | None:
    """Map one raw spool record to an envelope record and optional content."""
    event_type = obj.get("event_type")
    if not isinstance(event_type, str):
        return None
    context = obj.get("context") or {}
    if not isinstance(context, dict):
        context = {}
    base = event_type.split(":", 1)[0]
    value = _MapInput(
        event_type=event_type,
        context=context,
        occurred_at=float(obj.get("captured_at") or 0.0),
        source=f"hook:{base}",
        capture_method=f"hook:{event_type}",
        runtime=runtime_stamp(base),
        offset=offset,
        session_ids=session_ids,
        outbox=outbox,
    )
    handler = _EVENT_MAPPERS.get(event_type)
    return handler(value) if handler is not None else None


def _map_gateway_start(value: _MapInput):
    channels = value.context.get("platforms")
    return (
        build_record(
            event_type="runtime.gateway_started",
            occurred_at=value.occurred_at,
            source=value.source,
            capture_method=value.capture_method,
            runtime=gateway_runtime_stamp(
                channels=channels,
                gateway_id=_gateway_id(
                    value.outbox, value.occurred_at, value.offset
                ),
            ),
            correlation_id=f"gateway:{value.offset}",
            payload=_clean({"platforms": channels}),
        ),
        None,
    )


def _map_session_start(value: _MapInput):
    session_id = value.context.get("session_id")
    session_key = value.context.get("session_key")
    if session_id and session_key:
        value.session_ids[session_key] = session_id
    return (
        build_record(
            event_type="session.created",
            occurred_at=value.occurred_at,
            source=value.source,
            capture_method=value.capture_method,
            runtime=value.runtime,
            correlation_id=session_id or session_key or f"hook:{value.offset}",
            session_id=session_id,
            session_key=session_key,
            payload=_clean(
                {
                    "platform": value.context.get("platform"),
                    "surface": value.context.get("platform") or None,
                    "user_id": value.context.get("user_id"),
                }
            ),
        ),
        None,
    )


def _map_session_end(value: _MapInput):
    session_key = value.context.get("session_key")
    session_id = value.session_ids.get(session_key) if session_key else None
    reason = "reset" if value.event_type == "session:reset" else "end"
    return (
        build_record(
            event_type="session.ended",
            occurred_at=value.occurred_at,
            source=value.source,
            capture_method=value.capture_method,
            runtime=value.runtime,
            correlation_id=session_id or session_key or f"hook:{value.offset}",
            session_id=session_id,
            session_key=session_key,
            partial=True,
            payload=_clean(
                {
                    "platform": value.context.get("platform"),
                    "user_id": value.context.get("user_id"),
                    "reason": reason,
                }
            ),
        ),
        None,
    )


def _map_agent(value: _MapInput):
    session_id = value.context.get("session_id")
    is_start = value.event_type == "agent:start"
    payload = {
        "platform": value.context.get("platform"),
        "user_id": value.context.get("user_id"),
        "chat_type": value.context.get("chat_type"),
    }
    if is_start:
        payload["thread_id"] = value.context.get("thread_id")
        payload["chat_id"] = value.context.get("chat_id")
    return (
        build_record(
            event_type="invocation.started" if is_start else "invocation.completed",
            occurred_at=value.occurred_at,
            source=value.source,
            capture_method=value.capture_method,
            runtime=value.runtime,
            correlation_id=session_id or f"hook:{value.offset}",
            session_id=session_id,
            invocation_id=_pair_invocation_id(
                value.outbox, session_id, value.offset, is_start
            ),
            partial=True,
            payload=_clean(payload),
        ),
        None,
    )


_EVENT_MAPPERS = {
    "gateway:startup": _map_gateway_start,
    "session:start": _map_session_start,
    "session:end": _map_session_end,
    "session:reset": _map_session_end,
    "agent:start": _map_agent,
    "agent:end": _map_agent,
}
