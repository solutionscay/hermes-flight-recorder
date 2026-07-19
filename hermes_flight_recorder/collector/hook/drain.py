"""Bridge-side drain for the live hook spool.

The in-gateway spooler appends one JSON line per Hermes lifecycle event to
``hook-spool.jsonl``. This module runs in the Bridge environment and turns
those raw lines into canonical envelope v1 records: it maps each event,
encrypts the content, assigns the ``producer_sequence`` via the outbox, and
appends with a dedup key.

Durability model (issue #4): at-least-once with dedup at the drain. The read
cursor is a byte offset stored in the outbox meta. On a Bridge stop between
an append and the cursor commit, the next drain re-reads the same lines at
the same byte offsets; the dedup key is the line's offset, so re-processing
is idempotent (no duplicate row, no consumed sequence). A partial trailing
line (the gateway died mid-write) is left for the next drain. The hook is
lossy by design; a lost or dropped line is caught by the reconciler against
``state.db``.

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

import json
from pathlib import Path
from typing import Any

from .._common import build_record, runtime_stamp
from . import CURSOR_NAME, SPOOL_FILENAME


def drain(outbox: Any, bridge_home: str | Path | None = None) -> dict[str, int]:
    """Drain new spool lines into the outbox. Returns per-type counts.

    ``bridge_home`` defaults to the outbox's own home, so the spool and the
    outbox always align. Only newly-created rows are counted (a dedup hit on
    re-drain does not count).
    """
    home = Path(bridge_home) if bridge_home else Path(outbox.path).parent
    spool = home / SPOOL_FILENAME
    if not spool.exists():
        return {}

    cursor = int(outbox.get_cursor(CURSOR_NAME) or 0)
    if spool.stat().st_size < cursor:
        cursor = 0  # spool was truncated or rotated; restart from the top

    with open(spool, "rb") as fh:
        fh.seek(cursor)
        blob = fh.read()
    if not blob:
        return {}

    # Everything before the final newline is a complete line. A non-empty
    # trailing element is a partial write; drop it and re-read it next drain.
    complete = blob.split(b"\n")[:-1]

    counts: dict[str, int] = {}
    session_ids: dict[str, str] = {}
    consumed = 0
    for raw in complete:
        line_offset = cursor + consumed
        consumed += len(raw) + 1  # + the newline byte
        text = raw.decode("utf-8", "replace").strip()
        if not text:
            continue
        try:
            obj = json.loads(text)
        except ValueError:
            continue  # skip an undecodable line rather than fail the drain
        mapped = _map_event(obj, line_offset, session_ids, outbox)
        if mapped is None:
            continue
        record, content = mapped
        outbox.append(record, content=content, dedup_key=f"hook-spool:{line_offset}")
        if outbox.last_append_created:
            event_type = record["payload"]["event_type"]
            counts[event_type] = counts.get(event_type, 0) + 1

    outbox.set_cursor(CURSOR_NAME, cursor + consumed)
    return counts


def _clean(payload: dict[str, Any]) -> dict[str, Any]:
    """Drop None-valued keys so payloads stay tidy."""
    return {k: v for k, v in payload.items() if v is not None}


def _pair_invocation_id(outbox: Any, sid: str | None, offset: int, is_start: bool) -> str:
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


def _map_event(
    obj: dict[str, Any], offset: int, session_ids: dict[str, str], outbox: Any
) -> tuple[dict[str, Any], str | None] | None:
    """Map one raw spool record to (envelope_record, content) or None.

    ``session_ids`` accumulates the ``session_key`` -> ``session_id`` map
    from session-start events, so a later session-end can recover its
    ``session_id`` within the same drain.
    """
    event_type = obj.get("event_type")
    if not isinstance(event_type, str):
        return None
    ctx = obj.get("context") or {}
    if not isinstance(ctx, dict):
        ctx = {}
    occurred_at = float(obj.get("captured_at") or 0.0)

    base = event_type.split(":", 1)[0]
    capture_method = f"hook:{event_type}"
    source = f"hook:{base}"
    runtime = runtime_stamp(base)

    if event_type == "gateway:startup":
        return (
            build_record(
                event_type="runtime.gateway_started",
                occurred_at=occurred_at,
                source=source,
                capture_method=capture_method,
                runtime=runtime,
                correlation_id=f"gateway:{offset}",
                payload=_clean({"platforms": ctx.get("platforms")}),
            ),
            None,
        )

    if event_type == "session:start":
        sid = ctx.get("session_id")
        skey = ctx.get("session_key")
        if sid and skey:
            session_ids[skey] = sid
        return (
            build_record(
                event_type="session.created",
                occurred_at=occurred_at,
                source=source,
                capture_method=capture_method,
                runtime=runtime,
                correlation_id=sid or skey or f"hook:{offset}",
                session_id=sid,
                session_key=skey,
                payload=_clean(
                    {"platform": ctx.get("platform"), "user_id": ctx.get("user_id")}
                ),
            ),
            None,
        )

    if event_type in ("session:end", "session:reset"):
        skey = ctx.get("session_key")
        sid = session_ids.get(skey) if skey else None
        return (
            build_record(
                event_type="session.ended",
                occurred_at=occurred_at,
                source=source,
                capture_method=capture_method,
                runtime=runtime,
                correlation_id=sid or skey or f"hook:{offset}",
                session_id=sid,
                session_key=skey,
                partial=True,  # provisional: a reset is not a real end, and
                # end_reason is unknown until the state adapter reconstructs it
                payload=_clean(
                    {
                        "platform": ctx.get("platform"),
                        "user_id": ctx.get("user_id"),
                        "reason": "reset" if event_type == "session:reset" else "end",
                    }
                ),
            ),
            None,
        )

    if event_type in ("agent:start", "agent:end"):
        sid = ctx.get("session_id")
        is_start = event_type == "agent:start"
        payload = {
            "platform": ctx.get("platform"),
            "user_id": ctx.get("user_id"),
            "chat_type": ctx.get("chat_type"),
        }
        if is_start:
            payload["thread_id"] = ctx.get("thread_id")
            payload["chat_id"] = ctx.get("chat_id")
        content = ctx.get("message") if is_start else ctx.get("response")
        return (
            build_record(
                event_type="invocation.started" if is_start else "invocation.completed",
                occurred_at=occurred_at,
                source=source,
                capture_method=capture_method,
                runtime=runtime,
                correlation_id=sid or f"hook:{offset}",
                session_id=sid,
                # No turn id is exposed to hooks; paired via outbox meta so
                # start and end share one id (see _pair_invocation_id).
                invocation_id=_pair_invocation_id(outbox, sid, offset, is_start),
                partial=True,
                payload=_clean(payload),
            ),
            content or None,
        )

    return None  # an event we do not map; ignore it
