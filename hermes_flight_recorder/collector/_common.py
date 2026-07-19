"""Shared helpers for the durable-state adapters.

Build producer records (the envelope fields a producer fills in; the
outbox stamps event_id, installation_id, producer_sequence, recorded_at)
and normalize Hermes timestamps.
"""

from __future__ import annotations

import datetime
from pathlib import Path
from typing import Any


def resolve_hermes_home(hermes_home: str | Path | None) -> Path:
    """The Hermes data root: explicit arg, then $HERMES_HOME, then ~/.hermes."""
    import os

    if hermes_home:
        return Path(hermes_home).expanduser()
    env = os.environ.get("HERMES_HOME")
    return Path(env).expanduser() if env else Path.home() / ".hermes"


def to_epoch(value: Any) -> float | None:
    """Normalize a Hermes timestamp to epoch seconds.

    Hermes uses two shapes: epoch floats (state.db) and ISO 8601 strings
    with a timezone (cron executions.db). Return None for a missing value.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return datetime.datetime.fromisoformat(value).timestamp()


def runtime_stamp(kind: str) -> dict[str, Any]:
    """A minimal runtime inventory stamp. Best-effort in the POC."""
    return {"kind": kind, "engine": "standard"}


def root_session(session_id: str | None, parent_map: dict[str, str | None]) -> str | None:
    """Walk parent_session_id to the top ancestor (the correlation root)."""
    seen: set[str] = set()
    sid = session_id
    while sid is not None and parent_map.get(sid) and parent_map[sid] not in seen:
        seen.add(sid)
        sid = parent_map[sid]
    return sid


def build_record(
    *,
    event_type: str,
    occurred_at: float,
    source: str,
    capture_method: str,
    runtime: dict[str, Any],
    correlation_id: str,
    payload: dict[str, Any] | None = None,
    session_id: str | None = None,
    session_key: str | None = None,
    parent_session_id: str | None = None,
    invocation_id: str | None = None,
    causation_id: str | None = None,
    tenant_id: str = "default",
    profile: str = "default",
    partial: bool = False,
) -> dict[str, Any]:
    """Assemble a producer record. The outbox stamps the rest."""
    pl = dict(payload or {})
    pl["event_type"] = event_type
    rec: dict[str, Any] = {
        "occurred_at": float(occurred_at),
        "tenant_id": tenant_id,
        "profile": profile or "default",
        "runtime": runtime,
        "correlation_id": correlation_id,
        "source": source,
        "capture_method": capture_method,
        "payload": pl,
        "partial": partial,
    }
    for key, val in (
        ("session_id", session_id),
        ("session_key", session_key),
        ("parent_session_id", parent_session_id),
        ("invocation_id", invocation_id),
        ("causation_id", causation_id),
    ):
        if val is not None:
            rec[key] = val
    return rec
