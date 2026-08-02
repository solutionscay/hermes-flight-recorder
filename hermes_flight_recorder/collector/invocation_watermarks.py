"""Incremental hook invocation index for state database attribution."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from .watermark import Watermark

_CURSOR = "state.db:invocation-events:v1"
_OVERLAP = 64
_MAX_WINDOWS_PER_SESSION = 32
_META_PREFIX = "state.db:invocation-windows:v1:"


@dataclass(frozen=True)
class InvocationWindow:
    invocation_id: str
    started_at: float
    ended_at: float | None


def read_invocation_windows(
    outbox: Any, session_ids: set[str]
) -> dict[str, list[InvocationWindow]]:
    """Index new hook events and load windows for selected sessions."""
    watermark = Watermark(outbox, _CURSOR, overlap=_OVERLAP)
    high_water = outbox.high_water()
    changed: dict[str, list[InvocationWindow]] = {}
    for event in outbox.iter_events(after_sequence=watermark.lower_bound()):
        sequence = int(event.get("producer_sequence") or 0)
        if sequence > high_water:
            break
        event_type = event.get("payload", {}).get("event_type")
        if event_type not in ("invocation.started", "invocation.completed"):
            continue
        if not str(event.get("capture_method", "")).startswith("hook:agent:"):
            continue
        session_id = event.get("session_id")
        invocation_id = event.get("invocation_id")
        if not isinstance(session_id, str) or not isinstance(invocation_id, str):
            continue
        windows = changed.setdefault(
            session_id, _load_session_windows(outbox, session_id)
        )
        occurred_at = _number(event.get("occurred_at"))
        existing = next(
            (window for window in windows if window.invocation_id == invocation_id),
            None,
        )
        if event_type == "invocation.started":
            if existing is None:
                windows.append(InvocationWindow(invocation_id, occurred_at, None))
            elif occurred_at < existing.started_at:
                windows.remove(existing)
                windows.append(
                    InvocationWindow(invocation_id, occurred_at, existing.ended_at)
                )
        elif existing is not None and (
            existing.ended_at is None or occurred_at < existing.ended_at
        ):
            windows.remove(existing)
            windows.append(
                InvocationWindow(invocation_id, existing.started_at, occurred_at)
            )

    for session_id, windows in changed.items():
        ordered = sorted(windows, key=lambda window: window.started_at)
        _save_session_windows(outbox, session_id, ordered[-_MAX_WINDOWS_PER_SESSION:])
    watermark.advance(high_water)

    result = {}
    for session_id in session_ids:
        windows = changed.get(session_id)
        if windows is None:
            windows = _load_session_windows(outbox, session_id)
        if windows:
            result[session_id] = _cap_open_windows(windows)
    return result


def _cap_open_windows(windows: list[InvocationWindow]) -> list[InvocationWindow]:
    ordered = sorted(windows, key=lambda window: window.started_at)
    result = []
    for index, window in enumerate(ordered):
        ended_at = window.ended_at
        if index + 1 < len(ordered):
            next_start = ordered[index + 1].started_at
            if ended_at is None or ended_at >= next_start:
                ended_at = next_start
        result.append(
            InvocationWindow(window.invocation_id, window.started_at, ended_at)
        )
    return result


def _meta_key(session_id: str) -> str:
    digest = hashlib.sha256(session_id.encode()).hexdigest()
    return f"{_META_PREFIX}{digest}"


def _load_session_windows(outbox, session_id: str) -> list[InvocationWindow]:
    raw = outbox.get_meta(_meta_key(session_id))
    if raw is None:
        return []
    try:
        values = json.loads(raw)
    except (TypeError, ValueError):
        return []
    if not isinstance(values, list):
        return []
    result = []
    for value in values:
        if not isinstance(value, dict):
            continue
        invocation_id = value.get("invocation_id")
        started_at = value.get("started_at")
        ended_at = value.get("ended_at")
        if not isinstance(invocation_id, str) or not isinstance(
            started_at, (int, float)
        ):
            continue
        if ended_at is not None and not isinstance(ended_at, (int, float)):
            continue
        result.append(InvocationWindow(invocation_id, float(started_at), ended_at))
    return result


def _save_session_windows(
    outbox, session_id: str, windows: list[InvocationWindow]
) -> None:
    value = [
        {
            "invocation_id": window.invocation_id,
            "started_at": window.started_at,
            "ended_at": window.ended_at,
        }
        for window in windows
    ]
    outbox.set_meta(_meta_key(session_id), json.dumps(value, separators=(",", ":")))


def _number(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


__all__ = ["InvocationWindow", "read_invocation_windows"]
