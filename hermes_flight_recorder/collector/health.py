"""Durable health state for capture sources and reconciliation."""

from __future__ import annotations

import json
from typing import Any

SOURCE_HEALTH_PREFIX = "health:capture-source:"
RECONCILE_HEALTH_KEY = "health:reconcile"


def source_health_key(source: str) -> str:
    return f"{SOURCE_HEALTH_PREFIX}{source}"


def read_health(outbox: Any, key: str) -> dict[str, Any]:
    raw = outbox.get_meta(key)
    if raw is None:
        return {}
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return {"unreadable": raw}
    return value if isinstance(value, dict) else {"unreadable": raw}


def record_success(outbox: Any, key: str, when: float) -> None:
    state = read_health(outbox, key)
    state.pop("unreadable", None)
    state["last_success_at"] = float(when)
    state["consecutive_failures"] = 0
    outbox.set_meta(key, json.dumps(state, sort_keys=True, separators=(",", ":")))


def record_error(outbox: Any, key: str, when: float, exc: Exception) -> None:
    state = read_health(outbox, key)
    try:
        failures = int(state.get("consecutive_failures", 0)) + 1
    except (TypeError, ValueError):
        failures = 1
    state.pop("unreadable", None)
    state["last_error_at"] = float(when)
    state["last_error"] = f"{type(exc).__name__}: {exc}"[:1000]
    state["consecutive_failures"] = failures
    outbox.set_meta(key, json.dumps(state, sort_keys=True, separators=(",", ":")))


__all__ = [
    "RECONCILE_HEALTH_KEY",
    "SOURCE_HEALTH_PREFIX",
    "read_health",
    "record_error",
    "record_success",
    "source_health_key",
]
