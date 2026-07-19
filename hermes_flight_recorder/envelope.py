"""Canonical event envelope v1.

The envelope is the single append-only event contract that every part of
the collector serializes to. This module is deliberately self-contained:
it imports only the standard library and nothing from ``collector``,
``outbox``, or any state adapter, so the contract has no dependency on the
things that produce or consume it.

See ``docs/schema/envelope-v1.md`` for the prose specification.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Mapping

__all__ = [
    "SCHEMA_VERSION",
    "P0_POC_EVENT_TYPES",
    "RESERVED_EVENT_TYPES",
    "ALL_EVENT_TYPES",
    "EnvelopeValidationError",
    "validate",
    "serialize",
    "parse",
]

SCHEMA_VERSION = "1"

# --- Event-type surface -------------------------------------------------
# P0-poc: captured and observed in the Phase 0 POC.
P0_POC_EVENT_TYPES = frozenset(
    {
        "runtime.gateway_started",
        "session.created",
        "session.ended",
        "invocation.started",
        "invocation.completed",
        "model.usage_recorded",
        "tool.call_completed",
        "subagent.child_spawned",
        "subagent.completed",
        "delegation.dispatched",
        "cron.ticker_heartbeat",
        "cron.run_claimed",
        "cron.run_finished",
        "cron.run_missed",
        "reconcile.gap_detected",
        "reconcile.terminal_missing",
    }
)

# reserved: defined in v1 but not captured in the POC.
RESERVED_EVENT_TYPES = frozenset(
    {
        "runtime.gateway_stopped",
        "session.finalized",
        "session.compressed",
        "step.iterated",
        "model.call_requested",
        "model.call_succeeded",
        "model.call_failed",
        "tool.call_requested",
        "tool.approval_requested",
        "tool.approval_responded",
        "delegation.delivered",
        "delegation.progress",
        "cron.definition_changed",
        "command.invoked",
        "handoff.state_changed",
        "task.created",
        "task.claimed",
        "task.completed",
        "task.blocked",
        "task.failed_terminal",
        "knowledge.record_written",
        "knowledge.record_compacted",
    }
)

ALL_EVENT_TYPES = P0_POC_EVENT_TYPES | RESERVED_EVENT_TYPES


class EnvelopeValidationError(ValueError):
    """Raised when a record does not conform to envelope v1."""


# --- Type predicates ----------------------------------------------------
# bool is a subclass of int in Python; keep the two apart on purpose.
def _is_str(v: Any) -> bool:
    return isinstance(v, str)


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _is_number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _is_bool(v: Any) -> bool:
    return isinstance(v, bool)


def _is_dict(v: Any) -> bool:
    return isinstance(v, dict)


# field name -> (predicate, human type label, required)
_FIELDS: tuple[tuple[str, Callable[[Any], bool], str, bool], ...] = (
    ("schema_version", _is_str, "string", True),
    ("event_id", _is_str, "string", True),
    ("producer_sequence", _is_int, "integer", True),
    ("occurred_at", _is_number, "number", True),
    ("recorded_at", _is_number, "number", True),
    ("installation_id", _is_str, "string", True),
    ("tenant_id", _is_str, "string", True),
    ("profile", _is_str, "string", True),
    ("runtime", _is_dict, "object", True),
    ("session_id", _is_str, "string", False),
    ("session_key", _is_str, "string", False),
    ("parent_session_id", _is_str, "string", False),
    ("invocation_id", _is_str, "string", False),
    ("correlation_id", _is_str, "string", True),
    ("causation_id", _is_str, "string", False),
    ("source", _is_str, "string", True),
    ("capture_method", _is_str, "string", True),
    ("payload", _is_dict, "object", True),
    ("content_ciphertext", _is_str, "string", False),
    ("content_nonce", _is_str, "string", False),
    ("content_hash", _is_str, "string", False),
    ("key_version", _is_str, "string", False),
    ("partial", _is_bool, "boolean", True),
)

_KNOWN_FIELDS = frozenset(name for name, _, _, _ in _FIELDS)
_CONTENT_COMPANIONS = ("content_nonce", "content_hash", "key_version")


def _present(record: Mapping[str, Any], name: str) -> bool:
    """A field counts as present only when it exists and is not None."""
    return name in record and record[name] is not None


def validate(record: Mapping[str, Any], *, allow_unknown_fields: bool = False) -> Mapping[str, Any]:
    """Validate a record against envelope v1.

    Return the record on success. Raise ``EnvelopeValidationError`` on the
    first problem found. A required field must be present and not None. An
    optional field may be absent or None; when present it must have the
    right type.
    """
    if not isinstance(record, Mapping):
        raise EnvelopeValidationError(
            f"record must be a mapping, got {type(record).__name__}"
        )

    for name, predicate, label, required in _FIELDS:
        present = _present(record, name)
        if required and not present:
            raise EnvelopeValidationError(f"missing required field: {name!r}")
        if present and not predicate(record[name]):
            got = type(record[name]).__name__
            raise EnvelopeValidationError(
                f"field {name!r} must be {label}, got {got}"
            )

    if record.get("schema_version") != SCHEMA_VERSION:
        raise EnvelopeValidationError(
            f"unsupported schema_version {record.get('schema_version')!r}; "
            f"this validator handles {SCHEMA_VERSION!r}"
        )

    if not allow_unknown_fields:
        extra = set(record) - _KNOWN_FIELDS
        if extra:
            raise EnvelopeValidationError(
                f"unknown envelope fields: {sorted(extra)}"
            )

    # Content-field invariant: the three companions are present if and only
    # if content_ciphertext is present.
    has_content = _present(record, "content_ciphertext")
    for companion in _CONTENT_COMPANIONS:
        companion_present = _present(record, companion)
        if has_content and not companion_present:
            raise EnvelopeValidationError(
                f"content_ciphertext is present but {companion} is missing"
            )
        if not has_content and companion_present:
            raise EnvelopeValidationError(
                f"{companion} is present but content_ciphertext is missing"
            )

    # payload.event_type must be a known type.
    payload = record["payload"]
    event_type = payload.get("event_type")
    if not isinstance(event_type, str):
        raise EnvelopeValidationError("payload.event_type must be a string")
    if event_type not in ALL_EVENT_TYPES:
        raise EnvelopeValidationError(f"unknown event_type: {event_type!r}")

    return record


def serialize(record: Mapping[str, Any]) -> str:
    """Serialize a record to a stable JSON string (sorted keys, compact)."""
    return json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def parse(text: str) -> dict[str, Any]:
    """Parse a JSON string into an envelope record (a plain dict)."""
    obj = json.loads(text)
    if not isinstance(obj, dict):
        raise EnvelopeValidationError("a serialized envelope must be a JSON object")
    return obj
