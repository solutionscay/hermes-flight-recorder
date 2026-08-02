"""Shared finding output for reconciliation passes."""

from __future__ import annotations

from typing import Any

from ._common import append_and_count, build_record, runtime_stamp


def emit_finding(
    outbox,
    counts,
    *,
    event_type: str,
    occurred_at: float,
    correlation_id: str,
    payload: dict[str, Any],
    dedup_key: str,
    session_id: str | None = None,
    parent_session_id: str | None = None,
    invocation_id: str | None = None,
    profile: str = "default",
    partial: bool = True,
    content: str | None = None,
) -> None:
    record = build_record(
        event_type=event_type,
        occurred_at=occurred_at,
        source="reconciler",
        capture_method="derive:reconciler",
        runtime=runtime_stamp("reconciler"),
        correlation_id=correlation_id,
        payload=payload,
        session_id=session_id,
        parent_session_id=parent_session_id,
        invocation_id=invocation_id,
        profile=profile,
        partial=partial,
    )
    append_and_count(outbox, counts, record, content=content, dedup_key=dedup_key)


def emit_terminal_missing(
    outbox,
    counts,
    *,
    occurred_at,
    correlation_id,
    subject_type,
    subject_id,
    start_event_type,
    expected_terminal_event_type,
    dedup_key,
    details=None,
    session_id=None,
    parent_session_id=None,
    invocation_id=None,
    profile="default",
) -> None:
    payload = {
        "subject_type": subject_type,
        "subject_id": subject_id,
        "start_event_type": start_event_type,
        "expected_terminal_event_type": expected_terminal_event_type,
    }
    payload.update(details or {})
    emit_finding(
        outbox,
        counts,
        event_type="reconcile.terminal_missing",
        occurred_at=occurred_at,
        correlation_id=correlation_id,
        session_id=session_id,
        parent_session_id=parent_session_id,
        invocation_id=invocation_id,
        profile=profile,
        partial=True,
        payload=payload,
        dedup_key=dedup_key,
    )


__all__ = ["emit_finding", "emit_terminal_missing"]
