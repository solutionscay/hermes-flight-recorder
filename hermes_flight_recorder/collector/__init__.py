"""Collector — capture Hermes events, buffer them, and reconcile.

Components:

- ``outbox``:    durable local SQLite queue with a monotonic
                 producer_sequence
- ``hook``:      in-gateway spooler plus a Bridge-side drain for live
                 lifecycle capture
- ``state_db``:  adapter that reads Hermes ``state.db`` into
                 canonical events
- ``cron_db``:   adapter that reads the cron execution store
- ``kanban_db``: adapter that reads the Kanban board stores into
                 ``task.*`` lifecycle events
- ``gateway_log``: read-only adapter for terminal model-provider failures
- ``reconcile``: diff the durable stores against the outbox to detect
                 gaps, missing terminals, and missed cron runs
- ``sync``:      batch pending outbox events for an acknowledged transport
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable


def run_pass(
    outbox: Any,
    hermes_home: str | Path | None = None,
    *,
    on_source_error: Callable[[str, Exception], None] | None = None,
) -> dict[str, int]:
    """One capture pass: drain the hook spool, then poll the durable stores.

    This is the pipeline ``hermes-flight-recorder run`` executes; the gate
    scripts call the same function so they validate the real thing. Returns
    per-event-type counts of newly captured records.

    ``on_source_error`` receives each tolerated per-source failure (any
    exception from the hook drain — a bad spool must not sink the poll pass —
    and a missing durable store from a poll). When it is None, every failure
    propagates instead.
    """
    from collections import Counter

    from . import cron_db, gateway_log, kanban_db, state_db
    from .hook import drain as drain_hook_spool

    totals: Counter[str] = Counter()
    sources: tuple[tuple[str, Callable[[], dict[str, int]], type[Exception]], ...] = (
        ("hook drain", lambda: drain_hook_spool(outbox), Exception),
        ("state.db", lambda: state_db.poll(outbox, hermes_home), FileNotFoundError),
        ("cron", lambda: cron_db.poll(outbox, hermes_home), FileNotFoundError),
        ("kanban", lambda: kanban_db.poll(outbox, hermes_home), FileNotFoundError),
        ("gateway log", lambda: gateway_log.poll(outbox, hermes_home), FileNotFoundError),
    )
    for label, poll, tolerated in sources:
        try:
            totals.update(poll())
        except tolerated as exc:
            if on_source_error is None:
                raise
            on_source_error(label, exc)
    return dict(totals)
