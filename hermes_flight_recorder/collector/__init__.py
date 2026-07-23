"""Collector — capture Hermes events, buffer them, and reconcile.

Components:

- ``outbox``:    durable local SQLite queue with a monotonic
                 producer_sequence
- ``hook``:      in-gateway spooler plus a Flight Recorder-side drain for live
                 lifecycle capture
- ``state_db``:  adapter that reads Hermes ``state.db`` into
                 canonical events
- ``cron_db``:   adapter that reads the cron execution store
- ``kanban_db``: adapter that reads the Kanban board stores into
                 ``task.*`` lifecycle events
- ``gateway_log``: read-only adapter for terminal model-provider failures
- ``reconcile``: diff the durable stores against the outbox to detect
                 gaps, missing terminals, and missed cron runs
- ``retention``: prune only server-acknowledged rows by age or byte budget
- ``sync``:      batch pending outbox events for an acknowledged transport
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

# Meta key holding the wall-clock epoch of the last completed capture pass.
# The reconciler reads it to prove the capture loop is still ticking; a frozen
# value while reconcile keeps running is the silent-outage signal (a dead timer,
# a crash-loop, a hung pass).
CAPTURE_HEARTBEAT_KEY = "capture:last_success_at"

def _capture_since(outbox: Any) -> float | None:
    """The capture horizon epoch, or None when backfill is enabled (default).

    Returns the ``installed_at`` marker only when backfill is explicitly off, so
    collectors emit nothing that occurred before the recorder was installed.
    """
    from ._common import CAPTURE_BACKFILL_META_KEY, INSTALLED_AT_META_KEY

    if outbox.get_meta(CAPTURE_BACKFILL_META_KEY) != "false":
        return None
    raw = outbox.get_meta(INSTALLED_AT_META_KEY)
    try:
        return float(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None

# A durable-store poll may hit a transient fault that is not a missing file: the
# Hermes DB is momentarily locked (``sqlite3.OperationalError`` while Hermes
# checkpoints), unreadable (``PermissionError``), or malformed mid-write. These
# must degrade to a skipped source for this tick (the next tick re-scans, and
# dedup is the backstop), never crash the whole pass and drop every later source.
# ``FileNotFoundError`` is an ``OSError`` subclass, so missing-store handling is
# preserved.
_DURABLE_STORE_ERRORS: tuple[type[Exception], ...] = (OSError, sqlite3.Error)

if TYPE_CHECKING:
    from .recorder_config import CaptureConfig, KnowledgeConfig


def run_pass(
    outbox: Any,
    hermes_home: str | Path | None = None,
    *,
    capture_config: CaptureConfig | None = None,
    knowledge_config: KnowledgeConfig | None = None,
    on_source_error: Callable[[str, Exception], None] | None = None,
    now: float | None = None,
) -> dict[str, int]:
    """One capture pass: drain the hook spool, then poll the durable stores.

    This is the pipeline ``hermes-flight-recorder run`` executes; the gate
    scripts call the same function so they validate the real thing. Returns
    per-event-type counts of newly captured records.

    ``on_source_error`` receives each tolerated per-source failure (any
    exception from the hook drain — a bad spool must not sink the poll pass —
    and a missing durable store from a poll). When it is None, every failure
    propagates instead.

    ``now`` overrides the wall clock stamped into the capture heartbeat; it
    exists for deterministic fixtures (the exit gate) that reconcile against a
    fixed synthetic clock. Production leaves it None and uses ``time.time()``.
    """
    from collections import Counter

    from . import cron_db, gateway_log, kanban_db, knowledge_store, state_db
    from .hook import drain as drain_hook_spool

    since = _capture_since(outbox)

    totals: Counter[str] = Counter()
    sources: tuple[
        tuple[str, Callable[[], dict[str, int]], tuple[type[Exception], ...]], ...
    ] = (
        ("hook drain", lambda: drain_hook_spool(outbox), (Exception,)),
        (
            "state.db",
            lambda: state_db.poll(
                outbox, hermes_home, capture_config=capture_config, since=since
            ),
            _DURABLE_STORE_ERRORS,
        ),
        ("cron", lambda: cron_db.poll(outbox, hermes_home, since=since), _DURABLE_STORE_ERRORS),
        ("kanban", lambda: kanban_db.poll(outbox, hermes_home, since=since), _DURABLE_STORE_ERRORS),
        ("gateway log", lambda: gateway_log.poll(outbox, hermes_home, since=since), _DURABLE_STORE_ERRORS),
        (
            "knowledge",
            lambda: knowledge_store.poll(
                outbox, hermes_home, knowledge_config=knowledge_config
            ),
            _DURABLE_STORE_ERRORS,
        ),
    )
    for label, poll, tolerated in sources:
        try:
            totals.update(poll())
        except tolerated as exc:
            if on_source_error is None:
                raise
            on_source_error(label, exc)

    # Stamp the capture heartbeat once the pass completes. A pass that reached
    # here is a live capture loop even if a source degraded to a skip (the next
    # tick re-scans); the heartbeat proves the loop ran, not that every source
    # succeeded. If a source raised uncaught (on_source_error is None), we never
    # reach this — a crashing pass is not a success.
    stamped = time.time() if now is None else float(now)
    outbox.set_meta(CAPTURE_HEARTBEAT_KEY, repr(stamped))
    return dict(totals)
