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
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

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
    and a missing durable store from a poll) and each per-item error a source
    tolerated while its poll still completed (an unreadable knowledge
    artifact). When it is None, a failed poll propagates instead; per-item
    errors of a completed poll are only recorded in source health.

    ``now`` overrides the wall clock stamped into the capture heartbeat; it
    exists for deterministic fixtures (the exit gate) that reconcile against a
    fixed synthetic clock. Production leaves it None and uses ``time.time()``.
    """
    from collections import Counter

    from . import cron_db, gateway_log, kanban_db, knowledge_store, state_db
    from ._common import read_home_mode
    from .health import record_error, record_success, source_health_key
    from .hook import drain as drain_hook_spool
    from .recorder_config import CaptureConfig, source_enabled

    since = _capture_since(outbox)
    capture = capture_config or CaptureConfig()
    # Resolve the terminal home-mode policy once per pass; every durable-store
    # poll stamps it, and re-reading config.yaml per source (or per matched log
    # line) is pure waste (issue #164).
    home_mode = read_home_mode(hermes_home)

    totals: Counter[str] = Counter()

    def poll_knowledge() -> tuple[dict[str, int], list[Exception]]:
        """Knowledge scan adapted to the uniform ``(counts, errors)`` protocol.

        ``knowledge_store.poll`` reports each unreadable artifact through its
        ``on_artifact_error`` callback and keeps scanning; this adapter collects
        those tolerated per-artifact errors and returns them alongside the
        counts, so the loop below never needs to know which source they came
        from (issue #168).
        """
        errors: list[Exception] = []
        counts = knowledge_store.poll(
            outbox,
            hermes_home,
            knowledge_config=knowledge_config,
            on_artifact_error=lambda _artifact_id, exc: errors.append(exc),
            home_mode=home_mode,
        )
        return counts, errors

    # Every poll returns ``(counts, errors)``: per-event-type counts of newly
    # captured records, plus any per-item errors the source tolerated without
    # failing the whole poll (a degraded pass — counts still land, the next
    # tick re-scans). Only knowledge currently produces tolerated errors; a
    # source that grows per-item tolerance later just returns them here.
    sources: tuple[
        tuple[
            str,
            str,
            Callable[[], tuple[dict[str, int], list[Exception]]],
            tuple[type[Exception], ...],
        ],
        ...,
    ] = (
        (
            "hook",
            "hook drain",
            lambda: (drain_hook_spool(outbox), []),
            (Exception,),
        ),
        (
            "state_db",
            "state.db",
            lambda: (
                state_db.poll(
                    outbox,
                    hermes_home,
                    capture_config=capture,
                    knowledge_config=knowledge_config,
                    since=since,
                    home_mode=home_mode,
                ),
                [],
            ),
            _DURABLE_STORE_ERRORS,
        ),
        (
            "cron",
            "cron",
            lambda: (
                cron_db.poll(outbox, hermes_home, since=since, home_mode=home_mode),
                [],
            ),
            _DURABLE_STORE_ERRORS,
        ),
        (
            "kanban",
            "kanban",
            lambda: (
                kanban_db.poll(outbox, hermes_home, since=since, home_mode=home_mode),
                [],
            ),
            _DURABLE_STORE_ERRORS,
        ),
        (
            "gateway_log",
            "gateway log",
            lambda: (
                gateway_log.poll(
                    outbox, hermes_home, since=since, home_mode=home_mode
                ),
                [],
            ),
            _DURABLE_STORE_ERRORS,
        ),
        ("knowledge", "knowledge", poll_knowledge, _DURABLE_STORE_ERRORS),
    )
    health_at = time.time() if now is None else float(now)
    for source_name, label, poll, tolerated in sources:
        if not source_enabled(capture, source_name):
            continue
        try:
            source_totals, source_errors = poll()
        except tolerated as exc:
            record_error(outbox, source_health_key(source_name), health_at, exc)
            if on_source_error is None:
                raise
            on_source_error(label, exc)
        else:
            totals.update(source_totals)
            if source_errors:
                # Degraded but not dead: the poll completed and its counts are
                # kept, while the health record shows the per-item failure so
                # observers see the degradation. Tolerated per-item errors
                # never propagate, even without an error handler.
                record_error(
                    outbox,
                    source_health_key(source_name),
                    health_at,
                    source_errors[-1],
                )
                if on_source_error is not None:
                    for exc in source_errors:
                        on_source_error(label, exc)
            else:
                record_success(outbox, source_health_key(source_name), health_at)

    # Stamp the capture heartbeat once the pass completes. A pass that reached
    # here is a live capture loop even if a source degraded to a skip (the next
    # tick re-scans); the heartbeat proves the loop ran, not that every source
    # succeeded. If a source raised uncaught (on_source_error is None), we never
    # reach this — a crashing pass is not a success.
    stamped = time.time() if now is None else float(now)
    outbox.set_meta(CAPTURE_HEARTBEAT_KEY, repr(stamped))
    return dict(totals)
