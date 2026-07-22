"""Config-controlled retention for acknowledged outbox events.

The server acknowledgement cursor is always the deletion boundary. Retention
configuration can narrow the eligible set by age or storage budget, but it
cannot opt out of that delivery guarantee.
"""

from __future__ import annotations

import time
from typing import Any

from .outbox import PruneResult
from .recorder_config import RetentionConfig
from .sync import delivery_cursor

AUTO_PRUNE_INTERVAL_SECONDS = 6 * 60 * 60
_LAST_AUTO_PRUNE_META_KEY = "retention:last_auto_prune_at"


class RetentionError(RuntimeError):
    """The requested retention operation is unsafe or invalid."""


def prune(
    outbox: Any,
    config: RetentionConfig,
    *,
    now: float | None = None,
) -> PruneResult | None:
    """Apply the configured policies, or return ``None`` when disabled.

    Age and size are OR policies. Age removes every acknowledged row older
    than the cutoff. Size then removes the oldest remaining acknowledged rows
    until all retained event envelopes fit the budget, or until only
    undelivered rows remain.
    """
    if not config.enabled:
        return None
    if not config.require_delivered:
        raise RetentionError(
            "retention.require_delivered must be true; undelivered events "
            "cannot be pruned"
        )
    if config.vacuum != "auto":
        raise RetentionError("retention.vacuum must be 'auto'")

    timestamp = time.time() if now is None else now
    cutoff = (
        timestamp - config.max_age_days * 24 * 60 * 60
        if config.max_age_days is not None
        else None
    )
    try:
        return outbox.prune_delivered(
            delivery_cursor(outbox),
            older_than=cutoff,
            max_bytes=config.max_bytes,
            vacuum=True,
        )
    except Exception as exc:
        raise RetentionError(f"cannot prune the outbox: {exc}") from exc


def maybe_prune(
    outbox: Any,
    config: RetentionConfig,
    *,
    now: float | None = None,
    interval_seconds: float = AUTO_PRUNE_INTERVAL_SECONDS,
) -> PruneResult | None:
    """Apply enabled retention at most once per automatic-prune interval."""
    if not config.enabled:
        return None
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")

    timestamp = time.time() if now is None else now
    try:
        raw_last_run = outbox.get_meta(_LAST_AUTO_PRUNE_META_KEY)
    except Exception as exc:
        raise RetentionError(f"cannot read automatic-prune state: {exc}") from exc
    try:
        last_run = float(raw_last_run) if raw_last_run is not None else None
    except ValueError:
        last_run = None
    if last_run is not None and timestamp - last_run < interval_seconds:
        return None

    result = prune(outbox, config, now=timestamp)
    try:
        outbox.set_meta(_LAST_AUTO_PRUNE_META_KEY, repr(timestamp))
    except Exception as exc:
        raise RetentionError(f"cannot save automatic-prune state: {exc}") from exc
    return result


__all__ = [
    "AUTO_PRUNE_INTERVAL_SECONDS",
    "RetentionError",
    "maybe_prune",
    "prune",
]
