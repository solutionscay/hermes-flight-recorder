"""One shared sync pass for the CLI ``sync`` verb and the ``serve`` daemon.

The pass is: push events -> read the cursor delta -> ship pending wrapped
DEKs -> run throttled retention. This module owns that sequence once and
returns a structured :class:`SyncPassResult`; it never prints or logs, so
``cli`` renders it with ``print`` and ``serve`` with a logger without
duplicating the outcome dispatch (issue #167).

It sits above :mod:`sync` (batching, cursor movement) and
:mod:`transport` (delivery, offline-tolerant ``push``): the seam where the
two callers used to diverge.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .retention import RetentionError, maybe_prune
from .sync import DEFAULT_MAX_BYTES, DEFAULT_MAX_RECORDS, delivery_cursor
from .transport import TerminalTransportError, push, push_content_keys


@dataclass(frozen=True)
class SyncPassResult:
    """What one sync pass did, independent of the reporting sink.

    ``outcome`` is the event-push disposition: ``"ok"``, ``"offline"``,
    ``"auth"``, ``"cancelled"``, or ``"terminal"`` (the server rejected a
    batch as malformed — a client defect). ``detail`` carries the transport
    detail for a non-ok outcome.

    ``acked``, ``delivery_cursor``, and ``pending`` are read back from the
    outbox after the push, so they stay truthful even when a multi-batch
    pass ships some batches and then the network drops.

    ``key_outcome`` mirrors the wrapped-DEK side-channel ship with the same
    vocabulary, or is ``None`` when the ship was skipped (after a cancelled
    or terminal event push). The side-channel is best-effort: its outcome
    never changes the event-push disposition.

    ``pruned`` is the retention result object, or ``None`` when retention is
    disabled, throttled, or skipped. ``prune_error`` carries a refused prune.
    """

    outcome: str
    detail: str | None
    acked: int
    delivery_cursor: int
    pending: int
    key_outcome: str | None
    key_detail: str | None
    keys_sent: int
    pruned: Any | None
    prune_error: str | None


def run_sync_pass(
    outbox: Any,
    transport: Any,
    *,
    max_records: int = DEFAULT_MAX_RECORDS,
    max_bytes: int = DEFAULT_MAX_BYTES,
    max_batches: int | None = None,
    retention_config: Any | None = None,
) -> SyncPassResult:
    """Run one sync pass and return its structured result.

    A cancelled or terminal event push skips the wrapped-DEK ship and the
    prune: cancellation means the process is shutting down, and a terminal
    defect must be seen before anything else runs on the same transport.
    Every other failure still ships keys and prunes — the outbox keeps the
    events and the next pass resumes from the last ack.
    """
    before = delivery_cursor(outbox)
    detail: str | None = None
    try:
        outcome = push(
            outbox,
            transport,
            max_records=max_records,
            max_bytes=max_bytes,
            max_batches=max_batches,
        )
    except TerminalTransportError as exc:
        reason = "terminal"
        detail = str(exc)
    else:
        reason = outcome.reason
        detail = outcome.detail

    after = delivery_cursor(outbox)
    pending = outbox.high_water() - after

    key_outcome: str | None = None
    key_detail: str | None = None
    keys_sent = 0
    if reason not in ("cancelled", "terminal"):
        try:
            keys = push_content_keys(outbox, transport, max_batches=max_batches)
        except TerminalTransportError as exc:
            key_outcome = "terminal"
            key_detail = str(exc)
        else:
            key_outcome = keys.reason
            key_detail = keys.detail
            if keys.result is not None:
                keys_sent = keys.result.keys_sent

    pruned: Any | None = None
    prune_error: str | None = None
    if retention_config is not None and reason not in ("cancelled", "terminal"):
        try:
            pruned = maybe_prune(outbox, retention_config)
        except RetentionError as exc:
            prune_error = str(exc)

    return SyncPassResult(
        outcome=reason,
        detail=detail,
        acked=after - before,
        delivery_cursor=after,
        pending=pending,
        key_outcome=key_outcome,
        key_detail=key_detail,
        keys_sent=keys_sent,
        pruned=pruned,
        prune_error=prune_error,
    )


__all__ = ["SyncPassResult", "run_sync_pass"]
