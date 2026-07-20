"""Ship ordered outbox records through a pluggable transport.

The sync path has one durable delivery cursor. It is the highest
``producer_sequence`` that a transport acknowledged. It is not the outbox
producer high-water and it is not a producer read cursor.

This module owns batching and cursor movement only. It contains no network
code. A transport returns an :class:`Ack` after it durably accepts a batch.
If transport delivery fails, or if the process stops before the ack returns,
the cursor stays in place and the next pass sends the same records again.
"""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Protocol, TypedDict

from ..envelope import validate

PROTOCOL_VERSION = "1"
DELIVERY_CURSOR_NAME = "delivery"
DEFAULT_MAX_RECORDS = 500
DEFAULT_MAX_BYTES = 1024 * 1024


class Batch(TypedDict):
    """The ingestion protocol v1 request body."""

    protocol_version: str
    records: list[dict[str, Any]]


@dataclass(frozen=True)
class Ack:
    """A successful ingestion acknowledgement."""

    accepted: int
    duplicates: int
    high_water: int


class Transport(Protocol):
    """Deliver one batch and return only after it is acknowledged."""

    def send(self, batch: Batch) -> Ack:
        """Deliver ``batch`` or raise before acknowledgement."""
        ...


class SyncError(RuntimeError):
    """The local batch or the transport acknowledgement is not usable."""


@dataclass(frozen=True)
class SyncResult:
    """Summary of one sync pass."""

    batches_sent: int
    records_sent: int
    delivery_cursor: int
    producer_high_water: int

    @property
    def pending(self) -> int:
        """Return the sequence distance that is not yet acknowledged."""
        return max(0, self.producer_high_water - self.delivery_cursor)


class InMemoryTransport:
    """A deduplicating in-memory ingestion sink for tests.

    ``batches`` keeps every delivery attempt. ``records`` represents the
    durable server ledger and contains each ``event_id`` once.
    """

    def __init__(self) -> None:
        self.batches: list[Batch] = []
        self.records: list[dict[str, Any]] = []
        self._event_ids: set[str] = set()
        self.high_water = 0

    def send(self, batch: Batch) -> Ack:
        stored_batch = copy.deepcopy(batch)
        self.batches.append(stored_batch)

        accepted = 0
        duplicates = 0
        for record in stored_batch["records"]:
            event_id = record["event_id"]
            if event_id in self._event_ids:
                duplicates += 1
            else:
                self._event_ids.add(event_id)
                self.records.append(record)
                accepted += 1
            self.high_water = max(self.high_water, record["producer_sequence"])

        return Ack(
            accepted=accepted,
            duplicates=duplicates,
            high_water=self.high_water,
        )


def delivery_cursor(outbox: Any) -> int:
    """Read the durable delivery cursor. A new outbox starts at zero."""
    return int(outbox.get_cursor(DELIVERY_CURSOR_NAME) or 0)


def serialize_batch(batch: Batch) -> bytes:
    """Serialize a batch with the encoding used for the byte-size limit."""
    return json.dumps(
        batch,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def build_batches(
    records: Iterable[dict[str, Any]],
    *,
    max_records: int = DEFAULT_MAX_RECORDS,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> Iterator[Batch]:
    """Yield valid protocol batches within both configured limits.

    A record that cannot fit in a batch by itself is a terminal local error.
    The caller must not skip that record or advance past it.
    """
    if max_records < 1:
        raise ValueError("max_records must be at least 1")
    if max_bytes < 1:
        raise ValueError("max_bytes must be at least 1")

    current: list[dict[str, Any]] = []
    installation_id: str | None = None
    previous_sequence: int | None = None

    for record in records:
        validate(record)
        record_installation_id = record["installation_id"]
        if installation_id is None:
            installation_id = record_installation_id
        elif record_installation_id != installation_id:
            raise SyncError("one sync pass cannot mix installation_id values")

        sequence = record["producer_sequence"]
        if previous_sequence is not None and sequence <= previous_sequence:
            raise SyncError("records must be ordered by producer_sequence")
        previous_sequence = sequence

        single = _batch([record])
        if len(serialize_batch(single)) > max_bytes:
            raise SyncError(
                f"record at producer_sequence {sequence} exceeds the "
                f"{max_bytes}-byte batch limit"
            )

        candidate = _batch([*current, record])
        if current and (
            len(current) >= max_records or len(serialize_batch(candidate)) > max_bytes
        ):
            yield _batch(current)
            current = [record]
        else:
            current.append(record)

    if current:
        yield _batch(current)


def sync(
    outbox: Any,
    transport: Transport,
    *,
    max_records: int = DEFAULT_MAX_RECORDS,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> SyncResult:
    """Run one outbox-to-transport sync pass.

    Records at or below the persisted delivery cursor are already acked.
    Each later record is sent in ascending sequence order. The cursor moves
    after, and only after, a complete valid acknowledgement for each batch.
    Transport exceptions propagate so an operator can detect a failed pass.
    """
    start_cursor = delivery_cursor(outbox)
    installation_id = outbox.installation_id
    producer_high_water = outbox.high_water(installation_id)
    pending = (
        record
        for record in outbox.iter_events(installation_id)
        if record["producer_sequence"] > start_cursor
    )

    batches_sent = 0
    records_sent = 0
    cursor = start_cursor
    for batch in build_batches(
        pending,
        max_records=max_records,
        max_bytes=max_bytes,
    ):
        ack = transport.send(batch)
        _validate_ack(ack, batch)

        batch_cursor = batch["records"][-1]["producer_sequence"]
        outbox.set_cursor(DELIVERY_CURSOR_NAME, batch_cursor)
        cursor = batch_cursor
        batches_sent += 1
        records_sent += len(batch["records"])

    return SyncResult(
        batches_sent=batches_sent,
        records_sent=records_sent,
        delivery_cursor=cursor,
        producer_high_water=producer_high_water,
    )


def _batch(records: list[dict[str, Any]]) -> Batch:
    return {"protocol_version": PROTOCOL_VERSION, "records": records}


def _validate_ack(ack: Ack, batch: Batch) -> None:
    if not isinstance(ack, Ack):
        raise SyncError("transport returned an invalid acknowledgement")

    count = len(batch["records"])
    if ack.accepted < 0 or ack.duplicates < 0:
        raise SyncError("acknowledgement counts cannot be negative")
    if ack.accepted + ack.duplicates != count:
        raise SyncError("acknowledgement does not cover the complete batch")

    batch_high_water = batch["records"][-1]["producer_sequence"]
    if ack.high_water < batch_high_water:
        raise SyncError("acknowledgement high_water is below the batch high-water")


__all__ = [
    "Ack",
    "Batch",
    "DEFAULT_MAX_BYTES",
    "DEFAULT_MAX_RECORDS",
    "DELIVERY_CURSOR_NAME",
    "InMemoryTransport",
    "PROTOCOL_VERSION",
    "SyncError",
    "SyncResult",
    "Transport",
    "build_batches",
    "delivery_cursor",
    "serialize_batch",
    "sync",
]
