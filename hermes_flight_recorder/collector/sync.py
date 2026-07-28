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
# The hosted ingestion worker rejects request bodies above 4 MiB. This is a
# protocol ceiling, unlike DEFAULT_MAX_BYTES, which is only a batching target.
MAX_INGEST_BATCH_BYTES = 4 * 1024 * 1024


class Batch(TypedDict):
    """The ingestion protocol v1 request body."""

    protocol_version: str
    records: list[dict[str, Any]]


class KeyBatch(TypedDict):
    """The wrapped-DEK side-channel request body.

    A keyless companion to :class:`Batch`: each entry is an opaque wrapped DEK
    (``key_version``, ``recipient_key_id``, ``wrapped_dek``) for one
    ``installation_id``. The ingestion service stores it verbatim; it never
    unwraps it.
    """

    protocol_version: str
    keys: list[dict[str, Any]]


@dataclass(frozen=True)
class Ack:
    """A successful ingestion acknowledgement."""

    accepted: int
    duplicates: int
    high_water: int


@dataclass(frozen=True)
class KeyAck:
    """A successful wrapped-DEK acknowledgement (no producer sequence)."""

    accepted: int
    duplicates: int


class Transport(Protocol):
    """Deliver one batch and return only after it is acknowledged."""

    def send(self, batch: Batch) -> Ack:
        """Deliver ``batch`` or raise before acknowledgement."""
        ...

    def send_keys(self, batch: KeyBatch) -> KeyAck:
        """Deliver a wrapped-DEK batch or raise before acknowledgement."""
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


@dataclass(frozen=True)
class KeySyncResult:
    """Summary of one wrapped-DEK shipping pass."""

    batches_sent: int
    keys_sent: int


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
        # Wrapped-DEK side-channel ledger, deduped by (installation_id, key_version).
        self.key_batches: list[KeyBatch] = []
        self.keys: list[dict[str, Any]] = []
        self._key_ids: set[tuple[str, str]] = set()

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

    def send_keys(self, batch: KeyBatch) -> KeyAck:
        stored_batch = copy.deepcopy(batch)
        self.key_batches.append(stored_batch)

        accepted = 0
        duplicates = 0
        for record in stored_batch["keys"]:
            identity = (record["installation_id"], record["key_version"])
            if identity in self._key_ids:
                duplicates += 1
            else:
                self._key_ids.add(identity)
                self.keys.append(record)
                accepted += 1

        return KeyAck(accepted=accepted, duplicates=duplicates)


def delivery_cursor(outbox: Any) -> int:
    """Read the durable delivery cursor. A new outbox starts at zero."""
    return int(outbox.get_cursor(DELIVERY_CURSOR_NAME) or 0)


def serialize_batch(batch: "Batch | KeyBatch") -> bytes:
    """Serialize a batch with the encoding used for the byte-size limit."""
    return json.dumps(
        batch,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def singleton_batch_size(record: dict[str, Any]) -> int:
    """Return the exact wire size when ``record`` is sent by itself."""
    return len(serialize_batch(_batch([record])))


def build_batches(
    records: Iterable[dict[str, Any]],
    *,
    max_records: int = DEFAULT_MAX_RECORDS,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> Iterator[Batch]:
    """Yield valid protocol batches within the configured target limits.

    A single record can exceed the soft ``max_bytes`` target because records
    are indivisible. Yield it alone, but never emit a request above the
    protocol's hard limit. New content-bearing records are chunked by the
    outbox before they reach this path.
    """
    if max_records < 1:
        raise ValueError("max_records must be at least 1")
    if max_bytes < 1:
        raise ValueError("max_bytes must be at least 1")

    target_bytes = min(max_bytes, MAX_INGEST_BATCH_BYTES)
    current: list[dict[str, Any]] = []
    current_bytes = 0  # len(serialize_batch(_batch(current))), tracked incrementally
    envelope_bytes = len(serialize_batch(_batch([])))
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

        # Serialize each record once. Batch size is the fixed envelope plus
        # each record plus one comma between records, so the limit check
        # never re-serializes the accumulated batch.
        single_bytes = singleton_batch_size(record)
        if single_bytes > MAX_INGEST_BATCH_BYTES:
            raise SyncError(
                f"record at producer_sequence {sequence} exceeds the "
                f"{MAX_INGEST_BATCH_BYTES}-byte ingestion limit"
            )
        grown_bytes = current_bytes + (single_bytes - envelope_bytes) + 1
        if current and (len(current) >= max_records or grown_bytes > target_bytes):
            yield _batch(current)
            current = [record]
            current_bytes = single_bytes
        else:
            current.append(record)
            current_bytes = single_bytes if len(current) == 1 else grown_bytes

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
    # The cursor filter runs in SQL, so a steady-state pass never loads or
    # parses the already-acked history.
    pending = outbox.iter_events(installation_id, after_sequence=start_cursor)

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


def sync_content_keys(
    outbox: Any,
    transport: Transport,
    *,
    max_records: int = DEFAULT_MAX_RECORDS,
) -> KeySyncResult:
    """Ship wrapped DEKs that the ingestion service has not yet acknowledged.

    A companion to :func:`sync` for the keyless wrapped-DEK side-channel. Each
    record's ``shipped_at`` moves only after a complete acknowledgement, so a
    failure before the ack leaves the record unshipped and the next pass resends
    it. Delivery is idempotent server-side by ``(installation_id, key_version)``,
    so a resend after a lost ack is harmless. Transport exceptions propagate.
    """
    pending = list(outbox.iter_unshipped_content_keys())

    batches_sent = 0
    keys_sent = 0
    for batch in _build_key_batches(pending, max_records=max_records):
        ack = transport.send_keys(batch)
        _validate_key_ack(ack, batch)
        outbox.mark_content_keys_shipped(
            record["key_version"] for record in batch["keys"]
        )
        batches_sent += 1
        keys_sent += len(batch["keys"])

    return KeySyncResult(batches_sent=batches_sent, keys_sent=keys_sent)


def _build_key_batches(
    records: list[dict[str, Any]], *, max_records: int
) -> Iterator[KeyBatch]:
    """Group wrapped-DEK records into protocol batches within the limits.

    All records in a pass share one ``installation_id`` (they come from one
    outbox). Wrapped DEKs are tiny, but the hard 4 MiB request ceiling is still
    honored so a pathological accumulation never emits an over-limit body.
    """
    if max_records < 1:
        raise ValueError("max_records must be at least 1")

    installation_id: str | None = None
    current: list[dict[str, Any]] = []
    for record in records:
        record_installation_id = record["installation_id"]
        if installation_id is None:
            installation_id = record_installation_id
        elif record_installation_id != installation_id:
            raise SyncError("one key sync pass cannot mix installation_id values")

        candidate = current + [record]
        if current and (
            len(candidate) > max_records
            or len(serialize_batch(_key_batch(candidate))) > MAX_INGEST_BATCH_BYTES
        ):
            yield _key_batch(current)
            current = [record]
        else:
            current = candidate

    if current:
        yield _key_batch(current)


def _validate_key_ack(ack: KeyAck, batch: KeyBatch) -> None:
    if not isinstance(ack, KeyAck):
        raise SyncError("transport returned an invalid key acknowledgement")
    if ack.accepted < 0 or ack.duplicates < 0:
        raise SyncError("acknowledgement counts cannot be negative")
    if ack.accepted + ack.duplicates != len(batch["keys"]):
        raise SyncError("acknowledgement does not cover the complete key batch")


def _batch(records: list[dict[str, Any]]) -> Batch:
    return {"protocol_version": PROTOCOL_VERSION, "records": records}


def _key_batch(keys: list[dict[str, Any]]) -> KeyBatch:
    return {"protocol_version": PROTOCOL_VERSION, "keys": keys}


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
    "MAX_INGEST_BATCH_BYTES",
    "InMemoryTransport",
    "KeyAck",
    "KeyBatch",
    "KeySyncResult",
    "PROTOCOL_VERSION",
    "SyncError",
    "SyncResult",
    "Transport",
    "build_batches",
    "delivery_cursor",
    "serialize_batch",
    "singleton_batch_size",
    "sync",
    "sync_content_keys",
]
