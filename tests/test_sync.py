"""Tests for sync batching and the durable delivery cursor (issue #32)."""

from __future__ import annotations

import pytest

import helpers

from hermes_flight_recorder.collector.hook import CURSOR_NAME as HOOK_CURSOR_NAME
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.sync import (
    Ack,
    DELIVERY_CURSOR_NAME,
    InMemoryTransport,
    MAX_INGEST_BATCH_BYTES,
    SyncError,
    build_batches,
    delivery_cursor,
    serialize_batch,
    sync,
)

from test_outbox import base_record


def new_outbox(tmp_path) -> Outbox:
    """The outbox lives directly at ``tmp_path`` — no bridge/ subdir:
    these tests reopen the same directory (or keep config/keys beside it).
    """
    return helpers.new_outbox(tmp_path, subdir=None)


def append_n(outbox: Outbox, count: int) -> list[dict]:
    return [outbox.append(base_record()) for _ in range(count)]


def test_empty_outbox_is_a_no_op(tmp_path):
    outbox = new_outbox(tmp_path)
    transport = InMemoryTransport()

    result = sync(outbox, transport)

    assert result.batches_sent == 0
    assert result.records_sent == 0
    assert result.delivery_cursor == 0
    assert result.pending == 0
    assert transport.batches == []
    assert outbox.get_cursor(DELIVERY_CURSOR_NAME) is None
    outbox.close()


def test_sync_sends_records_in_sequence_order_and_advances_after_ack(tmp_path):
    outbox = new_outbox(tmp_path)
    append_n(outbox, 5)
    transport = InMemoryTransport()

    result = sync(outbox, transport, max_records=2)

    assert [len(batch["records"]) for batch in transport.batches] == [2, 2, 1]
    assert [
        record["producer_sequence"]
        for batch in transport.batches
        for record in batch["records"]
    ] == [1, 2, 3, 4, 5]
    assert all(batch["protocol_version"] == "1" for batch in transport.batches)
    assert delivery_cursor(outbox) == 5
    assert result.batches_sent == 3
    assert result.records_sent == 5
    assert result.pending == 0
    outbox.close()


def test_second_sync_only_sends_records_after_delivery_cursor(tmp_path):
    outbox = new_outbox(tmp_path)
    append_n(outbox, 3)
    first_transport = InMemoryTransport()
    sync(outbox, first_transport)

    append_n(outbox, 2)
    second_transport = InMemoryTransport()
    result = sync(outbox, second_transport)

    assert [r["producer_sequence"] for r in second_transport.records] == [4, 5]
    assert result.records_sent == 2
    assert result.delivery_cursor == 5
    outbox.close()


def test_sync_tick_stops_after_configured_batch_limit(tmp_path):
    outbox = new_outbox(tmp_path)
    append_n(outbox, 7)
    transport = InMemoryTransport()

    first = sync(outbox, transport, max_records=2, max_batches=2)

    assert [len(batch["records"]) for batch in transport.batches] == [2, 2]
    assert first.records_sent == 4
    assert first.delivery_cursor == 4
    assert first.pending == 3

    second = sync(outbox, transport, max_records=2, max_batches=2)

    assert [len(batch["records"]) for batch in transport.batches] == [2, 2, 2, 1]
    assert second.records_sent == 3
    assert second.delivery_cursor == 7
    assert second.pending == 0
    outbox.close()


def test_sync_rejects_non_positive_batch_limit(tmp_path):
    outbox = new_outbox(tmp_path)
    with pytest.raises(ValueError, match="max_batches"):
        sync(outbox, InMemoryTransport(), max_batches=0)
    outbox.close()


class CrashBeforeAck:
    def __init__(self, sink: InMemoryTransport):
        self.sink = sink

    def send(self, batch):
        self.sink.send(batch)  # the remote side can store it
        raise RuntimeError("process stopped before the ack returned")


class CrashOnSecondBatch:
    def __init__(self, sink: InMemoryTransport):
        self.sink = sink
        self.calls = 0

    def send(self, batch):
        self.calls += 1
        self.sink.send(batch)
        if self.calls == 2:
            raise RuntimeError("process stopped before the second ack returned")
        return Ack(
            accepted=len(batch["records"]),
            duplicates=0,
            high_water=batch["records"][-1]["producer_sequence"],
        )


def test_crash_before_ack_reships_same_batch_after_restart(tmp_path):
    outbox = new_outbox(tmp_path)
    original = append_n(outbox, 3)
    sink = InMemoryTransport()

    with pytest.raises(RuntimeError, match="before the ack"):
        sync(outbox, CrashBeforeAck(sink))
    assert delivery_cursor(outbox) == 0
    assert outbox.high_water() == 3
    outbox.close()

    reopened = Outbox.open(tmp_path)
    result = sync(reopened, sink)

    assert len(sink.batches) == 2
    assert sink.batches[0] == sink.batches[1]
    assert [r["event_id"] for r in sink.records] == [r["event_id"] for r in original]
    assert result.records_sent == 3
    assert delivery_cursor(reopened) == 3
    reopened.close()


def test_restart_resumes_after_last_acked_batch(tmp_path):
    outbox = new_outbox(tmp_path)
    append_n(outbox, 5)
    sink = InMemoryTransport()

    with pytest.raises(RuntimeError, match="second ack"):
        sync(outbox, CrashOnSecondBatch(sink), max_records=2)
    assert delivery_cursor(outbox) == 2
    outbox.close()

    reopened = Outbox.open(tmp_path)
    sync(reopened, sink, max_records=2)

    delivered_sequences = [
        [record["producer_sequence"] for record in batch["records"]]
        for batch in sink.batches
    ]
    assert delivered_sequences == [[1, 2], [3, 4], [3, 4], [5]]
    assert [record["producer_sequence"] for record in sink.records] == [1, 2, 3, 4, 5]
    assert delivery_cursor(reopened) == 5
    reopened.close()


class PartialAckTransport:
    def send(self, batch):
        return Ack(accepted=1, duplicates=0, high_water=1)


def test_incomplete_ack_does_not_advance_cursor(tmp_path):
    outbox = new_outbox(tmp_path)
    append_n(outbox, 2)

    with pytest.raises(SyncError, match="complete batch"):
        sync(outbox, PartialAckTransport())

    assert delivery_cursor(outbox) == 0
    outbox.close()


def test_batches_are_bounded_by_serialized_byte_size(tmp_path):
    outbox = new_outbox(tmp_path)
    records = append_n(outbox, 3)
    two_record_size = len(serialize_batch(next(build_batches(records, max_records=2))))

    batches = list(build_batches(records, max_bytes=two_record_size))

    assert [len(batch["records"]) for batch in batches] == [2, 1]
    assert all(len(serialize_batch(batch)) <= two_record_size for batch in batches)
    outbox.close()


def test_record_larger_than_batch_target_is_sent_alone(tmp_path):
    outbox = new_outbox(tmp_path)
    append_n(outbox, 1)
    transport = InMemoryTransport()

    result = sync(outbox, transport, max_bytes=1)

    assert len(transport.batches) == 1
    assert len(transport.batches[0]["records"]) == 1
    assert len(serialize_batch(transport.batches[0])) > 1
    assert result.records_sent == 1
    assert delivery_cursor(outbox) == 1
    outbox.close()


def test_chunked_record_and_later_event_both_clear_the_queue(tmp_path):
    outbox = new_outbox(tmp_path)
    content = b"x" * 3_200_000
    parent = outbox.append(
        base_record("knowledge.record_written"),
        content=content,
        dedup_key="large",
    )
    later = outbox.append(base_record("session.created"), dedup_key="later")
    transport = InMemoryTransport()

    result = sync(outbox, transport)

    shipped = [
        record
        for batch in transport.batches
        for record in batch["records"]
    ]
    assert [record["producer_sequence"] for record in shipped] == [1, 2, 3, 4]
    assert shipped[-2]["event_id"] == parent["event_id"]
    assert shipped[-1]["event_id"] == later["event_id"]
    assert all(
        len(serialize_batch(batch)) <= MAX_INGEST_BATCH_BYTES
        for batch in transport.batches
    )
    assert result.records_sent == 4
    assert result.pending == 0
    assert delivery_cursor(outbox) == 4
    outbox.close()


def test_legacy_oversized_record_fails_before_transport(tmp_path):
    outbox = new_outbox(tmp_path)
    record = outbox.append(base_record())
    record["payload"]["legacy_oversized"] = "x" * MAX_INGEST_BATCH_BYTES

    with pytest.raises(SyncError, match="ingestion limit"):
        list(build_batches([record]))

    outbox.close()


def test_delivery_cursor_is_independent_of_other_outbox_state(tmp_path):
    outbox = new_outbox(tmp_path)
    append_n(outbox, 3)
    outbox.set_cursor(HOOK_CURSOR_NAME, 9876)

    sync(outbox, InMemoryTransport(), max_records=2)

    assert delivery_cursor(outbox) == 3
    assert int(outbox.get_cursor(HOOK_CURSOR_NAME)) == 9876
    assert outbox.high_water() == 3
    assert DELIVERY_CURSOR_NAME != HOOK_CURSOR_NAME
    outbox.close()
