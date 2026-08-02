"""Shipping the wrapped-DEK side-channel to the ingestion service (FR#121 glue).

Content is sealed to the operator public key and the wrapped DEK lives in the
outbox ``content_keys`` table; these tests cover shipping those opaque blobs to
the ingestion service's ``/ingest/keys`` endpoint out of band from events:
durable ack via ``shipped_at``, idempotency, offline tolerance, and the HTTPS
transport's key endpoint.
"""

from __future__ import annotations

import json

import pytest

import helpers

from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector import keystore
from hermes_flight_recorder.collector.sync import (
    InMemoryTransport,
    KeyAck,
    SyncError,
    sync_content_keys,
)
from hermes_flight_recorder.collector.transport import (
    HttpsTransport,
    RetryableTransportError,
    RetryingTransport,
    push_content_keys,
)

from test_outbox import base_record
from test_transport import _FakeResponse, urlopen_returning


def new_outbox(tmp_path) -> Outbox:
    """The outbox lives directly at ``tmp_path`` — no bridge/ subdir:
    these tests reopen the same directory (or keep config/keys beside it).
    """
    return helpers.new_outbox(tmp_path, subdir=None)


def _seal_some_content(outbox, text="secret") -> str:
    """Append content (minting/sealing this process's DEK) and return key_version."""
    rec = outbox.append(base_record("tool.call_completed"), content=text)
    return rec["key_version"]


# --- outbox read/mark ---------------------------------------------------
def test_unshipped_records_carry_the_wire_shape(tmp_path):
    outbox = new_outbox(tmp_path)
    key_version = _seal_some_content(outbox)
    operator_key_id = keystore.load_public_key(tmp_path).key_id

    pending = list(outbox.iter_unshipped_content_keys())
    assert len(pending) == 1
    record = pending[0]
    assert record == {
        "installation_id": outbox.installation_id,
        "key_version": key_version,
        "recipient_key_id": operator_key_id,  # the operator key epoch it seals to
        "wrapped_dek": record["wrapped_dek"],
    }
    assert record["wrapped_dek"]  # opaque base64, non-empty
    outbox.close()


def test_mark_shipped_excludes_from_next_pass(tmp_path):
    outbox = new_outbox(tmp_path)
    key_version = _seal_some_content(outbox)

    outbox.mark_content_keys_shipped([key_version])
    assert list(outbox.iter_unshipped_content_keys()) == []
    outbox.close()


# --- sync_content_keys --------------------------------------------------
def test_ships_pending_wrapped_deks_and_marks_them(tmp_path):
    outbox = new_outbox(tmp_path)
    key_version = _seal_some_content(outbox)
    transport = InMemoryTransport()

    result = sync_content_keys(outbox, transport)

    assert result.keys_sent == 1
    assert result.batches_sent == 1
    assert len(transport.keys) == 1
    shipped = transport.keys[0]
    assert shipped["key_version"] == key_version
    assert shipped["installation_id"] == outbox.installation_id
    # A second pass ships nothing (the first was durably acked).
    assert sync_content_keys(outbox, transport).keys_sent == 0
    assert len(transport.keys) == 1
    outbox.close()


def test_zero_knowledge_wrapped_dek_ships_verbatim(tmp_path):
    outbox = new_outbox(tmp_path)
    _seal_some_content(outbox)
    stored = list(outbox.iter_unshipped_content_keys())[0]["wrapped_dek"]
    transport = InMemoryTransport()

    sync_content_keys(outbox, transport)

    assert transport.keys[0]["wrapped_dek"] == stored  # byte-for-byte, never unwrapped
    outbox.close()


def test_multiple_epochs_all_ship(tmp_path):
    # Two processes/epochs against one operator key -> two content_keys rows.
    ob1 = new_outbox(tmp_path)
    kv1 = _seal_some_content(ob1, "one")
    ob1.close()
    ob2 = new_outbox(tmp_path)
    kv2 = _seal_some_content(ob2, "two")

    transport = InMemoryTransport()
    result = sync_content_keys(ob2, transport)

    assert result.keys_sent == 2
    assert {k["key_version"] for k in transport.keys} == {kv1, kv2}
    ob2.close()


def test_empty_outbox_ships_nothing(tmp_path):
    outbox = new_outbox(tmp_path)
    transport = InMemoryTransport()
    result = sync_content_keys(outbox, transport)
    assert result.keys_sent == 0 and result.batches_sent == 0
    assert transport.keys == []
    outbox.close()


def test_batches_respect_max_records(tmp_path):
    ob = new_outbox(tmp_path)
    # Three epochs -> three rows.
    kvs = []
    for i in range(3):
        o = new_outbox(tmp_path)
        kvs.append(_seal_some_content(o, f"c{i}"))
        o.close()
    transport = InMemoryTransport()

    result = sync_content_keys(ob, transport, max_records=1)

    assert result.keys_sent == 3
    assert result.batches_sent == 3
    assert len(transport.key_batches) == 3
    ob.close()


# --- resilience / idempotency ------------------------------------------
class _FlakyKeyTransport:
    """Fails send_keys until ``fail_times`` is exhausted (retryable)."""

    def __init__(self, fail_times: int):
        self.fail_times = fail_times
        self.sent: list = []

    def send_keys(self, batch):
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RetryableTransportError("network down")
        self.sent.append(batch)
        return KeyAck(accepted=len(batch["records"]), duplicates=0)


def test_offline_leaves_records_unshipped_for_next_pass(tmp_path):
    outbox = new_outbox(tmp_path)
    _seal_some_content(outbox)
    transport = _FlakyKeyTransport(fail_times=99)

    outcome = push_content_keys(outbox, transport)

    assert outcome.ok is False and outcome.reason == "offline"
    # shipped_at stayed unset, so the next (online) pass will resend.
    assert len(list(outbox.iter_unshipped_content_keys())) == 1
    outbox.close()


def test_resend_after_lost_ack_is_deduped_server_side(tmp_path):
    outbox = new_outbox(tmp_path)
    _seal_some_content(outbox)
    transport = InMemoryTransport()

    # First pass ships and marks.
    sync_content_keys(outbox, transport)
    # Simulate a lost ack: force the record unshipped and resend.
    outbox._conn.execute("UPDATE content_keys SET shipped_at=NULL")
    result = sync_content_keys(outbox, transport)

    assert result.keys_sent == 1  # attempted again
    # The server-side ledger still holds exactly one (idempotent by identity).
    assert len(transport.keys) == 1
    outbox.close()


def test_mixed_installation_id_is_rejected(tmp_path):
    outbox = new_outbox(tmp_path)

    class _TwoInstalls:
        def iter_unshipped_content_keys(self):
            yield {"installation_id": "a", "key_version": "a#1", "recipient_key_id": "opk1:x", "wrapped_dek": "w"}
            yield {"installation_id": "b", "key_version": "b#1", "recipient_key_id": "opk1:y", "wrapped_dek": "w"}

    with pytest.raises(SyncError, match="mix installation_id"):
        sync_content_keys(_TwoInstalls(), InMemoryTransport())
    outbox.close()


# --- HTTPS transport key endpoint --------------------------------------
def test_https_transport_derives_keys_url():
    t = HttpsTransport(ingest_url="https://host/ingest", headers={})
    assert t.keys_url == "https://host/ingest/keys"


def test_https_send_keys_posts_to_keys_url_and_parses_ack():
    captured: list = []
    body = json.dumps({"accepted": 2, "duplicates": 1}).encode()
    t = HttpsTransport(
        ingest_url="https://host/ingest",
        headers={"CF-Access-Client-Id": "cid"},
        _urlopen=urlopen_returning(_FakeResponse(202, body), capture=captured),
    )

    ack = t.send_keys(
        {
            "protocol_version": "1",
            "records": [
                {
                    "installation_id": "i",
                    "key_version": "k1",
                    "recipient_key_id": "opk1:x",
                    "wrapped_dek": "w",
                }
            ],
        }
    )

    assert ack == KeyAck(accepted=2, duplicates=1)
    assert captured[0].full_url == "https://host/ingest/keys"
    assert captured[0].get_header("Cf-access-client-id") == "cid"  # auth reused
    assert json.loads(captured[0].data) == {
        "protocol_version": "1",
        "records": [
            {
                "installation_id": "i",
                "key_version": "k1",
                "recipient_key_id": "opk1:x",
                "wrapped_dek": "w",
            }
        ],
    }


def test_retrying_transport_retries_send_keys():
    inner = _FlakyKeyTransport(fail_times=2)
    retrying = RetryingTransport(inner, max_attempts=5, sleep=lambda _s: None, rng=lambda: 0.0)

    ack = retrying.send_keys(
        {
            "protocol_version": "1",
            "records": [
                {
                    "installation_id": "i",
                    "key_version": "k",
                    "recipient_key_id": "r",
                    "wrapped_dek": "w",
                }
            ],
        }
    )

    assert ack.accepted == 1
    assert inner.fail_times == 0  # exhausted the transient failures


# --- serialize-once key batching (issue #166) ----------------------------
def _fresh_key_wire(batch) -> bytes:
    return json.dumps(
        {"protocol_version": batch["protocol_version"], "records": batch["records"]},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _reference_key_grouping(records, max_records):
    """The pre-#166 O(n^2) grouping semantics, by full re-serialization."""
    from hermes_flight_recorder.collector.sync import MAX_INGEST_BATCH_BYTES

    groups, current = [], []
    for record in records:
        candidate = current + [record]
        size = len(_fresh_key_wire({"protocol_version": "1", "records": candidate}))
        if current and (len(candidate) > max_records or size > MAX_INGEST_BATCH_BYTES):
            groups.append(current)
            current = [record]
        else:
            current = candidate
    if current:
        groups.append(current)
    return groups


def _wrapped_key_record(index: int, dek_bytes: int = 120) -> dict:
    return {
        "installation_id": "inst-1",
        "key_version": f"op#{index:016x}",
        "recipient_key_id": "op",
        "wrapped_dek": "A" * dek_bytes,
    }


def test_key_batches_match_prior_grouping_and_exact_wire_bytes():
    from hermes_flight_recorder.collector.sync import (
        _build_key_batches,
        serialize_batch,
    )

    # A mixed workload: many small wrapped DEKs plus pathological large ones
    # that force the byte ceiling (not just max_records) to split batches.
    records = [_wrapped_key_record(i) for i in range(7)]
    records += [_wrapped_key_record(100 + i, dek_bytes=1_500_000) for i in range(3)]

    batches = list(_build_key_batches(records, max_records=4))

    assert [batch["records"] for batch in batches] == _reference_key_grouping(
        records, max_records=4
    )
    assert [
        record["key_version"] for batch in batches for record in batch["records"]
    ] == [record["key_version"] for record in records]
    for batch in batches:
        assert batch["protocol_version"] == "1"
        assert serialize_batch(batch) == _fresh_key_wire(batch)
