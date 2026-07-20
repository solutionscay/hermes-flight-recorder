"""Tests for the ingestion protocol v1 golden batch (issue #31).

The protocol contract itself is prose (docs/schema/ingestion-protocol-v1.md);
these tests pin the golden batch so the frozen contract has an executable
witness. They assert the batch round-trips (build -> serialize -> parse) and
that it satisfies the exact acceptance rules the server enforces at POST
/ingest in hermes-dbass — so the client and the service cannot drift.
"""

from __future__ import annotations

import json
from pathlib import Path

from hermes_flight_recorder.envelope import parse, serialize, validate

BATCH_PATH = Path(__file__).parent / "fixtures" / "golden_batch.json"


def _load() -> dict:
    return json.loads(BATCH_PATH.read_text())


# --- shape --------------------------------------------------------------
def test_batch_is_object_with_nonempty_records():
    batch = _load()
    assert isinstance(batch, dict)
    assert isinstance(batch.get("records"), list)
    assert len(batch["records"]) > 0


def test_protocol_version_is_v1():
    assert _load().get("protocol_version") == "1"


# --- round-trip ---------------------------------------------------------
def test_batch_round_trips():
    batch = _load()
    text = json.dumps(batch)
    again = json.loads(text)
    assert again == batch
    # Each record also round-trips through the envelope serializer/parser.
    for rec in batch["records"]:
        assert parse(serialize(rec)) == rec


# --- every record is valid envelope v1 ----------------------------------
def test_every_record_validates():
    for rec in _load()["records"]:
        validate(rec)  # raises on any violation


# --- the server's acceptance rules (mirror hermes-dbass POST /ingest) ---
def test_all_records_share_one_installation_id():
    ids = {r["installation_id"] for r in _load()["records"]}
    assert len(ids) == 1


def test_every_record_has_event_id_and_numeric_sequence():
    for r in _load()["records"]:
        assert isinstance(r.get("event_id"), str) and r["event_id"]
        seq = r.get("producer_sequence")
        assert isinstance(seq, int) and not isinstance(seq, bool)


def test_records_ascend_by_producer_sequence():
    seqs = [r["producer_sequence"] for r in _load()["records"]]
    assert seqs == sorted(seqs)
    assert len(set(seqs)) == len(seqs)  # no repeats within a batch


# --- privacy boundary: only opaque ciphertext, never plaintext content --
def test_content_fields_are_opaque_or_absent():
    for r in _load()["records"]:
        # If content travels, it travels only as the four encrypted fields;
        # a record must never carry a plaintext content body.
        assert "content" not in r
        if "content_ciphertext" in r:
            for companion in ("content_nonce", "content_hash", "key_version"):
                assert companion in r
