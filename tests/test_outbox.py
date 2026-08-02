"""Tests for the durable local outbox (issue #3)."""

from __future__ import annotations

import os
import stat
import threading
from pathlib import Path

import pytest

from hermes_flight_recorder.collector.outbox import (
    Outbox,
    OutboxError,
    default_flight_recorder_home,
)
from hermes_flight_recorder.collector.sync import (
    MAX_INGEST_BATCH_BYTES,
    singleton_batch_size,
)
from hermes_flight_recorder.envelope import EnvelopeValidationError, validate


def base_record(event_type: str = "session.created") -> dict:
    """A producer record, minus the fields the outbox stamps."""
    return {
        "occurred_at": 1752861993.417,  # source event time, set by the producer
        "tenant_id": "default",
        "profile": "default",
        "runtime": {
            "kind": "cli",
            "hermes_version": "0.18.2",
            "install_method": "git",
            "state_schema_version": 22,
        },
        "correlation_id": "corr-1",
        "source": "state.db:messages",
        "capture_method": "poll:state.db:messages",
        "payload": {"event_type": event_type},
        "partial": False,
    }


def open_outbox(tmp_path) -> Outbox:
    ob = Outbox.open(tmp_path)
    ob.initialize()
    return ob


# --- identity -----------------------------------------------------------
def test_init_is_idempotent_and_id_is_stable(tmp_path):
    ob = Outbox.open(tmp_path)
    first = ob.initialize()
    assert ob.initialize() == first  # idempotent
    ob.close()

    reopened = Outbox.open(tmp_path)
    assert reopened.installation_id == first  # survives restart
    reopened.close()


def test_outbox_lives_at_flight_recorder_path(tmp_path):
    ob = open_outbox(tmp_path)
    assert ob.path == tmp_path.resolve() / "outbox.sqlite"
    assert ob.path.exists()
    ob.close()


def test_open_makes_new_home_and_database_private_with_public_umask(tmp_path):
    home = tmp_path / "flight-recorder"
    previous_umask = os.umask(0o022)
    try:
        ob = Outbox.open(home)
    finally:
        os.umask(previous_umask)

    try:
        assert stat.S_IMODE(home.stat().st_mode) == 0o700
        for path in home.glob("outbox.sqlite*"):
            assert stat.S_IMODE(path.stat().st_mode) == 0o600
    finally:
        ob.close()


def test_open_repairs_public_permissions_on_existing_home(tmp_path):
    home = tmp_path / "flight-recorder"
    home.mkdir(mode=0o755)
    os.chmod(home, 0o755)

    ob = Outbox.open(home)
    try:
        assert stat.S_IMODE(home.stat().st_mode) == 0o700
    finally:
        ob.close()


def test_open_rejects_database_symlink(tmp_path):
    home = tmp_path / "flight-recorder"
    home.mkdir()
    target = tmp_path / "outside.sqlite"
    target.touch()
    (home / "outbox.sqlite").symlink_to(target)

    with pytest.raises(OutboxError, match="symbolic link"):
        Outbox.open(home)


def test_flight_recorder_home_env_overrides_default(tmp_path, monkeypatch):
    monkeypatch.setenv("SC_HERMES_FLIGHT_RECORDER_HOME", str(tmp_path))
    assert default_flight_recorder_home() == tmp_path


def test_default_home_is_namespaced_child_of_hermes_home(monkeypatch, tmp_path):
    monkeypatch.delenv("SC_HERMES_FLIGHT_RECORDER_HOME", raising=False)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    assert default_flight_recorder_home() == tmp_path / "flight-recorder"


def test_default_home_falls_back_to_dot_hermes(monkeypatch):
    monkeypatch.delenv("SC_HERMES_FLIGHT_RECORDER_HOME", raising=False)
    monkeypatch.delenv("HERMES_HOME", raising=False)
    assert default_flight_recorder_home() == Path.home() / ".hermes" / "flight-recorder"


def test_allows_namespaced_child_under_hermes_home(tmp_path, monkeypatch):
    # The new default location — a child of the Hermes home — is allowed.
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    ob = Outbox.open(tmp_path / "flight-recorder")
    assert ob.path == (tmp_path / "flight-recorder").resolve() / "outbox.sqlite"
    ob.close()


def test_refuses_hermes_root_itself(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    with pytest.raises(OutboxError):
        Outbox.open(tmp_path, hermes_home=tmp_path)


# --- sequence -----------------------------------------------------------
def test_append_increments_sequence_by_one(tmp_path):
    ob = open_outbox(tmp_path)
    seqs = [ob.append(base_record())["producer_sequence"] for _ in range(5)]
    assert seqs == [1, 2, 3, 4, 5]
    ob.close()


def test_appended_record_validates(tmp_path):
    ob = open_outbox(tmp_path)
    rec = ob.append(base_record())
    validate(rec)  # must not raise
    assert rec["installation_id"] == ob.installation_id
    assert "event_id" in rec and "recorded_at" in rec
    ob.close()


def test_sequence_survives_restart_without_reuse(tmp_path):
    ob = open_outbox(tmp_path)
    for _ in range(3):
        ob.append(base_record())
    assert ob.high_water() == 3
    ob.close()

    reopened = Outbox.open(tmp_path)
    rec = reopened.append(base_record())
    assert rec["producer_sequence"] == 4  # continues, no reuse
    reopened.close()


def test_invalid_record_does_not_consume_a_sequence(tmp_path):
    ob = open_outbox(tmp_path)
    bad = base_record()
    del bad["payload"]  # required field
    with pytest.raises(EnvelopeValidationError):
        ob.append(bad)
    assert ob.high_water() == 0
    assert ob.count() == 0
    # the next good append still gets sequence 1
    assert ob.append(base_record())["producer_sequence"] == 1
    ob.close()


# --- dedup --------------------------------------------------------------
def test_dedup_key_appends_once(tmp_path):
    ob = open_outbox(tmp_path)
    first = ob.append(base_record(), dedup_key="msg:5127")
    second = ob.append(base_record(), dedup_key="msg:5127")
    assert ob.count() == 1
    assert first["producer_sequence"] == 1
    assert second["event_id"] == first["event_id"]  # returned the stored row
    # dedup hit must not have consumed a sequence
    assert ob.high_water() == 1
    assert ob.append(base_record(), dedup_key="msg:9999")["producer_sequence"] == 2
    ob.close()


def test_append_if_new_reports_insert_and_dedup_hit(tmp_path):
    ob = open_outbox(tmp_path)

    assert ob.append_if_new(base_record(), dedup_key="msg:outcome") is True
    assert ob.append_if_new(base_record(), dedup_key="msg:outcome") is False

    assert ob.count() == 1
    assert ob.high_water() == 1
    ob.close()


# --- ordering -----------------------------------------------------------
def test_iter_events_in_sequence_order(tmp_path):
    ob = open_outbox(tmp_path)
    for _ in range(4):
        ob.append(base_record())
    seqs = [r["producer_sequence"] for r in ob.iter_events()]
    assert seqs == [1, 2, 3, 4]
    ob.close()


# --- content encryption -------------------------------------------------
def test_content_is_encrypted_with_hash_and_companions(tmp_path):
    ob = open_outbox(tmp_path)
    rec = ob.append(base_record("tool.call_completed"), content="secret tool args")
    assert rec["content_ciphertext"] and rec["content_nonce"] and rec["key_version"]
    assert rec["content_hash"].startswith("sha256:")
    assert "secret tool args" not in rec["content_ciphertext"]  # not plaintext
    assert ob.decrypt_content(rec) == b"secret tool args"  # round-trips
    validate(rec)
    ob.close()


def test_no_content_leaves_content_fields_absent(tmp_path):
    ob = open_outbox(tmp_path)
    rec = ob.append(base_record())
    for f in ("content_ciphertext", "content_nonce", "content_hash", "key_version"):
        assert f not in rec
    ob.close()


def test_large_content_that_still_fits_hard_limit_remains_inline(tmp_path):
    ob = open_outbox(tmp_path)
    content = b"x" * 3_000_000

    record = ob.append(base_record("tool.call_completed"), content=content)

    assert record["content_ciphertext"]
    assert "content_storage" not in record["payload"]
    assert singleton_batch_size(record) <= MAX_INGEST_BATCH_BYTES
    assert ob.decrypt_content(record) == content
    assert ob.count() == 1
    ob.close()


def test_oversized_content_is_chunked_atomically_and_restores(tmp_path):
    ob = open_outbox(tmp_path)
    content = b"x" * 3_200_000

    parent = ob.append(
        base_record("knowledge.record_written"),
        content=content,
        dedup_key="knowledge:large:v1",
    )

    events = list(ob.iter_events())
    chunks = [
        event
        for event in events
        if event["payload"]["event_type"] == "runtime.content_chunk_recorded"
    ]
    assert len(chunks) == 2
    assert [chunk["payload"]["chunk_index"] for chunk in chunks] == [0, 1]
    assert parent["producer_sequence"] == 3
    assert parent["payload"]["content_storage"] == "chunked"
    assert parent["payload"]["content_ref"] == parent["event_id"]
    assert parent["payload"]["content_chunk_count"] == 2
    assert "content_ciphertext" not in parent
    assert ob.decrypt_content(parent) == content
    assert all(
        singleton_batch_size(event) <= MAX_INGEST_BATCH_BYTES
        for event in events
    )

    duplicate = ob.append(
        base_record("knowledge.record_written"),
        content=content,
        dedup_key="knowledge:large:v1",
    )
    assert duplicate["event_id"] == parent["event_id"]
    assert ob.high_water() == 3
    assert ob.count() == 3
    ob.close()


def test_metadata_larger_than_hard_limit_is_rejected_without_sequence(tmp_path):
    ob = open_outbox(tmp_path)
    record = base_record()
    record["payload"]["unbounded"] = "x" * MAX_INGEST_BATCH_BYTES

    with pytest.raises(OutboxError, match="metadata exceeds"):
        ob.append(record)

    assert ob.high_water() == 0
    assert ob.count() == 0
    ob.close()


def test_chunk_append_failure_rolls_back_the_complete_chunk_set(tmp_path, monkeypatch):
    ob = open_outbox(tmp_path)
    original_insert = ob._insert_event
    insert_count = 0

    def fail_after_first_insert(record, *, dedup_key):
        nonlocal insert_count
        original_insert(record, dedup_key=dedup_key)
        insert_count += 1
        if insert_count == 1:
            raise RuntimeError("simulated stop during chunk append")

    monkeypatch.setattr(ob, "_insert_event", fail_after_first_insert)

    with pytest.raises(RuntimeError, match="simulated stop"):
        ob.append(
            base_record("knowledge.record_written"),
            content=b"x" * 3_200_000,
            dedup_key="knowledge:interrupted:v1",
        )

    assert ob.high_water() == 0
    assert ob.count() == 0
    ob.close()


# --- concurrency --------------------------------------------------------
def test_concurrent_appends_no_gap_no_reuse(tmp_path):
    open_outbox(tmp_path).close()  # initialize once

    threads_n, per_thread = 4, 25
    errors: list[Exception] = []

    def worker():
        try:
            ob = Outbox.open(tmp_path)
            try:
                for _ in range(per_thread):
                    ob.append(base_record())
            finally:
                ob.close()
        except Exception as exc:  # pragma: no cover - surfaced via assert
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(threads_n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, errors
    ob = Outbox.open(tmp_path)
    seqs = sorted(r["producer_sequence"] for r in ob.iter_events())
    total = threads_n * per_thread
    assert seqs == list(range(1, total + 1))  # exactly 1..N, no gap, no dup
    ob.close()


def test_concurrent_append_if_new_has_one_winner(tmp_path):
    open_outbox(tmp_path).close()
    threads_n = 4
    outcomes: list[bool] = []
    errors: list[Exception] = []
    start = threading.Barrier(threads_n)

    def worker():
        try:
            ob = Outbox.open(tmp_path)
            try:
                start.wait()
                outcomes.append(
                    ob.append_if_new(base_record(), dedup_key="msg:concurrent")
                )
            finally:
                ob.close()
        except Exception as exc:  # pragma: no cover - surfaced via assert
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(threads_n)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors, errors
    assert outcomes.count(True) == 1
    assert outcomes.count(False) == threads_n - 1
    ob = Outbox.open(tmp_path)
    assert ob.count() == 1
    assert ob.high_water() == 1
    ob.close()


# --- batched appends (issue #160) -----------------------------------------
def test_batch_commits_appends_as_one_transaction(tmp_path):
    ob = open_outbox(tmp_path)
    with ob.batch():
        first = ob.append(base_record())
        second = ob.append(base_record())
    assert (first["producer_sequence"], second["producer_sequence"]) == (1, 2)
    ob.close()

    reopened = Outbox.open(tmp_path)  # both rows are durable after the commit
    assert reopened.count() == 2
    assert reopened.high_water() == 2
    reopened.close()


def test_batch_exception_rolls_back_rows_meta_and_sequence(tmp_path):
    ob = open_outbox(tmp_path)
    ob.append(base_record())
    with pytest.raises(RuntimeError):
        with ob.batch():
            ob.append(base_record())
            ob.set_cursor("source-under-test", 7)
            ob.set_meta("meta-under-test", "42")
            raise RuntimeError("crash mid-batch")

    # Rows, cursor, and meta written inside the batch all rolled back as one.
    assert ob.count() == 1
    assert ob.get_cursor("source-under-test") is None
    assert ob.get_meta("meta-under-test") is None
    # The rolled-back sequence number is reused: no permanent gap.
    assert ob.append(base_record())["producer_sequence"] == 2
    ob.close()


def test_dedup_inside_a_batch_matches_rows_from_before_and_within(tmp_path):
    ob = open_outbox(tmp_path)
    ob.append(base_record(), dedup_key="pre-existing")
    with ob.batch():
        assert not ob.append_if_new(base_record(), dedup_key="pre-existing")
        assert ob.append_if_new(base_record(), dedup_key="fresh")
        assert not ob.append_if_new(base_record(), dedup_key="fresh")
    assert ob.count() == 2
    assert ob.high_water() == 2  # dedup hits consumed no sequence
    ob.close()


def test_nested_batch_joins_the_open_transaction(tmp_path):
    ob = open_outbox(tmp_path)
    with ob.batch():
        ob.append(base_record())
        with ob.batch():  # must join, not deadlock on the shared connection
            ob.append(base_record())
        ob.append(base_record())
    assert ob.count() == 3
    ob.close()


def test_exception_in_a_nested_batch_rolls_back_the_whole_batch(tmp_path):
    ob = open_outbox(tmp_path)
    with pytest.raises(RuntimeError):
        with ob.batch():
            ob.append(base_record())
            with ob.batch():
                ob.append(base_record())
                raise RuntimeError("inner crash")
    assert ob.count() == 0
    assert ob.high_water() == 0
    ob.close()


def test_failed_append_inside_a_batch_keeps_the_earlier_appends(tmp_path):
    ob = open_outbox(tmp_path)
    with ob.batch():
        ob.append(base_record())
        bad = base_record()
        del bad["payload"]
        with pytest.raises(EnvelopeValidationError):
            ob.append(bad)  # rejects the record, leaves the batch usable
        ob.append(base_record())
    assert ob.count() == 2
    ob.close()


def test_content_written_after_a_batch_rollback_stays_decryptable(tmp_path):
    # The rolled-back batch may have minted the process data key; its wrapped
    # form rolled back with it, so the outbox must mint a fresh key rather
    # than encrypt under a key version that no longer exists.
    ob = open_outbox(tmp_path)
    with pytest.raises(RuntimeError):
        with ob.batch():
            ob.append(base_record(), content="lost with the batch")
            raise RuntimeError("crash mid-batch")
    stored = ob.append(base_record(), content="survives")
    assert ob.decrypt_content(stored) == b"survives"
    ob.close()
