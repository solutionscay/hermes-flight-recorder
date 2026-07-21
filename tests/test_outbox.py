"""Tests for the durable local outbox (issue #3)."""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from hermes_flight_recorder.collector.outbox import (
    Outbox,
    OutboxError,
    default_flight_recorder_home,
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


def test_flight_recorder_home_env_overrides_default(tmp_path, monkeypatch):
    monkeypatch.setenv("SC_HERMES_FLIGHT_RECORDER_HOME", str(tmp_path))
    assert default_flight_recorder_home() == tmp_path


def test_legacy_home_env_is_ignored(monkeypatch, tmp_path):
    monkeypatch.delenv("SC_HERMES_FLIGHT_RECORDER_HOME", raising=False)
    monkeypatch.setenv("BRIDGE" + "_HOME", str(tmp_path))
    assert default_flight_recorder_home() == Path.home() / ".hermes-flight-recorder"


def test_refuses_path_under_hermes_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    with pytest.raises(OutboxError):
        Outbox.open(tmp_path / "inside")


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
