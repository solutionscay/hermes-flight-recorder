"""Tests for the Flight Recorder-side drain event mapping (issue #4).

The drain reads raw spool lines and produces canonical envelope v1 records.
These write spool lines directly (independent of the in-gateway handler) and
assert the per-event mapping: the event_type/capture_method/source, the
``partial`` flags the acceptance criteria require, content encryption +
``content_hash`` on agent turns, ``occurred_at`` taken from ``captured_at``,
best-effort ``correlation_id``/``invocation_id`` synthesis, ``session_id``
recovery on session-end from the in-drain ``session_key`` map, and that an
unmapped event type is ignored.
"""

from __future__ import annotations

import json
from pathlib import Path

from hermes_flight_recorder.collector.hook import SPOOL_FILENAME, drain
from hermes_flight_recorder.collector.outbox import Outbox


def new_outbox(flight_recorder_home: Path) -> Outbox:
    ob = Outbox.open(flight_recorder_home)
    ob.initialize()
    return ob


def write_spool(flight_recorder_home: Path, events: list[tuple[str, dict, float]]) -> None:
    """Write (event_type, context, captured_at) tuples as the whole spool file."""
    lines = [
        json.dumps({"event_type": et, "context": ctx, "captured_at": ts})
        for et, ctx, ts in events
    ]
    (flight_recorder_home / SPOOL_FILENAME).write_text("\n".join(lines) + "\n")


def append_spool(flight_recorder_home: Path, events: list[tuple[str, dict, float]]) -> None:
    """Append tuples to an existing spool file, as the real spooler does."""
    lines = [
        json.dumps({"event_type": et, "context": ctx, "captured_at": ts})
        for et, ctx, ts in events
    ]
    with open(flight_recorder_home / SPOOL_FILENAME, "a") as fh:
        fh.write("\n".join(lines) + "\n")


def drain_to_records(flight_recorder_home: Path) -> list[dict]:
    ob = new_outbox(flight_recorder_home)
    drain(ob)
    records = list(ob.iter_events())
    ob.close()
    return records


def by_type(records: list[dict]) -> dict[str, dict]:
    return {r["payload"]["event_type"]: r for r in records}


def test_gateway_startup_maps_to_runtime_started(tmp_path: Path) -> None:
    write_spool(tmp_path, [("gateway:startup", {"platforms": ["cli", "tui"]}, 100.0)])
    rec = drain_to_records(tmp_path)[0]
    assert rec["payload"]["event_type"] == "runtime.gateway_started"
    assert rec["capture_method"] == "hook:gateway:startup"
    assert rec["source"] == "hook:gateway"
    assert rec["occurred_at"] == 100.0
    assert rec["partial"] is False
    assert rec["payload"]["platforms"] == ["cli", "tui"]


def test_session_start_maps_to_session_created(tmp_path: Path) -> None:
    write_spool(
        tmp_path,
        [("session:start", {"session_id": "s1", "session_key": "k1", "platform": "cli"}, 5.0)],
    )
    rec = drain_to_records(tmp_path)[0]
    assert rec["payload"]["event_type"] == "session.created"
    assert rec["capture_method"] == "hook:session:start"
    assert rec["session_id"] == "s1"
    assert rec["session_key"] == "k1"
    assert rec["correlation_id"] == "s1"


def test_agent_start_end_are_partial_with_encrypted_content(tmp_path: Path) -> None:
    write_spool(
        tmp_path,
        [
            ("agent:start", {"session_id": "s1", "message": "the prompt"}, 6.0),
            ("agent:end", {"session_id": "s1", "response": "the answer"}, 7.0),
        ],
    )
    recs = by_type(drain_to_records(tmp_path))
    start, end = recs["invocation.started"], recs["invocation.completed"]

    for rec, method in ((start, "hook:agent:start"), (end, "hook:agent:end")):
        assert rec["partial"] is True
        assert rec["capture_method"] == method
        assert rec["correlation_id"] == "s1"
        assert rec["invocation_id"].startswith("s1:hook:")
        # Content is encrypted with an integrity hash; plaintext never leaks
        # into the payload.
        assert "content_ciphertext" in rec and rec["content_hash"].startswith("sha256:")
        assert "message" not in rec["payload"] and "response" not in rec["payload"]


def test_agent_start_and_end_share_one_invocation_id(tmp_path: Path) -> None:
    """Issue #23: a completed turn must pair, or the reconciler false-flags it."""
    write_spool(
        tmp_path,
        [
            ("agent:start", {"session_id": "s1", "message": "the prompt"}, 6.0),
            ("agent:end", {"session_id": "s1", "response": "the answer"}, 7.0),
        ],
    )
    recs = by_type(drain_to_records(tmp_path))
    assert recs["invocation.started"]["invocation_id"] == recs["invocation.completed"]["invocation_id"]


def test_agent_start_and_end_pair_across_separate_drains(tmp_path: Path) -> None:
    """A start drained in one `run` and an end drained in a later one still pair."""
    write_spool(tmp_path, [("agent:start", {"session_id": "s1", "message": "the prompt"}, 6.0)])
    ob = new_outbox(tmp_path)
    drain(ob)
    started = next(ob.iter_events())

    append_spool(tmp_path, [("agent:end", {"session_id": "s1", "response": "the answer"}, 7.0)])
    drain(ob)
    records = list(ob.iter_events())
    ob.close()

    ended = by_type(records)["invocation.completed"]
    assert ended["invocation_id"] == started["invocation_id"]


def test_agent_start_without_end_gets_a_fresh_id_next_time(tmp_path: Path) -> None:
    """A dropped end must not silently pair with an unrelated later start."""
    write_spool(tmp_path, [("agent:start", {"session_id": "s1", "message": "first"}, 6.0)])
    ob = new_outbox(tmp_path)
    drain(ob)
    first_start = next(ob.iter_events())

    # No agent:end for the first turn; a second turn starts fresh.
    append_spool(tmp_path, [("agent:start", {"session_id": "s1", "message": "second"}, 8.0)])
    drain(ob)
    records = list(ob.iter_events())
    ob.close()

    starts = [r for r in records if r["payload"]["event_type"] == "invocation.started"]
    assert len(starts) == 2
    assert starts[1]["invocation_id"] != first_start["invocation_id"]


def test_agent_content_is_actually_the_text(tmp_path: Path) -> None:
    write_spool(tmp_path, [("agent:start", {"session_id": "s1", "message": "hello"}, 6.0)])
    ob = new_outbox(tmp_path)
    drain(ob)
    rec = next(ob.iter_events())
    assert ob.decrypt_content(rec) == b"hello"
    ob.close()


def test_session_end_recovers_session_id_from_start(tmp_path: Path) -> None:
    write_spool(
        tmp_path,
        [
            ("session:start", {"session_id": "s1", "session_key": "k1"}, 5.0),
            ("session:end", {"session_key": "k1"}, 9.0),
        ],
    )
    ended = by_type(drain_to_records(tmp_path))["session.ended"]
    assert ended["session_id"] == "s1"  # recovered via the session_key map
    assert ended["correlation_id"] == "s1"
    assert ended["partial"] is True
    assert ended["payload"]["reason"] == "end"


def test_session_end_without_start_falls_back_to_session_key(tmp_path: Path) -> None:
    write_spool(tmp_path, [("session:end", {"session_key": "orphan"}, 9.0)])
    ended = drain_to_records(tmp_path)[0]
    assert ended.get("session_id") is None
    assert ended["session_key"] == "orphan"
    assert ended["correlation_id"] == "orphan"
    assert ended["partial"] is True


def test_session_reset_maps_to_session_ended_with_reason(tmp_path: Path) -> None:
    write_spool(tmp_path, [("session:reset", {"session_key": "k1"}, 9.0)])
    ended = drain_to_records(tmp_path)[0]
    assert ended["payload"]["event_type"] == "session.ended"
    assert ended["capture_method"] == "hook:session:reset"
    assert ended["payload"]["reason"] == "reset"


def test_unknown_event_is_ignored(tmp_path: Path) -> None:
    write_spool(
        tmp_path,
        [
            ("agent:step", {"session_id": "s1", "iteration": 2}, 6.0),  # not mapped
            ("session:start", {"session_id": "s1", "session_key": "k1"}, 5.0),
        ],
    )
    records = drain_to_records(tmp_path)
    assert [r["payload"]["event_type"] for r in records] == ["session.created"]


def test_missing_spool_is_a_noop(tmp_path: Path) -> None:
    ob = new_outbox(tmp_path)
    assert drain(ob) == {}
    assert ob.count() == 0
    ob.close()
