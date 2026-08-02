"""Tests for the drain's durability / crash-safety semantics (issue #4).

The spool-and-drain contract is at-least-once with dedup at the drain, keyed
on a byte-offset cursor stored in the outbox meta. These assert: the cursor
advances by the bytes of complete lines only; a partial trailing line (a
gateway that died mid-write) is left and picked up on the next drain; a
re-drain after the SAME lines (a Flight Recorder stop before the cursor committed) is
idempotent via the dedup key — no duplicate row, no consumed sequence; a
truncated/rotated spool resets the cursor; and an undecodable line is skipped
rather than sinking the pass.
"""

from __future__ import annotations

import importlib
import json
import os
from pathlib import Path

import helpers
import pytest

from hermes_flight_recorder.collector.hook import CURSOR_NAME, SPOOL_FILENAME, drain
from hermes_flight_recorder.collector.outbox import Outbox

drain_module = importlib.import_module("hermes_flight_recorder.collector.hook.drain")


def new_outbox(flight_recorder_home: Path) -> Outbox:
    """The outbox lives directly at ``flight_recorder_home`` — no bridge/ subdir:
    these tests reopen the same directory (or keep config/keys beside it).
    """
    return helpers.new_outbox(flight_recorder_home, subdir=None)


def line(event_type: str, ctx: dict, ts: float = 1.0) -> str:
    return json.dumps({"event_type": event_type, "context": ctx, "captured_at": ts})


def test_cursor_advances_to_end_of_complete_lines(tmp_path: Path) -> None:
    spool = tmp_path / SPOOL_FILENAME
    body = (line("gateway:startup", {"platforms": []}) + "\n") * 2
    spool.write_text(body)
    ob = new_outbox(tmp_path)
    drain(ob)
    assert int(ob.get_cursor(CURSOR_NAME)) == len(body.encode("utf-8"))
    ob.close()


def test_partial_trailing_line_is_deferred_then_completed(tmp_path: Path) -> None:
    spool = tmp_path / SPOOL_FILENAME
    complete = line("session:start", {"session_id": "s1", "session_key": "k1"}) + "\n"
    partial = line("agent:start", {"session_id": "s1", "message": "hi"})  # no newline
    spool.write_text(complete + partial)

    ob = new_outbox(tmp_path)
    assert drain(ob) == {"session.created": 1}  # only the complete line
    assert ob.count() == 1

    # The gateway finishes the write; the next drain picks up the rest.
    with open(spool, "a") as fh:
        fh.write("\n")
    assert drain(ob) == {"invocation.started": 1}
    assert ob.count() == 2
    ob.close()


def test_redrain_of_same_lines_is_idempotent(tmp_path: Path) -> None:
    """Simulates a Flight Recorder stop after append but before the cursor commit."""
    spool = tmp_path / SPOOL_FILENAME
    spool.write_text(line("session:start", {"session_id": "s1", "session_key": "k1"}) + "\n")

    ob = new_outbox(tmp_path)
    drain(ob)
    assert ob.count() == 1
    hw = ob.high_water()

    # Rewind the cursor as if the commit never happened, then re-drain.
    ob.set_cursor(CURSOR_NAME, 0)
    assert drain(ob) == {}  # dedup hit: nothing newly created
    assert ob.count() == 1  # no duplicate row
    assert ob.high_water() == hw  # no sequence consumed
    ob.close()


def test_truncated_spool_resets_cursor(tmp_path: Path) -> None:
    spool = tmp_path / SPOOL_FILENAME
    ob = new_outbox(tmp_path)
    spool.write_text(
        line(
            "session:start",
            {"session_id": "session-with-a-long-id", "session_key": "key"},
        )
        + "\n"
    )
    assert drain(ob) == {"session.created": 1}

    # write_text truncates the same inode and reuses offset 0.
    spool.write_text(line("gateway:startup", {"platforms": []}) + "\n")
    assert drain(ob) == {"runtime.gateway_started": 1}
    assert ob.count() == 2
    ob.close()


def test_replaced_spool_reuses_offset_without_dedup_collision(tmp_path: Path) -> None:
    spool = tmp_path / SPOOL_FILENAME
    ob = new_outbox(tmp_path)
    spool.write_text(line("gateway:startup", {"platforms": []}, 1.0) + "\n")
    assert drain(ob) == {"runtime.gateway_started": 1}
    first_generation = ob.get_meta("hook-spool:generation")

    replacement = tmp_path / "replacement"
    replacement.write_text(
        line(
            "session:start",
            {"session_id": "s1", "session_key": "k1"},
            2.0,
        )
        + "\n"
    )
    os.replace(replacement, spool)

    assert drain(ob) == {"session.created": 1}
    assert ob.count() == 2
    assert ob.get_meta("hook-spool:generation") != first_generation
    ob.close()


def test_undecodable_line_is_skipped(tmp_path: Path) -> None:
    spool = tmp_path / SPOOL_FILENAME
    spool.write_text(
        "this is not json\n"
        + line("session:start", {"session_id": "s1", "session_key": "k1"})
        + "\n"
    )
    ob = new_outbox(tmp_path)
    assert drain(ob) == {"session.created": 1}
    # The bad line is still consumed by the cursor (it is not retried forever).
    assert int(ob.get_cursor(CURSOR_NAME)) == spool.stat().st_size
    ob.close()


def test_incremental_drain_across_calls(tmp_path: Path) -> None:
    spool = tmp_path / SPOOL_FILENAME
    ob = new_outbox(tmp_path)

    spool.write_text(line("gateway:startup", {"platforms": []}) + "\n")
    assert drain(ob) == {"runtime.gateway_started": 1}

    with open(spool, "a") as fh:
        fh.write(line("session:start", {"session_id": "s1", "session_key": "k1"}) + "\n")
    # Second drain sees only the newly-appended line.
    assert drain(ob) == {"session.created": 1}
    assert ob.count() == 2
    ob.close()


def test_compaction_reuses_offsets_without_dropping_new_events(
    tmp_path: Path, monkeypatch
) -> None:
    spool = tmp_path / SPOOL_FILENAME
    ob = new_outbox(tmp_path)

    spool.write_text(line("gateway:startup", {"platforms": []}, 1.0) + "\n")
    assert drain(ob) == {"runtime.gateway_started": 1}

    # A pass at the cap compacts the consumed generation and resets offset 0.
    monkeypatch.setattr(drain_module, "MAX_SPOOL_BYTES", 1)
    assert drain(ob) == {}
    spool.write_text(
        line("session:start", {"session_id": "s1", "session_key": "k1"}, 2.0)
        + "\n"
    )

    assert drain(ob) == {"session.created": 1}
    assert ob.count() == 2
    assert int(ob.get_cursor(CURSOR_NAME)) == 0
    assert not spool.exists()
    ob.close()


def test_continuous_writes_rotate_without_a_quiet_pass(
    tmp_path: Path, monkeypatch
) -> None:
    spool = tmp_path / SPOOL_FILENAME
    ob = new_outbox(tmp_path)
    assert drain_module.MAX_SPOOL_BYTES == 64 * 1024 * 1024
    monkeypatch.setattr(drain_module, "MAX_SPOOL_BYTES", 300)

    for index in range(20):
        with spool.open("a") as fh:
            fh.write(
                line(
                    "session:start",
                    {"session_id": f"s{index}", "session_key": f"k{index}"},
                    float(index),
                )
                + "\n"
            )
        assert drain(ob) == {"session.created": 1}
        assert not spool.exists() or spool.stat().st_size < 300

    drain(ob)
    assert ob.count() == 20
    assert not list(tmp_path.glob(f"{SPOOL_FILENAME}.segment.*"))
    ob.close()


def test_partial_line_survives_cap_rotation(tmp_path: Path, monkeypatch) -> None:
    spool = tmp_path / SPOOL_FILENAME
    ob = new_outbox(tmp_path)
    complete = line("gateway:startup", {"platforms": []}, 1.0) + "\n"
    partial = line(
        "session:start", {"session_id": "s1", "session_key": "k1"}, 2.0
    )
    spool.write_text(complete + partial)
    monkeypatch.setattr(drain_module, "MAX_SPOOL_BYTES", len(complete.encode()))

    assert drain(ob) == {"runtime.gateway_started": 1}
    segment = next(tmp_path.glob(f"{SPOOL_FILENAME}.segment.*"))
    with segment.open("a") as fh:
        fh.write("\n")

    assert drain(ob) == {"session.created": 1}
    assert not segment.exists()
    ob.close()


# --- chunked drain transactions (issue #160) -------------------------------
def test_drain_commits_per_chunk_and_a_crash_rolls_back_one_chunk(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(drain_module, "_DRAIN_CHUNK_LINES", 3)
    spool = tmp_path / SPOOL_FILENAME
    body = "".join(
        line("session:start", {"session_id": f"s{i}", "session_key": f"k{i}"}) + "\n"
        for i in range(5)
    )
    spool.write_text(body)
    ob = new_outbox(tmp_path)

    real_append_if_new = ob.append_if_new
    calls = {"count": 0}

    def crash_on_fifth(record, **kwargs):
        calls["count"] += 1
        if calls["count"] == 5:
            raise RuntimeError("simulated crash mid-chunk")
        return real_append_if_new(record, **kwargs)

    monkeypatch.setattr(ob, "append_if_new", crash_on_fifth)
    with pytest.raises(RuntimeError):
        drain(ob)

    # The first chunk (lines 1-3) committed as one transaction. The second
    # chunk rolled back whole: line 4 had already appended inside it, and the
    # crash on line 5 discarded it too. The cursor never advanced and the
    # spool is untouched, so no line is dropped before it is durable.
    assert ob.count() == 3
    assert ob.get_cursor(CURSOR_NAME) is None
    assert spool.read_text() == body

    # Recovery: the next drain replays the whole generation; dedup absorbs
    # the committed chunk and the rolled-back lines land exactly once, with
    # a gapless sequence (the rollback consumed no sequence numbers).
    monkeypatch.setattr(ob, "append_if_new", real_append_if_new)
    assert drain(ob) == {"session.created": 2}
    assert ob.count() == 5
    assert int(ob.get_cursor(CURSOR_NAME)) == len(body.encode("utf-8"))
    seqs = sorted(r["producer_sequence"] for r in ob.iter_events())
    assert seqs == [1, 2, 3, 4, 5]
    ob.close()


def test_drain_of_a_large_backlog_commits_every_chunk(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(drain_module, "_DRAIN_CHUNK_LINES", 2)
    spool = tmp_path / SPOOL_FILENAME
    body = "".join(
        line("session:start", {"session_id": f"s{i}", "session_key": f"k{i}"}) + "\n"
        for i in range(7)
    )
    spool.write_text(body)
    ob = new_outbox(tmp_path)
    assert drain(ob) == {"session.created": 7}
    assert ob.count() == 7
    assert int(ob.get_cursor(CURSOR_NAME)) == len(body.encode("utf-8"))
    ob.close()
