"""Tests for the drain's durability / crash-safety semantics (issue #4).

The spool-and-drain contract is at-least-once with dedup at the drain, keyed
on a byte-offset cursor stored in the outbox meta. These assert: the cursor
advances by the bytes of complete lines only; a partial trailing line (a
gateway that died mid-write) is left and picked up on the next drain; a
re-drain after the SAME lines (a Bridge stop before the cursor committed) is
idempotent via the dedup key — no duplicate row, no consumed sequence; a
truncated/rotated spool resets the cursor; and an undecodable line is skipped
rather than sinking the pass.
"""

from __future__ import annotations

import json
from pathlib import Path

from hermes_flight_recorder.collector.hook import CURSOR_NAME, SPOOL_FILENAME, drain
from hermes_flight_recorder.collector.outbox import Outbox


def new_outbox(bridge_home: Path) -> Outbox:
    ob = Outbox.open(bridge_home)
    ob.initialize()
    return ob


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
    """Simulates a Bridge stop after append but before the cursor commit."""
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
    ob = new_outbox(tmp_path)
    # Cursor points past the end of a now-smaller spool (as after a rotation).
    ob.set_cursor(CURSOR_NAME, 10_000)
    (tmp_path / SPOOL_FILENAME).write_text(line("gateway:startup", {"platforms": []}) + "\n")
    counts = drain(ob)
    assert counts == {"runtime.gateway_started": 1}
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
