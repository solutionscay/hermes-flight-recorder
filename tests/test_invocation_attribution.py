"""Invocation-window attribution and model delta tests for issue #48."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from hermes_flight_recorder.collector import state_db
from hermes_flight_recorder.collector._common import build_record
from hermes_flight_recorder.collector.hook import SPOOL_FILENAME, drain
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.recorder_config import CaptureConfig


def new_outbox(tmp_path: Path) -> Outbox:
    outbox = Outbox.open(tmp_path / "bridge")
    outbox.initialize()
    return outbox


def make_state_db(home: Path, sessions: list[tuple] | None = None) -> sqlite3.Connection:
    connection = sqlite3.connect(home / "state.db")
    connection.executescript(
        """
        CREATE TABLE sessions (id TEXT, source TEXT, parent_session_id TEXT, model TEXT,
            message_count INT, tool_call_count INT, input_tokens INT, output_tokens INT,
            estimated_cost_usd REAL, started_at REAL, ended_at REAL, end_reason TEXT,
            profile_name TEXT, expiry_finalized INT);
        CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT,
            tool_name TEXT, tool_call_id TEXT, effect_disposition TEXT, content TEXT,
            timestamp REAL);
        CREATE TABLE session_model_usage (session_id TEXT, model TEXT, task TEXT,
            api_call_count INT, input_tokens INT, output_tokens INT, cache_read_tokens INT,
            reasoning_tokens INT, estimated_cost_usd REAL, cost_status TEXT, last_seen REAL);
        CREATE TABLE async_delegations (delegation_id TEXT, origin_session TEXT,
            parent_session_id TEXT, state TEXT, delivery_state TEXT, owner_pid INT,
            dispatched_at REAL, event_json TEXT, result_json TEXT);
        """
    )
    connection.executemany(
        "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        sessions or [("root", "discord", None, "model-a", 0, 0, 0, 0, 0.0, 1.0, None, None, None, 0)],
    )
    connection.commit()
    return connection


def append_spool(home: Path, events: list[tuple[str, str, float]]) -> None:
    spool = home / SPOOL_FILENAME
    with spool.open("a", encoding="utf-8") as stream:
        for event_type, session_id, occurred_at in events:
            stream.write(json.dumps({
                "event_type": event_type,
                "context": {"session_id": session_id},
                "captured_at": occurred_at,
            }) + "\n")


def events_of(outbox: Outbox, event_type: str) -> list[dict]:
    return [
        event for event in outbox.iter_events()
        if event["payload"]["event_type"] == event_type
    ]


def test_back_to_back_turns_keep_activity_separate_and_outside_activity_unattributed(tmp_path):
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    db = make_state_db(hermes)
    db.executemany(
        "INSERT INTO messages VALUES (?, 'root', 'tool', 'terminal', NULL, NULL, '{}', ?)",
        [(1, 105.0), (2, 115.0), (3, 125.0)],
    )
    db.commit()
    db.close()

    outbox = new_outbox(tmp_path)
    append_spool(tmp_path / "bridge", [
        ("agent:start", "root", 100.0),
        ("agent:end", "root", 110.0),
        ("agent:start", "root", 120.0),
        ("agent:end", "root", 130.0),
    ])
    drain(outbox)
    starts = events_of(outbox, "invocation.started")

    state_db.poll(outbox, hermes)
    tools = events_of(outbox, "tool.call_completed")
    assert [event.get("invocation_id") for event in tools] == [
        starts[0]["invocation_id"],
        None,
        starts[1]["invocation_id"],
    ]
    assert tools[0]["payload"]["invocation_attribution"] == "inferred_from_session_window"
    assert "invocation_attribution" not in tools[1]["payload"]


def test_full_user_and_assistant_content_reuses_hook_invocation(tmp_path):
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    db = make_state_db(hermes)
    prompt = "full prompt " * 900
    response = "full response " * 700
    db.executemany(
        "INSERT INTO messages VALUES (?, 'root', ?, NULL, NULL, NULL, ?, ?)",
        [
            (1, "user", prompt, 98.0),
            (2, "assistant", "", 103.0),  # tool-call scaffold, not content
            (3, "assistant", response, 109.0),
        ],
    )
    db.commit()
    db.close()

    outbox = new_outbox(tmp_path)
    append_spool(tmp_path / "bridge", [
        ("agent:start", "root", 100.0),
        ("agent:end", "root", 110.0),
    ])
    drain(outbox)
    hook_events = [
        event for event in outbox.iter_events()
        if event["capture_method"].startswith("hook:agent:")
    ]
    assert all("content_ciphertext" not in event for event in hook_events)
    invocation_id = events_of(outbox, "invocation.started")[0]["invocation_id"]

    state_db.poll(outbox, hermes)
    durable = [
        event for event in outbox.iter_events()
        if event["source"] == "state.db:messages"
    ]
    assert [event["payload"]["message_role"] for event in durable] == [
        "user", "assistant"
    ]
    assert all(event["invocation_id"] == invocation_id for event in durable)
    assert all(event["partial"] is False for event in durable)
    assert outbox.decrypt_content(durable[0]).decode() == prompt
    assert outbox.decrypt_content(durable[1]).decode() == response


def test_content_limit_is_utf8_safe_explicit_and_applies_to_tools(tmp_path):
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    db = make_state_db(hermes)
    db.executemany(
        "INSERT INTO messages VALUES (?, 'root', ?, ?, NULL, NULL, ?, ?)",
        [
            (1, "user", None, "ééé", 98.0),
            (2, "tool", "terminal", '{"exit_code":0,"padding":"123"}', 105.0),
        ],
    )
    db.commit()
    db.close()

    outbox = new_outbox(tmp_path)
    append_spool(tmp_path / "bridge", [("agent:start", "root", 100.0)])
    drain(outbox)
    state_db.poll(
        outbox,
        hermes,
        capture_config=CaptureConfig(max_content_bytes=5),
    )

    durable = {
        event["payload"]["message_row_id"]: event
        for event in outbox.iter_events()
        if event["source"] == "state.db:messages"
    }
    user = durable[1]
    assert outbox.decrypt_content(user) == "éé".encode()
    assert user["payload"]["content_original_bytes"] == 6
    assert user["payload"]["content_captured_bytes"] == 4
    assert user["payload"]["content_truncated"] is True
    assert user["partial"] is True

    tool = durable[2]
    assert outbox.decrypt_content(tool) == b'{"exi'
    assert tool["payload"]["status"] == "ok"  # derived before truncation
    assert tool["payload"]["content_truncated"] is True
    assert tool["partial"] is True


def test_message_role_configuration_filters_state_capture(tmp_path):
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    db = make_state_db(hermes)
    db.executemany(
        "INSERT INTO messages VALUES (?, 'root', ?, ?, NULL, NULL, ?, ?)",
        [
            (1, "user", None, "prompt", 98.0),
            (2, "assistant", None, "response", 109.0),
            (3, "tool", "terminal", "{}", 105.0),
        ],
    )
    db.commit()
    db.close()

    outbox = new_outbox(tmp_path)
    counts = state_db.poll(
        outbox,
        hermes,
        capture_config=CaptureConfig(message_roles=("user",)),
    )

    assert counts.get("invocation.started") == 1
    assert "invocation.completed" not in counts
    assert "tool.call_completed" not in counts


def test_versioned_message_cursor_backfills_rows_skipped_by_tool_only_releases(tmp_path):
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    db = make_state_db(hermes)
    db.executemany(
        "INSERT INTO messages VALUES (?, 'root', ?, ?, NULL, NULL, ?, ?)",
        [
            (1, "user", None, "historical prompt", 98.0),
            (2, "tool", "terminal", "{}", 105.0),
            (3, "assistant", None, "historical response", 109.0),
        ],
    )
    db.commit()
    db.close()

    outbox = new_outbox(tmp_path)
    # Older recorder versions advanced this cursor to the table high-water
    # while capturing only tools.
    outbox.append(
        build_record(
            event_type="tool.call_completed",
            occurred_at=105.0,
            source="state.db:messages",
            capture_method="poll:state.db:messages",
            runtime={"kind": "tool", "engine": "standard"},
            correlation_id="root",
            session_id="root",
            payload={"message_row_id": 2},
        ),
        content="{}",
        dedup_key="state.db:tool:2",
    )
    outbox.set_cursor("state.db:messages", 3)

    counts = state_db.poll(outbox, hermes)

    assert counts["invocation.started"] == 1
    assert counts["invocation.completed"] == 1
    assert "tool.call_completed" not in counts
    assert outbox.get_cursor("state.db:messages") == "3"
    assert outbox.get_cursor("state.db:messages:v2") == "3"
    count_after_backfill = outbox.count()
    assert state_db.poll(outbox, hermes).get("invocation.started", 0) == 0
    assert outbox.count() == count_after_backfill


def test_polling_before_and_after_terminal_hook_keeps_one_attribution(tmp_path):
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    db = make_state_db(hermes)
    db.execute(
        "INSERT INTO messages VALUES (1, 'root', 'tool', 'terminal', NULL, NULL, '{}', 205.0)"
    )
    db.commit()

    outbox = new_outbox(tmp_path)
    append_spool(tmp_path / "bridge", [("agent:start", "root", 200.0)])
    drain(outbox)
    invocation_id = events_of(outbox, "invocation.started")[0]["invocation_id"]
    state_db.poll(outbox, hermes)
    assert events_of(outbox, "tool.call_completed")[0]["invocation_id"] == invocation_id

    append_spool(tmp_path / "bridge", [("agent:end", "root", 210.0)])
    drain(outbox)
    db.execute(
        "INSERT INTO messages VALUES (2, 'root', 'tool', 'terminal', NULL, NULL, '{}', 206.0)"
    )
    db.commit()
    db.close()
    state_db.poll(outbox, hermes)

    tools = events_of(outbox, "tool.call_completed")
    assert [event["invocation_id"] for event in tools] == [invocation_id, invocation_id]
    assert events_of(outbox, "invocation.completed")[0]["invocation_id"] == invocation_id


def test_child_activity_is_not_attached_to_overlapping_root_turn(tmp_path):
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    sessions = [
        ("root", "discord", None, "model-a", 0, 0, 0, 0, 0.0, 1.0, None, None, None, 0),
        ("child", "subagent", "root", "model-a", 0, 0, 0, 0, 0.0, 301.0, None, None, None, 0),
    ]
    db = make_state_db(hermes, sessions)
    db.executemany(
        "INSERT INTO messages VALUES (?, ?, 'tool', 'terminal', NULL, NULL, '{}', ?)",
        [(1, "root", 310.0), (2, "child", 311.0)],
    )
    db.execute(
        "INSERT INTO async_delegations VALUES "
        "('deleg-1', 'root', 'root', 'completed', 'delivered', 1, 312.0, '{}', '{}')"
    )
    db.commit()
    db.close()

    outbox = new_outbox(tmp_path)
    append_spool(tmp_path / "bridge", [
        ("agent:start", "root", 300.0),
        ("agent:end", "root", 330.0),
    ])
    drain(outbox)
    invocation_id = events_of(outbox, "invocation.started")[0]["invocation_id"]
    state_db.poll(outbox, hermes)

    tools = {event["session_id"]: event for event in events_of(outbox, "tool.call_completed")}
    assert tools["root"]["invocation_id"] == invocation_id
    assert tools["child"].get("invocation_id") is None
    delegation = events_of(outbox, "delegation.dispatched")[0]
    assert delegation["payload"]["state"] == "completed"
    assert delegation["invocation_id"] == invocation_id


def test_later_start_caps_incomplete_turn_without_guessing_before_it(tmp_path):
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    db = make_state_db(hermes)
    db.executemany(
        "INSERT INTO messages VALUES (?, 'root', 'tool', 'terminal', NULL, NULL, '{}', ?)",
        [(1, 395.0), (2, 405.0), (3, 425.0)],
    )
    db.commit()
    db.close()

    outbox = new_outbox(tmp_path)
    append_spool(tmp_path / "bridge", [
        ("agent:start", "root", 400.0),
        ("agent:start", "root", 420.0),
    ])
    drain(outbox)
    starts = events_of(outbox, "invocation.started")
    state_db.poll(outbox, hermes)

    tools = events_of(outbox, "tool.call_completed")
    assert [event.get("invocation_id") for event in tools] == [
        None,
        starts[0]["invocation_id"],
        starts[1]["invocation_id"],
    ]


def test_model_usage_emits_idempotent_deltas_and_tracks_updates(tmp_path):
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    db = make_state_db(hermes)
    db.execute(
        "INSERT INTO session_model_usage VALUES "
        "('root', 'model-a', '', 1, 10, 2, 3, 1, 0.10, 'estimated', 510.0)"
    )
    db.commit()

    outbox = new_outbox(tmp_path)
    append_spool(tmp_path / "bridge", [
        ("agent:start", "root", 500.0),
        ("agent:end", "root", 600.0),
    ])
    drain(outbox)
    invocation_id = events_of(outbox, "invocation.started")[0]["invocation_id"]
    state_db.poll(outbox, hermes)

    db.execute(
        "UPDATE session_model_usage SET api_call_count=2, input_tokens=25, "
        "output_tokens=5, cache_read_tokens=7, reasoning_tokens=2, "
        "estimated_cost_usd=0.25, last_seen=520.0"
    )
    db.commit()
    state_db.poll(outbox, hermes)
    count_after_update = outbox.count()
    assert state_db.poll(outbox, hermes).get("model.usage_recorded", 0) == 0
    assert outbox.count() == count_after_update
    db.close()

    usage = events_of(outbox, "model.usage_recorded")
    assert len(usage) == 2
    assert all(event["invocation_id"] == invocation_id for event in usage)
    assert all(event["payload"]["usage_semantics"] == "monotonic_delta" for event in usage)
    assert [event["payload"]["input_tokens"] for event in usage] == [10, 15]
    assert [event["payload"]["api_call_count"] for event in usage] == [1, 1]
    assert sum(event["payload"]["estimated_cost_usd"] for event in usage) == 0.25
    assert usage[-1]["payload"]["cumulative_input_tokens"] == 25
