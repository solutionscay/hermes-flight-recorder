"""Foreground knowledge mutation capture from the real Hermes message schema."""

from __future__ import annotations

import json
import sqlite3

import pytest

from hermes_flight_recorder.collector import keystore, knowledge_store, state_db
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.envelope import validate


def new_outbox(tmp_path) -> Outbox:
    outbox = Outbox.open(tmp_path / "bridge")
    outbox.initialize()
    return outbox


def make_state_db(home, calls) -> None:
    db = sqlite3.connect(home / "state.db")
    db.executescript(
        """
        CREATE TABLE sessions (
            id TEXT, source TEXT, parent_session_id TEXT, model TEXT,
            message_count INT, tool_call_count INT, input_tokens INT,
            output_tokens INT, estimated_cost_usd REAL, started_at REAL,
            ended_at REAL, end_reason TEXT, profile_name TEXT,
            expiry_finalized INT
        );
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            role TEXT NOT NULL,
            content TEXT,
            tool_call_id TEXT,
            tool_calls TEXT,
            tool_name TEXT,
            effect_disposition TEXT,
            timestamp REAL NOT NULL,
            token_count INTEGER,
            finish_reason TEXT,
            reasoning TEXT,
            reasoning_content TEXT,
            reasoning_details TEXT,
            codex_reasoning_items TEXT,
            codex_message_items TEXT,
            platform_message_id TEXT,
            observed INTEGER DEFAULT 0,
            active INTEGER NOT NULL DEFAULT 1,
            compacted INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE session_model_usage (
            session_id TEXT, model TEXT, task TEXT, api_call_count INT,
            input_tokens INT, output_tokens INT, cache_read_tokens INT,
            reasoning_tokens INT, estimated_cost_usd REAL, cost_status TEXT,
            last_seen REAL
        );
        CREATE TABLE async_delegations (
            delegation_id TEXT, origin_session TEXT, parent_session_id TEXT,
            state TEXT, delivery_state TEXT, owner_pid INT, dispatched_at REAL,
            event_json TEXT, result_json TEXT
        );
        """
    )
    db.execute(
        "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("S", "cli", None, "m", len(calls) * 2, len(calls), 0, 0, 0.0,
         1000.0, None, None, "default", 0),
    )
    for index, (tool_name, arguments, result) in enumerate(calls, start=1):
        call_id = f"call-{index}"
        assistant_id = index * 2 - 1
        result_id = index * 2
        tool_calls = json.dumps(
            [
                {
                    "id": call_id,
                    "call_id": call_id,
                    "type": "function",
                    "function": {
                        "name": tool_name,
                        "arguments": json.dumps(arguments, ensure_ascii=False),
                    },
                }
            ]
        )
        db.execute(
            "INSERT INTO messages("
            "id,session_id,role,content,tool_calls,timestamp,finish_reason"
            ") VALUES (?,?,?,?,?,?,?)",
            (assistant_id, "S", "assistant", "", tool_calls,
             1000.0 + assistant_id, "tool_calls"),
        )
        db.execute(
            "INSERT INTO messages("
            "id,session_id,role,content,tool_call_id,tool_name,timestamp"
            ") VALUES (?,?,?,?,?,?,?)",
            (result_id, "S", "tool", json.dumps(result), call_id, tool_name,
             1000.0 + result_id),
        )
    db.commit()
    db.close()


def knowledge_events(outbox, event_type="knowledge.record_written"):
    return [
        event
        for event in outbox.iter_events()
        if event["payload"]["event_type"] == event_type
    ]


def test_create_and_delete_between_scans_keep_restorable_history(tmp_path):
    home = tmp_path / "hermes"
    home.mkdir()
    skill_text = "---\nname: flash\n---\n\n# Flash\n"
    make_state_db(
        home,
        [
            (
                "skill_manage",
                {
                    "action": "create",
                    "name": "flash",
                    "category": "ops",
                    "content": skill_text,
                },
                {"success": True, "category": "ops"},
            ),
            (
                "skill_manage",
                {"action": "delete", "name": "flash", "absorbed_into": ""},
                {"success": True},
            ),
        ],
    )
    outbox = new_outbox(tmp_path)

    counts = state_db.poll(outbox, home)

    assert counts["knowledge.record_written"] == 2
    events = knowledge_events(outbox)
    assert [event["payload"]["action"] for event in events] == ["create", "delete"]
    assert all(event["payload"]["origin"] == "foreground" for event in events)
    versions = outbox.knowledge_versions("skill:ops/flash")
    assert [version["seq"] for version in versions] == [1, 2]
    assert versions[0]["linked_event_id"] == events[0]["event_id"]
    assert versions[1]["linked_event_id"] == events[1]["event_id"]
    assert versions[1]["is_tombstone"] is True
    assert knowledge_store.restore_version(outbox, "skill:ops/flash", 1) == {
        "SKILL.md": skill_text.encode()
    }
    assert knowledge_store.restore_version(outbox, "skill:ops/flash", 2) == {}

    stored_rows = "\n".join(
        row[0]
        for row in outbox._conn.execute(
            "SELECT envelope_json FROM events ORDER BY producer_sequence"
        )
    )
    assert skill_text not in stored_rows
    assert all(event.get("content_ciphertext") for event in events)
    assert json.loads(outbox.decrypt_content(events[0]))["content"] == skill_text
    for event in outbox.iter_events():
        validate(event)


def test_failed_and_staged_calls_make_no_knowledge_record(tmp_path):
    home = tmp_path / "hermes"
    home.mkdir()
    make_state_db(
        home,
        [
            (
                "skill_manage",
                {"action": "create", "name": "failed", "content": "# failed"},
                {"success": False, "error": "no"},
            ),
            (
                "memory",
                {"action": "add", "target": "memory", "content": "later"},
                {"success": True, "staged": True, "pending_id": "p1"},
            ),
        ],
    )
    outbox = new_outbox(tmp_path)

    counts = state_db.poll(outbox, home)

    assert counts.get("knowledge.record_written", 0) == 0
    assert knowledge_events(outbox) == []
    assert outbox.knowledge_artifact_ids() == []


def test_all_skill_actions_preserve_exact_action_and_order(tmp_path):
    home = tmp_path / "hermes"
    home.mkdir()
    make_state_db(
        home,
        [
            (
                "skill_manage",
                {"action": "create", "name": "work", "content": "# Work\nv1\n"},
                {"success": True},
            ),
            (
                "skill_manage",
                {"action": "edit", "name": "work", "content": "# Work\nv2\n"},
                {"success": True},
            ),
            (
                "skill_manage",
                {
                    "action": "patch",
                    "name": "work",
                    "old_string": "v2",
                    "new_string": "v3",
                },
                {"success": True},
            ),
            (
                "skill_manage",
                {
                    "action": "write_file",
                    "name": "work",
                    "file_path": "references/a.md",
                    "file_content": "a\n",
                },
                {"success": True},
            ),
            (
                "skill_manage",
                {
                    "action": "remove_file",
                    "name": "work",
                    "file_path": "references/a.md",
                },
                {"success": True},
            ),
            (
                "skill_manage",
                {"action": "delete", "name": "work", "absorbed_into": ""},
                {"success": True},
            ),
        ],
    )
    outbox = new_outbox(tmp_path)

    state_db.poll(outbox, home)

    actions = [event["payload"]["action"] for event in knowledge_events(outbox)]
    assert actions == [
        "create",
        "edit",
        "patch",
        "write_file",
        "remove_file",
        "delete",
    ]
    versions = outbox.knowledge_versions("skill:work")
    assert [version["seq"] for version in versions] == list(range(1, 7))
    assert knowledge_store.restore_version(outbox, "skill:work", 3) == {
        "SKILL.md": b"# Work\nv3\n"
    }
    assert knowledge_store.restore_version(outbox, "skill:work", 4)[
        "references/a.md"
    ] == b"a\n"


def test_memory_batch_uses_arguments_to_create_a_restorable_version(tmp_path):
    home = tmp_path / "hermes"
    home.mkdir()
    operations = [
        {"action": "add", "content": "first"},
        {"action": "add", "content": "second"},
        {"action": "replace", "old_text": "first", "content": "updated"},
    ]
    make_state_db(
        home,
        [
            (
                "memory",
                {"target": "memory", "operations": operations},
                {"success": True, "entry_count": 2},
            )
        ],
    )
    outbox = new_outbox(tmp_path)

    state_db.poll(outbox, home)

    event = knowledge_events(outbox)[0]
    assert event["payload"]["action"] == "batch"
    assert event["payload"]["operation_actions"] == ["add", "add", "replace"]
    assert event["payload"]["origin"] == "foreground"
    assert knowledge_store.restore_version(outbox, "memory:memory", 1) == {
        "MEMORY.md": "updated\n§\nsecond".encode()
    }
    assert json.loads(outbox.decrypt_content(event))["operations"] == operations


def test_consolidation_delete_emits_compaction(tmp_path):
    home = tmp_path / "hermes"
    home.mkdir()
    make_state_db(
        home,
        [
            (
                "skill_manage",
                {"action": "create", "name": "small", "content": "# Small\n"},
                {"success": True},
            ),
            (
                "skill_manage",
                {
                    "action": "delete",
                    "name": "small",
                    "absorbed_into": "umbrella",
                },
                {"success": True},
            ),
        ],
    )
    outbox = new_outbox(tmp_path)

    counts = state_db.poll(outbox, home)

    assert counts["knowledge.record_compacted"] == 1
    compacted = knowledge_events(outbox, "knowledge.record_compacted")
    assert len(compacted) == 1
    assert compacted[0]["payload"]["source_artifact_id"] == "skill:small"
    assert compacted[0]["payload"]["target_artifact_id"] == "skill:umbrella"
    assert compacted[0]["causation_id"] == knowledge_events(outbox)[-1]["event_id"]

    event_count = outbox.count()
    outbox.set_cursor("state.db:messages:v2", 0)
    state_db.poll(outbox, home)
    assert outbox.count() == event_count
    assert len(knowledge_events(outbox, "knowledge.record_compacted")) == 1


def test_restart_and_cursor_reset_do_not_duplicate_events_or_versions(tmp_path):
    home = tmp_path / "hermes"
    home.mkdir()
    make_state_db(
        home,
        [
            (
                "skill_manage",
                {"action": "create", "name": "brief", "content": "# Brief\n"},
                {"success": True},
            ),
            (
                "skill_manage",
                {"action": "delete", "name": "brief", "absorbed_into": ""},
                {"success": True},
            ),
        ],
    )
    outbox = new_outbox(tmp_path)
    state_db.poll(outbox, home)
    event_count = outbox.count()
    outbox.close()

    reopened = Outbox.open(tmp_path / "bridge")
    reopened.initialize()
    reopened.set_cursor("state.db:messages:v2", 0)
    state_db.poll(reopened, home)

    assert reopened.count() == event_count
    assert len(reopened.knowledge_versions("skill:brief")) == 2
    assert len(knowledge_events(reopened)) == 2


def test_fleet_foreground_mutations_work_with_public_key_only(tmp_path):
    operator = tmp_path / "operator"
    operator.mkdir()
    keypair = keystore.mint_operator_keypair(operator)
    bridge = tmp_path / "bridge"
    bridge.mkdir()
    keystore.write_public_key(bridge, keypair.public)
    home = tmp_path / "hermes"
    home.mkdir()
    make_state_db(
        home,
        [
            (
                "skill_manage",
                {"action": "create", "name": "fleet", "content": "# Fleet\nv1\n"},
                {"success": True},
            ),
            (
                "skill_manage",
                {"action": "edit", "name": "fleet", "content": "# Fleet\nv2\n"},
                {"success": True},
            ),
        ],
    )
    outbox = new_outbox(tmp_path)

    counts = state_db.poll(outbox, home)

    assert counts["knowledge.record_written"] == 2
    assert not keystore.has_secret(bridge)
    assert [version["seq"] for version in outbox.knowledge_versions("skill:fleet")] == [
        1,
        2,
    ]
    encrypted_events = knowledge_events(outbox)
    assert len(encrypted_events) == 2
    assert all(outbox.decrypt_content(event, keypair=keypair) for event in encrypted_events)


def test_fleet_mutation_uses_disk_after_state_after_restart(tmp_path, monkeypatch):
    operator = tmp_path / "operator"
    operator.mkdir()
    keypair = keystore.mint_operator_keypair(operator)
    bridge = tmp_path / "bridge"
    bridge.mkdir()
    keystore.write_public_key(bridge, keypair.public)
    home = tmp_path / "hermes"
    home.mkdir()
    make_state_db(
        home,
        [
            (
                "skill_manage",
                {"action": "create", "name": "fleet", "content": "# Fleet\nv1\n"},
                {"success": True},
            )
        ],
    )
    outbox = new_outbox(tmp_path)
    state_db.poll(outbox, home)
    outbox.close()

    skill = home / "skills" / "fleet"
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text("# Fleet\nv2\n")
    arguments = {
        "action": "patch",
        "name": "fleet",
        "old_string": "v1",
        "new_string": "v2",
    }
    tool_calls = json.dumps(
        [
            {
                "id": "call-2",
                "call_id": "call-2",
                "type": "function",
                "function": {
                    "name": "skill_manage",
                    "arguments": json.dumps(arguments),
                },
            }
        ]
    )
    database = sqlite3.connect(home / "state.db")
    database.execute(
        "INSERT INTO messages("
        "id,session_id,role,content,tool_calls,timestamp,finish_reason"
        ") VALUES (?,?,?,?,?,?,?)",
        (3, "S", "assistant", "", tool_calls, 1003.0, "tool_calls"),
    )
    database.execute(
        "INSERT INTO messages("
        "id,session_id,role,content,tool_call_id,tool_name,timestamp"
        ") VALUES (?,?,?,?,?,?,?)",
        (4, "S", "tool", '{"success":true}', "call-2", "skill_manage", 1004.0),
    )
    database.commit()
    database.close()

    reopened = Outbox.open(bridge)
    monkeypatch.setattr(
        reopened,
        "get_blob",
        lambda *_args, **_kwargs: pytest.fail("fleet mutation tried to decrypt a blob"),
    )
    counts = state_db.poll(reopened, home)

    assert counts["knowledge.record_written"] == 1
    versions = reopened.knowledge_versions("skill:fleet")
    assert [version["seq"] for version in versions] == [1, 2]
    assert versions[-1]["manifest"][0]["blob_hash"] == reopened._content_hash(
        b"# Fleet\nv2\n"
    )
    assert not keystore.has_secret(bridge)
