"""Tests for the durable-state adapters (issue #5).

Fixtures mirror the real probe session: a CLI parent session (still open),
a subagent child, a terminal tool call, model usage rows, an async
delegation, and two completed cron executions plus a ticker heartbeat.
"""

from __future__ import annotations

import json
import sqlite3
from collections import Counter

from hermes_flight_recorder.collector import cron_db, state_db
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.envelope import validate


# --- fixtures -----------------------------------------------------------
def make_state_db(hermes_home) -> None:
    db = sqlite3.connect(hermes_home / "state.db")
    db.executescript(
        """
        CREATE TABLE sessions (id TEXT, source TEXT, parent_session_id TEXT, model TEXT,
            message_count INT, tool_call_count INT, input_tokens INT, output_tokens INT,
            estimated_cost_usd REAL, started_at REAL, ended_at REAL, end_reason TEXT,
            profile_name TEXT, expiry_finalized INT);
        CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT,
            tool_name TEXT, tool_call_id TEXT, effect_disposition TEXT, content TEXT, timestamp REAL);
        CREATE TABLE session_model_usage (session_id TEXT, model TEXT, task TEXT,
            api_call_count INT, input_tokens INT, output_tokens INT, cache_read_tokens INT,
            reasoning_tokens INT, estimated_cost_usd REAL, cost_status TEXT, last_seen REAL);
        CREATE TABLE async_delegations (delegation_id TEXT, origin_session TEXT,
            parent_session_id TEXT, state TEXT, delivery_state TEXT,
            owner_pid INT, dispatched_at REAL, event_json TEXT, result_json TEXT);
        """
    )
    # Parent CLI session — still open (ended_at NULL). Subagent child — ended.
    db.executemany(
        "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("P", "cli", None, "m", 8, 2, 18071, 825, 0.0, 1000.0, None, None, None, 0),
            ("C", "subagent", "P", "m", 4, 1, 12278, 126, 0.0, 1007.0, 1015.0, "agent_close", None, None),
        ],
    )
    db.executemany(
        "INSERT INTO messages VALUES (?,?,?,?,?,?,?,?)",
        [
            (3, "P", "user", None, None, None, "do the thing", 1000.5),
            (5, "P", "tool", "terminal", None, None, '{"output":"Sat","exit_code":0}', 1002.0),
            (7, "P", "tool", "delegate_task", None, None, '{"status":"dispatched","count":1}', 1006.0),
            (9, "C", "assistant", None, None, None, "", 1009.0),
            (10, "C", "tool", "read_file", None, None, '{"content":"Sat Jul 18"}', 1010.0),
        ],
    )
    db.executemany(
        "INSERT INTO session_model_usage VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("P", "m", "", 4, 18071, 825, 55296, 452, 0.0, "estimated", 1014.0),
            ("P", "m", "title_generation", 1, 259, 8, 0, 0, 0.0, None, 1001.0),
        ],
    )
    db.execute(
        "INSERT INTO async_delegations VALUES (?,?,?,?,?,?,?,?,?)",
        ("deleg_1", "P", "P", "completed", "delivered", 4023601, 1006.0,
         '{"goal":"read the file","is_batch":true}', '{"results":[{"summary":"Saturday"}]}'),
    )
    db.commit()
    db.close()


def make_cron(hermes_home) -> None:
    cron = hermes_home / "cron"
    cron.mkdir()
    db = sqlite3.connect(cron / "executions.db")
    db.execute(
        "CREATE TABLE executions (id TEXT, job_id TEXT, source TEXT, pid INT, status TEXT, "
        "claimed_at TEXT, started_at TEXT, finished_at TEXT, error TEXT)"
    )
    db.executemany(
        "INSERT INTO executions VALUES (?,?,?,?,?,?,?,?,?)",
        [
            ("e1", "j1", "builtin", 111, "completed",
             "2026-07-18T20:48:39.800009-05:00", "2026-07-18T20:48:39.804167-05:00",
             "2026-07-18T20:48:39.817041-05:00", None),
            ("e2", "j1", "builtin", 222, "completed",
             "2026-07-18T20:49:57.261723-05:00", "2026-07-18T20:49:57.265400-05:00",
             "2026-07-18T20:49:57.277658-05:00", None),
        ],
    )
    db.commit()
    db.close()
    (cron / "ticker_heartbeat").write_text("1784415389.44")
    (cron / "ticker_last_success").write_text("1784415389.44")
    (cron / "jobs.json").write_text(json.dumps({"jobs": [{"id": "j1", "name": "flight-recorder-probe"}]}))


def new_outbox(tmp_path):
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


def types(outbox) -> Counter:
    return Counter(e["payload"]["event_type"] for e in outbox.iter_events())


# --- state.db -----------------------------------------------------------
def test_state_poll_event_mapping(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    make_state_db(hh)
    ob = new_outbox(tmp_path)

    counts = state_db.poll(ob, hh)
    assert counts == {
        "session.created": 1,          # parent (cli)
        "subagent.child_spawned": 1,   # child (subagent)
        "subagent.completed": 1,       # child ended
        "tool.call_completed": 3,      # terminal + delegate_task + read_file (all role=tool)
        "model.usage_recorded": 2,     # main + title_generation
        "delegation.dispatched": 1,
    }
    # every appended record is a valid envelope
    for e in ob.iter_events():
        validate(e)


def test_open_parent_session_has_no_terminal(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    make_state_db(hh)
    ob = new_outbox(tmp_path)
    state_db.poll(ob, hh)
    # parent P has ended_at NULL -> no session.ended (reconciler decides later)
    assert types(ob)["session.ended"] == 0


def test_correlation_groups_child_under_parent(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    make_state_db(hh)
    ob = new_outbox(tmp_path)
    state_db.poll(ob, hh)
    by = {(e["payload"]["event_type"], e.get("session_id")): e for e in ob.iter_events()}
    assert by[("session.created", "P")]["correlation_id"] == "P"
    assert by[("subagent.child_spawned", "C")]["correlation_id"] == "P"  # child rolls up to parent
    assert by[("subagent.child_spawned", "C")]["parent_session_id"] == "P"


def test_tool_status_and_content_encrypted(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    make_state_db(hh)
    ob = new_outbox(tmp_path)
    state_db.poll(ob, hh)
    tool_events = [e for e in ob.iter_events() if e["payload"]["event_type"] == "tool.call_completed"]
    terminal = next(e for e in tool_events if e["payload"]["tool_name"] == "terminal")
    assert terminal["payload"]["status"] == "ok"  # exit_code 0 -> ok
    assert "content_ciphertext" in terminal and terminal["content_hash"].startswith("sha256:")
    assert ob.decrypt_content(terminal) == b'{"output":"Sat","exit_code":0}'


def test_default_profile_normalization(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    make_state_db(hh)
    ob = new_outbox(tmp_path)
    state_db.poll(ob, hh)
    created = next(e for e in ob.iter_events() if e["payload"]["event_type"] == "session.created")
    assert created["profile"] == "default"  # profile_name was NULL


def test_state_repoll_is_idempotent(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    make_state_db(hh)
    ob = new_outbox(tmp_path)
    state_db.poll(ob, hh)
    n = ob.count()
    second = state_db.poll(ob, hh)  # re-poll
    assert ob.count() == n  # no duplicates (dedup)
    assert second.get("tool.call_completed", 0) == 0  # cursor honored: no re-scan of messages


def test_adapter_never_writes_state_db(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    make_state_db(hh)
    ob = new_outbox(tmp_path)
    before = (hh / "state.db").read_bytes()
    state_db.poll(ob, hh)
    assert (hh / "state.db").read_bytes() == before  # byte-for-byte unchanged


# --- cron ---------------------------------------------------------------
def test_cron_poll_event_mapping(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    make_cron(hh)
    ob = new_outbox(tmp_path)
    counts = cron_db.poll(ob, hh)
    assert counts == {
        "cron.run_claimed": 2,
        "cron.run_finished": 2,
        "cron.ticker_heartbeat": 1,
    }
    for e in ob.iter_events():
        validate(e)


def test_cron_finished_ok_and_iso_timestamps_parsed(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    make_cron(hh)
    ob = new_outbox(tmp_path)
    cron_db.poll(ob, hh)
    finished = [e for e in ob.iter_events() if e["payload"]["event_type"] == "cron.run_finished"]
    assert all(e["payload"]["ok"] is True for e in finished)
    # ISO string -> epoch float
    assert all(isinstance(e["occurred_at"], float) and e["occurred_at"] > 0 for e in finished)
    assert all(e["correlation_id"] == "j1" for e in finished)


def test_cron_repoll_is_idempotent(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    make_cron(hh)
    ob = new_outbox(tmp_path)
    cron_db.poll(ob, hh)
    n = ob.count()
    cron_db.poll(ob, hh)
    assert ob.count() == n


def test_missing_stores_are_tolerated(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()  # no state.db, no cron dir
    ob = new_outbox(tmp_path)
    assert cron_db.poll(ob, hh) == {}  # cron dir absent -> nothing, no crash
    try:
        state_db.poll(ob, hh)
        raised = False
    except FileNotFoundError:
        raised = True
    assert raised  # state.db absent raises (the CLI reports it)
