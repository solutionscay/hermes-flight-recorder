"""Tests for payload.surface on session.created (issue #14).

`surface` records the originating surface a session entered Hermes through.
The state.db producer carries the verbatim sessions.source; the live hook
carries the gateway platform value. It is an open-ended free-form string
(plugin platforms extend it), never enum-validated, and additive against
the frozen envelope v1 contract (payload is a free-form dict).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from hermes_flight_recorder.collector import state_db
from hermes_flight_recorder.collector.hook import SPOOL_FILENAME, drain
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.envelope import validate

from test_hook_drain import by_type, drain_to_records, write_spool
from test_state_adapter import new_outbox


# --- state.db producer --------------------------------------------------
def _sessions_db(hh, rows) -> None:
    db = sqlite3.connect(hh / "state.db")
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
    db.executemany(
        "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows
    )
    db.commit()
    db.close()


def test_state_surface_matches_source_per_row(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    _sessions_db(
        hh,
        [
            ("A", "cli", None, "m", 1, 0, 0, 0, 0.0, 1000.0, None, None, None, 0),
            ("B", "discord", None, "m", 1, 0, 0, 0, 0.0, 1001.0, None, None, None, 0),
            ("C", "subagent", "A", "m", 1, 0, 0, 0, 0.0, 1002.0, 1003.0, "x", None, None),
        ],
    )
    ob = new_outbox(tmp_path)
    state_db.poll(ob, hh)

    surface = {
        e.get("session_id"): e["payload"].get("surface")
        for e in ob.iter_events()
        if e["payload"]["event_type"] in ("session.created", "subagent.child_spawned")
    }
    assert surface == {"A": "cli", "B": "discord", "C": "subagent"}
    for e in ob.iter_events():
        validate(e)


def test_state_surface_open_ended_plugin_platform(tmp_path):
    # A plugin platform name (not in any fixed enum) must pass through verbatim.
    hh = tmp_path / "hermes"
    hh.mkdir()
    _sessions_db(
        hh,
        [("Z", "irc", None, "m", 1, 0, 0, 0, 0.0, 1000.0, None, None, None, 0)],
    )
    ob = new_outbox(tmp_path)
    state_db.poll(ob, hh)
    created = next(
        e for e in ob.iter_events() if e["payload"]["event_type"] == "session.created"
    )
    assert created["payload"]["surface"] == "irc"


# --- live hook producer -------------------------------------------------
def test_hook_session_start_carries_platform_surface(tmp_path: Path):
    write_spool(
        tmp_path,
        [("session:start", {"platform": "telegram", "session_id": "s1", "session_key": "k1"}, 100.0)],
    )
    created = by_type(drain_to_records(tmp_path))["session.created"]
    assert created["payload"]["surface"] == "telegram"


def test_surface_has_shared_ingress_meaning_across_producers(tmp_path: Path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    _sessions_db(
        hh,
        [("A", "telegram", None, "m", 1, 0, 0, 0, 0.0, 1000.0, None, None, None, 0)],
    )
    ob = new_outbox(tmp_path)
    state_db.poll(ob, hh)
    durable = next(
        e for e in ob.iter_events() if e["payload"]["event_type"] == "session.created"
    )

    hook_dir = tmp_path / "hook"
    hook_dir.mkdir()
    write_spool(
        hook_dir,
        [("session:start", {"platform": "telegram", "session_id": "B"}, 100.0)],
    )
    live = by_type(drain_to_records(hook_dir))["session.created"]

    assert durable["payload"]["surface"] == live["payload"]["surface"] == "telegram"


def test_hook_local_session_drops_empty_surface(tmp_path: Path):
    # The hook sends platform='' for a LOCAL/None session; surface must be
    # absent (not stored as ''), because `or None` lets _clean() drop it.
    write_spool(
        tmp_path,
        [("session:start", {"platform": "", "session_id": "s2", "session_key": "k2"}, 100.0)],
    )
    created = by_type(drain_to_records(tmp_path))["session.created"]
    assert "surface" not in created["payload"]
