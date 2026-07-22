"""Tests for the observe TREE view structure (issue #7): render_tree, _Index,
_render_session.

Cover deep (3-level) nesting, multiple sorted roots, an orphan child whose
parent_session_id names a session with no session.created (must still
render, as its own root), tool leaves under their owning session, open vs
ended status text, the root vs child marker with indentation depth, and
order independence when a subagent.child_spawned is seen before its
parent's session.created. One integration test drives the real
state_db.poll pipeline to build a 3-level chain and confirms the tree
renders identically to the hand-built case.

Self-contained: no imports from other test modules. Records are appended
through a real Outbox so they carry producer_sequence and validate as
envelopes; nothing here relies on wall-clock time.
"""

from __future__ import annotations

import sqlite3

from hermes_flight_recorder import observe
from hermes_flight_recorder.cli import main  # noqa: F401  (kept per spec's import list; unused directly here)
from hermes_flight_recorder.collector import cron_db, state_db  # noqa: F401  (cron_db unused; state_db used below)
from hermes_flight_recorder.collector._common import build_record
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile  # noqa: F401

B = 1784415000.0


def new_outbox(tmp_path) -> Outbox:
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


def add(ob, event_type, *, occurred_at=B, session_id=None, parent_session_id=None,
        correlation_id="corr", invocation_id=None, payload=None, partial=False,
        content=None):
    rec = build_record(
        event_type=event_type,
        occurred_at=occurred_at,
        source="test",
        capture_method="test",
        runtime={"kind": "cli", "engine": "standard"},
        correlation_id=correlation_id,
        session_id=session_id,
        parent_session_id=parent_session_id,
        invocation_id=invocation_id,
        payload=payload or {},
        partial=partial,
    )
    return ob.append(rec, content=content)


# --- deep nesting ---------------------------------------------------------
def test_deep_three_level_nesting_markers_and_indentation(tmp_path):
    """A -> B -> C, each one level deeper: root marker/pad 0, then ○ at
    depth 1 (2-space pad) and depth 2 (4-space pad)."""
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="A", correlation_id="A",
        payload={"kind": "cli"})
    add(ob, "subagent.child_spawned", session_id="B", parent_session_id="A",
        correlation_id="A", payload={"kind": "subagent"})
    add(ob, "subagent.child_spawned", session_id="C", parent_session_id="B",
        correlation_id="A", payload={"kind": "subagent"})

    lines = observe.render_tree(observe.load(ob))

    a_line = next(l for l in lines if "cli A" in l)
    b_line = next(l for l in lines if "subagent B" in l)
    c_line = next(l for l in lines if "subagent C" in l)

    assert a_line.startswith("● ")           # root marker, depth 0, no pad
    assert b_line.startswith("  ○ ")         # child marker, depth 1, 2-space pad
    assert c_line.startswith("    ○ ")       # grandchild, depth 2, 4-space pad

    # ordering: parent header precedes child header precedes grandchild header
    assert lines.index(a_line) < lines.index(b_line) < lines.index(c_line)


# --- multiple independent roots --------------------------------------------
def test_multiple_independent_roots_sorted_alphabetically_with_separator(tmp_path):
    ob = new_outbox(tmp_path)
    # Insert "zeta" first so append order differs from the expected sort order.
    add(ob, "session.created", session_id="zeta", correlation_id="zeta",
        payload={"kind": "cli"})
    add(ob, "session.created", session_id="alpha", correlation_id="alpha",
        payload={"kind": "cli"})

    lines = observe.render_tree(observe.load(ob))

    assert "alpha" in lines[0] and lines[0].startswith("● ")
    assert lines[1] == ""  # blank separator between independent root blocks
    assert "zeta" in lines[2] and lines[2].startswith("● ")


# --- orphan child (dangling parent_session_id) -----------------------------
def test_orphan_child_with_no_matching_session_created_renders_as_root(tmp_path):
    """C's parent_session_id names 'ghost', which never gets a
    session.created/subagent.child_spawned of its own. Per _Index.roots(),
    a node whose parent id is not itself a known session counts as a root,
    so C must still render (as a root, with the root marker)."""
    ob = new_outbox(tmp_path)
    add(ob, "subagent.child_spawned", session_id="C", parent_session_id="ghost",
        correlation_id="ghost", payload={"kind": "subagent", "model": "m"})

    lines = observe.render_tree(observe.load(ob))

    assert len(lines) == 1
    assert lines[0].startswith("● ")   # root marker despite being a "subagent" kind
    assert "subagent C" in lines[0]
    assert "[open]" in lines[0]
    assert "ghost" not in lines[0]     # the dangling parent id itself never renders


# --- tool leaves ------------------------------------------------------------
def test_tool_leaves_listed_under_owning_session_in_append_order(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="P", correlation_id="P",
        payload={"kind": "cli"})
    add(ob, "tool.call_completed", session_id="P", correlation_id="P",
        payload={"tool_name": "read_file", "status": "ok"}, content="x")
    add(ob, "tool.call_completed", session_id="P", correlation_id="P",
        payload={"tool_name": "terminal", "status": "error"}, content="y")

    lines = observe.render_tree(observe.load(ob))
    tool_lines = [l for l in lines if "├─ tool" in l]

    assert tool_lines == [
        "    ├─ tool read_file [ok]",
        "    ├─ tool terminal [error]",
    ]


# --- open vs ended status text ----------------------------------------------
def test_open_vs_ended_status_text(tmp_path):
    ob = new_outbox(tmp_path)
    # Open: created only, no terminal event.
    add(ob, "session.created", session_id="O", correlation_id="O",
        payload={"kind": "cli"})
    # Ended with an explicit end_reason.
    add(ob, "session.created", session_id="D", correlation_id="D",
        payload={"kind": "cli"})
    add(ob, "session.ended", session_id="D", correlation_id="D",
        payload={"kind": "cli", "end_reason": "done"})
    # Ended subagent with NO end_reason in the payload -> falls back to "ended".
    add(ob, "subagent.child_spawned", session_id="S", parent_session_id="D",
        correlation_id="D", payload={"kind": "subagent"})
    add(ob, "subagent.completed", session_id="S", parent_session_id="D",
        correlation_id="D", payload={"kind": "subagent"})

    lines = observe.render_tree(observe.load(ob))
    open_line = next(l for l in lines if " O " in l)
    ended_line = next(l for l in lines if " D " in l)
    fallback_line = next(l for l in lines if "subagent S" in l)

    assert "[open]" in open_line
    assert "[done]" in ended_line
    assert "[ended]" in fallback_line  # end_reason absent -> default "ended"


# --- order independence -----------------------------------------------------
def test_subagent_child_spawned_seen_before_parent_session_created(tmp_path):
    """The child event referencing parent 'P' is appended before P's own
    session.created. The final tree must still nest the child correctly
    (not treat it as an orphan root), since _Index builds the whole graph
    before computing roots()."""
    ob = new_outbox(tmp_path)
    add(ob, "subagent.child_spawned", session_id="C", parent_session_id="P",
        correlation_id="P", payload={"kind": "subagent", "model": "m"})
    add(ob, "session.created", session_id="P", correlation_id="P",
        payload={"kind": "cli", "model": "m"})

    lines = observe.render_tree(observe.load(ob))

    root_lines = [l for l in lines if l.startswith("● ")]
    assert len(root_lines) == 1
    assert "cli P" in root_lines[0]

    child_line = next(l for l in lines if "subagent C" in l)
    assert child_line.startswith("  ○ ")  # nested one level under P, not a root


# --- session filter as pseudo-root over a nested descendant -----------------
def test_session_filter_uses_target_as_pseudo_root_for_nested_descendant(tmp_path):
    """Filtering on the *middle* node of a 3-level chain (A -> B -> C) makes
    B the rendered root (even though B has its own parent A, which is
    excluded from the filtered record set), with C still nested beneath it."""
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="A", correlation_id="A",
        payload={"kind": "cli"})
    add(ob, "subagent.child_spawned", session_id="B", parent_session_id="A",
        correlation_id="A", payload={"kind": "subagent"})
    add(ob, "subagent.child_spawned", session_id="C", parent_session_id="B",
        correlation_id="A", payload={"kind": "subagent"})

    records = observe.load(ob, session="B")
    lines = observe.render_tree(records, session="B")
    text = "\n".join(lines)

    assert "cli A" not in text  # A is outside the filtered subtree
    b_line = next(l for l in lines if "subagent B" in l)
    c_line = next(l for l in lines if "subagent C" in l)
    assert b_line.startswith("● ")     # B renders as the (pseudo-)root
    assert c_line.startswith("  ○ ")   # C still nests one level under B


# --- no sessions captured ----------------------------------------------------
def test_render_tree_placeholder_when_no_session_nodes_exist(tmp_path):
    """A tool event referencing a session id that never got a
    session.created/subagent.child_spawned of its own contributes no root,
    so the tree is empty and shows the placeholder line."""
    ob = new_outbox(tmp_path)
    add(ob, "tool.call_completed", session_id="X", correlation_id="X",
        payload={"tool_name": "read_file", "status": "ok"})

    lines = observe.render_tree(observe.load(ob))
    assert lines == ["(no sessions captured)"]


# --- integration: real state_db poll pipeline builds the deep chain --------
def test_deep_nesting_via_real_state_db_poll_pipeline(tmp_path):
    """Drive the actual state_db.poll() adapter over a 3-level session chain
    (root cli session -> subagent -> subagent) and confirm the resulting
    outbox renders the identical tree shape as the hand-built case."""
    hh = tmp_path / "hermes"
    hh.mkdir()
    db = sqlite3.connect(hh / "state.db")
    db.executescript(
        """
        CREATE TABLE sessions (id TEXT, source TEXT, parent_session_id TEXT, model TEXT,
            message_count INT, tool_call_count INT, input_tokens INT, output_tokens INT,
            estimated_cost_usd REAL, started_at REAL, ended_at REAL, end_reason TEXT,
            profile_name TEXT, expiry_finalized INT);
        CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT,
            tool_name TEXT, tool_call_id TEXT, effect_disposition TEXT, content TEXT,
            timestamp REAL, finish_reason TEXT);
        CREATE TABLE session_model_usage (session_id TEXT, model TEXT, task TEXT,
            api_call_count INT, input_tokens INT, output_tokens INT, cache_read_tokens INT,
            reasoning_tokens INT, estimated_cost_usd REAL, cost_status TEXT, last_seen REAL);
        CREATE TABLE async_delegations (delegation_id TEXT, origin_session TEXT,
            parent_session_id TEXT, state TEXT, delivery_state TEXT,
            owner_pid INT, dispatched_at REAL, event_json TEXT, result_json TEXT);
        """
    )
    db.execute(
        "INSERT INTO sessions VALUES ('P','cli',NULL,'m',0,0,0,0,0.0,?,?,'done','default',1)",
        (B, B + 50),
    )
    db.execute(
        "INSERT INTO sessions VALUES ('C1','subagent','P','m',0,0,0,0,0.0,?,?,'agent_close','default',1)",
        (B + 1, B + 40),
    )
    db.execute(
        "INSERT INTO sessions VALUES ('C2','subagent','C1','m',0,0,0,0,0.0,?,?,'agent_close','default',1)",
        (B + 2, B + 30),
    )
    db.commit()
    db.close()

    ob = new_outbox(tmp_path)
    state_db.poll(ob, hh)

    lines = observe.render_tree(observe.load(ob))

    p_line = next(l for l in lines if "cli P" in l)
    c1_line = next(l for l in lines if "subagent C1" in l)
    c2_line = next(l for l in lines if "subagent C2" in l)

    assert p_line.startswith("● ") and "[done]" in p_line
    assert c1_line.startswith("  ○ ") and "[agent_close]" in c1_line
    assert c2_line.startswith("    ○ ") and "[agent_close]" in c2_line
    assert lines.index(p_line) < lines.index(c1_line) < lines.index(c2_line)
