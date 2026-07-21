"""Tests for the tree-view token/cost rollup math in hermes_flight_recorder.observe.

Focus: `_Index.own_tokens` and `_Index.subtree_tokens` — the precedence of
session.ended over model.usage_recorded, the summing fallback across
multiple usage rows, the recursive subtree sum across all descendants (not
just one child), and the zero case when a session has neither. Self
contained: builds its own outbox fixture and record helper, mirroring
tests/test_observe.py's style, and never imports from other test modules.
"""

from __future__ import annotations

from hermes_flight_recorder import observe
from hermes_flight_recorder.cli import main
from hermes_flight_recorder.collector._common import build_record
from hermes_flight_recorder.collector.outbox import Outbox

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


# --- own_tokens: precedence and fallback --------------------------------
def test_own_tokens_ended_takes_precedence_no_double_count(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="P", correlation_id="P", payload={"kind": "cli"})
    # Usage rows that would, if wrongly added on top of the ended totals,
    # produce a visibly different (bigger) number.
    add(ob, "model.usage_recorded", session_id="P", correlation_id="P",
        payload={"model": "m", "input_tokens": 999, "output_tokens": 888,
                 "estimated_cost_usd": 9.99})
    add(ob, "session.ended", session_id="P", correlation_id="P",
        payload={"kind": "cli", "end_reason": "done", "input_tokens": 55,
                 "output_tokens": 13, "estimated_cost_usd": 0.0215})
    idx = observe._Index(observe.load(ob))
    assert idx.own_tokens("P") == (55, 13, 0.0215)

    lines = observe.render_tree(observe.load(ob))
    p_line = next(l for l in lines if l.startswith("● "))
    assert "tokens=55/13" in p_line
    assert "cost=$0.0215" in p_line
    assert "999" not in p_line and "888" not in p_line


def test_own_tokens_sums_multiple_usage_rows_when_no_ended(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="Q", correlation_id="Q", payload={"kind": "cli"})
    add(ob, "model.usage_recorded", session_id="Q", correlation_id="Q",
        payload={"model": "m", "input_tokens": 12, "output_tokens": 5,
                 "estimated_cost_usd": 0.0025})
    add(ob, "model.usage_recorded", session_id="Q", correlation_id="Q",
        payload={"model": "m", "input_tokens": 33, "output_tokens": 9,
                 "estimated_cost_usd": 0.0075})
    idx = observe._Index(observe.load(ob))
    assert idx.own_tokens("Q") == (45, 14, 0.01)

    lines = observe.render_tree(observe.load(ob))
    q_line = next(l for l in lines if l.startswith("● "))
    assert "tokens=45/14" in q_line
    assert "cost=$0.0100" in q_line


def test_own_tokens_zero_when_neither_ended_nor_usage(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="Z", correlation_id="Z", payload={"kind": "cli"})
    idx = observe._Index(observe.load(ob))
    assert idx.own_tokens("Z") == (0, 0, 0.0)
    assert idx.subtree_tokens("Z") == (0, 0, 0.0)

    lines = observe.render_tree(observe.load(ob))
    z_line = next(l for l in lines if l.startswith("● "))
    assert "tokens=0/0" in z_line and "cost=$0.0000" in z_line
    assert "(subtree tokens=0/0 cost=$0.0000)" in z_line


# --- subtree_tokens: recursive rollup over all descendants ---------------
def test_subtree_tokens_three_node_tree_sums_all_descendants(tmp_path):
    """Root R with two children A and B (a 3-node tree). A wrong sum — e.g.
    only counting one child, or overwriting instead of accumulating across
    siblings — would visibly diverge from the correct total below.
    """
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="R", correlation_id="R", payload={"kind": "cli"})
    add(ob, "session.ended", session_id="R", correlation_id="R",
        payload={"kind": "cli", "end_reason": "done", "input_tokens": 17,
                 "output_tokens": 3, "estimated_cost_usd": 0.0135})

    add(ob, "subagent.child_spawned", session_id="A", parent_session_id="R",
        correlation_id="R", payload={"kind": "subagent", "model": "m"})
    add(ob, "subagent.completed", session_id="A", parent_session_id="R",
        correlation_id="R", payload={"kind": "subagent", "end_reason": "agent_close",
                                     "input_tokens": 29, "output_tokens": 11,
                                     "estimated_cost_usd": 0.0275})

    add(ob, "subagent.child_spawned", session_id="B", parent_session_id="R",
        correlation_id="R", payload={"kind": "subagent", "model": "m"})
    add(ob, "subagent.completed", session_id="B", parent_session_id="R",
        correlation_id="R", payload={"kind": "subagent", "end_reason": "agent_close",
                                     "input_tokens": 41, "output_tokens": 7,
                                     "estimated_cost_usd": 0.0195})

    idx = observe._Index(observe.load(ob))
    assert idx.own_tokens("R") == (17, 3, 0.0135)
    # 17+29+41=87, 3+11+7=21, 0.0135+0.0275+0.0195=0.0605
    assert idx.subtree_tokens("R") == (87, 21, 0.0605)

    lines = observe.render_tree(observe.load(ob))
    r_line = next(l for l in lines if l.startswith("● "))
    assert "tokens=17/3" in r_line
    assert "cost=$0.0135" in r_line
    assert "subtree tokens=87/21" in r_line
    assert "cost=$0.0605" in r_line.split("subtree")[-1]


def test_subtree_tokens_matches_own_tokens_for_childless_root(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="L", correlation_id="L", payload={"kind": "cli"})
    add(ob, "session.ended", session_id="L", correlation_id="L",
        payload={"kind": "cli", "end_reason": "done", "input_tokens": 9,
                 "output_tokens": 4, "estimated_cost_usd": 0.0031})
    idx = observe._Index(observe.load(ob))
    assert idx.subtree_tokens("L") == idx.own_tokens("L") == (9, 4, 0.0031)


def test_subtree_rollup_combines_ended_parent_with_usage_summed_child(tmp_path):
    """Mix both own_tokens rules within one tree: the root uses its ended
    payload, the child has no ended row and falls back to summed usage."""
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="R", correlation_id="R", payload={"kind": "cli"})
    add(ob, "session.ended", session_id="R", correlation_id="R",
        payload={"kind": "cli", "end_reason": "done", "input_tokens": 8,
                 "output_tokens": 2, "estimated_cost_usd": 0.0025})

    add(ob, "subagent.child_spawned", session_id="X", parent_session_id="R",
        correlation_id="R", payload={"kind": "subagent", "model": "m"})
    add(ob, "model.usage_recorded", session_id="X", correlation_id="R",
        payload={"model": "m", "input_tokens": 6, "output_tokens": 1,
                 "estimated_cost_usd": 0.002})
    add(ob, "model.usage_recorded", session_id="X", correlation_id="R",
        payload={"model": "m", "input_tokens": 13, "output_tokens": 3,
                 "estimated_cost_usd": 0.0055})
    # X never receives subagent.completed: still has no "ended" row, so its
    # own_tokens must fall back to summing the two usage rows above.

    idx = observe._Index(observe.load(ob))
    assert idx.own_tokens("X") == (19, 4, 0.0075)
    # subtree(R) = own(R) + subtree(X) = (8+19, 2+4, 0.0025+0.0075)
    assert idx.subtree_tokens("R") == (27, 6, 0.01)

    lines = observe.render_tree(observe.load(ob))
    r_line = next(l for l in lines if l.startswith("● "))
    assert "subtree tokens=27/6" in r_line
    assert "cost=$0.0100" in r_line


def test_child_line_shows_own_tokens_without_a_subtree_suffix(tmp_path):
    """Only the root line carries the '(subtree ...)' rollup suffix; a
    non-root child line shows its own tokens plainly."""
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="R", correlation_id="R", payload={"kind": "cli"})
    add(ob, "session.ended", session_id="R", correlation_id="R",
        payload={"kind": "cli", "end_reason": "done", "input_tokens": 5,
                 "output_tokens": 1, "estimated_cost_usd": 0.001})
    add(ob, "subagent.child_spawned", session_id="A", parent_session_id="R",
        correlation_id="R", payload={"kind": "subagent", "model": "m"})
    add(ob, "subagent.completed", session_id="A", parent_session_id="R",
        correlation_id="R", payload={"kind": "subagent", "end_reason": "agent_close",
                                     "input_tokens": 21, "output_tokens": 6,
                                     "estimated_cost_usd": 0.0125})
    lines = observe.render_tree(observe.load(ob))
    a_line = next(l for l in lines if "subagent A" in l)
    assert "tokens=21/6" in a_line
    assert "cost=$0.0125" in a_line
    assert "subtree" not in a_line


def test_cli_tree_end_to_end_renders_exact_rollup(tmp_path, capsys):
    """Drive the same 3-node rollup through the CLI entry point end to end."""
    bridge = str(tmp_path / "bridge")
    ob = Outbox.open(bridge)
    ob.initialize()
    add(ob, "session.created", session_id="R", correlation_id="R", payload={"kind": "cli"})
    add(ob, "session.ended", session_id="R", correlation_id="R",
        payload={"kind": "cli", "end_reason": "done", "input_tokens": 3,
                 "output_tokens": 7, "estimated_cost_usd": 0.0011})
    add(ob, "subagent.child_spawned", session_id="A", parent_session_id="R",
        correlation_id="R", payload={"kind": "subagent", "model": "m"})
    add(ob, "subagent.completed", session_id="A", parent_session_id="R",
        correlation_id="R", payload={"kind": "subagent", "end_reason": "agent_close",
                                     "input_tokens": 14, "output_tokens": 2,
                                     "estimated_cost_usd": 0.0029})
    ob.close()

    code = main(["observe", "--tree", "--flight-recorder-home", bridge])
    out = capsys.readouterr().out
    assert code == 0
    # own totals for root: 3/7 cost=$0.0011; subtree = 3+14=17, 7+2=9,
    # 0.0011+0.0029=0.0040
    assert "tokens=3/7" in out
    assert "cost=$0.0011" in out
    assert "subtree tokens=17/9 cost=$0.0040" in out
