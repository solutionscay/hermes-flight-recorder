"""Tests for the local observe surface (issue #7).

Cover the three views (stream, tree, report), the load/filters, --since
parsing, and the CLI exit-code contract. Records are appended through a
real Outbox so they carry producer_sequence and validate as envelopes.
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


def seed_session_tree(ob):
    """A root cli session P with a tool call, model usage, and a subagent C."""
    add(ob, "session.created", session_id="P", correlation_id="P",
        payload={"kind": "cli", "model": "m"})
    add(ob, "tool.call_completed", session_id="P", correlation_id="P",
        payload={"tool_name": "read_file", "status": "ok"}, content='{"x":1}')
    add(ob, "model.usage_recorded", session_id="P", correlation_id="P",
        payload={"model": "m", "input_tokens": 100, "output_tokens": 20,
                 "estimated_cost_usd": 0.01})
    add(ob, "subagent.child_spawned", session_id="C", parent_session_id="P",
        correlation_id="P", payload={"kind": "subagent", "model": "m"})
    add(ob, "subagent.completed", session_id="C", parent_session_id="P",
        correlation_id="P", payload={"kind": "subagent", "end_reason": "agent_close",
                                     "input_tokens": 40, "output_tokens": 8,
                                     "estimated_cost_usd": 0.004})
    add(ob, "session.ended", session_id="P", correlation_id="P",
        payload={"kind": "cli", "end_reason": "done", "input_tokens": 100,
                 "output_tokens": 20, "estimated_cost_usd": 0.01})


# --- stream -------------------------------------------------------------
def test_stream_is_in_producer_sequence_order(tmp_path):
    ob = new_outbox(tmp_path)
    for i in range(5):
        add(ob, "session.created", session_id=f"S{i}", payload={"kind": "cli"})
    lines = observe.render_stream(observe.load(ob))
    seqs = [int(line.split()[0]) for line in lines]
    assert seqs == [1, 2, 3, 4, 5]
    assert "session.created" in lines[0]
    assert "S0" in lines[0]


def test_stream_shows_key_payload_and_content_hash_not_plaintext(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "tool.call_completed", session_id="P",
        payload={"tool_name": "terminal", "status": "ok"}, content="SECRET-ARGS")
    line = observe.render_stream(observe.load(ob))[0]
    assert "tool_name=terminal" in line and "status=ok" in line
    assert "hash=sha256:" in line
    assert "SECRET-ARGS" not in line  # content is never rendered


# --- tree ---------------------------------------------------------------
def test_tree_nests_subagent_under_parent_with_rollups(tmp_path):
    ob = new_outbox(tmp_path)
    seed_session_tree(ob)
    lines = observe.render_tree(observe.load(ob))
    text = "\n".join(lines)
    # root P present with its own tokens and a subtree rollup
    p_line = next(l for l in lines if l.startswith("● "))
    assert "cli P" in p_line and "[done]" in p_line
    assert "tokens=100/20" in p_line
    # subtree = parent (100/20) + child (40/8) = 140/28
    assert "subtree tokens=140/28" in p_line
    # the tool leaf and the nested child both render, child indented deeper
    assert "├─ tool read_file [ok]" in text
    child_line = next(l for l in lines if "subagent C" in l)
    assert child_line.startswith("  ○ ")  # nested one level under P


def test_tree_session_filter_scopes_to_one_root(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="A", correlation_id="A", payload={"kind": "cli"})
    add(ob, "session.created", session_id="B", correlation_id="B", payload={"kind": "cli"})
    lines = observe.render_tree(observe.load(ob, session="A"), session="A")
    text = "\n".join(lines)
    assert "cli A" in text and "cli B" not in text


def test_tree_open_session_shows_open_status(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="P", correlation_id="P", payload={"kind": "cli"})
    line = observe.render_tree(observe.load(ob))[0]
    assert "[open]" in line


# --- report -------------------------------------------------------------
def test_report_clean_exits_zero(tmp_path):
    ob = new_outbox(tmp_path)
    seed_session_tree(ob)  # no reconcile.* events
    lines, code = observe.render_report(observe.load(ob))
    assert code == 0
    assert "clean" in lines[0]


def test_report_lists_findings_and_exits_nonzero(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "reconcile.gap_detected", correlation_id="i",
        payload={"gap_kind": "sequence", "missing_sequence": 3,
                 "prev_sequence": 2, "next_sequence": 4}, partial=False)
    add(ob, "reconcile.terminal_missing", session_id="P", correlation_id="P",
        payload={"subject_type": "session", "subject_id": "P",
                 "expected_terminal_event_type": "session.ended", "age_seconds": 500},
        partial=True)
    add(ob, "cron.run_missed", correlation_id="j1",
        payload={"job_id": "j1", "expected_fire_at": B, "missed_count": 2}, partial=True)
    lines, code = observe.render_report(observe.load(ob))
    text = "\n".join(lines)
    assert code == 1
    assert "missing #3" in text
    assert "session P has no session.ended" in text
    assert "job j1 missed 2 fire(s)" in text
    assert "reconcile.gap_detected=1" in text


# --- load / filters -----------------------------------------------------
def test_load_since_filter(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="old", occurred_at=B, payload={"kind": "cli"})
    add(ob, "session.created", session_id="new", occurred_at=B + 100, payload={"kind": "cli"})
    kept = observe.load(ob, since=B + 50)
    ids = [r.get("session_id") for r in kept]
    assert ids == ["new"]


def test_load_session_filter_matches_correlation_and_parent(tmp_path):
    ob = new_outbox(tmp_path)
    seed_session_tree(ob)  # all correlation_id="P"; child parent_session_id="P"
    add(ob, "session.created", session_id="Z", correlation_id="Z", payload={"kind": "cli"})
    kept = observe.load(ob, session="P")
    assert all(r.get("correlation_id") == "P" for r in kept)
    assert not any(r.get("session_id") == "Z" for r in kept)


def test_parse_since_accepts_epoch_and_iso():
    assert observe.parse_since("1784415000") == 1784415000.0
    assert observe.parse_since("1784415000.5") == 1784415000.5
    iso = observe.parse_since("2026-07-18T20:48:39-05:00")
    assert isinstance(iso, float) and iso > 1_700_000_000


# --- CLI ----------------------------------------------------------------
def test_cli_report_exit_code_and_no_init(tmp_path, capsys):
    bridge = str(tmp_path / "bridge")
    # not initialized -> exit 2
    assert main(["observe", "--report", "--flight-recorder-home", bridge]) == 2
    assert "not initialized" in capsys.readouterr().err

    ob = Outbox.open(bridge); ob.initialize()
    add(ob, "cron.run_missed", correlation_id="j1",
        payload={"job_id": "j1", "expected_fire_at": B, "missed_count": 1}, partial=True)
    ob.close()

    code = main(["observe", "--report", "--flight-recorder-home", bridge])
    out = capsys.readouterr().out
    assert code == 1  # a finding exists
    assert "job j1 missed" in out


def test_cli_stream_default_view(tmp_path, capsys):
    bridge = str(tmp_path / "bridge")
    ob = Outbox.open(bridge); ob.initialize()
    add(ob, "session.created", session_id="P", payload={"kind": "cli"})
    ob.close()
    code = main(["observe", "--flight-recorder-home", bridge])  # no view flag -> stream
    out = capsys.readouterr().out
    assert code == 0
    assert "── stream (1 events) ──" in out
    assert "session.created" in out
