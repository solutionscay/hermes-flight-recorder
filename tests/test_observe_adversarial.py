"""Adversarial / robustness tests for the observe render functions (issue #7).

These feed the render functions plain, hand-built envelope-shaped dicts
(not run through Outbox.append / envelope.validate) so we can exercise
shapes validate() would normally reject: an unknown event_type, a payload
missing the fields a summary expects, and a parent_session_id cycle. The
render functions are documented as "testable without an outbox" precisely
because they only take plain record lists, so this is in-contract.

Two tests here (test_render_tree_session_filtered_mutual_cycle_terminates_bounded
and test_render_tree_self_parent_cycle_terminates_bounded) guard bounded
termination for a cyclic parent_session_id graph reached through the
--session filter: render_tree and subtree_tokens carry a visited-set, so a
malformed cycle renders each session once instead of recursing forever.
"""

from __future__ import annotations

from hermes_flight_recorder import observe
from hermes_flight_recorder.collector._common import build_record
from hermes_flight_recorder.collector.outbox import Outbox

B = 1784415000.0


# --- outbox-backed helpers (mirrors tests/test_observe.py) --------------
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


# --- raw record helper (bypasses envelope validation on purpose) -------
def mk(event_type, *, session_id=None, parent_session_id=None, correlation_id="corr",
       occurred_at=B, producer_sequence=1, installation_id="inst",
       payload=None, partial=False):
    """A minimal plain record dict -- exactly the shape render_* consume,
    deliberately unconstrained by envelope.validate() so we can build the
    adversarial shapes (unknown event_type, missing fields, id cycles)."""
    p = dict(payload or {})
    p["event_type"] = event_type
    return {
        "occurred_at": occurred_at,
        "producer_sequence": producer_sequence,
        "installation_id": installation_id,
        "session_id": session_id,
        "parent_session_id": parent_session_id,
        "correlation_id": correlation_id,
        "payload": p,
        "partial": partial,
    }


# --- session_id=None -----------------------------------------------------
def test_render_stream_and_tree_session_id_none_does_not_crash():
    records = [mk("session.created", session_id=None, payload={"kind": "cli"})]

    lines = observe.render_stream(records)
    assert len(lines) == 1
    assert "session.created" in lines[0]
    assert "None" not in lines[0]  # falls back to "-", never prints the literal None

    # a session-less record contributes nothing to the tree, but must not
    # raise (the _Index build path keys off session_id).
    tree_lines = observe.render_tree(records)
    assert tree_lines == ["(no sessions captured)"]


# --- payload missing summary fields --------------------------------------
def test_render_stream_missing_summary_fields_does_not_raise():
    # tool.call_completed normally has tool_name/status/effect_disposition;
    # give it none of them.
    records = [mk("tool.call_completed", session_id="P", payload={})]
    lines = observe.render_stream(records)
    assert len(lines) == 1
    assert "tool.call_completed" in lines[0]
    assert "tool_name=" not in lines[0]
    assert "status=" not in lines[0]


# --- unknown event_type fallback path -------------------------------------
def test_render_stream_unknown_event_type_uses_fallback_fields():
    payload = {"alpha": "one", "beta": "two", "gamma": "three", "delta": "four", "epsilon": "five"}
    records = [mk("totally.unknown.event", session_id="P", payload=payload)]
    line = observe.render_stream(records)[0]
    assert "totally.unknown.event" in line
    # fallback keeps the first 4 non-event_type keys, in insertion order
    assert "alpha=one" in line
    assert "beta=two" in line
    assert "gamma=three" in line
    assert "delta=four" in line
    assert "epsilon=five" not in line  # 5th field dropped by the [:4] slice


# --- unicode / long value truncation --------------------------------------
def test_short_truncates_long_unicode_value():
    long_val = "λμ秘密🔥" * 10  # unicode, well over 40 chars
    assert len(long_val) > 40
    s = observe._short(long_val)
    assert len(s) == 40
    assert s.endswith("…")
    assert s == long_val[:39] + "…"


# --- content ciphertext / plaintext must never leak -----------------------
def test_stream_tree_report_never_leak_ciphertext_or_plaintext(tmp_path):
    ob = new_outbox(tmp_path)
    secret = "秘密-SECRET-🔥" * 5
    add(ob, "tool.call_completed", session_id="P",
        payload={"tool_name": "terminal", "status": "ok"}, content=secret)
    add(ob, "reconcile.gap_detected", correlation_id="i",
        payload={"gap_kind": "sequence", "missing_sequence": 3,
                  "prev_sequence": 2, "next_sequence": 4})
    records = observe.load(ob)
    ciphertext_b64 = records[0]["content_ciphertext"]

    all_text = "\n".join(
        observe.render_stream(records)
        + observe.render_tree(records)
        + observe.render_report(records)[0]
    )
    assert secret not in all_text
    assert ciphertext_b64 not in all_text
    ob.close()


# --- report with missing optional finding fields --------------------------
def test_render_report_missing_optional_fields_does_not_raise():
    records = [
        mk("reconcile.terminal_missing", session_id="P",
           payload={"subject_type": "session", "subject_id": "P",
                    "expected_terminal_event_type": "session.ended"}),
    ]
    lines, code = observe.render_report(records)
    assert code == 1
    text = "\n".join(lines)
    assert "session P has no session.ended" in text
    assert "None" not in text  # age missing -> no stray "~Nones past window"


# --- parent_session_id cycles ---------------------------------------------
def test_render_tree_unfiltered_excludes_pure_cycle_members():
    """A mutual cycle with no reachable non-cycle root renders safely: the
    roots() scan requires parent is None or parent missing from the index,
    which every cycle member fails, so the whole component is (silently)
    dropped rather than traversed. This is the safe half of the cycle
    story -- contrast with the --session-filtered tests below."""
    records = [
        mk("session.created", session_id="A", parent_session_id="B", producer_sequence=1),
        mk("session.created", session_id="B", parent_session_id="A", producer_sequence=2),
    ]
    lines = observe.render_tree(records)
    assert lines == ["(no sessions captured)"]


def test_render_tree_session_filtered_mutual_cycle_terminates_bounded():
    """SPEC: render_tree must terminate with bounded output for any input,
    including a corrupted/replayed parent_session_id graph that forms a
    2-cycle (A.parent=B, B.parent=A). Reached via --session, which is
    exactly the CLI's --tree --session <id> path.

    Regression guard: _Index.subtree_tokens and _render_session carry a
    visited-set, so the cycle renders each session once and terminates
    instead of blowing the recursion limit.
    """
    records = [
        mk("session.created", session_id="A", parent_session_id="B", producer_sequence=1),
        mk("session.created", session_id="B", parent_session_id="A", producer_sequence=2),
    ]
    lines = observe.render_tree(records, session="A")
    assert isinstance(lines, list)
    assert 0 < len(lines) < 10_000


def test_render_tree_self_parent_cycle_terminates_bounded():
    """SPEC: a session that lists itself as its own parent_session_id is a
    degenerate 1-cycle; render_tree must still terminate with bounded
    output when filtered to that session.

    Regression guard: the visited-set stops subtree_tokens("A") from
    recursing into children["A"] == ["A"] forever.
    """
    records = [
        mk("session.created", session_id="A", parent_session_id="A", producer_sequence=1),
    ]
    lines = observe.render_tree(records, session="A")
    assert isinstance(lines, list)
    assert 0 < len(lines) < 10_000
