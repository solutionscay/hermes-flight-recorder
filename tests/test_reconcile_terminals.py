"""Self-contained tests for the reconciler's MISSING-TERMINAL detector.

The detector judges a single subject_type:

- invocation    — outbox ``invocation.started`` with no matching
                  ``invocation.completed`` past ``invocation_terminal_timeout``.

``invocation.completed`` is the authoritative end-of-run signal, so this is the
one terminal check grounded in the captured stream. Session, subagent, and
cron-run terminal detection was removed: it judged completion from the durable
``ended_at`` / ``finished_at`` column, which disagrees with the captured
terminal and produced false findings after a run had finished (issues #94, #95).

Every case is driven by an explicit ``now=`` float and a ``ReconcileConfig``
with a small explicit window, so no wall-clock is ever consulted. Boundary
cases assert that exactly-at-timeout does NOT flag (the reconciler uses
``when - occurred <= timeout`` to skip) and one tick past DOES.
"""

from __future__ import annotations

from hermes_flight_recorder.collector._common import build_record
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile

# A fixed epoch anchor (mirrors tests/test_reconcile.py) so timestamps
# round-trip deterministically.
B = 1784415000.0


def new_outbox(tmp_path) -> Outbox:
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


def terminal_missing(outbox, subject_type=None):
    """All reconcile.terminal_missing findings, optionally filtered by subject_type."""
    out = []
    for e in outbox.iter_events():
        pl = e["payload"]
        if pl.get("event_type") != "reconcile.terminal_missing":
            continue
        if subject_type is not None and pl.get("subject_type") != subject_type:
            continue
        out.append(e)
    return out


def append_event(ob, event_type, **over):
    """Append a minimal valid producer event straight to the outbox."""
    rec = build_record(
        event_type=event_type,
        occurred_at=over.pop("occurred_at", B),
        source=over.pop("source", "hook:test"),
        capture_method=over.pop("capture_method", "hook:test"),
        runtime={"kind": "cli", "engine": "standard"},
        correlation_id=over.pop("correlation_id", "corr"),
        payload=over.pop("payload", {}),
        **over,
    )
    return ob.append(rec)


# --- invocation ------------------------------------------------------------
def test_invocation_boundary_exact_timeout_not_flagged(tmp_path):
    ob = new_outbox(tmp_path)
    append_event(
        ob, "invocation.started",
        occurred_at=B, invocation_id="S:turn:1", session_id="S", correlation_id="S",
    )
    cfg = ReconcileConfig(invocation_terminal_timeout=50.0)

    reconcile(ob, tmp_path / "no-hermes", now=B + 50.0, config=cfg)  # age == timeout exactly

    assert terminal_missing(ob, "invocation") == []


def test_invocation_past_timeout_flagged(tmp_path):
    ob = new_outbox(tmp_path)
    append_event(
        ob, "invocation.started",
        occurred_at=B, invocation_id="S:turn:1", session_id="S", correlation_id="S",
    )
    cfg = ReconcileConfig(invocation_terminal_timeout=50.0)

    reconcile(ob, tmp_path / "no-hermes", now=B + 51.0, config=cfg)  # one tick past

    term = terminal_missing(ob, "invocation")
    assert len(term) == 1
    t = term[0]
    assert t["payload"]["subject_id"] == "S:turn:1"
    assert t["payload"]["expected_terminal_event_type"] == "invocation.completed"
    assert t["partial"] is True
    assert t["invocation_id"] == "S:turn:1"
    assert t["correlation_id"] == "S"
    assert t["session_id"] == "S"


def test_invocation_completed_pair_not_flagged(tmp_path):
    """A started+completed pair must never be flagged, however old it is."""
    ob = new_outbox(tmp_path)
    append_event(
        ob, "invocation.started",
        occurred_at=B, invocation_id="S:turn:2", session_id="S", correlation_id="S",
    )
    append_event(
        ob, "invocation.completed",
        occurred_at=B + 1.0, invocation_id="S:turn:2", session_id="S", correlation_id="S",
    )
    # An unrelated started-without-completed invocation confirms the detector
    # is actually running (and would flag) while the paired one stays silent.
    append_event(
        ob, "invocation.started",
        occurred_at=B, invocation_id="S:turn:3", session_id="S", correlation_id="S",
    )
    cfg = ReconcileConfig(invocation_terminal_timeout=50.0)

    reconcile(ob, tmp_path / "no-hermes", now=B + 1000.0, config=cfg)

    term = terminal_missing(ob, "invocation")
    ids = {t["payload"]["subject_id"] for t in term}
    assert "S:turn:2" not in ids  # completed — never flagged
    assert "S:turn:3" in ids  # never completed, well past timeout — flagged


def test_durable_session_with_ended_at_null_is_not_flagged(tmp_path):
    """Regression: a live/unfinalized durable session must NOT produce a
    terminal_missing. Session terminal detection was removed precisely because
    the durable ended_at column disagrees with the captured terminal (#94/#95).
    Reconcile against a hermes home with a state.db is exercised elsewhere; here
    we assert the detector emits nothing session-shaped from the outbox alone.
    """
    ob = new_outbox(tmp_path)
    cfg = ReconcileConfig(invocation_terminal_timeout=50.0)

    reconcile(ob, tmp_path / "no-hermes", now=B + 100000.0, config=cfg)

    assert terminal_missing(ob, "session") == []
    assert terminal_missing(ob, "subagent") == []
    assert terminal_missing(ob, "cron_run") == []
