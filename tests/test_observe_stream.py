"""Tests for the observe STREAM view (issue #7): render_stream, _payload_summary,
_SUMMARY_FIELDS, and the _short / _iso rendering helpers.

Self-contained: does not import anything from tests/test_observe.py or
tests/test_reconcile.py. Records are appended through a real Outbox (so
they carry a real producer_sequence and validate as envelopes) except
where the test is specifically about render_stream's own sort behavior
across installations, which uses plain dict records per observe.py's own
docstring: "The render functions take a plain list of envelope records,
so they are testable without an outbox."
"""

from __future__ import annotations

import datetime
import hashlib
import json
import sqlite3

from hermes_dbass import observe
from hermes_dbass.cli import main  # noqa: F401  (imported per task spec; CLI not re-tested here)
from hermes_dbass.collector import cron_db, state_db  # noqa: F401  (state_db imported per task spec)
from hermes_dbass.collector._common import build_record
from hermes_dbass.collector.outbox import Outbox
from hermes_dbass.collector.reconcile import ReconcileConfig, reconcile  # noqa: F401

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


def iso(epoch: float) -> str:
    return datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc).isoformat()


# --- strict (installation_id, producer_sequence) ordering ----------------
def test_stream_sorts_across_installations_even_when_insertion_order_disagrees():
    """render_stream takes a plain list (per its docstring) and must sort by
    (installation_id, producer_sequence), not by list/insertion order. Build
    a mix across two installation ids where insertion order and the correct
    sort order are different in both the installation and the sequence axis.
    """
    def rec(inst, seq, sid):
        return {
            "installation_id": inst,
            "producer_sequence": seq,
            "occurred_at": B,
            "session_id": sid,
            "payload": {"event_type": "session.created", "kind": "cli"},
            "partial": False,
        }

    # Insertion order: B-2, A-5, B-1, A-1. Correct order: A-1, A-5, B-1, B-2.
    records = [
        rec("B", 2, "s-B2"),
        rec("A", 5, "s-A5"),
        rec("B", 1, "s-B1"),
        rec("A", 1, "s-A1"),
    ]
    lines = observe.render_stream(records)
    sids_in_order = [l.split()[3] for l in lines]  # session_id column
    assert sids_in_order == ["s-A1", "s-A5", "s-B1", "s-B2"]


def test_stream_resorts_by_sequence_regardless_of_how_records_are_passed_in(tmp_path):
    """Within one installation, feed render_stream a shuffled list (not the
    outbox's own natural order) and confirm it re-derives ascending
    producer_sequence order rather than trusting caller order.
    """
    ob = new_outbox(tmp_path)
    for i in range(5):
        add(ob, "session.created", session_id=f"S{i}", payload={"kind": "cli"})
    natural = observe.load(ob)
    shuffled = list(reversed(natural))
    assert [r["producer_sequence"] for r in shuffled] == [5, 4, 3, 2, 1]

    lines = observe.render_stream(shuffled)
    seqs = [int(l.split()[0]) for l in lines]
    assert seqs == [1, 2, 3, 4, 5]


# --- per-event-family summary fields --------------------------------------
_CASES: list[tuple[str, dict, str]] = [
    ("tool.call_completed",
     {"tool_name": "read_file", "status": "ok", "effect_disposition": "none"},
     "tool_name=read_file status=ok effect_disposition=none"),
    ("model.usage_recorded",
     {"model": "gpt", "input_tokens": 100, "output_tokens": 20, "estimated_cost_usd": 0.0125},
     "model=gpt input_tokens=100 output_tokens=20 estimated_cost_usd=0.0125"),
    ("session.created",
     {"kind": "cli", "model": "gpt"},
     "kind=cli model=gpt"),
    ("session.ended",
     {"kind": "cli", "end_reason": "done", "input_tokens": 50, "output_tokens": 10,
      "estimated_cost_usd": 0.02},
     "kind=cli end_reason=done input_tokens=50 output_tokens=10 estimated_cost_usd=0.02"),
    ("subagent.child_spawned",
     {"kind": "subagent", "model": "gpt"},
     "kind=subagent model=gpt"),
    ("subagent.completed",
     {"kind": "subagent", "end_reason": "agent_close"},
     "kind=subagent end_reason=agent_close"),
    ("delegation.dispatched",
     {"delegation_id": "d1", "state": "open", "is_batch": False},
     "delegation_id=d1 state=open is_batch=False"),
    ("cron.run_claimed",
     {"job_id": "j1", "status": "claimed"},
     "job_id=j1 status=claimed"),
    ("cron.run_finished",
     {"job_id": "j1", "status": "completed", "ok": True},
     "job_id=j1 status=completed ok=True"),
    ("cron.run_missed",
     {"job_id": "j1", "expected_fire_at": B, "missed_count": 2},
     f"job_id=j1 expected_fire_at={observe._iso(B)} missed_count=2"),
    ("cron.ticker_heartbeat",
     {"heartbeat": B, "last_success": B - 30},
     f"heartbeat={observe._iso(B)} last_success={observe._iso(B - 30)}"),
    ("reconcile.gap_detected",
     {"gap_kind": "sequence", "subject_type": "session", "subject_id": "S", "missing_sequence": 3},
     "gap_kind=sequence subject_type=session subject_id=S missing_sequence=3"),
    ("reconcile.terminal_missing",
     {"subject_type": "session", "subject_id": "P", "expected_terminal_event_type": "session.ended"},
     "subject_type=session subject_id=P expected_terminal_event_type=session.ended"),
]


def test_summary_fields_match_spec_per_event_family(tmp_path):
    ob = new_outbox(tmp_path)
    for et, payload, _expected in _CASES:
        add(ob, et, payload=payload)
    lines = observe.render_stream(observe.load(ob))
    assert len(lines) == len(_CASES)
    for (et, _payload, expected_summary), line in zip(_CASES, lines):
        assert line.endswith(expected_summary), f"{et}: got {line!r}, want suffix {expected_summary!r}"


def test_summary_omits_fields_that_are_absent_or_none(tmp_path):
    """_payload_summary skips a _SUMMARY_FIELDS key when it is absent or
    explicitly None (e.g. effect_disposition not yet known)."""
    ob = new_outbox(tmp_path)
    add(ob, "tool.call_completed",
        payload={"tool_name": "terminal", "status": "ok", "effect_disposition": None})
    line = observe.render_stream(observe.load(ob))[0]
    assert "tool_name=terminal status=ok" in line
    assert "effect_disposition" not in line


def test_fallback_summary_for_event_type_outside_summary_fields(tmp_path):
    """An event_type with no entry in _SUMMARY_FIELDS falls back to the
    first four plaintext payload keys (event_type itself excluded)."""
    assert "runtime.gateway_started" not in observe._SUMMARY_FIELDS
    ob = new_outbox(tmp_path)
    add(ob, "runtime.gateway_started", payload={"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})
    line = observe.render_stream(observe.load(ob))[0]
    assert "a=1 b=2 c=3 d=4" in line
    assert "e=5" not in line  # only the first four keys are kept


# --- partial marker --------------------------------------------------------
def test_partial_marker_appears_only_when_record_is_partial(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "reconcile.terminal_missing", session_id="P",
        payload={"subject_type": "session", "subject_id": "P",
                 "expected_terminal_event_type": "session.ended"}, partial=True)
    add(ob, "session.created", session_id="Q", payload={"kind": "cli"}, partial=False)
    lines = observe.render_stream(observe.load(ob))
    assert lines[0].endswith("partial")
    assert not lines[1].endswith("partial")


# --- content_hash: shown truncated, plaintext content never shown ---------
def test_content_hash_shown_truncated_and_never_the_plaintext(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "tool.call_completed", session_id="P",
        payload={"tool_name": "terminal", "status": "ok"}, content="SECRET-ARGS")
    record = observe.load(ob)[0]
    expected_hash = "sha256:" + hashlib.sha256(b"SECRET-ARGS").hexdigest()
    assert record["content_hash"] == expected_hash

    line = observe.render_stream([record])[0]
    assert f"hash={expected_hash[:14]}…" in line
    assert expected_hash not in line  # only the 14-char prefix is shown
    assert "SECRET-ARGS" not in line


# --- occurred_at as ISO-UTC -------------------------------------------------
def test_occurred_at_rendered_as_iso_utc(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "session.created", occurred_at=B, session_id="P", payload={"kind": "cli"})
    line = observe.render_stream(observe.load(ob))[0]
    expected = datetime.datetime.fromtimestamp(B, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert expected in line
    assert expected == "2026-07-18T22:50:00Z"  # B is a known fixed epoch; pin the literal too


# --- _short: epochs vs small floats format differently ----------------------
def test_short_renders_epoch_as_iso_and_cost_as_decimal():
    # An epoch-scale float (e.g. expected_fire_at) renders as an ISO string.
    assert observe._short(B) == observe._iso(B)
    assert observe._short(B).endswith("Z") and "T" in observe._short(B)
    # A small float (e.g. a dollar cost) renders as a plain decimal, not ISO.
    assert observe._short(0.0125) == "0.0125"
    assert observe._short(0.02) == "0.02"
    assert observe._short(0.0) == "0"
    # The boundary is a strict ">"; exactly 1_000_000_000 is still decimal.
    assert observe._short(1_000_000_000.0) == "1000000000"
    assert observe._short(1_000_000_001.0) == observe._iso(1_000_000_001.0)


# --- integration: real cron_db.poll + reconcile pipeline through stream ----
def _executions_db(cron_dir, rows) -> None:
    db = sqlite3.connect(cron_dir / "executions.db")
    db.execute(
        "CREATE TABLE executions (id TEXT, job_id TEXT, source TEXT, pid INT, status TEXT, "
        "claimed_at TEXT, started_at TEXT, finished_at TEXT, error TEXT)"
    )
    db.executemany(
        "INSERT INTO executions VALUES (?,?,'builtin',1,?,?,?,?,NULL)",
        [(exid, job, status, claimed, started, finished)
         for (exid, job, status, claimed, started, finished) in rows],
    )
    db.commit()
    db.close()


def _jobs_json(cron_dir, jobs) -> None:
    (cron_dir / "jobs.json").write_text(json.dumps({"jobs": jobs}))


def test_stream_renders_real_cron_pipeline_output_in_strict_sequence_order(tmp_path):
    """cron_db.poll() captures run_claimed/run_finished, reconcile() derives
    a cron.run_missed finding; render_stream must show all of them, in
    strict producer_sequence order, with the documented per-family fields.
    """
    hh = tmp_path / "hermes"
    hh.mkdir()
    cron = hh / "cron"
    cron.mkdir()
    # A 1-minute job: fired at B+60 and B+240, so B+120/B+180 are missed
    # (collapsed into a single run_missed with missed_count=2).
    _executions_db(cron, [
        ("e1", "j1", "completed", iso(B + 60), iso(B + 60), iso(B + 61)),
        ("e2", "j1", "completed", iso(B + 240), iso(B + 240), iso(B + 241)),
    ])
    _jobs_json(cron, [{
        "id": "j1", "enabled": True, "state": "scheduled", "created_at": iso(B),
        "schedule": {"kind": "interval", "minutes": 1},
        "repeat": {"times": None, "completed": 0},
    }])
    (cron / "ticker_heartbeat").write_text(str(B + 250))

    ob = new_outbox(tmp_path)
    cron_db.poll(ob, hh)
    counts = reconcile(ob, hh, now=B + 250)
    assert counts.get("cron.run_missed") == 1

    lines = observe.render_stream(observe.load(ob))
    seqs = [int(l.split()[0]) for l in lines]
    assert seqs == sorted(seqs)  # strict producer_sequence order

    claimed = [l for l in lines if "cron.run_claimed" in l]
    finished = [l for l in lines if "cron.run_finished" in l]
    missed = [l for l in lines if "cron.run_missed" in l]
    assert len(claimed) == 2 and all("job_id=j1" in l and "status=completed" in l for l in claimed)
    assert len(finished) == 2 and all("ok=True" in l for l in finished)
    assert len(missed) == 1
    assert f"expected_fire_at={observe._iso(B + 120)}" in missed[0]
    assert "missed_count=2" in missed[0]
