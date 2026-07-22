"""Tests for the observe REPORT view (render_report, _finding_detail, FINDING_TYPES).

Self-contained: mirrors tests/test_observe.py's style (a fixed epoch B, a
new_outbox(tmp_path) helper, and an add(...) helper built on
hermes_flight_recorder.collector._common.build_record + Outbox.append) but does not
import anything from another test module.

Covers: clean input -> exit 0 + 'clean' line; the exact detail wording for
each finding type (sequence gap, uncaptured_row, terminal_missing with/without
an age suffix, cron.run_missed with an ISO expected_fire_at); non-finding
events are ignored; the per-type summary line; ordering by producer_sequence;
and that exit code is 1 whenever >=1 finding exists, including one pass
through a real reconcile() run against a durable state.db.
"""

from __future__ import annotations

import datetime
import sqlite3

import pytest

from hermes_flight_recorder import observe
from hermes_flight_recorder.cli import main
from hermes_flight_recorder.collector._common import build_record
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile

B = 1784415000.0

_SESSIONS_SCHEMA = """
CREATE TABLE sessions (id TEXT, source TEXT, parent_session_id TEXT,
    started_at REAL, ended_at REAL, expiry_finalized INT, profile_name TEXT);
CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT);
CREATE TABLE session_model_usage (session_id TEXT, model TEXT, task TEXT);
"""


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


def raw_finding(event_type, seq, *, installation_id="inst-1", **payload):
    """A minimal plain envelope-shaped dict, bypassing the outbox entirely.

    observe.render_report's own docstring says the render functions take a
    plain list of records "so they are testable without an outbox" — used
    here to control producer_sequence directly for the ordering test.
    render_report/_stream_key only read payload, installation_id, and
    producer_sequence, so no full-envelope validation is involved.
    """
    payload = dict(payload)
    payload["event_type"] = event_type
    return {"installation_id": installation_id, "producer_sequence": seq, "payload": payload}


def _iso(epoch: float) -> str:
    return datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --- clean report ---------------------------------------------------------
def test_clean_input_exits_zero_with_clean_line(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="P", payload={"kind": "cli"})
    add(ob, "tool.call_completed", session_id="P",
        payload={"tool_name": "read_file", "status": "ok"})

    lines, code = observe.render_report(observe.load(ob))

    assert code == 0
    assert lines == ["clean: no gaps, missing terminals, or missed cron runs"]


def test_non_finding_events_are_ignored(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="P", payload={"kind": "cli"})
    add(ob, "tool.call_completed", session_id="P",
        payload={"tool_name": "read_file", "status": "ok"})
    add(ob, "reconcile.gap_detected", correlation_id="i",
        payload={"gap_kind": "sequence", "missing_sequence": 3,
                 "prev_sequence": 2, "next_sequence": 4})

    lines, code = observe.render_report(observe.load(ob))

    assert code == 1
    assert lines[0] == "1 finding(s):"
    assert "session.created" not in "\n".join(lines)
    assert "tool.call_completed" not in "\n".join(lines)


# --- exact detail wording ---------------------------------------------------
def test_sequence_gap_detail_wording(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "reconcile.gap_detected", correlation_id="i",
        payload={"gap_kind": "sequence", "missing_sequence": 3,
                 "prev_sequence": 2, "next_sequence": 4})

    lines, code = observe.render_report(observe.load(ob))

    assert code == 1
    detail_line = lines[2]
    assert detail_line.strip().endswith("sequence gap: missing #3 (between 2 and 4)")


def test_uncaptured_row_detail_wording(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "reconcile.gap_detected", session_id="S", correlation_id="S",
        payload={"gap_kind": "uncaptured_row", "subject_type": "session",
                 "subject_id": "S", "source_table": "state.db:sessions"})

    lines, code = observe.render_report(observe.load(ob))

    assert code == 1
    detail_line = lines[2]
    assert detail_line.strip().endswith("uncaptured session: S (state.db:sessions)")


@pytest.mark.parametrize(
    "extra_payload, expected_suffix",
    [
        ({"age_seconds": 500.7}, ", ~500s past window"),
        ({"staleness_seconds": 725.0}, ", ~725s past window"),
        ({}, ""),
    ],
    ids=["age_seconds", "staleness_seconds_fallback", "no_age_field"],
)
def test_terminal_missing_detail_wording(tmp_path, extra_payload, expected_suffix):
    ob = new_outbox(tmp_path)
    payload = {
        "subject_type": "session",
        "subject_id": "P",
        "expected_terminal_event_type": "session.ended",
        **extra_payload,
    }
    add(ob, "reconcile.terminal_missing", session_id="P", correlation_id="P",
        payload=payload, partial=True)

    lines, code = observe.render_report(observe.load(ob))

    assert code == 1
    detail_line = lines[2]
    expected = f"session P has no session.ended{expected_suffix}"
    assert detail_line.strip().endswith(expected)


def test_cron_run_missed_detail_wording_with_iso_expected_fire_at(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "cron.run_missed", correlation_id="j1",
        payload={"job_id": "j1", "expected_fire_at": B, "missed_count": 2}, partial=True)

    lines, code = observe.render_report(observe.load(ob))

    assert code == 1
    detail_line = lines[2]
    expected = f"job j1 missed 2 fire(s) from {_iso(B)}"
    assert detail_line.strip().endswith(expected)
    # sanity: the ISO string is the well-known UTC form, not a raw epoch
    assert detail_line.strip().endswith("Z")


# --- summary line ------------------------------------------------------------
def test_summary_line_counts_per_type(tmp_path):
    ob = new_outbox(tmp_path)
    for i in range(2):
        add(ob, "reconcile.gap_detected", correlation_id=f"g{i}",
            payload={"gap_kind": "sequence", "missing_sequence": i,
                     "prev_sequence": i - 1, "next_sequence": i + 1})
    add(ob, "reconcile.terminal_missing", session_id="P", correlation_id="P",
        payload={"subject_type": "session", "subject_id": "P",
                 "expected_terminal_event_type": "session.ended"})
    for i in range(3):
        add(ob, "cron.run_missed", correlation_id=f"j{i}",
            payload={"job_id": f"j{i}", "expected_fire_at": B, "missed_count": 1})

    lines, code = observe.render_report(observe.load(ob))

    assert code == 1
    assert lines[0] == "6 finding(s):"
    assert lines[-1] == (
        "summary: cron.run_missed=3, reconcile.gap_detected=2, reconcile.terminal_missing=1"
    )


# --- ordering ----------------------------------------------------------------
def test_report_orders_findings_by_producer_sequence(tmp_path):
    # Fed in scrambled order; render_report must sort by producer_sequence.
    records = [
        raw_finding("cron.run_missed", 5, job_id="job-C", expected_fire_at=B, missed_count=1),
        raw_finding("reconcile.gap_detected", 1, gap_kind="sequence",
                    missing_sequence=9, prev_sequence=8, next_sequence=10),
        raw_finding("reconcile.terminal_missing", 3, subject_type="session",
                    subject_id="mid-B", expected_terminal_event_type="session.ended"),
    ]

    lines, code = observe.render_report(records)

    assert code == 1
    assert lines[0] == "3 finding(s):"
    detail_lines = lines[2:5]
    assert "missing #9" in detail_lines[0]
    assert "mid-B" in detail_lines[1]
    assert "job-C" in detail_lines[2]


# --- exit code contract --------------------------------------------------
@pytest.mark.parametrize("event_type", list(observe.FINDING_TYPES))
def test_exit_code_is_one_for_every_finding_type(tmp_path, event_type):
    ob = new_outbox(tmp_path)
    payloads = {
        "reconcile.gap_detected": {"gap_kind": "sequence", "missing_sequence": 3,
                                    "prev_sequence": 2, "next_sequence": 4},
        "reconcile.terminal_missing": {"subject_type": "session", "subject_id": "S",
                                        "expected_terminal_event_type": "session.ended"},
        "reconcile.capture_stale": {"last_success_at": B, "staleness_seconds": 600.0,
                                     "threshold_seconds": 300.0},
        "cron.run_missed": {"job_id": "j1", "expected_fire_at": B, "missed_count": 1},
        "runtime.gateway_start_failed": {"reason_class": "token_conflict",
                                          "gateway_state": "degraded", "platform": "discord"},
    }
    add(ob, event_type, correlation_id="x", payload=payloads[event_type], partial=True)

    _, code = observe.render_report(observe.load(ob))

    assert code == 1


# --- CLI + real reconciler integration ---------------------------------------
def test_report_reflects_a_real_reconciler_terminal_missing_finding(tmp_path):
    hh = tmp_path / "hermes"
    hh.mkdir()
    db = sqlite3.connect(hh / "state.db")
    db.executescript(_SESSIONS_SCHEMA)
    db.execute("INSERT INTO sessions VALUES ('S','cli',NULL,?,NULL,0,NULL)", (B,))
    db.commit()
    db.close()

    ob = new_outbox(tmp_path)
    cfg = ReconcileConfig(session_terminal_timeout=100.0)
    reconcile(ob, hh, now=B + 500, config=cfg)  # 500s > 100s window -> terminal_missing
    ob.close()

    bridge = str(tmp_path / "bridge")
    code = main(["observe", "--report", "--flight-recorder-home", bridge])

    assert code == 1  # >=1 finding exists (terminal_missing, plus a coverage gap)


def test_cli_clean_report_exits_zero(tmp_path, capsys):
    bridge = str(tmp_path / "bridge")
    ob = Outbox.open(bridge)
    ob.initialize()
    add(ob, "session.created", session_id="P", payload={"kind": "cli"})
    ob.close()

    code = main(["observe", "--report", "--flight-recorder-home", bridge])
    out = capsys.readouterr().out

    assert code == 0
    assert "clean: no gaps, missing terminals, or missed cron runs" in out
