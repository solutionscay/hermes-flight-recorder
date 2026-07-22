"""Full-pipeline integration tests for the observe surface (issue #7).

These tests build a synthetic ``~/.hermes`` state.db and cron store (with
the same schemas ``tests/test_reconcile.py`` uses), then run the REAL
pipeline end to end:

    state_db.poll(ob, hh) -> cron_db.poll(ob, hh) -> reconcile(ob, ...)
    -> observe.load(ob) -> render_stream / render_tree / render_report

This proves state_db, cron_db, reconcile, and observe compose: a durable
row polled into the outbox shows up in the tree with correct token
rollups, an unterminated durable row and a missed cron fire become
reconcile findings that render_report surfaces with a non-zero exit code,
and content stays encrypted throughout (never decrypted by observe).

Self-contained: no imports from other test modules.
"""

from __future__ import annotations

import datetime
import json
import sqlite3
from pathlib import Path

from hermes_flight_recorder import observe
from hermes_flight_recorder.cli import main
from hermes_flight_recorder.collector import cron_db, state_db
from hermes_flight_recorder.collector._common import build_record
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile

# A fixed epoch anchor (never wall-clock) and a US-Central-like offset for
# the cron store's ISO timestamps, matching tests/test_reconcile.py.
B = 1784415000.0
TZ = datetime.timezone(datetime.timedelta(hours=-5))

# Reconcile thresholds small enough that B + 250 already crosses them, so
# the whole pipeline is deterministic with no wall-clock dependency.
CFG = ReconcileConfig(subagent_terminal_timeout=100.0, cron_run_terminal_timeout=100.0)
NOW = B + 250  # subagent C (started B+5) is 245s old; the j1 cron gap sits at B+120


def iso(epoch: float) -> str:
    return datetime.datetime.fromtimestamp(epoch, TZ).isoformat()


def new_outbox(tmp_path) -> Outbox:
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


def add(
    ob,
    event_type,
    *,
    occurred_at=B,
    session_id=None,
    parent_session_id=None,
    correlation_id="corr",
    invocation_id=None,
    payload=None,
    partial=False,
    content=None,
):
    """Append a producer record straight to the outbox (bypassing polling)."""
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


# --- synthetic durable stores --------------------------------------------
def _write_state_db(hh: Path) -> None:
    """A root cli session P (ended) with a subagent child C (still open).

    P: message_count=2, tool_call_count=1, ended with its own token/cost
    rollup (100/20, $0.01) — so the tree reads its own_tokens from the
    session.ended row.
    C: never ends, so the tree must fall back to summing
    session_model_usage rows for it (40/8, $0.004) — proving the rollup is
    genuinely *derived* from the polled usage row, not hardcoded.
    """
    hh.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(hh / "state.db")
    conn.executescript(
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
    conn.execute(
        "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("P", "cli", None, "m", 2, 1, 100, 20, 0.01, B, B + 50, "done", "default", 1),
    )
    conn.execute(
        "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("C", "subagent", "P", "m", 1, 0, 0, 0, 0.0, B + 5, None, None, "default", 0),
    )
    conn.execute(
        "INSERT INTO messages VALUES (?,?,?,?,?,?,?,?,?)",
        (5, "P", "tool", "read_file", "tc1", None,
         '{"exit_code":0,"detail":"SECRET-XYZ"}', B + 2, None),
    )
    conn.execute(
        "INSERT INTO session_model_usage VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        ("P", "m", "chat", 2, 100, 20, 0, 0, 0.01, "estimated", B + 45),
    )
    conn.execute(
        "INSERT INTO session_model_usage VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        ("C", "m", "chat", 1, 40, 8, 0, 0, 0.004, "estimated", B + 10),
    )
    conn.commit()
    conn.close()


def _insert_extra_tool_message(hh: Path, msg_id: int, session_id: str, content: str, ts: float) -> None:
    """Simulate a row that lands in state.db after a poll already ran."""
    conn = sqlite3.connect(hh / "state.db")
    conn.execute(
        "INSERT INTO messages VALUES (?,?,?,?,?,?,?,?,?)",
        (msg_id, session_id, "tool", "write_file", "tc2", None, content, ts, None),
    )
    conn.commit()
    conn.close()


def _write_cron_store(hh: Path) -> None:
    """A 1-minute interval job j1 with a gap at B+120/B+180 (test_reconcile
    pattern): fired at B+60 and B+240, missing the two slots between.
    """
    cron = hh / "cron"
    cron.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(cron / "executions.db")
    conn.execute(
        "CREATE TABLE executions (id TEXT, job_id TEXT, source TEXT, pid INT, status TEXT, "
        "claimed_at TEXT, started_at TEXT, finished_at TEXT, error TEXT)"
    )
    conn.executemany(
        "INSERT INTO executions VALUES (?,?,'builtin',1,?,?,?,?,NULL)",
        [
            ("e1", "j1", "completed", iso(B + 60), iso(B + 60), iso(B + 61)),
            ("e2", "j1", "completed", iso(B + 240), iso(B + 240), iso(B + 241)),
        ],
    )
    conn.commit()
    conn.close()
    (cron / "jobs.json").write_text(json.dumps({"jobs": [{
        "id": "j1",
        "enabled": True,
        "state": "scheduled",
        "created_at": iso(B),
        "schedule": {"kind": "interval", "minutes": 1},
        "repeat": {"times": None, "completed": 0},
    }]}))
    (cron / "ticker_heartbeat").write_text(str(B + 250))


def _run_pipeline(tmp_path) -> tuple[Outbox, Path]:
    """state_db.poll -> cron_db.poll -> reconcile, over a fresh durable home."""
    hh = tmp_path / "hermes"
    _write_state_db(hh)
    _write_cron_store(hh)
    ob = new_outbox(tmp_path)
    state_db.poll(ob, hh)
    cron_db.poll(ob, hh)
    reconcile(ob, hh, now=NOW, config=CFG)
    return ob, hh


# --- tree: durable rows compose into a nested tree with real rollups -----
def test_full_pipeline_tree_nests_subagent_with_rollups_from_polled_rows(tmp_path):
    ob, _hh = _run_pipeline(tmp_path)
    lines = observe.render_tree(observe.load(ob))
    text = "\n".join(lines)

    p_line = next(l for l in lines if l.startswith("● "))
    assert "cli P" in p_line and "[done]" in p_line
    # P's own tokens come from its session.ended row.
    assert "tokens=100/20" in p_line and "cost=$0.0100" in p_line
    # subtree = own(P) 100/20 + own(C, derived from session_model_usage) 40/8
    assert "subtree tokens=140/28" in p_line and "cost=$0.0140" in p_line

    child_line = next(l for l in lines if "subagent C" in l)
    assert child_line.startswith("  ○ ")  # nested one level under P
    assert "[open]" in child_line  # C never ended in state.db

    assert "├─ tool read_file [ok]" in text  # exit_code 0 -> status "ok"


def test_session_filter_scopes_to_subagent_subtree_and_excludes_unrelated(tmp_path):
    ob, _hh = _run_pipeline(tmp_path)
    # An unrelated session appended straight to the outbox (not from any
    # durable row) must never leak into the P/C subtree view.
    add(ob, "session.created", session_id="Z", correlation_id="Z", payload={"kind": "cli"})

    filtered = observe.load(ob, session="P")
    assert all(r.get("correlation_id") == "P" for r in filtered)
    text = "\n".join(observe.render_tree(filtered, session="P"))
    assert "cli P" in text and "subagent C" in text
    assert "cli Z" not in text


# --- report: reconcile findings surface with the right exit code --------
def test_full_pipeline_report_surfaces_terminal_missing_and_cron_missed(tmp_path):
    ob, _hh = _run_pipeline(tmp_path)
    lines, code = observe.render_report(observe.load(ob))
    text = "\n".join(lines)
    assert code == 1
    assert "subagent C has no subagent.completed" in text
    assert "job j1 missed 2 fire(s)" in text
    assert "reconcile.terminal_missing=1" in text
    assert "cron.run_missed=1" in text


def test_full_pipeline_stream_is_ordered_and_covers_every_stage(tmp_path):
    ob, _hh = _run_pipeline(tmp_path)
    records = observe.load(ob)
    lines = observe.render_stream(records)
    seqs = [int(l.split()[0]) for l in lines]
    assert seqs == sorted(seqs) == list(range(1, len(seqs) + 1))

    seen = {r["payload"]["event_type"] for r in records}
    # state_db.poll, cron_db.poll, and reconcile all contributed events.
    assert {
        "session.created", "subagent.child_spawned", "tool.call_completed",
        "model.usage_recorded", "cron.run_claimed", "cron.run_finished",
        "cron.ticker_heartbeat", "reconcile.terminal_missing", "cron.run_missed",
    } <= seen


# --- coverage gap: a row that lands after the poll is proven dropped ----
def test_coverage_gap_detects_row_added_after_initial_poll(tmp_path):
    ob, hh = _run_pipeline(tmp_path)
    _insert_extra_tool_message(hh, 6, "P", '{"exit_code":0}', B + 45)

    counts = reconcile(ob, hh, now=NOW, config=CFG)  # no re-poll: row is "dropped"
    assert counts.get("reconcile.gap_detected") == 1

    gaps = [
        r for r in observe.load(ob)
        if r["payload"]["event_type"] == "reconcile.gap_detected"
    ]
    assert len(gaps) == 1
    assert gaps[0]["payload"]["subject_type"] == "message"
    assert gaps[0]["payload"]["subject_id"] == "6"

    lines, code = observe.render_report(observe.load(ob))
    assert code == 1
    assert "uncaptured message: 6 (state.db:messages)" in "\n".join(lines)


# --- idempotency across the whole pipeline -------------------------------
def test_reconcile_is_idempotent_after_full_pipeline(tmp_path):
    ob, hh = _run_pipeline(tmp_path)
    n = ob.count()
    second = reconcile(ob, hh, now=NOW, config=CFG)
    assert second == {}
    assert ob.count() == n


# --- content stays encrypted through every view --------------------------
def test_content_never_decrypted_in_any_rendered_view(tmp_path):
    ob, _hh = _run_pipeline(tmp_path)
    records = observe.load(ob)
    stream_lines = observe.render_stream(records)
    tree_lines = observe.render_tree(records)
    report_lines, _code = observe.render_report(records)
    everything = "\n".join(stream_lines + tree_lines + report_lines)

    assert "SECRET-XYZ" not in everything
    assert any("hash=sha256:" in l for l in stream_lines)


# --- CLI end to end --------------------------------------------------------
def test_cli_observe_report_reads_full_pipeline_outbox_end_to_end(tmp_path, capsys):
    ob, _hh = _run_pipeline(tmp_path)
    flight_recorder_home = str(ob._flight_recorder_home)
    ob.close()

    code = main(["observe", "--report", "--flight-recorder-home", flight_recorder_home])
    out = capsys.readouterr().out
    assert code == 1
    assert "job j1 missed 2 fire(s)" in out
    assert "subagent C has no subagent.completed" in out
