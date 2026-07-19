#!/usr/bin/env python3
"""Phase 0 POC exit-gate — capture, reconcile, observe end to end (issue #8).

Runs the whole Bridge pipeline against a throwaway, synthetic-but-schema-
accurate Hermes home and proves the Phase 0 claim: **loss is detectable.**
Four scenarios, each on its own disposable outbox:

1. Happy path       — a full session (hook + poll) reconciles clean; report exit 0.
2. Dropped capture  — delete one captured event; the reconciler flags exactly
                      one `reconcile.gap_detected` (sequence hole); report exit != 0.
3. Missed cron      — a scheduled fire with no execution row yields exactly one
                      `cron.run_missed`; report exit != 0.
4. Bridge restart   — reopen the outbox; the `producer_sequence` high-water mark
                      survives, and the next append continues with no reuse or gap.

Determinism: a fixed `now` (no wall clock) and a fixed synthetic home, so the
result is identical on any host at any time. This is the automated gate; for a
run against a *real* Hermes home, see docs/poc-demo.md.

Usage:  python scripts/poc_exit_gate.py [-v]
Exit:   0 if every scenario passes its assertions, 1 otherwise.
"""

from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from hermes_flight_recorder import observe
from hermes_flight_recorder.collector import cron_db, state_db
from hermes_flight_recorder.collector.hook import SPOOL_FILENAME, drain as drain_hook
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile

# A fixed clock. Everything is anchored to it so the gate never depends on when
# it runs. NOW sits shortly after the demo session and cron activity.
NOW = 1_800_000_000.0
CFG = ReconcileConfig()
VERBOSE = "-v" in sys.argv[1:]


def _iso(epoch: float) -> str:
    """A Hermes-style ISO 8601 timestamp with an explicit UTC offset."""
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


# --- synthetic Hermes home ------------------------------------------------
def build_state_db(home: Path) -> None:
    """A CLI parent session and a subagent child, both ended, with tool calls,
    model usage, and one delegation — mirrors the real probe schema."""
    home.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(home / "state.db")
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
        "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            # Both ended, so a clean home has no missing session terminal.
            ("root", "cli", None, "m", 8, 2, 18071, 825, 0.0, NOW - 300, NOW - 60, "cli_close", None, 1),
            ("sub", "subagent", "root", "m", 4, 1, 12278, 126, 0.0, NOW - 250, NOW - 200, "agent_close", None, 1),
        ],
    )
    db.executemany(
        "INSERT INTO messages VALUES (?,?,?,?,?,?,?,?)",
        [
            (3, "root", "user", None, None, None, "do the thing", NOW - 299),
            (5, "root", "tool", "terminal", None, None, '{"output":"ok","exit_code":0}', NOW - 280),
            (7, "root", "tool", "delegate_task", None, None, '{"status":"dispatched","count":1}', NOW - 255),
            (10, "sub", "tool", "read_file", None, None, '{"content":"data"}', NOW - 230),
        ],
    )
    db.executemany(
        "INSERT INTO session_model_usage VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [("root", "m", "", 4, 18071, 825, 55296, 452, 0.0, "estimated", NOW - 100)],
    )
    db.execute(
        "INSERT INTO async_delegations VALUES (?,?,?,?,?,?,?,?,?)",
        ("deleg_1", "root", "root", "completed", "delivered", 4023601, NOW - 255,
         '{"goal":"read the file","is_batch":true}', '{"results":[{"summary":"data"}]}'),
    )
    db.commit()
    db.close()


def build_cron_store(home: Path, *, missed: bool = False) -> None:
    """A cron store. With ``missed=True`` a 1-minute interval job has a fire
    instant with no execution row, so the reconciler must flag one missed run."""
    cron = home / "cron"
    cron.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(cron / "executions.db")
    db.execute(
        "CREATE TABLE executions (id TEXT, job_id TEXT, source TEXT, pid INT, status TEXT, "
        "claimed_at TEXT, started_at TEXT, finished_at TEXT, error TEXT)"
    )
    rows = [
        ("e1", "j1", "builtin", 111, "completed", _iso(NOW - 200), _iso(NOW - 200), _iso(NOW - 199), None),
    ]
    jobs: list[dict] = [{"id": "j1", "name": "probe"}]  # no schedule -> never "missed"
    if missed:
        # Interval fires expected at NOW-130, NOW-70, NOW-10 (anchored on the
        # first execution). NOW-70 has no execution -> exactly one missed run.
        # The last execution sits at NOW-10 so no open-ended tail forms.
        rows += [
            ("m1", "j2", "builtin", 222, "completed", _iso(NOW - 130), _iso(NOW - 130), _iso(NOW - 129), None),
            ("m2", "j2", "builtin", 333, "completed", _iso(NOW - 10), _iso(NOW - 10), _iso(NOW - 9), None),
        ]
        jobs.append({
            "id": "j2", "name": "heartbeat-probe", "created_at": _iso(NOW - 600),
            "schedule": {"kind": "interval", "minutes": 1},
        })
    db.executemany("INSERT INTO executions VALUES (?,?,?,?,?,?,?,?,?)", rows)
    db.commit()
    db.close()
    # A fresh heartbeat, so the scheduler is not judged dead.
    (cron / "ticker_heartbeat").write_text(str(NOW - 10))
    (cron / "ticker_last_success").write_text(str(NOW - 10))
    (cron / "jobs.json").write_text(json.dumps({"jobs": jobs}))


def seed_spool(bridge: Path) -> None:
    """The live hook spool for one Discord-style turn on session ``root``."""
    bridge.mkdir(parents=True, exist_ok=True)
    events = [
        ("gateway:startup", {"platforms": ["cli"]}, NOW - 305),
        ("session:start", {"platform": "cli", "user_id": "u1", "session_id": "root", "session_key": "lane"}, NOW - 300),
        ("agent:start", {"platform": "cli", "user_id": "u1", "session_id": "root", "chat_type": "dm", "message": "hello"}, NOW - 295),
        ("agent:end", {"platform": "cli", "user_id": "u1", "session_id": "root", "chat_type": "dm", "message": "hello", "response": "hi back"}, NOW - 280),
        ("session:end", {"platform": "cli", "user_id": "u1", "session_key": "lane"}, NOW - 60),
    ]
    lines = [json.dumps({"event_type": et, "context": ctx, "captured_at": ts}) for et, ctx, ts in events]
    (bridge / SPOOL_FILENAME).write_text("\n".join(lines) + "\n")


# --- pipeline helpers -----------------------------------------------------
def run_pipeline(ob: Outbox, hermes_home: Path) -> None:
    """What `hermes-flight-recorder run` does: drain the hook spool, then poll
    the durable stores into the same outbox."""
    drain_hook(ob)
    state_db.poll(ob, hermes_home)
    cron_db.poll(ob, hermes_home)


def report_code(ob: Outbox) -> tuple[list[str], int]:
    _, code = None, 0
    lines, code = observe.render_report(observe.load(ob))
    return lines, code


def show(ob: Outbox, title: str) -> None:
    if not VERBOSE:
        return
    print(f"\n    --- {title}: observe --report ---")
    lines, code = report_code(ob)
    for ln in lines:
        print(f"    {ln}")
    print(f"    [exit {code}]")


def drop_event(ob_path: Path, event_type: str) -> int:
    """Delete one captured event by type — simulate a lost hook capture. The
    high-water mark is untouched, so a sequence hole opens (no reuse)."""
    conn = sqlite3.connect(ob_path)
    conn.row_factory = sqlite3.Row
    seq = None
    for row in conn.execute("SELECT producer_sequence, envelope_json FROM events"):
        if json.loads(row["envelope_json"])["payload"]["event_type"] == event_type:
            seq = row["producer_sequence"]
            break
    if seq is None:
        conn.close()
        raise AssertionError(f"no {event_type} event to drop")
    conn.execute("DELETE FROM events WHERE producer_sequence=?", (seq,))
    conn.commit()
    conn.close()
    return seq


# --- scenarios ------------------------------------------------------------
def scenario_happy(tmp: Path) -> list[str]:
    fails: list[str] = []
    bridge, hermes = tmp / "b", tmp / "h"
    build_state_db(hermes)
    build_cron_store(hermes)
    seed_spool(bridge)

    ob = Outbox.open(bridge)
    ob.initialize()
    run_pipeline(ob, hermes)
    findings = reconcile(ob, hermes, now=NOW, config=CFG)
    show(ob, "happy")
    _, code = report_code(ob)

    if findings:
        fails.append(f"happy: expected no findings, got {dict(findings)}")
    if code != 0:
        fails.append(f"happy: expected report exit 0, got {code}")
    if ob.count() < 12:
        fails.append(f"happy: expected a full stream, only {ob.count()} events")
    ob.close()
    return fails


def scenario_dropped_capture(tmp: Path) -> list[str]:
    fails: list[str] = []
    bridge, hermes = tmp / "b", tmp / "h"
    build_state_db(hermes)
    build_cron_store(hermes)
    seed_spool(bridge)

    ob = Outbox.open(bridge)
    ob.initialize()
    run_pipeline(ob, hermes)
    ob.close()

    # Lose one live event (the hook's invocation.completed): a middle sequence.
    missing = drop_event(bridge / "outbox.sqlite", "invocation.completed")

    ob = Outbox.open(bridge)
    findings = reconcile(ob, hermes, now=NOW, config=CFG)
    show(ob, "dropped-capture")
    _, code = report_code(ob)

    if findings != {"reconcile.gap_detected": 1}:
        fails.append(f"dropped: expected exactly one gap_detected, got {dict(findings)}")
    gap = next((e for e in ob.iter_events() if e["payload"]["event_type"] == "reconcile.gap_detected"), None)
    if not gap or gap["payload"].get("gap_kind") != "sequence" or gap["payload"].get("missing_sequence") != missing:
        fails.append(f"dropped: gap does not point at the lost sequence {missing}")
    if code == 0:
        fails.append("dropped: expected report exit != 0")
    ob.close()
    return fails


def scenario_missed_cron(tmp: Path) -> list[str]:
    fails: list[str] = []
    bridge, hermes = tmp / "b", tmp / "h"
    build_state_db(hermes)
    build_cron_store(hermes, missed=True)
    seed_spool(bridge)

    ob = Outbox.open(bridge)
    ob.initialize()
    run_pipeline(ob, hermes)
    findings = reconcile(ob, hermes, now=NOW, config=CFG)
    show(ob, "missed-cron")
    _, code = report_code(ob)

    if findings != {"cron.run_missed": 1}:
        fails.append(f"missed-cron: expected exactly one cron.run_missed, got {dict(findings)}")
    miss = next((e for e in ob.iter_events() if e["payload"]["event_type"] == "cron.run_missed"), None)
    if not miss or miss["payload"].get("job_id") != "j2":
        fails.append("missed-cron: finding does not point at job j2")
    if code == 0:
        fails.append("missed-cron: expected report exit != 0")
    ob.close()
    return fails


def scenario_restart(tmp: Path) -> list[str]:
    fails: list[str] = []
    bridge, hermes = tmp / "b", tmp / "h"
    build_state_db(hermes)
    build_cron_store(hermes)
    seed_spool(bridge)

    ob = Outbox.open(bridge)
    ob.initialize()
    run_pipeline(ob, hermes)
    hw, n, inst = ob.high_water(), ob.count(), ob.installation_id
    ob.close()  # simulate the Bridge process stopping

    # Reopen — a fresh process/handle onto the same durable outbox.
    ob = Outbox.open(bridge)
    if ob.high_water() != hw:
        fails.append(f"restart: high-water {ob.high_water()} != pre-restart {hw}")
    if ob.installation_id != inst:
        fails.append("restart: installation_id changed across restart")
    if ob.count() != n:
        fails.append(f"restart: event count {ob.count()} != {n}")

    # A new capture appends to the durable spool; the drain continues from the
    # persisted cursor (which survived the restart), so the sequence continues
    # with no reuse, no gap, and no duplicate.
    with open(bridge / SPOOL_FILENAME, "a", encoding="utf-8") as fh:
        fh.write(json.dumps({"event_type": "gateway:startup", "context": {"platforms": []}, "captured_at": NOW}) + "\n")
    drain_hook(ob)
    if ob.high_water() != hw + 1:
        fails.append(f"restart: next sequence {ob.high_water()} != {hw + 1}")
    if ob.count() != n + 1:
        fails.append(f"restart: expected exactly one new row, count {ob.count()} != {n + 1}")
    ob.close()
    return fails


SCENARIOS = [
    ("happy path", scenario_happy),
    ("dropped capture", scenario_dropped_capture),
    ("missed cron", scenario_missed_cron),
    ("bridge restart", scenario_restart),
]


def main() -> int:
    print("Phase 0 POC exit-gate (issue #8)")
    print("=" * 48)
    all_fails: list[str] = []
    for name, fn in SCENARIOS:
        with tempfile.TemporaryDirectory() as d:
            fails = fn(Path(d))
        status = "PASS" if not fails else "FAIL"
        print(f"  [{status}] {name}")
        for f in fails:
            print(f"         - {f}")
        all_fails += fails
    print("=" * 48)
    if all_fails:
        print(f"GATE FAILED — {len(all_fails)} assertion(s) failed")
        return 1
    print("GATE PASSED — capture is loss-detectable across restarts and cron misses")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
