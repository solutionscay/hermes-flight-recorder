#!/usr/bin/env python3
"""Cron-death live check — the stale-ticker finding against real Hermes cron.

``tests/test_reconcile_stale_ticker.py`` proves the reconciler's stale-ticker
semantics against *hand-built* ``jobs.json`` / ``executions.db`` /
``ticker_heartbeat`` fixtures. This check closes the gap those fixtures leave
open: it drives the **real** Hermes cron subsystem so every input the detector
reads is a byte Hermes itself wrote, then asserts the one behaviour from the
project's headline question — *"how do you detect a cron run that never
existed?"*:

- **Leg A — real running home, read-only (no false positive).** Capture the
  live Hermes home and reconcile at wall-clock ``now``. A *healthy* ticker (a
  fresh ``ticker_heartbeat``) must raise **zero** ``cron_ticker`` findings, even
  though real cron executions and a real heartbeat are present. If the host's
  ticker is not currently healthy (no store, or a heartbeat already older than
  the stale window), the leg SKIPS rather than asserting — it cannot prove a
  no-false-positive against a genuinely dead scheduler.

- **Leg B — disposable home, scheduler ran then died (detector fires).** On a
  throwaway ``HERMES_HOME``: ``hermes cron create`` three real 1-minute jobs,
  force a real execution of each (``hermes cron run`` + ``hermes cron tick`` ->
  genuine ``executions.db`` rows), then write **one** real heartbeat through
  Hermes's own ``cron.jobs.record_ticker_heartbeat`` — the scheduler's last
  breath — and stop. Nothing advances the heartbeat after that, so the ticker
  is dead. Reconciling with ``now`` past the stale window must emit **exactly
  one** ``reconcile.terminal_missing`` with ``subject_type='cron_ticker'`` for
  the whole installation (NOT one alert per job), and must not spray per-job
  ``cron.run_missed`` for the open-ended tails a dead ticker explains.

The recorder pass is strictly read-only against the cron store (asserted
byte-for-byte). Leg B writes only under the throwaway home; Leg A never writes
to the Hermes home at all.

Requirements:
- The ``hermes`` CLI on PATH (or ``$HERMES_CLI``) to drive Leg B's cron store.
- Hermes's own interpreter to write a real heartbeat: ``$HERMES_AGENT_HOME``
  (default ``~/.hermes/hermes-agent``) must contain ``venv/bin/python`` with the
  ``cron`` package importable. Leg B SKIPS with a note if either is missing.

Usage:  .venv/bin/python scripts/cron_death_live_check.py [--hermes-home PATH] [-v]
Exit:   0 if every non-skipped assertion passes, 1 otherwise.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

# Runnable standalone and spec-loadable: put the sibling _gate module on the path.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _gate import run_gate
from hermes_flight_recorder.collector import run_pass
from hermes_flight_recorder.collector._common import (
    executions_db_path,
    read_float,
    resolve_hermes_home,
    ticker_heartbeat_path,
)
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile

VERBOSE = "-v" in sys.argv[1:]

# A generous cushion past the stale window so Leg B never races wall-clock:
# `now` is derived from the frozen heartbeat, not from the real clock.
_DEAD_MARGIN = 600.0


def _hermes_home() -> Path:
    for i, a in enumerate(sys.argv):
        if a == "--hermes-home" and i + 1 < len(sys.argv):
            return Path(sys.argv[i + 1]).expanduser()
    return resolve_hermes_home(None)


def _log(msg: str) -> None:
    if VERBOSE:
        print(f"      {msg}")


def _new_outbox(tmp: Path) -> Outbox:
    ob = Outbox.open(tmp / "bridge")
    ob.initialize()
    return ob


def _ticker_findings(outbox) -> list[dict]:
    return [
        e
        for e in outbox.iter_events()
        if e["source"] == "reconciler"
        and e["payload"]["event_type"] == "reconcile.terminal_missing"
        and e["payload"].get("subject_type") == "cron_ticker"
    ]


def _hermes_cli() -> str | None:
    return os.environ.get("HERMES_CLI") or shutil.which("hermes")


def _hermes_python() -> Path | None:
    agent_home = Path(
        os.environ.get("HERMES_AGENT_HOME", str(Path.home() / ".hermes" / "hermes-agent"))
    ).expanduser()
    candidate = agent_home / "venv" / "bin" / "python"
    return candidate if candidate.exists() else None


# --- Leg A: real running home, no false positive --------------------------
def check_real_home_no_false_positive(home: Path, tmp: Path) -> list[str]:
    """A healthy live ticker must raise no stale-ticker finding, on real data."""
    fails: list[str] = []
    execs = executions_db_path(home)
    hb = read_float(ticker_heartbeat_path(home))
    if not execs.exists() or hb is None:
        _log("real-home: no live cron store / heartbeat on this host, skipped")
        return fails

    cfg = ReconcileConfig()
    age = time.time() - hb
    if age > cfg.ticker_stale_after:
        # The host's own ticker is genuinely stale right now; a "no false
        # positive" claim would be meaningless. Skip rather than mis-assert.
        _log(f"real-home: live heartbeat is {age:.0f}s old (> stale window), skipped")
        return fails

    ob = _new_outbox(tmp)
    try:
        run_pass(ob, home, on_source_error=lambda label, exc: None)
        reconcile(ob, home, now=time.time(), config=cfg)
        claimed = [
            e for e in ob.iter_events()
            if e["payload"]["event_type"] == "cron.run_claimed"
        ]
        heartbeat_events = [
            e for e in ob.iter_events()
            if e["payload"]["event_type"] == "cron.ticker_heartbeat"
        ]
        ticker = _ticker_findings(ob)
    finally:
        ob.close()

    if not heartbeat_events:
        _log("real-home: no heartbeat event captured, skipped")
        return fails
    if ticker:
        fails.append(
            f"real-home: {len(ticker)} stale-ticker false positive(s) against a "
            f"healthy live ticker (heartbeat age {age:.0f}s)"
        )
    _log(
        f"real-home: {len(claimed)} real cron execution(s) + live heartbeat "
        f"(age {age:.0f}s) -> {len(ticker)} ticker finding(s) (expected 0)"
    )
    return fails


# --- Leg B: disposable home, scheduler ran then died ----------------------
def _run(cmd: list[str], env: dict, label: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd, env=env, capture_output=True, text=True, timeout=120, check=False
    )


def check_dead_ticker_fires_once(home: Path, tmp: Path) -> list[str]:
    """Real jobs, real executions, one real heartbeat, then death -> one finding."""
    fails: list[str] = []
    cli = _hermes_cli()
    py = _hermes_python()
    if cli is None:
        _log("dead-ticker: `hermes` CLI not found (set $HERMES_CLI), skipped")
        return fails
    if py is None:
        _log("dead-ticker: Hermes venv python not found (set $HERMES_AGENT_HOME), skipped")
        return fails

    hh = tmp / "hermes"
    (hh / "scripts").mkdir(parents=True)
    (hh / "scripts" / "noop.sh").write_text("#!/usr/bin/env bash\necho cron-death-probe\n")
    (hh / "scripts" / "noop.sh").chmod(0o755)
    env = {**os.environ, "HERMES_HOME": str(hh), "HERMES_ACCEPT_HOOKS": "1"}

    # 1) Three real interval jobs sharing the one installation-wide ticker.
    #    "every 1m" is a recurring interval; a bare "1m" would be a one-shot
    #    `once` job, whose missed run is a discrete event, not the open-ended
    #    tail this check is about suppressing.
    for name in ("probe-a", "probe-b", "probe-c"):
        r = _run(
            [cli, "cron", "--accept-hooks", "create", "every 1m",
             "--name", name, "--no-agent", "--script", "noop.sh"],
            env, f"create {name}",
        )
        if r.returncode != 0:
            fails.append(f"dead-ticker: `cron create {name}` failed: {r.stderr.strip()[:200]}")
            return fails

    jobs = json.loads((hh / "cron" / "jobs.json").read_text())["jobs"]
    job_ids = [j["id"] for j in jobs]
    if len(job_ids) != 3:
        fails.append(f"dead-ticker: expected 3 jobs, jobs.json has {len(job_ids)}")
        return fails

    # 2) Force each job due now, then one tick fires them -> real executions.db rows.
    for jid in job_ids:
        _run([cli, "cron", "--accept-hooks", "run", jid], env, f"run {jid}")
    _run([cli, "cron", "--accept-hooks", "tick"], env, "tick")

    exec_db = executions_db_path(hh)
    if not exec_db.exists():
        fails.append("dead-ticker: no executions.db after cron tick")
        return fails
    import sqlite3

    conn = sqlite3.connect(f"file:{exec_db}?mode=ro", uri=True)
    try:
        n_exec = conn.execute("SELECT count(*) FROM executions").fetchone()[0]
    finally:
        conn.close()
    if n_exec == 0:
        fails.append("dead-ticker: cron tick produced zero real executions")
        return fails

    # 3) The scheduler's last breath: one real heartbeat via Hermes's own code.
    #    Then nothing advances it -> the ticker is dead.
    r = _run(
        [str(py), "-c", "from cron.jobs import record_ticker_heartbeat; "
         "record_ticker_heartbeat(success=True)"],
        env, "heartbeat",
    )
    if r.returncode != 0:
        fails.append(f"dead-ticker: real heartbeat write failed: {r.stderr.strip()[:200]}")
        return fails
    hb = read_float(ticker_heartbeat_path(hh))
    if hb is None:
        fails.append("dead-ticker: Hermes wrote no readable ticker_heartbeat")
        return fails

    # 4) Recorder pass, read-only, judged past the stale window.
    before = {p: p.read_bytes() for p in (exec_db, hh / "cron" / "jobs.json") if p.exists()}
    cfg = ReconcileConfig()
    now = hb + cfg.ticker_stale_after + _DEAD_MARGIN  # staleness > window -> dead

    ob = _new_outbox(tmp)
    try:
        run_pass(ob, hh, on_source_error=lambda label, exc: None)
        counts = reconcile(ob, hh, now=now, config=cfg)
        ticker = _ticker_findings(ob)
        missed = [
            e for e in ob.iter_events()
            if e["source"] == "reconciler"
            and e["payload"]["event_type"] == "cron.run_missed"
        ]
    finally:
        ob.close()

    for p, data in before.items():
        if p.read_bytes() != data:
            fails.append(f"dead-ticker: recorder mutated {p.name} (must be read-only)")

    if len(ticker) != 1:
        fails.append(
            f"dead-ticker: {len(ticker)} cron_ticker finding(s) across 3 jobs, want exactly 1"
        )
    else:
        if ticker[0]["correlation_id"] != "cron:ticker":
            fails.append(
                f"dead-ticker: finding correlation_id {ticker[0]['correlation_id']!r} != 'cron:ticker'"
            )
    if missed:
        fails.append(
            f"dead-ticker: {len(missed)} per-job cron.run_missed emitted; a dead ticker "
            f"must suppress open-ended tails, not spray per-job alerts"
        )
    _log(
        f"dead-ticker: {n_exec} real execution(s), heartbeat frozen at {hb:.0f}, "
        f"judged at +{cfg.ticker_stale_after + _DEAD_MARGIN:.0f}s -> "
        f"{len(ticker)} ticker finding (expected 1), {len(missed)} per-job miss (expected 0)"
    )
    _ = counts
    return fails


CHECKS = [
    ("real running home raises no stale-ticker false positive", check_real_home_no_false_positive),
    ("dead ticker on real cron store fires exactly one finding", check_dead_ticker_fires_once),
]


def main() -> int:
    home = _hermes_home()
    header = [
        "Cron-death live check — stale-ticker finding vs real Hermes cron",
        f"Hermes home: {home}",
    ]
    return run_gate(
        [header[0], header[1] if home.exists() else f"{header[1]} (absent; real-home leg skips)"],
        [
            ("real running home raises no stale-ticker false positive",
             lambda tmp: check_real_home_no_false_positive(home, tmp)),
            ("dead ticker on real cron store fires exactly one finding",
             lambda tmp: check_dead_ticker_fires_once(home, tmp)),
        ],
        passed="CHECK PASSED — stale-ticker detection holds against real Hermes cron",
        failed="CHECK FAILED",
        width=64,
        catch=True,
    )


if __name__ == "__main__":
    raise SystemExit(main())
