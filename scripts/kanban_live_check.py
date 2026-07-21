#!/usr/bin/env python3
"""Live Kanban check — the Phase 2 stale-lease detector vs real Hermes (issue #56).

Two legs, the same shape as ``scripts/cron_death_live_check.py``: one strictly
read-only pass over the live board, one that drives the real ``hermes kanban``
CLI in a disposable home to manufacture a genuine dead attempt.

- **Leg A (live home, READ-ONLY).** Poll the real ``kanban.db`` with
  ``kanban_db.poll`` into a throwaway outbox and reconcile at ``now``. A
  healthy/active board raises **zero** false ``reconcile.terminal_missing``
  findings against a live claim (an unexpired lease). If the live board has no
  open claims to prove against, the positive assertion is skipped — but the live
  ``kanban.db`` (and every board db) is **always** asserted byte-for-byte
  unchanged.
- **Leg B (disposable HERMES_HOME).** Drive the real ``hermes kanban`` CLI to
  create a task and claim it — a genuine ``task_runs`` row with a real
  ``claim_lock`` / ``claim_expires`` — then reconcile with ``now`` past the lease
  window using a small ``ReconcileConfig``. Assert **exactly one**
  ``reconcile.terminal_missing`` with ``subject_type='task_run'`` for the
  installation. Skipped cleanly when the CLI, its verbs, or its venv are
  unavailable.

Usage:  python scripts/kanban_live_check.py [--hermes-home PATH] [-v]
Exit:   0 if every non-skipped assertion passes, 1 otherwise.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from functools import partial
from pathlib import Path

# Runnable standalone and spec-loadable: put the repo root first (so a shared
# venv / git worktree imports the co-located package) then the sibling _gate.
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _gate import run_gate
from hermes_flight_recorder.collector import kanban_db
from hermes_flight_recorder.collector._common import (
    kanban_board_dbs,
    open_sqlite_read_only,
    resolve_hermes_home,
)
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile

VERBOSE = "-v" in sys.argv[1:]

# A small window for Leg B so the manufactured dead attempt trips promptly once
# ``now`` is pushed just past the lease.
LEG_B_CFG = ReconcileConfig(task_lease_grace=60.0, task_heartbeat_stale_after=60.0)


def _hermes_home() -> Path:
    for i, a in enumerate(sys.argv):
        if a == "--hermes-home" and i + 1 < len(sys.argv):
            return Path(sys.argv[i + 1]).expanduser()
    return resolve_hermes_home(None)


def _log(msg: str) -> None:
    if VERBOSE:
        print(f"      {msg}")


def _note(msg: str) -> None:
    """A skip/status note, always shown so a SKIP is never silent."""
    print(f"      {msg}")


def _terminal_missing_runs(ob: Outbox) -> list[dict]:
    """Every ``reconcile.terminal_missing`` payload for a ``task_run``."""
    return [
        e["payload"]
        for e in ob.iter_events()
        if e["payload"]["event_type"] == "reconcile.terminal_missing"
        and e["payload"].get("subject_type") == "task_run"
    ]


def _open_runs(home: Path) -> list[dict]:
    """Every still-open attempt (``outcome`` NULL) across all boards, with its
    lease fields — read-only, the same rows the detector judges."""
    runs: list[dict] = []
    for board, db_path in kanban_board_dbs(home):
        conn = open_sqlite_read_only(db_path)
        try:
            rows = conn.execute(
                "SELECT id, claim_lock, claim_expires, last_heartbeat_at "
                "FROM task_runs WHERE outcome IS NULL"
            ).fetchall()
        finally:
            conn.close()
        for r in rows:
            runs.append(
                {
                    "board": board,
                    "id": r["id"],
                    "claim_lock": r["claim_lock"],
                    "claim_expires": r["claim_expires"],
                    "last_heartbeat_at": r["last_heartbeat_at"],
                }
            )
    return runs


def _would_fire(run: dict, when: float, cfg: ReconcileConfig) -> bool:
    """Whether the stale-lease detector would flag this open run at ``when`` —
    mirrors ``_detect_stale_task_leases`` so a *healthy* run is told from a
    genuinely dead one without re-running the reconciler."""
    expires = run["claim_expires"]
    if expires is None or when - expires <= cfg.task_lease_grace:
        return False
    hb = run["last_heartbeat_at"]
    if hb is not None and when - hb <= cfg.task_heartbeat_stale_after:
        return False
    return True


# --- Leg A: live, read-only ----------------------------------------------
def leg_a_live_readonly(home: Path, tmp: Path) -> list[str]:
    """Poll + reconcile the live board; prove no false positive and no write."""
    fails: list[str] = []
    boards = kanban_board_dbs(home)
    if not boards:
        _note("Leg A: no Kanban board on the live home — nothing to poll (read-only trivially holds)")
        return fails

    # Byte-for-byte snapshot of every board db before we touch it.
    before = {db_path: db_path.read_bytes() for _, db_path in boards}
    when = time.time()
    cfg = ReconcileConfig()
    open_runs = _open_runs(home)
    healthy = {(r["board"], r["id"]) for r in open_runs if not _would_fire(r, when, cfg)}

    ob = Outbox.open(tmp / "bridge")
    ob.initialize()
    try:
        poll_counts = kanban_db.poll(ob, home)
        reconcile(ob, home, now=when, config=cfg)
        findings = _terminal_missing_runs(ob)
    finally:
        ob.close()

    # Read-only: the board dbs must be identical after poll + reconcile.
    for db_path, data in before.items():
        if db_path.read_bytes() != data:
            fails.append(f"read-only: {db_path} changed during poll/reconcile")
    _log(f"read-only: {len(before)} board db(s) unchanged, poll captured {dict(poll_counts)}")

    # No false positive: no finding may target a run whose lease is still live.
    false_hits = [
        p for p in findings if (p.get("board"), p.get("run_id")) in healthy
    ]
    if false_hits:
        fails.append(
            f"false-positive: {len(false_hits)} terminal_missing against a live claim "
            f"(e.g. run {false_hits[0].get('run_id')} on board {false_hits[0].get('board')})"
        )
    if not healthy:
        _note(
            f"Leg A: {len(open_runs)} open claim(s), none with a live lease to prove against "
            "— positive assertion skipped (read-only still asserted)"
        )
    else:
        _log(f"no-false-positive: {len(healthy)} live claim(s), {len(findings)} task_run finding(s), 0 false")
    return fails


# --- Leg B: disposable home, real CLI ------------------------------------
def _resolve_cli() -> str | None:
    """The real ``hermes`` CLI path, or None when unavailable."""
    cli = os.environ.get("HERMES_CLI") or "/home/jose/.local/bin/hermes"
    path = Path(cli).expanduser()
    if path.exists() and os.access(path, os.X_OK):
        return str(path)
    return None


def _run_cli(cli: str, env: dict, *args: str, timeout: float = 60.0) -> subprocess.CompletedProcess:
    return subprocess.run(
        [cli, "kanban", *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def leg_b_real_cli(home: Path, tmp: Path) -> list[str]:
    """Manufacture a real dead attempt via the CLI, then detect it. Skips clean."""
    fails: list[str] = []
    cli = _resolve_cli()
    if cli is None:
        _note("Leg B: hermes CLI not found/executable ($HERMES_CLI or ~/.local/bin/hermes) — skipped")
        return fails

    disposable = tmp / "home"
    disposable.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env["HERMES_HOME"] = str(disposable)
    # Pin the board db to the legacy top-level path so our poller finds it as
    # board "default", independent of the CLI's active-board state.
    env["HERMES_KANBAN_DB"] = str(disposable / "kanban.db")

    # Confirm the CLI exposes the verbs we need before committing to it.
    try:
        helped = _run_cli(cli, env, "--help")
    except (OSError, subprocess.SubprocessError) as exc:
        _note(f"Leg B: could not run 'hermes kanban --help' ({type(exc).__name__}) — skipped")
        return fails
    if helped.returncode != 0 or not {"create", "claim"} <= set(helped.stdout.split()):
        _note("Leg B: 'hermes kanban' lacks the create/claim verbs — skipped")
        return fails

    try:
        _run_cli(cli, env, "init")  # idempotent; ignore its advisory output
        created = _run_cli(cli, env, "create", "flight-recorder livecheck probe",
                           "--assignee", "probe", "--json")
    except (OSError, subprocess.SubprocessError) as exc:
        _note(f"Leg B: CLI create failed to run ({type(exc).__name__}) — skipped")
        return fails
    if created.returncode != 0:
        _note(f"Leg B: 'hermes kanban create' exited {created.returncode} — skipped")
        return fails
    task_id = _parse_task_id(created.stdout)
    if task_id is None:
        _note("Leg B: could not parse a task id from create --json — skipped")
        return fails

    claimed = _run_cli(cli, env, "claim", task_id, "--ttl", "900")
    if claimed.returncode != 0:
        _note(f"Leg B: 'hermes kanban claim' exited {claimed.returncode} — skipped")
        return fails

    # Read the genuine open run the CLI just wrote (read-only).
    open_runs = _open_runs(disposable)
    if not open_runs:
        _note("Leg B: claim produced no open task_runs row — skipped")
        return fails
    expires = open_runs[0]["claim_expires"]
    if expires is None:
        _note("Leg B: claimed run carries no claim_expires — skipped")
        return fails

    # Reconcile with now pushed past the lease window: the worker never
    # heartbeats or ends, so the reconciler must call the attempt dead.
    when = float(expires) + LEG_B_CFG.task_lease_grace + 60.0
    ob = Outbox.open(tmp / "bridge")
    ob.initialize()
    try:
        kanban_db.poll(ob, disposable)
        counts = reconcile(ob, disposable, now=when, config=LEG_B_CFG)
        findings = _terminal_missing_runs(ob)
    finally:
        ob.close()

    if counts.get("reconcile.terminal_missing") != 1 or len(findings) != 1:
        fails.append(
            f"Leg B: expected exactly one task_run terminal_missing, got {len(findings)} "
            f"(counts {dict(counts)})"
        )
    else:
        p = findings[0]
        if p.get("expected_terminal_event_type") != "task.attempt_ended":
            fails.append("Leg B: finding expected_terminal_event_type is not task.attempt_ended")
        _log(
            f"Leg B: created {task_id}, claimed a real run on board {p.get('board')}, "
            f"one terminal_missing for run {p.get('run_id')}"
        )
    return fails


def _parse_task_id(stdout: str) -> str | None:
    """The task id from ``create --json`` output (tolerates leading advisory text)."""
    start = stdout.find("{")
    if start < 0:
        return None
    try:
        obj = json.loads(stdout[start:])
    except ValueError:
        return None
    task_id = obj.get("id")
    return task_id if isinstance(task_id, str) else None


LEGS = [
    ("Leg A — live board, read-only, no false positive", leg_a_live_readonly),
    ("Leg B — real CLI claim reconciled to a dead attempt", leg_b_real_cli),
]


def main() -> int:
    home = _hermes_home()
    if not home.exists():
        print(f"FAIL — Hermes home not found at {home}")
        return 1
    return run_gate(
        [
            "Live Kanban check — Phase 2 stale-lease detector vs a real Hermes home",
            f"Hermes home: {home}",
        ],
        [(name, partial(fn, home)) for name, fn in LEGS],
        passed="CHECK PASSED — the stale-lease detector holds against the live board and a real dead claim",
        failed="CHECK FAILED",
        width=64,
        catch=True,
    )


if __name__ == "__main__":
    raise SystemExit(main())
