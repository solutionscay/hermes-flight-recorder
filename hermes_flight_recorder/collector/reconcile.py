"""Reconciler — gap, missing-terminal, and missed-cron detection.

The reconciler is the exit criterion of Phase 0. It turns "we hope we
captured everything" into "we can prove what we lost". It reads the outbox
(the captured stream) and the durable stores (``state.db``, cron
``executions.db``, ``jobs.json``, the ticker heartbeat) read-only, diffs
them, and emits each finding as a first-class ``reconcile.*`` / ``cron.*``
envelope record back into the outbox.

Four detectors, each grounded in the real probe (see issue #6):

- **Sequence-gap.** Scan the outbox by (``installation_id``,
  ``producer_sequence``). A missing integer means the append path lost a
  capture. Emit ``reconcile.gap_detected`` with ``gap_kind='sequence'``.
- **Coverage-gap.** A durable row (session, tool message, model-usage,
  execution) with no matching captured event means the live stream dropped
  it. Emit ``reconcile.gap_detected`` with ``gap_kind='uncaptured_row'``.
- **Missing-terminal.** A start-node past its lifetime with no terminal.
  Sessions, cron runs, and open Kanban task attempts are judged from the
  authoritative durable row (``ended_at`` / ``finished_at`` / a lapsed
  ``claim_expires`` on a run whose ``outcome`` is still NULL); invocations are
  judged from the outbox (``invocation.started`` with no
  ``invocation.completed``), because the ``turn_id`` lives only in memory.
  Emit ``reconcile.terminal_missing``.
- **Missed-cron.** Reconstruct the expected fire instants from the
  ``jobs.json`` schedule (interval, once, or cron expression) and diff them
  against ``executions.db`` ``claimed_at``. Emit ``cron.run_missed`` with
  ``expected_fire_at``. A contiguous run of misses collapses to one row. A
  paused or repeat-exhausted job is suppressed. A stale ticker heartbeat is
  a single installation-wide signal, not one finding per job.

Every finding carries a deterministic ``dedup_key``, so a second run over
the same durable state appends nothing new (idempotent). The reconciler
never writes to any Hermes store.
"""

from __future__ import annotations

import datetime
import re
import time
from bisect import bisect_left
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..envelope import SESSION_LIFECYCLE, SESSION_START_TYPES
from ._common import (
    append_and_count,
    build_record,
    executions_db_path,
    gateway_starts_log_path,
    gateway_state_path,
    jobs_path,
    kanban_board_dbs,
    load_json_dict,
    open_sqlite_read_only,
    read_float,
    resolve_hermes_home,
    root_session,
    runtime_stamp,
    state_db_path,
    ticker_heartbeat_path,
    to_epoch,
)
from .cron_schedule import expected_instants
from .recorder_config import CaptureConfig

_SOURCE = "reconciler"
_CAPTURE = "derive:reconciler"
_PID_RE = re.compile(r"PID (\d+)")


@dataclass(frozen=True)
class ReconcileConfig:
    """Thresholds for the reconciler. Defaults suit a real install; tests
    pass small windows and a fixed ``now``.
    """

    # A start-node older than this with no terminal is judged missing.
    session_terminal_timeout: float = 12 * 3600.0
    subagent_terminal_timeout: float = 30 * 60.0
    invocation_terminal_timeout: float = 60 * 60.0
    cron_run_terminal_timeout: float = 15 * 60.0
    # An open task claim whose lease lapsed by more than this grace, with a
    # heartbeat older than the staleness window, is judged a dead attempt. The
    # grace lets Hermes's own reclaim run first (its defer grace is ~120 s); the
    # staleness window is the Hermes default claim TTL (a live worker renews the
    # lease within it, so a stale heartbeat means the worker is gone).
    task_lease_grace: float = 120.0
    task_heartbeat_stale_after: float = 15 * 60.0
    # A heartbeat older than this means the whole scheduler is dead.
    ticker_stale_after: float = 300.0
    # How far an execution may sit from an expected fire and still count as
    # that fire. Absorbs ticker jitter (a "1m" job fired ~78s apart).
    cron_match_slack: float = 45.0
    once_match_slack: float = 300.0
    # Bound the cron-expression lookback so instant expansion stays cheap.
    cron_lookback: float = 24 * 3600.0


def reconcile(
    outbox: Any,
    hermes_home: str | Path | None = None,
    *,
    now: float | None = None,
    config: ReconcileConfig | None = None,
    capture_config: CaptureConfig | None = None,
) -> dict[str, int]:
    """One reconcile pass. Returns per-event-type counts of new findings."""
    cfg = config or ReconcileConfig()
    capture = capture_config or CaptureConfig()
    when = float(now) if now is not None else time.time()
    home = resolve_hermes_home(hermes_home)
    installation_id = outbox.installation_id

    # Snapshot the retained stream and compact retention summaries once,
    # before any emission, so findings appended this pass never perturb
    # detection. Summaries keep intentionally pruned sequences and durable
    # subjects from looking like capture loss without restoring event bodies.
    events = list(outbox.iter_events(installation_id))
    events.extend(outbox.iter_pruned_summaries(installation_id))
    # Snapshot the cron executions once too; three detectors read them.
    exec_rows = _load_execution_rows(home)
    counts: dict[str, int] = defaultdict(int)

    _detect_sequence_gaps(outbox, events, installation_id, counts, when)
    session_rows, parent_map = _detect_coverage_gaps(
        outbox, events, home, exec_rows, counts, when, capture
    )
    _detect_missing_terminals(
        outbox, events, exec_rows, counts, when, cfg, session_rows, parent_map
    )
    _detect_missed_cron(outbox, home, exec_rows, counts, when, cfg)
    _detect_gateway_start_failed(outbox, home, counts, when)
    _detect_stale_task_leases(outbox, home, counts, when, cfg)
    return dict(counts)


# --- sequence gaps ------------------------------------------------------
def _detect_sequence_gaps(outbox, events, installation_id, counts, when) -> None:
    seqs = sorted(e["producer_sequence"] for e in events)
    for prev_seq, next_seq in zip(seqs, seqs[1:]):
        for missing in range(prev_seq + 1, next_seq):
            _emit(
                outbox,
                counts,
                event_type="reconcile.gap_detected",
                occurred_at=when,
                correlation_id=installation_id,
                partial=False,  # a lost sequence is a fact, not an inference
                payload={
                    "gap_kind": "sequence",
                    "missing_sequence": missing,
                    "prev_sequence": prev_seq,
                    "next_sequence": next_seq,
                },
                dedup_key=f"reconcile:seq:{installation_id}:{missing}",
            )


# --- coverage gaps ------------------------------------------------------
def _detect_coverage_gaps(
    outbox, events, home, exec_rows, counts, when, capture_config
):
    """A durable row with no captured event proves a dropped capture."""
    captured = _captured_subjects(events)
    session_rows = []
    parent_map = {}

    state_path = state_db_path(home)
    if state_path.exists():
        conn = open_sqlite_read_only(state_path)
        try:
            session_rows = conn.execute(
                "SELECT id, source, parent_session_id, started_at, ended_at, "
                "profile_name FROM sessions"
            ).fetchall()
            parent_map = {r["id"]: r["parent_session_id"] for r in session_rows}
            _coverage_sessions(
                outbox, session_rows, parent_map, captured, counts, when
            )
            _coverage_messages(
                outbox,
                conn,
                parent_map,
                captured,
                counts,
                when,
                capture_config,
            )
            _coverage_model_usage(outbox, conn, parent_map, captured, counts, when)
        finally:
            conn.close()

    for r in exec_rows:
        if r["id"] in captured["executions"]:
            continue
        _emit_coverage(
            outbox, counts, when,
            subject_type="execution", subject_id=r["id"],
            source_table="cron:executions.db", correlation_id=r["job_id"],
        )
    _coverage_kanban(outbox, home, captured, counts, when)
    return session_rows, parent_map


def _coverage_sessions(outbox, rows, parent_map, captured, counts, when) -> None:
    for r in rows:
        sid = r["id"]
        if sid in captured["sessions"]:
            continue
        corr = root_session(sid, parent_map) or sid
        _emit_coverage(
            outbox, counts, when,
            subject_type="session", subject_id=sid,
            source_table="state.db:sessions", correlation_id=corr,
            session_id=sid, parent_session_id=r["parent_session_id"],
        )


def _coverage_messages(
    outbox, conn, parent_map, captured, counts, when, capture_config
) -> None:
    roles = tuple(
        role
        for role in ("user", "assistant", "tool")
        if role in capture_config.message_roles
    )
    if not roles:
        return
    columns = {row[1] for row in conn.execute("PRAGMA table_info(messages)")}
    # Some narrow synthetic/legacy schemas do not expose content. In that
    # case only tool rows can be proven capture-worthy; user/assistant rows
    # need content to distinguish real text from empty tool-call scaffolding.
    if "content" not in columns:
        roles = tuple(role for role in roles if role == "tool")
        if not roles:
            return
    placeholders = ",".join("?" for _ in roles)
    content_predicate = (
        " AND (role='tool' OR (content IS NOT NULL AND length(content) > 0))"
        if "content" in columns
        else ""
    )
    rows = conn.execute(
        "SELECT id, session_id FROM messages "
        f"WHERE role IN ({placeholders}){content_predicate}",
        roles,
    ).fetchall()
    for r in rows:
        if r["id"] in captured["messages"]:
            continue
        sid = r["session_id"]
        corr = root_session(sid, parent_map) or sid
        _emit_coverage(
            outbox, counts, when,
            subject_type="message", subject_id=str(r["id"]),
            source_table="state.db:messages", correlation_id=corr, session_id=sid,
        )


def _coverage_model_usage(outbox, conn, parent_map, captured, counts, when) -> None:
    rows = conn.execute(
        "SELECT session_id, model, task FROM session_model_usage"
    ).fetchall()
    for r in rows:
        key = (r["session_id"], r["model"], r["task"])
        if key in captured["model_usage"]:
            continue
        sid = r["session_id"]
        corr = root_session(sid, parent_map) or sid
        _emit_coverage(
            outbox, counts, when,
            subject_type="model_usage", subject_id=f"{sid}:{r['model']}:{r['task']}",
            source_table="state.db:session_model_usage", correlation_id=corr, session_id=sid,
        )


def _coverage_kanban(outbox, home, captured, counts, when) -> None:
    """A durable Kanban task/run with no captured ``task.*`` event.

    The Kanban analog of the session/execution coverage diff: every board's
    ``tasks`` and ``task_runs`` rows are authoritative, so a row the live poll
    never turned into a captured event is a dropped capture. The subject_id is
    board-scoped (``board:id``) so equal ids across boards never collide and the
    shared ``reconcile:cover:*`` dedup key stays unique per board.
    """
    for board, db_path in kanban_board_dbs(home):
        conn = open_sqlite_read_only(db_path)
        try:
            present = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            tasks = (
                conn.execute("SELECT id, session_id FROM tasks").fetchall()
                if "tasks" in present else []
            )
            runs = (
                conn.execute("SELECT id, task_id FROM task_runs").fetchall()
                if "task_runs" in present else []
            )
        finally:
            conn.close()
        for r in tasks:
            if (board, r["id"]) in captured["tasks"]:
                continue
            _emit_coverage(
                outbox, counts, when,
                subject_type="task", subject_id=f"{board}:{r['id']}",
                source_table=f"kanban:{board}:tasks", correlation_id=r["id"],
                session_id=r["session_id"],
            )
        for r in runs:
            if (board, r["id"]) in captured["task_runs"]:
                continue
            _emit_coverage(
                outbox, counts, when,
                subject_type="task_run", subject_id=f"{board}:{r['id']}",
                source_table=f"kanban:{board}:task_runs", correlation_id=r["task_id"],
            )


def _captured_subjects(events) -> dict[str, set]:
    """Index the captured stream by the durable subject each event covers."""
    sessions: set[str] = set()
    messages: set[int] = set()
    model_usage: set[tuple] = set()
    executions: set[str] = set()
    tasks: set[tuple] = set()
    task_runs: set[tuple] = set()
    for e in events:
        pl = e.get("payload", {})
        et = pl.get("event_type")
        if et in SESSION_START_TYPES:
            if e.get("session_id") is not None:
                sessions.add(e["session_id"])
        mid = pl.get("message_row_id")
        if mid is not None:
            messages.add(mid)
        if et == "model.usage_recorded":
            model_usage.add((e.get("session_id"), pl.get("model"), pl.get("task")))
        elif et == "cron.run_claimed":
            exid = pl.get("execution_id")
            if exid is not None:
                executions.add(exid)
        elif isinstance(et, str) and et.startswith("task."):
            # Every task.* event carries board + task_id; task.claimed and
            # task.attempt_ended additionally carry the owning run_id.
            board = pl.get("board")
            task_id = pl.get("task_id")
            if board is not None and task_id is not None:
                tasks.add((board, task_id))
            run_id = pl.get("run_id")
            if board is not None and run_id is not None:
                task_runs.add((board, run_id))
    return {
        "sessions": sessions,
        "messages": messages,
        "model_usage": model_usage,
        "executions": executions,
        "tasks": tasks,
        "task_runs": task_runs,
    }


def _emit_coverage(
    outbox, counts, when, *, subject_type, subject_id, source_table, correlation_id,
    session_id=None, parent_session_id=None,
) -> None:
    _emit(
        outbox, counts,
        event_type="reconcile.gap_detected",
        occurred_at=when,
        correlation_id=correlation_id,
        session_id=session_id,
        parent_session_id=parent_session_id,
        partial=True,  # inferred: the poll saw a row the live stream missed
        payload={
            "gap_kind": "uncaptured_row",
            "subject_type": subject_type,
            "subject_id": subject_id,
            "source_table": source_table,
        },
        dedup_key=f"reconcile:cover:{subject_type}:{subject_id}",
    )


# --- missing terminals --------------------------------------------------
def _detect_missing_terminals(
    outbox, events, exec_rows, counts, when, cfg, session_rows, parent_map
) -> None:
    _terminals_sessions(outbox, session_rows, parent_map, counts, when, cfg)
    _terminals_cron_runs(outbox, exec_rows, counts, when, cfg)
    _terminals_invocations(outbox, events, counts, when, cfg)


def _terminals_sessions(outbox, rows, parent_map, counts, when, cfg) -> None:
    """A durable session/subagent row with ended_at NULL past its window.

    The durable row is authoritative: a live session keeps ended_at=NULL and
    is not a crash, so judge it only after the lifetime window.
    """
    for r in rows:
        if r["ended_at"] is not None:
            continue
        started = to_epoch(r["started_at"])
        if started is None:
            continue
        is_sub = r["source"] == "subagent"
        timeout = cfg.subagent_terminal_timeout if is_sub else cfg.session_terminal_timeout
        age = when - started
        if age <= timeout:
            continue  # still within its lifetime — provisional, not missing
        subject_type = "subagent" if is_sub else "session"
        start_type, expected = SESSION_LIFECYCLE[subject_type]
        sid = r["id"]
        corr = root_session(sid, parent_map) or sid
        _emit_terminal_missing(
            outbox,
            counts,
            occurred_at=when,
            correlation_id=corr,
            subject_type=subject_type,
            subject_id=sid,
            start_event_type=start_type,
            expected_terminal_event_type=expected,
            session_id=sid,
            parent_session_id=r["parent_session_id"],
            profile=r["profile_name"] or "default",
            details={
                "start_occurred_at": started,
                "age_seconds": age,
            },
            dedup_key=f"reconcile:terminal:{subject_type}:{sid}",
        )


def _terminals_cron_runs(outbox, exec_rows, counts, when, cfg) -> None:
    """A durable execution with finished_at NULL past its window."""
    for r in exec_rows:
        if r["finished_at"] is not None:
            continue
        claimed = r["claimed_epoch"]
        if claimed is None:
            continue
        age = when - claimed
        if age <= cfg.cron_run_terminal_timeout:
            continue
        exid = r["id"]
        _emit_terminal_missing(
            outbox,
            counts,
            occurred_at=when,
            correlation_id=r["job_id"],
            subject_type="cron_run",
            subject_id=exid,
            start_event_type="cron.run_claimed",
            expected_terminal_event_type="cron.run_finished",
            details={
                "job_id": r["job_id"],
                "status": r["status"],
                "start_occurred_at": claimed,
                "age_seconds": age,
            },
            dedup_key=f"reconcile:terminal:cron_run:{exid}",
        )


def _terminals_invocations(outbox, events, counts, when, cfg) -> None:
    """An invocation.started with no invocation.completed, past its window.

    The turn_id lives only in memory, so invocations are judged from the
    captured (hook) stream, not a durable table.
    """
    completed: set[str] = set()
    started: dict[str, dict[str, Any]] = {}
    for e in events:
        pl = e.get("payload", {})
        inv = e.get("invocation_id")
        if inv is None:
            continue
        et = pl.get("event_type")
        if et == "invocation.completed":
            completed.add(inv)
        elif et == "invocation.started":
            started.setdefault(inv, e)

    for inv, e in started.items():
        if inv in completed:
            continue
        occurred = e.get("occurred_at")
        if occurred is None or when - occurred <= cfg.invocation_terminal_timeout:
            continue
        _emit_terminal_missing(
            outbox,
            counts,
            occurred_at=when,
            correlation_id=e.get("correlation_id") or inv,
            subject_type="invocation",
            subject_id=inv,
            start_event_type="invocation.started",
            expected_terminal_event_type="invocation.completed",
            session_id=e.get("session_id"),
            parent_session_id=e.get("parent_session_id"),
            invocation_id=inv,
            profile=e.get("profile") or "default",
            details={
                "start_occurred_at": occurred,
                "age_seconds": when - occurred,
            },
            dedup_key=f"reconcile:terminal:invocation:{inv}",
        )


def _emit_terminal_missing(
    outbox,
    counts,
    *,
    occurred_at,
    correlation_id,
    subject_type,
    subject_id,
    start_event_type,
    expected_terminal_event_type,
    dedup_key,
    details=None,
    session_id=None,
    parent_session_id=None,
    invocation_id=None,
    profile="default",
) -> None:
    payload = {
        "subject_type": subject_type,
        "subject_id": subject_id,
        "start_event_type": start_event_type,
        "expected_terminal_event_type": expected_terminal_event_type,
    }
    payload.update(details or {})
    _emit(
        outbox,
        counts,
        event_type="reconcile.terminal_missing",
        occurred_at=occurred_at,
        correlation_id=correlation_id,
        session_id=session_id,
        parent_session_id=parent_session_id,
        invocation_id=invocation_id,
        profile=profile,
        partial=True,
        payload=payload,
        dedup_key=dedup_key,
    )


# --- stale task leases --------------------------------------------------
def _detect_stale_task_leases(outbox, home, counts, when, cfg) -> None:
    """An open Kanban claim whose lease lapsed with a dead heartbeat.

    The Kanban analog of the stale-ticker signal. A ``task_runs`` row still open
    (``outcome`` NULL) whose ``claim_expires`` passed — past a grace, with a
    heartbeat stale beyond the window — is a worker that died mid-attempt: no
    terminal is coming until Hermes reclaims it. The durable row is
    authoritative and current (a live worker renews ``claim_expires`` by
    heartbeat, so a lapsed lease *is* the death signal), exactly as a cron
    execution is judged from its own ``finished_at``.
    """
    for run in _load_open_task_runs(home):
        if not _lease_is_dead(run, when, cfg):
            continue
        board, run_id = run["board"], run["id"]
        _emit_terminal_missing(
            outbox,
            counts,
            occurred_at=when,
            correlation_id=run["task_id"],
            subject_type="task_run",
            subject_id=str(run_id),
            start_event_type="task.claimed",
            expected_terminal_event_type="task.attempt_ended",
            details={
                "board": board,
                "task_id": run["task_id"],
                "run_id": run_id,
                "holder": run["claim_lock"],
                "claim_expires": run["claim_expires"],
                "last_heartbeat_at": run["last_heartbeat_at"],
                "start_occurred_at": run["started_at"],
                "age_seconds": when - run["claim_expires"],
            },
            dedup_key=f"reconcile:terminal:task_run:{board}:{run_id}",
        )


def _lease_is_dead(run: dict[str, Any], when: float, cfg: ReconcileConfig) -> bool:
    """Whether an open attempt's lease has lapsed with a dead heartbeat.

    The shared stale-lease predicate: the ``claim_expires`` lapsed past the
    grace, and the heartbeat is stale beyond the window (or absent). A live
    worker renews ``claim_expires`` by heartbeat, so both failing means the
    worker is gone. Public to the live-check gate so it validates this exact
    boundary rather than a copy of it.
    """
    expires = run["claim_expires"]
    if expires is None or when - expires <= cfg.task_lease_grace:
        return False  # no lease, or still within its (possibly renewed) lease
    hb = run["last_heartbeat_at"]
    if hb is not None and when - hb <= cfg.task_heartbeat_stale_after:
        return False  # a fresh heartbeat — the worker is alive, Hermes will renew
    return True


def _load_open_task_runs(home: Path) -> list[dict[str, Any]]:
    """Every still-open attempt (``outcome`` NULL) across all boards."""
    runs: list[dict[str, Any]] = []
    for board, db_path in kanban_board_dbs(home):
        conn = open_sqlite_read_only(db_path)
        try:
            rows = conn.execute(
                "SELECT id, task_id, claim_lock, claim_expires, worker_pid, "
                "last_heartbeat_at, started_at FROM task_runs WHERE outcome IS NULL"
            ).fetchall()
        finally:
            conn.close()
        for r in rows:
            runs.append(
                {
                    "board": board,
                    "id": r["id"],
                    "task_id": r["task_id"],
                    "claim_lock": r["claim_lock"],
                    "claim_expires": r["claim_expires"],
                    "worker_pid": r["worker_pid"],
                    "last_heartbeat_at": r["last_heartbeat_at"],
                    "started_at": r["started_at"],
                }
            )
    return runs


# --- missed cron --------------------------------------------------------
def _detect_missed_cron(outbox, home, exec_rows, counts, when, cfg) -> None:
    jobs = _load_jobs(jobs_path(home))
    if not jobs:
        return

    # A stale heartbeat means the whole scheduler is dead: one installation
    # signal, and suppress the per-job trailing catch-up it would explain.
    ticker_dead = _ticker_is_stale(outbox, home, counts, when, cfg)

    exec_by_job: dict[str, list[float]] = defaultdict(list)
    for r in exec_rows:
        if r["claimed_epoch"] is not None:
            exec_by_job[r["job_id"]].append(r["claimed_epoch"])
    for job in jobs:
        _missed_for_job(outbox, job, exec_by_job, counts, when, cfg, ticker_dead)


def _missed_for_job(outbox, job, exec_by_job, counts, when, cfg, ticker_dead) -> None:
    if not _job_is_active(job):
        return  # paused, disabled, or repeat-exhausted — no fire is expected
    sched = job.get("schedule") or {}
    kind = sched.get("kind")
    job_id = job.get("id")
    execs = sorted(exec_by_job.get(job_id, []))
    created = to_epoch(job.get("created_at"))

    if kind == "interval":
        minutes = sched.get("minutes")
        if not minutes or minutes <= 0:
            return
        runs = _interval_missed(execs, created, minutes * 60.0, when, cfg.cron_match_slack)
    elif kind == "once":
        run_at = to_epoch(sched.get("run_at") or job.get("next_run_at"))
        runs = _once_missed(execs, run_at, when, cfg.once_match_slack)
    elif kind == "cron":
        expr = sched.get("expression") or sched.get("cron") or sched.get("expr")
        # Match wall-clock fields in the job's own UTC offset (carried by its
        # ISO timestamps), so the diff is identical on any host timezone.
        job_tz = _tz_of(job.get("created_at")) or _tz_of(job.get("next_run_at"))
        runs = _cron_missed(expr, execs, created, when, cfg, tz=job_tz)
    else:
        return

    for first_at, count, is_tail in runs:
        # A dead scheduler explains the open-ended tail; don't double-report
        # it per job — the single ticker signal already covers it.
        if is_tail and ticker_dead:
            continue
        _emit(
            outbox, counts,
            event_type="cron.run_missed",
            occurred_at=first_at,
            correlation_id=job_id,
            partial=True,
            payload={
                "job_id": job_id,
                "expected_fire_at": first_at,
                "missed_count": count,
                "schedule_kind": kind,
                "catch_up": count > 1,
            },
            dedup_key=f"reconcile:missed:{job_id}:{int(first_at)}",
        )


def _interval_missed(execs, created, step, now, slack):
    """Contiguous runs of missed interval fires as (first_at, count, is_tail).

    Anchored on the first real fire (or created_at when none fired), so the
    startup gap before the ticker began does not read as a miss. Re-anchors
    on each real fire, so ticker jitter never accumulates into a false gap.
    """
    runs: list[tuple[float, int, bool]] = []
    if not execs:
        if created is None:
            return runs
        first = created + step
        if now < first - slack:
            return runs  # not due yet
        count = max(1, int((now - created) // step))
        return [(first, count, True)]

    i = 0
    n = len(execs)
    expected = execs[0] + step
    run_first: float | None = None
    run_count = 0
    while expected <= now + slack:
        while i < n and execs[i] < expected - slack:
            i += 1
        if i < n and execs[i] <= expected + slack:
            if run_count:
                runs.append((run_first, run_count, False))
                run_first, run_count = None, 0
            expected = execs[i] + step
            i += 1
        else:
            if run_count == 0:
                run_first = expected
            run_count += 1
            expected += step
    if run_count:
        runs.append((run_first, run_count, True))  # open-ended tail to now
    return runs


def _once_missed(execs, run_at, now, slack):
    if run_at is None or now < run_at + slack:
        return []
    if _near(run_at, execs, slack):
        return []
    return [(run_at, 1, False)]


def _cron_missed(expr, execs, created, now, cfg, tz=None):
    anchor = execs[0] if execs else created
    if anchor is None:
        return []
    lower = max(anchor, now - cfg.cron_lookback)
    expected = expected_instants(expr, lower, now, tz)
    slack = cfg.cron_match_slack
    runs: list[tuple[float, int, bool]] = []
    run_first: float | None = None
    run_count = 0
    for inst in expected:
        if _near(inst, execs, slack):
            if run_count:
                runs.append((run_first, run_count, False))
                run_first, run_count = None, 0
        else:
            if run_count == 0:
                run_first = inst
            run_count += 1
    if run_count:
        # A trailing miss that reaches "now" is an open-ended tail.
        is_tail = expected[-1] >= now - 60.0
        runs.append((run_first, run_count, is_tail))
    return runs


# --- gateway start failure ----------------------------------------------
def _detect_gateway_start_failed(outbox, home, counts, when) -> None:
    """A gateway that failed to start, hit a token conflict, or vanished.

    Hermes fires the ``gateway:startup`` hook only on success, so a failed
    start emits no hook event — it is invisible to live capture (the same
    silent-failure class as ``cron.run_missed``). Hermes does write the
    failure durably, so the reconciler reads it read-only:

    - **Case A — startup_failed.** ``gateway_state.json`` has
      ``gateway_state='startup_failed'`` with an ``exit_reason``.
    - **Case B — token_conflict.** A duplicate bot token keeps the gateway
      degraded/running but marks the platform with an ``error_code`` ending
      ``_lock`` (e.g. ``discord-bot-token_lock``) / an "already in use (PID N)"
      message. Names the platform and the conflicting PID.
    - **Case C — absent.** ``gateway-starts.log`` shows the gateway started
      before, but its runtime status file is gone. Conservative: only when no
      ``gateway_state.json`` exists at all, so a clean ``gateway stop`` (which
      leaves ``gateway_state='stopped'``) is never flagged.

    Never keys liveness off ``updated_at`` — a healthy idle gateway never
    advances it. Every finding is ``partial``; each dedup key is anchored on a
    durable event time (never the reconcile-run ``when``), so a second pass
    over the same state appends nothing. The raw ``exit_reason`` /
    ``error_message`` is sensitive and goes only into encrypted content.
    """
    state_path = gateway_state_path(home)
    if state_path.exists():
        data = load_json_dict(state_path)
        state = data.get("gateway_state")
        updated_at = to_epoch(data.get("updated_at")) or 0.0

        if state == "startup_failed":
            reason = data.get("exit_reason") or ""
            _emit(
                outbox, counts,
                event_type="runtime.gateway_start_failed",
                occurred_at=updated_at or when,
                correlation_id="gateway",
                partial=True,
                payload={"reason_class": _classify_gateway_reason(reason), "gateway_state": state},
                content=reason or None,
                dedup_key=f"reconcile:gateway_start_failed:startup_failed:{int(updated_at)}",
            )

        platforms = data.get("platforms")
        if isinstance(platforms, dict):
            for pname, pinfo in platforms.items():
                if not isinstance(pinfo, dict):
                    continue
                code = pinfo.get("error_code") or ""
                msg = pinfo.get("error_message") or ""
                if not (code.endswith("_lock") or "already in use" in msg):
                    continue
                pid = _parse_pid(msg)
                p_updated = to_epoch(pinfo.get("updated_at")) or updated_at or when
                _emit(
                    outbox, counts,
                    event_type="runtime.gateway_start_failed",
                    occurred_at=p_updated,
                    correlation_id=f"gateway:{pname}",
                    partial=True,
                    payload={
                        "reason_class": "token_conflict",
                        "gateway_state": state,
                        "platform": pname,
                        "error_code": code or None,
                        "conflicting_pid": pid,
                    },
                    content=msg or None,
                    dedup_key=(
                        f"reconcile:gateway_start_failed:token_conflict:"
                        f"{pname}:{pid if pid is not None else 'unknown'}"
                    ),
                )
        return

    # Case C: started before (history) but the status file is gone.
    last_start = _last_start_epoch(gateway_starts_log_path(home))
    if last_start is not None:
        _emit(
            outbox, counts,
            event_type="runtime.gateway_start_failed",
            occurred_at=last_start,
            correlation_id="gateway",
            partial=True,
            payload={"reason_class": "absent", "last_start_at": last_start},
            dedup_key=f"reconcile:gateway_start_failed:absent:{int(last_start)}",
        )


def _classify_gateway_reason(text: str) -> str:
    """Map a gateway exit_reason to a plaintext reason class. Best-effort."""
    low = (text or "").lower()
    if "already in use" in low or "_lock" in low or "conflict" in low:
        return "token_conflict"
    if "policy" in low:
        return "policy_open"
    if "config" in low or "invalid" in low or "not found" in low:
        return "config_invalid"
    return "unknown"


def _parse_pid(text: str) -> int | None:
    match = _PID_RE.search(text or "")
    return int(match.group(1)) if match else None


def _last_start_epoch(path: Path) -> float | None:
    """The last epoch in gateway-starts.log (a newline list of floats)."""
    if not path.exists():
        return None
    last: float | None = None
    try:
        for line in path.read_text().splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            try:
                last = float(stripped)
            except ValueError:
                continue
    except OSError:
        return None
    return last


# --- ticker liveness ----------------------------------------------------
def _ticker_is_stale(outbox, home, counts, when, cfg) -> bool:
    hb = read_float(ticker_heartbeat_path(home))
    if hb is None:
        return False
    staleness = when - hb
    if staleness <= cfg.ticker_stale_after:
        return False
    _emit_terminal_missing(
        outbox,
        counts,
        occurred_at=when,
        correlation_id="cron:ticker",
        subject_type="cron_ticker",
        subject_id="cron:ticker",
        start_event_type="cron.ticker_heartbeat",
        expected_terminal_event_type="cron.ticker_heartbeat",
        details={
            "heartbeat": hb,
            "staleness_seconds": staleness,
        },
        dedup_key=f"reconcile:ticker_stale:{int(hb)}",
    )
    return True


# --- durable helpers ----------------------------------------------------
def _job_is_active(job: dict[str, Any]) -> bool:
    if job.get("enabled") is False:
        return False
    if job.get("state") == "paused" or job.get("paused_at"):
        return False
    repeat = job.get("repeat") or {}
    times = repeat.get("times")
    if times is not None and (repeat.get("completed") or 0) >= times:
        return False
    return True


def _load_execution_rows(home: Path) -> list[dict[str, Any]]:
    """One snapshot of executions.db, with ``claimed_at`` pre-converted.

    Coverage, missing-terminal, and missed-cron detection all consume this,
    so the store is opened and scanned once per reconcile pass.
    """
    exec_path = executions_db_path(home)
    if not exec_path.exists():
        return []
    conn = open_sqlite_read_only(exec_path)
    try:
        rows = conn.execute(
            "SELECT id, job_id, status, claimed_at, finished_at FROM executions"
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "id": r["id"],
            "job_id": r["job_id"],
            "status": r["status"],
            "claimed_epoch": to_epoch(r["claimed_at"]),
            "finished_at": r["finished_at"],
        }
        for r in rows
    ]


def _load_jobs(path: Path) -> list[dict[str, Any]]:
    jobs = load_json_dict(path).get("jobs")
    return [j for j in jobs if isinstance(j, dict)] if isinstance(jobs, list) else []


def _near(value: float, sorted_epochs: list[float], slack: float) -> bool:
    i = bisect_left(sorted_epochs, value - slack)
    return i < len(sorted_epochs) and sorted_epochs[i] <= value + slack


def _tz_of(value: Any) -> datetime.tzinfo | None:
    """The tzinfo embedded in a Hermes ISO timestamp, or None if absent."""
    if not isinstance(value, str):
        return None
    try:
        return datetime.datetime.fromisoformat(value).tzinfo
    except ValueError:
        return None


# --- emit ---------------------------------------------------------------
def _emit(
    outbox,
    counts,
    *,
    event_type: str,
    occurred_at: float,
    correlation_id: str,
    payload: dict[str, Any],
    dedup_key: str,
    session_id: str | None = None,
    parent_session_id: str | None = None,
    invocation_id: str | None = None,
    profile: str = "default",
    partial: bool = True,
    content: str | None = None,
) -> None:
    rec = build_record(
        event_type=event_type,
        occurred_at=occurred_at,
        source=_SOURCE,
        capture_method=_CAPTURE,
        runtime=runtime_stamp("reconciler"),
        correlation_id=correlation_id,
        payload=payload,
        session_id=session_id,
        parent_session_id=parent_session_id,
        invocation_id=invocation_id,
        profile=profile,
        partial=partial,
    )
    append_and_count(outbox, counts, rec, content=content, dedup_key=dedup_key)
