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
  Sessions and cron runs are judged from the authoritative durable row
  (``ended_at`` / ``finished_at`` still NULL); invocations are judged from
  the outbox (``invocation.started`` with no ``invocation.completed``),
  because the ``turn_id`` lives only in memory. Emit
  ``reconcile.terminal_missing``.
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
import math
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from ._common import build_record, resolve_hermes_home, root_session, runtime_stamp, to_epoch

_SOURCE = "reconciler"
_CAPTURE = "derive:reconciler"


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
) -> dict[str, int]:
    """One reconcile pass. Returns per-event-type counts of new findings."""
    cfg = config or ReconcileConfig()
    when = float(now) if now is not None else _wall_clock()
    home = resolve_hermes_home(hermes_home)
    installation_id = outbox.installation_id

    # Snapshot the captured stream once, before any emission, so findings
    # appended this pass never perturb detection within the same pass.
    events = list(outbox.iter_events(installation_id))
    counts: dict[str, int] = defaultdict(int)

    _detect_sequence_gaps(outbox, events, installation_id, counts, when)
    _detect_coverage_gaps(outbox, events, home, counts, when)
    _detect_missing_terminals(outbox, events, home, counts, when, cfg)
    _detect_missed_cron(outbox, home, counts, when, cfg)
    _detect_gateway_start_failed(outbox, home, counts, when, cfg)
    return dict(counts)


# --- sequence gaps ------------------------------------------------------
def _detect_sequence_gaps(outbox, events, installation_id, counts, when) -> None:
    seqs = sorted(e["producer_sequence"] for e in events)
    if not seqs:
        return
    present = set(seqs)
    lo, hi = seqs[0], seqs[-1]
    for missing in range(lo + 1, hi):
        if missing in present:
            continue
        # Bracket the hole with its surviving neighbours for context.
        prev_seq = max((s for s in present if s < missing), default=None)
        next_seq = min((s for s in present if s > missing), default=None)
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
def _detect_coverage_gaps(outbox, events, home, counts, when) -> None:
    """A durable row with no captured event proves a dropped capture."""
    captured = _captured_subjects(events)

    state_path = home / "state.db"
    if state_path.exists():
        conn = _open_ro(state_path)
        try:
            parent_map = {
                r["id"]: r["parent_session_id"]
                for r in conn.execute("SELECT id, parent_session_id FROM sessions")
            }
            _coverage_sessions(outbox, conn, parent_map, captured, counts, when)
            _coverage_tool_messages(outbox, conn, parent_map, captured, counts, when)
            _coverage_model_usage(outbox, conn, parent_map, captured, counts, when)
        finally:
            conn.close()

    exec_path = home / "cron" / "executions.db"
    if exec_path.exists():
        conn = _open_ro(exec_path)
        try:
            rows = conn.execute("SELECT id, job_id FROM executions").fetchall()
        finally:
            conn.close()
        for r in rows:
            if r["id"] in captured["executions"]:
                continue
            _emit_coverage(
                outbox, counts, when,
                subject_type="execution", subject_id=r["id"],
                source_table="cron:executions.db", correlation_id=r["job_id"],
            )


def _coverage_sessions(outbox, conn, parent_map, captured, counts, when) -> None:
    for r in conn.execute("SELECT id, parent_session_id FROM sessions"):
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


def _coverage_tool_messages(outbox, conn, parent_map, captured, counts, when) -> None:
    rows = conn.execute(
        "SELECT id, session_id FROM messages WHERE role='tool'"
    ).fetchall()
    for r in rows:
        if r["id"] in captured["tool_messages"]:
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


def _captured_subjects(events) -> dict[str, set]:
    """Index the captured stream by the durable subject each event covers."""
    sessions: set[str] = set()
    tool_messages: set[int] = set()
    model_usage: set[tuple] = set()
    executions: set[str] = set()
    for e in events:
        pl = e.get("payload", {})
        et = pl.get("event_type")
        if et in ("session.created", "subagent.child_spawned"):
            if e.get("session_id") is not None:
                sessions.add(e["session_id"])
        elif et == "tool.call_completed":
            mid = pl.get("message_row_id")
            if mid is not None:
                tool_messages.add(mid)
        elif et == "model.usage_recorded":
            model_usage.add((e.get("session_id"), pl.get("model"), pl.get("task")))
        elif et == "cron.run_claimed":
            exid = pl.get("execution_id")
            if exid is not None:
                executions.add(exid)
    return {
        "sessions": sessions,
        "tool_messages": tool_messages,
        "model_usage": model_usage,
        "executions": executions,
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
def _detect_missing_terminals(outbox, events, home, counts, when, cfg) -> None:
    _terminals_sessions(outbox, home, counts, when, cfg)
    _terminals_cron_runs(outbox, home, counts, when, cfg)
    _terminals_invocations(outbox, events, counts, when, cfg)


def _terminals_sessions(outbox, home, counts, when, cfg) -> None:
    """A durable session/subagent row with ended_at NULL past its window.

    The durable row is authoritative: a live session keeps ended_at=NULL and
    is not a crash, so judge it only after the lifetime window.
    """
    state_path = home / "state.db"
    if not state_path.exists():
        return
    conn = _open_ro(state_path)
    try:
        rows = conn.execute(
            "SELECT id, source, parent_session_id, started_at, ended_at, "
            "expiry_finalized, profile_name FROM sessions"
        ).fetchall()
        parent_map = {r["id"]: r["parent_session_id"] for r in rows}
    finally:
        conn.close()

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
        expected = "subagent.completed" if is_sub else "session.ended"
        sid = r["id"]
        corr = root_session(sid, parent_map) or sid
        _emit(
            outbox, counts,
            event_type="reconcile.terminal_missing",
            occurred_at=when,
            correlation_id=corr,
            session_id=sid,
            parent_session_id=r["parent_session_id"],
            profile=r["profile_name"] or "default",
            partial=True,
            payload={
                "subject_type": subject_type,
                "subject_id": sid,
                "start_event_type": "subagent.child_spawned" if is_sub else "session.created",
                "expected_terminal_event_type": expected,
                "start_occurred_at": started,
                "age_seconds": age,
            },
            dedup_key=f"reconcile:terminal:{subject_type}:{sid}",
        )


def _terminals_cron_runs(outbox, home, counts, when, cfg) -> None:
    """A durable execution with finished_at NULL past its window."""
    exec_path = home / "cron" / "executions.db"
    if not exec_path.exists():
        return
    conn = _open_ro(exec_path)
    try:
        rows = conn.execute(
            "SELECT id, job_id, status, claimed_at, finished_at FROM executions"
        ).fetchall()
    finally:
        conn.close()
    for r in rows:
        if r["finished_at"] is not None:
            continue
        claimed = to_epoch(r["claimed_at"])
        if claimed is None:
            continue
        age = when - claimed
        if age <= cfg.cron_run_terminal_timeout:
            continue
        exid = r["id"]
        _emit(
            outbox, counts,
            event_type="reconcile.terminal_missing",
            occurred_at=when,
            correlation_id=r["job_id"],
            partial=True,
            payload={
                "subject_type": "cron_run",
                "subject_id": exid,
                "job_id": r["job_id"],
                "start_event_type": "cron.run_claimed",
                "expected_terminal_event_type": "cron.run_finished",
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
        _emit(
            outbox, counts,
            event_type="reconcile.terminal_missing",
            occurred_at=when,
            correlation_id=e.get("correlation_id") or inv,
            session_id=e.get("session_id"),
            parent_session_id=e.get("parent_session_id"),
            invocation_id=inv,
            profile=e.get("profile") or "default",
            partial=True,
            payload={
                "subject_type": "invocation",
                "subject_id": inv,
                "start_event_type": "invocation.started",
                "expected_terminal_event_type": "invocation.completed",
                "start_occurred_at": occurred,
                "age_seconds": when - occurred,
            },
            dedup_key=f"reconcile:terminal:invocation:{inv}",
        )


# --- missed cron --------------------------------------------------------
def _detect_missed_cron(outbox, home, counts, when, cfg) -> None:
    cron_dir = home / "cron"
    jobs_path = cron_dir / "jobs.json"
    if not jobs_path.exists():
        return
    jobs = _load_jobs(jobs_path)
    if not jobs:
        return

    # A stale heartbeat means the whole scheduler is dead: one installation
    # signal, and suppress the per-job trailing catch-up it would explain.
    ticker_dead = _ticker_is_stale(outbox, cron_dir, counts, when, cfg)

    exec_by_job = _execution_epochs_by_job(cron_dir)
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
    fields = _parse_cron(expr)
    if fields is None:
        return []
    anchor = execs[0] if execs else created
    if anchor is None:
        return []
    lower = max(anchor, now - cfg.cron_lookback)
    expected = _cron_instants(fields, lower, now, tz)
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


# --- cron expression parsing --------------------------------------------
def _parse_cron(expr: str | None):
    """Parse a standard 5-field cron expression into per-field int sets."""
    if not isinstance(expr, str):
        return None
    parts = expr.split()
    if len(parts) != 5:
        return None
    bounds = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 6))
    try:
        return tuple(_parse_cron_field(p, lo, hi) for p, (lo, hi) in zip(parts, bounds))
    except ValueError:
        return None


def _parse_cron_field(field: str, lo: int, hi: int) -> set[int]:
    values: set[int] = set()
    for token in field.split(","):
        step = 1
        if "/" in token:
            token, step_s = token.split("/", 1)
            step = int(step_s)
            if step <= 0:
                raise ValueError("step must be positive")
        if token in ("*", ""):
            start, end = lo, hi
        elif "-" in token:
            a, b = token.split("-", 1)
            start, end = int(a), int(b)
        else:
            start = end = int(token)
        if start < lo or end > hi or start > end:
            raise ValueError("field out of range")
        values.update(range(start, end + 1, step))
    return values


def _cron_instants(fields, lower: float, now: float, tz=None) -> list[float]:
    # Resolve each candidate epoch to wall-clock fields in a fixed zone (the
    # job's own offset, or UTC as a deterministic fallback) — never the host
    # process's ambient timezone, so the expansion is environment-independent.
    zone = tz or datetime.timezone.utc
    minute, hour, dom, month, dow = fields
    dom_restricted = len(dom) < 31
    dow_restricted = len(dow) < 7
    out: list[float] = []
    t = math.ceil(lower / 60.0) * 60.0
    while t <= now:
        d = datetime.datetime.fromtimestamp(t, zone)
        # Standard cron day matching: when both day-of-month and
        # day-of-week are restricted, a match on either satisfies the day;
        # otherwise both restrictions apply (a "*" is not a restriction).
        dom_hit, dow_hit = d.day in dom, _dow(d) in dow
        if dom_restricted and dow_restricted:
            day_ok = dom_hit or dow_hit
        else:
            day_ok = (dom_hit or not dom_restricted) and (dow_hit or not dow_restricted)
        if d.minute in minute and d.hour in hour and d.month in month and day_ok:
            out.append(float(int(t)))
        t += 60.0
    return out


def _dow(d: datetime.datetime) -> int:
    """Cron day-of-week: Sunday is 0 (Python's Monday-0 shifted)."""
    return (d.weekday() + 1) % 7


# --- gateway start failure ----------------------------------------------
def _detect_gateway_start_failed(outbox, home, counts, when, cfg) -> None:
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
    state_path = home / "gateway_state.json"
    if state_path.exists():
        data = _load_json(state_path)
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
    last_start = _last_start_epoch(home / "gateway-starts.log")
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
    import re

    match = re.search(r"PID (\d+)", text or "")
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


def _load_json(path: Path) -> dict[str, Any]:
    import json

    try:
        obj = json.loads(path.read_text())
    except (ValueError, OSError):
        return {}
    return obj if isinstance(obj, dict) else {}


# --- ticker liveness ----------------------------------------------------
def _ticker_is_stale(outbox, cron_dir, counts, when, cfg) -> bool:
    hb = _read_float(cron_dir / "ticker_heartbeat")
    if hb is None:
        return False
    staleness = when - hb
    if staleness <= cfg.ticker_stale_after:
        return False
    _emit(
        outbox, counts,
        event_type="reconcile.terminal_missing",
        occurred_at=when,
        correlation_id="cron:ticker",
        partial=True,
        payload={
            "subject_type": "cron_ticker",
            "subject_id": "cron:ticker",
            "start_event_type": "cron.ticker_heartbeat",
            "expected_terminal_event_type": "cron.ticker_heartbeat",
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


def _execution_epochs_by_job(cron_dir: Path) -> dict[str, list[float]]:
    exec_path = cron_dir / "executions.db"
    if not exec_path.exists():
        return {}
    conn = _open_ro(exec_path)
    try:
        rows = conn.execute("SELECT job_id, claimed_at FROM executions").fetchall()
    finally:
        conn.close()
    by_job: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        epoch = to_epoch(r["claimed_at"])
        if epoch is not None:
            by_job[r["job_id"]].append(epoch)
    return by_job


def _load_jobs(path: Path) -> list[dict[str, Any]]:
    import json

    try:
        obj = json.loads(path.read_text())
    except (ValueError, OSError):
        return []
    jobs = obj.get("jobs") if isinstance(obj, dict) else None
    return [j for j in jobs if isinstance(j, dict)] if isinstance(jobs, list) else []


def _read_float(path: Path) -> float | None:
    if not path.exists():
        return None
    text = path.read_text().strip()
    try:
        return float(text) if text else None
    except ValueError:
        return None


def _near(value: float, sorted_epochs: Iterable[float], slack: float) -> bool:
    return any(abs(e - value) <= slack for e in sorted_epochs)


def _tz_of(value: Any) -> datetime.tzinfo | None:
    """The tzinfo embedded in a Hermes ISO timestamp, or None if absent."""
    if not isinstance(value, str):
        return None
    try:
        return datetime.datetime.fromisoformat(value).tzinfo
    except ValueError:
        return None


def _open_ro(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _wall_clock() -> float:
    import time

    return time.time()


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
    outbox.append(rec, content=content, dedup_key=dedup_key)
    if outbox.last_append_created:
        counts[event_type] += 1
