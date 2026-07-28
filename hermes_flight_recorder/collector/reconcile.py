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

from . import CAPTURE_HEARTBEAT_KEY, knowledge_store
from ..envelope import SESSION_LIFECYCLE, SESSION_START_TYPES
from ._common import (
    INSTALLED_AT_META_KEY,
    append_and_count,
    build_record,
    executions_db_path,
    gateway_starts_log_path,
    gateway_state_path,
    jobs_path,
    kanban_board_dbs,
    load_json_dict,
    occurred_before,
    open_sqlite_read_only,
    read_float,
    read_home_mode,
    resolve_hermes_home,
    root_session,
    runtime_stamp,
    sqlite_column_or_default,
    sqlite_select_list,
    sqlite_table_columns,
    sqlite_table_exists,
    state_db_path,
    ticker_heartbeat_path,
    to_epoch,
)
from .cron_schedule import expected_instants
from .recorder_config import CaptureConfig, KnowledgeConfig

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
    # A durable row must remain absent from the captured stream through more
    # than one capture tick before it is a coverage gap. Reconcile and capture
    # can start at the same second, so an immediate diff creates false alerts.
    coverage_grace: float = 30.0
    # An open task claim whose lease lapsed by more than this grace, with a
    # heartbeat older than the staleness window, is judged a dead attempt. The
    # grace lets Hermes's own reclaim run first (its defer grace is ~120 s); the
    # staleness window is the Hermes default claim TTL (a live worker renews the
    # lease within it, so a stale heartbeat means the worker is gone).
    task_lease_grace: float = 120.0
    task_heartbeat_stale_after: float = 15 * 60.0
    # A heartbeat older than this means the whole scheduler is dead.
    ticker_stale_after: float = 300.0
    # The Flight Recorder's own capture heartbeat (``capture:last_success_at``,
    # stamped every completed capture pass ~every 15s). Older than this and the
    # capture loop has stopped ticking — the silent outage. 5 min = 20 missed
    # ticks, comfortably above deploy-restart jitter yet far tighter than the
    # 3h20m blackout that went unseen.
    capture_stale_after: float = 300.0
    # A knowledge change on disk (or a store version with no event) younger than
    # this is not yet drift — a healthy capture pass runs every ~15s and would
    # still be catching up. Older than this, the scanner/emitter genuinely missed
    # it. Same 5-min margin as the capture heartbeat; avoids the :00 boundary
    # race where capture and reconcile both see a just-written file.
    knowledge_drift_grace: float = 300.0
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
    knowledge_config: KnowledgeConfig | None = None,
) -> dict[str, int]:
    """One reconcile pass. Returns per-event-type counts of new findings."""
    cfg = config or ReconcileConfig()
    capture = capture_config or CaptureConfig()
    knowledge = knowledge_config or KnowledgeConfig()
    when = float(now) if now is not None else time.time()
    home = resolve_hermes_home(hermes_home)
    installation_id = outbox.installation_id
    horizon = _install_horizon(outbox)

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
        outbox, events, home, exec_rows, counts, when, cfg, capture, horizon
    )
    _detect_missing_terminals(
        outbox, events, exec_rows, counts, when, cfg, session_rows, parent_map, horizon
    )
    _detect_missed_cron(outbox, home, exec_rows, counts, when, cfg, horizon)
    _detect_gateway_start_failed(outbox, home, counts, when)
    _detect_stale_task_leases(outbox, home, counts, when, cfg)
    _detect_capture_stale(outbox, counts, when, cfg)
    _detect_knowledge_gaps(outbox, home, counts, when, cfg, knowledge)
    return dict(counts)


# --- install horizon ----------------------------------------------------
def _install_horizon(outbox: Any) -> float:
    """The epoch before which the reconciler ignores durable history.

    The ``installed_at`` marker stamped at ``install`` (see lifecycle). Returns
    ``0.0`` (no horizon — reconcile the full store, the pre-#109 behavior) when
    the marker is absent, so an install that predates this marker is protected
    the moment it re-runs ``install``. The "should have finished by now"
    detectors (``terminal_missing``, ``cron.run_missed``) skip subjects that
    started before this, so a fresh install over a long-lived Hermes home does
    not flag work that ended before the recorder existed. Coverage-gap detection
    also ignores durable rows that predate the horizon; otherwise a no-backfill
    install over a long-lived Hermes home immediately reports the whole historic
    store as uncaptured.
    """
    raw = outbox.get_meta(INSTALLED_AT_META_KEY)
    if raw is not None:
        try:
            return float(raw)
        except (TypeError, ValueError):
            pass
    return 0.0


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
    outbox, events, home, exec_rows, counts, when, config, capture_config, horizon
):
    """A durable row with no captured event proves a dropped capture."""
    captured = _captured_subjects(events)
    session_rows = []
    parent_map = {}

    state_path = state_db_path(home)
    if state_path.exists():
        conn = open_sqlite_read_only(state_path)
        try:
            session_cols = sqlite_table_columns(conn, "sessions")
            session_select = ", ".join(
                sqlite_column_or_default(session_cols, name)
                for name in (
                    "id",
                    "source",
                    "parent_session_id",
                    "started_at",
                    "ended_at",
                    "profile_name",
                )
            )
            session_rows = conn.execute(
                f"SELECT {session_select} FROM sessions"
            ).fetchall()
            parent_map = {r["id"]: r["parent_session_id"] for r in session_rows}
            session_started = {r["id"]: r["started_at"] for r in session_rows}
            _coverage_sessions(
                outbox, session_rows, parent_map, captured, counts, when, config, horizon
            )
            _coverage_messages(
                outbox,
                conn,
                parent_map,
                session_started,
                captured,
                counts,
                when,
                config,
                capture_config,
                horizon,
            )
            _coverage_model_usage(
                outbox, conn, parent_map, session_started, captured, counts, when, config, horizon
            )
        finally:
            conn.close()

    for r in exec_rows:
        if occurred_before(horizon, r["claimed_epoch"] or r["finished_at"]):
            continue
        if r["id"] in captured["executions"]:
            _clear_coverage_pending(outbox, "execution", r["id"])
            continue
        _emit_coverage(
            outbox, counts, when,
            subject_type="execution", subject_id=r["id"],
            source_table="cron:executions.db", correlation_id=r["job_id"],
            grace=config.coverage_grace,
        )
    _coverage_kanban(outbox, home, captured, counts, when, config, horizon)
    return session_rows, parent_map


def _coverage_sessions(
    outbox, rows, parent_map, captured, counts, when, config, horizon
) -> None:
    for r in rows:
        if occurred_before(horizon, r["started_at"]):
            continue
        sid = r["id"]
        if sid in captured["sessions"]:
            _clear_coverage_pending(outbox, "session", sid)
            continue
        corr = root_session(sid, parent_map) or sid
        _emit_coverage(
            outbox, counts, when,
            subject_type="session", subject_id=sid,
            source_table="state.db:sessions", correlation_id=corr,
            session_id=sid, parent_session_id=r["parent_session_id"],
            grace=config.coverage_grace,
        )


def _coverage_messages(
    outbox, conn, parent_map, session_started, captured, counts, when, config, capture_config, horizon
) -> None:
    roles = tuple(
        role
        for role in ("user", "assistant", "tool")
        if role in capture_config.message_roles
    )
    if not roles:
        return
    columns = {row[1] for row in conn.execute("PRAGMA table_info(messages)")}
    if not columns:
        return  # no messages table on this Hermes home — nothing to reconcile
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
    timestamp_expr = sqlite_column_or_default(columns, "timestamp")
    rows = conn.execute(
        f"SELECT id, session_id, {timestamp_expr} FROM messages "
        f"WHERE role IN ({placeholders}){content_predicate}",
        roles,
    ).fetchall()
    for r in rows:
        sid = r["session_id"]
        if occurred_before(horizon, r["timestamp"]):
            continue
        if r["timestamp"] is None and occurred_before(horizon, session_started.get(sid)):
            continue
        if r["id"] in captured["messages"]:
            _clear_coverage_pending(outbox, "message", str(r["id"]))
            continue
        corr = root_session(sid, parent_map) or sid
        _emit_coverage(
            outbox, counts, when,
            subject_type="message", subject_id=str(r["id"]),
            source_table="state.db:messages", correlation_id=corr, session_id=sid,
            grace=config.coverage_grace,
        )


def _coverage_model_usage(
    outbox, conn, parent_map, session_started, captured, counts, when, config, horizon
) -> None:
    if not sqlite_table_exists(conn, "session_model_usage"):
        return
    rows = conn.execute(
        "SELECT session_id, model, task FROM session_model_usage"
    ).fetchall()
    for r in rows:
        if occurred_before(horizon, session_started.get(r["session_id"])):
            continue
        key = (r["session_id"], r["model"], r["task"])
        subject_id = f"{r['session_id']}:{r['model']}:{r['task']}"
        if key in captured["model_usage"]:
            _clear_coverage_pending(outbox, "model_usage", subject_id)
            continue
        sid = r["session_id"]
        corr = root_session(sid, parent_map) or sid
        _emit_coverage(
            outbox, counts, when,
            subject_type="model_usage", subject_id=subject_id,
            source_table="state.db:session_model_usage", correlation_id=corr, session_id=sid,
            grace=config.coverage_grace,
        )


def _coverage_kanban(outbox, home, captured, counts, when, config, horizon) -> None:
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
            if "tasks" in present:
                task_cols = sqlite_table_columns(conn, "tasks")
                tasks = conn.execute(
                    "SELECT id, session_id, "
                    f"{sqlite_column_or_default(task_cols, 'created_at')}, "
                    f"{sqlite_column_or_default(task_cols, 'started_at')} FROM tasks"
                ).fetchall()
            else:
                tasks = []
            if "task_runs" in present:
                run_cols = sqlite_table_columns(conn, "task_runs")
                runs = conn.execute(
                    "SELECT id, task_id, "
                    f"{sqlite_column_or_default(run_cols, 'started_at')}, "
                    f"{sqlite_column_or_default(run_cols, 'ended_at')} FROM task_runs"
                ).fetchall()
            else:
                runs = []
        finally:
            conn.close()
        for r in tasks:
            if occurred_before(horizon, r["created_at"] or r["started_at"]):
                continue
            if (board, r["id"]) in captured["tasks"]:
                _clear_coverage_pending(outbox, "task", f"{board}:{r['id']}")
                continue
            _emit_coverage(
                outbox, counts, when,
                subject_type="task", subject_id=f"{board}:{r['id']}",
                source_table=f"kanban:{board}:tasks", correlation_id=r["id"],
                session_id=r["session_id"],
                grace=config.coverage_grace,
            )
        for r in runs:
            if occurred_before(horizon, r["started_at"] or r["ended_at"]):
                continue
            if (board, r["id"]) in captured["task_runs"]:
                _clear_coverage_pending(outbox, "task_run", f"{board}:{r['id']}")
                continue
            _emit_coverage(
                outbox, counts, when,
                subject_type="task_run", subject_id=f"{board}:{r['id']}",
                source_table=f"kanban:{board}:task_runs", correlation_id=r["task_id"],
                grace=config.coverage_grace,
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
    grace, session_id=None, parent_session_id=None,
) -> None:
    if not _coverage_ready(outbox, subject_type, subject_id, when, grace):
        return
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


def _coverage_pending_key(subject_type: str, subject_id: Any) -> str:
    return f"reconcile:coverage_pending:{subject_type}:{subject_id}"


def _clear_coverage_pending(outbox, subject_type: str, subject_id: Any) -> None:
    outbox.delete_meta(_coverage_pending_key(subject_type, subject_id))


def _coverage_ready(
    outbox, subject_type: str, subject_id: Any, when: float, grace: float
) -> bool:
    """Wait through a capture tick before an absent durable row is a gap."""
    if grace <= 0:
        return True
    key = _coverage_pending_key(subject_type, subject_id)
    raw = outbox.get_meta(key)
    if raw is None:
        outbox.set_meta(key, repr(when))
        return False
    try:
        first_seen = float(raw)
    except (TypeError, ValueError):
        outbox.set_meta(key, repr(when))
        return False
    return when - first_seen >= grace


# --- missing terminals --------------------------------------------------
def _detect_missing_terminals(
    outbox, events, exec_rows, counts, when, cfg, session_rows, parent_map, horizon
) -> None:
    _terminals_sessions(outbox, session_rows, parent_map, counts, when, cfg, horizon)
    _terminals_cron_runs(outbox, exec_rows, counts, when, cfg, horizon)
    _terminals_invocations(outbox, events, counts, when, cfg, horizon)


def _terminals_sessions(outbox, rows, parent_map, counts, when, cfg, horizon) -> None:
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
        if started < horizon:
            continue  # started before the recorder existed — not our crash
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


def _terminals_cron_runs(outbox, exec_rows, counts, when, cfg, horizon) -> None:
    """A durable execution with finished_at NULL past its window."""
    for r in exec_rows:
        if r["finished_at"] is not None:
            continue
        claimed = r["claimed_epoch"]
        if claimed is None:
            continue
        if claimed < horizon:
            continue  # claimed before the recorder existed
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


def _terminals_invocations(outbox, events, counts, when, cfg, horizon) -> None:
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
        if occurred < horizon:
            continue  # started before the recorder existed
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
            cols = sqlite_table_columns(conn, "task_runs")
            if "outcome" not in cols:
                rows = []  # no such table/column — nothing open to judge
            else:
                select = sqlite_select_list(
                    conn,
                    "task_runs",
                    ("id", "task_id", "claim_lock", "claim_expires", "worker_pid",
                     "last_heartbeat_at", "started_at"),
                )
                rows = conn.execute(
                    f"SELECT {select} FROM task_runs WHERE outcome IS NULL"
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
def _detect_missed_cron(outbox, home, exec_rows, counts, when, cfg, horizon) -> None:
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
        _missed_for_job(outbox, job, exec_by_job, counts, when, cfg, ticker_dead, horizon)


def _missed_for_job(outbox, job, exec_by_job, counts, when, cfg, ticker_dead, horizon) -> None:
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
        if first_at < horizon:
            continue  # the fire was due before the recorder existed
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
        deadline = now - slack
        if deadline < first:
            return runs  # not due yet
        count = max(1, int((deadline - created) // step))
        return [(first, count, True)]

    i = 0
    n = len(execs)
    expected = execs[0] + step
    run_first: float | None = None
    run_count = 0
    deadline = now - slack
    while expected <= now + slack:
        while i < n and execs[i] < expected - slack:
            i += 1
        if i < n and execs[i] <= expected + slack:
            if run_count:
                runs.append((run_first, run_count, False))
                run_first, run_count = None, 0
            expected = execs[i] + step
            i += 1
        elif expected <= deadline:
            if run_count == 0:
                run_first = expected
            run_count += 1
            expected += step
        else:
            break
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
    deadline = now - slack
    runs: list[tuple[float, int, bool]] = []
    run_first: float | None = None
    run_count = 0
    for inst in expected:
        if _near(inst, execs, slack):
            if run_count:
                runs.append((run_first, run_count, False))
                run_first, run_count = None, 0
        elif inst <= deadline:
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


# --- capture liveness ---------------------------------------------------
def _detect_capture_stale(outbox, counts, when, cfg) -> None:
    """The Flight Recorder watching its OWN capture loop.

    ``run_pass`` stamps ``capture:last_success_at`` on every completed pass.
    The reconciler fires on its own realtime timer, independent of capture, so
    a capture loop that stopped ticking — a dead timer, a crash-loop, a hung
    pass — leaves this heartbeat frozen while reconcile keeps running. That is
    exactly the silent outage that ran ~3h20m unseen: capture reported
    active/success while never firing.

    A heartbeat older than the window emits ONE ``reconcile.capture_stale``
    finding, keyed on the frozen heartbeat so a dead capture alerts once, not
    once per reconcile minute. An absent heartbeat means no baseline yet (a
    fresh install where capture never ran); it raises no alert, mirroring the
    ticker-staleness rule. The signal is installation-wide, so it correlates on
    the installation id like a sequence gap does.
    """
    raw = outbox.get_meta(CAPTURE_HEARTBEAT_KEY)
    if raw is None:
        return
    try:
        last = float(raw)
    except (TypeError, ValueError):
        return  # malformed heartbeat: treat as no baseline, never crash
    staleness = when - last
    if staleness <= cfg.capture_stale_after:
        return
    _emit(
        outbox,
        counts,
        event_type="reconcile.capture_stale",
        occurred_at=when,
        correlation_id=outbox.installation_id,
        partial=True,
        payload={
            "last_success_at": last,
            "staleness_seconds": staleness,
            "threshold_seconds": cfg.capture_stale_after,
        },
        dedup_key=f"reconcile:capture_stale:{int(last)}",
    )


# --- knowledge drift + event gap ----------------------------------------
def _detect_knowledge_gaps(outbox, home, counts, when, cfg, knowledge_config) -> None:
    """The Phase 3 analog of the coverage/missed-cron reconcilers.

    Two integrity checks over the two-stage knowledge pipeline (disk → store →
    event), each a backstop for one stage the scanner runs on the capture path:

    - **Store-vs-disk drift.** A tracked artifact whose on-disk content no longer
      matches its latest store version — the scanner missed a change (capture was
      down, or a write slipped a scan). Emit ``gap_kind='uncaptured_knowledge'``
      and heal by recording the missed version through the scanner's own path.
    - **Store-vs-event gap.** A store version the emitter never turned into a
      ``knowledge.record_written`` — the transport fell behind. Emit
      ``gap_kind='unemitted_knowledge'``.

    Both walk the same Hermes-created surface the scanner does, so a bundled or
    Hub skill is never flagged or backfilled. Both apply a grace window so a
    just-written file (which a healthy capture would still be catching up on) is
    not mistaken for a missed scan, and dedup on durable identity + content hash,
    never the reconcile clock.
    """
    home_mode = read_home_mode(home)
    _detect_knowledge_drift(outbox, home, home_mode, counts, when, cfg, knowledge_config)
    _detect_unemitted_knowledge(outbox, counts, when, cfg)


def _detect_knowledge_drift(
    outbox, home, home_mode, counts, when, cfg, knowledge_config
) -> None:
    for artifact_id, kind, name, category, files in knowledge_store.iter_disk_artifacts(home):
        try:
            manifest, occurred_at = knowledge_store.read_manifest(outbox, files)
        except OSError:
            continue  # a live file vanished/locked between listing and read
        if not manifest:
            continue
        disk_hash = outbox._manifest_hash(manifest)
        latest = outbox.latest_knowledge_version(artifact_id)
        stored_hash = (
            None if (latest is None or latest["is_tombstone"]) else latest["manifest_hash"]
        )
        if stored_hash == disk_hash:
            continue  # the store already reflects disk — no drift
        if when - occurred_at <= cfg.knowledge_drift_grace:
            continue  # too fresh — a healthy capture would still be catching up
        _emit(
            outbox,
            counts,
            event_type="reconcile.gap_detected",
            occurred_at=when,
            correlation_id=f"knowledge:{artifact_id}",
            partial=True,
            payload={
                "gap_kind": "uncaptured_knowledge",
                "subject_type": kind,
                "subject_id": artifact_id,
                "source_table": "fs:knowledge",
                "disk_manifest_hash": disk_hash,
                "stored_manifest_hash": stored_hash,
            },
            dedup_key=f"reconcile:knowledge:{artifact_id}:{disk_hash}",
        )
        # Heal: record the missed version (and emit its event) so the content is
        # captured, not just flagged. Idempotent — a re-run finds no drift.
        knowledge_store.heal_artifact(
            outbox, knowledge_config, home_mode, artifact_id, kind, name, category, files
        )


def _detect_unemitted_knowledge(outbox, counts, when, cfg) -> None:
    for artifact_id in outbox.knowledge_artifact_ids():
        last_emitted = int(outbox.get_meta(f"knowledge:emitted:{artifact_id}") or 0)
        for version in outbox.knowledge_versions(artifact_id):
            if version["seq"] <= last_emitted:
                continue  # already emitted
            if when - version["occurred_at"] <= cfg.knowledge_drift_grace:
                continue  # freshly captured — the emitter will ship it next tick
            _emit(
                outbox,
                counts,
                event_type="reconcile.gap_detected",
                occurred_at=when,
                correlation_id=f"knowledge:{artifact_id}",
                partial=True,
                payload={
                    "gap_kind": "unemitted_knowledge",
                    "subject_type": "knowledge_version",
                    "subject_id": f"{artifact_id}:v{version['seq']}",
                    "source_table": "store:knowledge_version",
                    "origin": version["origin"],
                    "version_seq": version["seq"],
                },
                dedup_key=f"reconcile:knowledge_unemitted:{artifact_id}:v{version['seq']}",
            )


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
        if not sqlite_table_exists(conn, "executions"):
            return []
        select = sqlite_select_list(
            conn, "executions", ("id", "job_id", "status", "claimed_at", "finished_at")
        )
        rows = conn.execute(f"SELECT {select} FROM executions").fetchall()
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
