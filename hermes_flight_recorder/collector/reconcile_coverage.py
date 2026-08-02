"""Complete durable-source coverage audit."""

from __future__ import annotations

from typing import Any

from ..envelope import SESSION_START_TYPES
from ._common import (
    kanban_board_dbs,
    occurred_before,
    open_sqlite_read_only,
    root_session,
    sqlite_column_or_default,
    sqlite_table_columns,
    sqlite_table_exists,
    state_db_path,
)
from .recorder_config import source_enabled
from .reconcile_common import emit_finding as _emit
from .watermark import meta_float


# --- coverage gaps ------------------------------------------------------
def _detect_coverage_gaps(
    outbox, events, home, exec_rows, counts, when, config, capture_config, horizon
):
    """A durable row with no captured event proves a dropped capture."""
    captured = _captured_subjects(events)
    pending = _CoveragePending(outbox)
    session_rows = []
    parent_map = {}

    state_path = state_db_path(home)
    if source_enabled(capture_config, "state_db") and state_path.exists():
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
                outbox,
                session_rows,
                parent_map,
                captured,
                pending,
                counts,
                when,
                config,
                horizon,
            )
            _coverage_messages(
                outbox,
                conn,
                parent_map,
                session_started,
                captured,
                pending,
                counts,
                when,
                config,
                capture_config,
                horizon,
            )
            _coverage_model_usage(
                outbox,
                conn,
                parent_map,
                session_started,
                captured,
                pending,
                counts,
                when,
                config,
                horizon,
            )
        finally:
            conn.close()

    if source_enabled(capture_config, "cron"):
        # exec_rows was loaded once per reconcile pass; emit in one transaction.
        with outbox.batch():
            for r in exec_rows:
                if occurred_before(horizon, r["claimed_epoch"] or r["finished_at"]):
                    continue
                if r["id"] in captured["executions"]:
                    pending.clear("execution", r["id"])
                    continue
                _emit_coverage(
                    outbox,
                    counts,
                    when,
                    subject_type="execution",
                    subject_id=r["id"],
                    source_table="cron:executions.db",
                    correlation_id=r["job_id"],
                    grace=config.coverage_grace,
                )
    if source_enabled(capture_config, "kanban"):
        _coverage_kanban(
            outbox, home, captured, pending, counts, when, config, horizon
        )
    pending.flush()


def _coverage_sessions(
    outbox, rows, parent_map, captured, pending, counts, when, config, horizon
) -> None:
    # ``rows`` was fetched by the caller; emit findings in one transaction.
    with outbox.batch():
        for r in rows:
            if occurred_before(horizon, r["started_at"]):
                continue
            sid = r["id"]
            if sid in captured["sessions"]:
                pending.clear("session", sid)
                continue
            corr = root_session(sid, parent_map) or sid
            _emit_coverage(
                outbox,
                counts,
                when,
                subject_type="session",
                subject_id=sid,
                source_table="state.db:sessions",
                correlation_id=corr,
                session_id=sid,
                parent_session_id=r["parent_session_id"],
                grace=config.coverage_grace,
            )


def _coverage_messages(
    outbox,
    conn,
    parent_map,
    session_started,
    captured,
    pending,
    counts,
    when,
    config,
    capture_config,
    horizon,
) -> None:
    roles = tuple(
        role
        for role in ("user", "assistant", "tool")
        if role in capture_config.message_roles
    )
    if not roles:
        return
    columns = sqlite_table_columns(conn, "messages")
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
    # The durable rows are fetched above; emit findings in one transaction.
    with outbox.batch():
        for r in rows:
            sid = r["session_id"]
            if occurred_before(horizon, r["timestamp"]):
                continue
            if r["timestamp"] is None and occurred_before(
                horizon, session_started.get(sid)
            ):
                continue
            if r["id"] in captured["messages"]:
                pending.clear("message", str(r["id"]))
                continue
            corr = root_session(sid, parent_map) or sid
            _emit_coverage(
                outbox,
                counts,
                when,
                subject_type="message",
                subject_id=str(r["id"]),
                source_table="state.db:messages",
                correlation_id=corr,
                session_id=sid,
                grace=config.coverage_grace,
            )


def _coverage_model_usage(
    outbox,
    conn,
    parent_map,
    session_started,
    captured,
    pending,
    counts,
    when,
    config,
    horizon,
) -> None:
    if not sqlite_table_exists(conn, "session_model_usage"):
        return
    rows = conn.execute(
        "SELECT session_id, model, task FROM session_model_usage"
    ).fetchall()
    # The durable rows are fetched above; emit findings in one transaction.
    with outbox.batch():
        for r in rows:
            if occurred_before(horizon, session_started.get(r["session_id"])):
                continue
            key = (r["session_id"], r["model"], r["task"])
            subject_id = f"{r['session_id']}:{r['model']}:{r['task']}"
            if key in captured["model_usage"]:
                pending.clear("model_usage", subject_id)
                continue
            sid = r["session_id"]
            corr = root_session(sid, parent_map) or sid
            _emit_coverage(
                outbox,
                counts,
                when,
                subject_type="model_usage",
                subject_id=subject_id,
                source_table="state.db:session_model_usage",
                correlation_id=corr,
                session_id=sid,
                grace=config.coverage_grace,
            )


def _coverage_kanban(
    outbox, home, captured, pending, counts, when, config, horizon
) -> None:
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
        # The board rows are fetched (and the board connection closed) above;
        # emit this board's findings in one transaction.
        with outbox.batch():
            for r in tasks:
                if occurred_before(horizon, r["created_at"] or r["started_at"]):
                    continue
                if (board, r["id"]) in captured["tasks"]:
                    pending.clear("task", f"{board}:{r['id']}")
                    continue
                _emit_coverage(
                    outbox,
                    counts,
                    when,
                    subject_type="task",
                    subject_id=f"{board}:{r['id']}",
                    source_table=f"kanban:{board}:tasks",
                    correlation_id=r["id"],
                    session_id=r["session_id"],
                    grace=config.coverage_grace,
                )
            for r in runs:
                if occurred_before(horizon, r["started_at"] or r["ended_at"]):
                    continue
                if (board, r["id"]) in captured["task_runs"]:
                    pending.clear("task_run", f"{board}:{r['id']}")
                    continue
                _emit_coverage(
                    outbox,
                    counts,
                    when,
                    subject_type="task_run",
                    subject_id=f"{board}:{r['id']}",
                    source_table=f"kanban:{board}:task_runs",
                    correlation_id=r["task_id"],
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
    outbox,
    counts,
    when,
    *,
    subject_type,
    subject_id,
    source_table,
    correlation_id,
    grace,
    session_id=None,
    parent_session_id=None,
) -> None:
    if not _coverage_ready(outbox, subject_type, subject_id, when, grace):
        return
    _emit(
        outbox,
        counts,
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


_COVERAGE_PENDING_PREFIX = "reconcile:coverage_pending:"


def _coverage_pending_key(subject_type: str, subject_id: Any) -> str:
    return f"{_COVERAGE_PENDING_PREFIX}{subject_type}:{subject_id}"


class _CoveragePending:
    """The ``reconcile:coverage_pending:*`` grace markers, loaded once per pass.

    A full audit used to issue one ``DELETE FROM meta`` per already-captured
    durable row — tens of thousands of no-op DELETEs on a large home (issue
    #162). One SELECT loads the marker keys actually present (exact-prefix
    match, so watermark cursors and knowledge emit cursors in the same table
    are never touched); ``clear`` collects only those keys, and ``flush``
    deletes them in one batched transaction.
    """

    def __init__(self, outbox) -> None:
        self._outbox = outbox
        self._present = set(outbox.meta_keys_with_prefix(_COVERAGE_PENDING_PREFIX))
        self._cleared: list[str] = []

    def clear(self, subject_type: str, subject_id: Any) -> None:
        """Drop a captured subject's grace marker, if one exists."""
        key = _coverage_pending_key(subject_type, subject_id)
        if key in self._present:
            self._present.remove(key)
            self._cleared.append(key)

    def flush(self) -> None:
        """Delete the collected markers as one ``executemany`` batch."""
        if not self._cleared:
            return
        with self._outbox.batch():
            self._outbox.delete_meta_many(self._cleared)
        self._cleared.clear()


def _coverage_ready(
    outbox, subject_type: str, subject_id: Any, when: float, grace: float
) -> bool:
    """Wait through a capture tick before an absent durable row is a gap."""
    if grace <= 0:
        return True
    key = _coverage_pending_key(subject_type, subject_id)
    first_seen = meta_float(outbox, key)
    if first_seen is None:
        outbox.set_meta(key, repr(when))
        return False
    return when - first_seen >= grace


__all__ = ["_detect_coverage_gaps"]
