"""Shared test helpers (issue #158).

Plain functions used across the suite. Test modules import them directly
(``from helpers import ...``); pytest puts ``tests/`` on ``sys.path`` when it
collects this directory. Fixtures live in ``tests/conftest.py``.

The legacy-schema tests (``test_legacy_schema_cron_kanban.py``) intentionally
keep their own hand-written DDL and do not use these builders.
"""

from __future__ import annotations

import datetime
import json
import sqlite3
from pathlib import Path
from typing import Any

from hermes_flight_recorder.collector._common import build_record
from hermes_flight_recorder.collector.outbox import Outbox

# A fixed epoch anchor and a US-Central-like offset, mirroring the real cron
# store. Every deterministic-clock test in the suite anchors on this value.
B = 1784415000.0
TZ = datetime.timezone(datetime.timedelta(hours=-5))


def iso(epoch: float) -> str:
    return datetime.datetime.fromtimestamp(epoch, TZ).isoformat()


# --- outbox ---------------------------------------------------------------
def new_outbox(tmp_path: Path, subdir: str | None = "bridge") -> Outbox:
    """Open and initialize an Outbox for a test.

    Most tests keep the outbox in a ``bridge/`` subdirectory so ``tmp_path``
    stays free for a fake Hermes home. Pass ``subdir=None`` to open
    ``tmp_path`` itself (sync/transport/hook tests that reopen the same
    directory or store sync config and keys beside the outbox).
    """
    home = tmp_path / subdir if subdir else tmp_path
    ob = Outbox.open(home)
    ob.initialize()
    return ob


# --- producer events ------------------------------------------------------
def append_event(ob: Outbox, event_type: str, **over: Any) -> dict:
    """Append a minimal valid producer event straight to the outbox.

    Defaults mirror the reconcile-family tests (``hook:test`` capture).
    Remaining keyword arguments pass through to ``build_record``
    (``session_id``, ``parent_session_id``, ``invocation_id``, ``partial``,
    ...).
    """
    content = over.pop("content", None)
    rec = build_record(
        event_type=event_type,
        occurred_at=over.pop("occurred_at", B),
        source=over.pop("source", "hook:test"),
        capture_method=over.pop("capture_method", "hook:test"),
        runtime=over.pop("runtime", {"kind": "cli", "engine": "standard"}),
        correlation_id=over.pop("correlation_id", "corr"),
        payload=over.pop("payload", None) or {},
        **over,
    )
    return ob.append(rec, content=content)


def add(ob: Outbox, event_type: str, **over: Any) -> dict:
    """Observe-family variant of :func:`append_event` (``test`` capture)."""
    over.setdefault("source", "test")
    over.setdefault("capture_method", "test")
    return append_event(ob, event_type, **over)


# --- fake Hermes state.db -------------------------------------------------
# Column sets seen in real Hermes homes. MIN is the subset the reconciler
# reads; FULL mirrors the state adapter's probe fixtures.
SESSIONS_MIN = (
    "id TEXT, source TEXT, parent_session_id TEXT,\n"
    "    started_at REAL, ended_at REAL, expiry_finalized INT, profile_name TEXT"
)
SESSIONS_FULL = (
    "id TEXT, source TEXT, parent_session_id TEXT, model TEXT,\n"
    "    message_count INT, tool_call_count INT, input_tokens INT, output_tokens INT,\n"
    "    estimated_cost_usd REAL, started_at REAL, ended_at REAL, end_reason TEXT,\n"
    "    profile_name TEXT, expiry_finalized INT"
)
MESSAGES_MIN = "id INTEGER PRIMARY KEY, session_id TEXT, role TEXT"
MESSAGES_FULL = (
    "id INTEGER PRIMARY KEY, session_id TEXT, role TEXT,\n"
    "    tool_name TEXT, tool_call_id TEXT, effect_disposition TEXT, content TEXT,\n"
    "    timestamp REAL, finish_reason TEXT"
)
USAGE_MIN = "session_id TEXT, model TEXT, task TEXT"
USAGE_FULL = (
    "session_id TEXT, model TEXT, task TEXT,\n"
    "    api_call_count INT, input_tokens INT, output_tokens INT, cache_read_tokens INT,\n"
    "    reasoning_tokens INT, estimated_cost_usd REAL, cost_status TEXT, last_seen REAL"
)
ASYNC_DELEGATIONS_DDL = (
    "CREATE TABLE async_delegations (delegation_id TEXT, origin_session TEXT,\n"
    "    parent_session_id TEXT, state TEXT, delivery_state TEXT,\n"
    "    owner_pid INT, dispatched_at REAL, event_json TEXT, result_json TEXT);"
)


def make_state_db(
    home: Path,
    *,
    sessions=(),
    messages=(),
    model_usage=(),
    sessions_columns: str = SESSIONS_MIN,
    messages_columns: str = MESSAGES_MIN,
    usage_columns: str = USAGE_MIN,
    extra_ddl: str = "",
    extra_rows: dict[str, list[tuple]] | None = None,
) -> None:
    """Build a fake Hermes ``state.db`` under ``home``.

    Each rows argument is a list of column-value tuples matching the column
    order of the chosen ``*_columns`` DDL. ``extra_ddl`` appends further
    ``CREATE TABLE`` statements; ``extra_rows`` maps table name to rows.
    """
    db = sqlite3.connect(Path(home) / "state.db")
    db.executescript(
        f"CREATE TABLE sessions ({sessions_columns});\n"
        f"CREATE TABLE messages ({messages_columns});\n"
        f"CREATE TABLE session_model_usage ({usage_columns});\n"
        + extra_ddl
    )
    tables = {
        "sessions": sessions,
        "messages": messages,
        "session_model_usage": model_usage,
        **(extra_rows or {}),
    }
    for table, rows in tables.items():
        rows = list(rows)
        if not rows:
            continue
        width = len(db.execute(f"PRAGMA table_info({table})").fetchall())
        marks = ",".join("?" * width)
        db.executemany(f"INSERT INTO {table} VALUES ({marks})", rows)
    db.commit()
    db.close()


# --- fake Hermes cron store -----------------------------------------------
def _executions_db(cron: Path, rows) -> None:
    """rows: (exec_id, job_id, status, claimed_at_iso, started_at_iso, finished_at_iso)."""
    db = sqlite3.connect(cron / "executions.db")
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


def _jobs_json(cron: Path, jobs) -> None:
    (cron / "jobs.json").write_text(json.dumps({"jobs": jobs}))


def _interval_job(job_id: str, *, minutes, created, extra=None) -> dict:
    job = {
        "id": job_id,
        "enabled": True,
        "state": "scheduled",
        "created_at": iso(created),
        "schedule": {"kind": "interval", "minutes": minutes},
        "repeat": {"times": None, "completed": 0},
    }
    if extra:
        job.update(extra)
    return job
