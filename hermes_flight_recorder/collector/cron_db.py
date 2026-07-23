"""Durable-state adapter for Hermes cron.

Poll ``cron/executions.db`` and the ticker heartbeat files read-only and
emit envelope v1 records. Grounded in a real probe (see issue #5):

- An ``executions`` row is ``status='completed'`` on success; ``error`` holds
  failure text; ``job_id`` links to the job; timestamps are ISO 8601 strings.
  Emit ``cron.run_claimed`` (claimed_at) and ``cron.run_finished`` (finished_at).
- ``executions.id`` is a hex UUID (not monotonic), so dedup by id rather than
  a numeric cursor.
- ``ticker_heartbeat`` is the scheduler-liveness signal. A stale heartbeat
  means the whole scheduler is down; the reconciler judges that, this adapter
  just reports the heartbeat.

Missed-run reconstruction from ``jobs.json`` lives in the reconciler (#6).
This adapter never writes to the cron store.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

from ._common import (
    occurred_before,
    append_and_count,
    build_record,
    executions_db_path,
    open_sqlite_read_only,
    read_float,
    read_home_mode,
    resolve_hermes_home,
    runtime_stamp,
    ticker_heartbeat_path,
    ticker_last_success_path,
    to_epoch,
)


def poll(
    outbox: Any, hermes_home: str | Path | None = None, *, since: float | None = None
) -> dict[str, int]:
    """One read-only poll pass over the cron store. Returns per-type counts.

    ``since`` is the capture horizon (``install --no-backfill``); executions
    claimed before it are skipped so history is not backfilled.
    """
    home = resolve_hermes_home(hermes_home)
    home_mode = read_home_mode(hermes_home)
    counts: dict[str, int] = defaultdict(int)
    _poll_executions(outbox, home, counts, home_mode, since)
    _poll_heartbeat(outbox, home, counts, home_mode)
    return dict(counts)


def _poll_executions(outbox, home: Path, counts, home_mode, since=None) -> None:
    db_path = executions_db_path(home)
    if not db_path.exists():
        return
    conn = open_sqlite_read_only(db_path)
    try:
        rows = conn.execute(
            "SELECT id, job_id, source, pid, status, claimed_at, started_at, "
            "finished_at, error FROM executions"
        ).fetchall()
    finally:
        conn.close()

    for r in rows:
        if occurred_before(since, r["claimed_at"]):
            continue  # claimed before the capture horizon (no backfill)
        exid, job = r["id"], r["job_id"]
        claimed = to_epoch(r["claimed_at"]) or 0.0
        rt = runtime_stamp("cron", home_mode=home_mode)

        record = build_record(
            event_type="cron.run_claimed",
            occurred_at=claimed,
            source="cron:executions.db",
            capture_method="poll:cron:executions.db",
            runtime=rt,
            correlation_id=job,
            payload={
                "job_id": job,
                "execution_id": exid,
                "run_source": r["source"],
                "pid": r["pid"],
                "status": r["status"],
            },
        )
        append_and_count(outbox, counts, record, dedup_key=f"cron:claimed:{exid}")

        if r["finished_at"] is None:
            continue
        ok = r["status"] == "completed" and not r["error"]
        record = build_record(
            event_type="cron.run_finished",
            occurred_at=to_epoch(r["finished_at"]) or claimed,
            source="cron:executions.db",
            capture_method="poll:cron:executions.db",
            runtime=rt,
            correlation_id=job,
            payload={
                "job_id": job,
                "execution_id": exid,
                "status": r["status"],
                "ok": ok,
                "started_at": r["started_at"],
                "finished_at": r["finished_at"],
            },
        )
        append_and_count(
            outbox,
            counts,
            record,
            content=r["error"] if r["error"] else None,
            dedup_key=f"cron:finished:{exid}",
        )


def _poll_heartbeat(outbox, home: Path, counts, home_mode) -> None:
    hb = read_float(ticker_heartbeat_path(home))
    if hb is None:
        return
    last_success = read_float(ticker_last_success_path(home))
    record = build_record(
        event_type="cron.ticker_heartbeat",
        occurred_at=hb,
        source="cron:heartbeat",
        capture_method="poll:cron:heartbeat",
        runtime=runtime_stamp("cron", home_mode=home_mode),
        correlation_id="cron:ticker",
        payload={"heartbeat": hb, "last_success": last_success},
    )
    append_and_count(outbox, counts, record, dedup_key=f"cron:heartbeat:{hb}")
