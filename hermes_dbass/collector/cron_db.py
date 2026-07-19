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

import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any

from ._common import build_record, resolve_hermes_home, runtime_stamp, to_epoch


def poll(outbox: Any, hermes_home: str | Path | None = None) -> dict[str, int]:
    """One read-only poll pass over the cron store. Returns per-type counts."""
    cron_dir = resolve_hermes_home(hermes_home) / "cron"
    counts: dict[str, int] = defaultdict(int)
    _poll_executions(outbox, cron_dir, counts)
    _poll_heartbeat(outbox, cron_dir, counts)
    return dict(counts)


def _poll_executions(outbox, cron_dir: Path, counts) -> None:
    db_path = cron_dir / "executions.db"
    if not db_path.exists():
        return
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT id, job_id, source, pid, status, claimed_at, started_at, "
            "finished_at, error FROM executions"
        ).fetchall()
    finally:
        conn.close()

    for r in rows:
        exid, job = r["id"], r["job_id"]
        claimed = to_epoch(r["claimed_at"]) or 0.0
        rt = runtime_stamp("cron")

        outbox.append(
            build_record(
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
            ),
            dedup_key=f"cron:claimed:{exid}",
        )
        if outbox.last_append_created:
            counts["cron.run_claimed"] += 1

        if r["finished_at"] is None:
            continue
        ok = r["status"] == "completed" and not r["error"]
        outbox.append(
            build_record(
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
            ),
            content=r["error"] if r["error"] else None,
            dedup_key=f"cron:finished:{exid}",
        )
        if outbox.last_append_created:
            counts["cron.run_finished"] += 1


def _poll_heartbeat(outbox, cron_dir: Path, counts) -> None:
    hb_file = cron_dir / "ticker_heartbeat"
    if not hb_file.exists():
        return
    hb = _read_float(hb_file)
    if hb is None:
        return
    last_success = _read_float(cron_dir / "ticker_last_success")
    outbox.append(
        build_record(
            event_type="cron.ticker_heartbeat",
            occurred_at=hb,
            source="cron:heartbeat",
            capture_method="poll:cron:heartbeat",
            runtime=runtime_stamp("cron"),
            correlation_id="cron:ticker",
            payload={"heartbeat": hb, "last_success": last_success},
        ),
        dedup_key=f"cron:heartbeat:{hb}",
    )
    if outbox.last_append_created:
        counts["cron.ticker_heartbeat"] += 1


def _read_float(path: Path) -> float | None:
    if not path.exists():
        return None
    text = path.read_text().strip()
    try:
        return float(text) if text else None
    except ValueError:
        return None
