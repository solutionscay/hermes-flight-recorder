"""Scale checks for incremental collector queries (issue #134)."""

from __future__ import annotations

import sqlite3

from hermes_flight_recorder.collector import cron_db, state_db
from hermes_flight_recorder.collector.outbox import Outbox


def _cron_history(root, rows: int):
    hermes = root / "hermes"
    database = hermes / "cron" / "executions.db"
    database.parent.mkdir(parents=True)
    conn = sqlite3.connect(database)
    conn.execute(
        "CREATE TABLE executions (id TEXT, job_id TEXT, source TEXT, pid INT, "
        "status TEXT, claimed_at REAL, started_at REAL, finished_at REAL, error TEXT)"
    )
    conn.executemany(
        "INSERT INTO executions VALUES (?, 'job', 'test', 1, 'running', ?, NULL, NULL, NULL)",
        ((f"run-{row}", float(row)) for row in range(1, rows + 2)),
    )
    conn.commit()
    conn.close()

    outbox = Outbox.open(root / "recorder")
    outbox.initialize()
    outbox.set_cursor("cron.db:executions:v1", rows)
    return hermes, outbox


def test_cron_poll_work_does_not_grow_with_old_history(tmp_path, monkeypatch):
    """SQLite work stays bounded when only one row follows the watermark."""
    small_home, small_outbox = _cron_history(tmp_path / "small", 100)
    large_home, large_outbox = _cron_history(tmp_path / "large", 50_000)
    steps: dict[str, int] = {"small": 0, "large": 0}
    real_open = cron_db.open_sqlite_read_only

    def measured_open(path):
        conn = real_open(path)
        label = "large" if "large" in path.parts else "small"

        def count_step():
            steps[label] += 1
            return 0

        conn.set_progress_handler(count_step, 1)
        return conn

    monkeypatch.setattr(cron_db, "open_sqlite_read_only", measured_open)

    assert cron_db.poll(small_outbox, small_home) == {"cron.run_claimed": 33}
    assert cron_db.poll(large_outbox, large_home) == {"cron.run_claimed": 33}

    assert steps["large"] <= steps["small"] + 50
    small_outbox.close()
    large_outbox.close()


def _state_history(root, rows: int):
    hermes = root / "hermes"
    hermes.mkdir(parents=True)
    conn = sqlite3.connect(hermes / "state.db")
    conn.executescript(
        """
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY, source TEXT, parent_session_id TEXT, model TEXT,
            message_count INT, tool_call_count INT, started_at REAL, ended_at REAL,
            end_reason TEXT, profile_name TEXT, expiry_finalized INT
        );
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY, session_id TEXT, role TEXT, content TEXT,
            timestamp REAL, finish_reason TEXT
        );
        CREATE TABLE session_model_usage (
            session_id TEXT, model TEXT, task TEXT, api_call_count INT,
            input_tokens INT, output_tokens INT, cache_read_tokens INT,
            reasoning_tokens INT, estimated_cost_usd REAL, cost_status TEXT,
            last_seen REAL
        );
        CREATE INDEX usage_session_id ON session_model_usage(session_id);
        CREATE TABLE async_delegations (
            delegation_id TEXT, origin_session TEXT, parent_session_id TEXT,
            state TEXT, delivery_state TEXT, owner_pid INT, dispatched_at REAL,
            event_json TEXT, result_json TEXT
        );
        """
    )
    conn.executemany(
        "INSERT INTO sessions VALUES (?, 'cli', NULL, 'model', 1, 0, ?, ?, 'done', NULL, 1)",
        ((f"session-{row}", float(row), float(row)) for row in range(1, rows + 2)),
    )
    conn.executemany(
        "INSERT INTO messages VALUES (?, ?, 'user', 'request', ?, NULL)",
        ((row, f"session-{row}", float(row)) for row in range(1, rows + 2)),
    )
    conn.executemany(
        "INSERT INTO session_model_usage VALUES "
        "(?, 'model', '', 1, 1, 1, 0, 0, 0, 'estimated', ?)",
        ((f"session-{row}", float(row)) for row in range(1, rows + 2)),
    )
    conn.executemany(
        "INSERT INTO async_delegations VALUES "
        "(?, ?, NULL, 'sent', 'pending', 1, ?, '{}', NULL)",
        (
            (f"delegation-{row}", f"session-{row}", float(row))
            for row in range(1, rows + 2)
        ),
    )
    conn.commit()
    conn.close()

    outbox = Outbox.open(root / "recorder")
    outbox.initialize()
    outbox.set_cursor("state.db:sessions:v1", rows)
    outbox.set_cursor("state.db:messages:v2", rows)
    outbox.set_cursor("state.db:delegations:v1", rows)
    outbox.set_meta("state.db:model-usage-state-version", "delta-v1")
    return hermes, outbox


def test_state_poll_work_does_not_grow_with_old_history(tmp_path, monkeypatch):
    small_home, small_outbox = _state_history(tmp_path / "small-state", 1000)
    large_home, large_outbox = _state_history(tmp_path / "large-state", 50_000)
    steps: dict[str, int] = {"small": 0, "large": 0}
    real_open = state_db.open_sqlite_read_only

    def measured_open(path):
        conn = real_open(path)
        label = "large" if "large-state" in path.parts else "small"

        def count_step():
            steps[label] += 1
            return 0

        conn.set_progress_handler(count_step, 1)
        return conn

    monkeypatch.setattr(state_db, "open_sqlite_read_only", measured_open)

    small_counts = state_db.poll(small_outbox, small_home)
    large_counts = state_db.poll(large_outbox, large_home)

    assert large_counts == small_counts
    assert steps["large"] <= steps["small"] + 100
    small_outbox.close()
    large_outbox.close()
