"""Legacy-schema tolerance for the cron and kanban stores (issue #107).

Completes the schema tolerance #106 added for state.db: an older Hermes home
whose cron `executions` or kanban `kanban.db` lacks a column or table must not
crash capture or reconcile.
"""

from __future__ import annotations

import sqlite3

from hermes_flight_recorder.collector import cron_db, kanban_db
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile


def _outbox(tmp_path) -> Outbox:
    ob = Outbox.open(tmp_path / "fr")
    ob.initialize()
    return ob


# --- cron ---------------------------------------------------------------
def _reduced_cron(hermes) -> None:
    (hermes / "cron").mkdir()
    db = sqlite3.connect(hermes / "cron" / "executions.db")
    # Missing source, pid, started_at, finished_at, error.
    db.executescript(
        "CREATE TABLE executions (id INTEGER PRIMARY KEY, job_id TEXT, status TEXT, claimed_at REAL);"
    )
    db.execute("INSERT INTO executions VALUES (1,'job-a','running',1000.0)")
    db.commit()
    db.close()


def test_cron_capture_tolerates_reduced_executions_schema(tmp_path):
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    _reduced_cron(hermes)
    ob = _outbox(tmp_path)

    counts = cron_db.poll(ob, hermes)  # must not raise

    assert counts.get("cron.run_claimed") == 1
    ob.close()


def test_cron_reconcile_tolerates_reduced_executions_schema(tmp_path):
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    _reduced_cron(hermes)
    ob = _outbox(tmp_path)

    reconcile(ob, hermes, now=2000.0, config=ReconcileConfig())  # must not raise
    ob.close()


# --- kanban -------------------------------------------------------------
def test_kanban_capture_tolerates_missing_task_runs_and_reduced_tasks(tmp_path):
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    db = sqlite3.connect(hermes / "kanban.db")  # legacy top-level board "default"
    db.executescript(
        """
        CREATE TABLE tasks (id TEXT, status TEXT, session_id TEXT);  -- missing metadata cols
        CREATE TABLE task_events (id INTEGER PRIMARY KEY, task_id TEXT, run_id TEXT,
            kind TEXT, created_at REAL);
        -- no task_runs table at all
        """
    )
    db.execute("INSERT INTO tasks VALUES ('t1','done','s1')")
    db.execute("INSERT INTO task_events VALUES (1,'t1',NULL,'created',1000.0)")
    db.execute("INSERT INTO task_events VALUES (2,'t1',NULL,'completed',1001.0)")
    db.commit()
    db.close()
    ob = _outbox(tmp_path)

    counts = kanban_db.poll(ob, hermes)  # must not raise

    assert counts.get("task.created") == 1
    assert counts.get("task.completed") == 1
    ob.close()


def test_kanban_missing_task_events_table_is_tolerated(tmp_path):
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    db = sqlite3.connect(hermes / "kanban.db")
    db.executescript("CREATE TABLE tasks (id TEXT, status TEXT, session_id TEXT);")
    db.commit()
    db.close()
    ob = _outbox(tmp_path)

    assert kanban_db.poll(ob, hermes) == {}  # no task_events → nothing, no crash
    ob.close()


# --- reconcile without a messages table ---------------------------------
def test_reconcile_tolerates_state_db_without_messages_table(tmp_path):
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    db = sqlite3.connect(hermes / "state.db")
    db.executescript(
        """
        CREATE TABLE sessions (id TEXT, source TEXT, parent_session_id TEXT,
            started_at REAL, ended_at REAL);
        """
    )
    db.execute("INSERT INTO sessions VALUES ('S','cli',NULL,1000.0,1001.0)")
    db.commit()
    db.close()
    ob = _outbox(tmp_path)

    reconcile(ob, hermes, now=2000.0, config=ReconcileConfig())  # must not raise
    ob.close()
