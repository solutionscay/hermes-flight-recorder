"""Reconcile install-horizon (issue #109).

A fresh install over a long-lived Hermes home must not emit
``reconcile.terminal_missing`` (or ``cron.run_missed``) findings for work that
started before the recorder existed. The horizon is the ``installed_at`` marker
stamped at install, falling back to the earliest ``recorded_at``.
"""

from __future__ import annotations

import sqlite3

from hermes_flight_recorder.collector._common import INSTALLED_AT_META_KEY
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile

HORIZON = 1_700_000_000.0  # a fixed "install" epoch
DAY = 86_400.0


def _outbox(tmp_path, *, installed_at: float | None = HORIZON) -> Outbox:
    ob = Outbox.open(tmp_path / "fr")
    ob.initialize()
    if installed_at is not None:
        ob.set_meta(INSTALLED_AT_META_KEY, repr(installed_at))
    return ob


def _sessions_db(hermes, rows) -> None:
    db = sqlite3.connect(hermes / "state.db")
    db.executescript(
        """
        CREATE TABLE sessions (id TEXT, source TEXT, parent_session_id TEXT,
            model TEXT, message_count INT, tool_call_count INT, input_tokens INT,
            output_tokens INT, estimated_cost_usd REAL, started_at REAL,
            ended_at REAL, end_reason TEXT, profile_name TEXT, expiry_finalized INT);
        CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT);
        CREATE TABLE session_model_usage (session_id TEXT, model TEXT, task TEXT);
        """
    )
    for sid, started in rows:
        db.execute(
            "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (sid, "subagent", None, "m", 0, 0, 0, 0, 0.0, started, None, None, "default", 0),
        )
    db.commit()
    db.close()


def _flagged(ob) -> set[str]:
    return {
        e["payload"].get("subject_id")
        for e in ob.iter_events()
        if e["payload"]["event_type"] == "reconcile.terminal_missing"
    }


def test_pre_install_session_not_flagged_post_install_is(tmp_path):
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    _sessions_db(hermes, [("OLD", HORIZON - 10 * DAY), ("NEW", HORIZON + 10)])
    ob = _outbox(tmp_path)

    reconcile(ob, hermes, now=HORIZON + 100 * DAY, config=ReconcileConfig())

    flagged = _flagged(ob)
    assert "OLD" not in flagged  # started before install — suppressed
    assert "NEW" in flagged  # started after install, never ended — flagged
    ob.close()


def test_all_flagged_when_no_horizon(tmp_path):
    # No installed_at marker and no captured events → horizon 0.0 → pre-#109
    # behavior (judge the whole store). Both unended sessions are flagged.
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    _sessions_db(hermes, [("OLD", HORIZON - 10 * DAY), ("NEW", HORIZON + 10)])
    ob = _outbox(tmp_path, installed_at=None)

    reconcile(ob, hermes, now=HORIZON + 100 * DAY, config=ReconcileConfig())

    assert _flagged(ob) == {"OLD", "NEW"}
    ob.close()


def test_install_stamps_the_horizon(tmp_path):
    # The real `install` path stamps installed_at, so a reconcile afterward has
    # a live horizon with no manual meta write.
    from hermes_flight_recorder.collector import lifecycle

    hermes = tmp_path / "hermes"
    hermes.mkdir()
    (hermes / "config.yaml").write_text("terminal:\n  home_mode: auto\n")
    fr = lifecycle.install(None, str(hermes), log=lambda *a: None)
    ob = Outbox.open(fr)
    assert ob.get_meta(INSTALLED_AT_META_KEY) is not None
    ob.close()
