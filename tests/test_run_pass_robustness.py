"""run_pass must survive a transient store fault, not crash the whole capture pass.

The recorder polls several durable stores per tick. A momentary
``sqlite3.OperationalError`` ("database is locked" while Hermes checkpoints) or a
``PermissionError`` on one store must degrade to a skipped source for that tick —
never propagate out and drop every source ordered after it (the silent-drop class
the reliability audit flagged).
"""

from __future__ import annotations

import sqlite3

from hermes_flight_recorder.collector import (
    CAPTURE_HEARTBEAT_KEY,
    cron_db,
    knowledge_store,
    run_pass,
)
from hermes_flight_recorder.collector.health import read_health, source_health_key
from hermes_flight_recorder.collector.outbox import Outbox


def test_run_pass_tolerates_a_locked_store(tmp_path, monkeypatch):
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()

    def boom(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(cron_db, "poll", boom)

    errors: list[tuple[str, str]] = []
    # No Hermes home here, so the other durable sources raise their own OSErrors
    # (unable to open) — all of which must also be tolerated, not raised.
    totals = run_pass(
        ob,
        tmp_path / "no-such-hermes-home",
        on_source_error=lambda label, exc: errors.append((label, type(exc).__name__)),
    )

    assert isinstance(totals, dict)  # completed instead of raising
    assert ("cron", "OperationalError") in errors  # the locked store was tolerated


def test_run_pass_still_propagates_when_no_error_handler(tmp_path, monkeypatch):
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()

    def boom(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(cron_db, "poll", boom)

    # With no on_source_error, a tolerated store error still surfaces (fail-loud
    # for callers that want it, e.g. gate scripts). A missing Hermes home makes
    # the first durable source raise, which is enough to prove propagation.
    raised = False
    try:
        run_pass(ob, tmp_path / "no-such-hermes-home")
    except (OSError, sqlite3.Error):
        raised = True
    assert raised


def test_completed_pass_stamps_the_capture_heartbeat(tmp_path):
    """A pass that completes (even with every source degraded to a skip) stamps
    ``capture:last_success_at`` — the liveness proof the reconciler reads."""
    import time

    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()

    before = time.time()
    # No Hermes home, so every durable source skips; the pass still completes.
    run_pass(ob, tmp_path / "no-such-hermes-home", on_source_error=lambda *_: None)
    after = time.time()

    raw = ob.get_meta(CAPTURE_HEARTBEAT_KEY)
    assert raw is not None
    stamped = float(raw)
    assert before <= stamped <= after


def test_crashing_pass_does_not_stamp_the_heartbeat(tmp_path, monkeypatch):
    """A pass that raises (no error handler) is not a success and must leave the
    heartbeat untouched, so the reconciler still sees the loop as stalled."""
    def boom(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(cron_db, "poll", boom)

    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()

    try:
        run_pass(ob, tmp_path / "no-such-hermes-home")
    except (OSError, sqlite3.Error):
        pass

    assert ob.get_meta(CAPTURE_HEARTBEAT_KEY) is None


def test_source_failures_are_durable_and_reset_after_success(tmp_path, monkeypatch):
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()

    def boom(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(cron_db, "poll", boom)
    for _ in range(2):
        run_pass(
            ob,
            tmp_path / "missing",
            on_source_error=lambda *_: None,
            now=100.0,
        )

    failed = read_health(ob, source_health_key("cron"))
    assert failed["last_error_at"] == 100.0
    assert failed["consecutive_failures"] == 2
    assert "OperationalError" in failed["last_error"]

    monkeypatch.setattr(cron_db, "poll", lambda *args, **kwargs: {})
    run_pass(
        ob,
        tmp_path / "missing",
        on_source_error=lambda *_: None,
        now=200.0,
    )
    recovered = read_health(ob, source_health_key("cron"))
    assert recovered["last_success_at"] == 200.0
    assert recovered["last_error_at"] == 100.0
    assert recovered["consecutive_failures"] == 0


def test_knowledge_artifact_errors_set_source_failure(tmp_path, monkeypatch):
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()

    def partial_failure(*args, on_artifact_error=None, **kwargs):
        on_artifact_error("skill:test", PermissionError("denied"))
        return {"knowledge.record_written": 1}

    monkeypatch.setattr(knowledge_store, "poll", partial_failure)
    run_pass(
        ob,
        tmp_path / "missing",
        on_source_error=lambda *_: None,
        now=300.0,
    )

    state = read_health(ob, source_health_key("knowledge"))
    assert state["consecutive_failures"] == 1
    assert state["last_error_at"] == 300.0
    assert "PermissionError" in state["last_error"]


def test_degraded_knowledge_poll_keeps_counts_and_reports_every_error(
    tmp_path, monkeypatch
):
    """The uniform (counts, errors) path preserves the degraded-source contract:
    a poll that completes with tolerated per-item errors keeps its counts,
    reports each error through on_source_error, records the last one in source
    health — and never raises, even with no error handler."""
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()

    first = PermissionError("denied: skill one")
    second = PermissionError("denied: skill two")

    def partial_failure(*args, on_artifact_error=None, **kwargs):
        on_artifact_error("skill:one", first)
        on_artifact_error("skill:two", second)
        return {"knowledge.record_written": 3}

    monkeypatch.setattr(knowledge_store, "poll", partial_failure)

    reported: list[tuple[str, Exception]] = []
    totals = run_pass(
        ob,
        tmp_path / "missing",
        on_source_error=lambda label, exc: reported.append((label, exc)),
        now=400.0,
    )

    # The degraded poll's counts still land in the pass totals.
    assert totals.get("knowledge.record_written") == 3
    # Every tolerated per-item error reaches the handler, under the source label.
    assert [(label, exc) for label, exc in reported if label == "knowledge"] == [
        ("knowledge", first),
        ("knowledge", second),
    ]
    # Health records the failure (the last error), exactly as before.
    state = read_health(ob, source_health_key("knowledge"))
    assert state["consecutive_failures"] == 1
    assert state["last_error_at"] == 400.0
    assert "denied: skill two" in state["last_error"]
    assert "last_success_at" not in state

    # Without a handler, tolerated per-item errors of a completed poll still do
    # not propagate — cron alone raises on the missing home, knowledge does not.
    monkeypatch.setattr(cron_db, "poll", lambda *args, **kwargs: {})
    for module in ("state_db", "kanban_db", "gateway_log"):
        monkeypatch.setattr(
            __import__(
                f"hermes_flight_recorder.collector.{module}", fromlist=["poll"]
            ),
            "poll",
            lambda *args, **kwargs: {},
        )
    totals = run_pass(ob, tmp_path / "missing", now=500.0)
    assert totals.get("knowledge.record_written") == 3
    state = read_health(ob, source_health_key("knowledge"))
    assert state["consecutive_failures"] == 2
    assert state["last_error_at"] == 500.0


def test_capture_pass_reads_config_yaml_at_most_once(tmp_path, monkeypatch):
    """One capture pass resolves home_mode exactly once (issue #164).

    Before the fix, every durable-store poll re-read ``config.yaml`` — and
    ``gateway_log`` re-read it once per matched log line. ``run_pass`` now
    resolves it a single time and hands the string down.
    """
    from pathlib import Path

    home = tmp_path / "hermes"
    (home / "logs").mkdir(parents=True)
    (home / "config.yaml").write_text("terminal:\n  home_mode: profile\n")
    # Two matched failure lines: the per-line re-read was the worst offender.
    (home / "logs" / "agent.log").write_text(
        "2026-08-01 10:00:00,000 ERROR [S1] agent.conversation_loop: "
        "API call failed after 3 retries. HTTP 429 | provider=p model=m\n"
        "2026-08-01 10:00:01,000 ERROR [S2] agent.conversation_loop: "
        "API call failed after 3 retries. HTTP 500 | provider=p model=m\n"
    )

    config_reads: list[Path] = []
    real_read_text = Path.read_text

    def counting_read_text(self, *args, **kwargs):
        if self.name == "config.yaml":
            config_reads.append(self)
        return real_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", counting_read_text)

    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    totals = run_pass(ob, home, on_source_error=lambda *_: None)

    assert totals.get("model.call_failed") == 2  # the log lines were captured
    assert len(config_reads) == 1
