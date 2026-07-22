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
    run_pass,
)
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
