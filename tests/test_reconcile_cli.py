"""Tests for the ``reconcile`` CLI subcommand (issue #6 exit criterion).

Drives ``hermes_flight_recorder.cli.main(["reconcile", ...])`` end to end through
``capsys`` rather than calling the reconciler function directly, so these
assert on the CLI's own contract: the not-initialized exit code and stderr
hint, the "reconciled N new finding(s)" summary line, the per-event-type
lines, and tolerance of missing durable stores.

The CLI's ``reconcile`` subcommand has no ``--now``/config flags — it always
calls ``reconcile(outbox, hermes_home)`` with ``now=None``, which falls back
to the real wall clock inside ``reconcile.py``. So, unlike the direct
reconciler unit tests, these cases are built to be robust to *when* the test
happens to run rather than pinned to a fixed epoch:

- Sequence-gap detection does not consult ``now``. Coverage-gap detection
  uses a short confirmation window, which these tests drive in two passes.
- The one scenario that does depend on an age window (a stale open session)
  anchors ``started_at`` using the real ``time.time()`` offset by a margin
  safely past (or under) the default ``ReconcileConfig`` threshold, so the
  outcome cannot flip due to test latency.
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

from hermes_flight_recorder import cli
from hermes_flight_recorder.collector._common import build_record
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile  # noqa: F401

# A fixed epoch anchor, mirrored from tests/test_reconcile.py's style, used
# as the default `occurred_at` for events whose timestamp value is
# irrelevant to the detector under test (sequence-gap detection never
# inspects `occurred_at`).
B = 1784415000.0


# --- fixtures / helpers ---------------------------------------------------
def new_outbox(flight_recorder_home: Path) -> Outbox:
    """Open and initialize an Outbox, matching tests/test_reconcile.py."""
    ob = Outbox.open(flight_recorder_home)
    ob.initialize()
    return ob


def make_initialized_flight_recorder_home(flight_recorder_home: Path) -> Path:
    """Initialize an outbox at ``flight_recorder_home`` and close it, so the CLI's
    own ``Outbox.open()`` call reopens the same on-disk installation.
    """
    ob = new_outbox(flight_recorder_home)
    ob.close()
    return flight_recorder_home


def append_raw(flight_recorder_home: Path, event_type: str, **over) -> None:
    """Append one minimal valid producer event straight to the outbox,
    through a short-lived connection (mirrors the CLI's own lifecycle).
    """
    ob = Outbox.open(flight_recorder_home)
    try:
        rec = build_record(
            event_type=event_type,
            occurred_at=over.pop("occurred_at", B),
            source=over.pop("source", "hook:test"),
            capture_method=over.pop("capture_method", "hook:test"),
            runtime={"kind": "cli", "engine": "standard"},
            correlation_id=over.pop("correlation_id", "corr"),
            payload=over.pop("payload", {}),
            **over,
        )
        ob.append(rec)
    finally:
        ob.close()


def drop_sequence(flight_recorder_home: Path, seq: int) -> None:
    """Simulate a dropped capture by deleting a producer_sequence row."""
    conn = sqlite3.connect(flight_recorder_home / "outbox.sqlite")
    try:
        conn.execute("DELETE FROM events WHERE producer_sequence=?", (seq,))
        conn.commit()
    finally:
        conn.close()


def make_state_db(hermes_home: Path, session_rows: list[tuple]) -> None:
    """A minimal state.db with just the sessions table (plus the empty
    messages / session_model_usage tables the coverage detector queries).

    The sessions column list and order match exactly what
    ``_terminals_sessions`` selects: (id, source, parent_session_id,
    started_at, ended_at, expiry_finalized, profile_name).
    """
    db = sqlite3.connect(hermes_home / "state.db")
    db.executescript(
        """
        CREATE TABLE sessions (id TEXT, source TEXT, parent_session_id TEXT,
            started_at REAL, ended_at REAL, expiry_finalized INT, profile_name TEXT);
        CREATE TABLE messages (id INTEGER PRIMARY KEY, session_id TEXT, role TEXT);
        CREATE TABLE session_model_usage (session_id TEXT, model TEXT, task TEXT);
        """
    )
    db.executemany("INSERT INTO sessions VALUES (?,?,?,?,?,?,?)", session_rows)
    db.commit()
    db.close()


# --- not-initialized -------------------------------------------------------
def test_reconcile_not_initialized_exits_2_with_stderr_hint(tmp_path, capsys):
    bridge = tmp_path / "bridge-uninit"  # Outbox.open() creates the dir/file
    hermes_home = tmp_path / "hermes"  # never even created; irrelevant here

    code = cli.main(
        ["reconcile", "--flight-recorder-home", str(bridge), "--hermes-home", str(hermes_home)]
    )
    captured = capsys.readouterr()

    assert code == 2
    assert captured.out == ""
    assert "install" in captured.err
    assert "not initialized" in captured.err.lower()


# --- normal run: summary + per-event-type lines ---------------------------
def test_reconcile_normal_run_reports_summary_and_gap_line(tmp_path, capsys):
    bridge = tmp_path / "bridge"
    make_initialized_flight_recorder_home(bridge)
    for _ in range(5):
        append_raw(bridge, "session.created")
    drop_sequence(bridge, 3)  # one dropped capture -> exactly one gap

    hermes_home = tmp_path / "hermes-missing"  # doesn't exist; tolerated

    code = cli.main(
        ["reconcile", "--flight-recorder-home", str(bridge), "--hermes-home", str(hermes_home)]
    )
    captured = capsys.readouterr()

    assert code == 0
    assert "reconciled 1 new finding(s)" in captured.out
    assert "  reconcile.gap_detected: 1" in captured.out


def test_reconcile_is_idempotent_across_cli_invocations(tmp_path, capsys):
    bridge = tmp_path / "bridge-idem"
    make_initialized_flight_recorder_home(bridge)
    for _ in range(3):
        append_raw(bridge, "session.created")
    drop_sequence(bridge, 2)
    hermes_home = tmp_path / "hermes-missing2"

    code1 = cli.main(
        ["reconcile", "--flight-recorder-home", str(bridge), "--hermes-home", str(hermes_home)]
    )
    out1 = capsys.readouterr().out
    assert code1 == 0
    assert "reconciled 1 new finding(s)" in out1

    code2 = cli.main(
        ["reconcile", "--flight-recorder-home", str(bridge), "--hermes-home", str(hermes_home)]
    )
    out2 = capsys.readouterr().out
    assert code2 == 0
    assert "reconciled 0 new finding(s)" in out2  # dedup: nothing new the 2nd time


# --- missing durable stores are tolerated ---------------------------------
def test_reconcile_missing_state_db_and_cron_dir_tolerated(tmp_path, capsys):
    bridge = tmp_path / "bridge-empty-home"
    make_initialized_flight_recorder_home(bridge)
    hermes_home = tmp_path / "hermes-empty"
    hermes_home.mkdir()  # exists, but no state.db and no cron/ subdir

    code = cli.main(
        ["reconcile", "--flight-recorder-home", str(bridge), "--hermes-home", str(hermes_home)]
    )
    captured = capsys.readouterr()

    assert code == 0
    assert "reconciled 0 new finding(s)" in captured.out
    non_empty_lines = [ln for ln in captured.out.splitlines() if ln.strip()]
    assert len(non_empty_lines) == 1  # summary line only, no per-type lines


def test_reconcile_nonexistent_hermes_home_tolerated(tmp_path, capsys):
    bridge = tmp_path / "bridge-no-home-dir"
    make_initialized_flight_recorder_home(bridge)
    hermes_home = tmp_path / "does-not-exist-at-all"  # never created

    code = cli.main(
        ["reconcile", "--flight-recorder-home", str(bridge), "--hermes-home", str(hermes_home)]
    )
    captured = capsys.readouterr()

    assert code == 0
    assert "reconciled 0 new finding(s)" in captured.out


# --- state.db-backed detectors through the CLI -----------------------------
def test_reconcile_reports_coverage_gap_for_uncaptured_session(tmp_path, capsys):
    bridge = tmp_path / "bridge-cover"
    make_initialized_flight_recorder_home(bridge)
    hermes_home = tmp_path / "hermes-cover"
    hermes_home.mkdir()
    # started "now" (real wall clock): comfortably inside the default
    # session_terminal_timeout window (12h), so only the coverage-gap
    # fires, not a terminal-missing.
    recent_started = time.time()
    make_state_db(hermes_home, [("S", "cli", None, recent_started, None, 0, None)])

    code = cli.main(
        ["reconcile", "--flight-recorder-home", str(bridge), "--hermes-home", str(hermes_home)]
    )
    captured = capsys.readouterr()

    assert code == 0
    assert "reconciled 0 new finding(s)" in captured.out

    outbox = Outbox.open(bridge)
    outbox.set_meta(
        "reconcile:coverage_pending:session:S",
        repr(time.time() - ReconcileConfig().coverage_grace - 1),
    )
    outbox.close()
    code = cli.main(
        ["reconcile", "--flight-recorder-home", str(bridge), "--hermes-home", str(hermes_home)]
    )
    captured = capsys.readouterr()

    assert code == 0
    assert "reconciled 1 new finding(s)" in captured.out
    assert "  reconcile.gap_detected: 1" in captured.out
    assert "reconcile.terminal_missing" not in captured.out


def test_reconcile_reports_terminal_missing_for_stale_session(tmp_path, capsys):
    bridge = tmp_path / "bridge-term"
    make_initialized_flight_recorder_home(bridge)
    hermes_home = tmp_path / "hermes-term"
    hermes_home.mkdir()

    # Anchor on the real default threshold plus a large safety margin, so
    # this can never flip regardless of how long the test takes to run.
    default_timeout = ReconcileConfig().session_terminal_timeout
    stale_started = time.time() - default_timeout - 3600.0
    make_state_db(hermes_home, [("S", "cli", None, stale_started, None, 0, None)])

    code = cli.main(
        ["reconcile", "--flight-recorder-home", str(bridge), "--hermes-home", str(hermes_home)]
    )
    captured = capsys.readouterr()

    assert code == 0
    # Terminal detection is immediate. Coverage detection waits through the
    # capture grace and reports the same durable row on the next pass.
    assert "reconciled 1 new finding(s)" in captured.out
    assert "  reconcile.terminal_missing: 1" in captured.out

    outbox = Outbox.open(bridge)
    outbox.set_meta(
        "reconcile:coverage_pending:session:S",
        repr(time.time() - ReconcileConfig().coverage_grace - 1),
    )
    outbox.close()
    code = cli.main(
        ["reconcile", "--flight-recorder-home", str(bridge), "--hermes-home", str(hermes_home)]
    )
    captured = capsys.readouterr()

    assert code == 0
    assert "reconciled 1 new finding(s)" in captured.out
    assert "  reconcile.gap_detected: 1" in captured.out
    assert "reconcile.terminal_missing" not in captured.out
