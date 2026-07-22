"""Tests for the ``status`` CLI subcommand.

The on-demand health readout — the human counterpart to the
``reconcile.capture_stale`` alert. Store-only (no Hermes home, no network), so
a cron/monitor can gate on the exit code: 0 healthy, 1 unhealthy (capture stale
or never recorded a success), 2 not initialized.

Driven through ``hermes_flight_recorder.cli.main(["status", ...])`` with
``capsys``, asserting on the CLI's own contract (verdict text + exit code). The
capture heartbeat is set on the outbox directly and anchored on the real
``time.time()`` offset by a safe margin past/under the default threshold, so the
outcome cannot flip due to test latency.
"""

from __future__ import annotations

import time
from pathlib import Path

from hermes_flight_recorder import cli
from hermes_flight_recorder.collector import CAPTURE_HEARTBEAT_KEY
from hermes_flight_recorder.collector._common import build_record
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig

THRESHOLD = ReconcileConfig().capture_stale_after


def init_home(flight_recorder_home: Path, heartbeat: float | None = None) -> Path:
    ob = Outbox.open(flight_recorder_home)
    ob.initialize()
    if heartbeat is not None:
        ob.set_meta(CAPTURE_HEARTBEAT_KEY, repr(heartbeat))
    ob.close()
    return flight_recorder_home


def run_status(bridge: Path) -> int:
    return cli.main(["status", "--flight-recorder-home", str(bridge)])


def test_status_not_initialized_exits_2(tmp_path, capsys):
    code = run_status(tmp_path / "uninit")
    err = capsys.readouterr().err
    assert code == 2
    assert "not initialized" in err.lower()


def test_status_fresh_capture_is_healthy(tmp_path, capsys):
    bridge = init_home(tmp_path / "b", heartbeat=time.time() - 10.0)
    code = run_status(bridge)
    out = capsys.readouterr().out
    assert code == 0
    assert "capture:" in out
    assert "OK" in out
    assert "installation:" in out
    assert "pending 0" in out


def test_status_stale_capture_exits_1(tmp_path, capsys):
    stale = time.time() - THRESHOLD - 3600.0  # safely past the window
    bridge = init_home(tmp_path / "b", heartbeat=stale)
    code = run_status(bridge)
    out = capsys.readouterr().out
    assert code == 1
    assert "STALE" in out


def test_status_no_heartbeat_exits_1(tmp_path, capsys):
    bridge = init_home(tmp_path / "b")  # never captured
    code = run_status(bridge)
    out = capsys.readouterr().out
    assert code == 1
    assert "NO SUCCESS RECORDED" in out


def _append_events(bridge: Path, n: int) -> None:
    """Append ``n`` valid events without syncing, so pending == n."""
    ob = Outbox.open(bridge)
    try:
        for _ in range(n):
            ob.append(
                build_record(
                    event_type="session.created",
                    occurred_at=1784415000.0,
                    source="test",
                    capture_method="test",
                    runtime={"kind": "cli", "engine": "standard"},
                    correlation_id="corr",
                    payload={},
                )
            )
    finally:
        ob.close()


def test_status_reports_pending_backlog(tmp_path, capsys):
    bridge = init_home(tmp_path / "b", heartbeat=time.time() - 10.0)
    _append_events(bridge, 3)  # never synced -> delivery cursor 0, pending 3

    code = run_status(bridge)
    out = capsys.readouterr().out

    assert "producer high-water 3" in out
    assert "delivery cursor 0" in out
    assert "pending 3" in out


def test_status_backlog_alone_does_not_flip_exit_code(tmp_path, capsys):
    """A pending backlog (e.g. the network is down and the outbox is buffering)
    is not a capture failure — only a stale/absent heartbeat flips the exit
    code. Capture liveness and delivery lag are distinct signals."""
    bridge = init_home(tmp_path / "b", heartbeat=time.time() - 10.0)
    _append_events(bridge, 500)

    code = run_status(bridge)
    out = capsys.readouterr().out

    assert code == 0  # fresh capture heartbeat -> healthy despite the backlog
    assert "pending 500" in out
    assert "OK" in out


def test_status_unreadable_heartbeat_exits_1(tmp_path, capsys):
    bridge = tmp_path / "b"
    ob = Outbox.open(bridge)
    ob.initialize()
    ob.set_meta(CAPTURE_HEARTBEAT_KEY, "not-a-number")
    ob.close()

    code = run_status(bridge)
    out = capsys.readouterr().out
    assert code == 1
    assert "UNREADABLE" in out
