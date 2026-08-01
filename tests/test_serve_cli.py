"""The unified foreground ``serve`` process (issue #101).

``serve`` captures and reconciles on independent intervals (and syncs when
configured) in one process, guarded by a single-instance lock. A failing pass is
logged but never stops the loop; SIGINT/SIGTERM shut it down cleanly.

The scheduling tests drive :func:`serve` directly with fake passes so they are
deterministic — no wall-clock sleeps or thread races. A pass sets the stop event
once it has fired enough times, which is how a real signal would end the loop.
"""

from __future__ import annotations

import signal
import threading
import time

import pytest

from hermes_flight_recorder import cli
from hermes_flight_recorder.collector import recorder_config, serve as S
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.sync import Ack
from hermes_flight_recorder.collector.runtime_lock import RuntimeLock

from test_outbox import base_record


class _DummyOutbox:
    installation_id = "test-install"

    def close(self):  # serve never closes the injected outbox, but be safe
        pass


def _run_serve(monkeypatch, *, capture, reconcile, stop, **kw):
    monkeypatch.setattr(S, "_capture", capture)
    monkeypatch.setattr(S, "_reconcile", reconcile)
    return S.serve(
        _DummyOutbox(),
        "unused-hermes-home",
        recorder_config.RecorderConfig(),
        capture_interval=0.001,
        reconcile_interval=0.001,
        stop_event=stop,
        install_signal_handlers=False,
        **kw,
    )


def test_capture_and_reconcile_fire_independently(monkeypatch):
    stop = threading.Event()
    caps = {"n": 0}
    recs = {"n": 0}

    def capture(*a):
        caps["n"] += 1
        if caps["n"] >= 3:
            stop.set()

    def reconcile(*a):
        recs["n"] += 1

    rc = _run_serve(monkeypatch, capture=capture, reconcile=reconcile, stop=stop)
    assert rc == S.SERVE_OK
    assert caps["n"] >= 3
    assert recs["n"] >= 1  # reconcile also ran, on its own schedule


def test_failing_capture_does_not_stop_reconcile(monkeypatch):
    stop = threading.Event()
    recs = {"n": 0}

    def capture(*a):
        raise RuntimeError("capture boom")

    def reconcile(*a):
        recs["n"] += 1
        if recs["n"] >= 3:
            stop.set()

    rc = _run_serve(monkeypatch, capture=capture, reconcile=reconcile, stop=stop)
    assert rc == S.SERVE_OK  # a raising pass never crashes the daemon
    assert recs["n"] >= 3


def test_blocked_sync_does_not_delay_capture_or_shutdown(tmp_path, monkeypatch):
    outbox = Outbox.open(tmp_path / "fr")
    outbox.initialize()
    outbox.append(base_record())
    stop = threading.Event()
    sync_started = threading.Event()
    release_sync = threading.Event()
    sync_finished = threading.Event()
    captures = {"n": 0}

    class _BlockedTransport:
        def send(self, batch):
            sync_started.set()
            release_sync.wait(2.0)
            sync_finished.set()
            return Ack(
                accepted=len(batch["records"]),
                duplicates=0,
                high_water=batch["records"][-1]["producer_sequence"],
            )

        def send_keys(self, batch):
            raise AssertionError("no wrapped keys expected")

    def capture(*args):
        captures["n"] += 1
        if sync_started.is_set() and captures["n"] >= 3:
            stop.set()

    monkeypatch.setattr(S, "_capture", capture)
    monkeypatch.setattr(S, "_reconcile", lambda *args: None)

    started = time.monotonic()
    rc = S.serve(
        outbox,
        "unused-hermes-home",
        recorder_config.RecorderConfig(),
        transport=_BlockedTransport(),
        capture_interval=0.001,
        reconcile_interval=0.001,
        sync_interval=0.001,
        stop_event=stop,
        install_signal_handlers=False,
        sync_shutdown_timeout=0.02,
    )
    elapsed = time.monotonic() - started

    assert rc == S.SERVE_OK
    assert sync_started.is_set()
    assert captures["n"] >= 3
    assert elapsed < 0.5

    release_sync.set()
    assert sync_finished.wait(1.0)
    outbox.close()


def test_sigterm_handler_sets_the_sync_cancellation_event(monkeypatch):
    stop = threading.Event()
    handlers = {}

    def install(sig, handler):
        handlers[sig] = handler
        return signal.SIG_DFL

    monkeypatch.setattr(S.signal, "signal", install)
    restore = S._install_signal_handlers(stop, S.configure_logging())

    handlers[signal.SIGTERM](signal.SIGTERM, None)

    assert stop.is_set()
    restore()


def test_serve_exits_when_lock_held(tmp_path, monkeypatch):
    lock_path = tmp_path / "runtime.lock"
    holder = RuntimeLock(lock_path)
    holder.acquire()
    try:
        stop = threading.Event()
        rc = _run_serve(
            monkeypatch,
            capture=lambda *a: None,
            reconcile=lambda *a: None,
            stop=stop,
            lock=RuntimeLock(lock_path),
        )
        assert rc == S.SERVE_ALREADY_RUNNING
    finally:
        holder.release()


def test_cli_serve_exits_two_when_uninitialized(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("SC_HERMES_FLIGHT_RECORDER_HOME", raising=False)
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    # An empty, never-initialized recorder home: serve must refuse with code 2
    # before entering the loop (so this call does not block).
    rc = cli.main(
        [
            "serve",
            "--flight-recorder-home",
            str(tmp_path / "fr"),
            "--hermes-home",
            str(hermes),
            "--no-sync",
        ]
    )
    assert rc == 2
    assert "not initialized" in capsys.readouterr().err.lower()
