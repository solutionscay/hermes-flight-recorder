"""The shared sync pass behind the CLI `sync` verb and `serve` (issue #167).

`cli._sync_once` and `serve._sync` used to duplicate the push -> cursor-delta
summary -> wrapped-DEK ship -> prune sequence, and the copies had drifted
(`max_batches` and `"cancelled"` existed only in serve). Both now delegate to
`collector.sync_pass.run_sync_pass` and only render its structured result.
"""

from __future__ import annotations

import logging

import pytest

from hermes_flight_recorder import cli
from hermes_flight_recorder.collector import recorder_config, serve
from hermes_flight_recorder.collector import sync_pass
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.sync import InMemoryTransport, delivery_cursor
from hermes_flight_recorder.collector.transport import (
    RetryableTransportError,
    TerminalTransportError,
    TransportCancelled,
)

from test_outbox import base_record


def outbox_with_events(path, n=3):
    outbox = Outbox.open(path)
    outbox.initialize()
    for _ in range(n):
        outbox.append(base_record())
    return outbox


class _FailingTransport:
    """A transport whose event sends always raise ``error``."""

    def __init__(self, error):
        self.error = error

    def send(self, batch):
        raise self.error

    def send_keys(self, batch):
        raise AssertionError("no wrapped keys expected")


def _spy_results(monkeypatch):
    """Record every SyncPassResult the shared pass returns."""
    results = []
    real = sync_pass.run_sync_pass

    def wrapper(*args, **kwargs):
        result = real(*args, **kwargs)
        results.append(result)
        return result

    monkeypatch.setattr(sync_pass, "run_sync_pass", wrapper)
    return results


# --------------------------------------------------------------------------
# CLI and serve produce the same structured result for the same outbox state
# --------------------------------------------------------------------------
def test_cli_and_serve_share_one_pass_and_one_result(tmp_path, monkeypatch, capsys):
    results = _spy_results(monkeypatch)
    cli_outbox = outbox_with_events(tmp_path / "cli", n=3)
    serve_outbox = outbox_with_events(tmp_path / "serve", n=3)
    config = recorder_config.RecorderConfig()

    assert cli._sync_once(cli_outbox, InMemoryTransport()) == 0
    serve._sync(
        serve_outbox,
        InMemoryTransport(),
        config,
        logging.getLogger("test-serve-sync"),
    )

    assert len(results) == 2
    assert results[0] == results[1]  # one pass, one truth, two sinks
    assert results[0].outcome == "ok"
    assert results[0].acked == 3
    assert results[0].pending == 0
    cli_outbox.close()
    serve_outbox.close()
    capsys.readouterr()


def test_cli_and_serve_agree_when_the_network_is_down(tmp_path, monkeypatch, capsys):
    results = _spy_results(monkeypatch)
    cli_outbox = outbox_with_events(tmp_path / "cli", n=2)
    serve_outbox = outbox_with_events(tmp_path / "serve", n=2)
    config = recorder_config.RecorderConfig()

    code = cli._sync_once(
        cli_outbox, _FailingTransport(RetryableTransportError("down"))
    )
    serve._sync(
        serve_outbox,
        _FailingTransport(RetryableTransportError("down")),
        config,
        logging.getLogger("test-serve-sync"),
    )

    assert code == 1
    assert len(results) == 2
    assert results[0] == results[1]
    assert results[0].outcome == "offline"
    assert results[0].acked == 0
    assert results[0].pending == 2
    cli_outbox.close()
    serve_outbox.close()
    capsys.readouterr()


# --------------------------------------------------------------------------
# The drifted extras are now shared: cancelled handling and max_batches
# --------------------------------------------------------------------------
def test_cancelled_pass_skips_key_ship_and_prune(tmp_path):
    outbox = outbox_with_events(tmp_path, n=1)
    retention = recorder_config.RetentionConfig(enabled=True, max_bytes=1)

    result = sync_pass.run_sync_pass(
        outbox,
        _FailingTransport(TransportCancelled("shutdown")),
        retention_config=retention,
    )

    assert result.outcome == "cancelled"
    assert result.key_outcome is None  # side-channel never attempted
    assert result.pruned is None and result.prune_error is None
    outbox.close()


def test_cli_reports_cancelled_as_a_clean_stop(tmp_path, capsys):
    outbox = outbox_with_events(tmp_path, n=1)

    code = cli._sync_once(outbox, _FailingTransport(TransportCancelled("shutdown")))

    assert code == 0
    assert "sync stopped for shutdown" in capsys.readouterr().err
    assert delivery_cursor(outbox) == 0  # cursor untouched
    outbox.close()


def test_terminal_defect_skips_key_ship_and_prune(tmp_path):
    outbox = outbox_with_events(tmp_path, n=1)

    result = sync_pass.run_sync_pass(
        outbox, _FailingTransport(TerminalTransportError("400"))
    )

    assert result.outcome == "terminal"
    assert result.detail == "400"
    assert result.key_outcome is None
    outbox.close()


def test_max_batches_bounds_one_pass(tmp_path):
    outbox = outbox_with_events(tmp_path, n=3)

    result = sync_pass.run_sync_pass(
        outbox, InMemoryTransport(), max_records=1, max_batches=1
    )

    assert result.outcome == "ok"
    assert result.acked == 1
    assert result.pending == 2  # the next tick resumes from the cursor
    outbox.close()


def test_prune_refusal_is_reported_not_raised(tmp_path):
    outbox = outbox_with_events(tmp_path, n=1)
    # require_delivered=False is refused by retention with a RetentionError.
    retention = recorder_config.RetentionConfig(
        enabled=True, max_bytes=1, require_delivered=False
    )

    result = sync_pass.run_sync_pass(
        outbox, InMemoryTransport(), retention_config=retention
    )

    assert result.outcome == "ok"
    assert result.prune_error is not None
    assert "require_delivered" in result.prune_error
    outbox.close()
