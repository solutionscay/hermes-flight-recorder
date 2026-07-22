"""Tests for the `sync` CLI verb and run integration (issue #34)."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from hermes_flight_recorder import cli
from hermes_flight_recorder.collector import sync_config
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.sync import delivery_cursor
from hermes_flight_recorder.collector.sync_config import SyncConfig
from hermes_flight_recorder.collector.transport import (
    AuthError,
    RetryableTransportError,
    TerminalTransportError,
)

from test_outbox import base_record


# --------------------------------------------------------------------------
# A real local /ingest server (deduping ledger), shared shape with #33 tests.
# --------------------------------------------------------------------------
class _LedgerHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        self.server.request_count += 1
        raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        records = json.loads(raw)["records"]
        accepted = duplicates = 0
        for rec in records:
            if rec["event_id"] in self.server.ledger:
                duplicates += 1
            else:
                self.server.ledger[rec["event_id"]] = rec
                accepted += 1
            self.server.hw = max(self.server.hw, rec["producer_sequence"])
        out = json.dumps(
            {"accepted": accepted, "duplicates": duplicates, "high_water": self.server.hw}
        ).encode()
        self.send_response(202)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)


@pytest.fixture
def ingest_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _LedgerHandler)
    server.ledger, server.hw, server.request_count = {}, 0, 0
    threading.Thread(target=server.serve_forever, daemon=True).start()
    host, port = server.server_address
    server.url = f"http://{host}:{port}/ingest"
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()


def prepared_home(tmp_path, url, n_events=3):
    """An initialized outbox with events and a sync config pointing at ``url``."""
    outbox = Outbox.open(tmp_path)
    outbox.initialize()
    for _ in range(n_events):
        outbox.append(base_record())
    outbox.close()
    sync_config.save(SyncConfig(url, "cid", "csecret"), tmp_path)
    return tmp_path


def open_ready_outbox(tmp_path):
    outbox = Outbox.open(tmp_path)
    outbox.initialize()
    return outbox


# --------------------------------------------------------------------------
# CLI-level: happy path, "second pass ships nothing", arg/config errors
# --------------------------------------------------------------------------
def test_sync_ships_and_reports_summary(tmp_path, ingest_server, capsys):
    home = prepared_home(tmp_path, ingest_server.url, n_events=3)

    code = cli.main(["sync", "--flight-recorder-home", str(home), "--allow-insecure-url"])

    out = capsys.readouterr().out
    assert code == 0
    assert len(ingest_server.ledger) == 3
    assert "shipped 3 / acked 3 / pending 0" in out


def test_second_sync_ships_nothing(tmp_path, ingest_server, capsys):
    home = prepared_home(tmp_path, ingest_server.url, n_events=2)
    args = ["sync", "--flight-recorder-home", str(home), "--allow-insecure-url"]

    assert cli.main(args) == 0
    capsys.readouterr()
    assert cli.main(args) == 0  # nothing left to ship

    out = capsys.readouterr().out
    assert "shipped 0 / acked 0 / pending 0" in out
    assert len(ingest_server.ledger) == 2  # unchanged


def test_sync_uses_batch_limits_from_recorder_config(tmp_path, ingest_server, capsys):
    home = prepared_home(tmp_path, ingest_server.url, n_events=3)
    (home / "recorder-config.json").write_text(
        json.dumps({"sync": {"max_records": 1}})
    )

    code = cli.main(["sync", "--flight-recorder-home", str(home), "--allow-insecure-url"])

    assert code == 0
    assert ingest_server.request_count == 3
    assert len(ingest_server.ledger) == 3
    capsys.readouterr()


def test_sync_automatically_prunes_only_after_delivery_ack(
    tmp_path, ingest_server, capsys
):
    home = prepared_home(tmp_path, ingest_server.url, n_events=3)
    (home / "recorder-config.json").write_text(
        json.dumps(
            {
                "retention": {
                    "enabled": True,
                    "max_age_days": None,
                    "max_bytes": 1,
                    "require_delivered": True,
                    "vacuum": "auto",
                }
            }
        )
    )

    code = cli.main(
        ["sync", "--flight-recorder-home", str(home), "--allow-insecure-url"]
    )

    assert code == 0
    assert len(ingest_server.ledger) == 3
    outbox = Outbox.open(home)
    assert delivery_cursor(outbox) == 3
    assert outbox.high_water() == 3
    assert outbox.count() == 0
    outbox.close()
    assert "automatic retention: pruned 3 delivered event" in capsys.readouterr().out


def test_sync_uninitialized_outbox_is_config_error(tmp_path, capsys):
    code = cli.main(["sync", "--flight-recorder-home", str(tmp_path)])
    assert code == 2
    assert "not initialized" in capsys.readouterr().err


def test_sync_without_config_is_config_error(tmp_path, capsys):
    open_ready_outbox(tmp_path).close()  # initialized, but no sync-config.json
    code = cli.main(["sync", "--flight-recorder-home", str(tmp_path)])
    assert code == 2
    assert "not configured" in capsys.readouterr().err


def test_sync_rejects_plaintext_url_without_flag(tmp_path, ingest_server, capsys):
    home = prepared_home(tmp_path, ingest_server.url)  # an http:// url
    # No --allow-insecure-url: HttpsTransport refuses the plaintext endpoint.
    with pytest.raises(Exception):
        cli.main(["sync", "--flight-recorder-home", str(home)])


# --------------------------------------------------------------------------
# _sync_once exit codes with fake transports (fast, no sockets, no sleep)
# --------------------------------------------------------------------------
class _FakeTransport:
    def __init__(self, error):
        self.error = error

    def send(self, batch):
        raise self.error


def test_sync_once_offline_exits_unreachable(tmp_path, capsys):
    outbox = open_ready_outbox(tmp_path)
    outbox.append(base_record())
    before = delivery_cursor(outbox)

    code = cli._sync_once(outbox, _FakeTransport(RetryableTransportError("down")))

    assert code == 1
    error = capsys.readouterr().err
    assert "unreachable" in error
    assert "down" in error
    assert delivery_cursor(outbox) == before  # cursor untouched
    outbox.close()


def test_sync_once_auth_exits_three(tmp_path, capsys):
    outbox = open_ready_outbox(tmp_path)
    outbox.append(base_record())
    code = cli._sync_once(outbox, _FakeTransport(AuthError("403")))
    assert code == 3
    error = capsys.readouterr().err
    assert "service token" in error
    assert "403" in error
    outbox.close()


def test_sync_once_terminal_exits_four(tmp_path, capsys):
    outbox = open_ready_outbox(tmp_path)
    outbox.append(base_record())
    code = cli._sync_once(outbox, _FakeTransport(TerminalTransportError("400")))
    assert code == 4
    assert "client defect" in capsys.readouterr().err
    outbox.close()


# --------------------------------------------------------------------------
# Interval loop: runs a pass, then stops cleanly on interrupt
# --------------------------------------------------------------------------
def test_interval_loop_runs_then_stops(tmp_path, ingest_server, capsys, monkeypatch):
    home = prepared_home(tmp_path, ingest_server.url, n_events=1)
    calls = {"n": 0}

    def fake_sleep(_seconds):
        calls["n"] += 1
        raise KeyboardInterrupt

    monkeypatch.setattr(cli.time, "sleep", fake_sleep)
    code = cli.main(
        ["sync", "--flight-recorder-home", str(home), "--allow-insecure-url", "--interval", "5"]
    )

    assert code == 0
    assert calls["n"] == 1  # one pass, then the sleep interrupt stopped it
    assert len(ingest_server.ledger) == 1
    assert "stopped" in capsys.readouterr().err
