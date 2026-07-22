"""Tests for the HTTPS transport, retry, and offline tolerance (issue #33).

Two styles:

- An injected ``urlopen`` seam drives status classification and the retry
  schedule with no sockets and no real time.
- A real local ``/ingest`` server exercises the full ``sync`` path: the wire
  body, idempotent resend, cursor movement, and offline resume.
"""

from __future__ import annotations

import io
import json
import threading
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.sync import (
    Ack,
    DELIVERY_CURSOR_NAME,
    sync,
)
from hermes_flight_recorder.collector.sync_config import (
    SyncConfig,
    SyncConfigError,
    config_path,
)
from hermes_flight_recorder.collector import sync_config
from hermes_flight_recorder.collector.transport import (
    AuthError,
    HttpsTransport,
    RetryableTransportError,
    RetryingTransport,
    TerminalTransportError,
    TransportError,
    push,
)

from test_outbox import base_record


# --------------------------------------------------------------------------
# Fixtures and helpers
# --------------------------------------------------------------------------
def new_outbox(tmp_path) -> Outbox:
    outbox = Outbox.open(tmp_path)
    outbox.initialize()
    return outbox


def a_batch(records=None):
    if records is None:
        records = [{"event_id": "e1", "producer_sequence": 1, "installation_id": "i"}]
    return {"protocol_version": "1", "records": records}


class _FakeResponse:
    """A minimal stand-in for the object ``urlopen`` returns."""

    def __init__(self, status: int, body: bytes):
        self.status = status
        self._body = body

    def read(self) -> bytes:
        return self._body

    def getcode(self) -> int:
        return self.status

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def ok_ack_bytes(accepted=1, duplicates=0, high_water=1) -> bytes:
    return json.dumps(
        {"accepted": accepted, "duplicates": duplicates, "high_water": high_water}
    ).encode()


def urlopen_returning(response=None, *, raises=None, capture=None):
    """Build a fake ``urlopen`` that records the request it received."""

    def _fake(request, timeout=None):
        if capture is not None:
            capture.append(request)
        if raises is not None:
            raise raises
        return response

    return _fake


# --------------------------------------------------------------------------
# sync_config
# --------------------------------------------------------------------------
def test_config_load_from_file(tmp_path):
    sync_config.save(
        SyncConfig("https://ingest.example/ingest", "cid", "csecret"), tmp_path
    )
    cfg = sync_config.load(tmp_path)
    assert cfg.ingest_url == "https://ingest.example/ingest"
    assert cfg.auth_headers() == {
        "CF-Access-Client-Id": "cid",
        "CF-Access-Client-Secret": "csecret",
    }


def test_config_replaces_retired_hosted_domain_on_load_and_save(tmp_path):
    retired = "https://app.hermesdbass.com/ingest"
    current = "https://app.hermesdbaas.com/ingest"

    path = sync_config.save(SyncConfig(retired, "cid", "csecret"), tmp_path)

    assert json.loads(path.read_text())["ingest_url"] == current
    assert sync_config.load(tmp_path).ingest_url == current


def test_config_env_overrides_file(tmp_path, monkeypatch):
    sync_config.save(SyncConfig("https://file/ingest", "fid", "fsecret"), tmp_path)
    monkeypatch.setenv("HFR_INGEST_URL", "https://env/ingest")
    monkeypatch.setenv("HFR_CF_ACCESS_CLIENT_SECRET", "envsecret")
    cfg = sync_config.load(tmp_path)
    assert cfg.ingest_url == "https://env/ingest"
    assert cfg.cf_access_client_id == "fid"  # not overridden
    assert cfg.cf_access_client_secret == "envsecret"


def test_config_replaces_retired_hosted_domain_from_environment(
    tmp_path, monkeypatch
):
    sync_config.save(SyncConfig("https://file/ingest", "fid", "fsecret"), tmp_path)
    monkeypatch.setenv(
        "HFR_INGEST_URL", "https://app.hermesdbass.com/ingest"
    )

    assert sync_config.load(tmp_path).ingest_url == sync_config.HOSTED_INGEST_URL


def test_config_incomplete_raises(tmp_path):
    with pytest.raises(SyncConfigError, match="missing"):
        sync_config.load(tmp_path)


def test_config_file_is_private(tmp_path):
    path = sync_config.save(SyncConfig("https://x/ingest", "a", "b"), tmp_path)
    assert path == config_path(tmp_path)
    assert (path.stat().st_mode & 0o777) == 0o600


# --------------------------------------------------------------------------
# HttpsTransport: happy path, headers, body, status classification
# --------------------------------------------------------------------------
def test_send_posts_json_with_auth_headers_and_returns_ack():
    captured: list = []
    transport = HttpsTransport(
        ingest_url="https://ingest.example/ingest",
        headers={"CF-Access-Client-Id": "cid", "CF-Access-Client-Secret": "csec"},
        _urlopen=urlopen_returning(_FakeResponse(202, ok_ack_bytes()), capture=captured),
    )
    ack = transport.send(a_batch())

    assert ack == Ack(accepted=1, duplicates=0, high_water=1)
    request = captured[0]
    assert request.method == "POST"
    assert request.get_header("Content-type") == "application/json"
    assert request.get_header("Cf-access-client-id") == "cid"
    assert request.get_header("Cf-access-client-secret") == "csec"
    # A descriptive User-Agent, not urllib's default (which some edges refuse).
    ua = request.get_header("User-agent")
    assert ua and ua.startswith("hermes-flight-recorder/")
    assert json.loads(request.data)["records"][0]["event_id"] == "e1"


def test_require_https_rejects_plaintext_url():
    with pytest.raises(TransportError, match="HTTPS"):
        HttpsTransport(ingest_url="http://insecure/ingest", headers={})


@pytest.mark.parametrize(
    "status, expected",
    [
        (400, TerminalTransportError),
        (401, AuthError),
        (403, AuthError),
        (429, RetryableTransportError),
        (500, RetryableTransportError),
        (503, RetryableTransportError),
        (418, TerminalTransportError),  # unexpected → terminal
    ],
)
def test_status_classification(status, expected):
    err = urllib.error.HTTPError(
        "https://x/ingest", status, "msg", {}, io.BytesIO(b'{"error":"x"}')
    )
    transport = HttpsTransport(
        "https://x/ingest", {}, _urlopen=urlopen_returning(raises=err)
    )
    with pytest.raises(expected):
        transport.send(a_batch())


def test_network_error_is_retryable():
    transport = HttpsTransport(
        "https://x/ingest",
        {},
        _urlopen=urlopen_returning(raises=urllib.error.URLError("down")),
    )
    with pytest.raises(RetryableTransportError):
        transport.send(a_batch())


def test_malformed_ack_is_terminal():
    transport = HttpsTransport(
        "https://x/ingest",
        {},
        _urlopen=urlopen_returning(_FakeResponse(202, b"not json")),
    )
    with pytest.raises(TerminalTransportError):
        transport.send(a_batch())


# --------------------------------------------------------------------------
# RetryingTransport
# --------------------------------------------------------------------------
class _FlakyTransport:
    """Raise a retryable error N times, then return an ack."""

    def __init__(self, fail_times: int, ack: Ack | None = None):
        self.fail_times = fail_times
        self.calls = 0
        self.ack = ack or Ack(1, 0, 1)

    def send(self, batch):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RetryableTransportError("transient")
        return self.ack


def test_retry_succeeds_after_transient_failures():
    inner = _FlakyTransport(fail_times=2)
    delays: list[float] = []
    retrying = RetryingTransport(
        inner, max_attempts=5, sleep=delays.append, rng=lambda: 1.0
    )
    ack = retrying.send(a_batch())
    assert ack == Ack(1, 0, 1)
    assert inner.calls == 3
    # Full jitter with rng()==1.0 gives the ceiling: base, 2*base.
    assert delays == [0.5, 1.0]


def test_retry_exhaustion_raises():
    inner = _FlakyTransport(fail_times=99)
    retrying = RetryingTransport(inner, max_attempts=3, sleep=lambda _: None)
    with pytest.raises(RetryableTransportError):
        retrying.send(a_batch())
    assert inner.calls == 3


def test_auth_and_terminal_are_not_retried():
    class _AuthTransport:
        calls = 0

        def send(self, batch):
            type(self).calls += 1
            raise AuthError("401")

    inner = _AuthTransport()
    retrying = RetryingTransport(inner, max_attempts=5, sleep=lambda _: None)
    with pytest.raises(AuthError):
        retrying.send(a_batch())
    assert inner.calls == 1


def test_jitter_never_exceeds_capped_ceiling():
    inner = _FlakyTransport(fail_times=99)
    delays: list[float] = []
    retrying = RetryingTransport(
        inner,
        max_attempts=10,
        base_delay=1.0,
        max_delay=8.0,
        sleep=delays.append,
        rng=lambda: 1.0,
    )
    with pytest.raises(RetryableTransportError):
        retrying.send(a_batch())
    assert max(delays) <= 8.0
    assert delays == [1.0, 2.0, 4.0, 8.0, 8.0, 8.0, 8.0, 8.0, 8.0]


# --------------------------------------------------------------------------
# Real local /ingest server: full sync path, idempotency, offline resume
# --------------------------------------------------------------------------
class _LedgerHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # silence
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        self.server.bodies.append(raw)
        try:
            body = json.loads(raw)
            records = body["records"]
        except (ValueError, KeyError):
            return self._json(400, {"error": "bad_request", "message": "records"})

        accepted = duplicates = 0
        for rec in records:
            eid = rec["event_id"]
            if eid in self.server.ledger:
                duplicates += 1
            else:
                self.server.ledger[eid] = rec
                accepted += 1
            self.server.high_water = max(
                self.server.high_water, rec["producer_sequence"]
            )
        self._json(
            202,
            {
                "accepted": accepted,
                "duplicates": duplicates,
                "high_water": self.server.high_water,
            },
        )

    def _json(self, status, payload):
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


@pytest.fixture
def ingest_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _LedgerHandler)
    server.ledger = {}
    server.high_water = 0
    server.bodies = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    server.url = f"http://{host}:{port}/ingest"
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()


def _http_transport(url):
    return HttpsTransport(url, {"CF-Access-Client-Id": "t"}, require_https=False)


def test_full_sync_against_local_server(tmp_path, ingest_server):
    outbox = new_outbox(tmp_path)
    for _ in range(5):
        outbox.append(base_record())
    transport = _http_transport(ingest_server.url)

    result = sync(outbox, transport, max_records=2)

    assert result.records_sent == 5
    assert result.pending == 0
    assert len(ingest_server.ledger) == 5
    assert int(outbox.get_cursor(DELIVERY_CURSOR_NAME)) == result.delivery_cursor
    outbox.close()


def test_resend_is_idempotent_no_gap(tmp_path, ingest_server):
    outbox = new_outbox(tmp_path)
    for _ in range(3):
        outbox.append(base_record())
    transport = _http_transport(ingest_server.url)

    first = sync(outbox, transport)
    assert len(ingest_server.ledger) == 3

    # Rewind the delivery cursor and send again: the server dedups, the ledger
    # gains no row, and the cursor lands back on the same high-water.
    outbox.set_cursor(DELIVERY_CURSOR_NAME, 0)
    second = sync(outbox, transport)

    assert len(ingest_server.ledger) == 3  # no duplicate row
    assert second.delivery_cursor == first.delivery_cursor
    outbox.close()


def test_no_plaintext_content_on_the_wire(tmp_path, ingest_server):
    outbox = new_outbox(tmp_path)
    secret = "TOP-SECRET-PLAINTEXT-9137"
    outbox.append(base_record(), content=secret)
    transport = _http_transport(ingest_server.url)

    sync(outbox, transport)

    sent = b"".join(ingest_server.bodies)
    assert secret.encode() not in sent
    # The ciphertext envelope fields are what actually shipped.
    shipped = next(iter(ingest_server.ledger.values()))
    assert "content_ciphertext" in shipped
    outbox.close()


def test_push_is_offline_tolerant(tmp_path):
    outbox = new_outbox(tmp_path)
    outbox.append(base_record())

    class _DeadTransport:
        def send(self, batch):
            raise RetryableTransportError("network down")

    outcome = push(outbox, _DeadTransport())
    assert outcome.ok is False
    assert outcome.reason == "offline"
    # Cursor untouched: the event is still pending for the next pass.
    assert outbox.get_cursor(DELIVERY_CURSOR_NAME) is None
    outbox.close()


def test_push_reports_auth_without_spinning(tmp_path):
    outbox = new_outbox(tmp_path)
    outbox.append(base_record())

    class _AuthTransport:
        def send(self, batch):
            raise AuthError("403")

    outcome = push(outbox, _AuthTransport())
    assert outcome.ok is False
    assert outcome.reason == "auth"
    outbox.close()


def test_push_surfaces_terminal_defect(tmp_path):
    outbox = new_outbox(tmp_path)
    outbox.append(base_record())

    class _BadRequestTransport:
        def send(self, batch):
            raise TerminalTransportError("400 bad_record")

    with pytest.raises(TerminalTransportError):
        push(outbox, _BadRequestTransport())
    outbox.close()


def test_push_ok_then_resumes_from_last_ack(tmp_path, ingest_server):
    outbox = new_outbox(tmp_path)
    for _ in range(2):
        outbox.append(base_record())
    transport = _http_transport(ingest_server.url)

    first = push(outbox, transport)
    assert first.ok and first.result.records_sent == 2

    # New events after the first push ship without resending the old ones.
    outbox.append(base_record())
    second = push(outbox, transport)
    assert second.ok
    assert second.result.records_sent == 1
    assert len(ingest_server.ledger) == 3
    outbox.close()
