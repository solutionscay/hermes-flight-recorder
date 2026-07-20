#!/usr/bin/env python3
"""Phase 1 sync exit-gate: prove lossless delivery across the HTTP seam.

The gate runs the real outbox, batcher, HTTPS transport (with HTTPS disabled
only for loopback), retry wrapper, and delivery cursor against a deterministic
local ingestion server. The server owns an idempotent event ledger and can
drop a request before storage or after durable storage but before its ack.

Scenarios:

1. Happy path        - every batch is acked.
2. Dropped batch     - one batch is lost before storage, then re-sent.
3. Duplicate         - one ack is lost after storage, then re-sent and deduped.
4. Offline to online - one sync pass fails, then the next catches up.
5. Bridge restart    - a process stops after the server stores an unacked batch;
                       a reopened outbox resumes from its last durable cursor.

Usage:  python scripts/poc_sync_gate.py [-v]
Exit:   0 if every server ledger is complete and gap-free, 1 otherwise.
"""

from __future__ import annotations

import json
import socket
import sys
import tempfile
import threading
from collections import deque
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator

from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.sync import DELIVERY_CURSOR_NAME, sync
from hermes_flight_recorder.collector.transport import (
    HttpsTransport,
    RetryableTransportError,
    RetryingTransport,
    push,
)

CLIENT_ID = "sync-gate-client"
CLIENT_SECRET = "sync-gate-secret"
PRIVATE_CONTENT = "SYNC-GATE-PRIVATE-PROMPT"
VERBOSE = "-v" in sys.argv[1:]

NORMAL = "normal"
DROP_BEFORE_STORE = "drop_before_store"
DROP_AFTER_STORE = "drop_after_store"


class LedgerServer(ThreadingHTTPServer):
    """A minimal ingestion-protocol v1 server with deterministic faults."""

    daemon_threads = True

    def __init__(self, actions: list[str] | None = None):
        super().__init__(("127.0.0.1", 0), LedgerHandler)
        self.actions = deque(actions or [])
        self.ledger: dict[str, dict] = {}
        self.bodies: list[bytes] = []
        self.headers_seen: list[dict[str, str]] = []
        self.ack_history: list[dict[str, int]] = []
        self.high_water = 0
        self._lock = threading.Lock()
        host, port = self.server_address
        self.url = f"http://{host}:{port}/ingest"

    def next_action(self) -> str:
        with self._lock:
            return self.actions.popleft() if self.actions else NORMAL

    def store(self, records: list[dict]) -> dict[str, int]:
        accepted = 0
        duplicates = 0
        with self._lock:
            for record in records:
                event_id = record["event_id"]
                if event_id in self.ledger:
                    duplicates += 1
                else:
                    self.ledger[event_id] = record
                    accepted += 1
                self.high_water = max(
                    self.high_water, int(record["producer_sequence"])
                )
            return {
                "accepted": accepted,
                "duplicates": duplicates,
                "high_water": self.high_water,
            }

    def sequences(self) -> list[int]:
        with self._lock:
            return sorted(
                int(record["producer_sequence"])
                for record in self.ledger.values()
            )


class LedgerHandler(BaseHTTPRequestHandler):
    """Serve one ingestion request and apply the server's next fault."""

    server: LedgerServer

    def log_message(self, *_args: object) -> None:
        pass

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        self.server.bodies.append(raw)
        self.server.headers_seen.append(dict(self.headers.items()))

        if self.path != "/ingest":
            self._json(404, {"error": "not_found"})
            return

        try:
            body = json.loads(raw)
            records = body["records"]
            if body.get("protocol_version") != "1" or not records:
                raise ValueError("invalid protocol body")
        except (TypeError, ValueError, KeyError):
            self._json(400, {"error": "bad_request"})
            return

        action = self.server.next_action()
        if action == DROP_BEFORE_STORE:
            self._drop_connection()
            return

        ack = self.server.store(records)
        if action == DROP_AFTER_STORE:
            self._drop_connection()
            return

        self.server.ack_history.append(ack)
        self._json(202, ack)

    def _drop_connection(self) -> None:
        """Simulate a network loss without returning any HTTP response."""
        self.close_connection = True
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.connection.close()

    def _json(self, status: int, payload: dict) -> None:
        data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


@contextmanager
def ingestion_server(actions: list[str] | None = None) -> Iterator[LedgerServer]:
    server = LedgerServer(actions)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _new_outbox(home: Path, count: int = 5) -> Outbox:
    outbox = Outbox.open(home)
    outbox.initialize()
    for index in range(1, count + 1):
        outbox.append(
            {
                "occurred_at": 1_800_000_000.0 + index,
                "tenant_id": "local",
                "profile": "sync-gate",
                "runtime": {"kind": "test"},
                "correlation_id": "sync-gate-run",
                "source": "poc_sync_gate",
                "capture_method": "synthetic:sync-gate",
                "payload": {
                    "event_type": "tool.call_completed",
                    "status": "ok",
                    "tool_call_id": f"gate-{index}",
                },
                "partial": False,
            },
            content=f"{PRIVATE_CONTENT}-{index}",
        )
    return outbox


def _transport(
    server: LedgerServer,
    *,
    max_attempts: int = 3,
) -> RetryingTransport:
    http = HttpsTransport(
        server.url,
        {
            "CF-Access-Client-Id": CLIENT_ID,
            "CF-Access-Client-Secret": CLIENT_SECRET,
        },
        require_https=False,
    )
    return RetryingTransport(
        http,
        max_attempts=max_attempts,
        base_delay=0,
        max_delay=0,
    )


def _cursor(outbox: Outbox) -> int:
    return int(outbox.get_cursor(DELIVERY_CURSOR_NAME) or 0)


def _complete_stream_failures(
    name: str,
    outbox: Outbox,
    server: LedgerServer,
) -> list[str]:
    failures: list[str] = []
    high_water = outbox.high_water()
    expected = list(range(1, high_water + 1))
    actual = server.sequences()

    if actual != expected:
        failures.append(f"{name}: server sequences {actual} != {expected}")
    if server.high_water != high_water:
        failures.append(
            f"{name}: server high-water {server.high_water} != client {high_water}"
        )
    if _cursor(outbox) != high_water:
        failures.append(
            f"{name}: delivery cursor {_cursor(outbox)} != client {high_water}"
        )
    if len(server.ledger) != high_water:
        failures.append(
            f"{name}: ledger rows {len(server.ledger)} != client {high_water}"
        )

    wire = b"".join(server.bodies)
    if PRIVATE_CONTENT.encode("utf-8") in wire:
        failures.append(f"{name}: plaintext content appeared on the wire")
    if server.headers_seen:
        first_headers = {key.lower(): value for key, value in server.headers_seen[0].items()}
        if first_headers.get("cf-access-client-id") != CLIENT_ID:
            failures.append(f"{name}: client-id header missing")
        if first_headers.get("cf-access-client-secret") != CLIENT_SECRET:
            failures.append(f"{name}: client-secret header missing")

    if VERBOSE:
        print(
            f"      requests={len(server.bodies)} rows={len(server.ledger)} "
            f"server_high_water={server.high_water} cursor={_cursor(outbox)}"
        )
    return failures


def scenario_happy(tmp: Path) -> list[str]:
    outbox = _new_outbox(tmp / "bridge")
    try:
        with ingestion_server() as server:
            result = sync(outbox, _transport(server), max_records=2)
            failures = _complete_stream_failures("happy", outbox, server)
            if result.batches_sent != 3 or result.records_sent != 5:
                failures.append(
                    "happy: expected three batches and five shipped records"
                )
            return failures
    finally:
        outbox.close()


def scenario_dropped_batch(tmp: Path) -> list[str]:
    outbox = _new_outbox(tmp / "bridge")
    try:
        with ingestion_server([DROP_BEFORE_STORE, NORMAL]) as server:
            sync(outbox, _transport(server, max_attempts=2))
            failures = _complete_stream_failures("dropped", outbox, server)
            if len(server.bodies) != 2:
                failures.append("dropped: expected one lost request and one retry")
            elif server.bodies[0] != server.bodies[1]:
                failures.append("dropped: retry did not resend the same batch")
            if not server.ack_history or server.ack_history[-1]["accepted"] != 5:
                failures.append("dropped: retry did not store the complete batch")
            return failures
    finally:
        outbox.close()


def scenario_duplicate_delivery(tmp: Path) -> list[str]:
    outbox = _new_outbox(tmp / "bridge")
    try:
        with ingestion_server([DROP_AFTER_STORE, NORMAL]) as server:
            sync(outbox, _transport(server, max_attempts=2))
            failures = _complete_stream_failures("duplicate", outbox, server)
            if len(server.bodies) != 2:
                failures.append("duplicate: expected the same batch twice")
            elif server.bodies[0] != server.bodies[1]:
                failures.append("duplicate: retry body changed")
            if not server.ack_history or server.ack_history[-1]["duplicates"] != 5:
                failures.append("duplicate: retry was not fully deduplicated")
            return failures
    finally:
        outbox.close()


def scenario_offline_then_online(tmp: Path) -> list[str]:
    outbox = _new_outbox(tmp / "bridge")
    try:
        with ingestion_server([DROP_BEFORE_STORE]) as server:
            offline = push(outbox, _transport(server, max_attempts=1))
            failures: list[str] = []
            if offline.ok or offline.reason != "offline":
                failures.append("offline: failed pass did not report offline")
            if _cursor(outbox) != 0 or server.ledger:
                failures.append("offline: failed pass stored data or moved the cursor")

            online = push(outbox, _transport(server, max_attempts=1))
            if not online.ok:
                failures.append("offline: online pass did not recover")
            failures += _complete_stream_failures("offline", outbox, server)
            return failures
    finally:
        outbox.close()


def scenario_restart_mid_sync(tmp: Path) -> list[str]:
    bridge_home = tmp / "bridge"
    outbox = _new_outbox(bridge_home)
    with ingestion_server([NORMAL, DROP_AFTER_STORE]) as server:
        try:
            try:
                sync(outbox, _transport(server, max_attempts=1), max_records=2)
                failures = ["restart: interrupted sync unexpectedly succeeded"]
            except RetryableTransportError:
                failures = []

            if _cursor(outbox) != 2:
                failures.append(
                    f"restart: pre-restart cursor {_cursor(outbox)} != 2"
                )
            if server.sequences() != [1, 2, 3, 4]:
                failures.append(
                    f"restart: pre-restart server stream {server.sequences()} is wrong"
                )
        finally:
            outbox.close()

        reopened = Outbox.open(bridge_home)
        try:
            sync(reopened, _transport(server, max_attempts=1), max_records=2)
            failures += _complete_stream_failures("restart", reopened, server)
            duplicate_acks = [
                ack for ack in server.ack_history if ack["duplicates"] == 2
            ]
            if not duplicate_acks:
                failures.append("restart: stored unacked batch was not re-sent")
            return failures
        finally:
            reopened.close()


SCENARIOS = [
    ("happy path", scenario_happy),
    ("dropped batch", scenario_dropped_batch),
    ("duplicate delivery", scenario_duplicate_delivery),
    ("offline then online", scenario_offline_then_online),
    ("restart mid-sync", scenario_restart_mid_sync),
]


def main() -> int:
    print("Phase 1 network sync exit-gate (issue #35)")
    print("=" * 48)
    all_failures: list[str] = []
    for name, scenario in SCENARIOS:
        with tempfile.TemporaryDirectory() as directory:
            failures = scenario(Path(directory))
        status = "PASS" if not failures else "FAIL"
        print(f"  [{status}] {name}")
        for failure in failures:
            print(f"         - {failure}")
        all_failures += failures
    print("=" * 48)
    if all_failures:
        print(f"GATE FAILED - {len(all_failures)} assertion(s) failed")
        return 1
    print("GATE PASSED - network delivery is complete, gap-free, and idempotent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
