"""HTTPS transport for ingestion protocol v1.

This module implements the real network :class:`Transport` that
:func:`hermes_flight_recorder.collector.sync.sync` calls. It POSTs one batch
to the hosted ``/ingest`` endpoint, reads the ``202`` acknowledgement, and
maps every failure into one of three classes that the protocol defines:

- **retryable** — a network error, a timeout, ``429``, or any ``5xx``. The
  same batch is safe to send again, so :class:`RetryingTransport` retries it
  with exponential backoff and jitter.
- **auth** — ``401`` or ``403``. The service-token configuration is wrong.
  Stop; do not spin.
- **terminal** — ``400``. The batch is a client defect. Stop and surface it;
  resending the same body cannot help.

The transport ships records exactly as the outbox produced them. The
``content_ciphertext`` field is already encrypted; this module has no key and
no decrypt path, so no plaintext content can leave the host.

Uses only the standard library (``urllib``) to keep the runtime footprint at
one dependency.
"""

from __future__ import annotations

import json
import random
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable

from .sync import Ack, Batch, SyncResult, serialize_batch, sync
from .sync_config import SyncConfig

DEFAULT_TIMEOUT = 30.0
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BASE_DELAY = 0.5
DEFAULT_MAX_DELAY = 30.0

_JSON_CONTENT_TYPE = "application/json"


class TransportError(RuntimeError):
    """A batch could not be delivered and acknowledged."""


class RetryableTransportError(TransportError):
    """A transient failure. The same batch is safe to send again."""


class AuthError(TransportError):
    """The edge rejected the request. Fix the credential; do not retry."""


class TerminalTransportError(TransportError):
    """The server rejected the batch as malformed. A client defect."""


# A seam for tests: the same shape as ``urllib.request.urlopen``.
UrlOpen = Callable[..., Any]


@dataclass
class HttpsTransport:
    """POST one batch to ``/ingest`` and return its acknowledgement.

    ``require_https`` guards production against a plaintext endpoint. Tests
    against a local fake server set it to ``False``.
    """

    ingest_url: str
    headers: dict[str, str]
    timeout: float = DEFAULT_TIMEOUT
    require_https: bool = True
    _urlopen: UrlOpen = urllib.request.urlopen

    def __post_init__(self) -> None:
        if self.require_https and not self.ingest_url.lower().startswith("https://"):
            raise TransportError(
                f"ingest_url must be HTTPS, got {self.ingest_url!r}"
            )

    @classmethod
    def from_config(
        cls,
        config: SyncConfig,
        *,
        timeout: float = DEFAULT_TIMEOUT,
        require_https: bool = True,
        urlopen: UrlOpen | None = None,
    ) -> "HttpsTransport":
        """Build a transport from a loaded :class:`SyncConfig`."""
        kwargs: dict[str, Any] = {
            "ingest_url": config.ingest_url,
            "headers": config.auth_headers(),
            "timeout": timeout,
            "require_https": require_https,
        }
        if urlopen is not None:
            kwargs["_urlopen"] = urlopen
        return cls(**kwargs)

    def send(self, batch: Batch) -> Ack:
        body = serialize_batch(batch)
        request = urllib.request.Request(
            self.ingest_url,
            data=body,
            method="POST",
            headers={"Content-Type": _JSON_CONTENT_TYPE, **self.headers},
        )
        try:
            response = self._urlopen(request, timeout=self.timeout)
        except urllib.error.HTTPError as exc:
            # The server answered with a non-2xx status.
            detail = _read_error_body(exc)
            raise _classify_status(exc.code, detail) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            # No answer reached us: DNS, connection, TLS, or timeout.
            raise RetryableTransportError(f"network error: {exc}") from exc

        with response:
            status = getattr(response, "status", None) or response.getcode()
            payload = response.read()

        if status != 202:
            raise _classify_status(status, payload.decode("utf-8", "replace"))

        return _parse_ack(payload)


@dataclass
class RetryingTransport:
    """Wrap a transport and retry the retryable class with backoff + jitter.

    Auth and terminal failures propagate at once; only a
    :class:`RetryableTransportError` is retried. Backoff is exponential and
    capped, multiplied by full jitter so a fleet does not retry in lockstep.

    ``sleep`` and ``rng`` are seams so a test drives the schedule without real
    time or real randomness.
    """

    inner: Any
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    base_delay: float = DEFAULT_BASE_DELAY
    max_delay: float = DEFAULT_MAX_DELAY
    sleep: Callable[[float], None] = time.sleep
    rng: Callable[[], float] = random.random

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

    def send(self, batch: Batch) -> Ack:
        attempt = 0
        while True:
            try:
                return self.inner.send(batch)
            except RetryableTransportError:
                attempt += 1
                if attempt >= self.max_attempts:
                    raise
                self.sleep(self._delay(attempt))

    def _delay(self, attempt: int) -> float:
        # Full jitter: uniform(0, min(cap, base * 2**(attempt-1))).
        ceiling = min(self.max_delay, self.base_delay * (2 ** (attempt - 1)))
        return ceiling * self.rng()


@dataclass(frozen=True)
class PushOutcome:
    """The result of one offline-tolerant sync pass.

    ``reason`` is ``"ok"`` on success, ``"offline"`` when the network stayed
    down through every retry, or ``"auth"`` when the edge rejected the
    credential. ``result`` is the :class:`SyncResult` on success, else ``None``.
    """

    ok: bool
    reason: str
    result: SyncResult | None


def push(outbox: Any, transport: Any) -> PushOutcome:
    """Run one sync pass and never raise on a network or auth failure.

    The agent keeps working when the network is down. A retryable failure
    that outlives every retry, and an auth failure, both return an outcome
    instead of raising: the outbox keeps the events, the delivery cursor stays
    in place, and the next pass resumes from the last ack. A terminal batch
    defect still propagates, because it is a bug that must be seen.
    """
    try:
        result = sync(outbox, transport)
    except RetryableTransportError:
        return PushOutcome(ok=False, reason="offline", result=None)
    except AuthError:
        return PushOutcome(ok=False, reason="auth", result=None)
    return PushOutcome(ok=True, reason="ok", result=result)


def _classify_status(status: int, detail: str) -> TransportError:
    message = f"ingest returned {status}: {detail.strip()[:500]}"
    if status in (401, 403):
        return AuthError(message)
    if status == 400:
        return TerminalTransportError(message)
    if status == 429 or 500 <= status <= 599:
        return RetryableTransportError(message)
    # Any other unexpected status is terminal: retrying cannot help.
    return TerminalTransportError(message)


def _read_error_body(exc: urllib.error.HTTPError) -> str:
    try:
        return exc.read().decode("utf-8", "replace")
    except Exception:
        return ""


def _parse_ack(payload: bytes) -> Ack:
    try:
        data = json.loads(payload)
    except (ValueError, TypeError) as exc:
        raise TerminalTransportError(
            f"ingest acknowledgement is not JSON: {exc}"
        ) from exc
    if not isinstance(data, dict):
        raise TerminalTransportError("ingest acknowledgement is not an object")
    try:
        accepted = int(data["accepted"])
        duplicates = int(data["duplicates"])
        high_water = int(data["high_water"])
    except (KeyError, TypeError, ValueError) as exc:
        raise TerminalTransportError(
            f"ingest acknowledgement is missing a field: {exc}"
        ) from exc
    return Ack(accepted=accepted, duplicates=duplicates, high_water=high_water)


__all__ = [
    "AuthError",
    "DEFAULT_BASE_DELAY",
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_MAX_DELAY",
    "DEFAULT_TIMEOUT",
    "HttpsTransport",
    "PushOutcome",
    "RetryableTransportError",
    "RetryingTransport",
    "TerminalTransportError",
    "TransportError",
    "push",
]
