"""Read-only capture of terminal model-provider failures from ``agent.log``.

Hermes's gateway hooks expose an ``agent:end`` notification but not the
underlying provider exception.  The gateway writes one terminal log record
after retry exhaustion, including the Hermes session id and non-sensitive
routing metadata.  This adapter turns that record into ``model.call_failed``
without modifying the Hermes runtime or reading message content.

The raw provider summary is encrypted in the outbox.  Only a small, useful
classification (provider, model, retry count, and HTTP status where present)
is plaintext.
"""

from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from ._common import (
    append_and_count,
    build_record,
    read_home_mode,
    resolve_hermes_home,
    runtime_stamp,
)

_CURSOR = "gateway-log:agent.log"
_FAILURE = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) "
    r"ERROR \[(?P<session_id>[^\]]+)\] agent\.conversation_loop: "
    r"API call failed after (?P<attempts>\d+) retries\. (?P<summary>.*?)"
    r" \| provider=(?P<provider>\S+) model=(?P<model>\S+)"
)
_HTTP_STATUS = re.compile(r"\bHTTP\s+(\d{3})\b", re.IGNORECASE)


def poll(outbox: Any, hermes_home: str | Path | None = None) -> dict[str, int]:
    """Capture newly appended terminal provider failures from ``agent.log``."""
    home = resolve_hermes_home(hermes_home)
    path = home / "logs" / "agent.log"
    if not path.exists():
        return {}

    cursor = _read_cursor(outbox, path)
    counts: dict[str, int] = defaultdict(int)
    offset = cursor
    with path.open("rb") as fh:
        fh.seek(cursor)
        while True:
            line = fh.readline()
            if not line:
                break
            # Do not advance past a line that the writer may still be appending.
            if not line.endswith(b"\n"):
                break
            offset += len(line)
            _capture_line(
                outbox,
                line.decode("utf-8", errors="replace").rstrip("\r\n"),
                counts,
                home,
            )

    if offset != cursor:
        outbox.set_meta(_CURSOR, f"{path.stat().st_ino}:{offset}")
    return dict(counts)


def _read_cursor(outbox: Any, path: Path) -> int:
    """Return a safe byte offset, resetting after rotation or truncation."""
    raw = outbox.get_meta(_CURSOR)
    if raw is None:
        return 0
    try:
        inode_text, offset_text = raw.split(":", 1)
        offset = int(offset_text)
    except (TypeError, ValueError):
        return 0
    stat = path.stat()
    if inode_text != str(stat.st_ino) or offset < 0 or offset > stat.st_size:
        return 0
    return offset


def _capture_line(outbox: Any, line: str, counts: dict[str, int], home: Path) -> None:
    match = _FAILURE.match(line)
    if match is None:
        return
    fields = match.groupdict()
    summary = fields["summary"]
    http_status = _status(summary)
    occurred_at = _to_epoch(fields["timestamp"])
    payload: dict[str, Any] = {
        "provider": fields["provider"],
        "model": fields["model"],
        "attempts": int(fields["attempts"]),
        "error_class": _error_class(http_status),
    }
    if http_status is not None:
        payload["http_status"] = http_status
    fingerprint = hashlib.sha256(line.encode("utf-8")).hexdigest()
    record = build_record(
        event_type="model.call_failed",
        occurred_at=occurred_at,
        source="logs:agent.log",
        capture_method="poll:gateway-log",
        runtime=runtime_stamp("model", home_mode=read_home_mode(home)),
        correlation_id=fields["session_id"],
        session_id=fields["session_id"],
        payload=payload,
    )
    append_and_count(
        outbox,
        counts,
        record,
        content=summary,
        dedup_key=f"gateway-log:model.call_failed:{fields['session_id']}:{fingerprint}",
    )


def _to_epoch(value: str) -> float:
    """Interpret Hermes's local, millisecond log timestamp as local time."""
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S,%f").astimezone().timestamp()


def _status(summary: str) -> int | None:
    match = _HTTP_STATUS.search(summary)
    return int(match.group(1)) if match else None


def _error_class(http_status: int | None) -> str:
    if http_status == 400:
        return "invalid_request"
    if http_status in (401, 403):
        return "authentication"
    if http_status == 404:
        return "not_found"
    if http_status == 408:
        return "timeout"
    if http_status == 429:
        return "rate_limited"
    if http_status is not None and 500 <= http_status <= 599:
        return "provider_server"
    return "unknown"
