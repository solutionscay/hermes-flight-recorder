"""Shared helpers for the durable-state adapters.

Build producer records (the envelope fields a producer fills in; the
outbox stamps event_id, installation_id, producer_sequence, recorded_at)
and normalize Hermes timestamps.
"""

from __future__ import annotations

import datetime
from pathlib import Path
from typing import Any


def resolve_hermes_home(hermes_home: str | Path | None) -> Path:
    """The Hermes data root: explicit arg, then $HERMES_HOME, then ~/.hermes."""
    import os

    if hermes_home:
        return Path(hermes_home).expanduser()
    env = os.environ.get("HERMES_HOME")
    return Path(env).expanduser() if env else Path.home() / ".hermes"


def to_epoch(value: Any) -> float | None:
    """Normalize a Hermes timestamp to epoch seconds.

    Hermes uses two shapes: epoch floats (state.db) and ISO 8601 strings
    with a timezone (cron executions.db). Return None for a missing value.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return datetime.datetime.fromisoformat(value).timestamp()


def runtime_stamp(kind: str, home_mode: str | None = None) -> dict[str, Any]:
    """A minimal runtime inventory stamp. Best-effort in the POC.

    ``home_mode`` is the Hermes ``terminal.home_mode`` policy (see
    ``read_home_mode``). It is plaintext operational metadata that explains
    where tools run and which git identity they use. Omitted when absent, so
    the field is additive against envelope v1.
    """
    stamp: dict[str, Any] = {"kind": kind, "engine": "standard"}
    if home_mode is not None:
        stamp["home_mode"] = home_mode
    return stamp


# terminal.home_mode aliases Hermes normalizes to its canonical values.
# See hermes_constants.get_subprocess_home in the Hermes source.
_HOME_MODE_ALIASES = {
    "isolated": "profile",
    "profile_home": "profile",
    "profile-home": "profile",
    "host": "real",
    "user": "real",
    "real_home": "real",
    "real-home": "real",
}
_HOME_MODE_CANONICAL = ("auto", "real", "profile")


def read_home_mode(hermes_home: str | Path | None = None) -> str:
    """The Hermes ``terminal.home_mode`` policy, normalized. Best-effort.

    Reads ``config.yaml`` in the Hermes home and returns the canonical value
    (``auto`` | ``real`` | ``profile``). Returns ``"auto"`` — the Hermes
    default — when the file, the ``terminal`` block, or the key is absent,
    blank, unreadable, or an unrecognized value. Never raises. Captures only
    the enum; never the resolved HOME path (that is sensitive content).
    """
    raw = _read_terminal_home_mode(resolve_hermes_home(hermes_home) / "config.yaml")
    if not raw:
        return "auto"
    mode = raw.strip().lower()
    mode = _HOME_MODE_ALIASES.get(mode, mode)
    return mode if mode in _HOME_MODE_CANONICAL else "auto"


def _read_terminal_home_mode(config_path: Path) -> str | None:
    """The raw ``terminal.home_mode`` string from ``config.yaml``, or None.

    A tiny standard-library scanner — the project keeps its runtime deps to
    ``cryptography`` alone, so it does not import a YAML parser. It finds the
    top-level ``terminal:`` block and reads its ``home_mode:`` child, and it
    reads nothing else, so adjacent secret blocks (bot tokens) are never
    touched. Any read or parse trouble yields None.
    """
    try:
        text = config_path.read_text(encoding="utf-8")
    except OSError:
        return None
    in_terminal = False
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if line[:1] not in (" ", "\t"):  # a top-level key at column 0
            in_terminal = stripped.split(":", 1)[0].strip() == "terminal"
            continue
        if in_terminal and stripped.split(":", 1)[0].strip() == "home_mode":
            value = stripped.partition(":")[2]
            return value.split("#", 1)[0].strip().strip("'\"") or None
    return None


def root_session(session_id: str | None, parent_map: dict[str, str | None]) -> str | None:
    """Walk parent_session_id to the top ancestor (the correlation root)."""
    seen: set[str] = set()
    sid = session_id
    while sid is not None and parent_map.get(sid) and parent_map[sid] not in seen:
        seen.add(sid)
        sid = parent_map[sid]
    return sid


def build_record(
    *,
    event_type: str,
    occurred_at: float,
    source: str,
    capture_method: str,
    runtime: dict[str, Any],
    correlation_id: str,
    payload: dict[str, Any] | None = None,
    session_id: str | None = None,
    session_key: str | None = None,
    parent_session_id: str | None = None,
    invocation_id: str | None = None,
    causation_id: str | None = None,
    tenant_id: str = "default",
    profile: str = "default",
    partial: bool = False,
) -> dict[str, Any]:
    """Assemble a producer record. The outbox stamps the rest."""
    pl = dict(payload or {})
    pl["event_type"] = event_type
    rec: dict[str, Any] = {
        "occurred_at": float(occurred_at),
        "tenant_id": tenant_id,
        "profile": profile or "default",
        "runtime": runtime,
        "correlation_id": correlation_id,
        "source": source,
        "capture_method": capture_method,
        "payload": pl,
        "partial": partial,
    }
    for key, val in (
        ("session_id", session_id),
        ("session_key", session_key),
        ("parent_session_id", parent_session_id),
        ("invocation_id", invocation_id),
        ("causation_id", causation_id),
    ):
        if val is not None:
            rec[key] = val
    return rec
