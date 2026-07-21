"""Non-secret configuration for a Flight Recorder installation.

``recorder-config.json`` lives in the Bridge home.  It holds operational
settings that are safe to keep alongside the outbox; the ingest URL and
Cloudflare Access credential deliberately remain in :mod:`sync_config`.

Every setting has a built-in default.  An environment value overrides the
file, which overrides that default.  Missing sections and keys are therefore
safe during upgrades.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ._common import default_bridge_home
from .sync import DEFAULT_MAX_BYTES, DEFAULT_MAX_RECORDS

CONFIG_FILENAME = "recorder-config.json"
DEFAULT_MAX_CONTENT_BYTES = 65_536
DEFAULT_MESSAGE_ROLES = ("user", "assistant", "tool")


class RecorderConfigError(RuntimeError):
    """The recorder configuration cannot be read or has an invalid value."""


@dataclass(frozen=True)
class CaptureConfig:
    max_content_bytes: int = DEFAULT_MAX_CONTENT_BYTES
    message_roles: tuple[str, ...] = DEFAULT_MESSAGE_ROLES
    sources: dict[str, bool] = field(default_factory=dict)


@dataclass(frozen=True)
class RetentionConfig:
    enabled: bool = False
    max_age_days: int | None = 30
    max_bytes: int | None = None
    require_delivered: bool = True


@dataclass(frozen=True)
class SyncRuntimeConfig:
    # None preserves the existing CLI's one-pass default.  An explicit value
    # enables the same continuous mode as ``sync --interval``.
    interval_seconds: float | None = None
    max_records: int = DEFAULT_MAX_RECORDS
    max_bytes: int = DEFAULT_MAX_BYTES


@dataclass(frozen=True)
class RecorderConfig:
    capture: CaptureConfig = field(default_factory=CaptureConfig)
    retention: RetentionConfig = field(default_factory=RetentionConfig)
    sync: SyncRuntimeConfig = field(default_factory=SyncRuntimeConfig)


def config_path(bridge_home: str | os.PathLike[str] | None = None) -> Path:
    """Return the recorder config path inside the Bridge home."""
    home = Path(bridge_home).expanduser() if bridge_home else default_bridge_home()
    return home / CONFIG_FILENAME


def load(bridge_home: str | os.PathLike[str] | None = None) -> RecorderConfig:
    """Load config with environment-over-file-over-default precedence."""
    data = _read_file(config_path(bridge_home))
    capture = _section(data, "capture")
    retention = _section(data, "retention")
    sync = _section(data, "sync")

    return RecorderConfig(
        capture=CaptureConfig(
            max_content_bytes=_positive_int(
                _value(
                    "HFR_CAPTURE_MAX_CONTENT_BYTES",
                    capture,
                    "max_content_bytes",
                    DEFAULT_MAX_CONTENT_BYTES,
                ),
                "capture.max_content_bytes",
            ),
            message_roles=_roles(
                _value(
                    "HFR_CAPTURE_MESSAGE_ROLES",
                    capture,
                    "message_roles",
                    DEFAULT_MESSAGE_ROLES,
                )
            ),
            sources=_sources(_value("HFR_CAPTURE_SOURCES", capture, "sources", {})),
        ),
        retention=RetentionConfig(
            enabled=_boolean(
                _value("HFR_RETENTION_ENABLED", retention, "enabled", False),
                "retention.enabled",
            ),
            max_age_days=_optional_positive_int(
                _value("HFR_RETENTION_MAX_AGE_DAYS", retention, "max_age_days", 30),
                "retention.max_age_days",
            ),
            max_bytes=_optional_positive_int(
                _value("HFR_RETENTION_MAX_BYTES", retention, "max_bytes", None),
                "retention.max_bytes",
            ),
            require_delivered=_boolean(
                _value("HFR_RETENTION_REQUIRE_DELIVERED", retention, "require_delivered", True),
                "retention.require_delivered",
            ),
        ),
        sync=SyncRuntimeConfig(
            interval_seconds=_optional_positive_float(
                _value("HFR_SYNC_INTERVAL_SECONDS", sync, "interval_seconds", None),
                "sync.interval_seconds",
            ),
            max_records=_positive_int(
                _value("HFR_SYNC_MAX_RECORDS", sync, "max_records", DEFAULT_MAX_RECORDS),
                "sync.max_records",
            ),
            max_bytes=_positive_int(
                _value("HFR_SYNC_MAX_BYTES", sync, "max_bytes", DEFAULT_MAX_BYTES),
                "sync.max_bytes",
            ),
        ),
    )


def save(
    config: RecorderConfig, bridge_home: str | os.PathLike[str] | None = None
) -> Path:
    """Write config atomically enough for local use, with mode ``0600``."""
    path = config_path(bridge_home)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "capture": {
            "max_content_bytes": config.capture.max_content_bytes,
            "message_roles": list(config.capture.message_roles),
            "sources": config.capture.sources,
        },
        "retention": {
            "enabled": config.retention.enabled,
            "max_age_days": config.retention.max_age_days,
            "max_bytes": config.retention.max_bytes,
            "require_delivered": config.retention.require_delivered,
        },
        "sync": {
            "interval_seconds": config.sync.interval_seconds,
            "max_records": config.sync.max_records,
            "max_bytes": config.sync.max_bytes,
        },
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, text.encode("utf-8"))
    finally:
        os.close(fd)
    os.chmod(path, 0o600)
    return path


def _read_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, OSError) as exc:
        raise RecorderConfigError(
            f"cannot read recorder config at {path}: {exc}"
        ) from exc
    if not isinstance(data, dict):
        raise RecorderConfigError(f"recorder config at {path} is not a JSON object")
    return data


def _section(data: dict[str, Any], name: str) -> dict[str, Any]:
    value = data.get(name, {})
    if not isinstance(value, dict):
        raise RecorderConfigError(f"{name} must be an object")
    return value


def _value(env_name: str, section: dict[str, Any], key: str, default: Any) -> Any:
    value = os.environ.get(env_name)
    return value if value not in (None, "") else section.get(key, default)


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise RecorderConfigError(f"{name} must be a positive integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise RecorderConfigError(f"{name} must be a positive integer") from exc
    if result < 1 or (isinstance(value, float) and not value.is_integer()):
        raise RecorderConfigError(f"{name} must be a positive integer")
    return result


def _optional_positive_int(value: Any, name: str) -> int | None:
    return None if value is None else _positive_int(value, name)


def _optional_positive_float(value: Any, name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise RecorderConfigError(f"{name} must be a positive number or null")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise RecorderConfigError(f"{name} must be a positive number or null") from exc
    if result <= 0:
        raise RecorderConfigError(f"{name} must be a positive number or null")
    return result


def _boolean(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in {"true", "false"}:
        return value.lower() == "true"
    raise RecorderConfigError(f"{name} must be true or false")


def _roles(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError as exc:
            raise RecorderConfigError("capture.message_roles must be a JSON array") from exc
    if not isinstance(value, (list, tuple)) or not all(
        isinstance(role, str) and role for role in value
    ):
        raise RecorderConfigError(
            "capture.message_roles must be an array of non-empty strings"
        )
    return tuple(value)


def _sources(value: Any) -> dict[str, bool]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError as exc:
            raise RecorderConfigError("capture.sources must be a JSON object") from exc
    if not isinstance(value, dict) or not all(
        isinstance(k, str) and isinstance(v, bool) for k, v in value.items()
    ):
        raise RecorderConfigError("capture.sources must map source names to booleans")
    return dict(value)


__all__ = [
    "CONFIG_FILENAME",
    "CaptureConfig",
    "DEFAULT_MAX_CONTENT_BYTES",
    "RecorderConfig",
    "RecorderConfigError",
    "RetentionConfig",
    "SyncRuntimeConfig",
    "config_path",
    "load",
    "save",
]
