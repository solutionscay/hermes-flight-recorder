"""Non-secret configuration for a Flight Recorder installation.

``recorder-config.json`` lives in the Flight Recorder home.  It holds operational
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

from . import atomic_file
from ._common import default_flight_recorder_home
from .sync import DEFAULT_MAX_BYTES, DEFAULT_MAX_RECORDS, MAX_INGEST_BATCH_BYTES

CONFIG_FILENAME = "recorder-config.json"
DEFAULT_MAX_CONTENT_BYTES = 65_536
DEFAULT_MESSAGE_ROLES = ("user", "assistant", "tool")
DEFAULT_KNOWLEDGE_MAX_FILE_BYTES = 4 * 1024 * 1024
DEFAULT_KNOWLEDGE_MAX_FILE_COUNT = 256
DEFAULT_KNOWLEDGE_MAX_ARTIFACT_BYTES = 8 * 1024 * 1024
# Foreground `serve` cadences. Capture polls the fast path often; reconcile
# diffs the durable stores less often. These match the retired systemd timers.
DEFAULT_CAPTURE_INTERVAL_SECONDS = 15.0
DEFAULT_RECONCILE_INTERVAL_SECONDS = 60.0
DEFAULT_SYNC_MAX_BATCHES_PER_TICK = 1
CAPTURE_SOURCE_NAMES = (
    "hook",
    "state_db",
    "cron",
    "kanban",
    "gateway_log",
    "knowledge",
)
DEFAULT_REQUIRED_CAPTURE_SOURCES = ("hook", "state_db")


class RecorderConfigError(RuntimeError):
    """The recorder configuration cannot be read or has an invalid value."""


@dataclass(frozen=True)
class CaptureConfig:
    max_content_bytes: int = DEFAULT_MAX_CONTENT_BYTES
    message_roles: tuple[str, ...] = DEFAULT_MESSAGE_ROLES
    sources: dict[str, bool] = field(default_factory=dict)
    required_sources: tuple[str, ...] = DEFAULT_REQUIRED_CAPTURE_SOURCES
    # How often `serve` runs a capture pass. One-shot `run` ignores this.
    interval_seconds: float = DEFAULT_CAPTURE_INTERVAL_SECONDS

    def __post_init__(self) -> None:
        _validate_sources(self.sources)
        _validate_required_sources(self.required_sources)


@dataclass(frozen=True)
class ReconcileRuntimeConfig:
    # How often `serve` runs a reconcile pass — independent of capture so it
    # can flag capture staleness even when the capture pass is broken.
    interval_seconds: float = DEFAULT_RECONCILE_INTERVAL_SECONDS


@dataclass(frozen=True)
class RetentionConfig:
    enabled: bool = False
    max_age_days: int | None = 30
    max_bytes: int | None = None
    require_delivered: bool = True
    vacuum: str = "auto"


@dataclass(frozen=True)
class KnowledgeConfig:
    # History depth for the content-addressed knowledge store. ``full`` keeps
    # every version (cheap — unchanged files deduplicate); ``latest_only`` keeps
    # only current content and hashes, for a home already versioned in its own
    # git. ``max_versions`` caps the chain under ``full``; None keeps all.
    history: str = "full"
    max_versions: int | None = None
    # These limits bound each scan and the complete knowledge bundle that the
    # collector builds in memory. Files outside the limits remain on disk and
    # appear as explicit omissions in the event metadata.
    max_file_bytes: int = DEFAULT_KNOWLEDGE_MAX_FILE_BYTES
    max_file_count: int = DEFAULT_KNOWLEDGE_MAX_FILE_COUNT
    max_artifact_bytes: int = DEFAULT_KNOWLEDGE_MAX_ARTIFACT_BYTES


@dataclass(frozen=True)
class SyncRuntimeConfig:
    # None preserves the existing CLI's one-pass default.  An explicit value
    # enables the same continuous mode as ``sync --interval``.
    interval_seconds: float | None = None
    max_records: int = DEFAULT_MAX_RECORDS
    max_bytes: int = DEFAULT_MAX_BYTES
    max_batches_per_tick: int = DEFAULT_SYNC_MAX_BATCHES_PER_TICK


@dataclass(frozen=True)
class RecorderConfig:
    capture: CaptureConfig = field(default_factory=CaptureConfig)
    retention: RetentionConfig = field(default_factory=RetentionConfig)
    knowledge: KnowledgeConfig = field(default_factory=KnowledgeConfig)
    sync: SyncRuntimeConfig = field(default_factory=SyncRuntimeConfig)
    reconcile: ReconcileRuntimeConfig = field(default_factory=ReconcileRuntimeConfig)


def config_path(flight_recorder_home: str | os.PathLike[str] | None = None) -> Path:
    """Return the recorder config path inside the Flight Recorder home."""
    home = Path(flight_recorder_home).expanduser() if flight_recorder_home else default_flight_recorder_home()
    return home / CONFIG_FILENAME


def load(flight_recorder_home: str | os.PathLike[str] | None = None) -> RecorderConfig:
    """Load config with environment-over-file-over-default precedence."""
    data = _read_file(config_path(flight_recorder_home))
    capture = _section(data, "capture")
    retention = _section(data, "retention")
    knowledge = _section(data, "knowledge")
    sync = _section(data, "sync")
    reconcile = _section(data, "reconcile")

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
            required_sources=_source_names(
                _value(
                    "HFR_CAPTURE_REQUIRED_SOURCES",
                    capture,
                    "required_sources",
                    DEFAULT_REQUIRED_CAPTURE_SOURCES,
                ),
                "capture.required_sources",
            ),
            interval_seconds=_positive_float(
                _value(
                    "HFR_CAPTURE_INTERVAL_SECONDS",
                    capture,
                    "interval_seconds",
                    DEFAULT_CAPTURE_INTERVAL_SECONDS,
                ),
                "capture.interval_seconds",
            ),
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
            vacuum=_choice(
                _value("HFR_RETENTION_VACUUM", retention, "vacuum", "auto"),
                "retention.vacuum",
                {"auto"},
            ),
        ),
        knowledge=KnowledgeConfig(
            history=_choice(
                _value("HFR_KNOWLEDGE_HISTORY", knowledge, "history", "full"),
                "knowledge.history",
                {"full", "latest_only"},
            ),
            max_versions=_optional_positive_int(
                _value("HFR_KNOWLEDGE_MAX_VERSIONS", knowledge, "max_versions", None),
                "knowledge.max_versions",
            ),
            max_file_bytes=_positive_int(
                _value(
                    "HFR_KNOWLEDGE_MAX_FILE_BYTES",
                    knowledge,
                    "max_file_bytes",
                    DEFAULT_KNOWLEDGE_MAX_FILE_BYTES,
                ),
                "knowledge.max_file_bytes",
            ),
            max_file_count=_positive_int(
                _value(
                    "HFR_KNOWLEDGE_MAX_FILE_COUNT",
                    knowledge,
                    "max_file_count",
                    DEFAULT_KNOWLEDGE_MAX_FILE_COUNT,
                ),
                "knowledge.max_file_count",
            ),
            max_artifact_bytes=_positive_int(
                _value(
                    "HFR_KNOWLEDGE_MAX_ARTIFACT_BYTES",
                    knowledge,
                    "max_artifact_bytes",
                    DEFAULT_KNOWLEDGE_MAX_ARTIFACT_BYTES,
                ),
                "knowledge.max_artifact_bytes",
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
            max_bytes=_bounded_positive_int(
                _value("HFR_SYNC_MAX_BYTES", sync, "max_bytes", DEFAULT_MAX_BYTES),
                "sync.max_bytes",
                maximum=MAX_INGEST_BATCH_BYTES,
            ),
            max_batches_per_tick=_positive_int(
                _value(
                    "HFR_SYNC_MAX_BATCHES_PER_TICK",
                    sync,
                    "max_batches_per_tick",
                    DEFAULT_SYNC_MAX_BATCHES_PER_TICK,
                ),
                "sync.max_batches_per_tick",
            ),
        ),
        reconcile=ReconcileRuntimeConfig(
            interval_seconds=_positive_float(
                _value(
                    "HFR_RECONCILE_INTERVAL_SECONDS",
                    reconcile,
                    "interval_seconds",
                    DEFAULT_RECONCILE_INTERVAL_SECONDS,
                ),
                "reconcile.interval_seconds",
            ),
        ),
    )


def save(
    config: RecorderConfig, flight_recorder_home: str | os.PathLike[str] | None = None
) -> Path:
    """Write the config durably and atomically with mode ``0600``."""
    path = config_path(flight_recorder_home)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "capture": {
            "max_content_bytes": config.capture.max_content_bytes,
            "message_roles": list(config.capture.message_roles),
            "sources": config.capture.sources,
            "required_sources": list(config.capture.required_sources),
            "interval_seconds": config.capture.interval_seconds,
        },
        "retention": {
            "enabled": config.retention.enabled,
            "max_age_days": config.retention.max_age_days,
            "max_bytes": config.retention.max_bytes,
            "require_delivered": config.retention.require_delivered,
            "vacuum": config.retention.vacuum,
        },
        "knowledge": {
            "history": config.knowledge.history,
            "max_versions": config.knowledge.max_versions,
            "max_file_bytes": config.knowledge.max_file_bytes,
            "max_file_count": config.knowledge.max_file_count,
            "max_artifact_bytes": config.knowledge.max_artifact_bytes,
        },
        "sync": {
            "interval_seconds": config.sync.interval_seconds,
            "max_records": config.sync.max_records,
            "max_bytes": config.sync.max_bytes,
            "max_batches_per_tick": config.sync.max_batches_per_tick,
        },
        "reconcile": {
            "interval_seconds": config.reconcile.interval_seconds,
        },
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    try:
        return atomic_file.atomic_write(path, text.encode("utf-8"), mode=0o600)
    except OSError as exc:
        raise RecorderConfigError(
            f"cannot write recorder config at {path}: {exc}"
        ) from exc


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


def _bounded_positive_int(value: Any, name: str, *, maximum: int) -> int:
    result = _positive_int(value, name)
    if result > maximum:
        raise RecorderConfigError(f"{name} must be at most {maximum}")
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


def _positive_float(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise RecorderConfigError(f"{name} must be a positive number")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise RecorderConfigError(f"{name} must be a positive number") from exc
    if result <= 0:
        raise RecorderConfigError(f"{name} must be a positive number")
    return result


def _boolean(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in {"true", "false"}:
        return value.lower() == "true"
    raise RecorderConfigError(f"{name} must be true or false")


def _choice(value: Any, name: str, choices: set[str]) -> str:
    if not isinstance(value, str) or value not in choices:
        expected = ", ".join(sorted(repr(choice) for choice in choices))
        raise RecorderConfigError(f"{name} must be one of: {expected}")
    return value


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
    _validate_sources(value)
    return dict(value)


def _validate_sources(value: Any) -> None:
    if not isinstance(value, dict) or not all(
        isinstance(k, str) and isinstance(v, bool) for k, v in value.items()
    ):
        raise RecorderConfigError("capture.sources must map source names to booleans")
    unknown = sorted(set(value) - set(CAPTURE_SOURCE_NAMES))
    if unknown:
        names = ", ".join(repr(name) for name in unknown)
        supported = ", ".join(CAPTURE_SOURCE_NAMES)
        raise RecorderConfigError(
            f"capture.sources contains unknown source(s): {names}; supported: {supported}"
        )


def _source_names(value: Any, name: str) -> tuple[str, ...]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError as exc:
            raise RecorderConfigError(f"{name} must be a JSON array") from exc
    if not isinstance(value, (list, tuple)) or not all(
        isinstance(source, str) and source for source in value
    ):
        raise RecorderConfigError(f"{name} must be an array of source names")
    result = tuple(value)
    _validate_required_sources(result)
    return result


def _validate_required_sources(value: Any) -> None:
    if not isinstance(value, tuple) or not all(
        isinstance(source, str) and source for source in value
    ):
        raise RecorderConfigError("capture.required_sources must be a tuple of source names")
    if len(set(value)) != len(value):
        raise RecorderConfigError("capture.required_sources contains duplicate source names")
    unknown = sorted(set(value) - set(CAPTURE_SOURCE_NAMES))
    if unknown:
        names = ", ".join(repr(name) for name in unknown)
        supported = ", ".join(CAPTURE_SOURCE_NAMES)
        raise RecorderConfigError(
            "capture.required_sources contains unknown source(s): "
            f"{names}; supported: {supported}"
        )


def source_enabled(config: CaptureConfig, name: str) -> bool:
    """Return whether a supported capture source is enabled.

    Missing keys are enabled so older and partial configuration files retain
    the full capture behavior.
    """
    if name not in CAPTURE_SOURCE_NAMES:
        raise RecorderConfigError(f"unknown capture source: {name}")
    return config.sources.get(name, True)


def source_required(config: CaptureConfig, name: str) -> bool:
    """Return whether an enabled source must be healthy."""
    return source_enabled(config, name) and name in config.required_sources


__all__ = [
    "CAPTURE_SOURCE_NAMES",
    "CONFIG_FILENAME",
    "DEFAULT_REQUIRED_CAPTURE_SOURCES",
    "DEFAULT_CAPTURE_INTERVAL_SECONDS",
    "DEFAULT_MAX_CONTENT_BYTES",
    "DEFAULT_RECONCILE_INTERVAL_SECONDS",
    "DEFAULT_SYNC_MAX_BATCHES_PER_TICK",
    "CaptureConfig",
    "KnowledgeConfig",
    "ReconcileRuntimeConfig",
    "RecorderConfig",
    "RecorderConfigError",
    "RetentionConfig",
    "SyncRuntimeConfig",
    "source_enabled",
    "source_required",
    "config_path",
    "load",
    "save",
]
