"""Fingerprint keys, suppression baselines, and finding preparation."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from . import atomic_file, secret_detector
from .recorder_config import SecurityConfig

FINGERPRINT_KEY_FILENAME = "secret-scan.key"
_FINGERPRINT_RE = re.compile(r"^[A-Za-z0-9_-]{22}$")


class SecurityScanError(RuntimeError):
    """The local scanner state is missing or invalid."""


@dataclass(frozen=True)
class FindingMatch:
    type: str
    target: str
    byte_start: int
    byte_end: int
    fingerprint: str


@dataclass(frozen=True)
class FindingResult:
    status: str
    matches: tuple[FindingMatch, ...]
    error_type: str | None = None


def fingerprint_key_path(home: str | os.PathLike[str]) -> Path:
    return Path(home) / FINGERPRINT_KEY_FILENAME


def ensure_fingerprint_key(home: str | os.PathLike[str]) -> bytes:
    path = fingerprint_key_path(home)
    if path.is_symlink():
        raise SecurityScanError(f"refusing symbolic link for fingerprint key: {path}")
    if not path.exists():
        try:
            atomic_file.atomic_write(path, os.urandom(32), mode=0o600)
        except OSError as exc:
            raise SecurityScanError(f"cannot create fingerprint key at {path}") from exc
    try:
        value = path.read_bytes()
        os.chmod(path, 0o600)
    except OSError as exc:
        raise SecurityScanError(f"cannot read fingerprint key at {path}") from exc
    if len(value) != 32:
        raise SecurityScanError(f"fingerprint key at {path} must contain 32 bytes")
    return value


def baseline_path(home: str | os.PathLike[str], config: SecurityConfig) -> Path:
    return Path(home) / config.secret_scan_baseline


def _read_baseline(path: Path) -> set[tuple[str, str]]:
    if not path.exists():
        return set()
    data = atomic_file.read_json_object(
        path,
        error=SecurityScanError,
        description="secret scan baseline",
    )
    if data.get("version") != 1 or not isinstance(data.get("suppressions"), list):
        raise SecurityScanError(f"secret scan baseline at {path} has an invalid format")
    result: set[tuple[str, str]] = set()
    for item in data["suppressions"]:
        if not isinstance(item, dict) or set(item) != {"type", "fingerprint"}:
            raise SecurityScanError(f"secret scan baseline at {path} has an invalid entry")
        detector_type = item["type"]
        fingerprint = item["fingerprint"]
        _validate_suppression(detector_type, fingerprint)
        result.add((detector_type, fingerprint))
    return result


def _validate_suppression(detector_type: str, fingerprint: str) -> None:
    if not isinstance(detector_type, str) or not detector_type or len(detector_type) > 128:
        raise SecurityScanError("suppression type must be a non-empty detector name")
    if not isinstance(fingerprint, str) or not _FINGERPRINT_RE.fullmatch(fingerprint):
        raise SecurityScanError("suppression fingerprint must be 22 base64url characters")


def update_suppression(
    home: str | os.PathLike[str],
    config: SecurityConfig,
    detector_type: str,
    fingerprint: str,
    *,
    add: bool,
) -> bool:
    _validate_suppression(detector_type, fingerprint)
    path = baseline_path(home, config)
    suppressions = _read_baseline(path)
    item = (detector_type, fingerprint)
    changed = item not in suppressions if add else item in suppressions
    if add:
        suppressions.add(item)
    else:
        suppressions.discard(item)
    atomic_file.write_json_object(
        path,
        {
            "version": 1,
            "suppressions": [
                {"type": item_type, "fingerprint": item_fingerprint}
                for item_type, item_fingerprint in sorted(suppressions)
            ],
        },
        error=SecurityScanError,
        description="secret scan baseline",
    )
    return changed


def scan_targets(
    home: str | os.PathLike[str],
    config: SecurityConfig,
    targets: Mapping[str, bytes],
) -> FindingResult:
    if not config.secret_scan_enabled or not targets:
        return FindingResult("complete", ())
    try:
        key = ensure_fingerprint_key(home)
        suppressed = _read_baseline(baseline_path(home, config))
        result = secret_detector.scan(
            targets,
            max_bytes=config.secret_scan_max_bytes,
            deadline_ms=config.secret_scan_deadline_ms,
        )
        if result.status == "failed":
            return FindingResult("failed", (), result.error_type)
        findings = []
        for match in result.matches:
            digest = hmac.new(key, match.value, hashlib.sha256).digest()[:16]
            fingerprint = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
            if (match.type, fingerprint) in suppressed:
                continue
            findings.append(
                FindingMatch(
                    match.type,
                    match.target,
                    match.byte_start,
                    match.byte_end,
                    fingerprint,
                )
            )
        return FindingResult(result.status, tuple(findings))
    except Exception as exc:
        return FindingResult("failed", (), type(exc).__name__)


def finding_content(result: FindingResult) -> bytes:
    value = {
        "matches": [
            {
                "type": item.type,
                "target": item.target,
                "byte_start": item.byte_start,
                "byte_end": item.byte_end,
                "fingerprint": item.fingerprint,
            }
            for item in result.matches
        ]
    }
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


__all__ = [
    "FINGERPRINT_KEY_FILENAME",
    "FindingMatch",
    "FindingResult",
    "SecurityScanError",
    "ensure_fingerprint_key",
    "fingerprint_key_path",
    "finding_content",
    "scan_targets",
    "update_suppression",
]
