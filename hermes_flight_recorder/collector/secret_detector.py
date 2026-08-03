"""Bounded ``detect-secrets`` adapter for captured plaintext.

The upstream scanner works with text and line numbers. This adapter scans
in-memory text, applies the recorder policy, and maps every result back to a
zero-based, half-open byte range in the original plaintext.
"""

from __future__ import annotations

import re
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass

from detect_secrets.core.scan import scan_line
from detect_secrets.plugins.base import RegexBasedDetector
from detect_secrets.plugins.high_entropy_strings import (
    Base64HighEntropyString,
    HexHighEntropyString,
)
from detect_secrets.settings import get_plugins, transient_settings

DETECTOR_NAME = "detect-secrets-1.5.0"
CHUNK_BYTES = 16 * 1024
CHUNK_OVERLAP = 2 * 1024
MAX_CANDIDATE_BYTES = 16 * 1024
MAX_MATCHES = 512

_SETTINGS_LOCK = threading.RLock()
_BASE64_DETECTOR = Base64HighEntropyString(limit=4.5)
_HEX_DETECTOR = HexHighEntropyString(limit=3.0)
_ENTROPY_TYPES = {
    _BASE64_DETECTOR.secret_type: (_BASE64_DETECTOR, 20),
    _HEX_DETECTOR.secret_type: (_HEX_DETECTOR, 32),
}
_PLUGIN_CONFIG = (
    "ArtifactoryDetector",
    "AWSKeyDetector",
    "AzureStorageKeyDetector",
    "Base64HighEntropyString",
    "BasicAuthDetector",
    "CloudantDetector",
    "DiscordBotTokenDetector",
    "GitHubTokenDetector",
    "GitLabTokenDetector",
    "HexHighEntropyString",
    "IbmCloudIamDetector",
    "IbmCosHmacDetector",
    "JwtTokenDetector",
    "KeywordDetector",
    "MailchimpDetector",
    "NpmDetector",
    "OpenAIDetector",
    "PypiTokenDetector",
    "SendGridDetector",
    "SlackDetector",
    "SoftlayerDetector",
    "SquareOAuthDetector",
    "StripeDetector",
    "TelegramBotTokenDetector",
    "TwilioKeyDetector",
)
_SETTINGS = {"plugins_used": [{"name": name} for name in _PLUGIN_CONFIG]}

_TYPE_NAMES = {
    "AWS Access Key": "aws_access_key_id",
    "Base64 High Entropy String": "high_entropy_string",
    "GitHub Token": "github_token",
    "GitLab Token": "gitlab_token",
    "Hex High Entropy String": "high_entropy_string",
    "NPM tokens": "npm_token",
    "OpenAI Token": "openai_api_key",
    "Secret Keyword": "secret_assignment",
    "Slack Token": "slack_token",
    "Stripe Access Key": "stripe_secret_key",
}
_IDENTIFIER_RE = re.compile(r"(?<![A-Za-z0-9_.-])([A-Za-z_][A-Za-z0-9_.-]*)\s*[:=]\s*")
_UNQUOTED_VALUE_RE = re.compile(r"[^\s#;,]{8,1024}")
_SUSPECT_SUFFIXES = frozenset(
    {
        "auth",
        "credential",
        "credentials",
        "key",
        "pass",
        "passwd",
        "password",
        "pwd",
        "secret",
        "token",
    }
)
_UUID_RE = re.compile(
    r"(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
_PEM_RE = re.compile(
    rb"-----BEGIN ((?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY)-----"
    rb"[\s\S]{1,16000}?-----END \1-----"
)


@dataclass(frozen=True)
class Match:
    type: str
    target: str
    byte_start: int
    byte_end: int
    value: bytes


@dataclass(frozen=True)
class ScanResult:
    status: str
    matches: tuple[Match, ...]
    error_type: str | None = None


def _type_name(upstream_name: str) -> str:
    if upstream_name in _TYPE_NAMES:
        return _TYPE_NAMES[upstream_name]
    value = re.sub(r"[^a-z0-9]+", "_", upstream_name.lower()).strip("_")
    return value[:128]


def _is_suspect_identifier(identifier: str) -> bool:
    parts = tuple(part for part in re.split(r"[_.-]+", identifier.lower()) if part)
    return bool(parts) and (
        parts[-1] in _SUSPECT_SUFFIXES or identifier.lower() in _SUSPECT_SUFFIXES
    )


def _assignment_values(text: str) -> Iterable[tuple[int, int, str]]:
    for assignment in _IDENTIFIER_RE.finditer(text):
        if not _is_suspect_identifier(assignment.group(1)):
            continue
        value_start = assignment.end()
        if value_start >= len(text):
            continue
        quote = text[value_start] if text[value_start] in {"'", '"'} else None
        if quote:
            value_end = text.find(quote, value_start + 1)
            if value_end < 0 or value_end - value_start - 1 < 8:
                continue
            yield value_start + 1, value_end, text[value_start + 1 : value_end]
            continue
        value = _UNQUOTED_VALUE_RE.match(text, value_start)
        if value:
            yield value.start(), value.end(), value.group(0)


def _byte_offset(text: str, character_offset: int) -> int:
    return len(text[:character_offset].encode("utf-8", errors="surrogateescape"))


def _add_text_match(
    matches: dict[tuple[str, int, int], Match],
    *,
    detector_type: str,
    target: str,
    segment: str,
    segment_byte_start: int,
    character_start: int,
    character_end: int,
) -> None:
    value = segment[character_start:character_end].encode(
        "utf-8", errors="surrogateescape"
    )
    if not value or len(value) > MAX_CANDIDATE_BYTES:
        return
    byte_start = segment_byte_start + _byte_offset(segment, character_start)
    byte_end = byte_start + len(value)
    key = (target, byte_start, byte_end)
    candidate = Match(detector_type, target, byte_start, byte_end, value)
    existing = matches.get(key)
    if existing is None or existing.type == "high_entropy_string":
        matches[key] = candidate


def _entropy_candidate(upstream_type: str, value: str) -> bool:
    detector, minimum_length = _ENTROPY_TYPES[upstream_type]
    if len(value) < minimum_length or _UUID_RE.fullmatch(value):
        return False
    # An eager scan can return a complete NAME=value expression. The generic
    # assignment policy records only its value.
    if "=" in value[:-2]:
        return False
    return detector.calculate_shannon_entropy(value) > detector.entropy_limit


def _regex_value(match: re.Match[str]) -> tuple[int, int]:
    groups = [
        (match.start(index), match.end(index))
        for index, value in enumerate(match.groups(), start=1)
        if value
    ]
    if not groups:
        return match.span(0)
    start, end = max(groups, key=lambda span: span[1] - span[0])
    if end - start < 8 and match.end(0) - match.start(0) > end - start:
        return match.span(0)
    return start, end


def _scan_segment(
    matches: dict[tuple[str, int, int], Match],
    *,
    target: str,
    segment: str,
    segment_byte_start: int,
) -> None:
    findings = tuple(scan_line(segment))
    regex_plugin_types: set[str] = set()

    for plugin in get_plugins():
        if not isinstance(plugin, RegexBasedDetector):
            continue
        regex_plugin_types.add(plugin.secret_type)
        for pattern in plugin.denylist:
            for candidate in pattern.finditer(segment):
                start, end = _regex_value(candidate)
                detector_type = _type_name(plugin.secret_type)
                candidate_value = segment[start:end]
                if plugin.secret_type == "AWS Access Key" and not re.fullmatch(
                    r"(?:A3T[A-Z0-9]|ABIA|ACCA|AKIA|ASIA)[0-9A-Z]{16}",
                    candidate_value,
                ):
                    detector_type = "aws_secret_access_key"
                _add_text_match(
                    matches,
                    detector_type=detector_type,
                    target=target,
                    segment=segment,
                    segment_byte_start=segment_byte_start,
                    character_start=start,
                    character_end=end,
                )

    for finding in findings:
        value = finding.secret_value
        if not value or finding.type in regex_plugin_types:
            continue
        if finding.type in _ENTROPY_TYPES and not _entropy_candidate(
            finding.type, value
        ):
            continue
        start = 0
        while True:
            start = segment.find(value, start)
            if start < 0:
                break
            _add_text_match(
                matches,
                detector_type=_type_name(finding.type),
                target=target,
                segment=segment,
                segment_byte_start=segment_byte_start,
                character_start=start,
                character_end=start + len(value),
            )
            start += max(1, len(value))

    for start, end, _value in _assignment_values(segment):
        _add_text_match(
            matches,
            detector_type="generic_secret_assignment",
            target=target,
            segment=segment,
            segment_byte_start=segment_byte_start,
            character_start=start,
            character_end=end,
        )


def _segments(raw: bytes) -> Iterable[tuple[int, bytes]]:
    start = 0
    while start < len(raw):
        end = min(start + CHUNK_BYTES, len(raw))
        yield start, raw[start:end]
        if end >= len(raw):
            break
        start = end - CHUNK_OVERLAP


def scan(
    targets: Mapping[str, bytes] | Iterable[tuple[str, bytes]],
    *,
    max_bytes: int,
    deadline_ms: int,
    clock: Callable[[], float] = time.monotonic,
) -> ScanResult:
    """Scan bounded in-memory targets with ``detect-secrets`` and local policy."""
    deadline = clock() + (deadline_ms / 1000.0)
    remaining = max_bytes
    partial = False
    matches: dict[tuple[str, int, int], Match] = {}
    items = targets.items() if isinstance(targets, Mapping) else targets

    try:
        with _SETTINGS_LOCK, transient_settings(_SETTINGS):
            for target, value in items:
                if not isinstance(target, str) or not isinstance(value, bytes):
                    raise TypeError("scan targets must map strings to bytes")
                if remaining <= 0:
                    partial = True
                    break
                target_limit = min(len(value), remaining)
                if target_limit < len(value):
                    partial = True
                remaining -= target_limit
                raw = value[:target_limit]

                for pem in _PEM_RE.finditer(raw):
                    key = (target, pem.start(), pem.end())
                    matches[key] = Match(
                        "private_key_pem", target, pem.start(), pem.end(), pem.group(0)
                    )

                for segment_start, segment_bytes in _segments(raw):
                    if clock() >= deadline or len(matches) >= MAX_MATCHES:
                        partial = True
                        break
                    segment = segment_bytes.decode("utf-8", errors="surrogateescape")
                    _scan_segment(
                        matches,
                        target=target,
                        segment=segment,
                        segment_byte_start=segment_start,
                    )
                    if clock() >= deadline:
                        partial = True
                        break
                if partial and (clock() >= deadline or len(matches) >= MAX_MATCHES):
                    break

        ordered = tuple(
            sorted(
                matches.values(),
                key=lambda item: (item.target, item.byte_start, item.type),
            )
        )
        return ScanResult("partial" if partial else "complete", ordered[:MAX_MATCHES])
    except Exception as exc:  # noqa: BLE001 - capture must fail open with a class name
        return ScanResult("failed", (), type(exc).__name__)


__all__ = ["DETECTOR_NAME", "Match", "ScanResult", "scan"]
