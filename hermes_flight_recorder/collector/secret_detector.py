"""Bounded standard-library secret detector.

The detector works on bytes so every reported span is a zero-based, half-open
byte range in the exact plaintext target. It has no network code and no
runtime dependency.
"""

from __future__ import annotations

import math
import re
import time
from collections import Counter
from dataclasses import dataclass
from typing import Callable, Iterable, Mapping

DETECTOR_NAME = "hfr-secret-scan-v1"
CHUNK_BYTES = 64 * 1024
CHUNK_OVERLAP = 16 * 1024
MAX_CANDIDATE_BYTES = 16 * 1024
MAX_MATCHES = 512


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


@dataclass(frozen=True)
class _Rule:
    name: str
    pattern: re.Pattern[bytes]
    value_group: int = 0
    predicate: Callable[[bytes], bool] | None = None


_PEM_LABEL = rb"(?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY"
_RULES = (
    _Rule(
        "private_key_pem",
        re.compile(
            rb"-----BEGIN " + _PEM_LABEL + rb"-----[\s\S]{1,16000}?"
            rb"-----END " + _PEM_LABEL + rb"-----"
        ),
    ),
    _Rule(
        "aws_access_key_id",
        re.compile(rb"(?<![A-Z0-9])(?:AKIA|ASIA|AIDA|AROA|AIPA|ANPA|ANVA|ASCA)[A-Z0-9]{16}(?![A-Z0-9])"),
    ),
    _Rule(
        "aws_secret_access_key",
        re.compile(
            rb"(?i)\b(?:aws_?secret_?access_?key|secret_?access_?key)\b"
            rb"\s*[:=]\s*['\"]?([A-Za-z0-9/+]{40})['\"]?"
        ),
        value_group=1,
    ),
    _Rule("anthropic_api_key", re.compile(rb"(?<![A-Za-z0-9_-])sk-ant-[A-Za-z0-9_-]{20,200}(?![A-Za-z0-9_-])")),
    _Rule("openai_api_key", re.compile(rb"(?<![A-Za-z0-9_-])sk-(?:proj-|svcacct-)?[A-Za-z0-9_-]{20,200}(?![A-Za-z0-9_-])")),
    _Rule("github_token", re.compile(rb"(?<![A-Za-z0-9_])gh(?:p|o|u|s|r)_[A-Za-z0-9]{30,255}(?![A-Za-z0-9])")),
    _Rule("gitlab_token", re.compile(rb"(?<![A-Za-z0-9-])glpat-[A-Za-z0-9_-]{20,255}(?![A-Za-z0-9_-])")),
    _Rule("npm_token", re.compile(rb"(?<![A-Za-z0-9_])npm_[A-Za-z0-9]{30,255}(?![A-Za-z0-9])")),
    _Rule("huggingface_token", re.compile(rb"(?<![A-Za-z0-9_])hf_[A-Za-z0-9]{30,255}(?![A-Za-z0-9])")),
    _Rule("slack_token", re.compile(rb"(?<![A-Za-z0-9-])xox(?:b|p|a|r|s)-[A-Za-z0-9-]{20,255}(?![A-Za-z0-9-])")),
    _Rule("stripe_secret_key", re.compile(rb"(?<![A-Za-z0-9_])sk_(?:live|test)_[A-Za-z0-9]{20,200}(?![A-Za-z0-9])")),
    _Rule("google_api_key", re.compile(rb"(?<![A-Za-z0-9_-])AIza[A-Za-z0-9_-]{35}(?![A-Za-z0-9_-])")),
    _Rule(
        "bearer_token",
        re.compile(rb"(?i)\b(?:authorization\s*[:=]\s*)?bearer\s+([A-Za-z0-9._~+/=-]{16,1024})"),
        value_group=1,
    ),
    _Rule(
        "authorization_token",
        re.compile(
            rb"(?i)\bauthorization\s*[:=]\s*(?:(?:token|basic|api[_-]?key)\s+)?"
            rb"([A-Za-z0-9._~+/=-]{16,1024})"
        ),
        value_group=1,
    ),
    _Rule(
        "secret_assignment",
        re.compile(
            rb"(?i)\b(?:password|passwd|pwd|token|secret|api[_-]?key|access[_-]?key)\b"
            rb"\s*[:=]\s*(['\"])([^'\"\r\n]{8,1024})\1"
        ),
        value_group=2,
    ),
    _Rule(
        "high_entropy_quoted_value",
        re.compile(rb"(['\"])([A-Za-z0-9_~+/.=-]{32,256})\1"),
        value_group=2,
        predicate=lambda value: _shannon_entropy(value) >= 4.5,
    ),
)


def _shannon_entropy(value: bytes) -> float:
    if not value:
        return 0.0
    size = len(value)
    return -sum((count / size) * math.log2(count / size) for count in Counter(value).values())


def scan(
    targets: Mapping[str, bytes] | Iterable[tuple[str, bytes]],
    *,
    max_bytes: int,
    deadline_ms: int,
    clock: Callable[[], float] = time.monotonic,
) -> ScanResult:
    """Scan targets with fixed chunks, fixed overlap, and a monotonic deadline."""
    started = clock()
    deadline = started + (deadline_ms / 1000.0)
    remaining = max_bytes
    partial = False
    matches: dict[tuple[str, int, int], Match] = {}
    items = targets.items() if isinstance(targets, Mapping) else targets

    try:
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
            start = 0
            while start < target_limit:
                if clock() >= deadline:
                    return ScanResult("partial", tuple(matches.values()))
                end = min(start + CHUNK_BYTES, target_limit)
                chunk = value[start:end]
                for rule in _RULES:
                    if clock() >= deadline:
                        return ScanResult("partial", tuple(matches.values()))
                    for candidate in rule.pattern.finditer(chunk):
                        group = candidate.group(rule.value_group)
                        if len(group) > MAX_CANDIDATE_BYTES:
                            continue
                        if rule.predicate is not None and not rule.predicate(group):
                            continue
                        local_start, local_end = candidate.span(rule.value_group)
                        absolute_start = start + local_start
                        absolute_end = start + local_end
                        key = (target, absolute_start, absolute_end)
                        matches.setdefault(key, Match(
                            rule.name,
                            target,
                            absolute_start,
                            absolute_end,
                            group,
                        ))
                        if len(matches) >= MAX_MATCHES:
                            return ScanResult("partial", tuple(matches.values()))
                if end >= target_limit:
                    break
                start = end - CHUNK_OVERLAP
        status = "partial" if partial else "complete"
        ordered = tuple(
            sorted(matches.values(), key=lambda item: (item.target, item.byte_start, item.type))
        )
        return ScanResult(status, ordered)
    except Exception as exc:
        return ScanResult("failed", (), type(exc).__name__)


__all__ = ["DETECTOR_NAME", "Match", "ScanResult", "scan"]
