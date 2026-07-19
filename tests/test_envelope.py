"""Tests for the canonical event envelope v1 (issue #2)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from hermes_dbass.envelope import (
    ALL_EVENT_TYPES,
    P0_POC_EVENT_TYPES,
    RESERVED_EVENT_TYPES,
    SCHEMA_VERSION,
    EnvelopeValidationError,
    parse,
    serialize,
    validate,
)

_FIXTURE = Path(__file__).parent / "fixtures" / "golden_event.json"


def golden() -> dict:
    """A fresh, mutable copy of the golden example event."""
    return json.loads(_FIXTURE.read_text())


# --- happy path ---------------------------------------------------------
def test_golden_is_valid():
    assert validate(golden()) is not None


def test_roundtrip_unchanged():
    g = golden()
    assert parse(serialize(g)) == g
    validate(parse(serialize(g)))


def test_event_type_sets_are_disjoint_and_cover():
    assert P0_POC_EVENT_TYPES.isdisjoint(RESERVED_EVENT_TYPES)
    assert ALL_EVENT_TYPES == P0_POC_EVENT_TYPES | RESERVED_EVENT_TYPES


# --- required fields ----------------------------------------------------
@pytest.mark.parametrize(
    "field",
    [
        "schema_version",
        "event_id",
        "producer_sequence",
        "occurred_at",
        "recorded_at",
        "installation_id",
        "tenant_id",
        "profile",
        "runtime",
        "correlation_id",
        "source",
        "capture_method",
        "payload",
        "partial",
    ],
)
def test_missing_required_field_rejected(field):
    r = golden()
    del r[field]
    with pytest.raises(EnvelopeValidationError):
        validate(r)


# --- wrong types --------------------------------------------------------
def test_wrong_type_producer_sequence_rejected():
    r = golden()
    r["producer_sequence"] = "148372"
    with pytest.raises(EnvelopeValidationError):
        validate(r)


def test_bool_is_not_an_integer_sequence():
    r = golden()
    r["producer_sequence"] = True
    with pytest.raises(EnvelopeValidationError):
        validate(r)


def test_partial_must_be_bool_not_int():
    r = golden()
    r["partial"] = 0
    with pytest.raises(EnvelopeValidationError):
        validate(r)


def test_runtime_must_be_object():
    r = golden()
    r["runtime"] = "desktop"
    with pytest.raises(EnvelopeValidationError):
        validate(r)


# --- content-field invariant -------------------------------------------
@pytest.mark.parametrize("companion", ["content_nonce", "content_hash", "key_version"])
def test_ciphertext_without_companion_rejected(companion):
    r = golden()
    del r[companion]
    with pytest.raises(EnvelopeValidationError):
        validate(r)


def test_companion_without_ciphertext_rejected():
    r = golden()
    del r["content_ciphertext"]  # leaves nonce/hash/key_version dangling
    with pytest.raises(EnvelopeValidationError):
        validate(r)


def test_no_content_at_all_is_valid():
    r = golden()
    for k in ("content_ciphertext", "content_nonce", "content_hash", "key_version"):
        r.pop(k, None)
    assert validate(r) is not None


# --- optional fields, schema version, event types -----------------------
def test_optional_fields_may_be_null():
    r = golden()
    r["session_key"] = None
    r["parent_session_id"] = None
    r["causation_id"] = None
    assert validate(r) is not None


def test_schema_version_enforced():
    r = golden()
    r["schema_version"] = "2"
    with pytest.raises(EnvelopeValidationError):
        validate(r)


def test_unknown_event_type_rejected():
    r = golden()
    r["payload"]["event_type"] = "bogus.type"
    with pytest.raises(EnvelopeValidationError):
        validate(r)


def test_missing_event_type_rejected():
    r = golden()
    del r["payload"]["event_type"]
    with pytest.raises(EnvelopeValidationError):
        validate(r)


def test_reserved_event_type_is_accepted():
    r = golden()
    r["payload"]["event_type"] = "task.created"  # reserved, still a valid type
    assert validate(r) is not None


def test_unknown_top_level_field_rejected():
    r = golden()
    r["surprise"] = 1
    with pytest.raises(EnvelopeValidationError):
        validate(r)


# --- module isolation (acceptance criterion) ----------------------------
def test_envelope_imports_no_collector_or_state():
    """Importing the envelope must not drag in collector/outbox/state."""
    code = (
        "import sys, hermes_dbass.envelope; "
        "bad = [m for m in sys.modules "
        "if m.startswith('hermes_dbass.') and m != 'hermes_dbass.envelope']; "
        "assert not bad, bad"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
