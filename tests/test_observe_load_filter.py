"""Focused tests for observe.load filters and observe.parse_since.

Self-contained: does not import from any other test module. Mirrors the
fixture style of tests/test_observe.py (a fixed epoch, a new_outbox(tmp_path)
helper, and an add(...) helper that builds a producer record via
hermes_dbass.collector._common.build_record and appends it through a real
Outbox so producer_sequence/occurred_at are genuine).

Covers:
  - --since boundary: an event exactly at `since` is kept, strictly-earlier
    events are dropped (with real epoch values from a real outbox).
  - the occurred_at == 0 / missing edge case that observe._as_float folds
    to 0.0 (occurred_at is a *required* envelope field, so a record with it
    truly absent can never pass Outbox.append()'s validate() call; that
    edge is exercised against observe.load() with a minimal stand-in object
    exposing iter_events(), which is all load() actually depends on).
  - --session matches on correlation_id OR session_id OR parent_session_id,
    and excludes records that share none of those ids.
  - session + since combined.
  - an empty outbox yields [].
  - parse_since on an integer string, a float string, an ISO-8601 string,
    and that it raises ValueError on garbage.
"""

from __future__ import annotations

import pytest

from hermes_dbass import observe
from hermes_dbass.collector._common import build_record
from hermes_dbass.collector.outbox import Outbox

B = 1784415000.0


def new_outbox(tmp_path) -> Outbox:
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


def add(ob, event_type, *, occurred_at=B, session_id=None, parent_session_id=None,
        correlation_id="corr", invocation_id=None, payload=None, partial=False,
        content=None):
    rec = build_record(
        event_type=event_type,
        occurred_at=occurred_at,
        source="test",
        capture_method="test",
        runtime={"kind": "cli", "engine": "standard"},
        correlation_id=correlation_id,
        session_id=session_id,
        parent_session_id=parent_session_id,
        invocation_id=invocation_id,
        payload=payload or {},
        partial=partial,
    )
    return ob.append(rec, content=content)


class _FakeOutbox:
    """A stand-in exposing only what observe.load() actually calls.

    Used solely for the occurred_at-missing edge case: occurred_at is a
    required, numeric envelope field, so a record without it can never be
    durably appended through the real Outbox (validate() would reject it
    before any write). observe.load() only ever calls .iter_events() on
    its argument, so this minimal object is a faithful stand-in for that
    one case.
    """

    def __init__(self, records):
        self._records = records

    def iter_events(self):
        return iter(self._records)


# --- since boundary -------------------------------------------------------
def test_since_boundary_keeps_event_exactly_at_since_epoch(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="earlier", occurred_at=B - 1,
        payload={"kind": "cli"})
    add(ob, "session.created", session_id="at-since", occurred_at=B,
        payload={"kind": "cli"})
    add(ob, "session.created", session_id="later", occurred_at=B + 1,
        payload={"kind": "cli"})

    kept = observe.load(ob, since=B)
    ids = {r.get("session_id") for r in kept}

    assert ids == {"at-since", "later"}
    assert "earlier" not in ids


def test_since_boundary_with_occurred_at_zero(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="zeroed", occurred_at=0,
        payload={"kind": "cli"})
    add(ob, "session.created", session_id="positive", occurred_at=B,
        payload={"kind": "cli"})

    # since=0 keeps the zero-occurred_at event too (0.0 >= 0 is True).
    kept_at_zero = observe.load(ob, since=0)
    assert {r.get("session_id") for r in kept_at_zero} == {"zeroed", "positive"}

    # any positive since drops the zero-occurred_at event as strictly earlier.
    kept_positive = observe.load(ob, since=1.0)
    assert {r.get("session_id") for r in kept_positive} == {"positive"}


def test_since_with_missing_occurred_at_folds_to_zero():
    # occurred_at is a required envelope field, so this shape can never come
    # from a real, validated Outbox row -- it exercises observe._as_float's
    # fallback (missing -> 0.0) directly against observe.load's contract.
    missing = {"session_id": "no-timestamp", "payload": {"event_type": "session.created"}}
    present = {"session_id": "has-timestamp", "occurred_at": B,
               "payload": {"event_type": "session.created"}}
    fake = _FakeOutbox([missing, present])

    kept_at_zero = observe.load(fake, since=0)
    assert {r.get("session_id") for r in kept_at_zero} == {"no-timestamp", "has-timestamp"}

    kept_positive = observe.load(fake, since=1.0)
    assert {r.get("session_id") for r in kept_positive} == {"has-timestamp"}


# --- session filter ---------------------------------------------------------
def test_session_filter_matches_correlation_id(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="s1", correlation_id="X",
        payload={"kind": "cli"})
    add(ob, "session.created", session_id="s2", correlation_id="other",
        payload={"kind": "cli"})

    kept = observe.load(ob, session="X")
    assert [r.get("session_id") for r in kept] == ["s1"]


def test_session_filter_matches_session_id(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="Y", correlation_id="unrelated-corr",
        payload={"kind": "cli"})
    add(ob, "session.created", session_id="other", correlation_id="unrelated-corr2",
        payload={"kind": "cli"})

    kept = observe.load(ob, session="Y")
    assert [r.get("session_id") for r in kept] == ["Y"]


def test_session_filter_matches_parent_session_id(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "subagent.child_spawned", session_id="child", parent_session_id="Z",
        correlation_id="unrelated-corr", payload={"kind": "subagent", "model": "m"})
    add(ob, "session.created", session_id="other-root", correlation_id="other-corr",
        payload={"kind": "cli"})

    kept = observe.load(ob, session="Z")
    assert [r.get("session_id") for r in kept] == ["child"]


def test_session_filter_excludes_unrelated_ids(tmp_path):
    ob = new_outbox(tmp_path)
    add(ob, "session.created", session_id="unrelated-session",
        correlation_id="unrelated-corr", parent_session_id=None,
        payload={"kind": "cli"})

    kept = observe.load(ob, session="not-present-anywhere")
    assert kept == []


# --- combined session + since ------------------------------------------------
def test_session_and_since_combine(tmp_path):
    ob = new_outbox(tmp_path)
    # matches session but too early
    add(ob, "session.created", session_id="P", correlation_id="P",
        occurred_at=B - 10, payload={"kind": "cli"})
    # matches session and at/after since -> kept
    add(ob, "tool.call_completed", session_id="P", correlation_id="P",
        occurred_at=B, payload={"tool_name": "read_file", "status": "ok"})
    # at/after since but different session -> dropped
    add(ob, "session.created", session_id="Q", correlation_id="Q",
        occurred_at=B + 10, payload={"kind": "cli"})

    kept = observe.load(ob, session="P", since=B)
    assert len(kept) == 1
    assert kept[0].get("payload", {}).get("event_type") == "tool.call_completed"


# --- empty outbox -------------------------------------------------------------
def test_load_empty_outbox_returns_empty_list(tmp_path):
    ob = new_outbox(tmp_path)
    assert observe.load(ob) == []
    assert observe.load(ob, session="anything") == []
    assert observe.load(ob, since=0) == []


# --- parse_since --------------------------------------------------------------
def test_parse_since_accepts_integer_string():
    assert observe.parse_since("1784415000") == 1784415000.0


def test_parse_since_accepts_float_string():
    assert observe.parse_since("1784415000.5") == 1784415000.5


def test_parse_since_accepts_iso8601_string():
    iso = observe.parse_since("2026-07-18T20:48:39-05:00")
    assert isinstance(iso, float)
    assert iso > 1_700_000_000


def test_parse_since_raises_valueerror_on_garbage():
    with pytest.raises(ValueError):
        observe.parse_since("not-a-timestamp-at-all")
