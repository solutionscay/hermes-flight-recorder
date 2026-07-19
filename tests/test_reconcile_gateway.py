"""Tests for gateway start-failure detection (issue #13).

A gateway that fails to start emits no gateway:startup hook, so it is
invisible to live capture. The reconciler reads gateway_state.json and
gateway-starts.log read-only and emits runtime.gateway_start_failed:
Case A (startup_failed), Case B (token_conflict), Case C (absent). Raw
error text stays in encrypted content; dedup keys are event-anchored so a
second pass appends nothing.
"""

from __future__ import annotations

import json

from hermes_flight_recorder.collector import reconcile
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.envelope import serialize, validate


def new_outbox(tmp_path):
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


def _findings(ob):
    return [
        e
        for e in ob.iter_events()
        if e["payload"]["event_type"] == "runtime.gateway_start_failed"
    ]


def _state(hh, obj):
    (hh / "gateway_state.json").write_text(json.dumps(obj))


# --- Case A: startup_failed ---------------------------------------------
def test_startup_failed_surfaces_one_finding(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    _state(hh, {
        "gateway_state": "startup_failed",
        "exit_reason": "telegram: dm_policy open is not allowed without allowlist",
        "updated_at": "2026-07-19T21:29:01.661893+00:00",
    })
    ob = new_outbox(tmp_path)
    reconcile.reconcile(ob, hh, now=1_800_000_000.0)

    found = _findings(ob)
    assert len(found) == 1
    rec = found[0]
    assert rec["payload"]["reason_class"] == "policy_open"
    assert rec["partial"] is True
    validate(rec)
    # Raw exit_reason is encrypted, never in plaintext.
    assert "dm_policy" not in serialize(rec)
    assert ob.decrypt_content(rec) == b"telegram: dm_policy open is not allowed without allowlist"


def test_startup_failed_is_idempotent(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    _state(hh, {
        "gateway_state": "startup_failed",
        "exit_reason": "boom",
        "updated_at": "2026-07-19T21:29:01.661893+00:00",
    })
    ob = new_outbox(tmp_path)
    reconcile.reconcile(ob, hh, now=1_800_000_000.0)
    n = ob.count()
    reconcile.reconcile(ob, hh, now=1_800_000_050.0)  # later run, same state
    assert ob.count() == n  # dedup anchored on updated_at, not `when`


# --- Case B: token_conflict ---------------------------------------------
def test_token_conflict_names_platform_and_pid(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    _state(hh, {
        "gateway_state": "degraded",
        "exit_reason": None,
        "platforms": {
            "discord": {
                "state": "fatal",
                "error_code": "discord-bot-token_lock",
                "error_message": "Discord bot token already in use (PID 999). Stop the other gateway first.",
                "updated_at": "2026-07-19T21:29:01.661893+00:00",
            }
        },
    })
    ob = new_outbox(tmp_path)
    reconcile.reconcile(ob, hh, now=1_800_000_000.0)

    found = _findings(ob)
    assert len(found) == 1
    pl = found[0]["payload"]
    assert pl["reason_class"] == "token_conflict"
    assert pl["platform"] == "discord"
    assert pl["conflicting_pid"] == 999
    assert pl["error_code"] == "discord-bot-token_lock"
    # The raw error_message goes to encrypted content, never plaintext.
    blob = serialize(found[0])
    assert "already in use" not in blob
    assert ob.decrypt_content(found[0]).startswith(b"Discord bot token already in use")


def test_token_conflict_idempotent_even_as_updated_at_advances(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    base = {
        "gateway_state": "running",
        "platforms": {
            "discord": {
                "error_code": "discord-bot-token_lock",
                "error_message": "Discord bot token already in use (PID 42).",
                "updated_at": "2026-07-19T21:29:01.661893+00:00",
            }
        },
    }
    _state(hh, base)
    ob = new_outbox(tmp_path)
    reconcile.reconcile(ob, hh, now=1_800_000_000.0)
    n = ob.count()
    # A live gateway advances updated_at; the finding must not duplicate.
    base["updated_at"] = "2026-07-19T22:00:00+00:00"
    base["platforms"]["discord"]["updated_at"] = "2026-07-19T22:00:00+00:00"
    _state(hh, base)
    reconcile.reconcile(ob, hh, now=1_800_000_500.0)
    assert ob.count() == n  # dedup anchored on platform+pid, not timestamp


# --- Case C: absent -----------------------------------------------------
def test_absent_when_started_before_but_status_gone(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    (hh / "gateway-starts.log").write_text("1784496008.1\n1784496538.087068\n")
    # No gateway_state.json.
    ob = new_outbox(tmp_path)
    reconcile.reconcile(ob, hh, now=1_800_000_000.0)

    found = _findings(ob)
    assert len(found) == 1
    assert found[0]["payload"]["reason_class"] == "absent"
    assert found[0]["payload"]["last_start_at"] == 1784496538.087068


def test_absent_is_idempotent(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    (hh / "gateway-starts.log").write_text("1784496538.0\n")
    ob = new_outbox(tmp_path)
    reconcile.reconcile(ob, hh, now=1_800_000_000.0)
    n = ob.count()
    reconcile.reconcile(ob, hh, now=1_800_000_900.0)
    assert ob.count() == n


# --- no false positives -------------------------------------------------
def test_healthy_running_gateway_no_finding(tmp_path):
    hh = tmp_path / "hermes"; hh.mkdir()
    _state(hh, {
        "gateway_state": "running",
        "exit_reason": None,
        "platforms": {"discord": {"state": "connected", "error_code": None, "error_message": None}},
        "updated_at": "2026-07-19T21:29:01.661893+00:00",
    })
    (hh / "gateway-starts.log").write_text("1784496538.0\n")
    ob = new_outbox(tmp_path)
    reconcile.reconcile(ob, hh, now=1_800_000_000.0)
    assert _findings(ob) == []


def test_clean_stop_is_not_absent(tmp_path):
    # gateway_state='stopped' present -> a clean stop, not a vanished gateway.
    hh = tmp_path / "hermes"; hh.mkdir()
    _state(hh, {"gateway_state": "stopped", "updated_at": "2026-07-19T21:29:01+00:00"})
    (hh / "gateway-starts.log").write_text("1784496538.0\n")
    ob = new_outbox(tmp_path)
    reconcile.reconcile(ob, hh, now=1_800_000_000.0)
    assert _findings(ob) == []


def test_no_gateway_no_false_positive(tmp_path):
    # No state file, no start history -> nothing.
    hh = tmp_path / "hermes"; hh.mkdir()
    ob = new_outbox(tmp_path)
    reconcile.reconcile(ob, hh, now=1_800_000_000.0)
    assert _findings(ob) == []
