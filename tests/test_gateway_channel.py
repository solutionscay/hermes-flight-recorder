"""Tests for gateway channel/gateway_id on the runtime stamp (issue #15).

runtime.gateway_started is enriched with the connected channels (plaintext
Hermes platform names, never a bot token) and a stable per-boot gateway_id.
Additive against envelope v1: runtime is a free-form dict.
"""

from __future__ import annotations

import re
from pathlib import Path

from hermes_flight_recorder.collector._common import gateway_runtime_stamp
from hermes_flight_recorder.envelope import validate

from test_hook_drain import by_type, drain_to_records, write_spool


_GW_ID = re.compile(r"^gw-[0-9a-f]{16}$")


# --- gateway_runtime_stamp helper --------------------------------------
def test_gateway_stamp_shape():
    stamp = gateway_runtime_stamp(channels=["discord"], gateway_id="gw-abc")
    assert stamp == {
        "kind": "gateway",
        "engine": "standard",
        "channels": ["discord"],
        "gateway_id": "gw-abc",
    }


def test_gateway_stamp_no_channels_is_empty_list():
    assert gateway_runtime_stamp(channels=None)["channels"] == []
    assert gateway_runtime_stamp(channels=[])["channels"] == []


# --- drain: gateway:startup enrichment ---------------------------------
def test_gateway_startup_carries_channels_and_id(tmp_path: Path):
    write_spool(tmp_path, [("gateway:startup", {"platforms": ["discord"]}, 100.0)])
    rec = by_type(drain_to_records(tmp_path))["runtime.gateway_started"]
    assert rec["runtime"]["channels"] == ["discord"]
    assert _GW_ID.match(rec["runtime"]["gateway_id"])
    assert rec["payload"]["platforms"] == ["discord"]  # backward compat kept
    validate(rec)


def test_gateway_startup_empty_platforms(tmp_path: Path):
    write_spool(tmp_path, [("gateway:startup", {"platforms": []}, 100.0)])
    rec = by_type(drain_to_records(tmp_path))["runtime.gateway_started"]
    assert rec["runtime"]["channels"] == []
    assert _GW_ID.match(rec["runtime"]["gateway_id"])


def test_gateway_id_stable_across_redrain(tmp_path: Path):
    # Same spool line (same offset + occurred_at) -> same gateway_id.
    write_spool(tmp_path, [("gateway:startup", {"platforms": ["discord"]}, 100.0)])
    first = by_type(drain_to_records(tmp_path))["runtime.gateway_started"]["runtime"]["gateway_id"]
    second = by_type(drain_to_records(tmp_path))["runtime.gateway_started"]["runtime"]["gateway_id"]
    assert first == second


def test_gateway_id_differs_per_boot(tmp_path: Path):
    # Two distinct startup lines (different occurred_at) get different ids.
    write_spool(
        tmp_path,
        [
            ("gateway:startup", {"platforms": ["discord"]}, 100.0),
            ("gateway:startup", {"platforms": ["discord"]}, 200.0),
        ],
    )
    ids = [
        r["runtime"]["gateway_id"]
        for r in drain_to_records(tmp_path)
        if r["payload"]["event_type"] == "runtime.gateway_started"
    ]
    assert len(ids) == 2 and ids[0] != ids[1]


def test_channels_are_token_free(tmp_path: Path):
    # A serialized gateway record must contain only channel names, no secret.
    from hermes_flight_recorder.envelope import serialize

    write_spool(tmp_path, [("gateway:startup", {"platforms": ["telegram", "discord"]}, 100.0)])
    rec = by_type(drain_to_records(tmp_path))["runtime.gateway_started"]
    blob = serialize(rec)
    assert "telegram" in blob and "discord" in blob
    assert "token" not in blob.lower()
