"""Tests for the non-secret unified recorder configuration (issue #68)."""

from __future__ import annotations

import json
import stat

import pytest

from hermes_flight_recorder.collector import recorder_config


def write_config(tmp_path, payload) -> None:
    (tmp_path / recorder_config.CONFIG_FILENAME).write_text(json.dumps(payload))


def test_missing_or_partial_config_uses_current_defaults(tmp_path):
    missing = recorder_config.load(tmp_path)
    assert missing.capture.max_content_bytes == 65_536
    assert missing.capture.message_roles == ("user", "assistant", "tool")
    assert missing.retention.enabled is False
    assert missing.retention.vacuum == "auto"
    assert missing.knowledge.history == "full"
    assert missing.knowledge.max_versions is None
    assert missing.sync.interval_seconds is None
    assert missing.sync.max_records == 500
    assert missing.sync.max_bytes == 1024 * 1024

    write_config(tmp_path, {"sync": {"max_records": 25}})
    partial = recorder_config.load(tmp_path)
    assert partial.sync.max_records == 25
    assert partial.sync.max_bytes == 1024 * 1024
    assert partial.capture == missing.capture


def test_environment_overrides_file_values(tmp_path, monkeypatch):
    write_config(
        tmp_path,
        {
            "capture": {"max_content_bytes": 10, "message_roles": ["user"]},
            "retention": {"enabled": False, "max_age_days": 3},
            "sync": {"interval_seconds": 10, "max_records": 10, "max_bytes": 100},
        },
    )
    monkeypatch.setenv("HFR_CAPTURE_MAX_CONTENT_BYTES", "20")
    monkeypatch.setenv("HFR_RETENTION_ENABLED", "true")
    monkeypatch.setenv("HFR_RETENTION_VACUUM", "auto")
    monkeypatch.setenv("HFR_SYNC_INTERVAL_SECONDS", "2.5")
    monkeypatch.setenv("HFR_SYNC_MAX_RECORDS", "50")
    monkeypatch.setenv("HFR_CAPTURE_MESSAGE_ROLES", '["assistant", "tool"]')
    monkeypatch.setenv("HFR_KNOWLEDGE_HISTORY", "latest_only")
    monkeypatch.setenv("HFR_KNOWLEDGE_MAX_VERSIONS", "5")

    config = recorder_config.load(tmp_path)

    assert config.capture.max_content_bytes == 20
    assert config.capture.message_roles == ("assistant", "tool")
    assert config.retention.enabled is True
    assert config.knowledge.history == "latest_only"
    assert config.knowledge.max_versions == 5
    assert config.sync.interval_seconds == 2.5
    assert config.sync.max_records == 50
    assert config.sync.max_bytes == 100


@pytest.mark.parametrize(
    "payload, match",
    [
        ({"capture": {"max_content_bytes": 0}}, "capture.max_content_bytes"),
        ({"retention": {"enabled": "yes"}}, "retention.enabled"),
        ({"retention": {"vacuum": "never"}}, "retention.vacuum"),
        ({"sync": {"max_records": 1.5}}, "sync.max_records"),
        ({"capture": {"message_roles": "user"}}, "message_roles"),
        ({"capture": {"sources": {"hook": "yes"}}}, "capture.sources"),
        ({"knowledge": {"history": "some"}}, "knowledge.history"),
        ({"knowledge": {"max_versions": 0}}, "knowledge.max_versions"),
    ],
)
def test_invalid_values_are_rejected(tmp_path, payload, match):
    write_config(tmp_path, payload)
    with pytest.raises(recorder_config.RecorderConfigError, match=match):
        recorder_config.load(tmp_path)


def test_save_writes_private_file_and_round_trips(tmp_path):
    config = recorder_config.RecorderConfig(
        capture=recorder_config.CaptureConfig(sources={"hook": False}),
        knowledge=recorder_config.KnowledgeConfig(history="latest_only", max_versions=3),
        sync=recorder_config.SyncRuntimeConfig(max_records=25),
    )

    path = recorder_config.save(config, tmp_path)

    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert recorder_config.load(tmp_path) == config


# --- serve intervals (issue #101) ---------------------------------------
def test_interval_defaults(tmp_path):
    config = recorder_config.load(tmp_path)
    assert config.capture.interval_seconds == 15.0
    assert config.reconcile.interval_seconds == 60.0


def test_interval_file_and_env_overrides(tmp_path, monkeypatch):
    write_config(
        tmp_path,
        {"capture": {"interval_seconds": 5}, "reconcile": {"interval_seconds": 30}},
    )
    filed = recorder_config.load(tmp_path)
    assert filed.capture.interval_seconds == 5.0
    assert filed.reconcile.interval_seconds == 30.0

    monkeypatch.setenv("HFR_CAPTURE_INTERVAL_SECONDS", "2.5")
    monkeypatch.setenv("HFR_RECONCILE_INTERVAL_SECONDS", "45")
    env = recorder_config.load(tmp_path)
    assert env.capture.interval_seconds == 2.5
    assert env.reconcile.interval_seconds == 45.0


@pytest.mark.parametrize(
    "payload, match",
    [
        ({"capture": {"interval_seconds": 0}}, "capture.interval_seconds"),
        ({"capture": {"interval_seconds": -1}}, "capture.interval_seconds"),
        ({"reconcile": {"interval_seconds": 0}}, "reconcile.interval_seconds"),
    ],
)
def test_non_positive_intervals_rejected(tmp_path, payload, match):
    write_config(tmp_path, payload)
    with pytest.raises(recorder_config.RecorderConfigError, match=match):
        recorder_config.load(tmp_path)


def test_intervals_survive_save_round_trip(tmp_path):
    config = recorder_config.RecorderConfig(
        capture=recorder_config.CaptureConfig(interval_seconds=7.0),
        reconcile=recorder_config.ReconcileRuntimeConfig(interval_seconds=90.0),
    )
    recorder_config.save(config, tmp_path)
    assert recorder_config.load(tmp_path) == config
