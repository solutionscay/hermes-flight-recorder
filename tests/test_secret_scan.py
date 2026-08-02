from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from helpers import MESSAGES_FULL, SESSIONS_FULL, make_state_db, new_outbox
from hermes_flight_recorder import observe
from hermes_flight_recorder.cli import main
from hermes_flight_recorder.collector import (
    content_crypto,
    keystore,
    knowledge_store,
    recorder_config,
    secret_detector,
    security_scan,
    state_db,
)
from hermes_flight_recorder.collector._common import build_record
from hermes_flight_recorder.collector.outbox import Outbox


OPENAI_KEY = "sk-abcdefghijklmnopqrstuvwxyz1234567890"


def _record(event_type: str = "invocation.started") -> dict:
    return build_record(
        event_type=event_type,
        occurred_at=100.0,
        source="test",
        capture_method="test",
        runtime={"kind": "test"},
        correlation_id="corr",
    )


def _events(outbox, event_type: str):
    return [
        event
        for event in outbox.iter_events()
        if event["payload"]["event_type"] == event_type
    ]


def _finding_matches(outbox, finding):
    return json.loads(outbox.decrypt_content(finding))["matches"]


def test_message_secret_creates_one_atomic_encrypted_finding(tmp_path):
    outbox = new_outbox(tmp_path)
    source = outbox.append(
        _record(), content=f'token = "{OPENAI_KEY}"', dedup_key="message:1"
    )

    findings = _events(outbox, "security.secret_detected")
    assert len(findings) == 1
    finding = findings[0]
    assert finding["causation_id"] == source["event_id"]
    assert finding["payload"] == {
        "event_type": "security.secret_detected",
        "detector": "hfr-secret-scan-v1",
        "match_count": 1,
        "artifact_ref": source["event_id"],
        "scan_status": "complete",
    }
    assert OPENAI_KEY not in json.dumps(finding)
    assert _finding_matches(outbox, finding)[0]["type"] == "openai_api_key"

    outbox.append(_record(), content=OPENAI_KEY, dedup_key="message:1")
    assert len(_events(outbox, "security.secret_detected")) == 1


def test_exact_secret_has_one_installation_local_fingerprint(tmp_path):
    outbox = new_outbox(tmp_path)
    for index in range(2):
        outbox.append(_record(), content=OPENAI_KEY, dedup_key=f"message:{index}")
    findings = _events(outbox, "security.secret_detected")
    fingerprints = [
        _finding_matches(outbox, finding)[0]["fingerprint"] for finding in findings
    ]
    assert len(set(fingerprints)) == 1

    other = new_outbox(tmp_path, "other")
    other.append(_record(), content=OPENAI_KEY)
    other_fingerprint = _finding_matches(
        other, _events(other, "security.secret_detected")[0]
    )[0]["fingerprint"]
    assert other_fingerprint != fingerprints[0]


def test_unicode_offsets_are_byte_offsets():
    raw = f"snowman=☃ {OPENAI_KEY}".encode()
    result = secret_detector.scan(
        {"content": raw}, max_bytes=len(raw), deadline_ms=1000
    )
    match = next(item for item in result.matches if item.type == "openai_api_key")
    assert raw[match.byte_start : match.byte_end] == OPENAI_KEY.encode()
    assert match.byte_start == len("snowman=☃ ".encode())


@pytest.mark.parametrize(
    ("detector_type", "text"),
    [
        ("private_key_pem", "-----BEGIN PRIVATE KEY-----\nQUJDREVGR0g=\n-----END PRIVATE KEY-----"),
        ("aws_access_key_id", "AKIA" + "ABCDEFGHIJKLMNOP"),
        (
            "aws_secret_access_key",
            "AWS_SECRET_ACCESS_KEY='" + "abcdEFGHijklMNOPqrstUVWXyz0123456789ABCD" + "'",
        ),
        ("github_token", "ghp_abcdefghijklmnopqrstuvwxyz1234567890"),
        ("bearer_token", "Bearer abcdefghijklmnopqrstuvwxyz.123456"),
        ("authorization_token", "Authorization: Token abcdefghijklmnopqrstuvwxyz123456"),
        ("secret_assignment", "password = 'correct-horse-battery-staple'"),
        ("high_entropy_quoted_value", '"aB3dE5fG7hJ9kL2mN4pQ6rS8tU0vW1xY"'),
    ],
)
def test_first_release_rule_families(detector_type, text):
    result = secret_detector.scan(
        {"content": text.encode()}, max_bytes=100_000, deadline_ms=1000
    )
    assert detector_type in {match.type for match in result.matches}


def test_limit_and_deadline_return_partial_results():
    raw = (OPENAI_KEY + " z " + OPENAI_KEY).encode()
    limited = secret_detector.scan(
        {"content": raw}, max_bytes=len(OPENAI_KEY), deadline_ms=1000
    )
    assert limited.status == "partial"
    assert len(limited.matches) == 1

    ticks = iter([0.0, 0.0, 1.0])
    timed = secret_detector.scan(
        {"content": OPENAI_KEY.encode()},
        max_bytes=1000,
        deadline_ms=1,
        clock=lambda: next(ticks, 1.0),
    )
    assert timed.status == "partial"


def test_partial_scan_status_does_not_drop_the_source(tmp_path):
    home = tmp_path / "flight-recorder"
    recorder_config.save(
        recorder_config.RecorderConfig(
            security=recorder_config.SecurityConfig(
                secret_scan_max_bytes=len(OPENAI_KEY)
            )
        ),
        home,
    )
    outbox = Outbox.open(home)
    outbox.initialize()
    source = outbox.append(_record(), content=OPENAI_KEY + " trailing bytes")
    finding = _events(outbox, "security.secret_detected")[0]
    assert source["event_id"] == finding["causation_id"]
    assert finding["payload"]["scan_status"] == "partial"


def test_disabled_scanner_appends_only_the_source(tmp_path):
    home = tmp_path / "flight-recorder"
    recorder_config.save(
        recorder_config.RecorderConfig(
            security=recorder_config.SecurityConfig(secret_scan_enabled=False)
        ),
        home,
    )
    outbox = Outbox.open(home)
    outbox.initialize()
    outbox.append(_record(), content=OPENAI_KEY)
    assert len(list(outbox.iter_events())) == 1
    assert not _events(outbox, "security.secret_detected")


def test_scanner_fault_keeps_source_and_logs_only_exception_class(
    tmp_path, monkeypatch, caplog
):
    class DetectorFault(RuntimeError):
        pass

    def fail(*_args, **_kwargs):
        raise DetectorFault("raw-secret-must-not-be-logged")

    monkeypatch.setattr(secret_detector, "scan", fail)
    outbox = new_outbox(tmp_path)
    source = outbox.append(_record(), content=OPENAI_KEY)

    assert source in list(outbox.iter_events())
    assert not _events(outbox, "security.secret_detected")
    assert "DetectorFault" in caplog.text
    assert "raw-secret-must-not-be-logged" not in caplog.text
    assert OPENAI_KEY not in caplog.text


def test_source_and_finding_roll_back_together(tmp_path, monkeypatch):
    outbox = new_outbox(tmp_path)
    real_insert = outbox._insert_event

    def fail_finding(record, *, dedup_key):
        if record["payload"]["event_type"] == "security.secret_detected":
            raise RuntimeError("finding write fault")
        return real_insert(record, dedup_key=dedup_key)

    monkeypatch.setattr(outbox, "_insert_event", fail_finding)
    with pytest.raises(RuntimeError, match="finding write fault"):
        outbox.append(_record(), content=OPENAI_KEY, dedup_key="message:1")
    assert list(outbox.iter_events()) == []
    assert outbox.high_water() == 0


def test_suppression_prevents_a_finding(tmp_path):
    outbox = new_outbox(tmp_path)
    outbox.append(_record(), content=OPENAI_KEY, dedup_key="message:1")
    match = _finding_matches(
        outbox, _events(outbox, "security.secret_detected")[0]
    )[0]
    config = recorder_config.SecurityConfig()
    assert security_scan.update_suppression(
        outbox.flight_recorder_home,
        config,
        match["type"],
        match["fingerprint"],
        add=True,
    )

    reopened = Outbox.open(outbox.flight_recorder_home)
    reopened.append(_record(), content=OPENAI_KEY, dedup_key="message:2")
    assert len(_events(reopened, "security.secret_detected")) == 1
    baseline = json.loads(
        (reopened.flight_recorder_home / config.secret_scan_baseline).read_text()
    )
    assert OPENAI_KEY not in json.dumps(baseline)


def test_knowledge_file_is_scanned_before_bundle_encoding(tmp_path):
    hermes = tmp_path / "hermes"
    skill = hermes / "skills" / "example"
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text(f"credential: {OPENAI_KEY}\n")
    outbox = new_outbox(tmp_path)

    knowledge_store.poll(outbox, hermes)

    finding = _events(outbox, "security.secret_detected")[0]
    match = _finding_matches(outbox, finding)[0]
    assert match["target"] == "SKILL.md"
    knowledge_event = _events(outbox, "knowledge.record_written")[0]
    bundle = json.loads(outbox.decrypt_content(knowledge_event))
    assert bundle["files"][0]["content_b64"]


def test_state_message_scans_before_batch_transaction(tmp_path, monkeypatch):
    hermes = tmp_path / "hermes"
    hermes.mkdir()
    make_state_db(
        hermes,
        sessions=[
            ("s1", "cli", None, None, 1, 0, 0, 0, 0, 1.0, None, None, "default", 0)
        ],
        messages=[(1, "s1", "user", None, None, None, OPENAI_KEY, 2.0, None)],
        sessions_columns=SESSIONS_FULL,
        messages_columns=MESSAGES_FULL,
    )
    outbox = new_outbox(tmp_path)
    real_prepare = outbox.prepare_secret_scan

    def checked_prepare(content=None, **kwargs):
        if content is not None:
            assert outbox._batch_depth == 0
        return real_prepare(content, **kwargs)

    monkeypatch.setattr(outbox, "prepare_secret_scan", checked_prepare)
    state_db.poll(outbox, hermes)
    assert len(_events(outbox, "security.secret_detected")) == 1


def test_security_report_groups_fingerprints_and_fleet_needs_private_key(tmp_path):
    outbox = new_outbox(tmp_path)
    for index in range(2):
        outbox.append(_record(), content=OPENAI_KEY, dedup_key=f"message:{index}")
    lines, code = observe.render_security(outbox, list(outbox.iter_events()))
    text = "\n".join(lines)
    assert code == 1
    assert "occurrences=2" in text
    assert OPENAI_KEY not in text

    fleet_home = tmp_path / "fleet"
    fleet_home.mkdir()
    operator = content_crypto.generate_operator_keypair()
    keystore.write_public_key(fleet_home, operator.public)
    fleet = Outbox.open(fleet_home)
    fleet.initialize()
    fleet.append(_record(), content=OPENAI_KEY)
    with pytest.raises(observe.SecurityReportError, match="private key"):
        observe.render_security(fleet, list(fleet.iter_events()))


def test_fingerprint_key_is_private(tmp_path):
    key = security_scan.ensure_fingerprint_key(tmp_path)
    path = security_scan.fingerprint_key_path(tmp_path)
    assert len(key) == 32
    if os.name == "posix":
        assert path.stat().st_mode & 0o077 == 0


def test_security_cli_suppresses_and_observe_report_has_separate_exit_lane(
    tmp_path, capsys
):
    home = tmp_path / "flight-recorder"
    outbox = Outbox.open(home)
    outbox.initialize()
    outbox.append(_record(), content=OPENAI_KEY, dedup_key="message:1")
    finding = _events(outbox, "security.secret_detected")[0]
    match = _finding_matches(outbox, finding)[0]
    outbox.close()

    assert main(["observe", "--report", "--flight-recorder-home", str(home)]) == 0
    assert main(["observe", "--security", "--flight-recorder-home", str(home)]) == 1
    output = capsys.readouterr()
    assert OPENAI_KEY not in output.out
    assert "occurrences=1" in output.out

    assert main(
        [
            "security",
            "--flight-recorder-home",
            str(home),
            "suppress",
            match["type"],
            match["fingerprint"],
        ]
    ) == 0

    reopened = Outbox.open(home)
    reopened.append(_record(), content=OPENAI_KEY, dedup_key="message:2")
    assert len(_events(reopened, "security.secret_detected")) == 1
