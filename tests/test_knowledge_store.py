"""Tests for the content-addressed knowledge store (Phase 3, issue #78).

Fixtures build a real Hermes-shaped home — ``memories/MEMORY.md`` + ``USER.md``
and skills under ``skills/`` — and drive the scanner over it. The scenarios pin
the rules the contract (#76) turns on: only Hermes-created skills are tracked,
versions deduplicate blobs, a re-scan is idempotent, a delete tombstones without
losing history, and content round-trips through encryption.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import time
from pathlib import Path

import pytest

from helpers import new_outbox

from hermes_flight_recorder.collector import keystore, knowledge_store
from hermes_flight_recorder.collector.recorder_config import KnowledgeConfig
from hermes_flight_recorder.collector.sync import (
    MAX_INGEST_BATCH_BYTES,
    singleton_batch_size,
)


def knowledge_events(ob):
    """Every knowledge.record_written envelope stored in the outbox, in order."""
    out = []
    for (envelope_json,) in ob._conn.execute(
        "SELECT envelope_json FROM events ORDER BY producer_sequence"
    ):
        record = json.loads(envelope_json)
        if record.get("payload", {}).get("event_type") == knowledge_store.KNOWLEDGE_EVENT:
            out.append(record)
    return out


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def write_skill(skills, name, body="# skill\n", *, category=None, files=None):
    skill_dir = (skills / category / name) if category else (skills / name)
    write(skill_dir / "SKILL.md", body)
    for rel, content in (files or {}).items():
        write(skill_dir / rel, content)
    return skill_dir


def blob_count(ob):
    return ob._conn.execute("SELECT COUNT(*) FROM knowledge_blob").fetchone()[0]


def test_memory_and_agent_skill_are_versioned(tmp_path):
    home = tmp_path / "hermes"
    write(home / "memories" / "MEMORY.md", "remember this\n")
    write(home / "memories" / "USER.md", "the user is jose\n")
    write_skill(home / "skills", "deploy", "# deploy skill\n")
    ob = new_outbox(tmp_path)

    counts = knowledge_store.poll(ob, home)

    assert counts == {knowledge_store.KNOWLEDGE_EVENT: 3}
    ids = ob.knowledge_artifact_ids()
    assert ids == ["memory:memory", "memory:user", "skill:deploy"]
    latest = ob.latest_knowledge_version("skill:deploy")
    assert latest["seq"] == 1
    assert latest["origin"] == "background"
    assert not latest["is_tombstone"]


def test_bundled_and_hub_skills_are_not_tracked(tmp_path):
    home = tmp_path / "hermes"
    skills = home / "skills"
    write_skill(skills, "bundled-one")
    write_skill(skills, "hub-one")
    write_skill(skills, "agent-one")
    write(skills / ".bundled_manifest", "bundled-one:abc123\n")
    write(skills / ".hub" / "lock.json", json.dumps({"installed": {"hub-one": {}}}))
    ob = new_outbox(tmp_path)

    knowledge_store.poll(ob, home)

    assert ob.knowledge_artifact_ids() == ["skill:agent-one"]


def test_category_nested_skill_is_tracked(tmp_path):
    home = tmp_path / "hermes"
    write_skill(home / "skills", "publish", "# publish\n", category="ops")
    ob = new_outbox(tmp_path)

    knowledge_store.poll(ob, home)

    assert ob.knowledge_artifact_ids() == ["skill:ops/publish"]
    row = ob._conn.execute(
        "SELECT kind, name, category FROM knowledge_artifact WHERE artifact_id=?",
        ("skill:ops/publish",),
    ).fetchone()
    assert tuple(row) == ("skill", "publish", "ops")


def test_edit_adds_a_version_and_dedups_unchanged_blobs(tmp_path):
    home = tmp_path / "hermes"
    skills = home / "skills"
    write_skill(
        skills,
        "multi",
        "# body v1\n",
        files={"references/a.md": "alpha", "references/b.md": "beta"},
    )
    ob = new_outbox(tmp_path)
    knowledge_store.poll(ob, home)
    assert blob_count(ob) == 3  # SKILL.md + a.md + b.md

    # Change only one of the three files.
    write(skills / "multi" / "references" / "a.md", "alpha-2")
    counts = knowledge_store.poll(ob, home)

    assert counts == {knowledge_store.KNOWLEDGE_EVENT: 1}
    assert [v["seq"] for v in ob.knowledge_versions("skill:multi")] == [1, 2]
    assert blob_count(ob) == 4  # one new blob only; b.md and SKILL.md reused


def test_rescan_without_change_is_idempotent(tmp_path):
    home = tmp_path / "hermes"
    write(home / "memories" / "MEMORY.md", "stable\n")
    write_skill(home / "skills", "steady")
    ob = new_outbox(tmp_path)

    first = knowledge_store.poll(ob, home)
    second = knowledge_store.poll(ob, home)

    assert first == {knowledge_store.KNOWLEDGE_EVENT: 2}
    assert second == {}
    assert [v["seq"] for v in ob.knowledge_versions("skill:steady")] == [1]


def test_delete_records_a_tombstone_and_keeps_history(tmp_path):
    home = tmp_path / "hermes"
    skills = home / "skills"
    skill_dir = write_skill(skills, "doomed", "# v1\n")
    ob = new_outbox(tmp_path)
    knowledge_store.poll(ob, home)

    # Remove the skill from disk, then scan again.
    for path in sorted(skill_dir.rglob("*"), reverse=True):
        path.unlink()
    skill_dir.rmdir()
    counts = knowledge_store.poll(ob, home)

    assert counts == {knowledge_store.KNOWLEDGE_EVENT: 1}
    versions = ob.knowledge_versions("skill:doomed")
    assert len(versions) == 2
    assert versions[0]["is_tombstone"] is False
    assert versions[1]["is_tombstone"] is True
    assert versions[1]["manifest"] == []


def test_latest_only_keeps_a_single_version_and_gcs_blobs(tmp_path):
    home = tmp_path / "hermes"
    memory = home / "memories" / "MEMORY.md"
    write(memory, "v1\n")
    ob = new_outbox(tmp_path)
    config = KnowledgeConfig(history="latest_only")

    knowledge_store.poll(ob, home, knowledge_config=config)
    write(memory, "v2\n")
    knowledge_store.poll(ob, home, knowledge_config=config)

    versions = ob.knowledge_versions("memory:memory")
    assert [v["seq"] for v in versions] == [2]
    assert blob_count(ob) == 1  # the v1 blob was garbage-collected
    assert ob.get_blob(versions[0]["manifest"][0]["blob_hash"]) == b"v2\n"


def test_version_restores_byte_for_byte(tmp_path):
    home = tmp_path / "hermes"
    skills = home / "skills"
    body = "# deploy\nsteps: do the thing\n"
    ref = "detailed reference — with unicode\n"
    write_skill(skills, "deploy", body, files={"references/how.md": ref})
    ob = new_outbox(tmp_path)
    knowledge_store.poll(ob, home)

    manifest = ob.latest_knowledge_version("skill:deploy")["manifest"]
    restored = {e["path"]: ob.get_blob(e["blob_hash"]).decode("utf-8") for e in manifest}

    assert restored == {"SKILL.md": body, "references/how.md": ref}


def test_unreadable_artifact_is_isolated_not_fatal(tmp_path):
    home = tmp_path / "hermes"
    skills = home / "skills"
    write_skill(skills, "good", "# good\n")
    bad = write_skill(skills, "bad", "# bad\n")  # sorts before 'good'
    os.chmod(bad / "SKILL.md", 0)  # unreadable -> read_bytes raises PermissionError
    ob = new_outbox(tmp_path)
    try:
        counts = knowledge_store.poll(ob, home)  # must not raise
    finally:
        os.chmod(bad / "SKILL.md", 0o644)

    # The good skill (processed after the bad one) is still captured; the bad one
    # is skipped, not fatal, and not spuriously tombstoned.
    assert "skill:good" in ob.knowledge_artifact_ids()
    assert "skill:bad" not in ob.knowledge_artifact_ids()
    assert ob.latest_knowledge_version("skill:bad") is None
    assert counts == {knowledge_store.KNOWLEDGE_EVENT: 1}


def test_max_versions_caps_the_chain(tmp_path):
    home = tmp_path / "hermes"
    memory = home / "memories" / "MEMORY.md"
    ob = new_outbox(tmp_path)
    config = KnowledgeConfig(history="full", max_versions=2)

    for n in range(1, 5):
        write(memory, f"v{n}\n")
        knowledge_store.poll(ob, home, knowledge_config=config)

    versions = ob.knowledge_versions("memory:memory")
    assert [v["seq"] for v in versions] == [3, 4]  # only the newest two survive


def test_max_file_bytes_skips_large_binary_with_metadata_and_log(tmp_path, caplog):
    home = tmp_path / "hermes"
    skill = write_skill(home / "skills", "binary", "# binary\n")
    asset = skill / "assets" / "large.bin"
    asset.parent.mkdir(parents=True)
    asset.write_bytes(bytes(range(256)) * 8)
    ob = new_outbox(tmp_path)
    config = KnowledgeConfig(max_file_bytes=1024)
    caplog.set_level(logging.WARNING, logger="hermes_flight_recorder.serve.knowledge")

    knowledge_store.poll(ob, home, knowledge_config=config)

    event = knowledge_events(ob)[0]
    skipped = event["payload"]["skipped_files"]
    assert skipped == [
        {
            "path": "assets/large.bin",
            "reason": "max_file_bytes",
            "byte_count": 2048,
            "limit": 1024,
        }
    ]
    assert event["partial"] is True
    assert "path=assets/large.bin reason=max_file_bytes" in caplog.text
    bundle = json.loads(ob.decrypt_content(event))
    assert [item["path"] for item in bundle["files"]] == ["SKILL.md"]
    assert bundle["skipped_files"] == skipped


def test_max_file_count_skips_files_after_the_limit(tmp_path):
    home = tmp_path / "hermes"
    write_skill(
        home / "skills",
        "many",
        files={"references/a.md": "a", "references/b.md": "b"},
    )
    ob = new_outbox(tmp_path)

    knowledge_store.poll(ob, home, knowledge_config=KnowledgeConfig(max_file_count=2))

    event = knowledge_events(ob)[0]
    assert event["payload"]["file_count"] == 2
    assert event["payload"]["skipped_files"] == [
        {
            "path": "references/b.md",
            "reason": "max_file_count",
            "byte_count": 1,
            "limit": 2,
        }
    ]


def test_file_count_includes_a_file_skipped_for_size(tmp_path):
    home = tmp_path / "hermes"
    write_skill(
        home / "skills",
        "many",
        files={"references/a.md": "too large", "references/b.md": "b"},
    )
    ob = new_outbox(tmp_path)
    config = KnowledgeConfig(max_file_bytes=8, max_file_count=2)

    knowledge_store.poll(ob, home, knowledge_config=config)

    skipped = knowledge_events(ob)[0]["payload"]["skipped_files"]
    assert [item["reason"] for item in skipped] == [
        "max_file_bytes",
        "max_file_count",
    ]


def test_max_artifact_bytes_bounds_the_complete_bundle(tmp_path):
    home = tmp_path / "hermes"
    write_skill(
        home / "skills",
        "bounded",
        "12345678",
        files={"assets/a.bin": "abcdefgh", "assets/b.bin": "ijklmnop"},
    )
    ob = new_outbox(tmp_path)
    config = KnowledgeConfig(max_file_bytes=16, max_artifact_bytes=16)

    knowledge_store.poll(ob, home, knowledge_config=config)

    event = knowledge_events(ob)[0]
    assert event["payload"]["byte_count"] == 16
    assert event["payload"]["skipped_files"] == [
        {
            "path": "assets/b.bin",
            "reason": "max_artifact_bytes",
            "byte_count": 8,
            "limit": 16,
        }
    ]
    bundle = json.loads(ob.decrypt_content(event))
    assert sum(item["byte_count"] for item in bundle["files"]) <= 16


# --- transport: store versions -> knowledge.record_written events ----------


def test_emits_event_with_restorable_encrypted_content(tmp_path):
    home = tmp_path / "hermes"
    body = "# deploy\nsteps: do the thing\n"
    ref = "reference — with unicode ✈\n"
    write_skill(home / "skills", "deploy", body, files={"references/how.md": ref})
    ob = new_outbox(tmp_path)

    counts = knowledge_store.poll(ob, home)

    assert counts == {knowledge_store.KNOWLEDGE_EVENT: 1}
    events = knowledge_events(ob)
    assert len(events) == 1
    payload = events[0]["payload"]
    assert payload["artifact_kind"] == "skill"
    assert payload["action"] == "create"
    assert payload["skill_name"] == "deploy"
    assert payload["version_seq"] == 1
    assert payload["origin"] == "background"
    assert payload["file_count"] == 2
    assert payload["content_format"] == knowledge_store.KNOWLEDGE_BUNDLE_FORMAT
    # The metadata is plaintext; the artifact content is encrypted and restores
    # byte-for-byte from the event alone.
    assert events[0].get("content_ciphertext") is not None
    bundle = json.loads(ob.decrypt_content(events[0]))
    assert bundle["format"] == knowledge_store.KNOWLEDGE_BUNDLE_FORMAT
    assert bundle["artifact_id"] == payload["artifact_id"]
    assert bundle["version_seq"] == payload["version_seq"]
    assert bundle["manifest_hash"] == payload["manifest_hash"]
    assert [
        (f["path"], f["byte_count"], f["content_hash"])
        for f in bundle["files"]
    ] == [
        (entry["path"], len(ob.get_blob(entry["blob_hash"])), entry["blob_hash"])
        for entry in ob.latest_knowledge_version(payload["artifact_id"])["manifest"]
    ]
    restored = {
        f["path"]: base64.b64decode(f["content_b64"]).decode("utf-8")
        for f in bundle["files"]
    }
    assert restored == {"SKILL.md": body, "references/how.md": ref}


def test_large_skill_uses_encrypted_chunks_and_restores_from_parent(tmp_path):
    home = tmp_path / "hermes"
    body = "large but complete\n" + ("x" * 2_500_000)
    write_skill(home / "skills", "large", body)
    ob = new_outbox(tmp_path)

    counts = knowledge_store.poll(ob, home)

    assert counts == {knowledge_store.KNOWLEDGE_EVENT: 1}
    parent = knowledge_events(ob)[0]
    assert parent["payload"]["content_storage"] == "chunked"
    assert "content_ciphertext" not in parent
    chunks = [
        event
        for event in ob.iter_events()
        if event["payload"]["event_type"] == "runtime.content_chunk_recorded"
    ]
    assert len(chunks) == parent["payload"]["content_chunk_count"]
    assert all(
        singleton_batch_size(event) <= MAX_INGEST_BATCH_BYTES
        for event in [*chunks, parent]
    )

    bundle = json.loads(ob.decrypt_content(parent))
    restored = base64.b64decode(bundle["files"][0]["content_b64"]).decode()
    assert restored == body


def test_memory_target_and_action_update(tmp_path):
    home = tmp_path / "hermes"
    memory = home / "memories" / "MEMORY.md"
    write(memory, "v1\n")
    ob = new_outbox(tmp_path)
    knowledge_store.poll(ob, home)
    write(memory, "v2\n")
    knowledge_store.poll(ob, home)

    events = [e["payload"] for e in knowledge_events(ob)]
    assert [(p["action"], p["version_seq"]) for p in events] == [("create", 1), ("update", 2)]
    assert all(p["artifact_kind"] == "memory" and p["target"] == "memory" for p in events)


def test_delete_emits_a_contentless_tombstone_event(tmp_path):
    home = tmp_path / "hermes"
    skills = home / "skills"
    skill_dir = write_skill(skills, "gone", "# v1\n")
    ob = new_outbox(tmp_path)
    knowledge_store.poll(ob, home)  # create event

    for path in sorted(skill_dir.rglob("*"), reverse=True):
        path.unlink()
    skill_dir.rmdir()
    counts = knowledge_store.poll(ob, home)  # tombstone event

    assert counts == {knowledge_store.KNOWLEDGE_EVENT: 1}
    events = [e for e in knowledge_events(ob) if e["payload"]["artifact_id"] == "skill:gone"]
    assert [e["payload"]["action"] for e in events] == ["create", "delete"]
    assert events[-1].get("content_ciphertext") is None  # a tombstone carries no content
    assert "content_format" not in events[-1]["payload"]


def test_events_are_not_duplicated_on_repoll(tmp_path):
    home = tmp_path / "hermes"
    write_skill(home / "skills", "x")
    ob = new_outbox(tmp_path)

    knowledge_store.poll(ob, home)
    before = len(knowledge_events(ob))
    assert knowledge_store.poll(ob, home) == {}  # nothing new to emit
    assert len(knowledge_events(ob)) == before


def test_backfills_a_version_that_predates_the_transport(tmp_path):
    ob = new_outbox(tmp_path)
    # A version recorded directly (as older code did) leaves no event...
    blob = ob.put_blob("hello\n")
    ob.upsert_knowledge_artifact(
        "memory:memory", kind="memory", name="memory", category=None,
        provenance="agent", first_seen=1.0,
    )
    ob.append_knowledge_version(
        "memory:memory",
        manifest=[{"path": "MEMORY.md", "blob_hash": blob}],
        occurred_at=1.0,
        origin="background",
    )
    assert knowledge_events(ob) == []

    # ...until the emit pass backfills it from the stored blob.
    emitted = knowledge_store._emit_pending_events(ob, "auto")

    assert emitted == 1
    events = knowledge_events(ob)
    assert len(events) == 1
    bundle = json.loads(ob.decrypt_content(events[0]))
    assert base64.b64decode(bundle["files"][0]["content_b64"]) == b"hello\n"


def test_bundle_excludes_paths_that_are_not_safe_relative_posix_paths(tmp_path):
    home = tmp_path / "hermes"
    skill = write_skill(home / "skills", "safe", "# safe\n")
    write(skill / "references" / "bad\\name.md", "not portable\n")
    ob = new_outbox(tmp_path)

    knowledge_store.poll(ob, home)

    bundle = json.loads(ob.decrypt_content(knowledge_events(ob)[0]))
    assert [entry["path"] for entry in bundle["files"]] == ["SKILL.md"]


def test_skill_scan_rejects_file_symlink_outside_artifact(tmp_path):
    home = tmp_path / "hermes"
    skill = write_skill(home / "skills", "safe", "# safe\n")
    outside = tmp_path / "secret.txt"
    write(outside, "must not be captured\n")
    (skill / "assets").mkdir()
    (skill / "assets" / "linked.txt").symlink_to(outside)
    ob = new_outbox(tmp_path)

    knowledge_store.poll(ob, home)

    bundle = json.loads(ob.decrypt_content(knowledge_events(ob)[0]))
    assert [entry["path"] for entry in bundle["files"]] == ["SKILL.md"]


def test_skill_scan_rejects_directory_symlink_outside_artifact(tmp_path):
    home = tmp_path / "hermes"
    skill = write_skill(home / "skills", "safe", "# safe\n")
    outside = tmp_path / "outside-assets"
    write(outside / "secret.txt", "must not be captured\n")
    (skill / "assets").symlink_to(outside, target_is_directory=True)
    ob = new_outbox(tmp_path)

    knowledge_store.poll(ob, home)

    bundle = json.loads(ob.decrypt_content(knowledge_events(ob)[0]))
    assert [entry["path"] for entry in bundle["files"]] == ["SKILL.md"]


def test_memory_scan_rejects_symlink_outside_memory_root(tmp_path):
    home = tmp_path / "hermes"
    outside = tmp_path / "secret-memory.md"
    write(outside, "must not be captured\n")
    (home / "memories").mkdir(parents=True)
    (home / "memories" / "MEMORY.md").symlink_to(outside)
    ob = new_outbox(tmp_path)

    assert knowledge_store.poll(ob, home) == {}
    assert ob.knowledge_artifact_ids() == []


def test_normal_nested_skill_files_still_restore_byte_for_byte(tmp_path):
    home = tmp_path / "hermes"
    binary = bytes(range(256))
    skill = write_skill(home / "skills", "nested", "# nested\n")
    asset = skill / "assets" / "nested" / "bytes.bin"
    asset.parent.mkdir(parents=True)
    asset.write_bytes(binary)
    ob = new_outbox(tmp_path)

    knowledge_store.poll(ob, home)

    restored = knowledge_store.restore_version(ob, "skill:nested")
    assert restored == {
        "SKILL.md": b"# nested\n",
        "assets/nested/bytes.bin": binary,
    }


def test_fleet_agent_captures_and_sends_bundle_with_public_key_only(
    tmp_path, monkeypatch
):
    operator = tmp_path / "operator"
    operator.mkdir()
    keypair = keystore.mint_operator_keypair(operator)
    bridge = tmp_path / "bridge"
    bridge.mkdir()
    keystore.write_public_key(bridge, keypair.public)
    home = tmp_path / "hermes"
    write_skill(home / "skills", "fleet", "# fleet\n")
    ob = new_outbox(tmp_path)
    monkeypatch.setattr(
        ob,
        "get_blob",
        lambda *_args, **_kwargs: pytest.fail("fleet capture tried to decrypt a blob"),
    )

    counts = knowledge_store.poll(ob, home)

    assert counts == {knowledge_store.KNOWLEDGE_EVENT: 1}
    assert not keystore.has_secret(bridge)
    event = knowledge_events(ob)[0]
    bundle = json.loads(ob.decrypt_content(event, keypair=keypair))
    assert base64.b64decode(bundle["files"][0]["content_b64"]) == b"# fleet\n"


def test_knowledge_bundle_v1_matches_the_compatibility_fixture(tmp_path):
    fixture = json.loads(
        (Path(__file__).parent / "fixtures" / "knowledge_bundle_v1.json").read_text(
            encoding="utf-8"
        )
    )
    home = tmp_path / "hermes"
    body = "# Deploy\n\nRun the release checklist.\n"
    write_skill(home / "skills", "deploy", body)
    ob = new_outbox(tmp_path)

    knowledge_store.poll(ob, home)

    event = knowledge_events(ob)[0]
    assert event["payload"]["content_format"] == "knowledge.bundle.v1"
    assert json.loads(ob.decrypt_content(event)) == fixture


# --- capture-tick efficiency: stat short-circuit (#159), cursor query (#161)


def settle(monkeypatch):
    """Disable the racily-clean window so fresh test files cache immediately.

    ctime cannot be backdated from userspace, so a test that needs the stat
    cache to engage right after writing its fixture files shrinks the window
    instead of waiting it out.
    """
    monkeypatch.setattr(knowledge_store, "_RACY_WINDOW_SECONDS", -1.0)


def count_snapshots(monkeypatch):
    """Record every artifact `_snapshot_files` reads (the read+hash path)."""
    reads: list[str] = []
    real = knowledge_store._snapshot_files

    def counting(outbox, config, artifact_id, files, **kwargs):
        reads.append(artifact_id)
        return real(outbox, config, artifact_id, files, **kwargs)

    monkeypatch.setattr(knowledge_store, "_snapshot_files", counting)
    return reads


def test_unchanged_artifacts_are_not_reread_or_rehashed(tmp_path, monkeypatch):
    home = tmp_path / "hermes"
    write(home / "memories" / "MEMORY.md", "stable\n")
    write_skill(
        home / "skills", "steady", "# steady\n", files={"references/a.md": "alpha"}
    )
    settle(monkeypatch)
    ob = new_outbox(tmp_path)
    knowledge_store.poll(ob, home)

    reads = count_snapshots(monkeypatch)
    hashes: list[bytes] = []
    monkeypatch.setattr(
        ob, "put_blob", lambda raw: pytest.fail("unchanged poll stored a blob")
    )
    monkeypatch.setattr(
        type(ob), "_content_hash", staticmethod(lambda raw: hashes.append(raw))
    )

    assert knowledge_store.poll(ob, home) == {}
    assert reads == []
    assert hashes == []


def test_changed_file_is_recaptured_after_a_cached_poll(tmp_path, monkeypatch):
    home = tmp_path / "hermes"
    memory = home / "memories" / "MEMORY.md"
    write(memory, "v1\n")
    settle(monkeypatch)
    ob = new_outbox(tmp_path)
    knowledge_store.poll(ob, home)

    write(memory, "v2 is longer\n")
    reads = count_snapshots(monkeypatch)

    counts = knowledge_store.poll(ob, home)

    assert counts == {knowledge_store.KNOWLEDGE_EVENT: 1}
    assert reads == ["memory:memory"]
    assert ob.latest_knowledge_version("memory:memory")["seq"] == 2


def test_same_mtime_same_size_edit_is_still_captured(tmp_path, monkeypatch):
    home = tmp_path / "hermes"
    memory = home / "memories" / "MEMORY.md"
    write(memory, "aaaa\n")
    old = time.time() - 60.0
    os.utime(memory, (old, old))
    settle(monkeypatch)
    ob = new_outbox(tmp_path)
    knowledge_store.poll(ob, home)

    # Same byte count, mtime restored to the cached value. Only ctime moves
    # (utime cannot set it back), which the fingerprint must catch. The sleep
    # guarantees the rewrite lands on a later coarse-clock tick than the
    # cached ctime — the same separation the real 2 s window enforces.
    time.sleep(0.05)
    write(memory, "bbbb\n")
    os.utime(memory, (old, old))
    knowledge_store.poll(ob, home)

    latest = ob.latest_knowledge_version("memory:memory")
    assert latest["seq"] == 2
    assert ob.get_blob(latest["manifest"][0]["blob_hash"]) == b"bbbb\n"


def test_recently_modified_file_is_reread_on_the_next_poll(tmp_path, monkeypatch):
    home = tmp_path / "hermes"
    write(home / "memories" / "MEMORY.md", "fresh\n")  # mtime = now
    ob = new_outbox(tmp_path)
    knowledge_store.poll(ob, home)

    reads = count_snapshots(monkeypatch)

    # Inside the racily-clean window the fingerprint is not cached, so the
    # next tick re-reads (and finds nothing new).
    assert knowledge_store.poll(ob, home) == {}
    assert reads == ["memory:memory"]


def test_deleted_then_restored_skill_is_recaptured(tmp_path, monkeypatch):
    home = tmp_path / "hermes"
    skill = write_skill(home / "skills", "phoenix", "# v1\n")
    settle(monkeypatch)
    ob = new_outbox(tmp_path)
    knowledge_store.poll(ob, home)
    knowledge_store.poll(ob, home)  # cached, no-op

    (skill / "SKILL.md").unlink()
    skill.rmdir()
    knowledge_store.poll(ob, home)  # tombstone
    assert ob.latest_knowledge_version("skill:phoenix")["is_tombstone"]

    write_skill(home / "skills", "phoenix", "# v1\n")
    knowledge_store.poll(ob, home)

    latest = ob.latest_knowledge_version("skill:phoenix")
    assert latest["seq"] == 3
    assert not latest["is_tombstone"]


def test_knowledge_versions_after_returns_only_rows_above_seq(tmp_path):
    ob = new_outbox(tmp_path)
    ob.upsert_knowledge_artifact(
        "memory:memory", kind="memory", name="memory", category=None,
        provenance="agent", first_seen=1.0,
    )
    for step, text in enumerate(["v1\n", "v2\n", "v3\n"], start=1):
        blob = ob.put_blob(text)
        ob.append_knowledge_version(
            "memory:memory",
            manifest=[{"path": "MEMORY.md", "blob_hash": blob}],
            occurred_at=float(step),
            origin="background",
        )

    assert [v["seq"] for v in ob.knowledge_versions_after("memory:memory", 0)] == [1, 2, 3]
    assert [v["seq"] for v in ob.knowledge_versions_after("memory:memory", 2)] == [3]
    assert ob.knowledge_versions_after("memory:memory", 3) == []


def test_steady_state_emit_decodes_no_version_rows(tmp_path, monkeypatch):
    home = tmp_path / "hermes"
    write_skill(home / "skills", "quiet", "# quiet\n")
    ob = new_outbox(tmp_path)
    knowledge_store.poll(ob, home)  # capture + emit advance the cursor

    decoded: list[object] = []
    real_row = type(ob)._version_row
    monkeypatch.setattr(
        type(ob),
        "_version_row",
        staticmethod(lambda row: decoded.append(row) or real_row(row)),
    )

    assert knowledge_store._emit_pending_events(ob, "auto") == 0
    assert decoded == []


def test_emit_resumes_strictly_above_the_cursor(tmp_path):
    ob = new_outbox(tmp_path)
    ob.upsert_knowledge_artifact(
        "memory:memory", kind="memory", name="memory", category=None,
        provenance="agent", first_seen=1.0,
    )
    for step, text in enumerate(["v1\n", "v2\n", "v3\n"], start=1):
        blob = ob.put_blob(text)
        ob.append_knowledge_version(
            "memory:memory",
            manifest=[{"path": "MEMORY.md", "blob_hash": blob}],
            occurred_at=float(step),
            origin="background",
        )
    ob.set_meta("knowledge:emitted:memory:memory", "1")

    emitted = knowledge_store._emit_pending_events(ob, "auto")

    assert emitted == 2
    assert [e["payload"]["version_seq"] for e in knowledge_events(ob)] == [2, 3]
    assert ob.get_meta("knowledge:emitted:memory:memory") == "3"
