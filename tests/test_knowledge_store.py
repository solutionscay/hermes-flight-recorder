"""Tests for the content-addressed knowledge store (Phase 3, issue #78).

Fixtures build a real Hermes-shaped home — ``memories/MEMORY.md`` + ``USER.md``
and skills under ``skills/`` — and drive the scanner over it. The scenarios pin
the rules the contract (#76) turns on: only Hermes-created skills are tracked,
versions deduplicate blobs, a re-scan is idempotent, a delete tombstones without
losing history, and content round-trips through encryption.
"""

from __future__ import annotations

import json
import os

from hermes_flight_recorder.collector import knowledge_store
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.recorder_config import KnowledgeConfig


def new_outbox(tmp_path):
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


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

    assert counts == {knowledge_store.VERSIONS_RECORDED: 3}
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

    assert counts == {knowledge_store.VERSIONS_RECORDED: 1}
    assert [v["seq"] for v in ob.knowledge_versions("skill:multi")] == [1, 2]
    assert blob_count(ob) == 4  # one new blob only; b.md and SKILL.md reused


def test_rescan_without_change_is_idempotent(tmp_path):
    home = tmp_path / "hermes"
    write(home / "memories" / "MEMORY.md", "stable\n")
    write_skill(home / "skills", "steady")
    ob = new_outbox(tmp_path)

    first = knowledge_store.poll(ob, home)
    second = knowledge_store.poll(ob, home)

    assert first == {knowledge_store.VERSIONS_RECORDED: 2}
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

    assert counts == {knowledge_store.VERSIONS_RECORDED: 1}
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
    assert counts == {knowledge_store.VERSIONS_RECORDED: 1}


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
