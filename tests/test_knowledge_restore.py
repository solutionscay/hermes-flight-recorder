"""Restore round-trip for the content-addressed knowledge store (Phase 3, #80).

The gate-critical guarantee: any single store version restores its artifact
byte-for-byte from the deduplicated blobs alone. Drives the real scanner
(``knowledge_store.poll``) over a Hermes-shaped home, then reconstructs each
version through ``restore_version`` and compares bytes — proving create, a
background edit (blob dedup + ``origin='background'``), a memory add, and a
delete (tombstone that keeps prior versions restorable).
"""

from __future__ import annotations

from hermes_flight_recorder.collector import knowledge_store
from hermes_flight_recorder.collector.outbox import Outbox


def new_outbox(tmp_path) -> Outbox:
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


def write(path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def blob_count(ob) -> int:
    return ob._conn.execute("SELECT COUNT(*) FROM knowledge_blob").fetchone()[0]


def test_create_restores_byte_for_byte(tmp_path):
    home = tmp_path / "hermes"
    write(home / "skills" / "deploy" / "SKILL.md", "# deploy\nstep one\n")
    write(home / "skills" / "deploy" / "references" / "notes.md", "reference body\n")
    ob = new_outbox(tmp_path)

    knowledge_store.poll(ob, home)

    restored = knowledge_store.restore_version(ob, "skill:deploy", 1)
    assert restored == {
        "SKILL.md": b"# deploy\nstep one\n",
        "references/notes.md": b"reference body\n",
    }


def test_background_edit_dedups_blobs_and_marks_origin(tmp_path):
    home = tmp_path / "hermes"
    skill = home / "skills" / "deploy"
    write(skill / "SKILL.md", "# deploy v1\n")
    write(skill / "references" / "notes.md", "unchanged\n")
    ob = new_outbox(tmp_path)
    knowledge_store.poll(ob, home)
    assert blob_count(ob) == 2  # SKILL.md + notes.md

    # A background edit to one file only (the curator runs persist-disabled, so
    # the scanner — the filesystem — is the only witness).
    write(skill / "SKILL.md", "# deploy v2 improved\n")
    knowledge_store.poll(ob, home)

    latest = ob.latest_knowledge_version("skill:deploy")
    assert latest["seq"] == 2
    assert latest["origin"] == "background"
    assert blob_count(ob) == 3  # one new SKILL.md blob; notes.md blob reused
    # Both versions restore byte-for-byte from the shared/deduped blobs.
    assert knowledge_store.restore_version(ob, "skill:deploy", 1)["SKILL.md"] == b"# deploy v1\n"
    assert knowledge_store.restore_version(ob, "skill:deploy", 2)["SKILL.md"] == b"# deploy v2 improved\n"
    assert (
        knowledge_store.restore_version(ob, "skill:deploy", 1)["references/notes.md"]
        == knowledge_store.restore_version(ob, "skill:deploy", 2)["references/notes.md"]
    )


def test_memory_add_is_versioned_and_restorable(tmp_path):
    home = tmp_path / "hermes"
    mem = home / "memories" / "MEMORY.md"
    write(mem, "first fact\n")
    ob = new_outbox(tmp_path)
    knowledge_store.poll(ob, home)

    write(mem, "first fact\nsecond fact\n")  # a memory `add`
    knowledge_store.poll(ob, home)

    assert ob.latest_knowledge_version("memory:memory")["seq"] == 2
    assert knowledge_store.restore_version(ob, "memory:memory", 1) == {"MEMORY.md": b"first fact\n"}
    assert knowledge_store.restore_version(ob, "memory:memory", 2) == {
        "MEMORY.md": b"first fact\nsecond fact\n"
    }


def test_delete_tombstones_but_prior_version_still_restores(tmp_path):
    home = tmp_path / "hermes"
    skill = home / "skills" / "throwaway"
    write(skill / "SKILL.md", "# throwaway\nprecious content\n")
    ob = new_outbox(tmp_path)
    knowledge_store.poll(ob, home)

    import shutil

    shutil.rmtree(skill)  # a foreground hard delete
    knowledge_store.poll(ob, home)

    latest = ob.latest_knowledge_version("skill:throwaway")
    assert latest["is_tombstone"]
    assert latest["seq"] == 2
    # The tombstone restores to empty, but the pre-delete version is intact.
    assert knowledge_store.restore_version(ob, "skill:throwaway", 2) == {}
    assert knowledge_store.restore_version(ob, "skill:throwaway", 1) == {
        "SKILL.md": b"# throwaway\nprecious content\n"
    }


def test_restore_unknown_artifact_or_version_returns_none(tmp_path):
    ob = new_outbox(tmp_path)
    assert knowledge_store.restore_version(ob, "skill:nope") is None
    ob.upsert_knowledge_artifact(
        "skill:x", kind="skill", name="x", category=None, provenance="agent", first_seen=1.0
    )
    ob.append_knowledge_version(
        "skill:x",
        manifest=[{"path": "SKILL.md", "blob_hash": ob.put_blob(b"hi\n")}],
        occurred_at=1.0,
        origin="background",
    )
    assert knowledge_store.restore_version(ob, "skill:x", 99) is None
    assert knowledge_store.restore_version(ob, "skill:x", 1) == {"SKILL.md": b"hi\n"}
