"""Tests for the observe knowledge view (Phase 3, #80).

Drives ``observe.render_knowledge`` over a store populated by the real scanner.
Asserts the per-artifact contract: header with state + version count, the latest
manifest (or a deleted marker), the version history joined to
``knowledge.record_written`` events, and a manifest-level diff between the last
two versions. Never asserts on decrypted content — the view shows hashes, not
text.
"""

from __future__ import annotations

from hermes_flight_recorder import observe
from hermes_flight_recorder.collector import knowledge_store
from hermes_flight_recorder.collector.outbox import Outbox


def new_outbox(tmp_path) -> Outbox:
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


def write(path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def view(ob) -> str:
    return "\n".join(observe.render_knowledge(ob, list(ob.iter_events())))


def test_empty_store_message(tmp_path):
    ob = new_outbox(tmp_path)
    assert observe.render_knowledge(ob, []) == ["(no knowledge artifacts captured)"]


def test_single_skill_shows_header_files_and_history(tmp_path):
    home = tmp_path / "hermes"
    write(home / "skills" / "deploy" / "SKILL.md", "# deploy\n")
    ob = new_outbox(tmp_path)
    knowledge_store.poll(ob, home)

    body = view(ob)
    assert "◈ skill deploy  [live]  v1  (1 version(s), agent)" in body
    assert "files:" in body
    assert "SKILL.md" in body
    assert "sha256:" in body
    assert "v1  background" in body
    assert "event✓" in body  # the version's knowledge.record_written is joined


def test_second_version_shows_diff(tmp_path):
    home = tmp_path / "hermes"
    skill = home / "skills" / "deploy"
    write(skill / "SKILL.md", "# deploy v1\n")
    write(skill / "references" / "a.md", "keep\n")
    ob = new_outbox(tmp_path)
    knowledge_store.poll(ob, home)
    write(skill / "SKILL.md", "# deploy v2\n")  # change one file
    knowledge_store.poll(ob, home)

    body = view(ob)
    assert "v2" in body
    assert "diff v1→v2:" in body
    assert "~ SKILL.md" in body
    assert "~ references/a.md" not in body  # unchanged file not in the diff


def test_added_and_removed_files_in_diff(tmp_path):
    home = tmp_path / "hermes"
    skill = home / "skills" / "deploy"
    write(skill / "SKILL.md", "# deploy\n")
    write(skill / "references" / "old.md", "old\n")
    ob = new_outbox(tmp_path)
    knowledge_store.poll(ob, home)
    (skill / "references" / "old.md").unlink()
    write(skill / "references" / "new.md", "new\n")
    knowledge_store.poll(ob, home)

    body = view(ob)
    assert "+ references/new.md" in body
    assert "- references/old.md" in body


def test_deleted_artifact_shows_tombstone(tmp_path):
    home = tmp_path / "hermes"
    skill = home / "skills" / "gone"
    write(skill / "SKILL.md", "# gone\n")
    ob = new_outbox(tmp_path)
    knowledge_store.poll(ob, home)
    import shutil

    shutil.rmtree(skill)
    knowledge_store.poll(ob, home)

    body = view(ob)
    assert "[deleted]" in body
    assert "files: (deleted)" in body
    assert "tombstone" in body


def test_category_skill_label_and_memory(tmp_path):
    home = tmp_path / "hermes"
    write(home / "skills" / "ops" / "deploy" / "SKILL.md", "# nested\n")
    write(home / "memories" / "USER.md", "the user\n")
    ob = new_outbox(tmp_path)
    knowledge_store.poll(ob, home)

    body = view(ob)
    assert "◈ skill ops/deploy" in body  # category-qualified label
    assert "◈ user_profile user" in body
