"""Tests for the Phase 3 knowledge reconciler (issue #79).

Two integrity checks over the disk → store → event pipeline, each a backstop
for one stage the #78 scanner runs on the capture path:

- **Store-vs-disk drift** (``gap_kind='uncaptured_knowledge'``) — an artifact
  whose on-disk content the scanner never versioned. Flagged AND healed: the
  missed version is captured through the scanner's own path.
- **Store-vs-event gap** (``gap_kind='unemitted_knowledge'``) — a store version
  the emitter never turned into a ``knowledge.record_written``.

Self-contained: builds a real Hermes-shaped home and ages file mtimes with
``os.utime`` so drift is judged against a fixed clock, never wall-time. Every
``reconcile`` call passes an explicit ``now`` and a small grace window.
"""

from __future__ import annotations

import json
import os

from hermes_flight_recorder.collector import knowledge_store
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile
from hermes_flight_recorder.envelope import validate

B = 1_800_000_000.0


def new_outbox(tmp_path) -> Outbox:
    ob = Outbox.open(tmp_path / "bridge")
    ob.initialize()
    return ob


def write(path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def age(root, epoch: float) -> None:
    """Set the mtime of every file under ``root`` (or the file itself)."""
    paths = [root] if root.is_file() else list(root.rglob("*"))
    for p in paths:
        if p.is_file():
            os.utime(p, (epoch, epoch))


def gaps(ob, gap_kind: str):
    return [
        e
        for e in ob.iter_events()
        if e["payload"]["event_type"] == "reconcile.gap_detected"
        and e["payload"].get("gap_kind") == gap_kind
        and e["source"] == "reconciler"
    ]


def dedup_keys(ob):
    return [r[0] for r in ob._conn.execute("SELECT dedup_key FROM events").fetchall()]


CFG = ReconcileConfig(knowledge_drift_grace=300.0)


# --- store-vs-disk drift --------------------------------------------------
def test_missed_create_flags_and_heals(tmp_path):
    """An artifact on disk with no store version: a scanner-missed create.
    Reconcile flags it and captures the version (heal)."""
    home = tmp_path / "hermes"
    write(home / "skills" / "deploy" / "SKILL.md", "# deploy\n")
    age(home / "skills" / "deploy", B)
    ob = new_outbox(tmp_path)

    reconcile(ob, home, now=B + 1000, config=CFG)

    found = gaps(ob, "uncaptured_knowledge")
    assert len(found) == 1
    assert found[0]["payload"]["subject_id"] == "skill:deploy"
    assert found[0]["payload"]["stored_manifest_hash"] is None
    # Healed: the missed version is now captured AND emitted as an event (the
    # repair is a side effect, not a reconcile finding, so it's not in counts).
    latest = ob.latest_knowledge_version("skill:deploy")
    assert latest is not None and latest["seq"] == 1
    written = [
        e for e in ob.iter_events()
        if e["payload"]["event_type"] == "knowledge.record_written"
        and e["payload"]["artifact_id"] == "skill:deploy"
    ]
    assert len(written) == 1
    for e in ob.iter_events():
        validate(e)


def test_missed_patch_flags_and_heals_new_version(tmp_path):
    home = tmp_path / "hermes"
    skill = home / "skills" / "deploy"
    write(skill / "SKILL.md", "# deploy v1\n")
    age(skill, B)
    ob = new_outbox(tmp_path)
    knowledge_store.poll(ob, home)  # capture v1 normally
    assert ob.latest_knowledge_version("skill:deploy")["seq"] == 1

    # A change the scanner never saw (capture was down).
    write(skill / "SKILL.md", "# deploy v2 — patched\n")
    age(skill, B + 10)

    reconcile(ob, home, now=B + 1000, config=CFG)

    assert len(gaps(ob, "uncaptured_knowledge")) == 1
    assert ob.latest_knowledge_version("skill:deploy")["seq"] == 2  # healed


def test_memory_file_drift_flags_and_heals(tmp_path):
    home = tmp_path / "hermes"
    mem = home / "memories" / "MEMORY.md"
    write(mem, "remember v1\n")
    age(mem, B)
    ob = new_outbox(tmp_path)
    knowledge_store.poll(ob, home)

    write(mem, "remember v2 — changed\n")
    age(mem, B + 10)
    reconcile(ob, home, now=B + 1000, config=CFG)

    found = gaps(ob, "uncaptured_knowledge")
    assert len(found) == 1
    assert found[0]["payload"]["subject_id"] == "memory:memory"
    assert ob.latest_knowledge_version("memory:memory")["seq"] == 2


# --- store-vs-event gap ---------------------------------------------------
def test_unemitted_version_flags(tmp_path):
    """A store version the emitter never shipped (cursor never advanced)."""
    home = tmp_path / "hermes"
    ob = new_outbox(tmp_path)
    ob.upsert_knowledge_artifact(
        "skill:ghost", kind="skill", name="ghost", category=None,
        provenance="agent", first_seen=B,
    )
    ob.append_knowledge_version(
        "skill:ghost",
        manifest=[{"path": "SKILL.md", "blob_hash": "sha256:deadbeef"}],
        occurred_at=B,
        origin="foreground",
    )
    # No emitted cursor set, and the artifact is not on disk -> only the
    # store-vs-event check should fire, not drift.

    reconcile(ob, home, now=B + 1000, config=CFG)

    found = gaps(ob, "unemitted_knowledge")
    assert len(found) == 1
    assert found[0]["payload"]["subject_id"] == "skill:ghost:v1"
    assert found[0]["payload"]["origin"] == "foreground"
    assert gaps(ob, "uncaptured_knowledge") == []  # not on disk -> no drift


# --- no false positives ---------------------------------------------------
def test_bundled_skill_never_flagged_or_backfilled(tmp_path):
    home = tmp_path / "hermes"
    skills = home / "skills"
    write(skills / "bundled-one" / "SKILL.md", "# bundled\n")
    write(skills / ".bundled_manifest", "bundled-one:abc123\n")
    age(skills / "bundled-one", B)
    ob = new_outbox(tmp_path)

    counts = reconcile(ob, home, now=B + 1000, config=CFG)

    assert gaps(ob, "uncaptured_knowledge") == []
    assert ob.knowledge_artifact_ids() == []  # never backfilled into the store
    assert counts.get("reconcile.gap_detected", 0) == 0


def test_fresh_change_within_grace_not_flagged(tmp_path):
    home = tmp_path / "hermes"
    write(home / "skills" / "deploy" / "SKILL.md", "# deploy\n")
    age(home / "skills" / "deploy", B)
    ob = new_outbox(tmp_path)

    # now only 100s past the file's mtime; grace is 300 -> a healthy capture
    # would still be catching up, so this is not yet drift.
    counts = reconcile(ob, home, now=B + 100, config=CFG)

    assert gaps(ob, "uncaptured_knowledge") == []
    assert counts.get("reconcile.gap_detected", 0) == 0


# --- dedup / idempotency --------------------------------------------------
def test_drift_is_idempotent_after_heal(tmp_path):
    home = tmp_path / "hermes"
    write(home / "skills" / "deploy" / "SKILL.md", "# deploy\n")
    age(home / "skills" / "deploy", B)
    ob = new_outbox(tmp_path)

    reconcile(ob, home, now=B + 1000, config=CFG)
    assert len(gaps(ob, "uncaptured_knowledge")) == 1
    n = ob.count()

    # A second pass: disk now matches the healed store version -> no new drift,
    # and the healed version was emitted -> no unemitted gap either.
    second = reconcile(ob, home, now=B + 1000, config=CFG)
    assert ob.count() == n
    assert second.get("reconcile.gap_detected", 0) == 0
    key = [k for k in dedup_keys(ob) if k.startswith("reconcile:knowledge:skill:deploy:")]
    assert len(key) == 1  # exactly one drift finding, ever


def test_unemitted_dedup_is_stable(tmp_path):
    home = tmp_path / "hermes"
    ob = new_outbox(tmp_path)
    ob.upsert_knowledge_artifact(
        "skill:ghost", kind="skill", name="ghost", category=None,
        provenance="agent", first_seen=B,
    )
    ob.append_knowledge_version(
        "skill:ghost",
        manifest=[{"path": "SKILL.md", "blob_hash": "sha256:deadbeef"}],
        occurred_at=B, origin="foreground",
    )

    first = reconcile(ob, home, now=B + 1000, config=CFG)
    assert first.get("reconcile.gap_detected") == 1
    n = ob.count()
    second = reconcile(ob, home, now=B + 2000, config=CFG)
    assert ob.count() == n  # no re-fire despite a later clock
    assert second.get("reconcile.gap_detected", 0) == 0
