#!/usr/bin/env python3
"""Live knowledge check + PoC gate — Phase 3 (issue #80), the milestone gate.

Two legs, the same shape as ``scripts/kanban_live_check.py`` and
``scripts/cron_death_live_check.py``:

- **Leg A (live home, READ-ONLY).** Scan the real ``skills/`` and ``memories/``
  into a throwaway outbox with ``knowledge_store.poll``, then ``reconcile``. A
  home the scanner already captured raises **zero** false knowledge findings
  (no ``uncaptured_knowledge`` / ``unemitted_knowledge``), and **no bundled or
  Hub skill** is ever ingested. If the live home has any Hermes-created artifact,
  one is restored from the store and byte-compared to disk. The real
  ``skills/`` + ``memories/`` files are asserted byte-for-byte unchanged.

- **Leg B (disposable home).** Manufacture the exact on-disk artifacts Hermes
  writes — a skill (``SKILL.md`` + a reference file) and ``MEMORY.md`` — then
  drive the real pipeline: scan → store version → ``knowledge.record_written``
  event, and **restore the skill from the store, byte-for-byte**. Then a
  background edit (one file, blob-deduped, ``origin='background'``), a memory
  ``add``, and a delete (a tombstone whose prior version still restores).

Leg A proves the scanner/reconciler hold against genuine Hermes artifacts; Leg B
proves the create → capture → restore → delete round-trip end to end.

Usage:  python scripts/knowledge_live_check.py [--hermes-home PATH] [-v]
Exit:   0 if every non-skipped assertion passes, 1 otherwise.
"""

from __future__ import annotations

import sys
from functools import partial
from pathlib import Path

# Runnable standalone and spec-loadable: repo root first, then the sibling _gate.
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _gate import run_gate
from hermes_flight_recorder.collector import knowledge_store
from hermes_flight_recorder.collector._common import (
    hermes_created_skills,
    memory_files,
    resolve_hermes_home,
)
from hermes_flight_recorder.collector.outbox import Outbox
from hermes_flight_recorder.collector.reconcile import ReconcileConfig, reconcile

VERBOSE = "-v" in sys.argv[1:]

_KNOWLEDGE_GAP_KINDS = {"uncaptured_knowledge", "unemitted_knowledge"}


def _hermes_home() -> Path:
    for i, a in enumerate(sys.argv):
        if a == "--hermes-home" and i + 1 < len(sys.argv):
            return Path(sys.argv[i + 1]).expanduser()
    return resolve_hermes_home(None)


def _log(msg: str) -> None:
    if VERBOSE:
        print(f"      {msg}")


def _note(msg: str) -> None:
    print(f"      {msg}")


def _new_outbox(tmp: Path) -> Outbox:
    ob = Outbox.open(tmp / "flight-recorder")
    ob.initialize()
    return ob


def _knowledge_findings(ob: Outbox) -> list[dict]:
    return [
        e["payload"]
        for e in ob.iter_events()
        if e["payload"]["event_type"] == "reconcile.gap_detected"
        and e["payload"].get("gap_kind") in _KNOWLEDGE_GAP_KINDS
    ]


def _knowledge_events(ob: Outbox) -> list[dict]:
    return [
        e["payload"]
        for e in ob.iter_events()
        if e["payload"]["event_type"] == "knowledge.record_written"
    ]


# --- Leg A: live, read-only ----------------------------------------------
def leg_a_live_readonly(home: Path, tmp: Path) -> list[str]:
    fails: list[str] = []

    # Byte-for-byte snapshot of every tracked file before we touch the home.
    tracked = [p for _, p in memory_files(home)]
    for _n, _c, sdir in hermes_created_skills(home):
        tracked += [p for p in sdir.rglob("*") if p.is_file()]
    before = {p: p.read_bytes() for p in tracked}

    ob = _new_outbox(tmp)
    try:
        knowledge_store.poll(ob, home)  # capture current knowledge into the store
        # A home the scanner just captured must reconcile clean.
        reconcile(ob, home, config=ReconcileConfig())
        findings = _knowledge_findings(ob)
        captured = ob.knowledge_artifact_ids()

        # No false drift/unemitted findings after a full capture.
        if findings:
            kinds = [f.get("gap_kind") for f in findings]
            fails.append(f"Leg A: {len(findings)} false knowledge finding(s) after capture: {kinds}")

        # No bundled/Hub skill was ingested. hermes_created_skills is the ONLY
        # allowed source of skill artifacts; anything else is a backfill leak.
        allowed_skill_ids = set()
        for name, category, _dir in hermes_created_skills(home):
            allowed_skill_ids.add(f"skill:{category}/{name}" if category else f"skill:{name}")
        leaked = [
            a for a in captured
            if a.startswith("skill:") and a not in allowed_skill_ids
        ]
        if leaked:
            fails.append(f"Leg A: {len(leaked)} non-Hermes-created skill(s) ingested: {leaked}")

        # Positive: if any artifact exists, one must restore byte-for-byte.
        _leg_a_positive(ob, home, captured, fails)
    finally:
        ob.close()

    # Read-only: no tracked file may change during scan + reconcile.
    for p, data in before.items():
        if p.read_bytes() != data:
            fails.append(f"read-only: {p} changed during scan/reconcile")
    _log(f"read-only: {len(before)} knowledge file(s) unchanged")
    return fails


def _leg_a_positive(ob: Outbox, home: Path, captured: list[str], fails: list[str]) -> None:
    """Restore one live artifact from the store and byte-compare it to disk."""
    live = [a for a in captured if not (ob.latest_knowledge_version(a) or {}).get("is_tombstone")]
    if not live:
        _note("Leg A: no live Hermes-created artifact on the home — restore assertion skipped")
        return
    artifact_id = live[0]
    restored = knowledge_store.restore_version(ob, artifact_id)
    on_disk = _artifact_bytes_on_disk(home, artifact_id)
    if restored != on_disk:
        fails.append(f"Leg A: restore of {artifact_id} did not byte-match disk")
    else:
        _log(f"Leg A: restored {artifact_id} byte-for-byte from the store ({len(restored)} file(s))")


def _artifact_bytes_on_disk(home: Path, artifact_id: str) -> dict[str, bytes]:
    """The current on-disk ``{relative_path: bytes}`` for a captured artifact."""
    for target, path in memory_files(home):
        if artifact_id == f"memory:{target}":
            return {path.name: path.read_bytes()}
    for name, category, sdir in hermes_created_skills(home):
        aid = f"skill:{category}/{name}" if category else f"skill:{name}"
        if aid == artifact_id:
            return {
                str(p.relative_to(sdir)): p.read_bytes()
                for p in sdir.rglob("*")
                if p.is_file() and _is_tracked_skill_file(p, sdir)
            }
    return {}


def _is_tracked_skill_file(path: Path, sdir: Path) -> bool:
    rel = path.relative_to(sdir)
    if str(rel) == "SKILL.md":
        return True
    return rel.parts and rel.parts[0] in knowledge_store.SKILL_SUBDIRS


# --- Leg B: disposable home, full round-trip -----------------------------
def leg_b_roundtrip(home: Path, tmp: Path) -> list[str]:
    fails: list[str] = []
    disposable = tmp / "home"
    skill = disposable / "skills" / "livecheck-probe"
    _write(skill / "SKILL.md", "# livecheck probe\nstep one\n")
    _write(skill / "references" / "notes.md", "reference body\n")
    _write(disposable / "memories" / "MEMORY.md", "first fact\n")

    ob = _new_outbox(tmp)
    try:
        knowledge_store.poll(ob, disposable)

        # create → store version → event link.
        latest = ob.latest_knowledge_version("skill:livecheck-probe")
        if latest is None or latest["seq"] != 1:
            fails.append("Leg B: create did not produce v1 in the store")
            return fails
        events = [
            e for e in _knowledge_events(ob)
            if e["artifact_id"] == "skill:livecheck-probe" and e["version_seq"] == 1
        ]
        if len(events) != 1:
            fails.append(f"Leg B: create v1 has {len(events)} linked knowledge events, want 1")

        # restore round-trip, byte-for-byte.
        restored = knowledge_store.restore_version(ob, "skill:livecheck-probe", 1)
        expected = {
            "SKILL.md": b"# livecheck probe\nstep one\n",
            "references/notes.md": b"reference body\n",
        }
        if restored != expected:
            fails.append("Leg B: v1 did not restore byte-for-byte from the store")
        else:
            _log("Leg B: create → v1 → event, restored byte-for-byte")

        _leg_b_background_edit(ob, skill, disposable, fails)
        _leg_b_memory_add(ob, disposable, fails)
        _leg_b_delete(ob, skill, disposable, fails)
    finally:
        ob.close()
    return fails


def _leg_b_background_edit(ob, skill, disposable, fails) -> None:
    blobs_before = _blob_count(ob)
    _write(skill / "SKILL.md", "# livecheck probe v2\nstep one\nstep two\n")
    knowledge_store.poll(ob, disposable)
    latest = ob.latest_knowledge_version("skill:livecheck-probe")
    if latest["seq"] != 2 or latest["origin"] != "background":
        fails.append(f"Leg B: background edit → seq {latest['seq']} origin {latest['origin']}, want 2/background")
    if _blob_count(ob) != blobs_before + 1:
        fails.append("Leg B: background edit added != 1 blob (reference blob should dedup)")
    if knowledge_store.restore_version(ob, "skill:livecheck-probe", 2)["SKILL.md"] != (
        b"# livecheck probe v2\nstep one\nstep two\n"
    ):
        fails.append("Leg B: v2 did not restore byte-for-byte")
    else:
        _log("Leg B: background edit → v2 (origin=background, 1 new blob), restored")


def _leg_b_memory_add(ob, disposable, fails) -> None:
    _write(disposable / "memories" / "MEMORY.md", "first fact\nsecond fact\n")
    knowledge_store.poll(ob, disposable)
    if ob.latest_knowledge_version("memory:memory")["seq"] != 2:
        fails.append("Leg B: memory add did not produce v2")
    elif knowledge_store.restore_version(ob, "memory:memory", 2) != {"MEMORY.md": b"first fact\nsecond fact\n"}:
        fails.append("Leg B: memory v2 did not restore byte-for-byte")
    else:
        _log("Leg B: memory add → v2, restored")


def _leg_b_delete(ob, skill, disposable, fails) -> None:
    import shutil

    shutil.rmtree(skill)
    knowledge_store.poll(ob, disposable)
    latest = ob.latest_knowledge_version("skill:livecheck-probe")
    if not latest["is_tombstone"]:
        fails.append("Leg B: delete did not tombstone the artifact")
    # The pre-delete version must still restore — history is preserved.
    elif knowledge_store.restore_version(ob, "skill:livecheck-probe", 2)["SKILL.md"] != (
        b"# livecheck probe v2\nstep one\nstep two\n"
    ):
        fails.append("Leg B: pre-delete version lost after tombstone")
    else:
        _log("Leg B: delete → tombstone, prior version still restores")


def _blob_count(ob) -> int:
    return ob._conn.execute("SELECT COUNT(*) FROM knowledge_blob").fetchone()[0]


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


LEGS = [
    ("Leg A — live home, read-only, no false finding, no backfill", leg_a_live_readonly),
    ("Leg B — disposable home create → capture → restore → delete", leg_b_roundtrip),
]


def main() -> int:
    home = _hermes_home()
    if not home.exists():
        print(f"FAIL — Hermes home not found at {home}")
        return 1
    return run_gate(
        [
            "Live knowledge check — Phase 3 store/reconcile/restore vs a real Hermes home",
            f"Hermes home: {home}",
        ],
        [(name, partial(fn, home)) for name, fn in LEGS],
        passed="CHECK PASSED — the knowledge store, reconciler, and restore round-trip hold",
        failed="CHECK FAILED",
        width=66,
        catch=True,
    )


if __name__ == "__main__":
    raise SystemExit(main())
