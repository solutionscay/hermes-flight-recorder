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

Leg A proves the scanner/reconciler hold against genuine Hermes artifacts. Leg B
proves the scanner round-trip. Leg C proves a foreground create and delete when
no final artifact remains on disk.

Usage:  python scripts/knowledge_live_check.py [--hermes-home PATH] [-v]
Exit:   0 if every non-skipped assertion passes, 1 otherwise.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from functools import partial
from pathlib import Path

# Runnable standalone and spec-loadable: repo root first, then the sibling _gate.
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _gate import run_gate
from hermes_flight_recorder.collector import knowledge_store, state_db
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


# --- Leg C: foreground lifecycle with no final disk artifact -------------
def leg_c_foreground_lifecycle(_home: Path, tmp: Path) -> list[str]:
    fails: list[str] = []
    disposable = tmp / "foreground-home"
    disposable.mkdir()
    content = "---\nname: transient-probe\n---\n\n# Transient probe\n"
    _write_foreground_state_db(disposable, content)
    ob = _new_outbox(tmp)
    try:
        state_db.poll(ob, disposable)
        events = [
            event
            for event in ob.iter_events()
            if event["payload"]["event_type"] == "knowledge.record_written"
        ]
        actions = [event["payload"]["action"] for event in events]
        if actions != ["create", "delete"]:
            fails.append(
                f"Leg C: foreground actions are {actions}, want create then delete"
            )
            return fails

        versions = ob.knowledge_versions("skill:live/transient-probe")
        if len(versions) != 2 or not versions[-1]["is_tombstone"]:
            fails.append("Leg C: foreground lifecycle did not end in a v2 tombstone")
        restored = knowledge_store.restore_version(
            ob, "skill:live/transient-probe", 1
        )
        if restored != {"SKILL.md": content.encode()}:
            fails.append("Leg C: the transient create version did not restore")
        if (disposable / "skills" / "live" / "transient-probe").exists():
            fails.append("Leg C: the transient skill unexpectedly exists on disk")
        if any(event["payload"]["origin"] != "foreground" for event in events):
            fails.append("Leg C: a foreground event has the wrong origin")
        if not fails:
            _log(
                "Leg C: create and delete survived with no final artifact on disk"
            )
    finally:
        ob.close()
    return fails


def _write_foreground_state_db(home: Path, content: str) -> None:
    db = sqlite3.connect(home / "state.db")
    db.executescript(
        """
        CREATE TABLE sessions (
            id TEXT, source TEXT, parent_session_id TEXT, model TEXT,
            message_count INT, tool_call_count INT, input_tokens INT,
            output_tokens INT, estimated_cost_usd REAL, started_at REAL,
            ended_at REAL, end_reason TEXT, profile_name TEXT,
            expiry_finalized INT
        );
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL, role TEXT NOT NULL, content TEXT,
            tool_call_id TEXT, tool_calls TEXT, tool_name TEXT,
            effect_disposition TEXT, timestamp REAL NOT NULL,
            token_count INTEGER, finish_reason TEXT, reasoning TEXT,
            reasoning_content TEXT, reasoning_details TEXT,
            codex_reasoning_items TEXT, codex_message_items TEXT,
            platform_message_id TEXT, observed INTEGER DEFAULT 0,
            active INTEGER NOT NULL DEFAULT 1,
            compacted INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE session_model_usage (
            session_id TEXT, model TEXT, task TEXT, api_call_count INT,
            input_tokens INT, output_tokens INT, cache_read_tokens INT,
            reasoning_tokens INT, estimated_cost_usd REAL, cost_status TEXT,
            last_seen REAL
        );
        CREATE TABLE async_delegations (
            delegation_id TEXT, origin_session TEXT, parent_session_id TEXT,
            state TEXT, delivery_state TEXT, owner_pid INT, dispatched_at REAL,
            event_json TEXT, result_json TEXT
        );
        """
    )
    db.execute(
        "INSERT INTO sessions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("S", "cli", None, "m", 4, 2, 0, 0, 0.0, 1000.0, None, None,
         "default", 0),
    )
    calls = [
        (
            "call-create",
            {
                "action": "create",
                "name": "transient-probe",
                "category": "live",
                "content": content,
            },
            {"success": True, "category": "live"},
        ),
        (
            "call-delete",
            {
                "action": "delete",
                "name": "transient-probe",
                "absorbed_into": "",
            },
            {"success": True},
        ),
    ]
    for index, (call_id, arguments, result) in enumerate(calls):
        assistant_id = index * 2 + 1
        tool_id = assistant_id + 1
        tool_calls = json.dumps(
            [
                {
                    "id": call_id,
                    "type": "function",
                    "function": {
                        "name": "skill_manage",
                        "arguments": json.dumps(arguments),
                    },
                }
            ]
        )
        db.execute(
            "INSERT INTO messages("
            "id,session_id,role,content,tool_calls,timestamp,finish_reason"
            ") VALUES (?,?,?,?,?,?,?)",
            (assistant_id, "S", "assistant", "", tool_calls,
             1001.0 + assistant_id, "tool_calls"),
        )
        db.execute(
            "INSERT INTO messages("
            "id,session_id,role,content,tool_call_id,tool_name,timestamp"
            ") VALUES (?,?,?,?,?,?,?)",
            (tool_id, "S", "tool", json.dumps(result), call_id,
             "skill_manage", 1001.0 + tool_id),
        )
    db.commit()
    db.close()


LEGS = [
    ("Leg A — live home, read-only, no false finding, no backfill", leg_a_live_readonly),
    ("Leg B — disposable home create → capture → restore → delete", leg_b_roundtrip),
    (
        "Leg C — foreground create → delete with no final artifact",
        leg_c_foreground_lifecycle,
    ),
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
