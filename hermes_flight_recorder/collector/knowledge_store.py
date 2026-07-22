"""Content-addressed store for Hermes-created skills and built-in memories.

Phase 3, issue #78 — the *artifact* half of knowledge capture. Where the
state.db classifier (#77) records the foreground mutation *event*, this scanner
records the mutated *content*: it reads the filesystem read-only and writes a new
version of each tracked artifact whenever its content changes. Because it scans
the filesystem, it captures both foreground writes and background-curator writes
(the self-improvement review runs persist-disabled and never touches state.db),
so the store — not the event stream — is the source of truth for knowledge
content.

Grounded in the frozen contract (``docs/schema/envelope-v1.md``, issue #76) and
confirmed against Hermes source:

- Tracked artifacts are the two built-in memory files
  (``<home>/memories/MEMORY.md`` and ``USER.md``) and every **Hermes-created**
  skill under ``<home>/skills/`` — one absent from both ``.bundled_manifest`` and
  ``.hub/lock.json``. Bundled and Hub-installed skills are never ingested.
- A version is a manifest of ``{path, blob_hash}`` over the artifact's files
  (``SKILL.md`` plus the four supporting subdirectories). Unchanged files reuse
  their blob, so editing one file adds one blob, not a whole copy.
- A tracked artifact that vanishes from disk records a terminal (tombstone)
  version rather than erasing its history.

The scanner sets ``origin='background'`` for every version it detects: it sees
only the file state, not the writer. Foreground attribution (an ``origin`` of
``foreground`` with a ``linked_event_id``) is applied by the classifier (#77) and
the reconciler (#79). This adapter never writes to a Hermes home.
"""

from __future__ import annotations

import base64
import json
import time
from pathlib import Path
from typing import Any, Iterator

from ._common import (
    SKILL_SUBDIRS,
    build_record,
    hermes_created_skills,
    memory_files,
    read_home_mode,
    resolve_hermes_home,
    runtime_stamp,
)

KNOWLEDGE_EVENT = "knowledge.record_written"


def poll(
    outbox: Any,
    hermes_home: str | Path | None = None,
    *,
    knowledge_config: Any = None,
) -> dict[str, int]:
    """One read-only scan of the knowledge surface.

    Two steps: record any content change into the content-addressed store, then
    emit a ``knowledge.record_written`` event for every store version that does
    not yet have one (so the encrypted server ledger receives both foreground and
    background writes, and any versions that predate this transport are
    backfilled). Returns per-event-type counts.
    """
    from .recorder_config import KnowledgeConfig

    config = knowledge_config or KnowledgeConfig()
    home = resolve_hermes_home(hermes_home)
    home_mode = read_home_mode(hermes_home)

    seen: set[str] = set()
    for artifact_id, kind, name, category, files in _iter_artifacts(home):
        # Mark seen BEFORE the read so a transient I/O error on one artifact does
        # not make it look deleted (which would record a spurious tombstone).
        seen.add(artifact_id)
        try:
            _capture(outbox, config, artifact_id, kind, name, category, files)
        except OSError:
            # A live file can vanish or become unreadable between listing and
            # reading (TOCTOU), or hit a permission error. Isolate it: one bad
            # artifact must not sink the rest of the pass. The next tick re-scans.
            continue
    _tombstone_vanished(outbox, config, seen)

    emitted = _emit_pending_events(outbox, home_mode)
    return {KNOWLEDGE_EVENT: emitted} if emitted else {}


def iter_disk_artifacts(
    home: Path,
) -> Iterator[tuple[str, str, str, str | None, list[tuple[str, Path]]]]:
    """Public read-only view of the tracked artifacts on disk.

    The reconciler (#79) walks the same surface as the scanner to diff disk
    against the store, so both apply the identical Hermes-created filter and can
    never disagree on what is tracked.
    """
    return _iter_artifacts(home)


def restore_version(
    outbox: Any, artifact_id: str, seq: int | None = None
) -> dict[str, bytes] | None:
    """Reconstruct an artifact version's files from the store.

    Returns ``{relative_path: content_bytes}`` by decrypting each blob the
    version's manifest references, so any single version restores byte-for-byte
    from the content-addressed store alone. ``seq=None`` restores the latest.
    A tombstone restores to ``{}`` (the artifact was deleted at that version).
    Returns ``None`` when the artifact or the requested version is unknown.
    """
    versions = outbox.knowledge_versions(artifact_id)
    if not versions:
        return None
    if seq is None:
        version = versions[-1]
    else:
        version = next((v for v in versions if v["seq"] == seq), None)
        if version is None:
            return None
    return {entry["path"]: outbox.get_blob(entry["blob_hash"]) for entry in version["manifest"]}


def read_manifest(
    outbox: Any, files: list[tuple[str, Path]]
) -> tuple[list[dict[str, str]], float]:
    """The manifest and newest mtime for a file set, computed read-only.

    Hashes each file's plaintext exactly as ``put_blob`` would, but stores
    nothing — so the reconciler can compute an artifact's on-disk manifest hash
    and compare it to the store's latest version without writing a blob. The
    result is byte-identical to what ``_capture`` would produce for the same
    files, so a drift verdict is exact, not approximate.
    """
    manifest: list[dict[str, str]] = []
    occurred_at = 0.0
    for rel_path, path in files:
        manifest.append({"path": rel_path, "blob_hash": outbox._content_hash(path.read_bytes())})
        occurred_at = max(occurred_at, path.stat().st_mtime)
    manifest.sort(key=lambda entry: entry["path"])
    return manifest, occurred_at


def heal_artifact(
    outbox: Any,
    config: Any,
    home_mode: str,
    artifact_id: str,
    kind: str,
    name: str,
    category: str | None,
    files: list[tuple[str, Path]],
) -> bool:
    """Capture a version the scanner missed, and emit its event.

    The reconciler's drift repair (#79): when disk has drifted from the store,
    record the missed version through the same ``_capture`` path the scanner
    uses, then emit its ``knowledge.record_written`` so the healed version does
    not immediately read as an un-emitted store→event gap. Returns whether a new
    version landed.
    """
    created = _capture(outbox, config, artifact_id, kind, name, category, files)
    if created:
        runtime = runtime_stamp("knowledge", home_mode=home_mode)
        _emit_artifact_events(outbox, runtime, artifact_id)
    return created


def _iter_artifacts(
    home: Path,
) -> Iterator[tuple[str, str, str, str | None, list[tuple[str, Path]]]]:
    """Yield ``(artifact_id, kind, name, category, files)`` for each artifact.

    ``files`` is a list of ``(relative_path, absolute_path)``.
    """
    for target, path in memory_files(home):
        kind = "user_profile" if target == "user" else "memory"
        yield f"memory:{target}", kind, target, None, [(path.name, path)]
    for name, category, skill_dir in hermes_created_skills(home):
        artifact_id = f"skill:{category}/{name}" if category else f"skill:{name}"
        yield artifact_id, "skill", name, category, _skill_files(skill_dir)


def _skill_files(skill_dir: Path) -> list[tuple[str, Path]]:
    """A skill's files: ``SKILL.md`` plus the four supporting subdirectories."""
    files: list[tuple[str, Path]] = []
    skill_md = skill_dir / "SKILL.md"
    if skill_md.is_file():
        files.append(("SKILL.md", skill_md))
    for sub in SKILL_SUBDIRS:
        directory = skill_dir / sub
        if directory.is_dir():
            for path in sorted(directory.rglob("*")):
                if path.is_file():
                    files.append((str(path.relative_to(skill_dir)), path))
    return files


def _capture(
    outbox: Any,
    config: Any,
    artifact_id: str,
    kind: str,
    name: str,
    category: str | None,
    files: list[tuple[str, Path]],
) -> bool:
    """Record a new version of one artifact if its content changed."""
    manifest: list[dict[str, str]] = []
    occurred_at = 0.0
    for rel_path, path in files:
        manifest.append({"path": rel_path, "blob_hash": outbox.put_blob(path.read_bytes())})
        occurred_at = max(occurred_at, path.stat().st_mtime)
    if not manifest:
        return False
    manifest.sort(key=lambda entry: entry["path"])

    outbox.upsert_knowledge_artifact(
        artifact_id,
        kind=kind,
        name=name,
        category=category,
        provenance="agent",
        first_seen=occurred_at,
    )
    _seq, created = outbox.append_knowledge_version(
        artifact_id, manifest=manifest, occurred_at=occurred_at, origin="background"
    )
    if created:
        _apply_retention(outbox, config, artifact_id)
    return created


def _tombstone_vanished(outbox: Any, config: Any, seen: set[str]) -> int:
    """Record a tombstone for each tracked artifact now absent from disk."""
    recorded = 0
    for artifact_id in outbox.knowledge_artifact_ids():
        if artifact_id in seen:
            continue
        latest = outbox.latest_knowledge_version(artifact_id)
        if latest is None or latest["is_tombstone"]:
            continue
        _seq, created = outbox.append_knowledge_version(
            artifact_id,
            manifest=[],
            occurred_at=time.time(),
            origin="background",
            is_tombstone=True,
        )
        if created:
            _apply_retention(outbox, config, artifact_id)
            recorded += 1
    return recorded


def _apply_retention(outbox: Any, config: Any, artifact_id: str) -> None:
    """Enforce the store's own retention for one artifact.

    ``latest_only`` keeps a single version; ``full`` with ``max_versions`` set
    keeps that many; ``full`` with no cap keeps the whole chain. The latest
    version is always kept — pruning knowledge never drops current state.
    """
    if config.history == "latest_only":
        keep: int | None = 1
    elif config.max_versions is not None:
        keep = config.max_versions
    else:
        return
    if outbox.prune_knowledge_versions(artifact_id, keep=keep):
        outbox.gc_orphan_blobs()


def _emit_pending_events(outbox: Any, home_mode: str) -> int:
    """Emit a ``knowledge.record_written`` for every not-yet-shipped version.

    A per-artifact meta cursor tracks the highest version already turned into an
    event, so this is idempotent across restarts and backfills versions that
    predate the transport. Content is reconstructed from the deduped blobs, so
    each event carries the complete after-state of its version — foreground and
    background writes alike.
    """
    runtime = runtime_stamp("knowledge", home_mode=home_mode)
    emitted = 0
    for artifact_id in outbox.knowledge_artifact_ids():
        emitted += _emit_artifact_events(outbox, runtime, artifact_id)
    return emitted


def _emit_artifact_events(outbox: Any, runtime: dict[str, Any], artifact_id: str) -> int:
    """Emit every not-yet-shipped version of one artifact; return the count.

    Drives the per-artifact ``knowledge:emitted:<id>`` cursor so the emit is
    idempotent across restarts and shared by both the scanner's bulk pass and
    the reconciler's targeted heal.
    """
    artifact = outbox.knowledge_artifact(artifact_id)
    if artifact is None:
        return 0
    cursor_key = f"knowledge:emitted:{artifact_id}"
    last_emitted = int(outbox.get_meta(cursor_key) or 0)
    emitted = 0
    for version in outbox.knowledge_versions(artifact_id):
        if version["seq"] <= last_emitted:
            continue
        if _emit_version_event(outbox, runtime, artifact, version):
            emitted += 1
        outbox.set_meta(cursor_key, str(version["seq"]))
    return emitted


def _emit_version_event(
    outbox: Any, runtime: dict[str, Any], artifact: dict[str, Any], version: dict[str, Any]
) -> bool:
    """Build and append one knowledge event; return whether a new row landed."""
    kind = artifact["kind"]
    seq = version["seq"]
    is_tombstone = version["is_tombstone"]
    action = "delete" if is_tombstone else ("create" if seq == 1 else "update")

    if is_tombstone:
        content: str | None = None
        file_count = 0
        byte_count = 0
    else:
        files = []
        byte_count = 0
        for entry in version["manifest"]:
            raw = outbox.get_blob(entry["blob_hash"])
            byte_count += len(raw)
            files.append(
                {"path": entry["path"], "content_b64": base64.b64encode(raw).decode("ascii")}
            )
        file_count = len(files)
        content = json.dumps(
            {"manifest_hash": version["manifest_hash"], "files": files}
        )

    payload: dict[str, Any] = {
        "artifact_kind": kind,
        "action": action,
        "artifact_id": artifact["artifact_id"],
        "version_seq": seq,
        "manifest_hash": version["manifest_hash"],
        "content_hash": version["manifest_hash"],
        "origin": version["origin"],
        "provenance": artifact["provenance"],
        "file_count": file_count,
        "byte_count": byte_count,
    }
    if kind == "skill":
        payload["skill_name"] = artifact["name"]
        if artifact["category"]:
            payload["category"] = artifact["category"]
    else:  # memory / user_profile
        payload["target"] = artifact["name"]

    record = build_record(
        event_type=KNOWLEDGE_EVENT,
        occurred_at=version["occurred_at"],
        source="knowledge_store",
        capture_method="scan:knowledge_store",
        runtime=runtime,
        correlation_id=f"knowledge:{artifact['artifact_id']}",
        payload=payload,
        partial=False,
    )
    return outbox.append_if_new(
        record,
        content=content,
        dedup_key=f"knowledge:{artifact['artifact_id']}:v{seq}",
    )
