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
import heapq
import json
import logging
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from ._common import (
    SKILL_SUBDIRS,
    build_record,
    hermes_created_skills,
    memory_files,
    read_home_mode,
    resolve_hermes_home,
    runtime_stamp,
)
from .keystore import has_secret

KNOWLEDGE_EVENT = "knowledge.record_written"
KNOWLEDGE_BUNDLE_FORMAT = "knowledge.bundle.v1"
_LOG = logging.getLogger("hermes_flight_recorder.serve.knowledge")


def poll(
    outbox: Any,
    hermes_home: str | Path | None = None,
    *,
    knowledge_config: Any = None,
    on_artifact_error: Callable[[str, Exception], None] | None = None,
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
    runtime = runtime_stamp("knowledge", home_mode=home_mode)

    seen: set[str] = set()
    emitted = 0
    for artifact_id, kind, name, category, files in _iter_artifacts(home, config):
        # Mark seen BEFORE the read so a transient I/O error on one artifact does
        # not make it look deleted (which would record a spurious tombstone).
        seen.add(artifact_id)
        try:
            created = _capture(
                outbox,
                config,
                artifact_id,
                kind,
                name,
                category,
                files,
                runtime=runtime,
            )
            emitted += int(created)
        except OSError as exc:
            # A live file can vanish or become unreadable between listing and
            # reading (TOCTOU), or hit a permission error. Isolate it: one bad
            # artifact must not sink the rest of the pass. The next tick re-scans.
            if on_artifact_error is not None:
                on_artifact_error(artifact_id, exc)
            continue
    _tombstone_vanished(outbox, config, seen)

    emitted += _emit_pending_events(outbox, home_mode, config)
    return {KNOWLEDGE_EVENT: emitted} if emitted else {}


def iter_disk_artifacts(
    home: Path,
    knowledge_config: Any = None,
) -> Iterator[tuple[str, str, str, str | None, list[tuple[str, Path]]]]:
    """Public read-only view of the tracked artifacts on disk.

    The reconciler (#79) walks the same surface as the scanner to diff disk
    against the store, so both apply the identical Hermes-created filter and can
    never disagree on what is tracked.
    """
    from .recorder_config import KnowledgeConfig

    return _iter_artifacts(home, knowledge_config or KnowledgeConfig())


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
    outbox: Any, files: list[tuple[str, Path]], knowledge_config: Any = None
) -> tuple[list[dict[str, str]], float]:
    """The manifest and newest mtime for a file set, computed read-only.

    Hashes each file's plaintext exactly as ``put_blob`` would, but stores
    nothing — so the reconciler can compute an artifact's on-disk manifest hash
    and compare it to the store's latest version without writing a blob. The
    result is byte-identical to what ``_capture`` would produce for the same
    files, so a drift verdict is exact, not approximate.
    """
    from .recorder_config import KnowledgeConfig

    manifest, _plaintext, occurred_at, _skipped = _snapshot_files(
        outbox,
        knowledge_config or KnowledgeConfig(),
        "reconcile",
        files,
        store=False,
        log_skips=False,
    )
    return manifest, occurred_at


def read_snapshot(
    outbox: Any, files: list[tuple[str, Path]], knowledge_config: Any
) -> tuple[list[dict[str, str]], float, list[dict[str, Any]]]:
    """Return the limited disk manifest, newest mtime, and omissions."""
    manifest, _plaintext, occurred_at, skipped = _snapshot_files(
        outbox,
        knowledge_config,
        "reconcile",
        files,
        store=False,
        log_skips=False,
    )
    return manifest, occurred_at, skipped


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
    runtime = runtime_stamp("knowledge", home_mode=home_mode)
    return _capture(
        outbox,
        config,
        artifact_id,
        kind,
        name,
        category,
        files,
        runtime=runtime,
    )


def _iter_artifacts(
    home: Path,
    config: Any,
) -> Iterator[tuple[str, str, str, str | None, list[tuple[str, Path]]]]:
    """Yield ``(artifact_id, kind, name, category, files)`` for each artifact.

    ``files`` is a list of ``(relative_path, absolute_path)``.
    """
    memory_root = home / "memories"
    for target, path in memory_files(home):
        if not _safe_regular_file(path, memory_root):
            continue
        kind = "user_profile" if target == "user" else "memory"
        yield f"memory:{target}", kind, target, None, [(path.name, path)]
    skills_root = home / "skills"
    for name, category, skill_dir in hermes_created_skills(home):
        if not _safe_directory(skill_dir, skills_root):
            continue
        artifact_id = f"skill:{category}/{name}" if category else f"skill:{name}"
        yield artifact_id, "skill", name, category, _skill_files(skill_dir, config)


def _skill_files(skill_dir: Path, config: Any) -> list[tuple[str, Path]]:
    """A skill's files: ``SKILL.md`` plus the four supporting subdirectories."""
    files: list[tuple[str, Path]] = []
    # One more candidate makes the file-count omission visible. The scan stops
    # there, so a directory with an extreme entry count cannot fill memory.
    scan_limit = config.max_file_count + 1
    skill_md = skill_dir / "SKILL.md"
    if _safe_regular_file(skill_md, skill_dir):
        files.append(("SKILL.md", skill_md))
    for sub in SKILL_SUBDIRS:
        directory = skill_dir / sub
        if _safe_directory(directory, skill_dir):
            remaining = scan_limit - len(files)
            if remaining <= 0:
                return files
            candidates = (
                path
                for path in directory.rglob("*")
                if _safe_regular_file(path, skill_dir)
                and _safe_bundle_path(path.relative_to(skill_dir).as_posix())
            )
            selected = heapq.nsmallest(
                remaining,
                candidates,
                key=lambda path: path.relative_to(skill_dir).as_posix(),
            )
            for path in selected:
                files.append((path.relative_to(skill_dir).as_posix(), path))
                if len(files) >= scan_limit:
                    return files
    return files


def _skip_metadata(
    artifact_id: str,
    rel_path: str,
    reason: str,
    byte_count: int,
    limit: int,
    *,
    log_skip: bool,
) -> dict[str, Any]:
    item = {
        "path": rel_path,
        "reason": reason,
        "byte_count": byte_count,
        "limit": limit,
    }
    if log_skip:
        _LOG.warning(
            "knowledge file skipped artifact=%s path=%s reason=%s "
            "byte_count=%d limit=%d",
            artifact_id,
            rel_path,
            reason,
            byte_count,
            limit,
        )
    return item


def _snapshot_files(
    outbox: Any,
    config: Any,
    artifact_id: str,
    files: list[tuple[str, Path]],
    *,
    store: bool,
    log_skips: bool,
) -> tuple[list[dict[str, str]], dict[str, bytes], float, list[dict[str, Any]]]:
    """Read one artifact within its file, count, and byte limits."""
    manifest: list[dict[str, str]] = []
    plaintext_files: dict[str, bytes] = {}
    skipped: list[dict[str, Any]] = []
    occurred_at = 0.0
    captured_bytes = 0
    for file_index, (rel_path, path) in enumerate(files):
        stat = path.stat()
        byte_count = int(stat.st_size)
        occurred_at = max(occurred_at, stat.st_mtime)
        if file_index >= config.max_file_count:
            skipped.append(
                _skip_metadata(
                    artifact_id,
                    rel_path,
                    "max_file_count",
                    byte_count,
                    config.max_file_count,
                    log_skip=log_skips,
                )
            )
            continue
        if byte_count > config.max_file_bytes:
            skipped.append(
                _skip_metadata(
                    artifact_id,
                    rel_path,
                    "max_file_bytes",
                    byte_count,
                    config.max_file_bytes,
                    log_skip=log_skips,
                )
            )
            continue
        remaining = config.max_artifact_bytes - captured_bytes
        if byte_count > remaining:
            skipped.append(
                _skip_metadata(
                    artifact_id,
                    rel_path,
                    "max_artifact_bytes",
                    byte_count,
                    config.max_artifact_bytes,
                    log_skip=log_skips,
                )
            )
            continue

        # The size can change after stat(). Read at most one byte above the
        # active limit, so a concurrent append cannot create an unbounded read.
        read_limit = min(config.max_file_bytes, remaining)
        with path.open("rb") as handle:
            raw = handle.read(read_limit + 1)
        if len(raw) > config.max_file_bytes:
            skipped.append(
                _skip_metadata(
                    artifact_id,
                    rel_path,
                    "max_file_bytes",
                    len(raw),
                    config.max_file_bytes,
                    log_skip=log_skips,
                )
            )
            continue
        if len(raw) > remaining:
            skipped.append(
                _skip_metadata(
                    artifact_id,
                    rel_path,
                    "max_artifact_bytes",
                    len(raw),
                    config.max_artifact_bytes,
                    log_skip=log_skips,
                )
            )
            continue
        digest = outbox.put_blob(raw) if store else outbox._content_hash(raw)
        manifest.append({"path": rel_path, "blob_hash": digest})
        plaintext_files[rel_path] = raw
        captured_bytes += len(raw)

    manifest.sort(key=lambda entry: entry["path"])
    skipped.sort(key=lambda entry: entry["path"])
    return manifest, plaintext_files, occurred_at, skipped


def limit_plaintext_files(
    config: Any,
    artifact_id: str,
    files: dict[str, bytes],
) -> tuple[dict[str, bytes], list[dict[str, Any]]]:
    """Apply the knowledge limits to an in-memory foreground after-state."""
    selected: dict[str, bytes] = {}
    skipped: list[dict[str, Any]] = []
    captured_bytes = 0
    paths = heapq.nsmallest(config.max_file_count + 1, files)
    for file_index, path in enumerate(paths):
        content = files[path]
        byte_count = len(content)
        if file_index >= config.max_file_count:
            skipped.append(
                _skip_metadata(
                    artifact_id,
                    path,
                    "max_file_count",
                    byte_count,
                    config.max_file_count,
                    log_skip=False,
                )
            )
            continue
        if byte_count > config.max_file_bytes:
            skipped.append(
                _skip_metadata(
                    artifact_id,
                    path,
                    "max_file_bytes",
                    byte_count,
                    config.max_file_bytes,
                    log_skip=False,
                )
            )
            continue
        if captured_bytes + byte_count > config.max_artifact_bytes:
            skipped.append(
                _skip_metadata(
                    artifact_id,
                    path,
                    "max_artifact_bytes",
                    byte_count,
                    config.max_artifact_bytes,
                    log_skip=False,
                )
            )
            continue
        selected[path] = content
        captured_bytes += byte_count
    return selected, skipped


def log_skipped_files(artifact_id: str, skipped_files: list[dict[str, Any]]) -> None:
    """Write one warning for each new knowledge omission."""
    for item in skipped_files:
        _skip_metadata(
            artifact_id,
            item["path"],
            item["reason"],
            item["byte_count"],
            item["limit"],
            log_skip=True,
        )


def _safe_directory(path: Path, root: Path) -> bool:
    """Return true for a real directory below ``root`` with no link components."""
    return _safe_path(path, root) and path.is_dir()


def _safe_regular_file(path: Path, root: Path) -> bool:
    """Return true for a real file below ``root`` with no link components."""
    return _safe_path(path, root) and path.is_file()


def _safe_path(path: Path, root: Path) -> bool:
    """Reject links and paths that resolve outside an artifact root."""
    try:
        relative = path.relative_to(root)
        resolved_root = root.resolve(strict=True)
        resolved_path = path.resolve(strict=True)
        resolved_path.relative_to(resolved_root)
    except (OSError, ValueError):
        return False

    current = root
    if current.is_symlink():
        return False
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            return False
    return True


def _safe_bundle_path(path: str) -> bool:
    """Whether a producer path is safe for ``knowledge.bundle.v1``.

    The supported skill tree is local and read-only, but filenames can still
    contain backslashes or control characters on POSIX. Exclude those rather
    than emitting a bundle the browser must reject as unsafe.
    """
    if not path or len(path) > 512 or path.startswith("/") or "\\" in path:
        return False
    if any(ord(char) < 32 or ord(char) == 127 for char in path):
        return False
    return all(part not in {"", ".", ".."} for part in path.split("/"))


def _capture(
    outbox: Any,
    config: Any,
    artifact_id: str,
    kind: str,
    name: str,
    category: str | None,
    files: list[tuple[str, Path]],
    *,
    runtime: dict[str, Any] | None = None,
) -> bool:
    """Record a new version of one artifact if its content changed."""
    manifest, plaintext_files, occurred_at, skipped_files = _snapshot_files(
        outbox,
        config,
        artifact_id,
        files,
        store=True,
        log_skips=False,
    )
    if not manifest and not skipped_files:
        return False

    outbox.upsert_knowledge_artifact(
        artifact_id,
        kind=kind,
        name=name,
        category=category,
        provenance="agent",
        first_seen=occurred_at,
    )
    seq, created = outbox.append_knowledge_version(
        artifact_id,
        manifest=manifest,
        occurred_at=occurred_at,
        origin="background",
        skipped_files=skipped_files,
    )
    if created:
        log_skipped_files(artifact_id, skipped_files)
        _apply_retention(outbox, config, artifact_id)
        if runtime is not None:
            artifact = outbox.knowledge_artifact(artifact_id)
            version = next(
                item
                for item in outbox.knowledge_versions(artifact_id)
                if item["seq"] == seq
            )
            _emit_version_event(
                outbox,
                runtime,
                artifact,
                version,
                plaintext_files=plaintext_files,
                config=config,
            )
            outbox.set_meta(f"knowledge:emitted:{artifact_id}", str(seq))
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


def _emit_pending_events(outbox: Any, home_mode: str, config: Any = None) -> int:
    """Emit a ``knowledge.record_written`` for every not-yet-shipped version.

    A per-artifact meta cursor tracks the highest version already turned into an
    event, so this is idempotent across restarts and backfills versions that
    predate the transport. Content is reconstructed from the deduped blobs, so
    each event carries the complete after-state of its version — foreground and
    background writes alike.
    """
    from .recorder_config import KnowledgeConfig

    config = config or KnowledgeConfig()
    runtime = runtime_stamp("knowledge", home_mode=home_mode)
    emitted = 0
    for artifact_id in outbox.knowledge_artifact_ids():
        emitted += _emit_artifact_events(outbox, runtime, artifact_id, config)
    return emitted


def _emit_artifact_events(
    outbox: Any, runtime: dict[str, Any], artifact_id: str, config: Any
) -> int:
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
        if not version["is_tombstone"] and not has_secret(
            outbox._flight_recorder_home
        ):
            # A fleet agent cannot reconstruct an old bundle from encrypted
            # blobs. New versions emit from the plaintext captured in the same
            # pass and advance this cursor before they reach this fallback.
            continue
        if _emit_version_event(outbox, runtime, artifact, version, config=config):
            emitted += 1
        outbox.set_meta(cursor_key, str(version["seq"]))
    return emitted


def _emit_version_event(
    outbox: Any,
    runtime: dict[str, Any],
    artifact: dict[str, Any],
    version: dict[str, Any],
    *,
    plaintext_files: dict[str, bytes] | None = None,
    config: Any,
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
        skipped_files = list(version.get("skipped_files", []))
        for entry in version["manifest"]:
            raw_size = (
                len(plaintext_files[entry["path"]])
                if plaintext_files is not None
                else outbox.knowledge_blob_size(entry["blob_hash"])
            )
            if len(files) >= config.max_file_count:
                skipped_files.append(
                    _skip_metadata(
                        artifact["artifact_id"],
                        entry["path"],
                        "max_file_count",
                        raw_size,
                        config.max_file_count,
                        log_skip=True,
                    )
                )
                continue
            if raw_size > config.max_file_bytes:
                skipped_files.append(
                    _skip_metadata(
                        artifact["artifact_id"],
                        entry["path"],
                        "max_file_bytes",
                        raw_size,
                        config.max_file_bytes,
                        log_skip=True,
                    )
                )
                continue
            if byte_count + raw_size > config.max_artifact_bytes:
                skipped_files.append(
                    _skip_metadata(
                        artifact["artifact_id"],
                        entry["path"],
                        "max_artifact_bytes",
                        raw_size,
                        config.max_artifact_bytes,
                        log_skip=True,
                    )
                )
                continue
            if plaintext_files is None:
                raw = outbox.get_blob(entry["blob_hash"])
            else:
                raw = plaintext_files[entry["path"]]
            byte_count += len(raw)
            files.append(
                {
                    "path": entry["path"],
                    "byte_count": len(raw),
                    "content_hash": entry["blob_hash"],
                    "content_b64": base64.b64encode(raw).decode("ascii"),
                }
            )
        file_count = len(files)
        bundle = {
            "format": KNOWLEDGE_BUNDLE_FORMAT,
            "artifact_id": artifact["artifact_id"],
            "version_seq": seq,
            "manifest_hash": version["manifest_hash"],
            "files": files,
        }
        if skipped_files:
            bundle["skipped_files"] = skipped_files
        content = json.dumps(bundle, separators=(",", ":"))

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
    if not is_tombstone:
        payload["content_format"] = KNOWLEDGE_BUNDLE_FORMAT
        payload["skipped_file_count"] = len(skipped_files)
        if skipped_files:
            payload["skipped_files"] = skipped_files
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
        partial=bool(not is_tombstone and skipped_files),
    )
    return outbox.append_if_new(
        record,
        content=content,
        dedup_key=f"knowledge:{artifact['artifact_id']}:v{seq}",
    )
