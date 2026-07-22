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

import time
from pathlib import Path
from typing import Any, Iterator

from ._common import (
    SKILL_SUBDIRS,
    hermes_created_skills,
    memory_files,
    resolve_hermes_home,
)

# Non-envelope counter key returned by ``poll`` — the number of artifact
# versions newly recorded this pass (including tombstones). It is operational
# feedback for the run summary, not an event type.
VERSIONS_RECORDED = "knowledge:versions_recorded"


def poll(
    outbox: Any,
    hermes_home: str | Path | None = None,
    *,
    knowledge_config: Any = None,
) -> dict[str, int]:
    """One read-only scan of the knowledge surface. Returns per-key counts."""
    from .recorder_config import KnowledgeConfig

    config = knowledge_config or KnowledgeConfig()
    home = resolve_hermes_home(hermes_home)

    recorded = 0
    seen: set[str] = set()
    for artifact_id, kind, name, category, files in _iter_artifacts(home):
        seen.add(artifact_id)
        if _capture(outbox, config, artifact_id, kind, name, category, files):
            recorded += 1
    recorded += _tombstone_vanished(outbox, config, seen)

    return {VERSIONS_RECORDED: recorded} if recorded else {}


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
