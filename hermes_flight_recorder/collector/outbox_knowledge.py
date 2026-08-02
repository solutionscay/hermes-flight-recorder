"""Encrypted content-addressed knowledge repository for the outbox."""

from __future__ import annotations

import base64
import hashlib
import json
from typing import Any

from . import content_crypto as cc
from .outbox_errors import OutboxError


class KnowledgeOutboxMixin:
    # --- knowledge store ------------------------------------------------
    # A content-addressed store for Hermes-created skills and built-in
    # memories (Phase 3). It shares the outbox's content encryption and
    # connection but its own tables, so event retention never touches
    # knowledge state and knowledge retention never touches events.
    @staticmethod
    def _content_hash(raw: bytes) -> str:
        return "sha256:" + hashlib.sha256(raw).hexdigest()

    @staticmethod
    def _manifest_hash(manifest: list[dict[str, str]]) -> str:
        canonical = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
        return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def put_blob(self, content: str | bytes) -> str:
        """Store one file's content, encrypted, deduplicated by plaintext hash.

        Returns the content hash. Identical plaintext is stored once, so a
        multi-file skill that changes one file adds a single new blob.
        """
        raw = content.encode("utf-8") if isinstance(content, str) else content
        digest = self._content_hash(raw)
        if (
            self._conn.execute(
                "SELECT 1 FROM knowledge_blob WHERE content_hash=?", (digest,)
            ).fetchone()
            is not None
        ):
            return digest
        fields = self._encrypt_content(raw)
        self._conn.execute(
            "INSERT OR IGNORE INTO knowledge_blob("
            "content_hash, content_ciphertext, content_nonce, key_version, byte_len) "
            "VALUES(?,?,?,?,?)",
            (
                digest,
                fields["content_ciphertext"],
                fields["content_nonce"],
                fields["key_version"],
                len(raw),
            ),
        )
        return digest

    def get_blob(
        self, content_hash: str, keypair: cc.OperatorKeyPair | None = None
    ) -> bytes:
        """Decrypt and return a stored blob with the operator private key.

        For restore and tests on a solo or operator machine. Pass ``keypair``
        to decrypt with an off-host key; omit it to use the solo keystore.
        """
        row = self._conn.execute(
            "SELECT content_ciphertext, content_nonce, key_version FROM knowledge_blob "
            "WHERE content_hash=?",
            (content_hash,),
        ).fetchone()
        if row is None:
            raise OutboxError(f"no knowledge blob for {content_hash}")
        dek = self._dek_for_version(row[2], self._resolve_keypair(keypair))
        return cc.decrypt_content(
            dek, base64.b64decode(row[0]), base64.b64decode(row[1])
        )

    def knowledge_blob_size(self, content_hash: str) -> int:
        """Return a stored blob's plaintext byte length without decrypting it."""
        row = self._conn.execute(
            "SELECT byte_len FROM knowledge_blob WHERE content_hash=?",
            (content_hash,),
        ).fetchone()
        if row is None:
            raise OutboxError(f"no knowledge blob for {content_hash}")
        return int(row[0])

    def upsert_knowledge_artifact(
        self,
        artifact_id: str,
        *,
        kind: str,
        name: str,
        category: str | None,
        provenance: str,
        first_seen: float,
    ) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO knowledge_artifact("
            "artifact_id, kind, name, category, provenance, first_seen) "
            "VALUES(?,?,?,?,?,?)",
            (artifact_id, kind, name, category, provenance, float(first_seen)),
        )

    def knowledge_artifact_ids(self) -> list[str]:
        return [
            row[0]
            for row in self._conn.execute(
                "SELECT artifact_id FROM knowledge_artifact ORDER BY artifact_id"
            )
        ]

    def knowledge_artifact(self, artifact_id: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT artifact_id, kind, name, category, provenance, first_seen "
            "FROM knowledge_artifact WHERE artifact_id=?",
            (artifact_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "artifact_id": row[0],
            "kind": row[1],
            "name": row[2],
            "category": row[3],
            "provenance": row[4],
            "first_seen": row[5],
        }

    @staticmethod
    def _version_row(row: Any) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "artifact_id": row[0],
            "seq": row[1],
            "manifest": json.loads(row[2]),
            "manifest_hash": row[3],
            "occurred_at": row[4],
            "origin": row[5],
            "linked_event_id": row[6],
            "is_tombstone": bool(row[7]),
            "skipped_files": json.loads(row[8]),
        }

    _VERSION_COLUMNS = (
        "artifact_id, seq, manifest_json, manifest_hash, occurred_at, origin, "
        "linked_event_id, is_tombstone, skipped_json"
    )

    def latest_knowledge_version(self, artifact_id: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            f"SELECT {self._VERSION_COLUMNS} FROM knowledge_version "
            "WHERE artifact_id=? ORDER BY seq DESC LIMIT 1",
            (artifact_id,),
        ).fetchone()
        return self._version_row(row)

    def knowledge_versions(self, artifact_id: str) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            f"SELECT {self._VERSION_COLUMNS} FROM knowledge_version "
            "WHERE artifact_id=? ORDER BY seq",
            (artifact_id,),
        ).fetchall()
        return [v for v in (self._version_row(r) for r in rows) if v is not None]

    def knowledge_versions_after(
        self, artifact_id: str, seq: int
    ) -> list[dict[str, Any]]:
        """Versions of one artifact strictly above ``seq``, oldest first.

        The emit pass's cursor query (issue #161): the primary key on
        ``(artifact_id, seq)`` answers it with a range scan, and a cursor
        already at the latest seq decodes zero rows.
        """
        rows = self._conn.execute(
            f"SELECT {self._VERSION_COLUMNS} FROM knowledge_version "
            "WHERE artifact_id=? AND seq>? ORDER BY seq",
            (artifact_id, int(seq)),
        ).fetchall()
        return [v for v in (self._version_row(r) for r in rows) if v is not None]

    def append_knowledge_version(
        self,
        artifact_id: str,
        *,
        manifest: list[dict[str, str]],
        occurred_at: float,
        origin: str,
        linked_event_id: str | None = None,
        is_tombstone: bool = False,
        skipped_files: list[dict[str, Any]] | None = None,
    ) -> tuple[int, bool]:
        """Append a version unless the manifest equals the artifact's latest.

        Returns ``(seq, created)``. Idempotent against the *latest* version, so
        a re-scan of unchanged content writes nothing, while a revert to an
        earlier state is a genuine new version (it differs from the latest).
        """
        manifest_hash = self._manifest_hash(manifest)
        skipped = skipped_files or []
        latest = self.latest_knowledge_version(artifact_id)
        if (
            latest is not None
            and latest["manifest_hash"] == manifest_hash
            and latest["skipped_files"] == skipped
            and latest["is_tombstone"] == is_tombstone
        ):
            return latest["seq"], False
        seq = latest["seq"] + 1 if latest is not None else 1
        self._conn.execute(
            "INSERT INTO knowledge_version("
            "artifact_id, seq, manifest_json, manifest_hash, occurred_at, origin, "
            "linked_event_id, is_tombstone, skipped_json) VALUES(?,?,?,?,?,?,?,?,?)",
            (
                artifact_id,
                seq,
                json.dumps(manifest, separators=(",", ":")),
                manifest_hash,
                float(occurred_at),
                origin,
                linked_event_id,
                1 if is_tombstone else 0,
                json.dumps(skipped, separators=(",", ":")),
            ),
        )
        return seq, True

    def set_knowledge_version_attribution(
        self,
        artifact_id: str,
        seq: int,
        *,
        origin: str,
        linked_event_id: str,
    ) -> None:
        """Attach the foreground event to an existing knowledge version."""
        self._conn.execute(
            "UPDATE knowledge_version SET origin=?, linked_event_id=? "
            "WHERE artifact_id=? AND seq=?",
            (origin, linked_event_id, artifact_id, seq),
        )

    def prune_knowledge_versions(self, artifact_id: str, *, keep: int) -> int:
        """Keep the newest ``keep`` versions of an artifact; delete older ones.

        Always keeps at least the latest version. Returns the count deleted.
        Blobs are not reclaimed here — call :meth:`gc_orphan_blobs` after.
        """
        keep = max(1, keep)
        doomed = [
            row[0]
            for row in self._conn.execute(
                "SELECT seq FROM knowledge_version WHERE artifact_id=? "
                "ORDER BY seq DESC",
                (artifact_id,),
            ).fetchall()[keep:]
        ]
        for seq in doomed:
            self._conn.execute(
                "DELETE FROM knowledge_version WHERE artifact_id=? AND seq=?",
                (artifact_id, seq),
            )
        return len(doomed)

    def gc_orphan_blobs(self) -> int:
        """Delete blobs no surviving version manifest references."""
        referenced: set[str] = set()
        for (manifest_json,) in self._conn.execute(
            "SELECT manifest_json FROM knowledge_version"
        ):
            for entry in json.loads(manifest_json):
                referenced.add(entry["blob_hash"])
        orphans = [
            row[0]
            for row in self._conn.execute("SELECT content_hash FROM knowledge_blob")
            if row[0] not in referenced
        ]
        for content_hash in orphans:
            self._conn.execute(
                "DELETE FROM knowledge_blob WHERE content_hash=?", (content_hash,)
            )
        return len(orphans)


__all__ = ["KnowledgeOutboxMixin"]
