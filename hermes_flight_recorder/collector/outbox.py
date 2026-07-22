"""Durable local outbox.

The outbox is the local SQLite store and the single
``producer_sequence`` authority per installation. Every producer (the
hook and the state adapter) appends through it, so one monotonic sequence
covers the whole event stream and makes gaps detectable.

Key properties:

- One outbox is one installation. The outbox mints and stores the
  ``installation_id`` (a UUID) once, at ``initialize()``.
- ``producer_sequence`` is assigned inside a ``BEGIN IMMEDIATE``
  transaction, so concurrent producers serialize with no gap and no reuse.
- The high-water mark lives in the database, so it survives a restart.
- Dedup on a caller-supplied stable key stops a re-captured row from
  appending twice, and does not consume a sequence number.
- Content is encrypted before write with a local dev key (POC only; real
  key custody is deferred).
- Retention can remove acknowledged event rows, but never sequence or meta
  state. Compact non-content tombstones preserve deduplication and
  reconciliation identity. The independent high-water mark therefore remains
  authoritative.

The outbox database must never live under ``HERMES_HOME``.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from ..envelope import SCHEMA_VERSION, parse, serialize, validate
from ._common import default_flight_recorder_home, resolve_hermes_home

__all__ = [
    "OUTBOX_SCHEMA_VERSION",
    "Outbox",
    "OutboxError",
    "PruneResult",
    "default_flight_recorder_home",
]

OUTBOX_SCHEMA_VERSION = "1"
_KEY_VERSION = "aesgcm256:dev"

_DDL = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS seq (
    installation_id TEXT PRIMARY KEY,
    high_water      INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    rowid_pk          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id          TEXT NOT NULL UNIQUE,
    installation_id   TEXT NOT NULL,
    producer_sequence INTEGER NOT NULL,
    dedup_key         TEXT UNIQUE,
    recorded_at       REAL NOT NULL,
    envelope_json     TEXT NOT NULL,
    UNIQUE (installation_id, producer_sequence)
);
CREATE TABLE IF NOT EXISTS retention_tombstones (
    installation_id   TEXT NOT NULL,
    producer_sequence INTEGER NOT NULL,
    event_id          TEXT NOT NULL,
    dedup_key         TEXT UNIQUE,
    recorded_at       REAL NOT NULL,
    summary_json      TEXT NOT NULL,
    PRIMARY KEY (installation_id, producer_sequence)
);
CREATE TABLE IF NOT EXISTS knowledge_blob (
    content_hash       TEXT PRIMARY KEY,
    content_ciphertext TEXT NOT NULL,
    content_nonce      TEXT NOT NULL,
    key_version        TEXT NOT NULL,
    byte_len           INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS knowledge_artifact (
    artifact_id TEXT PRIMARY KEY,
    kind        TEXT NOT NULL,
    name        TEXT NOT NULL,
    category    TEXT,
    provenance  TEXT NOT NULL,
    first_seen  REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS knowledge_version (
    artifact_id     TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    manifest_json   TEXT NOT NULL,
    manifest_hash   TEXT NOT NULL,
    occurred_at     REAL NOT NULL,
    origin          TEXT NOT NULL,
    linked_event_id TEXT,
    is_tombstone    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (artifact_id, seq)
);
"""


class OutboxError(RuntimeError):
    pass


_RETENTION_PAYLOAD_KEYS = (
    "message_row_id",
    "model",
    "task",
    "execution_id",
    "board",
    "task_id",
    "run_id",
)


def _retention_summary(record: dict[str, Any], sequence: int) -> dict[str, Any]:
    """Return the non-content fields reconciliation needs after a prune."""
    payload = record.get("payload", {})
    summary_payload = {"event_type": payload.get("event_type")}
    for key in _RETENTION_PAYLOAD_KEYS:
        if key in payload:
            summary_payload[key] = payload[key]

    summary: dict[str, Any] = {
        "producer_sequence": sequence,
        "payload": summary_payload,
    }
    for key in ("session_id", "invocation_id"):
        if record.get(key) is not None:
            summary[key] = record[key]
    return summary


@dataclass(frozen=True)
class PruneResult:
    """Summary of one acknowledged-event prune."""

    pruned_count: int
    oldest_sequence: int | None
    newest_sequence: int | None
    event_bytes_removed: int
    event_bytes_before: int
    event_bytes_after: int
    database_bytes_reclaimed: int
    delivery_cursor: int
    space_reclaim_error: str | None = None


class Outbox:
    """Local event store and append-only sequence authority."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._flight_recorder_home = self.path.parent
        self._conn = sqlite3.connect(str(self.path), isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_DDL)
        self._content_key: bytes | None = None
        self._aead: AESGCM | None = None
        self._installation_id: str | None = None

    # --- construction ---------------------------------------------------
    @classmethod
    def open(cls, flight_recorder_home: str | os.PathLike[str] | None = None) -> "Outbox":
        home = Path(flight_recorder_home).expanduser() if flight_recorder_home else default_flight_recorder_home()
        home = home.resolve()
        path = home / "outbox.sqlite"
        hermes = resolve_hermes_home(None).resolve()
        if path.is_relative_to(hermes):
            raise OutboxError(
                f"refusing to place the outbox under HERMES_HOME ({hermes}); "
                f"set SC_HERMES_FLIGHT_RECORDER_HOME to a directory outside the Hermes home"
            )
        home.mkdir(parents=True, exist_ok=True)
        return cls(path)

    def initialize(self) -> str:
        """Create the installation identity and content key once.

        Idempotent: an already-initialized outbox keeps its
        ``installation_id`` and key. Returns the ``installation_id``.
        """
        self._conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES('installation_id', ?)",
            (str(uuid.uuid4()),),
        )
        self._conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES('outbox_schema_version', ?)",
            (OUTBOX_SCHEMA_VERSION,),
        )
        self._ensure_content_key()
        return self.installation_id

    # --- identity -------------------------------------------------------
    @property
    def installation_id(self) -> str:
        # Write-once at initialize(), so cache after the first read: the
        # append hot path asks for it once per record.
        if self._installation_id is None:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key='installation_id'"
            ).fetchone()
            if row is None:
                raise OutboxError("outbox is not initialized; call initialize() first")
            self._installation_id = row[0]
        return self._installation_id

    # --- content key ----------------------------------------------------
    @property
    def _key_path(self) -> Path:
        return self._flight_recorder_home / "content-dev.key"

    def _ensure_content_key(self) -> bytes:
        if self._content_key is not None:
            return self._content_key
        if self._key_path.exists():
            self._content_key = self._key_path.read_bytes()
        else:
            key = AESGCM.generate_key(bit_length=256)
            self._key_path.write_bytes(key)
            os.chmod(self._key_path, 0o600)
            self._content_key = key
        return self._content_key

    def _cipher(self) -> AESGCM:
        # One AESGCM instance (one key schedule) per outbox, not per record.
        if self._aead is None:
            self._aead = AESGCM(self._ensure_content_key())
        return self._aead

    def _encrypt_content(self, content: str | bytes) -> dict[str, str]:
        raw = content.encode("utf-8") if isinstance(content, str) else content
        nonce = os.urandom(12)
        ciphertext = self._cipher().encrypt(nonce, raw, None)
        return {
            "content_ciphertext": base64.b64encode(ciphertext).decode("ascii"),
            "content_nonce": base64.b64encode(nonce).decode("ascii"),
            "content_hash": "sha256:" + hashlib.sha256(raw).hexdigest(),
            "key_version": _KEY_VERSION,
        }

    def decrypt_content(self, record: dict[str, Any]) -> bytes:
        """Decrypt a record's content. For tooling and tests only.

        The POC observe command never calls this; content stays encrypted
        at rest and in the console.
        """
        ct = record.get("content_ciphertext")
        nonce = record.get("content_nonce")
        if ct is None or nonce is None:
            raise OutboxError("record has no encrypted content")
        return self._cipher().decrypt(
            base64.b64decode(nonce), base64.b64decode(ct), None
        )

    # --- knowledge store ------------------------------------------------
    # A content-addressed store for Hermes-created skills and built-in
    # memories (Phase 3). It shares the outbox's cipher and connection but
    # its own tables, so event retention never touches knowledge state and
    # knowledge retention never touches events.
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
        if self._conn.execute(
            "SELECT 1 FROM knowledge_blob WHERE content_hash=?", (digest,)
        ).fetchone() is not None:
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

    def get_blob(self, content_hash: str) -> bytes:
        """Decrypt and return a stored blob. For restore and tests."""
        row = self._conn.execute(
            "SELECT content_ciphertext, content_nonce FROM knowledge_blob "
            "WHERE content_hash=?",
            (content_hash,),
        ).fetchone()
        if row is None:
            raise OutboxError(f"no knowledge blob for {content_hash}")
        return self._cipher().decrypt(
            base64.b64decode(row[1]), base64.b64decode(row[0]), None
        )

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
        }

    _VERSION_COLUMNS = (
        "artifact_id, seq, manifest_json, manifest_hash, occurred_at, origin, "
        "linked_event_id, is_tombstone"
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

    def append_knowledge_version(
        self,
        artifact_id: str,
        *,
        manifest: list[dict[str, str]],
        occurred_at: float,
        origin: str,
        linked_event_id: str | None = None,
        is_tombstone: bool = False,
    ) -> tuple[int, bool]:
        """Append a version unless the manifest equals the artifact's latest.

        Returns ``(seq, created)``. Idempotent against the *latest* version, so
        a re-scan of unchanged content writes nothing, while a revert to an
        earlier state is a genuine new version (it differs from the latest).
        """
        manifest_hash = self._manifest_hash(manifest)
        latest = self.latest_knowledge_version(artifact_id)
        if latest is not None and latest["manifest_hash"] == manifest_hash:
            return latest["seq"], False
        seq = latest["seq"] + 1 if latest is not None else 1
        self._conn.execute(
            "INSERT INTO knowledge_version("
            "artifact_id, seq, manifest_json, manifest_hash, occurred_at, origin, "
            "linked_event_id, is_tombstone) VALUES(?,?,?,?,?,?,?,?)",
            (
                artifact_id,
                seq,
                json.dumps(manifest, separators=(",", ":")),
                manifest_hash,
                float(occurred_at),
                origin,
                linked_event_id,
                1 if is_tombstone else 0,
            ),
        )
        return seq, True

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

    # --- append ---------------------------------------------------------
    def append(
        self,
        record: dict[str, Any],
        *,
        content: str | bytes | None = None,
        dedup_key: str | None = None,
    ) -> dict[str, Any]:
        """Stamp, validate, and durably append one envelope record.

        The outbox sets ``installation_id`` (from its own identity),
        ``event_id``, ``producer_sequence``, and ``recorded_at``. When
        ``content`` is given, the outbox encrypts it and sets the four
        content fields. When ``dedup_key`` matches an existing row, no new
        row is written and no sequence number is consumed; the stored
        record is returned.
        """
        record, _ = self._append(
            record, content=content, dedup_key=dedup_key, return_stored=True
        )
        return record

    def append_if_new(
        self,
        record: dict[str, Any],
        *,
        content: str | bytes | None = None,
        dedup_key: str | None = None,
    ) -> bool:
        """Append one record and report whether a new row was inserted."""
        _, created = self._append(
            record, content=content, dedup_key=dedup_key, return_stored=False
        )
        return created

    def _append(
        self,
        record: dict[str, Any],
        *,
        content: str | bytes | None,
        dedup_key: str | None,
        return_stored: bool,
    ) -> tuple[dict[str, Any], bool]:
        """Implement both append APIs and return the stored record and outcome."""
        rec = dict(record)
        if content is not None:
            rec.update(self._encrypt_content(content))
        rec.setdefault("schema_version", SCHEMA_VERSION)
        rec["installation_id"] = self.installation_id
        rec["event_id"] = str(uuid.uuid4())
        rec["recorded_at"] = time.time()
        inst = rec["installation_id"]

        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            if dedup_key is not None:
                # append() promises the stored record on a dedup hit;
                # append_if_new() only needs the hit/miss, so skip fetching
                # and parsing the stored envelope on that (steady-state) path.
                column = "envelope_json" if return_stored else "1"
                existing = conn.execute(
                    f"SELECT {column} FROM events WHERE dedup_key=?", (dedup_key,)
                ).fetchone()
                if existing is not None:
                    conn.execute("COMMIT")
                    return (parse(existing[0]) if return_stored else rec), False

                pruned = conn.execute(
                    "SELECT event_id, producer_sequence, recorded_at "
                    "FROM retention_tombstones WHERE dedup_key=?",
                    (dedup_key,),
                ).fetchone()
                if pruned is not None:
                    conn.execute("COMMIT")
                    if return_stored:
                        # The full envelope was intentionally removed. Return
                        # the caller's logical record with the original stable
                        # identity instead of creating a replacement event.
                        rec["event_id"] = pruned[0]
                        rec["producer_sequence"] = pruned[1]
                        rec["recorded_at"] = pruned[2]
                    return rec, False

            row = conn.execute(
                "SELECT high_water FROM seq WHERE installation_id=?", (inst,)
            ).fetchone()
            seq = (row[0] if row else 0) + 1
            rec["producer_sequence"] = seq

            validate(rec)  # raises before any write on a bad record

            conn.execute(
                "INSERT INTO events (event_id, installation_id, producer_sequence, "
                "dedup_key, recorded_at, envelope_json) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    rec["event_id"],
                    inst,
                    seq,
                    dedup_key,
                    rec["recorded_at"],
                    serialize(rec),
                ),
            )
            conn.execute(
                "INSERT INTO seq (installation_id, high_water) VALUES (?, ?) "
                "ON CONFLICT(installation_id) DO UPDATE SET high_water=excluded.high_water",
                (inst, seq),
            )
            conn.execute("COMMIT")
            return rec, True
        except Exception:
            conn.execute("ROLLBACK")
            raise

    # --- poll cursors ---------------------------------------------------
    # Producers (the state adapter) keep an incremental cursor per source in
    # the outbox meta, so a re-poll scans only new rows. Dedup on the append
    # side is the backstop that guarantees no duplicate even if a cursor is
    # reset.
    def get_cursor(self, name: str) -> str | None:
        return self.get_meta(f"cursor:{name}")

    def set_cursor(self, name: str, value: str | int) -> None:
        self.set_meta(f"cursor:{name}", str(value))

    # --- generic meta -----------------------------------------------------
    # A producer may persist small bits of cross-drain state directly in the
    # meta table (e.g. the hook drain's start/end invocation pairing), keyed
    # by its own arbitrary name.
    def get_meta(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key=?", (key,)
        ).fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

    def delete_meta(self, key: str) -> None:
        self._conn.execute("DELETE FROM meta WHERE key=?", (key,))

    # --- read -----------------------------------------------------------
    def high_water(self, installation_id: str | None = None) -> int:
        inst = installation_id or self.installation_id
        row = self._conn.execute(
            "SELECT high_water FROM seq WHERE installation_id=?", (inst,)
        ).fetchone()
        return row[0] if row else 0

    def count(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    def prune_delivered(
        self,
        delivery_cursor: int,
        *,
        older_than: float | None = None,
        max_bytes: int | None = None,
        vacuum: bool = True,
    ) -> PruneResult:
        """Remove delivered events selected by age or a byte budget.

        ``delivery_cursor`` is a hard upper bound: rows with a greater
        sequence are never candidates. ``max_bytes`` measures the UTF-8
        bytes of stored envelope JSON, which keeps the policy independent of
        SQLite page size. When that budget is exceeded, acknowledged rows
        are removed oldest-first until the retained event bytes fit or no
        acknowledged rows remain.

        Sequence authority and every meta value are deliberately untouched.
        A vacuum runs only when rows were deleted.
        """
        if delivery_cursor < 0:
            raise ValueError("delivery_cursor cannot be negative")
        if max_bytes is not None and max_bytes < 1:
            raise ValueError("max_bytes must be at least 1")

        inst = self.installation_id
        conn = self._conn
        conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS retention_prune ("
            "rowid_pk INTEGER PRIMARY KEY, "
            "producer_sequence INTEGER NOT NULL, "
            "event_bytes INTEGER NOT NULL)"
        )
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("DELETE FROM retention_prune")
            database_bytes_before = self._allocated_bytes()
            event_bytes_before = conn.execute(
                "SELECT COALESCE(SUM(length(CAST(envelope_json AS BLOB))), 0) "
                "FROM events WHERE installation_id=?",
                (inst,),
            ).fetchone()[0]

            if older_than is not None:
                conn.execute(
                    "INSERT INTO retention_prune "
                    "(rowid_pk, producer_sequence, event_bytes) "
                    "SELECT rowid_pk, producer_sequence, "
                    "length(CAST(envelope_json AS BLOB)) "
                    "FROM events WHERE installation_id=? "
                    "AND producer_sequence<=? AND recorded_at<?",
                    (inst, delivery_cursor, older_than),
                )

            selected_bytes = conn.execute(
                "SELECT COALESCE(SUM(event_bytes), 0) FROM retention_prune"
            ).fetchone()[0]
            remaining_bytes = event_bytes_before - selected_bytes
            if max_bytes is not None and remaining_bytes > max_bytes:
                age_complement = (
                    "AND recorded_at>=? " if older_than is not None else ""
                )
                candidate_params = (
                    (inst, delivery_cursor, older_than)
                    if older_than is not None
                    else (inst, delivery_cursor)
                )
                size_candidates = conn.execute(
                    "SELECT rowid_pk, producer_sequence, "
                    "length(CAST(envelope_json AS BLOB)) "
                    "FROM events WHERE installation_id=? "
                    "AND producer_sequence<=? "
                    + age_complement
                    + "ORDER BY producer_sequence",
                    candidate_params,
                )
                batch: list[tuple[int, int, int]] = []
                try:
                    for rowid_pk, sequence, event_bytes in size_candidates:
                        batch.append((rowid_pk, sequence, event_bytes))
                        selected_bytes += event_bytes
                        remaining_bytes -= event_bytes
                        if len(batch) == 1_000 or remaining_bytes <= max_bytes:
                            conn.executemany(
                                "INSERT INTO retention_prune VALUES (?, ?, ?)",
                                batch,
                            )
                            batch.clear()
                        if remaining_bytes <= max_bytes:
                            break
                    if batch:
                        conn.executemany(
                            "INSERT INTO retention_prune VALUES (?, ?, ?)",
                            batch,
                        )
                finally:
                    size_candidates.close()

            pruned_count, oldest_sequence, newest_sequence = conn.execute(
                "SELECT COUNT(*), MIN(producer_sequence), MAX(producer_sequence) "
                "FROM retention_prune"
            ).fetchone()
            if pruned_count:
                self._store_retention_tombstones()
                conn.execute(
                    "DELETE FROM events WHERE rowid_pk IN "
                    "(SELECT rowid_pk FROM retention_prune)"
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        space_reclaim_error = None
        if pruned_count and vacuum:
            try:
                self._reclaim_space()
            except Exception as exc:
                # The event deletion committed before VACUUM. Preserve and
                # report that successful prune instead of misreporting the
                # whole operation as refused.
                space_reclaim_error = str(exc)

        database_bytes_after = self._allocated_bytes()
        return PruneResult(
            pruned_count=pruned_count,
            oldest_sequence=oldest_sequence,
            newest_sequence=newest_sequence,
            event_bytes_removed=selected_bytes,
            event_bytes_before=event_bytes_before,
            event_bytes_after=event_bytes_before - selected_bytes,
            database_bytes_reclaimed=max(
                0, database_bytes_before - database_bytes_after
            ),
            delivery_cursor=delivery_cursor,
            space_reclaim_error=space_reclaim_error,
        )

    def _store_retention_tombstones(self) -> None:
        """Keep compact idempotency and reconciliation state before deletion."""
        rows = self._conn.execute(
            "SELECT e.installation_id, e.producer_sequence, e.event_id, "
            "e.dedup_key, e.recorded_at, e.envelope_json "
            "FROM events AS e JOIN retention_prune AS p "
            "ON p.rowid_pk=e.rowid_pk ORDER BY e.producer_sequence"
        )
        batch: list[tuple[str, int, str, str | None, float, str]] = []
        try:
            for installation_id, sequence, event_id, dedup_key, recorded_at, raw in rows:
                record = parse(raw)
                summary = _retention_summary(record, sequence)
                batch.append(
                    (
                        installation_id,
                        sequence,
                        event_id,
                        dedup_key,
                        recorded_at,
                        json.dumps(
                            summary,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                    )
                )
                if len(batch) == 1_000:
                    self._insert_retention_tombstones(batch)
                    batch.clear()
            if batch:
                self._insert_retention_tombstones(batch)
        finally:
            rows.close()

    def _insert_retention_tombstones(
        self,
        rows: list[tuple[str, int, str, str | None, float, str]],
    ) -> None:
        self._conn.executemany(
            "INSERT INTO retention_tombstones "
            "(installation_id, producer_sequence, event_id, dedup_key, "
            "recorded_at, summary_json) VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )

    def _reclaim_space(self) -> None:
        """Rebuild the database and release its WAL pages to the filesystem."""
        self._conn.execute("VACUUM")
        checkpoint = self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if checkpoint is not None and checkpoint[0] != 0:
            raise sqlite3.OperationalError("WAL checkpoint remained busy")

    def _allocated_bytes(self) -> int:
        """Return bytes allocated to the SQLite database's pages."""
        page_size = self._conn.execute("PRAGMA page_size").fetchone()[0]
        page_count = self._conn.execute("PRAGMA page_count").fetchone()[0]
        return page_size * page_count

    def iter_pruned_summaries(
        self,
        installation_id: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield compact summaries for intentionally pruned event sequences."""
        inst = installation_id or self.installation_id
        rows = self._conn.execute(
            "SELECT summary_json FROM retention_tombstones "
            "WHERE installation_id=? ORDER BY producer_sequence",
            (inst,),
        )
        try:
            for (raw,) in rows:
                yield json.loads(raw)
        finally:
            rows.close()

    def iter_events(
        self,
        installation_id: str | None = None,
        *,
        after_sequence: int = 0,
    ) -> Iterator[dict[str, Any]]:
        """Yield records in (installation_id, producer_sequence) order.

        ``after_sequence`` skips records at or below a cursor in SQL (a range
        scan on the unique index), so a caller resuming from a cursor never
        pays to load and parse the already-handled history.
        """
        if installation_id is None:
            cur = self._conn.execute(
                "SELECT envelope_json FROM events WHERE producer_sequence>? "
                "ORDER BY installation_id, producer_sequence",
                (after_sequence,),
            )
        else:
            cur = self._conn.execute(
                "SELECT envelope_json FROM events "
                "WHERE installation_id=? AND producer_sequence>? "
                "ORDER BY producer_sequence",
                (installation_id, after_sequence),
            )
        for (blob,) in cur:
            yield parse(blob)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "Outbox":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
