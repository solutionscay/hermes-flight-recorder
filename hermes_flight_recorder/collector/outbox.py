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
- Content is encrypted before write with a per-process data key (DEK) that
  is sealed to the fleet operator public key; the plaintext DEK stays in
  memory only and its wrapped form is stored in ``content_keys``. Reading
  content back needs the operator private key (see :mod:`content_crypto` and
  :mod:`keystore`). Content too large for one ingest request is
  encrypted in bounded runtime chunk records and committed by its logical
  parent record.
- Retention can remove acknowledged event rows, but never sequence or meta
  state. Compact non-content tombstones preserve deduplication and
  reconciliation identity. The independent high-water mark therefore remains
  authoritative.

The outbox lives inside the Hermes home, under the namespaced
``flight-recorder`` child (``$HERMES_HOME/flight-recorder``) — one Hermes
home is one Flight Recorder installation. It may not sit at the Hermes root
itself, which belongs to Hermes.
"""

from __future__ import annotations

import base64
import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator

from ..envelope import SCHEMA_VERSION, parse, serialize, validate
from . import content_crypto as cc
from . import keystore
from ._common import (
    FLIGHT_RECORDER_DIR_NAME,
    default_flight_recorder_home,
    resolve_flight_recorder_home,
    resolve_hermes_home,
)
from .sync import MAX_INGEST_BATCH_BYTES, singleton_batch_size
from .outbox_errors import OutboxError
from .outbox_knowledge import KnowledgeOutboxMixin

__all__ = [
    "OUTBOX_SCHEMA_VERSION",
    "Outbox",
    "OutboxError",
    "PruneResult",
    "default_flight_recorder_home",
]

OUTBOX_SCHEMA_VERSION = "2"
_CONTENT_CHUNK_BYTES = 2 * 1024 * 1024
_CONTENT_CHUNK_EVENT = "runtime.content_chunk_recorded"
_CONTENT_FIELDS = (
    "content_ciphertext",
    "content_nonce",
    "content_hash",
    "key_version",
)
# ``old_version: (new_version, method_name)``. Each method runs inside the
# transaction that also advances the durable schema version.
_MIGRATIONS: dict[str, tuple[str, str]] = {
    "1": ("2", "_migrate_knowledge_skipped_files"),
}

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
CREATE TABLE IF NOT EXISTS content_keys (
    key_version     TEXT PRIMARY KEY,
    operator_key_id TEXT NOT NULL,
    wrapped_dek     TEXT NOT NULL,
    created_at      REAL NOT NULL,
    shipped_at      REAL
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
    skipped_json    TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (artifact_id, seq)
);
"""


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


class Outbox(KnowledgeOutboxMixin):
    """Local event store and append-only sequence authority."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._flight_recorder_home = self.path.parent
        self._conn = sqlite3.connect(str(self.path), isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_DDL)
        # The per-process data key (plaintext, in memory only) and the
        # key_version that identifies its wrapped form in ``content_keys``.
        self._dek: bytes | None = None
        self._key_version: str | None = None
        # Cache of DEKs unwrapped for reads this process, keyed by key_version,
        # so decrypting many records under one epoch unwraps once.
        self._dek_by_version: dict[str, bytes] = {}
        # Keep the current knowledge state in process memory. A fleet host can
        # use it for consecutive mutations without a private decryption key.
        self._knowledge_plaintext: dict[str, dict[str, bytes]] = {}
        self._installation_id: str | None = None
        # Depth of the open ``batch()`` context; 0 means every append runs in
        # its own ``BEGIN IMMEDIATE`` transaction (the historical behavior).
        self._batch_depth = 0
        self._apply_migrations()

    # --- construction ---------------------------------------------------
    @classmethod
    def open(
        cls,
        flight_recorder_home: str | os.PathLike[str] | None = None,
        *,
        hermes_home: str | os.PathLike[str] | None = None,
    ) -> "Outbox":
        home = resolve_flight_recorder_home(flight_recorder_home, hermes_home).resolve()
        hermes = resolve_hermes_home(hermes_home).resolve()
        if home == hermes:
            raise OutboxError(
                f"refusing to use the Hermes home root ({hermes}) as the flight "
                f"recorder home; use its namespaced '{FLIGHT_RECORDER_DIR_NAME}' "
                f"child or set SC_HERMES_FLIGHT_RECORDER_HOME"
            )
        home.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            os.chmod(home, 0o700)
        except OSError as exc:
            raise OutboxError(
                f"cannot make flight recorder home private: {home}"
            ) from exc
        database = home / "outbox.sqlite"
        if database.is_symlink():
            raise OutboxError(f"refusing symbolic link for recorder database: {database}")
        outbox = cls(database)
        try:
            for suffix in ("", "-wal", "-shm", "-journal"):
                path = Path(f"{database}{suffix}")
                if path.is_symlink():
                    raise OutboxError(
                        f"refusing symbolic link for recorder database file: {path}"
                    )
                if path.exists():
                    os.chmod(path, 0o600)
        except OSError as exc:
            outbox.close()
            raise OutboxError(
                f"cannot make flight recorder database private: {home}"
            ) from exc
        except OutboxError:
            outbox.close()
            raise
        return outbox

    def initialize(self) -> str:
        """Create the installation identity once.

        Idempotent: an already-initialized outbox keeps its
        ``installation_id``. Returns the ``installation_id``. The operator key
        that content is sealed to lives in the keystore (minted by ``install``
        / ``keygen``); the per-process data key is minted lazily on the first
        content write, so nothing key-related is created here.
        """
        self._conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES('installation_id', ?)",
            (str(uuid.uuid4()),),
        )
        self._conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES('outbox_schema_version', ?)",
            (OUTBOX_SCHEMA_VERSION,),
        )
        self._apply_migrations()
        return self.installation_id

    def _apply_migrations(self) -> None:
        """Apply registered schema migrations transactionally and in order."""
        version = self.get_meta("outbox_schema_version")
        if version is None:
            return
        while version != OUTBOX_SCHEMA_VERSION:
            migration = _MIGRATIONS.get(version)
            if migration is None:
                raise OutboxError(
                    f"unsupported outbox schema version {version!r}; "
                    f"this build supports {OUTBOX_SCHEMA_VERSION!r}"
                )
            next_version, method_name = migration
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                getattr(self, method_name)()
                self._conn.execute(
                    "UPDATE meta SET value=? WHERE key='outbox_schema_version'",
                    (next_version,),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            version = next_version

    def _migrate_knowledge_skipped_files(self) -> None:
        """Add durable omission metadata to knowledge versions."""
        self._conn.execute(
            "ALTER TABLE knowledge_version ADD COLUMN skipped_json "
            "TEXT NOT NULL DEFAULT '[]'"
        )

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

    # --- content keys ---------------------------------------------------
    # Writing needs only the operator *public* key: content is encrypted with
    # a per-process data key (DEK) that is sealed to that public key. The
    # plaintext DEK stays in memory; its wrapped form is persisted in
    # ``content_keys`` so a reader with the operator private key can recover
    # it. Reading is in the decryption section below and needs the private key.
    def _operator_public(self) -> cc.OperatorPublicKey:
        """The operator public key this installation seals content to.

        Solo bootstrap mirrors the old auto-generated dev key: if neither key
        half exists yet, mint a solo operator keypair (both halves local). A
        fleet agent already holds ``operator.pub`` and no private key; its
        public key is used and no key is minted.
        """
        home = self._flight_recorder_home
        if not keystore.has_public(home) and not keystore.has_secret(home):
            keystore.ensure_solo_keypair(home)
        return keystore.load_public_key(home)

    def _current_dek(self) -> tuple[str, bytes]:
        """Return this process's ``(key_version, dek)``, minting once.

        A fresh DEK per process/epoch is sealed to the operator public key and
        its wrapped form recorded in ``content_keys`` keyed by ``key_version``.
        The plaintext DEK never leaves memory. ``key_version`` ties a record to
        the operator key epoch (``operator_key_id``) plus this process's DEK.
        """
        if self._dek is not None and self._key_version is not None:
            return self._key_version, self._dek
        public = self._operator_public()
        dek = cc.generate_dek()
        wrapped = cc.wrap_dek(public, dek)
        key_version = f"{public.key_id}#{uuid.uuid4().hex[:16]}"
        self._conn.execute(
            "INSERT OR IGNORE INTO content_keys("
            "key_version, operator_key_id, wrapped_dek, created_at) "
            "VALUES(?,?,?,?)",
            (
                key_version,
                public.key_id,
                base64.b64encode(wrapped).decode("ascii"),
                time.time(),
            ),
        )
        self._dek = dek
        self._key_version = key_version
        return key_version, dek

    def _encrypt_content(self, content: str | bytes) -> dict[str, str]:
        key_version, dek = self._current_dek()
        ciphertext, nonce, content_hash = cc.encrypt_content(dek, content)
        return {
            "content_ciphertext": base64.b64encode(ciphertext).decode("ascii"),
            "content_nonce": base64.b64encode(nonce).decode("ascii"),
            "content_hash": content_hash,
            "key_version": key_version,
        }

    # --- wrapped DEK shipping -------------------------------------------
    # The wrapped DEKs are a small keyless side-channel: each is an opaque
    # blob sealed to the operator public key, shipped out of band so a reader
    # with the operator private key can unwrap it (the server never can). They
    # travel to the ingestion service's wrapped-DEK endpoint separately from
    # events; ``shipped_at`` records a durable ack so a later pass skips them.
    # Delivery is idempotent server-side by (installation_id, key_version).
    def iter_unshipped_content_keys(self) -> Iterator[dict[str, Any]]:
        """Yield wrapped-DEK records not yet acknowledged by the service.

        ``recipient_key_id`` is the operator key epoch the DEK was sealed to;
        ``wrapped_dek`` is opaque base64 already. ``installation_id`` ties each
        record to this installation, exactly as an event does.
        """
        inst = self.installation_id
        rows = self._conn.execute(
            "SELECT key_version, operator_key_id, wrapped_dek FROM content_keys "
            "WHERE shipped_at IS NULL ORDER BY created_at, key_version"
        ).fetchall()
        for key_version, operator_key_id, wrapped_dek in rows:
            yield {
                "installation_id": inst,
                "key_version": key_version,
                "recipient_key_id": operator_key_id,
                "wrapped_dek": wrapped_dek,
            }

    def mark_content_keys_shipped(self, key_versions: Iterable[str]) -> None:
        """Record that wrapped-DEK records were durably accepted."""
        now = time.time()
        self._conn.executemany(
            "UPDATE content_keys SET shipped_at=? WHERE key_version=?",
            [(now, kv) for kv in key_versions],
        )

    # --- reading (operator private key required) ------------------------
    def _resolve_keypair(self, keypair: cc.OperatorKeyPair | None) -> cc.OperatorKeyPair:
        """The keypair to decrypt with: the caller's, else the solo keystore's.

        Passing an explicit keypair supports the operator console, which holds
        the private key off-host. Falling back to the keystore keeps solo
        tooling and tests key-free. On a fleet agent (no private key) this
        raises, which is the intended posture: writes but no reads.
        """
        if keypair is not None:
            return keypair
        return keystore.load_keypair(self._flight_recorder_home)

    def _dek_for_version(
        self, key_version: str, keypair: cc.OperatorKeyPair
    ) -> bytes:
        """Unwrap (and cache) the DEK for ``key_version`` via the private key."""
        cached = self._dek_by_version.get(key_version)
        if cached is not None:
            return cached
        row = self._conn.execute(
            "SELECT wrapped_dek FROM content_keys WHERE key_version=?",
            (key_version,),
        ).fetchone()
        if row is None:
            raise OutboxError(
                f"no wrapped data key stored for key_version {key_version!r}"
            )
        dek = cc.unwrap_dek(keypair, base64.b64decode(row[0]))
        self._dek_by_version[key_version] = dek
        return dek

    def decrypt_content(
        self,
        record: dict[str, Any],
        keypair: cc.OperatorKeyPair | None = None,
    ) -> bytes:
        """Decrypt a record's content with the operator private key.

        For tooling and tests on a solo or operator machine only — reading
        requires the private key. The POC observe command never calls this;
        content stays encrypted at rest and in the console. Inline content
        decrypts directly. Chunked content is reconstructed from its preceding
        encrypted chunk records and verified against the parent record's byte
        count and hash. Pass ``keypair`` to decrypt with a key held off-host
        (the operator console); omit it to use the solo keystore keypair.
        """
        ct = record.get("content_ciphertext")
        nonce = record.get("content_nonce")
        if ct is not None and nonce is not None:
            return self._decrypt_inline_content(record, self._resolve_keypair(keypair))

        payload = record.get("payload", {})
        if payload.get("content_storage") != "chunked":
            raise OutboxError("record has no encrypted content")
        resolved = self._resolve_keypair(keypair)
        content_ref = payload.get("content_ref")
        chunk_count = payload.get("content_chunk_count")
        if not isinstance(content_ref, str) or not isinstance(chunk_count, int):
            raise OutboxError("chunked content metadata is invalid")

        chunks = []
        for event in self.iter_events(record.get("installation_id")):
            chunk_payload = event.get("payload", {})
            if (
                chunk_payload.get("event_type") == _CONTENT_CHUNK_EVENT
                and chunk_payload.get("content_ref") == content_ref
            ):
                chunks.append(event)
        chunks.sort(key=lambda event: event["payload"].get("chunk_index", -1))
        indices = [event["payload"].get("chunk_index") for event in chunks]
        if len(chunks) != chunk_count or indices != list(range(chunk_count)):
            raise OutboxError(
                f"chunked content {content_ref} is incomplete: "
                f"expected {chunk_count} chunks, found {len(chunks)}"
            )

        raw = b"".join(
            self._decrypt_inline_content(chunk, resolved) for chunk in chunks
        )
        expected_bytes = payload.get("content_plaintext_bytes")
        expected_hash = payload.get("content_plaintext_hash")
        if expected_bytes != len(raw) or expected_hash != self._content_hash(raw):
            raise OutboxError(f"chunked content {content_ref} failed verification")
        return raw

    def _decrypt_inline_content(
        self, record: dict[str, Any], keypair: cc.OperatorKeyPair
    ) -> bytes:
        ct = record.get("content_ciphertext")
        nonce = record.get("content_nonce")
        key_version = record.get("key_version")
        if ct is None or nonce is None or key_version is None:
            raise OutboxError("record has no inline encrypted content")
        dek = self._dek_for_version(key_version, keypair)
        return cc.decrypt_content(dek, base64.b64decode(ct), base64.b64decode(nonce))

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

    @contextmanager
    def batch(self) -> Iterator["Outbox"]:
        """Group appends and meta writes into one durable transaction.

        Opens ``BEGIN IMMEDIATE`` once, so every ``append`` /
        ``append_if_new``, cursor, and meta write inside the block joins a
        single write transaction that commits on exit and rolls back as one on
        an exception. A nested ``batch()`` joins the open transaction instead
        of deadlocking on the shared connection; only the outermost block
        commits or rolls back. Dedup re-checks inside a batch run within the
        already-open transaction, so a dedup hit starts no extra transaction.

        Never hold a batch open across slow non-database work (source file
        reads, content hashing): gather the source rows first, then batch only
        the append loop.
        """
        if self._batch_depth:
            self._batch_depth += 1
            try:
                yield self
            finally:
                self._batch_depth -= 1
            return
        self._conn.execute("BEGIN IMMEDIATE")
        self._batch_depth = 1
        try:
            yield self
        except BaseException:
            self._conn.execute("ROLLBACK")
            self._forget_process_dek()
            raise
        else:
            self._conn.execute("COMMIT")
        finally:
            self._batch_depth = 0

    def _forget_process_dek(self) -> None:
        """Drop the cached data key after a rollback.

        The rolled-back transaction may have minted this process's DEK, in
        which case its wrapped form in ``content_keys`` rolled back with it.
        Forgetting the cached plaintext makes the next content write mint (and
        durably record) a fresh key instead of encrypting under a key version
        that no longer exists.
        """
        self._dek = None
        self._key_version = None

    def event_by_dedup_key(self, dedup_key: str) -> dict[str, Any] | None:
        """Return a retained event by its stable producer identity."""
        row = self._conn.execute(
            "SELECT envelope_json FROM events WHERE dedup_key=?", (dedup_key,)
        ).fetchone()
        return parse(row[0]) if row is not None else None

    def has_dedup_key(self, dedup_key: str) -> bool:
        """Test retained events and compact retention tombstones."""
        if self._conn.execute(
            "SELECT 1 FROM events WHERE dedup_key=?", (dedup_key,)
        ).fetchone():
            return True
        return (
            self._conn.execute(
                "SELECT 1 FROM retention_tombstones WHERE dedup_key=?",
                (dedup_key,),
            ).fetchone()
            is not None
        )

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
        raw_content = (
            content.encode("utf-8") if isinstance(content, str) else content
        )
        rec.setdefault("schema_version", SCHEMA_VERSION)
        rec["installation_id"] = self.installation_id
        rec["event_id"] = str(uuid.uuid4())
        rec["recorded_at"] = time.time()
        inst = rec["installation_id"]

        conn = self._conn
        # Inside an open batch() the transaction already exists and commits
        # (or rolls back) with the batch; run every statement within it.
        in_batch = self._batch_depth > 0
        if not in_batch:
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
                    if not in_batch:
                        conn.execute("COMMIT")
                    return (parse(existing[0]) if return_stored else rec), False

                pruned = conn.execute(
                    "SELECT event_id, producer_sequence, recorded_at "
                    "FROM retention_tombstones WHERE dedup_key=?",
                    (dedup_key,),
                ).fetchone()
                if pruned is not None:
                    if not in_batch:
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
            high_water = row[0] if row else 0
            rec["producer_sequence"] = high_water + 1

            chunk_records: list[dict[str, Any]] = []
            if (
                raw_content is not None
                and self._inline_singleton_size(rec, len(raw_content))
                > MAX_INGEST_BATCH_BYTES
            ):
                chunk_records = self._content_chunk_records(
                    rec, raw_content, high_water
                )
                payload = dict(rec["payload"])
                payload.update(
                    {
                        "content_storage": "chunked",
                        "content_ref": rec["event_id"],
                        "content_chunk_count": len(chunk_records),
                        "content_plaintext_bytes": len(raw_content),
                        "content_plaintext_hash": self._content_hash(raw_content),
                    }
                )
                rec["payload"] = payload
                rec["producer_sequence"] = high_water + len(chunk_records) + 1
            elif raw_content is not None:
                rec.update(self._encrypt_content(raw_content))

            validate(rec)  # raises before any write on a bad record
            if singleton_batch_size(rec) > MAX_INGEST_BATCH_BYTES:
                raise OutboxError(
                    "record metadata exceeds the ingestion protocol's "
                    f"{MAX_INGEST_BATCH_BYTES}-byte limit"
                )

            for chunk in chunk_records:
                validate(chunk)
                if singleton_batch_size(chunk) > MAX_INGEST_BATCH_BYTES:
                    raise OutboxError(
                        "content chunk exceeds the ingestion protocol's "
                        f"{MAX_INGEST_BATCH_BYTES}-byte limit"
                    )
                self._insert_event(chunk, dedup_key=None)
            self._insert_event(rec, dedup_key=dedup_key)
            conn.execute(
                "INSERT INTO seq (installation_id, high_water) VALUES (?, ?) "
                "ON CONFLICT(installation_id) DO UPDATE SET high_water=excluded.high_water",
                (inst, rec["producer_sequence"]),
            )
            if not in_batch:
                conn.execute("COMMIT")
            return rec, True
        except Exception:
            # Inside a batch the open transaction stays usable: a failed
            # statement is rolled back by SQLite itself, earlier appends in
            # the batch survive, and the batch decides commit or rollback.
            if not in_batch:
                conn.execute("ROLLBACK")
                self._forget_process_dek()
            raise

    def _inline_singleton_size(self, record: dict[str, Any], plaintext_bytes: int) -> int:
        """Exact singleton wire size after encrypting ``plaintext_bytes``.

        AES-GCM adds a 16-byte tag. Base64 length is deterministic, and its
        alphabet needs no JSON escaping. Measuring a record with empty content
        fields and adding their encoded lengths avoids allocating and
        encrypting an arbitrarily large inline candidate only to discard it.
        The stamped ``key_version`` is this process's, so its exact length is
        known once the DEK is minted.
        """
        key_version, _ = self._current_dek()
        probe = dict(record)
        probe.update({field: "" for field in _CONTENT_FIELDS})
        ciphertext_bytes = plaintext_bytes + 16
        ciphertext_b64 = 4 * ((ciphertext_bytes + 2) // 3)
        nonce_b64 = 16  # a 12-byte AES-GCM nonce
        content_hash = len("sha256:") + 64
        return (
            singleton_batch_size(probe)
            + ciphertext_b64
            + nonce_b64
            + content_hash
            + len(key_version)
        )

    def _content_chunk_records(
        self, parent: dict[str, Any], raw: bytes, high_water: int
    ) -> list[dict[str, Any]]:
        """Build encrypted transport chunks that immediately precede a parent."""
        chunk_count = (len(raw) + _CONTENT_CHUNK_BYTES - 1) // _CONTENT_CHUNK_BYTES
        full_hash = self._content_hash(raw)
        records = []
        for index, offset in enumerate(
            range(0, len(raw), _CONTENT_CHUNK_BYTES)
        ):
            piece = raw[offset : offset + _CONTENT_CHUNK_BYTES]
            chunk = {
                "schema_version": parent["schema_version"],
                "event_id": str(uuid.uuid4()),
                "producer_sequence": high_water + index + 1,
                "occurred_at": parent["occurred_at"],
                "recorded_at": parent["recorded_at"],
                "installation_id": parent["installation_id"],
                "tenant_id": parent["tenant_id"],
                "profile": parent["profile"],
                "runtime": parent["runtime"],
                "correlation_id": parent["correlation_id"],
                "source": "outbox:content-chunker",
                "capture_method": "derive:outbox-content-chunk",
                "payload": {
                    "event_type": _CONTENT_CHUNK_EVENT,
                    "content_ref": parent["event_id"],
                    "chunk_index": index,
                    "chunk_count": chunk_count,
                    "chunk_plaintext_bytes": len(piece),
                    "content_plaintext_bytes": len(raw),
                    "content_plaintext_hash": full_hash,
                },
                "partial": False,
                **self._encrypt_content(piece),
            }
            for field in (
                "session_id",
                "session_key",
                "parent_session_id",
                "invocation_id",
            ):
                if field in parent:
                    chunk[field] = parent[field]
            records.append(chunk)
        return records

    def _insert_event(
        self, record: dict[str, Any], *, dedup_key: str | None
    ) -> None:
        self._conn.execute(
            "INSERT INTO events (event_id, installation_id, producer_sequence, "
            "dedup_key, recorded_at, envelope_json) VALUES (?, ?, ?, ?, ?, ?)",
            (
                record["event_id"],
                record["installation_id"],
                record["producer_sequence"],
                dedup_key,
                record["recorded_at"],
                serialize(record),
            ),
        )

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
