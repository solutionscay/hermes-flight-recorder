"""Durable local outbox.

The outbox is the append-only local SQLite store and the single
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

The outbox database must never live under ``HERMES_HOME``.
"""

from __future__ import annotations

import base64
import hashlib
import os
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Iterator

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from ..envelope import SCHEMA_VERSION, parse, serialize, validate
from ._common import default_bridge_home, resolve_hermes_home

__all__ = ["OUTBOX_SCHEMA_VERSION", "Outbox", "OutboxError", "default_bridge_home"]

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
"""


class OutboxError(RuntimeError):
    pass


class Outbox:
    """Append-only local event store and sequence authority."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._bridge_home = self.path.parent
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
    def open(cls, bridge_home: str | os.PathLike[str] | None = None) -> "Outbox":
        home = Path(bridge_home).expanduser() if bridge_home else default_bridge_home()
        home = home.resolve()
        path = home / "outbox.sqlite"
        hermes = resolve_hermes_home(None).resolve()
        if path.is_relative_to(hermes):
            raise OutboxError(
                f"refusing to place the outbox under HERMES_HOME ({hermes}); "
                f"set BRIDGE_HOME to a directory outside the Hermes home"
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
        return self._bridge_home / "content-dev.key"

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
