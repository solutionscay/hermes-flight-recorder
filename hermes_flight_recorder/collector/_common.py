"""Shared helpers for the durable-state adapters.

Build producer records (the envelope fields a producer fills in; the
outbox stamps event_id, installation_id, producer_sequence, recorded_at)
and normalize Hermes timestamps.
"""

from __future__ import annotations

import datetime
import json
import os
import sqlite3
from pathlib import Path
from typing import Any


# The namespaced child of a Hermes home that holds one Flight Recorder
# installation's runtime data (outbox, key, config, spool, lock).
FLIGHT_RECORDER_DIR_NAME = "flight-recorder"

# The pre-#101 default location. Retained only so `install` can detect it and
# refuse to silently strand it; nothing writes here any more.
LEGACY_FLIGHT_RECORDER_HOME = ".hermes-flight-recorder"


def resolve_hermes_home(hermes_home: str | Path | None) -> Path:
    """The Hermes data root: explicit arg, then $HERMES_HOME, then ~/.hermes."""
    if hermes_home:
        return Path(hermes_home).expanduser()
    env = os.environ.get("HERMES_HOME")
    return Path(env).expanduser() if env else Path.home() / ".hermes"


def resolve_flight_recorder_home(
    flight_recorder_home: str | Path | None = None,
    hermes_home: str | Path | None = None,
) -> Path:
    """The Flight Recorder data directory, by descending precedence.

    1. ``flight_recorder_home`` — an explicit ``--flight-recorder-home``.
    2. ``$SC_HERMES_FLIGHT_RECORDER_HOME`` — for unusual deployments.
    3. ``<hermes home>/flight-recorder`` — the default: one Hermes home is one
       Flight Recorder installation. The Hermes home resolves via
       :func:`resolve_hermes_home` (``hermes_home`` arg, then ``$HERMES_HOME``,
       then ``~/.hermes``).

    The path is expanded but not resolved, so callers control symlink and
    relative-path resolution.
    """
    if flight_recorder_home:
        return Path(flight_recorder_home).expanduser()
    env = os.environ.get("SC_HERMES_FLIGHT_RECORDER_HOME")
    if env:
        return Path(env).expanduser()
    return resolve_hermes_home(hermes_home) / FLIGHT_RECORDER_DIR_NAME


def default_flight_recorder_home() -> Path:
    """The Flight Recorder data directory with no explicit overrides.

    A thin no-arg wrapper over :func:`resolve_flight_recorder_home` for library
    callers (config readers) that only need the default. Its value is
    ``$SC_HERMES_FLIGHT_RECORDER_HOME`` or ``$HERMES_HOME/flight-recorder``.
    """
    return resolve_flight_recorder_home()


# --- Hermes durable-store layout ----------------------------------------
# The on-disk layout of a Hermes home is external knowledge this package
# does not control. Every path literal lives here, so a Hermes layout
# change is a one-file edit.
def state_db_path(home: Path) -> Path:
    return home / "state.db"


def executions_db_path(home: Path) -> Path:
    return home / "cron" / "executions.db"


def jobs_path(home: Path) -> Path:
    return home / "cron" / "jobs.json"


def ticker_heartbeat_path(home: Path) -> Path:
    return home / "cron" / "ticker_heartbeat"


def ticker_last_success_path(home: Path) -> Path:
    return home / "cron" / "ticker_last_success"


def gateway_state_path(home: Path) -> Path:
    return home / "gateway_state.json"


def gateway_starts_log_path(home: Path) -> Path:
    return home / "gateway-starts.log"


def kanban_db_path(home: Path) -> Path:
    """The legacy top-level board (board slug ``"default"``)."""
    return home / "kanban.db"


def kanban_boards_dir(home: Path) -> Path:
    return home / "kanban" / "boards"


def kanban_board_dbs(home: Path) -> list[tuple[str, Path]]:
    """Every Kanban board as ``(slug, kanban.db path)``.

    Hermes keeps one SQLite file per board under
    ``<home>/kanban/boards/<slug>/kanban.db``, plus a legacy top-level
    ``<home>/kanban.db`` reported as board ``"default"`` and listed first.
    Only existing files are returned, so a home with no Kanban yields ``[]``.
    """
    boards: list[tuple[str, Path]] = []
    legacy = kanban_db_path(home)
    if legacy.exists():
        boards.append(("default", legacy))
    board_dir = kanban_boards_dir(home)
    if board_dir.is_dir():
        for child in sorted(board_dir.iterdir()):
            db = child / "kanban.db"
            if child.is_dir() and db.exists():
                boards.append((child.name, db))
    return boards


def skills_dir(home: Path) -> Path:
    return home / "skills"


def memories_dir(home: Path) -> Path:
    return home / "memories"


def memory_files(home: Path) -> list[tuple[str, Path]]:
    """The two built-in memory artifacts as ``(target, path)``.

    ``target`` is Hermes's own selector: ``"memory"`` → ``MEMORY.md`` and
    ``"user"`` → ``USER.md``. Only existing files are returned.
    """
    md = memories_dir(home)
    out: list[tuple[str, Path]] = []
    for target, name in (("memory", "MEMORY.md"), ("user", "USER.md")):
        path = md / name
        if path.is_file():
            out.append((target, path))
    return out


# The four supporting subdirectories a skill may carry (Hermes
# ``ALLOWED_SUBDIRS``); there is no ``examples/``. SKILL.md sits at the root.
SKILL_SUBDIRS = ("references", "templates", "scripts", "assets")


def _provenance_skill_names(skills: Path) -> set[str]:
    """Skill names that are bundled or Hub-installed (never Hermes-created).

    Provenance lives in two sidecars under ``<skills>/``: ``.bundled_manifest``
    (``name:hash`` per line) and ``.hub/lock.json`` (an ``installed`` map keyed
    by name). Either presence means Hermes did not author the skill.
    """
    names: set[str] = set()
    try:
        for line in (skills / ".bundled_manifest").read_text().splitlines():
            entry = line.strip()
            if entry:
                names.add(entry.split(":", 1)[0].strip())
    except OSError:
        pass
    installed = load_json_dict(skills / ".hub" / "lock.json").get("installed")
    if isinstance(installed, dict):
        names.update(str(name) for name in installed)
    return names


def hermes_created_skills(home: Path) -> list[tuple[str, str | None, Path]]:
    """Hermes-created skills as ``(name, category, skill_dir)``.

    A skill directory holds a ``SKILL.md``, at ``<skills>/<name>/`` or
    ``<skills>/<category>/<name>/``. Bundled and Hub-installed skills are
    excluded by name; dot-prefixed sidecars (``.bundled_manifest``, ``.hub``,
    ``.usage.json``, ``.archive``, …) are skipped. A home with no skills, or
    only out-of-the-box ones, yields ``[]``.
    """
    skills = skills_dir(home)
    if not skills.is_dir():
        return []
    excluded = _provenance_skill_names(skills)
    out: list[tuple[str, str | None, Path]] = []

    def consider(name: str, category: str | None, path: Path) -> None:
        if name not in excluded and (path / "SKILL.md").is_file():
            out.append((name, category, path))

    for child in sorted(skills.iterdir()):
        if not child.is_dir() or child.name.startswith("."):
            continue
        if (child / "SKILL.md").is_file():
            consider(child.name, None, child)
            continue
        for grandchild in sorted(child.iterdir()):  # child is a category dir
            if grandchild.is_dir() and not grandchild.name.startswith("."):
                consider(grandchild.name, child.name, grandchild)
    return out


def to_epoch(value: Any) -> float | None:
    """Normalize a Hermes timestamp to epoch seconds.

    Hermes uses two shapes: epoch floats (state.db) and ISO 8601 strings
    with a timezone (cron executions.db). Return None for a missing value.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return datetime.datetime.fromisoformat(value).timestamp()


def gateway_runtime_stamp(
    channels: Any = None, gateway_id: str | None = None
) -> dict[str, Any]:
    """A runtime stamp for a gateway lifecycle event.

    Enriches the minimal stamp with the gateway's connected ``channels`` (a
    plaintext list of Hermes Platform names such as ``telegram`` / ``discord``
    — never a bot token) and a stable ``gateway_id``. There is no
    gateway-level ``transport`` in Hermes, so none is recorded; the channel
    list is the transport surface. Shared by ``runtime.gateway_started`` and,
    when they are wired, ``gateway_stopped`` / ``gateway_start_failed``.
    """
    stamp = runtime_stamp("gateway")
    stamp["channels"] = list(channels) if channels else []
    if gateway_id is not None:
        stamp["gateway_id"] = gateway_id
    return stamp


def runtime_stamp(kind: str, home_mode: str | None = None) -> dict[str, Any]:
    """A minimal runtime inventory stamp. Best-effort in the POC.

    ``home_mode`` is the Hermes ``terminal.home_mode`` policy (see
    ``read_home_mode``). It is plaintext operational metadata that explains
    where tools run and which git identity they use. Omitted when absent, so
    the field is additive against envelope v1.
    """
    stamp: dict[str, Any] = {"kind": kind, "engine": "standard"}
    if home_mode is not None:
        stamp["home_mode"] = home_mode
    return stamp


def safe_json_dict(text: str | None) -> dict[str, Any]:
    """Parse JSON text into a dict, or return {} on any trouble."""
    try:
        obj = json.loads(text)
    except (ValueError, TypeError):
        return {}
    return obj if isinstance(obj, dict) else {}


def load_json_dict(path: Path) -> dict[str, Any]:
    """Read and parse a JSON file into a dict, or return {} on any trouble."""
    try:
        text = path.read_text()
    except OSError:
        return {}
    return safe_json_dict(text)


def append_and_count(
    outbox: Any,
    counts: dict[str, int],
    record: dict[str, Any],
    *,
    content: str | bytes | None = None,
    dedup_key: str,
) -> None:
    """Append via dedup and count the event type only when a new row landed.

    ``counts`` must tolerate ``+= 1`` on a missing key (a ``defaultdict`` or
    ``Counter``).
    """
    if outbox.append_if_new(record, content=content, dedup_key=dedup_key):
        counts[record["payload"]["event_type"]] += 1


def open_sqlite_read_only(path: Path) -> sqlite3.Connection:
    """Open a SQLite database read-only and return rows keyed by column.

    A ``busy_timeout`` lets a momentary writer lock (Hermes checkpointing its own
    store) wait briefly instead of instantly raising ``SQLITE_BUSY``, which would
    otherwise surface as an ``OperationalError`` mid-poll.
    """
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def read_float(path: Path) -> float | None:
    """Read a float from a small text file, or return None when absent/invalid."""
    if not path.exists():
        return None
    text = path.read_text().strip()
    try:
        return float(text) if text else None
    except ValueError:
        return None


# terminal.home_mode aliases Hermes normalizes to its canonical values.
# See hermes_constants.get_subprocess_home in the Hermes source.
_HOME_MODE_ALIASES = {
    "isolated": "profile",
    "profile_home": "profile",
    "profile-home": "profile",
    "host": "real",
    "user": "real",
    "real_home": "real",
    "real-home": "real",
}
_HOME_MODE_CANONICAL = ("auto", "real", "profile")


def read_home_mode(hermes_home: str | Path | None = None) -> str:
    """The Hermes ``terminal.home_mode`` policy, normalized. Best-effort.

    Reads ``config.yaml`` in the Hermes home and returns the canonical value
    (``auto`` | ``real`` | ``profile``). Returns ``"auto"`` — the Hermes
    default — when the file, the ``terminal`` block, or the key is absent,
    blank, unreadable, or an unrecognized value. Never raises. Captures only
    the enum; never the resolved HOME path (that is sensitive content).
    """
    raw = _read_terminal_home_mode(resolve_hermes_home(hermes_home) / "config.yaml")
    if not raw:
        return "auto"
    mode = raw.strip().lower()
    mode = _HOME_MODE_ALIASES.get(mode, mode)
    return mode if mode in _HOME_MODE_CANONICAL else "auto"


def _read_terminal_home_mode(config_path: Path) -> str | None:
    """The raw ``terminal.home_mode`` string from ``config.yaml``, or None.

    A tiny standard-library scanner — the project keeps its runtime deps to
    ``cryptography`` alone, so it does not import a YAML parser. It finds the
    top-level ``terminal:`` block and reads its ``home_mode:`` child, and it
    reads nothing else, so adjacent secret blocks (bot tokens) are never
    touched. Any read or parse trouble yields None.
    """
    try:
        text = config_path.read_text(encoding="utf-8")
    except OSError:
        return None
    in_terminal = False
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if line[:1] not in (" ", "\t"):  # a top-level key at column 0
            in_terminal = stripped.split(":", 1)[0].strip() == "terminal"
            continue
        if in_terminal and stripped.split(":", 1)[0].strip() == "home_mode":
            value = stripped.partition(":")[2]
            return value.split("#", 1)[0].strip().strip("'\"") or None
    return None


def root_session(session_id: str | None, parent_map: dict[str, str | None]) -> str | None:
    """Walk parent_session_id to the top ancestor (the correlation root)."""
    seen: set[str] = set()
    sid = session_id
    while sid is not None and parent_map.get(sid) and parent_map[sid] not in seen:
        seen.add(sid)
        sid = parent_map[sid]
    return sid


def build_record(
    *,
    event_type: str,
    occurred_at: float,
    source: str,
    capture_method: str,
    runtime: dict[str, Any],
    correlation_id: str,
    payload: dict[str, Any] | None = None,
    session_id: str | None = None,
    session_key: str | None = None,
    parent_session_id: str | None = None,
    invocation_id: str | None = None,
    causation_id: str | None = None,
    tenant_id: str = "default",
    profile: str = "default",
    partial: bool = False,
) -> dict[str, Any]:
    """Assemble a producer record. The outbox stamps the rest."""
    pl = dict(payload or {})
    pl["event_type"] = event_type
    rec: dict[str, Any] = {
        "occurred_at": float(occurred_at),
        "tenant_id": tenant_id,
        "profile": profile or "default",
        "runtime": runtime,
        "correlation_id": correlation_id,
        "source": source,
        "capture_method": capture_method,
        "payload": pl,
        "partial": partial,
    }
    for key, val in (
        ("session_id", session_id),
        ("session_key", session_key),
        ("parent_session_id", parent_session_id),
        ("invocation_id", invocation_id),
        ("causation_id", causation_id),
    ):
        if val is not None:
            rec[key] = val
    return rec
