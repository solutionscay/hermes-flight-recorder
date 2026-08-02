"""Reconstruct knowledge artifact state after a successful mutation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .keystore import has_secret
from .knowledge_store import iter_disk_artifacts

MEMORY_DELIMITER = "\n§\n"


def after_state(
    outbox: Any,
    artifact: dict[str, Any],
    action: str,
    arguments: dict[str, Any],
    result: dict[str, Any],
    *,
    hermes_home: str | Path | None,
) -> dict[str, bytes] | None:
    """Return the complete artifact state after one mutation."""
    if artifact["kind"] == "skill":
        return _skill_after_state(
            outbox,
            artifact["artifact_id"],
            action,
            arguments,
            hermes_home=hermes_home,
        )
    return _memory_after_state(
        outbox,
        artifact,
        action,
        arguments,
        result,
        hermes_home=hermes_home,
    )


def _latest_files(outbox: Any, artifact_id: str) -> dict[str, bytes] | None:
    cached = outbox._knowledge_plaintext.get(artifact_id)
    if cached is not None:
        return dict(cached)
    latest = outbox.latest_knowledge_version(artifact_id)
    if latest is None or latest["is_tombstone"]:
        return None
    if not has_secret(outbox._flight_recorder_home):
        return None
    files = {
        entry["path"]: outbox.get_blob(entry["blob_hash"])
        for entry in latest["manifest"]
    }
    outbox._knowledge_plaintext[artifact_id] = dict(files)
    return files


def _disk_files(
    hermes_home: str | Path | None, artifact_id: str
) -> dict[str, bytes] | None:
    """Read the current after-state when a fleet host cannot decrypt its store."""
    if hermes_home is None:
        return None
    for found_id, _kind, _name, _category, files in iter_disk_artifacts(
        Path(hermes_home)
    ):
        if found_id == artifact_id:
            return {relative: path.read_bytes() for relative, path in files}
    return None


def _skill_after_state(
    outbox: Any,
    artifact_id: str,
    action: str,
    arguments: dict[str, Any],
    *,
    hermes_home: str | Path | None,
) -> dict[str, bytes] | None:
    if action == "delete":
        return {}
    if action == "create":
        return _create_skill(arguments)

    files = _latest_files(outbox, artifact_id)
    if files is None:
        # Hermes has applied the call when the result reaches state.db.
        # The disk snapshot is the complete after-state.
        return _disk_files(hermes_home, artifact_id)
    mutation = _SKILL_FILE_MUTATIONS.get(action)
    return mutation(files, arguments) if mutation is not None else None


def _create_skill(arguments: dict[str, Any]) -> dict[str, bytes] | None:
    content = arguments.get("content")
    return {"SKILL.md": content.encode()} if isinstance(content, str) else None


def _edit_skill(
    files: dict[str, bytes], arguments: dict[str, Any]
) -> dict[str, bytes] | None:
    content = arguments.get("content")
    if not isinstance(content, str):
        return None
    files["SKILL.md"] = content.encode()
    return files


def _write_skill_file(
    files: dict[str, bytes], arguments: dict[str, Any]
) -> dict[str, bytes] | None:
    path = arguments.get("file_path")
    content = arguments.get("file_content")
    if not isinstance(path, str) or not isinstance(content, str):
        return None
    files[path] = content.encode()
    return files


def _remove_skill_file(
    files: dict[str, bytes], arguments: dict[str, Any]
) -> dict[str, bytes] | None:
    path = arguments.get("file_path")
    if not isinstance(path, str) or path not in files:
        return None
    del files[path]
    return files


def _patch_skill_file(
    files: dict[str, bytes], arguments: dict[str, Any]
) -> dict[str, bytes] | None:
    path = arguments.get("file_path") or "SKILL.md"
    old = arguments.get("old_string")
    new = arguments.get("new_string")
    if (
        not isinstance(path, str)
        or path not in files
        or not isinstance(old, str)
        or not isinstance(new, str)
    ):
        return None
    text = files[path].decode("utf-8")
    count = text.count(old)
    replace_all = arguments.get("replace_all") is True
    if count == 0 or (count != 1 and not replace_all):
        return None
    files[path] = (
        text.replace(old, new) if replace_all else text.replace(old, new, 1)
    ).encode()
    return files


_SKILL_FILE_MUTATIONS = {
    "edit": _edit_skill,
    "write_file": _write_skill_file,
    "remove_file": _remove_skill_file,
    "patch": _patch_skill_file,
}


def _memory_after_state(
    outbox: Any,
    artifact: dict[str, Any],
    action: str,
    arguments: dict[str, Any],
    result: dict[str, Any],
    *,
    hermes_home: str | Path | None,
) -> dict[str, bytes] | None:
    filename = "USER.md" if artifact["name"] == "user" else "MEMORY.md"
    latest = outbox.latest_knowledge_version(artifact["artifact_id"])
    files = _latest_files(outbox, artifact["artifact_id"])
    if files is None and latest is not None and not latest["is_tombstone"]:
        return _disk_files(hermes_home, artifact["artifact_id"])
    had_base = files is not None and filename in files
    entries = _memory_entries(files[filename]) if had_base else []

    operations = arguments.get("operations")
    if not isinstance(operations, list) or not operations:
        operations = [
            {
                "action": action,
                "content": arguments.get("content"),
                "old_text": arguments.get("old_text"),
            }
        ]
    updated = _apply_memory_operations(entries, operations)
    if updated is None:
        return None

    if not had_base:
        entry_count = result.get("entry_count")
        if not isinstance(entry_count, int) or entry_count != len(updated):
            return None
    return {filename: MEMORY_DELIMITER.join(updated).encode()}


def _memory_entries(content: bytes) -> list[str]:
    text = content.decode("utf-8")
    return [entry.strip() for entry in text.split(MEMORY_DELIMITER) if entry.strip()]


def _apply_memory_operations(
    entries: list[str], operations: list[dict[str, Any]]
) -> list[str] | None:
    updated = list(entries)
    for operation in operations:
        if not isinstance(operation, dict):
            return None
        action = operation.get("action")
        content = operation.get("content")
        old_text = operation.get("old_text")
        if action == "add":
            if not isinstance(content, str) or not content.strip():
                return None
            content = content.strip()
            if content not in updated:
                updated.append(content)
            continue
        if action not in {"replace", "remove"} or not isinstance(old_text, str):
            return None
        old_text = old_text.strip()
        matches = [index for index, entry in enumerate(updated) if old_text in entry]
        if not matches or len({updated[index] for index in matches}) > 1:
            return None
        index = matches[0]
        if action == "remove":
            updated.pop(index)
        else:
            if not isinstance(content, str) or not content.strip():
                return None
            updated[index] = content.strip()
    return updated
