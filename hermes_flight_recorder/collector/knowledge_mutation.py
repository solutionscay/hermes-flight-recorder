"""Capture successful foreground knowledge mutations from Hermes tool calls."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ._common import build_record, runtime_stamp
from .knowledge_after_state import after_state as _after_state
from .knowledge_store import (
    _apply_retention,
    limit_plaintext_files,
    log_skipped_files,
)

SKILL_ACTIONS = {
    "create",
    "edit",
    "patch",
    "delete",
    "write_file",
    "remove_file",
}
MEMORY_ACTIONS = {"add", "replace", "remove"}


def capture(
    outbox: Any,
    *,
    tool_name: str,
    arguments_text: str,
    arguments: dict[str, Any],
    result: dict[str, Any],
    assistant_row_id: int,
    tool_result_row_id: int,
    tool_call_id: str,
    occurred_at: float,
    home_mode: str,
    correlation_id: str,
    session_id: str,
    invocation_id: str | None,
    profile: str,
    knowledge_config: Any,
    hermes_home: str | Path | None = None,
) -> dict[str, int]:
    """Record one paired and successful ``skill_manage`` or ``memory`` call."""
    if knowledge_config is None:
        from .recorder_config import KnowledgeConfig

        knowledge_config = KnowledgeConfig()
    if result.get("success") is not True or result.get("staged") is True:
        return {}

    classified = _classify(tool_name, arguments, result, outbox)
    if classified is None:
        return {}
    artifact, action = classified
    dedup_key = f"state.db:knowledge:{assistant_row_id}:{tool_call_id}"

    existing = outbox.event_by_dedup_key(dedup_key)
    if existing is not None:
        _restore_link_from_event(outbox, existing)
        return _capture_missing_compaction(
            outbox,
            arguments_text=arguments_text,
            arguments=arguments,
            artifact=artifact,
            action=action,
            assistant_row_id=assistant_row_id,
            tool_call_id=tool_call_id,
            occurred_at=occurred_at,
            home_mode=home_mode,
            correlation_id=correlation_id,
            session_id=session_id,
            invocation_id=invocation_id,
            profile=profile,
            causation_id=existing["event_id"],
        )
    if outbox.has_dedup_key(dedup_key):
        return {}

    version, version_files = _record_version(
        outbox,
        artifact=artifact,
        action=action,
        arguments=arguments,
        result=result,
        occurred_at=occurred_at,
        knowledge_config=knowledge_config,
        hermes_home=hermes_home,
    )
    payload = _event_payload(
        outbox=outbox,
        artifact=artifact,
        action=action,
        assistant_row_id=assistant_row_id,
        tool_result_row_id=tool_result_row_id,
        tool_call_id=tool_call_id,
        version=version,
        version_files=version_files,
        arguments=arguments,
    )
    record = build_record(
        event_type="knowledge.record_written",
        occurred_at=occurred_at,
        source="state.db:messages",
        capture_method="poll:state.db:messages",
        runtime=runtime_stamp("knowledge", home_mode=home_mode),
        correlation_id=correlation_id,
        session_id=session_id,
        invocation_id=invocation_id,
        profile=profile,
        payload=payload,
        partial=bool(version and version.get("skipped_files")),
    )
    stored = outbox.append(record, content=arguments_text, dedup_key=dedup_key)
    counts = {"knowledge.record_written": 1}

    if version is not None:
        outbox.set_knowledge_version_attribution(
            artifact["artifact_id"],
            version["seq"],
            origin="foreground",
            linked_event_id=stored["event_id"],
        )
        outbox.set_meta(
            f"knowledge:emitted:{artifact['artifact_id']}", str(version["seq"])
        )

    absorbed_into = arguments.get("absorbed_into")
    if (
        tool_name == "skill_manage"
        and action == "delete"
        and isinstance(absorbed_into, str)
        and absorbed_into
    ):
        compacted = _record_compaction(
            outbox,
            arguments_text=arguments_text,
            source_artifact_id=artifact["artifact_id"],
            target_name=absorbed_into,
            assistant_row_id=assistant_row_id,
            tool_call_id=tool_call_id,
            occurred_at=occurred_at,
            home_mode=home_mode,
            correlation_id=correlation_id,
            session_id=session_id,
            invocation_id=invocation_id,
            profile=profile,
            causation_id=stored["event_id"],
        )
        if compacted:
            counts["knowledge.record_compacted"] = 1
    return counts


def _capture_missing_compaction(
    outbox: Any,
    *,
    arguments_text: str,
    arguments: dict[str, Any],
    artifact: dict[str, Any],
    action: str,
    assistant_row_id: int,
    tool_call_id: str,
    occurred_at: float,
    home_mode: str,
    correlation_id: str,
    session_id: str,
    invocation_id: str | None,
    profile: str,
    causation_id: str,
) -> dict[str, int]:
    absorbed_into = arguments.get("absorbed_into")
    if (
        artifact["kind"] != "skill"
        or action != "delete"
        or not isinstance(absorbed_into, str)
        or not absorbed_into
    ):
        return {}
    created = _record_compaction(
        outbox,
        arguments_text=arguments_text,
        source_artifact_id=artifact["artifact_id"],
        target_name=absorbed_into,
        assistant_row_id=assistant_row_id,
        tool_call_id=tool_call_id,
        occurred_at=occurred_at,
        home_mode=home_mode,
        correlation_id=correlation_id,
        session_id=session_id,
        invocation_id=invocation_id,
        profile=profile,
        causation_id=causation_id,
    )
    return {"knowledge.record_compacted": 1} if created else {}


def _classify(
    tool_name: str,
    arguments: dict[str, Any],
    result: dict[str, Any],
    outbox: Any,
) -> tuple[dict[str, Any], str] | None:
    if tool_name == "skill_manage":
        action = arguments.get("action")
        name = arguments.get("name")
        if action not in SKILL_ACTIONS or not isinstance(name, str) or not name:
            return None
        category = arguments.get("category")
        if not isinstance(category, str) or not category:
            category = result.get("category")
        if not isinstance(category, str) or not category:
            category = None
        artifact_id = _skill_artifact_id(outbox, name, category)
        existing = outbox.knowledge_artifact(artifact_id)
        if existing is not None:
            category = existing["category"]
        return (
            {
                "artifact_id": artifact_id,
                "kind": "skill",
                "name": name,
                "category": category,
            },
            action,
        )

    if tool_name == "memory":
        target = arguments.get("target")
        if target is None:
            target = "memory"
        operations = arguments.get("operations")
        if isinstance(operations, list) and operations:
            if any(
                not isinstance(op, dict) or op.get("action") not in MEMORY_ACTIONS
                for op in operations
            ):
                return None
            action = "batch"
        else:
            action = arguments.get("action")
            if action not in MEMORY_ACTIONS:
                return None
        if target not in {"memory", "user"}:
            return None
        return (
            {
                "artifact_id": f"memory:{target}",
                "kind": "user_profile" if target == "user" else "memory",
                "name": target,
                "category": None,
            },
            action,
        )
    return None


def _skill_artifact_id(outbox: Any, name: str, category: str | None) -> str:
    """Use the stored category when a later skill call omits it."""
    if category:
        return f"skill:{category}/{name}"
    matches: list[dict[str, Any]] = []
    for artifact_id in outbox.knowledge_artifact_ids():
        artifact = outbox.knowledge_artifact(artifact_id)
        if artifact and artifact["kind"] == "skill" and artifact["name"] == name:
            latest = outbox.latest_knowledge_version(artifact_id)
            artifact["is_tombstone"] = bool(
                latest is not None and latest["is_tombstone"]
            )
            matches.append(artifact)
    live = [artifact for artifact in matches if not artifact["is_tombstone"]]
    candidates = live or matches
    if candidates:
        return max(candidates, key=lambda item: item["first_seen"])["artifact_id"]
    return f"skill:{name}"


def _record_version(
    outbox: Any,
    *,
    artifact: dict[str, Any],
    action: str,
    arguments: dict[str, Any],
    result: dict[str, Any],
    occurred_at: float,
    knowledge_config: Any,
    hermes_home: str | Path | None,
) -> tuple[dict[str, Any] | None, dict[str, bytes] | None]:
    files = _after_state(
        outbox,
        artifact,
        action,
        arguments,
        result,
        hermes_home=hermes_home,
    )
    if files is None:
        return None, None

    files, skipped_files = limit_plaintext_files(
        knowledge_config,
        artifact["artifact_id"],
        files,
    )

    manifest = [
        {"path": path, "blob_hash": outbox.put_blob(content)}
        for path, content in sorted(files.items())
    ]
    is_tombstone = action == "delete"
    outbox.upsert_knowledge_artifact(
        artifact["artifact_id"],
        kind=artifact["kind"],
        name=artifact["name"],
        category=artifact["category"],
        provenance="agent",
        first_seen=occurred_at,
    )
    seq, created = outbox.append_knowledge_version(
        artifact["artifact_id"],
        manifest=manifest,
        occurred_at=occurred_at,
        origin="foreground",
        is_tombstone=is_tombstone,
        skipped_files=skipped_files,
    )
    if created:
        log_skipped_files(artifact["artifact_id"], skipped_files)
    _apply_retention(outbox, knowledge_config, artifact["artifact_id"])
    version = next(
        (
            item
            for item in outbox.knowledge_versions(artifact["artifact_id"])
            if item["seq"] == seq
        ),
        None,
    )
    if is_tombstone:
        outbox.invalidate_knowledge_plaintext(artifact["artifact_id"])
    else:
        outbox.cache_knowledge_plaintext(artifact["artifact_id"], files)
    return version, files


def _event_payload(
    *,
    outbox: Any,
    artifact: dict[str, Any],
    action: str,
    assistant_row_id: int,
    tool_result_row_id: int,
    tool_call_id: str,
    version: dict[str, Any] | None,
    version_files: dict[str, bytes] | None,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "artifact_kind": artifact["kind"],
        "action": action,
        "artifact_id": artifact["artifact_id"],
        "origin": "foreground",
        "provenance": "agent",
        "message_row_id": assistant_row_id,
        "tool_result_row_id": tool_result_row_id,
        "tool_call_id": tool_call_id,
    }
    if artifact["kind"] == "skill":
        payload["skill_name"] = artifact["name"]
        if artifact["category"]:
            payload["category"] = artifact["category"]
    else:
        payload["target"] = artifact["name"]
        operations = arguments.get("operations")
        if isinstance(operations, list) and operations:
            payload["operation_count"] = len(operations)
            payload["operation_actions"] = [
                op.get("action") for op in operations if isinstance(op, dict)
            ]

    if version is not None:
        byte_count = sum(len(content) for content in (version_files or {}).values())
        payload.update(
            {
                "artifact_version_ref": (
                    f"{artifact['artifact_id']}:v{version['seq']}"
                ),
                "version_seq": version["seq"],
                "manifest_hash": version["manifest_hash"],
                "content_hash": version["manifest_hash"],
                "file_count": len(version["manifest"]),
                "byte_count": byte_count,
            }
        )
        skipped_files = version.get("skipped_files", [])
        payload["skipped_file_count"] = len(skipped_files)
        if skipped_files:
            payload["skipped_files"] = skipped_files
    return payload


def _restore_link_from_event(outbox: Any, event: dict[str, Any]) -> None:
    payload = event.get("payload", {})
    artifact_id = payload.get("artifact_id")
    seq = payload.get("version_seq")
    if isinstance(artifact_id, str) and isinstance(seq, int):
        outbox.set_knowledge_version_attribution(
            artifact_id,
            seq,
            origin="foreground",
            linked_event_id=event["event_id"],
        )
        outbox.set_meta(f"knowledge:emitted:{artifact_id}", str(seq))


def _record_compaction(
    outbox: Any,
    *,
    arguments_text: str,
    source_artifact_id: str,
    target_name: str,
    assistant_row_id: int,
    tool_call_id: str,
    occurred_at: float,
    home_mode: str,
    correlation_id: str,
    session_id: str,
    invocation_id: str | None,
    profile: str,
    causation_id: str,
) -> bool:
    target_artifact_id = _skill_artifact_id(outbox, target_name, None)
    record = build_record(
        event_type="knowledge.record_compacted",
        occurred_at=occurred_at,
        source="state.db:messages",
        capture_method="poll:state.db:messages",
        runtime=runtime_stamp("knowledge", home_mode=home_mode),
        correlation_id=correlation_id,
        session_id=session_id,
        invocation_id=invocation_id,
        causation_id=causation_id,
        profile=profile,
        payload={
            "artifact_kind": "skill",
            "action": "compact",
            "source_artifact_id": source_artifact_id,
            "target_artifact_id": target_artifact_id,
            "absorbed_into": target_name,
            "origin": "foreground",
            "provenance": "agent",
            "message_row_id": assistant_row_id,
            "tool_call_id": tool_call_id,
        },
    )
    return outbox.append_if_new(
        record,
        content=arguments_text,
        dedup_key=f"state.db:knowledge-compacted:{assistant_row_id}:{tool_call_id}",
    )
