"""Local observe surface — stream, tree, and report.

Render the captured outbox for a person, with no console and no network.
This is the read side of the Phase 0 POC: it opens the local outbox
read-only and prints what was captured. It never makes a network call and
never decrypts ``content_ciphertext`` — only plaintext payload metadata and
``content_hash`` are shown.

Four views:

- **stream** — every event in (``installation_id``, ``producer_sequence``)
  order, one line each, with the key plaintext payload fields.
- **tree** — the execution tree built from ``correlation_id`` and
  ``parent_session_id``: each root session, its tool-call leaves, and its
  subagent children nested underneath, with token and cost rollups.
- **report** — the reconciler findings (every type in
  :data:`hermes_flight_recorder.envelope.RECONCILE_FINDING_TYPES`). Returns a
  non-zero exit code when any finding exists, zero when clean, so a script
  can gate on it.
- **kanban** — the Kanban task boards built from the reserved ``task.*``
  events: each task grouped by (``board``, ``task_id``) with its latest
  status, current holder and lease state, the per-attempt timeline (each
  ``task.claimed`` paired with its ``task.attempt_ended`` by ``run_id``),
  and the task terminals. So an operator can explain a task's transitions.

The render functions take a plain list of envelope records, so they are
testable without an outbox. ``load()`` is the thin adapter that pulls and
filters records from an :class:`~hermes_flight_recorder.collector.outbox.Outbox`.
"""

from __future__ import annotations

import datetime
from typing import Any, Iterable

from .envelope import (
    RECONCILE_FINDING_TYPES as FINDING_TYPES,
    SESSION_START_TYPES,
    SESSION_TERMINAL_TYPES,
    TASK_EVENT_TYPES,
    TASK_TERMINAL_TYPES,
)

__all__ = [
    "load",
    "render_stream",
    "render_tree",
    "render_report",
    "render_kanban",
    "parse_since",
    "FINDING_TYPES",
]


# --- loading & filtering ------------------------------------------------
def load(
    outbox: Any,
    *,
    session: str | None = None,
    since: float | None = None,
) -> list[dict[str, Any]]:
    """Pull records from the outbox in stream order, applying filters.

    ``session`` keeps the whole operation an id takes part in (matched on
    ``correlation_id``, ``session_id``, or ``parent_session_id``). ``since``
    keeps events at or after an ``occurred_at`` epoch.
    """
    records = list(outbox.iter_events())
    if session is not None:
        records = [r for r in records if _touches_session(r, session)]
    if since is not None:
        records = [r for r in records if _as_float(r.get("occurred_at")) >= since]
    return records


def _touches_session(record: dict[str, Any], session: str) -> bool:
    return session in (
        record.get("correlation_id"),
        record.get("session_id"),
        record.get("parent_session_id"),
    )


def parse_since(value: str) -> float:
    """Parse a --since value: an epoch number or an ISO 8601 timestamp."""
    try:
        return float(value)
    except ValueError:
        pass
    try:
        return datetime.datetime.fromisoformat(value).timestamp()
    except ValueError as exc:
        raise ValueError(f"--since must be an epoch or ISO timestamp: {value!r}") from exc


# --- stream view --------------------------------------------------------
def render_stream(records: Iterable[dict[str, Any]]) -> list[str]:
    """One line per event in producer_sequence order."""
    rows = sorted(records, key=_stream_key)
    lines: list[str] = []
    for r in rows:
        seq = r.get("producer_sequence")
        when = _iso(r.get("occurred_at"))
        et = r.get("payload", {}).get("event_type", "?")
        sid = r.get("session_id") or "-"
        summary = _payload_summary(r)
        line = f"{seq:>6}  {when}  {et:<26}  {sid:<24}  {summary}"
        lines.append(line.rstrip())
    return lines


def _stream_key(r: dict[str, Any]) -> tuple:
    return (r.get("installation_id") or "", r.get("producer_sequence") or 0)


# key plaintext fields to surface per event family, in display order
_SUMMARY_FIELDS: dict[str, tuple[str, ...]] = {
    "tool.call_completed": ("tool_name", "status", "effect_disposition"),
    "model.usage_recorded": ("model", "input_tokens", "output_tokens", "estimated_cost_usd"),
    "session.created": ("kind", "model"),
    "session.ended": ("kind", "end_reason", "input_tokens", "output_tokens", "estimated_cost_usd"),
    "subagent.child_spawned": ("kind", "model"),
    "subagent.completed": ("kind", "end_reason"),
    "delegation.dispatched": ("delegation_id", "state", "is_batch"),
    "cron.run_claimed": ("job_id", "status"),
    "cron.run_finished": ("job_id", "status", "ok"),
    "cron.run_missed": ("job_id", "expected_fire_at", "missed_count"),
    "cron.ticker_heartbeat": ("heartbeat", "last_success"),
    "reconcile.gap_detected": ("gap_kind", "subject_type", "subject_id", "missing_sequence"),
    "reconcile.terminal_missing": ("subject_type", "subject_id", "expected_terminal_event_type"),
    "runtime.gateway_start_failed": ("reason_class", "gateway_state", "platform"),
}


def _payload_summary(record: dict[str, Any]) -> str:
    payload = record.get("payload", {})
    et = payload.get("event_type", "")
    fields = _SUMMARY_FIELDS.get(et)
    if fields is None:
        # Fallback: the first few plaintext keys, event_type aside.
        items = [(k, v) for k, v in payload.items() if k != "event_type"][:4]
    else:
        items = [(k, payload[k]) for k in fields if k in payload and payload[k] is not None]
    parts = [f"{k}={_short(v)}" for k, v in items]
    if record.get("content_hash"):
        parts.append(f"hash={record['content_hash'][:14]}…")
    if record.get("partial"):
        parts.append("partial")
    return " ".join(parts)


def _short(value: Any) -> str:
    if isinstance(value, float):
        # Epochs and costs both live here; keep epochs readable, costs exact-ish.
        if value > 1_000_000_000:
            return _iso(value)
        return f"{value:.4f}".rstrip("0").rstrip(".")
    text = str(value)
    return text if len(text) <= 40 else text[:39] + "…"


# --- tree view ----------------------------------------------------------
def render_tree(records: Iterable[dict[str, Any]], *, session: str | None = None) -> list[str]:
    """The execution tree: root sessions, tool leaves, subagent children."""
    idx = _Index(list(records))
    roots = idx.roots(session)
    lines: list[str] = []
    # One shared seen-set across all roots: each session has a single parent,
    # so it belongs to one subtree, and the guard makes a malformed
    # parent_session_id cycle terminate instead of recursing forever.
    seen: set[str] = set()
    for i, sid in enumerate(roots):
        if i:
            lines.append("")
        _render_session(sid, idx, 0, lines, is_root=True, seen=seen)
    if not lines:
        lines.append("(no sessions captured)")
    return lines


def _render_session(
    sid: str, idx: "_Index", depth: int, lines: list[str], *, is_root: bool, seen: set[str]
) -> None:
    if sid in seen:
        return  # a cycle or a re-parented node already rendered
    seen.add(sid)
    node = idx.sessions.get(sid, {})
    pad = "  " * depth
    kind = node.get("kind", "session")
    status = node.get("status", "open")
    own = idx.own_tokens(sid)
    marker = "●" if is_root else "○"
    header = (
        f"{pad}{marker} {kind} {sid}  [{status}]  "
        f"tokens={own[0]}/{own[1]}  cost=${own[2]:.4f}"
    )
    if is_root:
        sub = idx.subtree_tokens(sid)
        header += f"  (subtree tokens={sub[0]}/{sub[1]} cost=${sub[2]:.4f})"
    lines.append(header.rstrip())

    for inv in idx.invocations.get(sid, []):
        st = "done" if inv.get("completed") else "open"
        lines.append(f"{pad}    ▸ invocation {inv['invocation_id']} [{st}]")

    for t in idx.tools.get(sid, []):
        p = t.get("payload", {})
        lines.append(
            f"{pad}    ├─ tool {p.get('tool_name', '?')} "
            f"[{p.get('status', '?')}]"
        )

    for child in idx.children.get(sid, []):
        _render_session(child, idx, depth + 1, lines, is_root=False, seen=seen)


class _Index:
    """Group records into sessions, tools, usage, and lineage edges."""

    def __init__(self, records: list[dict[str, Any]]):
        self.sessions: dict[str, dict[str, Any]] = {}
        self.children: dict[str, list[str]] = {}
        self.tools: dict[str, list[dict[str, Any]]] = {}
        self.usage: dict[str, list[dict[str, Any]]] = {}
        self.invocations: dict[str, list[dict[str, Any]]] = {}
        self._invocation_seen: dict[str, dict[str, Any]] = {}
        self._build(records)

    def _build(self, records: list[dict[str, Any]]) -> None:
        for r in records:
            et = r.get("payload", {}).get("event_type")
            sid = r.get("session_id")
            if et in SESSION_START_TYPES:
                self._ensure_session(sid, r)
            elif et in SESSION_TERMINAL_TYPES:
                node = self._ensure_session(sid, r)
                node["status"] = r["payload"].get("end_reason") or "ended"
                node["ended"] = r
            elif et == "tool.call_completed":
                self.tools.setdefault(sid, []).append(r)
            elif et == "model.usage_recorded":
                self.usage.setdefault(sid, []).append(r)
            elif et in ("invocation.started", "invocation.completed"):
                self._track_invocation(r, et)

    def _ensure_session(self, sid: str | None, r: dict[str, Any]) -> dict[str, Any]:
        if sid is None:
            return {}
        node = self.sessions.get(sid)
        parent = r.get("parent_session_id")
        if node is None:
            kind = r.get("payload", {}).get("kind") or "session"
            node = {"session_id": sid, "kind": kind, "status": "open", "parent": parent}
            self.sessions[sid] = node
            if parent is not None:
                self.children.setdefault(parent, []).append(sid)
        elif parent is not None and node.get("parent") is None:
            node["parent"] = parent
            self.children.setdefault(parent, []).append(sid)
        return node

    def _track_invocation(self, r: dict[str, Any], et: str) -> None:
        inv = r.get("invocation_id")
        sid = r.get("session_id")
        if inv is None or sid is None:
            return
        rec = self._invocation_seen.get(inv)
        if rec is None:
            rec = {"invocation_id": inv, "completed": False}
            self._invocation_seen[inv] = rec
            self.invocations.setdefault(sid, []).append(rec)
        if et == "invocation.completed":
            rec["completed"] = True

    def roots(self, session: str | None) -> list[str]:
        if session is not None:
            return [session] if session in self.sessions else []
        roots = [
            sid for sid, node in self.sessions.items()
            if node.get("parent") is None or node["parent"] not in self.sessions
        ]
        return sorted(roots)

    def own_tokens(self, sid: str) -> tuple[int, int, float]:
        """(input, output, cost) for one session, from its ended row or usage."""
        node = self.sessions.get(sid, {})
        ended = node.get("ended")
        if ended is not None:
            p = ended["payload"]
            return (
                int(p.get("input_tokens") or 0),
                int(p.get("output_tokens") or 0),
                float(p.get("estimated_cost_usd") or 0.0),
            )
        tin = tout = 0
        cost = 0.0
        for u in self.usage.get(sid, []):
            p = u["payload"]
            tin += int(p.get("input_tokens") or 0)
            tout += int(p.get("output_tokens") or 0)
            cost += float(p.get("estimated_cost_usd") or 0.0)
        return tin, tout, cost

    def subtree_tokens(self, sid: str, _seen: set[str] | None = None) -> tuple[int, int, float]:
        if _seen is None:
            _seen = set()
        if sid in _seen:  # a malformed parent cycle — stop, don't double-count
            return (0, 0, 0.0)
        _seen.add(sid)
        tin, tout, cost = self.own_tokens(sid)
        for child in self.children.get(sid, []):
            cin, cout, ccost = self.subtree_tokens(child, _seen)
            tin, tout, cost = tin + cin, tout + cout, cost + ccost
        return tin, tout, cost


# --- report view --------------------------------------------------------
def render_report(records: Iterable[dict[str, Any]]) -> tuple[list[str], int]:
    """List reconciler findings. Returns (lines, exit_code)."""
    findings = [
        r for r in records
        if r.get("payload", {}).get("event_type") in FINDING_TYPES
    ]
    if not findings:
        return (["clean: no gaps, missing terminals, or missed cron runs"], 0)

    findings.sort(key=_stream_key)
    by_type: dict[str, int] = {}
    lines = [f"{len(findings)} finding(s):", ""]
    for r in findings:
        p = r["payload"]
        et = p["event_type"]
        by_type[et] = by_type.get(et, 0) + 1
        lines.append(f"  {et:<28}  {_finding_detail(r)}")
    lines.append("")
    lines.append("summary: " + ", ".join(f"{k}={v}" for k, v in sorted(by_type.items())))
    return (lines, 1)


def _finding_detail(record: dict[str, Any]) -> str:
    p = record["payload"]
    et = p["event_type"]
    if et == "reconcile.gap_detected":
        if p.get("gap_kind") == "sequence":
            return f"sequence gap: missing #{p.get('missing_sequence')} (between {p.get('prev_sequence')} and {p.get('next_sequence')})"
        return f"uncaptured {p.get('subject_type')}: {p.get('subject_id')} ({p.get('source_table')})"
    if et == "reconcile.terminal_missing":
        age = p.get("age_seconds") or p.get("staleness_seconds")
        age_s = f", ~{int(age)}s past window" if age else ""
        return f"{p.get('subject_type')} {p.get('subject_id')} has no {p.get('expected_terminal_event_type')}{age_s}"
    if et == "cron.run_missed":
        return f"job {p.get('job_id')} missed {p.get('missed_count')} fire(s) from {_iso(p.get('expected_fire_at'))}"
    if et == "runtime.gateway_start_failed":
        target = p.get("platform") or "gateway"
        return f"{target} failed to start: {p.get('reason_class')}"
    if et == "reconcile.capture_stale":
        age = p.get("staleness_seconds")
        age_s = f", ~{int(age)}s stale" if age else ""
        return f"capture loop stalled: last success {_iso(p.get('last_success_at'))}{age_s}"
    return str(p)


# --- kanban view --------------------------------------------------------
def render_kanban(records: Iterable[dict[str, Any]]) -> list[str]:
    """Per-task board view: status, current lease, and the attempt timeline."""
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in records:
        p = r.get("payload", {})
        if p.get("event_type") not in TASK_EVENT_TYPES:
            continue
        key = (p.get("board") or "-", p.get("task_id") or "-")
        groups.setdefault(key, []).append(r)

    if not groups:
        return ["(no kanban tasks captured)"]

    lines: list[str] = []
    for i, key in enumerate(sorted(groups)):
        if i:
            lines.append("")
        _render_task(key, groups[key], lines)
    return lines


def _render_task(
    key: tuple[str, str], records: list[dict[str, Any]], lines: list[str]
) -> None:
    board, task_id = key
    rows = sorted(records, key=_stream_key)  # newest last, per producer_sequence

    # Pair each attempt's claim with its end by run_id, and remember when each
    # run first appears so the timeline reads oldest-first even though run ids
    # are not chronological (a reclaimed early attempt can carry a higher id).
    claims: dict[Any, dict[str, Any]] = {}
    ends: dict[Any, dict[str, Any]] = {}
    run_time: dict[Any, float] = {}
    for r in rows:
        p = r["payload"]
        rid = p.get("run_id")
        if rid is not None:
            t = _as_float(r.get("occurred_at"))
            if rid not in run_time or t < run_time[rid]:
                run_time[rid] = t
        if p["event_type"] == "task.claimed" and rid is not None:
            claims[rid] = p
        elif p["event_type"] == "task.attempt_ended":
            ends[rid] = p

    # Latest status: the tasks.status snapshot on the newest task-level event
    # (rows are ascending, so the last non-attempt event is the newest).
    status = next(
        (
            r["payload"].get("status") or "?"
            for r in reversed(rows)
            if r["payload"]["event_type"] != "task.attempt_ended"
        ),
        "?",
    )
    lines.append(f"▣ task {task_id}  [{status}]  board={board}")

    # Current holder + lease: the newest event carrying a holder. Attempt events
    # are appended in run-id order, not chronological order, so pick by
    # occurred_at (tie-broken by stream position), not the last row. It is still
    # held only when that event is an open claim (no attempt_ended for its run).
    holder_ev = max(
        (r for r in rows if r["payload"].get("holder") is not None),
        key=lambda r: (_as_float(r.get("occurred_at")), _stream_key(r)),
        default=None,
    )
    if holder_ev is not None:
        p = holder_ev["payload"]
        rid = p.get("run_id")
        held = p["event_type"] == "task.claimed" and rid not in ends
        expires = p.get("claim_expires")
        lease = f"  expires={expires}" if expires is not None else ""
        lines.append(f"    holder {p['holder']}  [{'held' if held else 'released'}]{lease}")

    run_ids = sorted(set(claims) | set(ends), key=lambda rid: (run_time.get(rid, 0.0), rid))
    if run_ids:
        lines.append("    attempts:")
        for rid in run_ids:
            end = ends.get(rid)
            holder = (end or claims.get(rid) or {}).get("holder", "?")
            if end is not None:
                outcome = f"{end.get('run_outcome', '?')}/{end.get('attempt_disposition', '?')}"
            else:
                outcome = "running"
            lines.append(f"      run {rid}  {holder}  {outcome}")

    terminals = [r for r in rows if r["payload"]["event_type"] in TASK_TERMINAL_TYPES]
    if terminals:
        lines.append("    terminals:")
        for t in terminals:
            lines.append(f"      {_task_terminal_detail(t['payload'])}")


def _task_terminal_detail(payload: dict[str, Any]) -> str:
    """One task terminal line: the event, plus the raw kind that explains it."""
    et = payload["event_type"]
    kind = payload.get("hermes_event_kind")
    if et == "task.blocked" and payload.get("block_kind"):
        return f"{et}  ({kind}, block_kind={payload['block_kind']})"
    if kind:
        return f"{et}  ({kind})"
    return et


# --- shared helpers -----------------------------------------------------
def _iso(epoch: Any) -> str:
    f = _as_float(epoch)
    if f <= 0:
        return "-"
    return datetime.datetime.fromtimestamp(f, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
