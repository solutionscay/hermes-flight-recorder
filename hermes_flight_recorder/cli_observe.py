"""Implementation of the observe CLI command."""

from __future__ import annotations

import sys

from . import observe


def run(args, *, open_outbox, check_initialized) -> int:
    since = _parse_since(args.since)
    if args.since is not None and since is None:
        return 2

    outbox = open_outbox(args)
    try:
        if not check_initialized(outbox):
            return 2
        views = _selected_views(args)
        if "security" in views:
            from .collector import recorder_config

            try:
                recorder_config.load(outbox.flight_recorder_home)
            except recorder_config.RecorderConfigError as exc:
                print(f"security report configuration failed: {exc}", file=sys.stderr)
                return 2
        records = observe.load(outbox, session=args.session, since=since)
        exit_code = 0
        for index, view in enumerate(views):
            if index:
                print()
            try:
                code = _print_view(view, outbox, records, args.session)
            except observe.SecurityReportError as exc:
                print(str(exc), file=sys.stderr)
                return 2
            if code is not None:
                exit_code = max(exit_code, code)
        return exit_code
    finally:
        outbox.close()


def _parse_since(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return observe.parse_since(value)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return None


def _selected_views(args) -> list[str]:
    selected = [
        view
        for view in ("stream", "tree", "report", "kanban", "knowledge", "security")
        if getattr(args, view)
    ]
    return selected or ["stream"]


def _print_view(view, outbox, records, session) -> int | None:
    if view == "stream":
        print(f"── stream ({len(records)} events) ──")
        lines = observe.render_stream(records)
    elif view == "tree":
        print("── tree ──")
        lines = observe.render_tree(records, session=session)
    elif view == "report":
        print("── report ──")
        lines, exit_code = observe.render_report(records)
        _print_lines(lines)
        return exit_code
    elif view == "kanban":
        print("── kanban ──")
        lines = observe.render_kanban(records)
    elif view == "security":
        print("── security ──")
        lines, exit_code = observe.render_security(outbox, records)
        _print_lines(lines)
        return exit_code
    else:
        print("── knowledge ──")
        lines = observe.render_knowledge(outbox, records)
    _print_lines(lines)
    return None


def _print_lines(lines) -> None:
    for line in lines:
        print(line)


__all__ = ["run"]
