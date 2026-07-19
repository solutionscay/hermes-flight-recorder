"""Command-line entry point for the Bridge companion.

Subcommands land across the Phase 0 steps. ``init`` creates the local
outbox and mints the installation identity. ``run`` polls the durable
stores into the outbox. ``reconcile`` diffs the durable stores against the
captured outbox and emits reconcile findings. ``observe`` renders the
captured outbox locally (stream, tree, report) with no network.
"""

from __future__ import annotations

import argparse
import sys

from . import __version__


def _cmd_init(args: argparse.Namespace) -> int:
    # Imported lazily so `hermes-dbass --version` needs no heavy deps.
    from .collector.outbox import Outbox

    outbox = Outbox.open(args.bridge_home)
    try:
        installation_id = outbox.initialize()
        print(f"outbox:          {outbox.path}")
        print(f"installation_id: {installation_id}")
    finally:
        outbox.close()
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    from .collector import cron_db, state_db
    from .collector.outbox import Outbox, OutboxError

    outbox = Outbox.open(args.bridge_home)
    try:
        try:
            outbox.installation_id  # fails if not initialized
        except OutboxError:
            print("outbox not initialized; run `hermes-dbass init` first", file=sys.stderr)
            return 2

        totals: dict[str, int] = {}
        for label, poll in (("state.db", state_db.poll), ("cron", cron_db.poll)):
            try:
                for event_type, n in poll(outbox, args.hermes_home).items():
                    totals[event_type] = totals.get(event_type, 0) + n
            except FileNotFoundError as exc:
                print(f"  ({label}: {exc})", file=sys.stderr)

        print(f"polled {sum(totals.values())} events into {outbox.path}:")
        for event_type in sorted(totals):
            print(f"  {event_type}: {totals[event_type]}")
    finally:
        outbox.close()
    return 0


def _cmd_reconcile(args: argparse.Namespace) -> int:
    from .collector.outbox import Outbox, OutboxError
    from .collector.reconcile import reconcile

    outbox = Outbox.open(args.bridge_home)
    try:
        try:
            outbox.installation_id  # fails if not initialized
        except OutboxError:
            print("outbox not initialized; run `hermes-dbass init` first", file=sys.stderr)
            return 2

        counts = reconcile(outbox, args.hermes_home)
        total = sum(counts.values())
        print(f"reconciled {total} new finding(s) into {outbox.path}:")
        for event_type in sorted(counts):
            print(f"  {event_type}: {counts[event_type]}")
    finally:
        outbox.close()
    return 0


def _cmd_observe(args: argparse.Namespace) -> int:
    from . import observe
    from .collector.outbox import Outbox, OutboxError

    since: float | None = None
    if args.since is not None:
        try:
            since = observe.parse_since(args.since)
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 2

    outbox = Outbox.open(args.bridge_home)
    try:
        try:
            outbox.installation_id  # fails if not initialized
        except OutboxError:
            print("outbox not initialized; run `hermes-dbass init` first", file=sys.stderr)
            return 2

        records = observe.load(outbox, session=args.session, since=since)

        # Default to the stream view when no view is selected.
        views = [v for v in ("stream", "tree", "report") if getattr(args, v)]
        if not views:
            views = ["stream"]

        exit_code = 0
        for i, view in enumerate(views):
            if i:
                print()
            if view == "stream":
                print(f"── stream ({len(records)} events) ──")
                for line in observe.render_stream(records):
                    print(line)
            elif view == "tree":
                print("── tree ──")
                for line in observe.render_tree(records, session=args.session):
                    print(line)
            elif view == "report":
                print("── report ──")
                lines, code = observe.render_report(records)
                for line in lines:
                    print(line)
                exit_code = code
    finally:
        outbox.close()
    return exit_code


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hermes-dbass",
        description="Bridge — the local-first companion for Hermes DBaaS.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"hermes-dbass {__version__}",
    )
    sub = parser.add_subparsers(dest="command")

    p_init = sub.add_parser(
        "init", help="Create the local outbox and mint the installation id."
    )
    p_init.add_argument(
        "--bridge-home",
        default=None,
        help="Bridge data directory (default: $BRIDGE_HOME or ~/.hermes-dbass).",
    )
    p_init.set_defaults(func=_cmd_init)

    p_run = sub.add_parser(
        "run", help="Poll state.db and the cron store into the outbox (one pass)."
    )
    p_run.add_argument(
        "--bridge-home",
        default=None,
        help="Bridge data directory (default: $BRIDGE_HOME or ~/.hermes-dbass).",
    )
    p_run.add_argument(
        "--hermes-home",
        default=None,
        help="Hermes data root to read (default: $HERMES_HOME or ~/.hermes).",
    )
    p_run.set_defaults(func=_cmd_run)

    p_rec = sub.add_parser(
        "reconcile",
        help="Diff the durable stores against the outbox and emit reconcile findings.",
    )
    p_rec.add_argument(
        "--bridge-home",
        default=None,
        help="Bridge data directory (default: $BRIDGE_HOME or ~/.hermes-dbass).",
    )
    p_rec.add_argument(
        "--hermes-home",
        default=None,
        help="Hermes data root to read (default: $HERMES_HOME or ~/.hermes).",
    )
    p_rec.set_defaults(func=_cmd_reconcile)

    p_obs = sub.add_parser(
        "observe",
        help="Render the captured outbox locally: stream, tree, report (no network).",
    )
    p_obs.add_argument(
        "--bridge-home",
        default=None,
        help="Bridge data directory (default: $BRIDGE_HOME or ~/.hermes-dbass).",
    )
    p_obs.add_argument("--stream", action="store_true", help="Event stream in producer_sequence order.")
    p_obs.add_argument("--tree", action="store_true", help="Execution tree with token/cost rollups.")
    p_obs.add_argument(
        "--report",
        action="store_true",
        help="Reconciler findings; exits non-zero when any exist.",
    )
    p_obs.add_argument("--session", default=None, help="Filter to one session/operation id.")
    p_obs.add_argument("--since", default=None, help="Keep events at/after an epoch or ISO timestamp.")
    p_obs.set_defaults(func=_cmd_observe)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return 0
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
