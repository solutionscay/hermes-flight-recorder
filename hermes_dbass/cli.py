"""Command-line entry point for the Bridge companion.

Subcommands land across the Phase 0 steps. ``init`` (this step) creates
the local outbox and mints the installation identity. ``run``,
``reconcile``, and ``observe`` arrive in later steps.
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
