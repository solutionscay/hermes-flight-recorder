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
import time

from . import __version__

# Exit codes for `sync`, so a cron or a monitor can tell the cases apart.
_SYNC_OK = 0
_SYNC_UNREACHABLE = 1  # the network stayed down through every retry
_SYNC_CONFIG = 2  # the outbox or the sync config is not ready
_SYNC_AUTH = 3  # the edge rejected the service token
_SYNC_TERMINAL = 4  # the server rejected the batch as malformed (a client defect)


def _cmd_init(args: argparse.Namespace) -> int:
    # Imported lazily so `hermes-flight-recorder --version` needs no heavy deps.
    from .collector._common import resolve_hermes_home
    from .collector.hook import install_hook
    from .collector.outbox import Outbox

    outbox = Outbox.open(args.bridge_home)
    try:
        installation_id = outbox.initialize()
        print(f"outbox:          {outbox.path}")
        print(f"installation_id: {installation_id}")

        hermes_home = resolve_hermes_home(args.hermes_home)
        try:
            hook_dir = install_hook(hermes_home, outbox.path.parent, force=args.force)
            print(f"hook installed:  {hook_dir}")
            print("restart the Hermes gateway to load the hook.")
        except FileExistsError as exc:
            print(f"hook already installed at {exc} (use --force to reinstall)")
    finally:
        outbox.close()
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    from .collector import cron_db, state_db
    from .collector.hook import drain as drain_hook_spool
    from .collector.outbox import Outbox, OutboxError

    outbox = Outbox.open(args.bridge_home)
    try:
        try:
            outbox.installation_id  # fails if not initialized
        except OutboxError:
            print("outbox not initialized; run `hermes-flight-recorder init` first", file=sys.stderr)
            return 2

        totals: dict[str, int] = {}
        # Drain the live hook spool first, then poll the durable stores.
        try:
            for event_type, n in drain_hook_spool(outbox).items():
                totals[event_type] = totals.get(event_type, 0) + n
        except Exception as exc:  # a bad spool must not sink the poll pass
            print(f"  (hook drain: {exc})", file=sys.stderr)

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
            print("outbox not initialized; run `hermes-flight-recorder init` first", file=sys.stderr)
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
            print("outbox not initialized; run `hermes-flight-recorder init` first", file=sys.stderr)
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


def _sync_summary(outbox, before_cursor: int) -> tuple[int, int, int]:
    """Return ``(acked_this_pass, delivery_cursor, pending)`` from outbox state.

    The delivery cursor advances only after a durable ack, so its movement is
    the honest count of what shipped and acked this pass. ``pending`` is the
    distance the server is still behind the producer high-water. Read from the
    outbox, not from the pass result, so the summary is truthful even when a
    multi-batch pass ships some batches and then the network drops.
    """
    from .collector.sync import delivery_cursor

    after = delivery_cursor(outbox)
    producer_high_water = outbox.high_water()
    return after - before_cursor, after, producer_high_water - after


def _sync_once(outbox, transport) -> int:
    """One sync pass. Print the summary and return a sync exit code."""
    from .collector.sync import delivery_cursor
    from .collector.transport import TerminalTransportError, push

    before = delivery_cursor(outbox)
    try:
        outcome = push(outbox, transport)
    except TerminalTransportError as exc:
        # A client defect. Resending the same body cannot help.
        print(f"sync stopped: malformed batch (client defect): {exc}", file=sys.stderr)
        return _SYNC_TERMINAL

    acked, cursor, pending = _sync_summary(outbox, before)
    print(
        f"shipped {acked} / acked {acked} / pending {pending}  "
        f"(delivery cursor {cursor}, producer high-water {cursor + pending})"
    )
    if outcome.ok:
        return _SYNC_OK
    if outcome.reason == "auth":
        print("sync failed: the edge rejected the service token", file=sys.stderr)
        return _SYNC_AUTH
    print("sync failed: the ingestion service is unreachable", file=sys.stderr)
    return _SYNC_UNREACHABLE


def _cmd_sync(args: argparse.Namespace) -> int:
    from .collector import sync_config
    from .collector.outbox import Outbox, OutboxError
    from .collector.transport import HttpsTransport, RetryingTransport

    outbox = Outbox.open(args.bridge_home)
    try:
        try:
            outbox.installation_id  # fails if not initialized
        except OutboxError:
            print("outbox not initialized; run `hermes-flight-recorder init` first", file=sys.stderr)
            return _SYNC_CONFIG

        try:
            config = sync_config.load(args.bridge_home)
        except sync_config.SyncConfigError as exc:
            print(f"sync not configured: {exc}", file=sys.stderr)
            return _SYNC_CONFIG

        transport = RetryingTransport(
            HttpsTransport.from_config(
                config, require_https=not args.allow_insecure_url
            )
        )

        if args.interval is None:
            return _sync_once(outbox, transport)

        # Interval mode ships forever and tolerates an offline network: the
        # outbox buffers and the next pass catches up. Ctrl-C stops it cleanly.
        try:
            while True:
                _sync_once(outbox, transport)
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("sync stopped.", file=sys.stderr)
            return _SYNC_OK
    finally:
        outbox.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hermes-flight-recorder",
        description="Bridge — the local-first companion for Hermes Flight Recorder.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"hermes-flight-recorder {__version__}",
    )
    sub = parser.add_subparsers(dest="command")

    p_init = sub.add_parser(
        "init",
        help="Create the local outbox, mint the installation id, and install the hook.",
    )
    p_init.add_argument(
        "--bridge-home",
        default=None,
        help="Bridge data directory (default: $BRIDGE_HOME or ~/.hermes-flight-recorder).",
    )
    p_init.add_argument(
        "--hermes-home",
        default=None,
        help="Hermes data root to install the hook into (default: $HERMES_HOME or ~/.hermes).",
    )
    p_init.add_argument(
        "--force",
        action="store_true",
        help="Reinstall the hook even if it is already present.",
    )
    p_init.set_defaults(func=_cmd_init)

    p_run = sub.add_parser(
        "run", help="Poll state.db and the cron store into the outbox (one pass)."
    )
    p_run.add_argument(
        "--bridge-home",
        default=None,
        help="Bridge data directory (default: $BRIDGE_HOME or ~/.hermes-flight-recorder).",
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
        help="Bridge data directory (default: $BRIDGE_HOME or ~/.hermes-flight-recorder).",
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
        help="Bridge data directory (default: $BRIDGE_HOME or ~/.hermes-flight-recorder).",
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

    p_sync = sub.add_parser(
        "sync",
        help="Ship pending outbox events to the ingestion service (one pass by default).",
    )
    p_sync.add_argument(
        "--bridge-home",
        default=None,
        help="Bridge data directory (default: $BRIDGE_HOME or ~/.hermes-flight-recorder).",
    )
    p_sync.add_argument(
        "--interval",
        type=float,
        default=None,
        metavar="SECONDS",
        help="Ship repeatedly every SECONDS and tolerate an offline network. "
        "Omit for a single pass (the default), which a cron can schedule.",
    )
    p_sync.add_argument(
        "--allow-insecure-url",
        action="store_true",
        help="Permit a plaintext http:// ingest URL (local dev only; HTTPS is the default).",
    )
    p_sync.set_defaults(func=_cmd_sync)

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
