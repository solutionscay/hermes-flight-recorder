"""Command-line entry point for the Flight Recorder companion.

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


def _check_initialized(outbox) -> bool:
    """True when the outbox has an identity; else print the init hint."""
    from .collector.outbox import OutboxError

    try:
        outbox.installation_id
    except OutboxError:
        print(
            "outbox not initialized; run `hermes-flight-recorder init` first",
            file=sys.stderr,
        )
        return False
    return True


def _print_prune_result(result, *, automatic: bool = False) -> None:
    """Print an auditable summary for a retention pass."""
    prefix = "automatic retention: " if automatic else ""
    if result.pruned_count == 0:
        if not automatic:
            print(
                f"{prefix}pruned 0 events (delivery cursor "
                f"{result.delivery_cursor}; retained event bytes "
                f"{result.event_bytes_after})"
            )
        return
    print(
        f"{prefix}pruned {result.pruned_count} delivered event(s), "
        f"sequences {result.oldest_sequence}-{result.newest_sequence}; "
        f"removed {result.event_bytes_removed} event bytes and reclaimed "
        f"{result.database_bytes_reclaimed} database bytes"
    )
    if result.space_reclaim_error is not None:
        print(
            f"{prefix}space reclamation failed after pruning: "
            f"{result.space_reclaim_error}",
            file=sys.stderr,
        )


def _automatic_prune(outbox, config) -> None:
    """Run throttled retention without making capture or sync less durable."""
    from .collector.retention import RetentionError, maybe_prune

    try:
        result = maybe_prune(outbox, config)
    except RetentionError as exc:
        print(f"automatic retention skipped: {exc}", file=sys.stderr)
        return
    if result is not None:
        _print_prune_result(result, automatic=True)


def _cmd_init(args: argparse.Namespace) -> int:
    # Imported lazily so `hermes-flight-recorder --version` needs no heavy deps.
    from .collector._common import resolve_hermes_home
    from .collector.hook import install_hook
    from .collector.outbox import Outbox

    outbox = Outbox.open(args.flight_recorder_home)
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
    from .collector import recorder_config, run_pass
    from .collector.outbox import Outbox

    outbox = Outbox.open(args.flight_recorder_home)
    try:
        if not _check_initialized(outbox):
            return 2

        try:
            runtime_config = recorder_config.load(args.flight_recorder_home)
        except recorder_config.RecorderConfigError as exc:
            print(f"run not configured: {exc}", file=sys.stderr)
            return 2

        totals = run_pass(
            outbox,
            args.hermes_home,
            capture_config=runtime_config.capture,
            knowledge_config=runtime_config.knowledge,
            on_source_error=lambda label, exc: print(
                f"  ({label}: {exc})", file=sys.stderr
            ),
        )
        print(f"polled {sum(totals.values())} events into {outbox.path}:")
        for event_type in sorted(totals):
            print(f"  {event_type}: {totals[event_type]}")
        _automatic_prune(outbox, runtime_config.retention)
    finally:
        outbox.close()
    return 0


def _cmd_reconcile(args: argparse.Namespace) -> int:
    from .collector import recorder_config
    from .collector.outbox import Outbox
    from .collector.reconcile import reconcile

    outbox = Outbox.open(args.flight_recorder_home)
    try:
        if not _check_initialized(outbox):
            return 2

        try:
            capture_config = recorder_config.load(args.flight_recorder_home).capture
        except recorder_config.RecorderConfigError as exc:
            print(f"reconcile not configured: {exc}", file=sys.stderr)
            return 2

        counts = reconcile(
            outbox, args.hermes_home, capture_config=capture_config
        )
        total = sum(counts.values())
        print(f"reconciled {total} new finding(s) into {outbox.path}:")
        for event_type in sorted(counts):
            print(f"  {event_type}: {counts[event_type]}")
    finally:
        outbox.close()
    return 0


def _cmd_observe(args: argparse.Namespace) -> int:
    from . import observe
    from .collector.outbox import Outbox

    since: float | None = None
    if args.since is not None:
        try:
            since = observe.parse_since(args.since)
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 2

    outbox = Outbox.open(args.flight_recorder_home)
    try:
        if not _check_initialized(outbox):
            return 2

        records = observe.load(outbox, session=args.session, since=since)

        # Default to the stream view when no view is selected.
        views = [v for v in ("stream", "tree", "report", "kanban") if getattr(args, v)]
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
            elif view == "kanban":
                print("── kanban ──")
                for line in observe.render_kanban(records):
                    print(line)
    finally:
        outbox.close()
    return exit_code


def _cmd_prune(args: argparse.Namespace) -> int:
    from .collector import recorder_config
    from .collector.outbox import Outbox
    from .collector.retention import RetentionError, prune

    outbox = Outbox.open(args.flight_recorder_home)
    try:
        if not _check_initialized(outbox):
            return 2
        try:
            config = recorder_config.load(args.flight_recorder_home).retention
        except recorder_config.RecorderConfigError as exc:
            print(f"prune not configured: {exc}", file=sys.stderr)
            return 2

        try:
            result = prune(outbox, config)
        except RetentionError as exc:
            print(f"prune refused: {exc}", file=sys.stderr)
            return 2
        if result is None:
            print("retention disabled; no events pruned")
            return 0
        _print_prune_result(result)
        return 0
    finally:
        outbox.close()


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


def _sync_once(
    outbox,
    transport,
    *,
    max_records: int = 500,
    max_bytes: int = 1024 * 1024,
    retention_config=None,
) -> int:
    """One sync pass. Print the summary and return a sync exit code."""
    from .collector.sync import delivery_cursor
    from .collector.transport import TerminalTransportError, push

    before = delivery_cursor(outbox)
    try:
        outcome = push(outbox, transport, max_records=max_records, max_bytes=max_bytes)
    except TerminalTransportError as exc:
        # A client defect. Resending the same body cannot help.
        print(f"sync stopped: malformed batch (client defect): {exc}", file=sys.stderr)
        return _SYNC_TERMINAL

    acked, cursor, pending = _sync_summary(outbox, before)
    print(
        f"shipped {acked} / acked {acked} / pending {pending}  "
        f"(delivery cursor {cursor}, producer high-water {cursor + pending})"
    )
    if retention_config is not None:
        _automatic_prune(outbox, retention_config)
    if outcome.ok:
        return _SYNC_OK
    if outcome.reason == "auth":
        print("sync failed: the edge rejected the service token", file=sys.stderr)
        return _SYNC_AUTH
    print("sync failed: the ingestion service is unreachable", file=sys.stderr)
    return _SYNC_UNREACHABLE


def _cmd_sync(args: argparse.Namespace) -> int:
    from .collector import recorder_config, sync_config
    from .collector.outbox import Outbox
    from .collector.transport import HttpsTransport, RetryingTransport

    outbox = Outbox.open(args.flight_recorder_home)
    try:
        if not _check_initialized(outbox):
            return _SYNC_CONFIG

        try:
            config = sync_config.load(args.flight_recorder_home)
            runtime_config = recorder_config.load(args.flight_recorder_home)
        except (
            sync_config.SyncConfigError,
            recorder_config.RecorderConfigError,
        ) as exc:
            print(f"sync not configured: {exc}", file=sys.stderr)
            return _SYNC_CONFIG

        transport = RetryingTransport(
            HttpsTransport.from_config(
                config, require_https=not args.allow_insecure_url
            )
        )

        interval = (
            args.interval
            if args.interval is not None
            else runtime_config.sync.interval_seconds
        )
        sync_kwargs = {
            "max_records": runtime_config.sync.max_records,
            "max_bytes": runtime_config.sync.max_bytes,
            "retention_config": runtime_config.retention,
        }
        if interval is None:
            return _sync_once(outbox, transport, **sync_kwargs)

        # Interval mode ships forever and tolerates an offline network: the
        # outbox buffers and the next pass catches up. Ctrl-C stops it cleanly.
        try:
            while True:
                _sync_once(outbox, transport, **sync_kwargs)
                time.sleep(interval)
        except KeyboardInterrupt:
            print("sync stopped.", file=sys.stderr)
            return _SYNC_OK
    finally:
        outbox.close()


def _home_options(*, hermes: bool = False) -> argparse.ArgumentParser:
    """A parent parser carrying the data-directory options every subcommand shares."""
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument(
        "--flight-recorder-home",
        default=None,
        help="Flight Recorder data directory (default: $SC_HERMES_FLIGHT_RECORDER_HOME or ~/.hermes-flight-recorder).",
    )
    if hermes:
        parent.add_argument(
            "--hermes-home",
            default=None,
            help="Hermes data root (default: $HERMES_HOME or ~/.hermes).",
        )
    return parent


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hermes-flight-recorder",
        description="Hermes Flight Recorder — the local-first companion for Hermes agents.",
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
        parents=[_home_options(hermes=True)],
    )
    p_init.add_argument(
        "--force",
        action="store_true",
        help="Reinstall the hook even if it is already present.",
    )
    p_init.set_defaults(func=_cmd_init)

    p_run = sub.add_parser(
        "run",
        help="Poll state.db and the cron store into the outbox (one pass).",
        parents=[_home_options(hermes=True)],
    )
    p_run.set_defaults(func=_cmd_run)

    p_rec = sub.add_parser(
        "reconcile",
        help="Diff the durable stores against the outbox and emit reconcile findings.",
        parents=[_home_options(hermes=True)],
    )
    p_rec.set_defaults(func=_cmd_reconcile)

    p_obs = sub.add_parser(
        "observe",
        help="Render the captured outbox locally: stream, tree, report, kanban (no network).",
        parents=[_home_options()],
    )
    p_obs.add_argument("--stream", action="store_true", help="Event stream in producer_sequence order.")
    p_obs.add_argument("--tree", action="store_true", help="Execution tree with token/cost rollups.")
    p_obs.add_argument(
        "--report",
        action="store_true",
        help="Reconciler findings; exits non-zero when any exist.",
    )
    p_obs.add_argument(
        "--kanban",
        action="store_true",
        help="Kanban task boards: status, lease, and per-attempt timeline.",
    )
    p_obs.add_argument("--session", default=None, help="Filter to one session/operation id.")
    p_obs.add_argument("--since", default=None, help="Keep events at/after an epoch or ISO timestamp.")
    p_obs.set_defaults(func=_cmd_observe)

    p_prune = sub.add_parser(
        "prune",
        help="Prune delivered outbox events according to retention configuration.",
        parents=[_home_options()],
    )
    p_prune.set_defaults(func=_cmd_prune)

    p_sync = sub.add_parser(
        "sync",
        help="Ship pending outbox events to the ingestion service (one pass by default).",
        parents=[_home_options()],
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
