"""Command-line entry point for the Flight Recorder companion.

``install`` makes one Hermes home into one Flight Recorder installation
(``$HERMES_HOME/flight-recorder``): it creates the outbox, mints the
installation identity and key, writes config, and installs the hook.
``serve`` runs one portable foreground process that captures, reconciles, and
optionally syncs on their own intervals. ``run``/``reconcile``/``sync`` remain
as one-shot passes a scheduler can drive. ``observe`` renders the captured
outbox locally (stream, tree, report, kanban, knowledge) with no network.
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


def _flight_recorder_home(args: argparse.Namespace):
    """Resolve the Flight Recorder home once for a command.

    Applies the precedence ``--flight-recorder-home`` → env → the namespaced
    ``$HERMES_HOME/flight-recorder`` default, so every command sees the same
    location whether or not it also uses the Hermes home.
    """
    from .collector._common import resolve_flight_recorder_home

    return resolve_flight_recorder_home(args.flight_recorder_home, args.hermes_home)


def _open_outbox(args: argparse.Namespace):
    """Open the outbox at the resolved Flight Recorder home."""
    from .collector.outbox import Outbox

    return Outbox.open(_flight_recorder_home(args), hermes_home=args.hermes_home)


def _check_initialized(outbox) -> bool:
    """True when the outbox has an identity; else print the install hint."""
    from .collector.outbox import OutboxError

    try:
        outbox.installation_id
    except OutboxError:
        print(
            "outbox not initialized; run `hermes-flight-recorder install` first",
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


def _cmd_install(args: argparse.Namespace) -> int:
    # Imported lazily so `hermes-flight-recorder --version` needs no heavy deps.
    from .collector.lifecycle import InstallError, install

    try:
        install(
            args.flight_recorder_home,
            args.hermes_home,
            backfill=not args.no_backfill,
        )
    except InstallError as exc:
        print(f"install failed: {exc}", file=sys.stderr)
        return 2
    return 0


def _cmd_uninstall(args: argparse.Namespace) -> int:
    from .collector.lifecycle import UninstallError, uninstall

    try:
        uninstall(
            args.flight_recorder_home, args.hermes_home, purge_data=args.purge_data
        )
    except UninstallError as exc:
        print(f"uninstall failed: {exc}", file=sys.stderr)
        return 2
    return 0


def _cmd_serve(args: argparse.Namespace) -> int:
    from .collector import recorder_config, sync_config
    from .collector.runtime_lock import LOCK_FILENAME, RuntimeLock
    from .collector.serve import configure_logging, serve
    from .collector.transport import HttpsTransport, RetryingTransport

    log = configure_logging(args.log_level)
    fr_home = _flight_recorder_home(args)

    outbox = _open_outbox(args)
    try:
        if not _check_initialized(outbox):
            return 2

        try:
            config = recorder_config.load(fr_home)
        except recorder_config.RecorderConfigError as exc:
            print(f"serve not configured: {exc}", file=sys.stderr)
            return 2

        transport = None
        if not args.no_sync:
            try:
                sync = sync_config.load(fr_home)
                transport = RetryingTransport(
                    HttpsTransport.from_config(
                        sync, require_https=not args.allow_insecure_url
                    )
                )
            except sync_config.SyncConfigError as exc:
                log.info("sync disabled: %s", exc)

        return serve(
            outbox,
            args.hermes_home,
            config,
            transport=transport,
            capture_interval=args.capture_interval,
            reconcile_interval=args.reconcile_interval,
            sync_interval=args.sync_interval,
            lock=RuntimeLock(fr_home / LOCK_FILENAME),
            logger=log,
        )
    finally:
        outbox.close()


def _cmd_run(args: argparse.Namespace) -> int:
    from .collector import recorder_config, run_pass

    outbox = _open_outbox(args)
    try:
        if not _check_initialized(outbox):
            return 2

        try:
            runtime_config = recorder_config.load(_flight_recorder_home(args))
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
    from .collector.reconcile import reconcile

    outbox = _open_outbox(args)
    try:
        if not _check_initialized(outbox):
            return 2

        try:
            runtime_config = recorder_config.load(_flight_recorder_home(args))
        except recorder_config.RecorderConfigError as exc:
            print(f"reconcile not configured: {exc}", file=sys.stderr)
            return 2

        counts = reconcile(
            outbox,
            args.hermes_home,
            capture_config=runtime_config.capture,
            knowledge_config=runtime_config.knowledge,
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

    since: float | None = None
    if args.since is not None:
        try:
            since = observe.parse_since(args.since)
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 2

    outbox = _open_outbox(args)
    try:
        if not _check_initialized(outbox):
            return 2

        records = observe.load(outbox, session=args.session, since=since)

        # Default to the stream view when no view is selected.
        views = [
            v
            for v in ("stream", "tree", "report", "kanban", "knowledge")
            if getattr(args, v)
        ]
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
            elif view == "knowledge":
                print("── knowledge ──")
                for line in observe.render_knowledge(outbox, records):
                    print(line)
    finally:
        outbox.close()
    return exit_code


def _cmd_prune(args: argparse.Namespace) -> int:
    from .collector import recorder_config
    from .collector.retention import RetentionError, prune

    outbox = _open_outbox(args)
    try:
        if not _check_initialized(outbox):
            return 2
        try:
            config = recorder_config.load(_flight_recorder_home(args)).retention
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


def _cmd_status(args: argparse.Namespace) -> int:
    """Print a health readout from the outbox and return non-zero if unhealthy.

    On-demand answer to "is capture alive and is the server caught up?" — the
    human counterpart to the ``reconcile.capture_stale`` alert. Store-only (no
    Hermes home, no network), so it is safe to run any time and a cron/monitor
    can gate on the exit code: 0 healthy, 1 unhealthy (capture stale or never
    recorded a success).
    """
    from .collector import CAPTURE_HEARTBEAT_KEY
    from .collector.reconcile import ReconcileConfig
    from .collector.sync import delivery_cursor

    threshold = ReconcileConfig().capture_stale_after
    now = time.time()

    outbox = _open_outbox(args)
    try:
        if not _check_initialized(outbox):
            return 2

        print(f"installation:    {outbox.installation_id}")

        high_water = outbox.high_water()
        cursor = delivery_cursor(outbox)
        pending = high_water - cursor
        print(
            f"outbox:          producer high-water {high_water}, "
            f"delivery cursor {cursor}, pending {pending}"
        )

        raw = outbox.get_meta(CAPTURE_HEARTBEAT_KEY)
        healthy = True
        if raw is None:
            print("capture:         NO SUCCESS RECORDED (capture has never run)")
            healthy = False
        else:
            try:
                last = float(raw)
            except (TypeError, ValueError):
                print(f"capture:         UNREADABLE heartbeat ({raw!r})")
                healthy = False
            else:
                age = now - last
                stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last))
                verdict = "OK" if age <= threshold else "STALE"
                if age > threshold:
                    healthy = False
                print(
                    f"capture:         {verdict} — last success {stamp} "
                    f"({int(age)}s ago, threshold {int(threshold)}s)"
                )
    finally:
        outbox.close()
    return 0 if healthy else 1


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
        message = "sync failed: the edge rejected the service token"
        if outcome.detail:
            message += f": {outcome.detail}"
        print(message, file=sys.stderr)
        return _SYNC_AUTH
    message = "sync failed: the ingestion service is unreachable"
    if outcome.detail:
        message += f": {outcome.detail}"
    print(message, file=sys.stderr)
    return _SYNC_UNREACHABLE


def _cmd_sync(args: argparse.Namespace) -> int:
    from .collector import recorder_config, sync_config
    from .collector.transport import HttpsTransport, RetryingTransport

    fr_home = _flight_recorder_home(args)
    outbox = _open_outbox(args)
    try:
        if not _check_initialized(outbox):
            return _SYNC_CONFIG

        try:
            config = sync_config.load(fr_home)
            runtime_config = recorder_config.load(fr_home)
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


def _explicit_secret(args: argparse.Namespace) -> str | None:
    """The client secret from a non-interactive source, or None.

    Precedence: ``--client-secret-stdin`` (read one value from stdin), then
    ``--client-secret``, then ``$HFR_CF_ACCESS_CLIENT_SECRET``. No prompt here —
    an interactive prompt is a last resort handled by the command when the merge
    would otherwise be incomplete.
    """
    import os

    if args.client_secret_stdin:
        return sys.stdin.read().strip() or None
    if args.client_secret:
        return args.client_secret
    return os.environ.get("HFR_CF_ACCESS_CLIENT_SECRET") or None


def _redact_client_id(client_id: str) -> str:
    """Show enough of the client id to recognize it, hiding the rest."""
    if len(client_id) <= 8:
        return client_id[:2] + "…"
    return client_id[:8] + "…"


def _cmd_configure_sync(args: argparse.Namespace) -> int:
    from .collector import sync_config

    fr_home = _flight_recorder_home(args)
    secret = _explicit_secret(args)

    def attempt(sec: str | None):
        return sync_config.configure(
            fr_home,
            ingest_url=args.ingest_url,
            cf_access_client_id=args.client_id,
            cf_access_client_secret=sec,
        )

    try:
        config = attempt(secret)
    except sync_config.SyncConfigError as exc:
        # Prompt for the secret only when it is the one missing field and we can
        # ask interactively; otherwise the caller must supply the flags.
        only_secret_missing = (
            secret is None
            and "cf_access_client_secret" in str(exc)
            and "cf_access_client_id" not in str(exc)
            and "ingest_url" not in str(exc)
        )
        if only_secret_missing and sys.stdin.isatty():
            import getpass

            secret = getpass.getpass("Cloudflare Access client secret: ").strip() or None
            try:
                config = attempt(secret)
            except sync_config.SyncConfigError as exc2:
                print(f"configure-sync failed: {exc2}", file=sys.stderr)
                return 2
        else:
            print(f"configure-sync failed: {exc}", file=sys.stderr)
            return 2

    if not args.allow_insecure_url and config.ingest_url.startswith("http://"):
        print(
            "warning: ingest URL is plaintext http://; use https:// in production "
            "(sync/serve reject it unless --allow-insecure-url is set).",
            file=sys.stderr,
        )
    print(f"sync configured: {config.ingest_url}")
    print(f"client id:       {_redact_client_id(config.cf_access_client_id)}")
    print(f"config written:  {sync_config.config_path(fr_home)}")
    return 0


def _home_options() -> argparse.ArgumentParser:
    """A parent parser carrying the data-directory options every subcommand shares.

    Both flags apply everywhere now that the Flight Recorder home defaults to the
    ``flight-recorder`` child of the Hermes home: even commands that never touch
    the Hermes stores need ``--hermes-home`` to resolve that default.
    """
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument(
        "--flight-recorder-home",
        default=None,
        help="Flight Recorder data directory (default: $SC_HERMES_FLIGHT_RECORDER_HOME or $HERMES_HOME/flight-recorder).",
    )
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

    p_install = sub.add_parser(
        "install",
        help="Install (idempotently) into a Hermes home: outbox, identity, key, config, and hook.",
        parents=[_home_options()],
    )
    p_install.add_argument(
        "--no-backfill",
        action="store_true",
        help="Capture only activity from now on; do not ingest existing Hermes history.",
    )
    p_install.set_defaults(func=_cmd_install)

    p_uninstall = sub.add_parser(
        "uninstall",
        help="Remove the Hermes hook; preserve recorder data unless --purge-data is given.",
        parents=[_home_options()],
    )
    p_uninstall.add_argument(
        "--purge-data",
        action="store_true",
        help="Also delete the recorder home (outbox, key, config). Irreversible.",
    )
    p_uninstall.set_defaults(func=_cmd_uninstall)

    p_serve = sub.add_parser(
        "serve",
        help="Run one foreground process: capture, reconcile, and optional sync on their own intervals.",
        parents=[_home_options()],
    )
    p_serve.add_argument(
        "--capture-interval",
        type=float,
        default=None,
        metavar="SECONDS",
        help="Override the capture cadence (default: capture.interval_seconds).",
    )
    p_serve.add_argument(
        "--reconcile-interval",
        type=float,
        default=None,
        metavar="SECONDS",
        help="Override the reconcile cadence (default: reconcile.interval_seconds).",
    )
    p_serve.add_argument(
        "--sync-interval",
        type=float,
        default=None,
        metavar="SECONDS",
        help="Override the sync cadence (default: sync.interval_seconds, or 60s when a sync config exists).",
    )
    p_serve.add_argument(
        "--no-sync",
        action="store_true",
        help="Never sync, even when a sync config is present.",
    )
    p_serve.add_argument(
        "--allow-insecure-url",
        action="store_true",
        help="Permit a plaintext http:// ingest URL (local dev only; HTTPS is the default).",
    )
    p_serve.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ...). Default INFO.",
    )
    p_serve.set_defaults(func=_cmd_serve)

    p_run = sub.add_parser(
        "run",
        help="Poll state.db and the cron store into the outbox (one pass).",
        parents=[_home_options()],
    )
    p_run.set_defaults(func=_cmd_run)

    p_rec = sub.add_parser(
        "reconcile",
        help="Diff the durable stores against the outbox and emit reconcile findings.",
        parents=[_home_options()],
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
    p_obs.add_argument(
        "--knowledge",
        action="store_true",
        help="Knowledge store: per-artifact latest manifest, version history, and diff.",
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

    p_status = sub.add_parser(
        "status",
        help="Print capture freshness and outbox delivery lag; exits non-zero when capture is stale.",
        parents=[_home_options()],
    )
    p_status.set_defaults(func=_cmd_status)

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

    p_cfg = sub.add_parser(
        "configure-sync",
        help="Write the DBaaS ingest endpoint and Cloudflare Access credential (private, 0600).",
        parents=[_home_options()],
    )
    p_cfg.add_argument(
        "--ingest-url",
        default=None,
        help="Ingestion endpoint (default: the hosted Hermes DBaaS endpoint, or the existing value).",
    )
    p_cfg.add_argument(
        "--client-id",
        default=None,
        help="Cloudflare Access service-token client id (keeps its existing value if omitted).",
    )
    secret_group = p_cfg.add_mutually_exclusive_group()
    secret_group.add_argument(
        "--client-secret",
        default=None,
        help="Client secret (discouraged: visible in shell history; prefer stdin, env, or the prompt).",
    )
    secret_group.add_argument(
        "--client-secret-stdin",
        action="store_true",
        help="Read the client secret from stdin.",
    )
    p_cfg.add_argument(
        "--allow-insecure-url",
        action="store_true",
        help="Suppress the plaintext http:// warning.",
    )
    p_cfg.set_defaults(func=_cmd_configure_sync)

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
