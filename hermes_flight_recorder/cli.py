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

from .version import build_identity

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
            operator_pubkey=args.operator_pubkey,
        )
    except InstallError as exc:
        print(f"install failed: {exc}", file=sys.stderr)
        return 2
    return 0


def _cmd_keygen(args: argparse.Namespace) -> int:
    from .collector import keystore

    fr_home = _flight_recorder_home(args)
    fr_home.mkdir(mode=0o700, parents=True, exist_ok=True)

    # Without --rotate, minting over an existing key is refused; surface the
    # current public key instead so an operator can distribute it to agents.
    if not args.rotate and keystore.has_public(fr_home):
        public = keystore.load_public_key(fr_home)
        print(f"operator key already present ({public.key_id}); "
              f"pass --rotate to mint a new one.")
        print(f"public key file: {keystore.public_path(fr_home)}")
        print(public.to_text())
        return 0

    try:
        keypair = keystore.mint_operator_keypair(fr_home, rotate=args.rotate)
    except keystore.KeystoreError as exc:
        print(f"keygen failed: {exc}", file=sys.stderr)
        return 2

    verb = "rotated to" if args.rotate else "minted"
    print(f"operator keypair {verb} {keypair.key_id}")
    print(f"public key:  {keystore.public_path(fr_home)}")
    print(f"private key: {keystore.secret_path(fr_home)} (keep this secret; "
          f"never distribute or place on an agent host)")
    if args.rotate:
        print(f"the previous key was retained under "
              f"'{keystore.RETIRED_DIR_NAME}/' so existing history stays readable.")
    print()
    print("distribute this public key to fleet agents "
          "(`install --operator-pubkey <file>`):")
    print(keypair.public.to_text())
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


def _cmd_update(args: argparse.Namespace) -> int:
    from .collector.update import UpdateError, complete_update, update

    try:
        if args.complete:
            complete_update(
                args.flight_recorder_home,
                args.hermes_home,
                guard_owner_pid=args.guard_owner_pid,
            )
        else:
            update(
                args.flight_recorder_home,
                args.hermes_home,
                source=args.source,
                commit=args.commit,
                editable=args.editable,
            )
    except UpdateError as exc:
        print(f"update failed: {exc}", file=sys.stderr)
        return 2
    return 0


def _cmd_serve(args: argparse.Namespace) -> int:
    from .collector import recorder_config, sync_config
    from .collector.runtime_lock import LOCK_FILENAME, RuntimeLock, RuntimeLockError
    from .collector.serve import (
        SERVE_ALREADY_RUNNING,
        SYNC_REQUEST_TIMEOUT,
        configure_logging,
        serve,
    )
    from .collector.transport import HttpsTransport, RetryingTransport

    log = configure_logging(args.log_level)
    fr_home = _flight_recorder_home(args)
    fr_home.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock = RuntimeLock(fr_home / LOCK_FILENAME)
    try:
        lock.acquire()
    except RuntimeLockError as exc:
        log.error("%s", exc)
        return SERVE_ALREADY_RUNNING

    try:
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
                            sync,
                            timeout=SYNC_REQUEST_TIMEOUT,
                            require_https=not args.allow_insecure_url,
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
                lock=lock,
                logger=log,
            )
        finally:
            outbox.close()
    finally:
        lock.release()


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
    from .cli_observe import run

    return run(args, open_outbox=_open_outbox, check_initialized=_check_initialized)


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
    from .cli_status import run

    return run(
        args,
        open_outbox=_open_outbox,
        check_initialized=_check_initialized,
        flight_recorder_home=_flight_recorder_home,
    )


def _sync_once(
    outbox,
    transport,
    *,
    max_records: int = 500,
    max_bytes: int = 1024 * 1024,
    retention_config=None,
) -> int:
    """One sync pass. Print the summary and return a sync exit code.

    The pass itself (push -> cursor delta -> wrapped-DEK ship -> prune) is
    shared with ``serve`` in :mod:`.collector.sync_pass`; this function only
    renders the result and maps it to an exit code.
    """
    from .collector.sync_pass import run_sync_pass

    result = run_sync_pass(
        outbox,
        transport,
        max_records=max_records,
        max_bytes=max_bytes,
        retention_config=retention_config,
    )
    if result.outcome == "terminal":
        # A client defect. Resending the same body cannot help.
        print(
            f"sync stopped: malformed batch (client defect): {result.detail}",
            file=sys.stderr,
        )
        return _SYNC_TERMINAL

    print(
        f"shipped {result.acked} / acked {result.acked} / pending {result.pending}  "
        f"(delivery cursor {result.delivery_cursor}, "
        f"producer high-water {result.delivery_cursor + result.pending})"
    )
    _report_key_ship(result)
    if result.prune_error is not None:
        print(f"automatic retention skipped: {result.prune_error}", file=sys.stderr)
    elif result.pruned is not None:
        _print_prune_result(result.pruned, automatic=True)
    if result.outcome == "ok":
        return _SYNC_OK
    if result.outcome == "auth":
        message = "sync failed: the edge rejected the service token"
        if result.detail:
            message += f": {result.detail}"
        print(message, file=sys.stderr)
        return _SYNC_AUTH
    if result.outcome == "cancelled":
        # Shutdown stopped the pass mid-flight; the outbox keeps the events.
        print("sync stopped for shutdown", file=sys.stderr)
        return _SYNC_OK
    message = "sync failed: the ingestion service is unreachable"
    if result.detail:
        message += f": {result.detail}"
    print(message, file=sys.stderr)
    return _SYNC_UNREACHABLE


def _report_key_ship(result) -> None:
    """Print the wrapped-DEK side-channel outcome; never affects exit codes.

    The side-channel is independent of event delivery: a network or auth
    failure just leaves the records for the next pass (delivery is idempotent
    server-side). A terminal client defect is surfaced but still not fatal.
    """
    if result.key_outcome is None:
        return
    if result.key_outcome == "terminal":
        print(
            "wrapped-key sync stopped: malformed batch (client defect): "
            f"{result.key_detail}",
            file=sys.stderr,
        )
    elif result.key_outcome == "ok":
        if result.keys_sent:
            print(f"shipped {result.keys_sent} wrapped key(s)")
    else:
        print(f"wrapped-key sync deferred ({result.key_outcome})", file=sys.stderr)


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
        version=f"hermes-flight-recorder {build_identity()}",
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
    p_install.add_argument(
        "--operator-pubkey",
        default=None,
        metavar="FILE",
        help="Install as a fleet agent that seals content to this operator public "
        "key; no private key is written to the host. Omit for a solo install "
        "(a keypair is minted locally). Generate the key with `keygen`.",
    )
    p_install.set_defaults(func=_cmd_install)

    p_keygen = sub.add_parser(
        "keygen",
        help="Mint (or --rotate) the fleet operator keypair and print its public key.",
        parents=[_home_options()],
    )
    p_keygen.add_argument(
        "--rotate",
        action="store_true",
        help="Retire the current keypair (kept for reading history) and mint a new "
        "one. New content seals to the new key; forward-only.",
    )
    p_keygen.set_defaults(func=_cmd_keygen)

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

    p_update = sub.add_parser(
        "update",
        help="Back up and update an installed Flight Recorder from Git or a local checkout.",
        parents=[_home_options()],
    )
    p_update.add_argument(
        "--source",
        default="git+https://github.com/solutionscay/hermes-flight-recorder.git",
        help="Git URL or local checkout to install (default: the public repository).",
    )
    p_update.add_argument(
        "--commit",
        default=None,
        help="Required for Git sources. Give a full 40-character or 64-character hash.",
    )
    p_update.add_argument(
        "--editable",
        action="store_true",
        help="Install a local --source as editable for development testing.",
    )
    p_update.add_argument(
        "--complete",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    p_update.add_argument(
        "--guard-owner-pid",
        type=int,
        default=None,
        help=argparse.SUPPRESS,
    )
    p_update.set_defaults(func=_cmd_update)

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
