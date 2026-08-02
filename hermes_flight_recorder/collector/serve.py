"""One portable foreground process that runs a Flight Recorder installation.

``serve`` is the unified runtime the retired systemd units used to provide as
three separate timers. A single process:

- captures on ``capture.interval_seconds`` (drains the hook spool and polls the
  durable stores),
- reconciles independently on ``reconcile.interval_seconds`` (so it flags
  capture staleness even when the capture pass is broken),
- syncs on ``sync.interval_seconds`` when a sync config is present,
- holds a single-instance :class:`~.runtime_lock.RuntimeLock`,
- shuts down cleanly on SIGINT/SIGTERM,
- reports through structured logs and its exit status.

Native service registration (systemd, launchd, Windows Service) wraps this same
foreground command; the core package depends on none of them.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

from .runtime_lock import RuntimeLock, RuntimeLockError

# Exit codes (distinct from the `sync` one-shot namespace, which is scoped to
# that command). `serve` returns 0 on a clean signalled shutdown.
SERVE_OK = 0
SERVE_ALREADY_RUNNING = 3

# Used when a sync config is present but pins no interval: ship on this cadence
# rather than never (an explicit `sync.interval_seconds` overrides it).
_SYNC_FALLBACK_INTERVAL = 60.0
# A serve request cannot hold shutdown longer than this socket timeout. The
# retry wrapper uses the stop event, so it cancels every later wait and attempt.
SYNC_REQUEST_TIMEOUT = 5.0
SYNC_SHUTDOWN_TIMEOUT = 6.0

_LOGGER_NAME = "hermes_flight_recorder.serve"


def configure_logging(level: str = "INFO") -> logging.Logger:
    """Return the serve logger, attaching a stderr handler once."""
    logger = logging.getLogger(_LOGGER_NAME)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        )
        logger.addHandler(handler)
        logger.propagate = False
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    return logger


@dataclass
class _Task:
    name: str
    interval: float
    run: Callable[[], None]
    deadline: float  # next fire time, on the monotonic clock
    failures: int = 0


def serve(
    outbox: Any,
    hermes_home: Any,
    config: Any,
    *,
    transport: Any = None,
    capture_interval: float | None = None,
    reconcile_interval: float | None = None,
    sync_interval: float | None = None,
    lock: RuntimeLock | None = None,
    logger: logging.Logger | None = None,
    stop_event: threading.Event | None = None,
    install_signal_handlers: bool = True,
    sync_shutdown_timeout: float = SYNC_SHUTDOWN_TIMEOUT,
) -> int:
    """Run the capture/reconcile/sync loop until signalled to stop.

    ``config`` is a :class:`~.recorder_config.RecorderConfig`. ``transport`` (a
    ready sync transport) enables the sync pass; ``None`` disables it. The
    ``*_interval`` overrides win over the corresponding config values. Returns
    :data:`SERVE_ALREADY_RUNNING` when ``lock`` is already held, else
    :data:`SERVE_OK` after a clean shutdown.
    """
    log = logger or configure_logging()
    stop = stop_event or threading.Event()

    if lock is not None:
        try:
            lock.acquire()
        except RuntimeLockError as exc:
            log.error("%s", exc)
            return SERVE_ALREADY_RUNNING

    restore_signals = (
        _install_signal_handlers(stop, log) if install_signal_handlers else None
    )
    sync_worker: _SyncWorker | None = None
    try:
        sync_run = None
        if transport is not None:
            if hasattr(transport, "cancel_event"):
                transport.cancel_event = stop
            sync_worker = _SyncWorker(outbox, transport, config, log, stop)
            sync_worker.start()
            sync_run = sync_worker.request
        tasks = _build_tasks(
            outbox,
            hermes_home,
            config,
            sync_run,
            capture_interval,
            reconcile_interval,
            sync_interval,
            log,
        )
        log.info(
            "serve started: %s",
            ", ".join(f"{t.name}@{t.interval:g}s" for t in tasks),
        )
        _run_loop(tasks, stop, log)
        log.info("serve stopped cleanly")
        return SERVE_OK
    finally:
        if sync_worker is not None:
            stop.set()
            sync_worker.stop(sync_shutdown_timeout)
        if restore_signals is not None:
            restore_signals()
        if lock is not None:
            lock.release()


def _build_tasks(
    outbox,
    hermes_home,
    config,
    sync_run,
    capture_interval,
    reconcile_interval,
    sync_interval,
    log,
) -> list[_Task]:
    start = time.monotonic()  # every pass fires once immediately at startup
    tasks: list[_Task] = []

    cap_iv = capture_interval or config.capture.interval_seconds
    tasks.append(
        _Task("capture", cap_iv, lambda: _capture(outbox, hermes_home, config, log), start)
    )

    rec_iv = reconcile_interval or config.reconcile.interval_seconds
    tasks.append(
        _Task("reconcile", rec_iv, lambda: _reconcile(outbox, hermes_home, config, log), start)
    )
    audit_iv = config.reconcile.audit_interval_seconds
    tasks.append(
        _Task(
            "audit",
            audit_iv,
            lambda: _reconcile(outbox, hermes_home, config, log, True),
            start + audit_iv,
        )
    )

    if sync_run is not None:
        sync_iv = sync_interval or config.sync.interval_seconds or _SYNC_FALLBACK_INTERVAL
        tasks.append(
            _Task("sync", sync_iv, sync_run, start)
        )
    return tasks


class _SyncWorker:
    """Run one bounded sync tick outside the capture scheduler."""

    def __init__(self, outbox, transport, config, log, stop: threading.Event):
        self._flight_recorder_home = outbox.path.parent
        self._transport = transport
        self._config = config
        self._log = log
        self._stop = stop
        self._requested = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="hermes-flight-recorder-sync",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def request(self) -> None:
        """Request a tick. Multiple requests collapse while a tick runs."""
        self._requested.set()

    def stop(self, timeout: float) -> None:
        self._requested.set()
        self._thread.join(timeout=max(0.0, timeout))
        if self._thread.is_alive():
            self._log.warning(
                "sync worker exceeded the %.1fs shutdown deadline", timeout
            )

    def _run(self) -> None:
        from .outbox import Outbox

        try:
            outbox = Outbox.open(self._flight_recorder_home)
        except Exception as exc:
            self._log.exception("sync worker could not open the outbox: %s", exc)
            return
        try:
            while True:
                self._requested.wait()
                self._requested.clear()
                if self._stop.is_set():
                    return
                try:
                    _sync(
                        outbox,
                        self._transport,
                        self._config,
                        self._log,
                    )
                except Exception as exc:
                    self._log.exception("sync pass failed: %s", exc)
        finally:
            outbox.close()


def _run_loop(tasks: list[_Task], stop: threading.Event, log: logging.Logger) -> None:
    while not stop.is_set():
        now = time.monotonic()
        due = [t for t in tasks if t.deadline <= now]
        if not due:
            nxt = min(t.deadline for t in tasks)
            stop.wait(timeout=max(0.0, nxt - now))
            continue
        for task in due:
            if stop.is_set():
                break
            _run_task(task, log)
            task.deadline = time.monotonic() + task.interval


def _run_task(task: _Task, log: logging.Logger) -> None:
    try:
        task.run()
        task.failures = 0
    except Exception as exc:  # a bad pass must never kill the daemon
        task.failures += 1
        log.exception("%s pass failed (%d consecutive): %s", task.name, task.failures, exc)


# --- passes -------------------------------------------------------------
def _capture(outbox, hermes_home, config, log: logging.Logger) -> None:
    from . import run_pass

    totals = run_pass(
        outbox,
        hermes_home,
        capture_config=config.capture,
        knowledge_config=config.knowledge,
        on_source_error=lambda label, exc: log.warning("capture source %s: %s", label, exc),
    )
    log.info("capture: %d event(s)", sum(totals.values()))
    _maybe_prune(outbox, config.retention, log)


def _reconcile(
    outbox, hermes_home, config, log: logging.Logger, full_audit: bool = False
) -> None:
    from .reconcile import reconcile

    counts = reconcile(
        outbox,
        hermes_home,
        capture_config=config.capture,
        knowledge_config=config.knowledge,
        full_audit=full_audit,
    )
    label = "audit" if full_audit else "reconcile"
    log.info("%s: %d finding(s)", label, sum(counts.values()))


def _sync(outbox, transport, config, log: logging.Logger) -> None:
    """One sync pass, shared with the CLI in :mod:`.sync_pass`; log the result."""
    from .sync_pass import run_sync_pass

    result = run_sync_pass(
        outbox,
        transport,
        max_records=config.sync.max_records,
        max_bytes=config.sync.max_bytes,
        max_batches=config.sync.max_batches_per_tick,
        retention_config=config.retention,
    )
    if result.outcome == "terminal":
        log.error("sync stopped: malformed batch (client defect): %s", result.detail)
        return

    log.info("sync: acked %d, pending %d", result.acked, result.pending)
    if result.outcome == "auth":
        log.error("sync failed: the edge rejected the service token")
    elif result.outcome == "cancelled":
        log.info("sync stopped for shutdown")
        return
    elif result.outcome != "ok":
        log.warning("sync failed: the ingestion service is unreachable (buffered)")

    if result.key_outcome == "terminal":
        log.error(
            "wrapped-key sync stopped: malformed batch (client defect): %s",
            result.key_detail,
        )
    elif result.key_outcome == "ok":
        if result.keys_sent:
            log.info("wrapped-key sync: shipped %d", result.keys_sent)
    elif result.key_outcome == "auth":
        log.error("wrapped-key sync failed: the edge rejected the service token")
    elif result.key_outcome == "cancelled":
        log.info("wrapped-key sync stopped for shutdown")
    elif result.key_outcome is not None:
        log.warning("wrapped-key sync failed: the service is unreachable (buffered)")

    if result.prune_error is not None:
        log.warning("automatic retention skipped: %s", result.prune_error)
    elif result.pruned is not None and result.pruned.pruned_count:
        log.info("retention: pruned %d delivered event(s)", result.pruned.pruned_count)


def _maybe_prune(outbox, retention_config, log: logging.Logger) -> None:
    from .retention import RetentionError, maybe_prune

    try:
        result = maybe_prune(outbox, retention_config)
    except RetentionError as exc:
        log.warning("automatic retention skipped: %s", exc)
        return
    if result is not None and result.pruned_count:
        log.info("retention: pruned %d delivered event(s)", result.pruned_count)


# --- signals ------------------------------------------------------------
def _install_signal_handlers(stop: threading.Event, log: logging.Logger):
    """Route SIGINT/SIGTERM to ``stop``; return a callable that restores them.

    Signal handlers can only be installed from the main thread; when serve runs
    off-thread (as tests may drive it) we silently skip them and rely on the
    injected stop event.
    """
    previous: dict[int, Any] = {}

    def handler(signum, _frame):
        log.info("received signal %s; shutting down", signum)
        stop.set()

    for name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, name, None)
        if sig is None:
            continue
        try:
            previous[sig] = signal.signal(sig, handler)
        except (ValueError, OSError, AttributeError):
            pass

    def restore() -> None:
        for sig, prev in previous.items():
            try:
                signal.signal(sig, prev)
            except (ValueError, OSError, AttributeError):
                pass

    return restore


__all__ = [
    "serve",
    "configure_logging",
    "SERVE_OK",
    "SERVE_ALREADY_RUNNING",
    "SYNC_REQUEST_TIMEOUT",
    "SYNC_SHUTDOWN_TIMEOUT",
]
