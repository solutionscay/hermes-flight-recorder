"""Implementation of the status CLI command."""

from __future__ import annotations

import sys
import time
from typing import Any

from .collector import CAPTURE_HEARTBEAT_KEY, recorder_config
from .collector.health import RECONCILE_HEALTH_KEY, read_health, source_health_key
from .collector.reconcile import ReconcileConfig
from .collector.recorder_config import (
    CAPTURE_SOURCE_NAMES,
    source_enabled,
    source_required,
)
from .collector.sync import delivery_cursor
from .version import build_identity


def run(args, *, open_outbox, check_initialized, flight_recorder_home) -> int:
    threshold = ReconcileConfig().capture_stale_after
    now = time.time()
    try:
        config = recorder_config.load(flight_recorder_home(args))
    except recorder_config.RecorderConfigError as exc:
        print(f"status not configured: {exc}", file=sys.stderr)
        return 2

    outbox = open_outbox(args)
    try:
        if not check_initialized(outbox):
            return 2
        print(f"installation:    {outbox.installation_id}")
        checks = [
            _print_build_status(args, outbox),
            _print_capture_status(outbox, now, threshold),
            _print_source_status(outbox, config, now, threshold),
            _print_reconcile_status(outbox, config, now, threshold),
        ]
        _print_outbox_status(outbox)
        return 0 if all(checks) else 1
    finally:
        outbox.close()


def _print_build_status(args, outbox) -> bool:
    package_build = build_identity()
    print(f"package build:   {package_build}")
    installed_build = outbox.get_meta("installed_build")
    if installed_build is None:
        return True
    from .collector._common import resolve_hermes_home
    from .collector.hook import HOOK_DIR_NAME, baked_flight_recorder_build

    hook_dir = resolve_hermes_home(args.hermes_home) / "hooks" / HOOK_DIR_NAME
    hook_build = baked_flight_recorder_build(hook_dir)
    if installed_build == package_build == hook_build:
        print(f"hook build:      OK — {hook_build}")
        return True
    print(
        "hook build:      MISMATCH — "
        f"installed {installed_build!r}, package {package_build!r}, "
        f"hook {hook_build!r}"
    )
    return False


def _print_outbox_status(outbox) -> None:
    high_water = outbox.high_water()
    cursor = delivery_cursor(outbox)
    print(
        f"outbox:          producer high-water {high_water}, "
        f"delivery cursor {cursor}, pending {high_water - cursor}"
    )


def _print_capture_status(outbox, now: float, threshold: float) -> bool:
    raw = outbox.get_meta(CAPTURE_HEARTBEAT_KEY)
    if raw is None:
        print("capture:         NO SUCCESS RECORDED (capture has never run)")
        return False
    try:
        last = float(raw)
    except (TypeError, ValueError):
        print(f"capture:         UNREADABLE heartbeat ({raw!r})")
        return False
    age = now - last
    verdict = "OK" if age <= threshold else "STALE"
    stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last))
    print(
        f"capture:         {verdict} — last success {stamp} "
        f"({int(age)}s ago, threshold {int(threshold)}s)"
    )
    return age <= threshold


def _print_source_status(outbox, config, now: float, threshold: float) -> bool:
    healthy = True
    for source in CAPTURE_SOURCE_NAMES:
        if not source_enabled(config.capture, source):
            continue
        required = source_required(config.capture, source)
        state = read_health(outbox, source_health_key(source))
        verdict, source_healthy = health_verdict(state, now, threshold)
        if required and not source_healthy:
            healthy = False
        policy = "required" if required else "optional"
        print(f"source {source}: {policy} {verdict}; {health_details(state, now)}")
    return healthy


def _print_reconcile_status(outbox, config, now: float, threshold: float) -> bool:
    stale_after = max(threshold, config.reconcile.interval_seconds * 3)
    state = read_health(outbox, RECONCILE_HEALTH_KEY)
    verdict, healthy = health_verdict(state, now, stale_after)
    print(f"reconcile:       {verdict}; {health_details(state, now)}")
    return healthy


def health_verdict(
    state: dict[str, Any], now: float, stale_after: float
) -> tuple[str, bool]:
    if "unreadable" in state:
        return "UNREADABLE", False
    try:
        if int(state.get("consecutive_failures", 0)) > 0:
            return "BROKEN", False
    except (TypeError, ValueError):
        return "UNREADABLE", False
    try:
        age = now - float(state.get("last_success_at"))
    except (TypeError, ValueError):
        return "NO SUCCESS RECORDED", False
    return ("STALE", False) if age > stale_after else ("OK", True)


def health_details(state: dict[str, Any], now: float) -> str:
    if "unreadable" in state:
        return f"state {state['unreadable']!r}"
    last_success = _health_time(state.get("last_success_at"), now)
    last_error = _health_time(state.get("last_error_at"), now)
    error = state.get("last_error")
    error_text = f"{last_error} ({error})" if error else "never"
    return (
        f"last success {last_success}; last error {error_text}; "
        f"consecutive failures {state.get('consecutive_failures', 0)}"
    )


def _health_time(value: Any, now: float) -> str:
    try:
        when = float(value)
    except (TypeError, ValueError):
        return "never"
    stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(when))
    return f"{stamp} ({int(now - when)}s ago)"


__all__ = ["health_details", "health_verdict", "run"]
