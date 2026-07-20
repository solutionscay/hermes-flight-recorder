"""CI wrapper for the Phase 1 network sync exit-gate (issue #35)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    "poc_sync_gate", _ROOT / "scripts" / "poc_sync_gate.py"
)
gate = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gate)
gate.VERBOSE = False


def test_happy_path(tmp_path: Path) -> None:
    assert gate.scenario_happy(tmp_path) == []


def test_dropped_batch_is_resent_without_a_gap(tmp_path: Path) -> None:
    assert gate.scenario_dropped_batch(tmp_path) == []


def test_duplicate_delivery_is_idempotent(tmp_path: Path) -> None:
    assert gate.scenario_duplicate_delivery(tmp_path) == []


def test_offline_then_online_catches_up(tmp_path: Path) -> None:
    assert gate.scenario_offline_then_online(tmp_path) == []


def test_restart_resumes_from_the_last_ack(tmp_path: Path) -> None:
    assert gate.scenario_restart_mid_sync(tmp_path) == []


def test_gate_runner_passes() -> None:
    assert gate.main() == 0
