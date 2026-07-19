"""CI wrapper for the Phase 0 POC exit-gate (issue #8).

Imports `scripts/poc_exit_gate.py` and runs each scenario, so the exit-gate
is part of the normal `pytest` run — not a script someone has to remember to
invoke. Each scenario returns a list of assertion-failure strings; an empty
list is a pass. `main()` is exercised too, to cover the runner and its exit
code.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("poc_exit_gate", _ROOT / "scripts" / "poc_exit_gate.py")
gate = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gate)
gate.VERBOSE = False  # keep test output quiet regardless of pytest flags


def test_happy_path_reconciles_clean(tmp_path: Path) -> None:
    assert gate.scenario_happy(tmp_path) == []


def test_dropped_capture_flags_exactly_one_gap(tmp_path: Path) -> None:
    assert gate.scenario_dropped_capture(tmp_path) == []


def test_missed_cron_flags_exactly_one_run(tmp_path: Path) -> None:
    assert gate.scenario_missed_cron(tmp_path) == []


def test_bridge_restart_preserves_sequence(tmp_path: Path) -> None:
    assert gate.scenario_restart(tmp_path) == []


def test_gate_runner_passes() -> None:
    assert gate.main() == 0
