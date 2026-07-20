"""Shared scenario runner for the gate scripts.

Each gate defines named scenarios that take a throwaway temp directory and
return a list of failure strings. This module owns the banner, the per-
scenario ``[PASS]``/``[FAIL]`` output, the failure summary, and the exit
code, so the three gates format identically.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Callable, Iterable

Scenario = tuple[str, Callable[[Path], list[str]]]


def run_gate(
    header: Iterable[str],
    scenarios: Iterable[Scenario],
    *,
    passed: str,
    failed: str,
    width: int = 48,
    catch: bool = False,
) -> int:
    """Run every scenario in its own temp dir and report. Returns the exit code.

    ``catch=True`` turns an exception escaping a scenario into a failure
    line instead of aborting the whole gate.
    """
    for line in header:
        print(line)
    print("=" * width)
    all_failures: list[str] = []
    for name, scenario in scenarios:
        with tempfile.TemporaryDirectory() as directory:
            if catch:
                try:
                    failures = scenario(Path(directory))
                except Exception as exc:  # noqa: BLE001 — a crashing check is a failure
                    failures = [f"{name}: raised {type(exc).__name__}: {exc}"]
            else:
                failures = scenario(Path(directory))
        status = "PASS" if not failures else "FAIL"
        print(f"  [{status}] {name}")
        for failure in failures:
            print(f"         - {failure}")
        all_failures += failures
    print("=" * width)
    if all_failures:
        print(f"{failed} — {len(all_failures)} assertion(s) failed")
        return 1
    print(passed)
    return 0
