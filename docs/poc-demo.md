# Phase 0 POC exit-gate

_The Phase 0 exit criterion (issue #8): one host produces **loss-detectable,
replayable** semantic events across restarts and cron misses._

Everything before this proves capture *works*. The exit-gate proves that when
capture *fails*, the failure is **detected** — the property the Phase 1 cloud
work is built on.

## Run it

```bash
python scripts/poc_exit_gate.py        # pass/fail summary
python scripts/poc_exit_gate.py -v     # also print each `observe --report`
```

It also runs as part of the test suite (`tests/test_exit_gate.py`), so `pytest`
covers it.

The gate is fully self-contained and deterministic: it builds a throwaway,
synthetic-but-schema-accurate Hermes home (a `state.db`, a cron store, and a
live hook spool) under a temp dir, anchored to a fixed clock. It never touches
a real `~/.hermes` and never reaches the network.

## What it proves

Four scenarios, each on its own disposable outbox:

| Scenario | Injected fault | Expected result |
|---|---|---|
| **Happy path** | none | a full session (live hook + durable poll) reconciles clean; `observe --report` exits **0** |
| **Dropped capture** | delete one captured event (a hook `invocation.completed`) | reconciler finds **exactly one** `reconcile.gap_detected` (`gap_kind=sequence`) pointing at the lost `producer_sequence`; report exits **≠ 0** |
| **Missed cron** | a 1-minute interval job with an expected fire that has no execution row | **exactly one** `cron.run_missed` for that job; report exits **≠ 0** |
| **Bridge restart** | close and reopen the outbox mid-run | the `producer_sequence` high-water mark and `installation_id` survive; the next capture continues the sequence with no reuse, gap, or duplicate |

## Pass criteria

`GATE PASSED` (exit 0) requires **all** of:

- the happy path reconciles with **zero** findings and a clean report,
- the dropped-capture fault produces exactly the injected sequence gap and a non-zero report,
- the missed-cron fault produces exactly one `cron.run_missed` and a non-zero report,
- a restart preserves the sequence high-water mark with no duplicate or lost row,
- the demo touches only a throwaway home, stores content only as hashes and ciphertext, and leaves `git status` clean.

## Running it against a real Hermes home

The gate uses a synthetic home for determinism. To watch the same pipeline
against a real (dev) Hermes install — the manual flow that was validated live
on 2026-07-19 with a real Discord agent turn:

```bash
export HERMES_HOME=~/.hermes-dev            # a throwaway home, never production
hermes-flight-recorder init                 # creates the outbox, installs the hook
# restart the Hermes gateway so it loads the hook, then drive a session
hermes-flight-recorder run                  # drain the hook spool + poll the durable stores
hermes-flight-recorder reconcile            # diff the stores against the capture
hermes-flight-recorder observe --tree       # execution tree with token/cost rollups
hermes-flight-recorder observe --report     # findings; exits non-zero if any
```

Against a real home, `reconcile` uses the wall clock (there is no `--now`
flag), so the timeout-based findings (missing terminals, missed cron) depend on
elapsed time — unlike the fixed-clock gate above.

## Live capture check (real home, read-only)

`scripts/live_capture_check.py` runs the whole pipeline against the real Hermes
home read-only (asserted byte-for-byte) into a throwaway outbox, and asserts the
Phase 0 envelope enrichments hold against real data: `runtime.home_mode` on
every poll event (#16), `payload.surface` on session events (#14), gateway
`channels` + `gateway_id` (#15), and that gateway start-failure detection raises
no false positive on a healthy gateway while still firing on a synthetic
failure (#13).

```bash
python scripts/live_capture_check.py -v        # defaults to $HERMES_HOME / ~/.hermes
python scripts/live_capture_check.py --hermes-home ~/.hermes-dev
```

Exit `0` if every check passes. Safe to run any time — it never writes to the
Hermes home.
