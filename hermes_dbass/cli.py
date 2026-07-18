"""Command-line entry point for the Bridge companion.

Subcommands (``init``, ``run``, ``reconcile``) arrive across the Phase 0
steps. For now this exposes ``--version`` and prints help, so packaging
and the console-script entry point can be verified end to end.
"""

from __future__ import annotations

import argparse

from . import __version__


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hermes-dbass",
        description="Bridge — the local-first companion for Hermes DBaaS.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"hermes-dbass {__version__}",
    )
    parser.add_subparsers(dest="command")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
