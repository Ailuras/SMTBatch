"""Unified command-line entry point for smtbatch.

Examples:
    smtbatch run --solver z3 --input benchmarks --output results/demo
    smtbatch serve
    smtbatch export results/demo --output exports/demo.xlsx
    smtbatch collect --consistency hard --prefix benchmarks --output selected
"""

from __future__ import annotations

import argparse

COMMANDS = ("run", "serve", "export", "collect")


def main() -> int:
    parser = argparse.ArgumentParser(prog="smtbatch", description=__doc__)
    parser.add_argument("command", choices=COMMANDS, help="operation to run")
    parser.add_argument("arguments", nargs=argparse.REMAINDER, help="arguments passed through to the operation")
    args = parser.parse_args()
    if args.command == "run":
        from . import run
        return run.main(args.arguments)
    if args.command == "serve":
        from . import serve
        return serve.main(args.arguments)
    if args.command == "export":
        from . import report
        return report.main(args.arguments)
    from . import collect
    return collect.main(args.arguments)


if __name__ == "__main__":
    raise SystemExit(main())
