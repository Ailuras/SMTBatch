"""Reduction-only SMTBatch command line.

Commands:
    smtbatch serve
    smtbatch reduce prepare STUDY --output RESULTS --reducers ID... [--timeout S] [--jobs N]
    smtbatch reduce run RESULTS
    smtbatch reduce status RESULTS
    smtbatch reduce report RESULTS [--xlsx]
"""

from __future__ import annotations

import argparse


COMMANDS = ("serve", "reduce")


def main() -> int:
    parser = argparse.ArgumentParser(prog="smtbatch", description=__doc__)
    parser.add_argument("command", choices=COMMANDS, help="operation to run")
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command == "serve":
        from . import serve
        return serve.main(args.arguments)
    from . import reduce
    return reduce.main(args.arguments)


if __name__ == "__main__":
    raise SystemExit(main())
