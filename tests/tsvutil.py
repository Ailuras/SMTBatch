"""Shared TSV fixtures for the current results/jobs schema."""

from __future__ import annotations

import csv
from pathlib import Path

from smtbatch.task import RESULT_FIELDS


def jobs_tsv(pairs: list[tuple[int | str, str, Path | str, int]]) -> str:
    lines = ["job_id\tsolver\tfile\texpected"]
    for job_id, solver, file_path, expected in pairs:
        lines.append(f"{job_id}\t{solver}\t{file_path}\t{expected}")
    return "\n".join(lines) + "\n"


def result_row(
    *,
    job_id: int,
    solver: str,
    file: Path | str,
    result: str = "sat",
    time: str = "0.5",
    code: str = "0",
    output_path: str = "",
    queries: int = 1,
    sat: int = 1,
    unsat: int = 0,
    unknown: int = 0,
    error: int = 0,
    timeout: int = 0,
    unreached: int = 0,
    first: str = "sat",
    last: str = "sat",
    expected: int = 1,
    complete: str = "yes",
    file_status: str = "complete",
) -> dict[str, str]:
    return {
        "job_id": str(job_id),
        "solver": solver,
        "file": str(file),
        "result": result,
        "time": time,
        "code": code,
        "output_path": output_path,
        "queries": str(queries),
        "sat": str(sat),
        "unsat": str(unsat),
        "unknown": str(unknown),
        "error": str(error),
        "timeout": str(timeout),
        "unreached": str(unreached),
        "first": first,
        "last": last,
        "expected": str(expected),
        "complete": complete,
        "file_status": file_status,
    }


def write_results(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=RESULT_FIELDS, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
