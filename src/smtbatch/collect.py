#!/usr/bin/env python3
"""Collect selected SMT-LIB cases directly from batch result TSVs.

Input is one or more ``<output>`` directories (or individual ``results.tsv`` /
legacy ``task_*.tsv`` files) produced by ``smtbatch run``; the same inputs
``smtbatch export`` accepts.  Per-file cross-solver consistency is classified with the shared
:mod:`smtbatch.task` logic and every file whose category matches ``--consistency`` is
copied into ``--output``, preserving the directory structure below ``--prefix``.
"""

from __future__ import annotations

import argparse
import csv
import filecmp
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .task import (
    CONSISTENCY_TYPES,
    classify_consistency,
    iter_task_tsvs,
    read_file_details,
    results_by_file,
)


MANIFEST_FIELDS = [
    "source",
    "relative_path",
    "destination",
    "logic",
    "file_size",
    "consistency",
    "status",
]


@dataclass(frozen=True)
class CollectionSummary:
    matched: int = 0
    copied: int = 0
    unchanged: int = 0
    would_copy: int = 0
    oversize: int = 0
    missing: int = 0
    outside_prefix: int = 0
    conflict: int = 0

    @property
    def errors(self) -> int:
        return self.missing + self.outside_prefix + self.conflict


def consistency_type(value: str) -> str:
    normalized = value.strip().casefold()
    if normalized not in CONSISTENCY_TYPES:
        choices = ", ".join(CONSISTENCY_TYPES.values())
        raise argparse.ArgumentTypeError(f"invalid consistency {value!r}; choose from: {choices}")
    return CONSISTENCY_TYPES[normalized]


def size_value(value: str) -> int:
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*(B|KB|MB|GB)\s*", value, flags=re.IGNORECASE)
    if match is None:
        raise argparse.ArgumentTypeError("size must use B, KB, MB, or GB, for example 100MB")
    number = float(match.group(1))
    multiplier = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3}[match.group(2).upper()]
    size = int(number * multiplier)
    if size <= 0:
        raise argparse.ArgumentTypeError("size must be greater than zero")
    return size


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "inputs",
        nargs="*",
        type=Path,
        help="one or more <output> directories (or results.tsv/task_*.tsv files). If omitted, scans 'results'.",
    )
    parser.add_argument(
        "--consistency",
        action="append",
        required=True,
        type=consistency_type,
        help="consistency type to collect; may be repeated",
    )
    parser.add_argument("--prefix", required=True, type=Path, help="source path prefix to replace")
    parser.add_argument("--output", required=True, type=Path, help="destination directory replacing --prefix")
    parser.add_argument(
        "--maxsize",
        type=size_value,
        help="maximum size of each source file, for example 500KB or 100MB",
    )
    parser.add_argument("--dry-run", action="store_true", help="validate and report without copying or writing manifest")
    return parser.parse_args(argv)


def _manifest_record(
    source: Path,
    relative_path: Path | None,
    destination: Path | None,
    logic: str,
    file_size: object,
    consistency: str,
    status: str,
) -> dict[str, object]:
    return {
        "source": str(source),
        "relative_path": str(relative_path) if relative_path is not None else "",
        "destination": str(destination) if destination is not None else "",
        "logic": logic,
        "file_size": str(file_size) if file_size is not None else "",
        "consistency": consistency,
        "status": status,
    }


def _write_manifest(output: Path, records: Iterable[dict[str, object]]) -> None:
    output.mkdir(parents=True, exist_ok=True)
    manifest_path = output / "manifest.tsv"
    temporary_path = output / ".manifest.tsv.tmp"
    try:
        with temporary_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=MANIFEST_FIELDS, delimiter="\t")
            writer.writeheader()
            writer.writerows(records)
        temporary_path.replace(manifest_path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _is_within(path: Path, directory: Path) -> bool:
    try:
        path.relative_to(directory)
    except ValueError:
        return False
    return True


def _validated_paths(prefix: Path, output: Path) -> tuple[Path, Path]:
    source_prefix = prefix.expanduser().resolve()
    output_path = output.expanduser()
    if output_path.is_symlink():
        raise ValueError(f"output must not be a symbolic link: {output}")
    destination_root = output_path.resolve()
    if not source_prefix.is_dir():
        raise ValueError(f"prefix directory not found: {prefix}")
    if _is_within(destination_root, source_prefix) or _is_within(source_prefix, destination_root):
        raise ValueError(f"output and prefix must not overlap: {destination_root} and {source_prefix}")
    return source_prefix, destination_root


def _reset_output(output: Path) -> None:
    if output.exists() and not output.is_dir():
        raise ValueError(f"output exists but is not a directory: {output}")
    output.mkdir(parents=True, exist_ok=True)
    for child in output.iterdir():
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child)
        else:
            child.unlink()


def collect_cases(
    inputs: list[Path],
    consistencies: Iterable[str],
    prefix: Path,
    output: Path,
    *,
    max_size: int | None = None,
    dry_run: bool = False,
) -> CollectionSummary:
    selected = {value.casefold() for value in consistencies}
    if not selected:
        raise ValueError("at least one consistency type is required")
    source_prefix, destination_root = _validated_paths(prefix, output)
    task_files = iter_task_tsvs(inputs or [Path("results")])
    by_file = results_by_file(task_files)
    if not by_file:
        raise ValueError("no result rows found in the given inputs")

    counters = {field: 0 for field in CollectionSummary.__dataclass_fields__}
    records: list[dict[str, object]] = []
    if not dry_run:
        _reset_output(destination_root)

    for file_key in sorted(by_file):
        labels = list(by_file[file_key].values())
        consistency = classify_consistency(labels)
        if consistency.casefold() not in selected:
            continue
        counters["matched"] += 1

        source = Path(file_key).expanduser().resolve()
        try:
            relative_path = source.relative_to(source_prefix)
        except ValueError:
            counters["outside_prefix"] += 1
            records.append(_manifest_record(source, None, None, "", None, consistency, "outside_prefix"))
            continue

        destination = destination_root / relative_path
        details = read_file_details(source)
        if not source.is_file():
            counters["missing"] += 1
            records.append(_manifest_record(source, relative_path, destination, "", None, consistency, "missing"))
            continue
        if max_size is not None and source.stat().st_size > max_size:
            counters["oversize"] += 1
            records.append(_manifest_record(source, relative_path, destination, details.logic, details.file_size, consistency, "oversize"))
            continue
        if dry_run:
            counters["would_copy"] += 1
            records.append(_manifest_record(source, relative_path, destination, details.logic, details.file_size, consistency, "would_copy"))
            continue
        if destination.exists():
            if destination.is_file() and filecmp.cmp(source, destination, shallow=False):
                counters["unchanged"] += 1
                records.append(_manifest_record(source, relative_path, destination, details.logic, details.file_size, consistency, "unchanged"))
            else:
                counters["conflict"] += 1
                records.append(_manifest_record(source, relative_path, destination, details.logic, details.file_size, consistency, "conflict"))
            continue

        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        counters["copied"] += 1
        records.append(_manifest_record(source, relative_path, destination, details.logic, details.file_size, consistency, "copied"))

    if not dry_run:
        _write_manifest(destination_root, records)
    return CollectionSummary(**counters)


def _default_results_root() -> Path:
    try:
        from .config import load_config

        return load_config().results_root
    except RuntimeError:
        return Path("results")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        summary = collect_cases(
            args.inputs or [_default_results_root()],
            args.consistency,
            args.prefix,
            args.output,
            max_size=args.maxsize,
            dry_run=args.dry_run,
        )
    except (OSError, ValueError) as exc:
        print(f"error: {exc}")
        return 1

    mode = "dry-run" if args.dry_run else "collect"
    print(
        f"[{mode}] matched={summary.matched} copied={summary.copied} "
        f"unchanged={summary.unchanged} would_copy={summary.would_copy} oversize={summary.oversize} "
        f"missing={summary.missing} outside_prefix={summary.outside_prefix} conflict={summary.conflict}"
    )
    return 1 if summary.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
