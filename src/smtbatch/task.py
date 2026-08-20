"""Shared manifest, task-TSV, and consistency infrastructure for the batch tools."""

from __future__ import annotations

import csv
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence, TextIO

MANIFEST_FIELDS = ["task_id", "case_index", "file"]
TASK_FIELDS = ["solver", "file", "result", "time", "code", "output_path"]
JOB_FIELDS = ["job_id", "solver", "file", "expected"]
RESULT_CORE_FIELDS = ["job_id", *TASK_FIELDS]
RESULT_INCREMENTAL_FIELDS = [
    "queries",
    "sat",
    "unsat",
    "unknown",
    "error",
    "timeout",
    "unreached",
    "first",
    "last",
    "expected",
    "complete",
    "file_status",
]
RESULT_FIELDS = [*RESULT_CORE_FIELDS, *RESULT_INCREMENTAL_FIELDS]
VALID_TASK_RESULTS = {"sat", "unsat", "unknown", "timeout", "error"}
VALID_ANSWERS = {"sat", "unsat", "unknown"}
VALID_OUTCOMES = {"sat", "unsat", "unknown", "error"}
VALID_COMPLETE = {"yes", "no"}
VALID_FILE_STATUS = {"complete", "partial", "timeout", "error"}
QUERY_COUNT_FIELDS = ("sat", "unsat", "unknown", "error", "timeout", "unreached")
_CHECK_SAT_COMMANDS = frozenset({"check-sat", "check-sat-assuming"})
_TASK_NAME_RE = re.compile(r"^task_(\d+)\.tsv$")
_ERROR_LINE_RE = re.compile(r"^\(error(?:\s|$)", re.IGNORECASE)

# Result labels as recorded by smtbatch run in the task TSV (lowercase) and the
# uppercase labels used for cross-solver consistency classification.
RESULT_LABELS = {
    "sat": "SAT",
    "unsat": "UNSAT",
    "unknown": "UNKNOWN",
    "timeout": "TIMEOUT",
    "error": "ERROR",
}

CONSISTENCY_TYPES = {
    "consistent": "Consistent",
    "conflict": "Conflict",
    "hard": "Hard",
    "error": "Error",
    "other": "Other",
}


def jobs_header_ok(fieldnames: Sequence[str] | None) -> bool:
    """Accept only the current jobs.tsv header, including expected check-sat counts."""
    return list(fieldnames or []) == JOB_FIELDS


def results_header_ok(fieldnames: Sequence[str] | None) -> bool:
    """Accept only the current results.tsv header."""
    return list(fieldnames or []) == RESULT_FIELDS


def parse_outcomes(output: str) -> list[str]:
    """Collect every check-sat outcome from solver stdout: sat/unsat/unknown/error."""
    outcomes: list[str] = []
    for line in output.splitlines():
        outcome = outcome_from_line(line)
        if outcome is not None:
            outcomes.append(outcome)
    return outcomes


def parse_answers(output: str) -> list[str]:
    """Collect every SMT-LIB sat/unsat/unknown printed as the first token of a line."""
    return [outcome for outcome in parse_outcomes(output) if outcome in VALID_ANSWERS]


def outcome_from_line(line: str) -> str | None:
    """Return a check-sat outcome if this solver line is sat/unsat/unknown/error."""
    text = line.strip()
    if not text:
        return None
    if _ERROR_LINE_RE.match(text):
        return "error"
    token = text.split(maxsplit=1)[0].lower()
    if token in VALID_OUTCOMES:
        return token
    return None


def count_check_sat(path: Path) -> int:
    """Count top-level check-sat / check-sat-assuming commands in one SMT-LIB file."""
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            return _count_top_level_commands(handle, _CHECK_SAT_COMMANDS)
    except OSError:
        return 0


def _count_top_level_commands(handle: TextIO, names: frozenset[str]) -> int:
    depth = 0
    in_comment = False
    in_string = False
    string_quote_pending = False
    in_quoted_symbol = False
    token: list[str] = []
    command_name = ""
    count = 0

    def flush_token() -> None:
        nonlocal command_name
        if not token:
            return
        if depth == 1 and not command_name:
            command_name = "".join(token)
        token.clear()

    while True:
        chunk = handle.read(64 * 1024)
        if not chunk:
            break
        for char in chunk:
            if in_comment:
                if char == "\n":
                    in_comment = False
                continue
            if in_string:
                if string_quote_pending:
                    if char == '"':
                        string_quote_pending = False
                        continue
                    in_string = False
                    string_quote_pending = False
                else:
                    if char == '"':
                        string_quote_pending = True
                    continue
            if in_quoted_symbol:
                if char == "|":
                    in_quoted_symbol = False
                elif depth == 1 and not command_name:
                    token.append(char)
                continue
            if char == ";":
                flush_token()
                in_comment = True
            elif char == '"':
                flush_token()
                in_string = True
            elif char == "|":
                flush_token()
                in_quoted_symbol = True
            elif char == "(":
                flush_token()
                if depth == 0:
                    command_name = ""
                depth += 1
            elif char == ")":
                flush_token()
                if depth == 1:
                    if command_name.lower() in names:
                        count += 1
                    command_name = ""
                if depth > 0:
                    depth -= 1
            elif char.isspace():
                flush_token()
            elif depth == 1 and not command_name:
                token.append(char)
    return count


def classify_file_status(*, complete: bool, printed: int, result: str) -> str:
    """Process-level file status: complete, partial, timeout, or error."""
    if complete:
        return "complete"
    if printed > 0:
        return "partial"
    if result == "timeout":
        return "timeout"
    return "error"


def incremental_stats(
    outcomes: Sequence[str],
    expected: int,
    *,
    result: str,
    exit_ok: bool,
) -> dict[str, object]:
    """Partition one file's check-sat commands into query-level counts.

    ``sat + unsat + unknown + error + timeout + unreached == expected``.
    A GNU timeout / kill attributes at most one in-flight query to ``timeout``;
    later commands that never started are ``unreached``. A non-timeout crash
    attributes the in-flight query to ``error``. Printed ``unknown`` stays
    unknown (soft ``-t`` timeouts are not reclassified).

    ``complete`` means the process exited 0 and every expected check-sat printed
    an outcome. A process that exits 0 early is ``partial``/``error``, not complete.
    """
    expected = max(0, expected)
    capped = list(outcomes[:expected])
    sat = sum(1 for outcome in capped if outcome == "sat")
    unsat = sum(1 for outcome in capped if outcome == "unsat")
    unknown = sum(1 for outcome in capped if outcome == "unknown")
    printed_error = sum(1 for outcome in capped if outcome == "error")
    printed = len(capped)
    remaining = max(0, expected - printed)
    complete = bool(exit_ok) and remaining == 0
    timeout = 0
    extra_error = 0
    unreached = remaining
    if not complete and remaining > 0:
        if result == "timeout":
            timeout = 1
        else:
            extra_error = 1
        unreached = remaining - 1
    answers = [outcome for outcome in capped if outcome in VALID_ANSWERS]
    return {
        "queries": printed,
        "sat": sat,
        "unsat": unsat,
        "unknown": unknown,
        "error": printed_error + extra_error,
        "timeout": timeout,
        "unreached": unreached,
        "first": answers[0] if answers else "",
        "last": answers[-1] if answers else "",
        "expected": expected,
        "complete": "yes" if complete else "no",
        "file_status": classify_file_status(complete=complete, printed=printed, result=result),
    }


def incremental_tsv_fields(stats: Mapping[str, object]) -> dict[str, str]:
    """Serialize :func:`incremental_stats` (or a row view) for ``results.tsv``."""
    fields: dict[str, str] = {}
    for key in RESULT_INCREMENTAL_FIELDS:
        value = stats.get(key, "")
        fields[key] = str(value)
    return fields


def incremental_from_row(row: Mapping[str, str]) -> dict[str, object]:
    """Read the incremental columns written by the current batch runner."""
    complete = row_complete(row)
    result = (row.get("result") or "").strip().lower()
    queries = optional_nonneg_int(row.get("queries"))
    file_status = (row.get("file_status") or "").strip().lower()
    if file_status not in VALID_FILE_STATUS:
        file_status = classify_file_status(complete=complete, printed=queries, result=result)
    first = (row.get("first") or "").strip().lower()
    last = (row.get("last") or "").strip().lower()
    return {
        "queries": queries,
        "sat": optional_nonneg_int(row.get("sat")),
        "unsat": optional_nonneg_int(row.get("unsat")),
        "unknown": optional_nonneg_int(row.get("unknown")),
        "error": optional_nonneg_int(row.get("error")),
        "timeout": optional_nonneg_int(row.get("timeout")),
        "unreached": optional_nonneg_int(row.get("unreached")),
        "first": first if first in VALID_ANSWERS else "",
        "last": last if last in VALID_ANSWERS else "",
        "expected": optional_nonneg_int(row.get("expected")),
        "complete": "yes" if complete else "no",
        "file_status": file_status,
    }


def row_complete(row: Mapping[str, str]) -> bool:
    """Whether the solver finished every expected check-sat and exited 0."""
    return (row.get("complete") or "").strip().lower() == "yes"


def optional_nonneg_int(value: str | None, default: int = 0) -> int:
    if value is None or value == "":
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed >= 0 else default


def summarize_performance(
    completed_jobs: int,
    solved_jobs: int,
    solved_seconds: float,
    par2_seconds: float,
) -> dict[str, object]:
    """Build the dashboard's aggregate timing metrics from running totals."""
    return {
        "completed_jobs": completed_jobs,
        "solved_jobs": solved_jobs,
        "average_solved_seconds": round(solved_seconds / solved_jobs, 3) if solved_jobs else None,
        "par2_seconds": round(par2_seconds / completed_jobs, 3) if completed_jobs else None,
    }


@dataclass(frozen=True)
class TaskAudit:
    task_id: int
    status: str
    expected_cases: int
    completed_cases: int
    rerun_from: int
    error: str = ""

    @property
    def rerun_cases(self) -> int:
        return self.expected_cases - self.rerun_from


@dataclass(frozen=True)
class FileDetails:
    logic: str = ""
    file_size: int | None = None


@dataclass(frozen=True)
class JobSpec:
    job_id: int
    solver: str
    file_path: Path
    expected: int | None = None


@dataclass(frozen=True)
class JobAudit:
    status: str
    expected_jobs: int
    completed_jobs: int
    error: str = ""

    @property
    def pending_jobs(self) -> int:
        return self.expected_jobs - self.completed_jobs


def task_id_from_path(path: Path) -> int | None:
    match = _TASK_NAME_RE.match(path.name)
    return int(match.group(1)) if match else None


def write_manifest(path: Path, groups: Sequence[Sequence[Path]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_FIELDS, delimiter="\t")
        writer.writeheader()
        for task_id, files in enumerate(groups, start=1):
            for case_index, file_path in enumerate(files, start=1):
                writer.writerow(
                    {
                        "task_id": task_id,
                        "case_index": case_index,
                        "file": str(file_path),
                    }
                )
        handle.flush()


def load_manifest(path: Path) -> dict[int, list[Path]]:
    groups: dict[int, list[Path]] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if reader.fieldnames != MANIFEST_FIELDS:
            raise ValueError(f"invalid manifest header in {path}: {reader.fieldnames}")
        for row_number, row in enumerate(reader, start=2):
            try:
                task_id = int(row.get("task_id") or "")
                case_index = int(row.get("case_index") or "")
            except ValueError as exc:
                raise ValueError(f"invalid manifest index at {path}:{row_number}") from exc
            file_str = row.get("file") or ""
            if task_id <= 0 or case_index <= 0 or not file_str:
                raise ValueError(f"invalid manifest row at {path}:{row_number}")
            task = groups.setdefault(task_id, [])
            if case_index != len(task) + 1:
                raise ValueError(f"non-sequential case_index at {path}:{row_number}")
            task.append(Path(file_str))
    if sorted(groups) != list(range(1, len(groups) + 1)):
        raise ValueError(f"non-sequential task_id values in {path}")
    return groups


def write_jobs(path: Path, jobs: Iterable[JobSpec]) -> None:
    """Write the immutable solver-formula job queue for one batch run."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=JOB_FIELDS, delimiter="\t")
            writer.writeheader()
            for job in jobs:
                expected = job.expected if job.expected is not None else count_check_sat(job.file_path)
                writer.writerow(
                    {
                        "job_id": job.job_id,
                        "solver": job.solver,
                        "file": str(job.file_path),
                        "expected": expected,
                    }
                )
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def load_jobs(path: Path) -> list[JobSpec]:
    """Read and validate an immutable solver-formula job queue."""
    jobs: list[JobSpec] = []
    seen_pairs: set[tuple[str, str]] = set()
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if not jobs_header_ok(reader.fieldnames):
            raise ValueError(f"invalid jobs header in {path}: {reader.fieldnames}")
        for row_number, row in enumerate(reader, start=2):
            if None in row:
                raise ValueError(f"extra columns in jobs row at {path}:{row_number}")
            try:
                job_id = int(row.get("job_id") or "")
                expected = int(row.get("expected") or "")
            except ValueError as exc:
                raise ValueError(f"invalid job ID or expected count at {path}:{row_number}") from exc
            solver = (row.get("solver") or "").strip()
            file_name = row.get("file") or ""
            if job_id != len(jobs) + 1 or expected < 0 or not solver or not file_name:
                raise ValueError(f"invalid jobs row at {path}:{row_number}")
            pair = (solver, file_name)
            if pair in seen_pairs:
                raise ValueError(f"duplicate solver-file pair at {path}:{row_number}")
            seen_pairs.add(pair)
            jobs.append(JobSpec(job_id, solver, Path(file_name), expected))
    return jobs


def audit_results(jobs: Sequence[JobSpec], results_path: Path) -> JobAudit:
    """Validate a streaming results TSV against its immutable job manifest."""
    if not results_path.is_file():
        return JobAudit("missing", len(jobs), 0)
    expected = {job.job_id: job for job in jobs}
    completed: set[int] = set()
    try:
        with results_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            if not results_header_ok(reader.fieldnames):
                return JobAudit("corrupt", len(jobs), 0, f"invalid header: {reader.fieldnames}")
            for row_number, row in enumerate(reader, start=2):
                if None in row:
                    return JobAudit("corrupt", len(jobs), len(completed), f"row {row_number}: extra columns")
                try:
                    job_id = int(row.get("job_id") or "")
                except ValueError:
                    return JobAudit("corrupt", len(jobs), len(completed), f"row {row_number}: invalid job ID")
                job = expected.get(job_id)
                if job is None or job_id in completed:
                    return JobAudit("corrupt", len(jobs), len(completed), f"row {row_number}: unknown or duplicate job ID")
                error = _validate_result_row(row, str(job.file_path), job.solver)
                if error:
                    return JobAudit("corrupt", len(jobs), len(completed), f"row {row_number}: {error}")
                if job.expected is not None and optional_nonneg_int(row.get("expected")) != job.expected:
                    return JobAudit(
                        "corrupt",
                        len(jobs),
                        len(completed),
                        f"row {row_number}: expected count does not match jobs.tsv",
                    )
                completed.add(job_id)
    except (OSError, csv.Error) as exc:
        return JobAudit("corrupt", len(jobs), len(completed), f"{type(exc).__name__}: {exc}")
    if len(completed) == len(jobs):
        return JobAudit("complete", len(jobs), len(completed))
    return JobAudit("partial", len(jobs), len(completed))


def audit_task(
    task_id: int,
    expected_files: Sequence[Path],
    task_path: Path,
    expected_solver: str | None = None,
) -> TaskAudit:
    expected_count = len(expected_files)
    if not task_path.is_file():
        return TaskAudit(task_id, "missing", expected_count, 0, 0)

    completed = 0
    try:
        with task_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            if reader.fieldnames != TASK_FIELDS:
                return TaskAudit(
                    task_id,
                    "corrupt",
                    expected_count,
                    0,
                    0,
                    f"invalid header: {reader.fieldnames}",
                )
            for row_number, row in enumerate(reader, start=2):
                if completed >= expected_count:
                    return TaskAudit(task_id, "corrupt", expected_count, completed, 0, "extra result rows")
                expected_file = str(expected_files[completed])
                error = _validate_task_row(row, expected_file, expected_solver)
                if error:
                    return TaskAudit(
                        task_id,
                        "corrupt",
                        expected_count,
                        completed,
                        0,
                        f"row {row_number}: {error}",
                    )
                completed += 1
    except (OSError, csv.Error) as exc:
        return TaskAudit(task_id, "corrupt", expected_count, completed, 0, f"{type(exc).__name__}: {exc}")

    if completed == expected_count:
        return TaskAudit(task_id, "complete", expected_count, completed, expected_count)
    return TaskAudit(task_id, "partial", expected_count, completed, completed)


def _validate_task_row(row: dict[str, str], expected_file: str, expected_solver: str | None) -> str:
    if None in row:
        return "extra columns"
    solver = row.get("solver") or ""
    if not solver:
        return "missing solver"
    if expected_solver is not None and solver != expected_solver:
        return f"solver {solver!r} does not match {expected_solver!r}"
    if (row.get("file") or "") != expected_file:
        return f"file does not match manifest entry {expected_file!r}"
    result = (row.get("result") or "").lower()
    if result not in VALID_TASK_RESULTS:
        return f"invalid result {result!r}"
    try:
        float(row.get("time") or "")
    except ValueError:
        return f"invalid time {row.get('time')!r}"
    code = row.get("code") or ""
    if code:
        try:
            int(code)
        except ValueError:
            return f"invalid code {code!r}"
    return ""


def _validate_result_row(row: dict[str, str], expected_file: str, expected_solver: str | None) -> str:
    error = _validate_task_row(row, expected_file, expected_solver)
    if error:
        return error
    return _validate_incremental_row(row)


def _validate_incremental_row(row: Mapping[str, str]) -> str:
    for key in RESULT_INCREMENTAL_FIELDS:
        if key not in row:
            return f"missing {key}"
    for key in ("queries", "sat", "unsat", "unknown", "error", "timeout", "unreached", "expected"):
        raw = row.get(key)
        if raw in (None, ""):
            return f"invalid {key} {raw!r}"
        try:
            value = int(raw)
        except ValueError:
            return f"invalid {key} {raw!r}"
        if value < 0:
            return f"invalid {key} {raw!r}"
    for key in ("first", "last"):
        raw = (row.get(key) or "").strip().lower()
        if raw and raw not in VALID_ANSWERS:
            return f"invalid {key} {raw!r}"
    raw_complete = (row.get("complete") or "").strip().lower()
    if raw_complete not in VALID_COMPLETE:
        return f"invalid complete {row.get('complete')!r}"
    raw_status = (row.get("file_status") or "").strip().lower()
    if raw_status not in VALID_FILE_STATUS:
        return f"invalid file_status {row.get('file_status')!r}"
    expected = optional_nonneg_int(row.get("expected"))
    total = sum(optional_nonneg_int(row.get(key)) for key in QUERY_COUNT_FIELDS)
    if total != expected:
        return "query counts do not sum to expected"
    return ""


def manifest_path_for_task(task_path: Path) -> Path | None:
    if task_path.parent.name != "tsv":
        return None
    manifest_path = task_path.parent.parent / "manifest.tsv"
    return manifest_path if manifest_path.is_file() else None


def result_label(raw: str) -> str:
    """Normalize one task-TSV result cell to the uppercase consistency label."""
    return RESULT_LABELS.get(raw.strip().lower(), "ERROR")


def classify_consistency(results: Iterable[str]) -> str:
    """Classify cross-solver outcomes for one file into a consistency category."""
    results_set = set(r for r in results if r)
    if not results_set:
        return "Other"

    sat_count = sum(1 for r in results_set if r == "SAT")
    unsat_count = sum(1 for r in results_set if r == "UNSAT")

    if sat_count > 0 and unsat_count > 0:
        return "Conflict"

    if len(results_set) == 1:
        result = list(results_set)[0]
        if result in ("SAT", "UNSAT"):
            return "Consistent"
        if result not in ("SAT", "UNSAT", "UNKNOWN", "TIMEOUT"):
            return "Error"

    if sat_count == 0 and unsat_count == 0:
        has_valid_result = any(r in ("UNKNOWN", "TIMEOUT") for r in results_set)
        has_error = any(r not in ("SAT", "UNSAT", "UNKNOWN", "TIMEOUT") for r in results_set)
        if has_valid_result or has_error:
            return "Hard"

    return "Other"


def results_by_file(task_files: Sequence[Path]) -> dict[str, dict[str, str]]:
    """Group per-solver result labels by file path across task TSVs."""
    by_file: dict[str, dict[str, str]] = {}
    for task_file in task_files:
        with task_file.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle, delimiter="\t"):
                solver = (row.get("solver") or "").strip()
                file_str = (row.get("file") or "").strip()
                if not solver or not file_str:
                    continue
                by_file.setdefault(file_str, {})[solver] = result_label(row.get("result") or "")
    return by_file


def iter_task_tsvs(paths: Iterable[Path]) -> list[Path]:
    """Resolve inputs to complete legacy task TSVs or queue-backed results TSVs."""
    files: list[Path] = []
    seen: set[Path] = set()
    for raw in paths:
        path = raw.expanduser()
        if not path.exists():
            print(f"warning: input path not found: {path}", file=sys.stderr)
            continue
        if path.is_file():
            candidates = [path]
        elif path.is_dir():
            candidates = sorted([*path.rglob("task_*.tsv"), *path.rglob("results.tsv")])
        else:
            print(f"warning: unsupported input path ignored: {path}", file=sys.stderr)
            continue
        for candidate in candidates:
            resolved = candidate.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            files.append(resolved)
    return _complete_task_tsvs(files)


def _complete_task_tsvs(task_files: Sequence[Path]) -> list[Path]:
    """Keep only complete legacy tasks and complete queue-backed result streams."""
    complete: list[Path] = []
    manifest_cache: dict[Path, dict[int, list[Path]]] = {}
    invalid_manifests: set[Path] = set()
    for task_file in task_files:
        if task_file.name == "results.tsv":
            jobs_path = task_file.parent / "jobs.tsv"
            try:
                audit = audit_results(load_jobs(jobs_path), task_file)
            except (OSError, ValueError) as exc:
                print(f"warning: invalid job manifest; results ignored: {jobs_path}: {exc}", file=sys.stderr)
                continue
            if audit.status != "complete":
                detail = f" ({audit.error})" if audit.error else ""
                print(f"warning: {audit.status} results ignored: {task_file}{detail}", file=sys.stderr)
                continue
            complete.append(task_file)
            continue
        manifest_path = manifest_path_for_task(task_file)
        if manifest_path is None:
            complete.append(task_file)
            continue
        if manifest_path in invalid_manifests:
            continue
        try:
            if manifest_path not in manifest_cache:
                manifest_cache[manifest_path] = load_manifest(manifest_path)
            groups = manifest_cache[manifest_path]
        except (OSError, ValueError) as exc:
            print(f"warning: invalid manifest; task ignored: {manifest_path}: {exc}", file=sys.stderr)
            invalid_manifests.add(manifest_path)
            continue
        task_id = task_id_from_path(task_file)
        if task_id is None or task_id not in groups:
            print(f"warning: task absent from manifest and ignored: {task_file}", file=sys.stderr)
            continue
        audit = audit_task(task_id, groups[task_id], task_file)
        if audit.status != "complete":
            detail = f" ({audit.error})" if audit.error else ""
            print(f"warning: {audit.status} task ignored: {task_file}{detail}", file=sys.stderr)
            continue
        complete.append(task_file)

    return complete


def read_logic(handle: TextIO) -> str:
    """Return the first top-level set-logic symbol using constant memory."""
    depth = 0
    in_comment = False
    in_string = False
    string_quote_pending = False
    in_quoted_symbol = False
    token: list[str] = []
    command_tokens: list[str] = []

    def flush_token() -> None:
        if not token:
            return
        if depth == 1 and len(command_tokens) < 3:
            command_tokens.append("".join(token))
        token.clear()

    while True:
        chunk = handle.read(64 * 1024)
        if not chunk:
            break
        for char in chunk:
            if in_comment:
                if char == "\n":
                    in_comment = False
                continue
            if in_string:
                if string_quote_pending:
                    if char == '"':
                        string_quote_pending = False
                        continue
                    in_string = False
                    string_quote_pending = False
                else:
                    if char == '"':
                        string_quote_pending = True
                    continue
            if in_quoted_symbol:
                if char == "|":
                    in_quoted_symbol = False
                elif depth == 1:
                    token.append(char)
                continue
            if char == ";":
                flush_token()
                in_comment = True
            elif char == '"':
                flush_token()
                in_string = True
            elif char == "|":
                flush_token()
                in_quoted_symbol = True
            elif char == "(":
                flush_token()
                if depth == 0:
                    command_tokens = []
                depth += 1
            elif char == ")":
                flush_token()
                if depth == 1:
                    if len(command_tokens) == 2 and command_tokens[0].lower() == "set-logic":
                        return command_tokens[1]
                    command_tokens = []
                if depth > 0:
                    depth -= 1
            elif char.isspace():
                flush_token()
            elif depth == 1:
                token.append(char)
    return ""


def read_file_details(file_path: Path) -> FileDetails:
    """Read only the source-derived fields used by report and collect."""
    try:
        file_size = file_path.stat().st_size
    except OSError:
        return FileDetails()
    try:
        with file_path.open("r", encoding="utf-8", errors="replace") as handle:
            logic = read_logic(handle)
    except OSError:
        logic = ""
    return FileDetails(logic=logic, file_size=file_size)
