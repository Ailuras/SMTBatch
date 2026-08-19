"""Run a bounded parallel queue of solver-formula jobs.

Every selected SMT-LIB file is paired with every requested solver and becomes
one independently scheduled job. Results are streamed to ``results.tsv`` as
jobs complete; ``jobs.tsv`` is the immutable queue manifest. ``progress.json``
is refreshed while the queue runs for the dashboard UI.

Incremental SMT-LIB files (many ``check-sat`` commands in one file) are scored
at file granularity: the process must finish the whole file, and only the last
answer counts as sat/unsat. Intermediate ``unknown`` is allowed. A timeout or
kill is never a file success, even if a sat/unsat line was already printed.

Solver names and commands come from the nearest project ``smtbatch.toml``;
see smtbatch.config for the schema.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import fcntl
import hashlib
import json
import math
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
from collections import Counter, deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

from .config import Config, SolverSpec, load_config
from .task import (
    RESULT_FIELDS,
    JobSpec,
    count_check_sat,
    incremental_from_answers,
    load_jobs,
    optional_nonneg_int,
    parse_answers,
    results_header_ok,
    row_complete,
    summarize_performance,
    write_jobs,
)


RESULT_ORDER = ("sat", "unsat", "unknown", "timeout", "error")
SOLVER_BUNDLE_SCHEMA = "linked-artifacts-v1"


@contextmanager
def _defer_sigint():
    """Make one queue bookkeeping operation atomic with respect to cancellation."""
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
    try:
        yield
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)


@dataclass
class IncrementalCounts:
    file_complete: int = 0
    partial_timeout: int = 0
    answers: int = 0
    expected: int = 0

    def add(
        self,
        *,
        result: str,
        queries: int,
        expected: int,
        complete: bool,
    ) -> None:
        self.file_complete += int(complete)
        self.partial_timeout += int(result == "timeout" and queries > 0)
        self.answers += queries
        self.expected += expected

    def as_dict(self) -> dict[str, int]:
        return {
            "file_complete": self.file_complete,
            "partial_timeout": self.partial_timeout,
            "answers": self.answers,
            "expected": self.expected,
        }


@dataclass(frozen=True)
class JobResult:
    job: JobSpec
    duration_sec: float
    result: str
    code: int | None
    output: str
    queries: int = 0
    sat: int = 0
    unsat: int = 0
    unknown: int = 0
    first: str = ""
    last: str = ""
    expected: int = 0
    complete: bool = False


@dataclass
class ProgressTracker:
    output_dir: Path
    solvers: tuple[str, ...]
    total_jobs: int
    refresh_seconds: float
    recent_limit: int
    timeout: float = 0.0
    started_monotonic: float = field(default_factory=time.monotonic)
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    status: str = "running"
    completed_jobs: int = 0
    running: dict[int, JobSpec] = field(default_factory=dict)
    outcomes: Counter[str] = field(default_factory=Counter)
    by_solver: dict[str, Counter[str]] = field(default_factory=dict)
    solved_seconds: dict[str, float] = field(default_factory=dict)
    par2_seconds: dict[str, float] = field(default_factory=dict)
    recent_errors: deque[dict[str, object]] = field(default_factory=deque)
    incremental: IncrementalCounts = field(default_factory=IncrementalCounts)
    by_solver_incremental: dict[str, IncrementalCounts] = field(default_factory=dict)
    _last_render: float = 0.0

    def __post_init__(self) -> None:
        self.by_solver = {solver: Counter() for solver in self.solvers}
        self.solved_seconds = {solver: 0.0 for solver in self.solvers}
        self.par2_seconds = {solver: 0.0 for solver in self.solvers}
        self.by_solver_incremental = {solver: IncrementalCounts() for solver in self.solvers}
        self.recent_errors = deque(maxlen=self.recent_limit)

    @property
    def pending_jobs(self) -> int:
        return self.total_jobs - self.completed_jobs - len(self.running)

    def start(self, job: JobSpec) -> None:
        self.running[job.job_id] = job

    def finish(self, result: JobResult, log_path: Path | None) -> None:
        self.running.pop(result.job.job_id, None)
        self.completed_jobs += 1
        self.outcomes[result.result] += 1
        self.by_solver[result.job.solver][result.result] += 1
        self.incremental.add(
            result=result.result,
            queries=result.queries,
            expected=result.expected,
            complete=result.complete,
        )
        self.by_solver_incremental.setdefault(result.job.solver, IncrementalCounts()).add(
            result=result.result,
            queries=result.queries,
            expected=result.expected,
            complete=result.complete,
        )
        if result.result in {"sat", "unsat"}:
            self.solved_seconds[result.job.solver] += result.duration_sec
            self.par2_seconds[result.job.solver] += result.duration_sec
        else:
            self.par2_seconds[result.job.solver] += 2 * self.timeout
        record = {
            "job_id": result.job.job_id,
            "solver": result.job.solver,
            "file": str(result.job.file_path),
            "result": result.result,
            "time": round(result.duration_sec, 3),
            "code": result.code,
            "queries": result.queries,
            "last": result.last,
            "expected": result.expected,
            "complete": "yes" if result.complete else "no",
            "log_path": str(log_path) if log_path else "",
        }
        if result.result in {"timeout", "error"}:
            self.recent_errors.appendleft({**record, "output": result.output[-2000:]})

    def snapshot(self) -> dict[str, object]:
        elapsed = time.monotonic() - self.started_monotonic
        by_solver_performance = {}
        for solver, counts in self.by_solver.items():
            completed = sum(counts.values())
            solved = counts["sat"] + counts["unsat"]
            by_solver_performance[solver] = summarize_performance(
                completed,
                solved,
                self.solved_seconds[solver],
                self.par2_seconds[solver],
            )
        solved_jobs = self.outcomes["sat"] + self.outcomes["unsat"]
        return {
            "format": "pair-queue-progress-v1",
            "output_dir": str(self.output_dir),
            "status": self.status,
            "started_at": self.started_at,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "elapsed_seconds": round(elapsed, 3),
            "total_jobs": self.total_jobs,
            "completed_jobs": self.completed_jobs,
            "running_jobs": len(self.running),
            "pending_jobs": self.pending_jobs,
            "outcomes": dict(self.outcomes),
            "by_solver": {solver: dict(counts) for solver, counts in self.by_solver.items()},
            "performance": summarize_performance(
                self.completed_jobs,
                solved_jobs,
                sum(self.solved_seconds.values()),
                sum(self.par2_seconds.values()),
            ),
            "by_solver_performance": by_solver_performance,
            "incremental": {
                **self.incremental.as_dict(),
                "file_solved": solved_jobs,
                "by_solver": {
                    solver: counts.as_dict() for solver, counts in self.by_solver_incremental.items()
                },
            },
            "recent_errors": list(self.recent_errors),
        }

    def render(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_render < self.refresh_seconds:
            return
        self._last_render = now
        snapshot = self.snapshot()
        _atomic_write_text(
            self.output_dir / "progress.json",
            json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--solver",
        action="append",
        help="solver name from the nearest smtbatch.toml; repeat to schedule every solver-file pair "
        "(required unless --resume is used)",
    )
    parser.add_argument(
        "--input",
        action="append",
        type=Path,
        help="directory to scan for .smt2 files; repeat for multiple folders (required unless --resume is used)",
    )
    parser.add_argument("--output", type=Path, default=Path("results"), help="output directory (default: results)")
    parser.add_argument("--timeout", type=float, default=10.0, help="per-job solver timeout in seconds")
    parser.add_argument("--jobs", type=int, default=max(1, (os.cpu_count() or 2) // 2), help="maximum concurrent solver jobs")
    parser.add_argument("--limit", type=int, default=0, help="maximum number of selected files (0 = no limit)")
    parser.add_argument(
        "--log",
        choices=("fail", "all", "none"),
        default="fail",
        help="solver output logging policy: fail (default), all, or none",
    )
    parser.add_argument(
        "--hash-inputs",
        action="store_true",
        help="write input_hashes.tsv with SHA-256 and byte size before running",
    )
    parser.add_argument(
        "--progress-interval",
        type=float,
        default=1.0,
        help="minimum seconds between progress.json refreshes (default: 1)",
    )
    parser.add_argument(
        "--recent-limit",
        type=int,
        default=20,
        help="number of recent errors retained for the dashboard (default: 20)",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=25,
        help="fsync results.tsv after this many completed jobs (default: 25)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="resume an interrupted batch in --output: rerun only the jobs missing from results.tsv; "
        "cannot be combined with --input/--solver/--hash-inputs (recovered from the job queue)",
    )
    return parser.parse_args(argv)


def normalize_solvers(values: Iterable[str], config: Config) -> tuple[str, ...]:
    solvers: list[str] = []
    for raw in values:
        solver = raw.strip()
        if solver not in config.solvers:
            known = ", ".join(sorted(config.solvers))
            raise ValueError(f"unknown solver: {raw!r} (configured: {known})")
        if solver not in solvers:
            solvers.append(solver)
    if not solvers:
        raise ValueError("at least one solver is required")
    return tuple(solvers)


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def solver_artifacts(binary: Path) -> dict[str, dict[str, str]]:
    """Hash an executable and every file-backed dependency reported by ldd."""
    artifacts = {
        "binary": {
            "path": str(binary.resolve()),
            "sha256": sha256_path(binary),
        }
    }
    try:
        completed = subprocess.run(
            ["ldd", str(binary)],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return artifacts
    indirect = re.compile(r"^\s*(\S+)\s+=>\s+(\S+)")
    direct = re.compile(r"^\s*(/\S+)\s+\(")
    for line in completed.stdout.splitlines():
        match = indirect.match(line)
        if match is not None:
            name, raw_path = match.groups()
        else:
            match = direct.match(line)
            if match is None:
                continue
            raw_path = match.group(1)
            name = Path(raw_path).name
        dependency = Path(raw_path)
        if dependency.is_file():
            artifacts[name] = {
                "path": str(dependency.resolve()),
                "sha256": sha256_path(dependency),
            }
    return dict(sorted(artifacts.items()))


def solver_bundle_hash(artifacts: dict[str, dict[str, str]]) -> str:
    """Hash artifact identities and contents, independently of host paths."""
    digest = hashlib.sha256()
    for name, artifact in sorted(artifacts.items()):
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(artifact["sha256"].encode("ascii"))
        digest.update(b"\0")
    return digest.hexdigest()


def solver_provenance(
    spec: SolverSpec,
    artifact_cache: dict[Path, dict[str, dict[str, str]]] | None = None,
) -> dict[str, str]:
    completed = subprocess.run(
        [str(spec.binary), *spec.version_args],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=30,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"solver version query failed for {spec.name}: {completed.stdout.strip()}")
    version = completed.stdout.splitlines()[0].strip() if completed.stdout.splitlines() else ""
    artifacts = None if artifact_cache is None else artifact_cache.get(spec.binary)
    if artifacts is None:
        artifacts = solver_artifacts(spec.binary)
        if artifact_cache is not None:
            artifact_cache[spec.binary] = artifacts
    return {
        "solver_binary": str(spec.binary),
        "solver_binary_sha256": sha256_path(spec.binary),
        "solver_artifacts_json": json.dumps(artifacts, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
        "solver_bundle_schema": SOLVER_BUNDLE_SCHEMA,
        "solver_bundle_sha256": solver_bundle_hash(artifacts),
        "solver_version": version,
        "solver_command_json": json.dumps(list(spec.command), ensure_ascii=False, separators=(",", ":")),
    }


def _git_provenance(root: Path) -> dict[str, str]:
    commit = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    status = subprocess.run(
        ["git", "-C", str(root), "status", "--porcelain"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return {
        "commit": commit.stdout.strip() if commit.returncode == 0 else "",
        "dirty": "yes" if status.returncode != 0 or status.stdout.strip() else "no",
    }


def repository_provenance() -> dict[str, str]:
    project = _git_provenance(Path.cwd())
    runner = _git_provenance(Path(__file__).resolve().parent)
    return {
        "repository_commit": project["commit"],
        "repository_dirty": project["dirty"],
        "runner_repository_commit": runner["commit"],
        "runner_repository_dirty": runner["dirty"],
    }


def write_input_hashes(path: Path, files: Sequence[Path]) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["file", "bytes", "sha256"], delimiter="\t")
            writer.writeheader()
            for file_path in files:
                writer.writerow(
                    {
                        "file": str(file_path),
                        "bytes": file_path.stat().st_size,
                        "sha256": sha256_path(file_path),
                    }
                )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def classify_output(output: str) -> str:
    answers = parse_answers(output)
    return answers[-1] if answers else "error"


def _job_result_from_output(
    job: JobSpec,
    duration_sec: float,
    result: str,
    code: int | None,
    output: str,
) -> JobResult:
    answers = parse_answers(output)
    expected = count_check_sat(job.file_path)
    complete = code == 0
    stats = incremental_from_answers(answers, expected, complete=complete)
    return JobResult(
        job,
        duration_sec,
        result,
        code,
        output,
        queries=int(stats["queries"]),
        sat=int(stats["sat"]),
        unsat=int(stats["unsat"]),
        unknown=int(stats["unknown"]),
        first=stats["first"],
        last=stats["last"],
        expected=expected,
        complete=complete,
    )


def _result_row(item: JobResult, log_path: Path | None) -> dict[str, str]:
    return {
        "job_id": str(item.job.job_id),
        "solver": item.job.solver,
        "file": str(item.job.file_path),
        "result": item.result,
        "time": f"{item.duration_sec:.3f}",
        "code": "" if item.code is None else str(item.code),
        "output_path": str(log_path) if log_path else "",
        "queries": str(item.queries),
        "sat": str(item.sat),
        "unsat": str(item.unsat),
        "unknown": str(item.unknown),
        "first": item.first,
        "last": item.last,
        "expected": str(item.expected),
        "complete": "yes" if item.complete else "no",
    }


def discover_files(input_dirs: Sequence[Path], limit: int) -> list[Path]:
    files: list[Path] = []
    seen: set[Path] = set()
    for raw_dir in input_dirs:
        input_dir = raw_dir.expanduser().resolve()
        if not input_dir.is_dir():
            raise FileNotFoundError(f"input directory not found: {input_dir}")
        for path in sorted(input_dir.rglob("*.smt2")):
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            files.append(resolved)
            if limit > 0 and len(files) >= limit:
                return files
    return files


def iter_jobs(files: Sequence[Path], solvers: Sequence[str]) -> Iterable[JobSpec]:
    for job_id, (file_path, solver) in enumerate(
        ((file_path, solver) for file_path in files for solver in solvers),
        start=1,
    ):
        yield JobSpec(job_id, solver, file_path)


def run_job(spec: SolverSpec, job: JobSpec, timeout: float, outer_timeout: float) -> JobResult:
    start = time.perf_counter()
    code: int | None = None
    output = ""
    command = spec.render(job.file_path, timeout)
    try:
        process = subprocess.Popen(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            output = process.communicate(timeout=outer_timeout)[0] or ""
            code = process.returncode
            if code in {124, 137, 143}:
                result = "timeout"
            elif code == 0:
                result = classify_output(output)
            else:
                result = "error"
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            output = process.communicate()[0] or ""
            result = "timeout"
    except Exception as exc:  # noqa: BLE001 - preserve launcher failures in the result stream.
        output = f"{type(exc).__name__}: {exc}\n"
        result = "error"
    return _job_result_from_output(job, time.perf_counter() - start, result, code, output)


def write_job_output(logs_dir: Path, policy: str, item: JobResult) -> Path | None:
    if policy == "none" or (policy == "fail" and item.result not in {"timeout", "error"}):
        return None
    path = logs_dir / f"job_{item.job.job_id:07d}.out"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(item.output, encoding="utf-8", errors="replace")
    return path


def _atomic_write_text(path: Path, text: str) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        temporary.write_text(text, encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def clean_previous_outputs(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for name in (
        "jobs.tsv",
        "results.tsv",
        "input_hashes.tsv",
        "metadata.txt",
        "progress.json",
        "resume_history.jsonl",
        "manifest.tsv",
    ):
        (output_dir / name).unlink(missing_ok=True)
    for name in ("logs", "tsv", "summary", "failures"):
        path = output_dir / name
        if path.is_symlink():
            raise ValueError(f"refusing to remove symlinked batch output path: {path}")
        if path.is_dir():
            shutil.rmtree(path)


def write_metadata(output_dir: Path, metadata: dict[str, str]) -> None:
    _atomic_write_text(output_dir / "metadata.txt", "\n".join(f"{key}={value}" for key, value in metadata.items()) + "\n")


class _RunLock:
    """Cross-process run lock with a dashboard-readable controller pid."""

    def __init__(self, output_dir: Path) -> None:
        self.output_dir = output_dir
        self.pid_path = output_dir / ".run.pid"
        self._handle = None

    def acquire(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        handle = (self.output_dir / ".run.lock").open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            raise ValueError(f"another batch controller is already using {self.output_dir}") from None
        try:
            handle.seek(0)
            handle.truncate()
            handle.write(f"{os.getpid()}\n")
            handle.flush()
            os.fsync(handle.fileno())
            _atomic_write_text(self.pid_path, f"{os.getpid()}\n")
        except Exception:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()
            raise
        self._handle = handle

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return
        try:
            try:
                owner = int(self.pid_path.read_text(encoding="utf-8").strip())
            except (OSError, ValueError):
                owner = None
            if owner == os.getpid():
                self.pid_path.unlink(missing_ok=True)
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()
            self._handle = None


def _read_metadata(path: Path) -> dict[str, str]:
    """Read a key=value metadata.txt, keeping the first occurrence of each key."""
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        key, separator, value = line.partition("=")
        if separator and key and key not in values:
            values[key] = value
    return values


def _load_existing_results(
    results_path: Path,
    jobs: Sequence[JobSpec],
    timeout: float,
) -> tuple[
    set[int],
    Counter[str],
    dict[str, Counter[str]],
    dict[str, float],
    dict[str, float],
    IncrementalCounts,
    dict[str, IncrementalCounts],
]:
    """Validate and tally a streaming results file against its immutable queue."""
    outcomes: Counter[str] = Counter()
    by_solver: dict[str, Counter[str]] = {}
    solved_seconds: dict[str, float] = {}
    par2_seconds: dict[str, float] = {}
    incremental = IncrementalCounts()
    by_solver_incremental: dict[str, IncrementalCounts] = {}
    completed: set[int] = set()
    if not results_path.is_file():
        return (
            completed,
            outcomes,
            by_solver,
            solved_seconds,
            par2_seconds,
            incremental,
            by_solver_incremental,
        )
    expected = {job.job_id: job for job in jobs}
    try:
        with results_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            if not results_header_ok(reader.fieldnames):
                raise ValueError(f"invalid results header: {reader.fieldnames}")
            for row_number, row in enumerate(reader, start=2):
                if None in row or any(value is None for value in row.values()):
                    raise ValueError(f"malformed result row at {results_path}:{row_number}")
                try:
                    job_id = int(row.get("job_id") or "")
                except ValueError:
                    raise ValueError(f"invalid job_id at {results_path}:{row_number}") from None
                job = expected.get(job_id)
                if job is None or job_id in completed:
                    raise ValueError(f"unknown or duplicate job_id at {results_path}:{row_number}")
                solver = row.get("solver") or ""
                if solver != job.solver or (row.get("file") or "") != str(job.file_path):
                    raise ValueError(f"result does not match jobs.tsv at {results_path}:{row_number}")
                result = (row.get("result") or "").lower()
                if result not in RESULT_ORDER:
                    raise ValueError(f"invalid result at {results_path}:{row_number}: {result!r}")
                try:
                    duration = float(row.get("time") or "")
                except ValueError:
                    raise ValueError(f"invalid time at {results_path}:{row_number}") from None
                if not math.isfinite(duration) or duration < 0:
                    raise ValueError(f"invalid time at {results_path}:{row_number}")
                code = row.get("code") or ""
                if code:
                    try:
                        int(code)
                    except ValueError:
                        raise ValueError(f"invalid code at {results_path}:{row_number}") from None
                queries = optional_nonneg_int(row.get("queries"))
                expected_queries = optional_nonneg_int(row.get("expected"))
                complete = row_complete(row)
                completed.add(job_id)
                outcomes[result] += 1
                by_solver.setdefault(solver, Counter())[result] += 1
                solved_seconds.setdefault(solver, 0.0)
                par2_seconds.setdefault(solver, 0.0)
                incremental.add(
                    result=result,
                    queries=queries,
                    expected=expected_queries,
                    complete=complete,
                )
                by_solver_incremental.setdefault(solver, IncrementalCounts()).add(
                    result=result,
                    queries=queries,
                    expected=expected_queries,
                    complete=complete,
                )
                if result in {"sat", "unsat"}:
                    solved_seconds[solver] += duration
                    par2_seconds[solver] += duration
                else:
                    par2_seconds[solver] += 2 * timeout
    except csv.Error as exc:
        raise ValueError(f"invalid results TSV {results_path}: {exc}") from None
    return (
        completed,
        outcomes,
        by_solver,
        solved_seconds,
        par2_seconds,
        incremental,
        by_solver_incremental,
    )


def _results_writer_fields(results_path: Path, append_results: bool) -> list[str]:
    """Keep appending to an existing header; new runs write the incremental columns."""
    if append_results and results_path.is_file() and results_path.stat().st_size:
        with results_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            if not results_header_ok(reader.fieldnames):
                raise ValueError(f"invalid results header: {reader.fieldnames}")
            return list(reader.fieldnames or RESULT_FIELDS)
    return list(RESULT_FIELDS)


def _resume_config(metadata: dict[str, str]) -> Config:
    """Reload the exact solver configuration recorded by the original run."""
    recorded = metadata.get("solver_config")
    if not recorded:
        raise ValueError("metadata has no recorded solver_config")
    config_path = Path(recorded).expanduser().resolve()
    if not config_path.is_file():
        raise ValueError(f"recorded solver config not found: {config_path}")
    config = load_config(config_path.parent)
    if config.path != config_path:
        raise ValueError(f"unable to load recorded solver config: {config_path}")
    return config


def _verify_solver_provenance(
    name: str,
    spec: SolverSpec,
    metadata: dict[str, str],
    binary_hashes: dict[Path, str],
    artifact_cache: dict[Path, dict[str, dict[str, str]]],
) -> None:
    """Reject a resume when an executable or recorded command has drifted."""
    prefix = f"{name}_"
    recorded_binary = metadata.get(prefix + "solver_binary")
    recorded_sha256 = metadata.get(prefix + "solver_binary_sha256")
    if not recorded_binary or not recorded_sha256:
        raise ValueError(f"metadata has no complete solver provenance for {name}")
    if Path(recorded_binary).expanduser().resolve() != spec.binary:
        raise ValueError(f"solver binary changed since the run started: {name}")
    current_sha256 = binary_hashes.get(spec.binary)
    if current_sha256 is None:
        current_sha256 = sha256_path(spec.binary)
        binary_hashes[spec.binary] = current_sha256
    if current_sha256 != recorded_sha256:
        raise ValueError(f"solver binary hash changed since the run started: {name}")
    recorded_bundle = metadata.get(prefix + "solver_bundle_sha256")
    if recorded_bundle:
        if metadata.get(prefix + "solver_bundle_schema") != SOLVER_BUNDLE_SCHEMA:
            raise ValueError(f"unsupported solver bundle schema for {name}")
        artifacts = artifact_cache.get(spec.binary)
        if artifacts is None:
            artifacts = solver_artifacts(spec.binary)
            artifact_cache[spec.binary] = artifacts
        if solver_bundle_hash(artifacts) != recorded_bundle:
            raise ValueError(f"solver linked-artifact bundle changed since the run started: {name}")
    recorded_command = metadata.get(prefix + "solver_command_json")
    if recorded_command:
        try:
            command = json.loads(recorded_command)
        except json.JSONDecodeError:
            raise ValueError(f"invalid recorded solver command for {name}") from None
        if command != list(spec.command):
            raise ValueError(f"solver command changed since the run started: {name}")


def _append_resume_history(output_dir: Path, record: dict[str, object]) -> None:
    """Append one immutable resume attempt record without rewriting initial metadata."""
    path = output_dir / "resume_history.jsonl"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _ensure_append_boundary(path: Path) -> None:
    """Ensure a validated TSV's last record is separated from newly appended rows."""
    if not path.is_file() or path.stat().st_size == 0:
        return
    with path.open("rb+") as handle:
        handle.seek(-1, os.SEEK_END)
        if handle.read(1) not in {b"\n", b"\r"}:
            handle.seek(0, os.SEEK_END)
            handle.write(b"\n")
            handle.flush()
            os.fsync(handle.fileno())


@dataclass
class _RunPlan:
    """Everything main needs to run the queue, whether starting fresh or resuming."""

    output_dir: Path
    logs_dir: Path
    jobs: list[JobSpec]
    remaining: list[JobSpec]
    specs: dict[str, SolverSpec]
    solvers: tuple[str, ...]
    timeout: float
    log: str
    selected_file_count: int
    pair_count: int
    completed_before: int
    outcomes_before: Counter[str]
    by_solver_before: dict[str, Counter[str]]
    solved_seconds_before: dict[str, float]
    par2_seconds_before: dict[str, float]
    incremental_before: IncrementalCounts
    by_solver_incremental_before: dict[str, IncrementalCounts]
    append_results: bool


def _prepare_fresh(args: argparse.Namespace) -> _RunPlan:
    if not args.solver or not args.input:
        raise ValueError("--solver and --input are required unless --resume is used")
    config = load_config()
    solvers = normalize_solvers(args.solver, config)
    specs = {name: config.solvers[name] for name in solvers}
    artifact_cache: dict[Path, dict[str, dict[str, str]]] = {}
    provenance = {
        name: solver_provenance(spec, artifact_cache) for name, spec in specs.items()
    }
    files = discover_files(args.input, args.limit)
    if not files:
        raise ValueError("no .smt2 files selected")
    pair_count = len(files) * len(solvers)
    output_dir = args.output.expanduser().resolve()
    logs_dir = output_dir / "logs"
    clean_previous_outputs(output_dir)
    jobs = list(iter_jobs(files, solvers))
    write_jobs(output_dir / "jobs.tsv", jobs)
    if args.hash_inputs:
        write_input_hashes(output_dir / "input_hashes.tsv", files)
    metadata = {
        "format": "pair-queue-v1",
        "host": socket.gethostname().split(".")[0],
        "solvers": ",".join(solvers),
        "timeout": f"{args.timeout:g}",
        "jobs": str(args.jobs),
        "selected_files": str(len(files)),
        "solver_formula_pairs": str(pair_count),
        "log": args.log,
        "input_hashes": "input_hashes.tsv" if args.hash_inputs else "",
        "solver_config": str(config.path),
        "incremental": "yes",
    }
    for solver, values in provenance.items():
        for key, value in values.items():
            metadata[f"{solver}_{key}"] = value
    metadata.update(repository_provenance())
    write_metadata(output_dir, metadata)
    return _RunPlan(
        output_dir=output_dir,
        logs_dir=logs_dir,
        jobs=jobs,
        remaining=jobs,
        specs=specs,
        solvers=solvers,
        timeout=args.timeout,
        log=args.log,
        selected_file_count=len(files),
        pair_count=pair_count,
        completed_before=0,
        outcomes_before=Counter(),
        by_solver_before={},
        solved_seconds_before={},
        par2_seconds_before={},
        incremental_before=IncrementalCounts(),
        by_solver_incremental_before={},
        append_results=False,
    )


def _prepare_resume(args: argparse.Namespace) -> _RunPlan:
    if args.input or args.solver:
        raise ValueError("--resume cannot be combined with --input or --solver (recovered from jobs.tsv)")
    if args.hash_inputs:
        raise ValueError("--hash-inputs cannot be combined with --resume")
    output_dir = args.output.expanduser().resolve()
    if not output_dir.is_dir():
        raise ValueError(f"output directory not found: {output_dir}")
    jobs_path = output_dir / "jobs.tsv"
    if not jobs_path.is_file():
        raise ValueError(f"no job queue found to resume: {jobs_path}")
    jobs = load_jobs(jobs_path)
    solvers = tuple(dict.fromkeys(job.solver for job in jobs))
    metadata = _read_metadata(output_dir / "metadata.txt")
    config = _resume_config(metadata)
    missing = [name for name in solvers if name not in config.solvers]
    if missing:
        raise ValueError(f"resume needs solver definitions missing from {config.path}: {', '.join(missing)}")
    specs = {name: config.solvers[name] for name in solvers}
    binary_hashes: dict[Path, str] = {}
    artifact_cache: dict[Path, dict[str, dict[str, str]]] = {}
    for name, spec in specs.items():
        _verify_solver_provenance(
            name, spec, metadata, binary_hashes, artifact_cache
        )
    try:
        timeout = float(metadata.get("timeout") or "")
    except ValueError:
        raise ValueError(f"invalid timeout in metadata: {metadata.get('timeout')!r}") from None
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError(f"missing or invalid timeout in metadata: {timeout}")
    (
        completed_ids,
        outcomes_before,
        by_solver_before,
        solved_seconds_before,
        par2_seconds_before,
        incremental_before,
        by_solver_incremental_before,
    ) = _load_existing_results(output_dir / "results.tsv", jobs, timeout)
    remaining = [job for job in jobs if job.job_id not in completed_ids]
    resumed_at = datetime.now(timezone.utc).isoformat()
    _append_resume_history(
        output_dir,
        {
            "event": "resume_started",
            "at": resumed_at,
            "pid": os.getpid(),
            "workers": args.jobs,
            "completed_before": len(completed_ids),
            "remaining_jobs": len(remaining),
            "solver_config": str(config.path),
        },
    )
    return _RunPlan(
        output_dir=output_dir,
        logs_dir=output_dir / "logs",
        jobs=jobs,
        remaining=remaining,
        specs=specs,
        solvers=solvers,
        timeout=timeout,
        log=metadata.get("log", "fail"),
        selected_file_count=len({job.file_path for job in jobs}),
        pair_count=len(jobs),
        completed_before=len(completed_ids),
        outcomes_before=outcomes_before,
        by_solver_before=by_solver_before,
        solved_seconds_before=solved_seconds_before,
        par2_seconds_before=par2_seconds_before,
        incremental_before=incremental_before,
        by_solver_incremental_before=by_solver_incremental_before,
        append_results=(output_dir / "results.tsv").is_file(),
    )


def run_queue(
    jobs: Iterable[JobSpec],
    solvers: dict[str, SolverSpec],
    *,
    timeout: float,
    workers: int,
    logs_dir: Path,
    log_policy: str,
    checkpoint_every: int,
    tracker: ProgressTracker | None,
    results_path: Path,
    append_results: bool = False,
) -> Counter[str]:
    outer_timeout = timeout + max(15.0, timeout * 0.5)
    pending = iter(jobs)
    in_flight: dict[concurrent.futures.Future[JobResult], JobSpec] = {}
    outcomes: Counter[str] = Counter()
    completed = 0

    if append_results:
        _ensure_append_boundary(results_path)
    fieldnames = _results_writer_fields(results_path, append_results)
    with results_path.open("a" if append_results else "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore")
        if not append_results:
            writer.writeheader()
        handle.flush()

        def fill_workers(executor: concurrent.futures.ThreadPoolExecutor) -> None:
            while len(in_flight) < workers:
                try:
                    job = next(pending)
                except StopIteration:
                    return
                with _defer_sigint():
                    future = executor.submit(run_job, solvers[job.solver], job, timeout, outer_timeout)
                    in_flight[future] = job
                    if tracker is not None:
                        tracker.start(job)

        def persist_finished(done: Iterable[concurrent.futures.Future[JobResult]]) -> None:
            nonlocal completed
            with _defer_sigint():
                for future in done:
                    job = in_flight.pop(future)
                    try:
                        item = future.result()
                    except Exception as exc:  # noqa: BLE001 - persist an unexpected worker failure.
                        item = _job_result_from_output(
                            job, 0.0, "error", None, f"{type(exc).__name__}: {exc}\n"
                        )
                    log_path = write_job_output(logs_dir, log_policy, item)
                    writer.writerow(_result_row(item, log_path))
                    # Keep the per-example dashboard view current without forcing an fsync per job.
                    handle.flush()
                    completed += 1
                    if completed % checkpoint_every == 0:
                        os.fsync(handle.fileno())
                    outcomes[item.result] += 1
                    if tracker is not None:
                        tracker.finish(item, log_path)

        interrupted = False
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="solver-job") as executor:
            fill_workers(executor)
            if tracker is not None:
                tracker.render(force=True)
            try:
                while in_flight:
                    done, _ = concurrent.futures.wait(
                        in_flight,
                        timeout=0.25,
                        return_when=concurrent.futures.FIRST_COMPLETED,
                    )
                    if not done:
                        if tracker is not None:
                            tracker.render()
                        continue
                    persist_finished(done)
                    fill_workers(executor)
                    if tracker is not None:
                        tracker.render()
            except KeyboardInterrupt:
                interrupted = True
                if tracker is not None:
                    tracker.status = "cancelling"
                    tracker.render(force=True)
                # Do not submit any more work. Drain only the already-running jobs and
                # persist their results before reporting the interruption.
                while in_flight:
                    done, _ = concurrent.futures.wait(
                        in_flight,
                        return_when=concurrent.futures.FIRST_COMPLETED,
                    )
                    persist_finished(done)
                    if tracker is not None:
                        tracker.render()
        handle.flush()
        os.fsync(handle.fileno())
    if interrupted:
        raise KeyboardInterrupt
    return outcomes


def _main_locked(args: argparse.Namespace) -> int:
    try:
        plan = _prepare_resume(args) if args.resume else _prepare_fresh(args)
    except (FileNotFoundError, OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    output_dir = plan.output_dir
    tracker = ProgressTracker(
        output_dir,
        plan.solvers,
        plan.pair_count,
        args.progress_interval,
        args.recent_limit,
        timeout=plan.timeout,
    )
    tracker.completed_jobs = plan.completed_before
    tracker.outcomes = Counter(plan.outcomes_before)
    # Merge recovered tallies into the per-solver skeleton so every solver key exists,
    # even when the interrupted run had no recorded results yet.
    tracker.by_solver = {solver: Counter(plan.by_solver_before.get(solver, ())) for solver in plan.solvers}
    tracker.solved_seconds = {solver: plan.solved_seconds_before.get(solver, 0.0) for solver in plan.solvers}
    tracker.par2_seconds = {solver: plan.par2_seconds_before.get(solver, 0.0) for solver in plan.solvers}
    tracker.incremental = IncrementalCounts(
        file_complete=plan.incremental_before.file_complete,
        partial_timeout=plan.incremental_before.partial_timeout,
        answers=plan.incremental_before.answers,
        expected=plan.incremental_before.expected,
    )
    tracker.by_solver_incremental = {
        solver: IncrementalCounts(
            file_complete=plan.by_solver_incremental_before.get(solver, IncrementalCounts()).file_complete,
            partial_timeout=plan.by_solver_incremental_before.get(solver, IncrementalCounts()).partial_timeout,
            answers=plan.by_solver_incremental_before.get(solver, IncrementalCounts()).answers,
            expected=plan.by_solver_incremental_before.get(solver, IncrementalCounts()).expected,
        )
        for solver in plan.solvers
    }

    print(f"[batch] output={output_dir}")
    print(
        f"[batch] files={plan.selected_file_count} solvers={','.join(plan.solvers)} "
        f"pairs={plan.pair_count} workers={args.jobs}"
    )
    if plan.append_results:
        print(f"[batch] resume: {len(plan.remaining)} of {plan.pair_count} pairs remaining")
    print(f"[batch] progress={output_dir / 'progress.json'}")
    print("[batch] monitor with: smtbatch serve")

    def record_resume_finish(status: str, error: str = "") -> None:
        if not args.resume:
            return
        try:
            _append_resume_history(
                output_dir,
                {
                    "event": "resume_finished",
                    "at": datetime.now(timezone.utc).isoformat(),
                    "pid": os.getpid(),
                    "status": status,
                    "completed_jobs": tracker.completed_jobs,
                    "remaining_jobs": max(0, plan.pair_count - tracker.completed_jobs),
                    "error": error,
                },
            )
        except OSError as exc:
            print(f"warning: cannot append resume history: {exc}", file=sys.stderr)

    try:
        if not plan.remaining:
            tracker.status = "complete"
            tracker.render(force=True)
            record_resume_finish("complete")
            print("[resume] nothing to resume; all jobs are already complete")
            return 0
        outcomes = run_queue(
            plan.remaining,
            plan.specs,
            timeout=plan.timeout,
            workers=args.jobs,
            logs_dir=plan.logs_dir,
            log_policy=plan.log,
            checkpoint_every=args.checkpoint_every,
            tracker=tracker,
            results_path=output_dir / "results.tsv",
            append_results=plan.append_results,
        )
    except KeyboardInterrupt:
        tracker.status = "interrupted"
        tracker.render(force=True)
        record_resume_finish("interrupted")
        print("[batch] interrupted; completed results were preserved", file=sys.stderr)
        return 130
    except Exception as exc:  # noqa: BLE001 - persist progress before reporting a controller failure.
        tracker.status = "failed"
        tracker.render(force=True)
        record_resume_finish("failed", f"{type(exc).__name__}: {exc}")
        print(f"error: batch controller failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    tracker.status = "complete"
    tracker.render(force=True)
    record_resume_finish("complete")
    total_outcomes = Counter(plan.outcomes_before)
    total_outcomes.update(outcomes)
    summary = " ".join(f"{result}={total_outcomes[result]}" for result in RESULT_ORDER)
    print(f"[batch] complete pairs={plan.pair_count} {summary}")
    print(
        "[batch] incremental "
        f"file_complete={tracker.incremental.file_complete} "
        f"file_solved={tracker.outcomes['sat'] + tracker.outcomes['unsat']} "
        f"partial_timeout={tracker.incremental.partial_timeout} "
        f"answers={tracker.incremental.answers}/{tracker.incremental.expected}"
    )
    print(f"[batch] export with: smtbatch export {output_dir}")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        print("error: --timeout must be positive", file=sys.stderr)
        return 2
    if args.jobs <= 0 or args.limit < 0 or args.checkpoint_every <= 0 or args.recent_limit <= 0:
        print("error: --jobs, --checkpoint-every, and --recent-limit must be positive; --limit must be non-negative", file=sys.stderr)
        return 2
    if args.progress_interval < 0:
        print("error: --progress-interval must be non-negative", file=sys.stderr)
        return 2

    output_dir = args.output.expanduser().resolve()
    if args.resume and not output_dir.is_dir():
        print(f"error: output directory not found: {output_dir}", file=sys.stderr)
        return 2
    run_lock = _RunLock(output_dir)
    try:
        run_lock.acquire()
    except (OSError, ValueError) as exc:
        print(f"error: cannot acquire run lock: {exc}", file=sys.stderr)
        return 2
    try:
        return _main_locked(args)
    finally:
        run_lock.release()


if __name__ == "__main__":
    raise SystemExit(main())
