"""Run a bounded parallel queue of solver-formula jobs.

Every selected SMT-LIB file is paired with every requested solver and becomes
one independently scheduled job. Results are streamed to ``results.tsv`` as
jobs complete; ``jobs.tsv`` is the immutable queue manifest. ``progress.json``
is refreshed while the queue runs for the dashboard UI.

Incremental SMT-LIB files (many ``check-sat`` commands in one file) are scored
at two layers. Query counts partition every expected check-sat into
sat/unsat/unknown/error/timeout/unreached. The process-level ``result`` is still
the last printed outcome, or timeout/error if the process did not exit 0.
``file_status`` is complete, partial, timeout, or error. Solver stdout is
streamed to ``logs/`` while the job runs so a timeout still leaves a partial log.

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
import multiprocessing
import os
import platform
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from collections import Counter, deque
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Callable, Iterable, Mapping, Sequence

from .config import Config, SolverSpec, load_config, validate_target_branch
from .task import (
    RESULT_FIELDS,
    JobSpec,
    _validate_result_row,
    count_check_sat,
    incremental_from_row,
    incremental_stats,
    incremental_tsv_fields,
    is_timeout_exit,
    load_jobs,
    optional_nonneg_int,
    outcome_from_line,
    parse_outcomes,
    _read_timeout_evidence,
    results_header_ok,
    summarize_performance,
    write_jobs,
)

LOG_TAIL_BYTES = 16 * 1024
_LOG_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")
_BUILD_HASH_RE = re.compile(r"\bbuild hashcode ([0-9a-f]{40})\b")


RESULT_ORDER = ("sat", "unsat", "unknown", "timeout", "error")
SOLVER_BUNDLE_SCHEMA = "linked-artifacts-v1"
QUERY_EVENT_FIELDS = ("ordinal", "elapsed_ms", "delta_ms", "outcome", "source")
RUN_CONTROL_NAME = ".run.control"
RUN_CONTROL_PAUSED = "paused"
RUN_CONTROL_RUNNING = "running"


def run_control_path(output_dir: Path) -> Path:
    return output_dir / RUN_CONTROL_NAME


def read_run_control(output_dir: Path) -> str:
    """Return paused or running; missing or unknown files mean running."""
    try:
        value = run_control_path(output_dir).read_text(encoding="utf-8").strip().lower()
    except OSError:
        return RUN_CONTROL_RUNNING
    return RUN_CONTROL_PAUSED if value == RUN_CONTROL_PAUSED else RUN_CONTROL_RUNNING


def write_run_control(output_dir: Path, state: str) -> None:
    path = run_control_path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(f"{state}\n", encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


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
    file_partial: int = 0
    file_timeout: int = 0
    file_error: int = 0
    partial_timeout: int = 0
    answers: int = 0
    expected: int = 0
    sat: int = 0
    unsat: int = 0
    unknown: int = 0
    error: int = 0
    timeout: int = 0
    unreached: int = 0

    def add(self, stats: Mapping[str, object], *, result: str) -> None:
        file_status = str(stats.get("file_status") or "")
        printed = int(stats.get("queries") or 0)
        self.file_complete += int(file_status == "complete")
        self.file_partial += int(file_status == "partial")
        self.file_timeout += int(file_status == "timeout")
        self.file_error += int(file_status == "error")
        self.partial_timeout += int(result == "timeout" and printed > 0)
        self.answers += printed
        self.sat += int(stats.get("sat") or 0)
        self.unsat += int(stats.get("unsat") or 0)
        self.unknown += int(stats.get("unknown") or 0)
        self.error += int(stats.get("error") or 0)
        self.timeout += int(stats.get("timeout") or 0)
        self.unreached += int(stats.get("unreached") or 0)

    def as_dict(self) -> dict[str, int]:
        return {
            "file_complete": self.file_complete,
            "file_partial": self.file_partial,
            "file_timeout": self.file_timeout,
            "file_error": self.file_error,
            "partial_timeout": self.partial_timeout,
            "answers": self.answers,
            "expected": self.expected,
            "sat": self.sat,
            "unsat": self.unsat,
            "unknown": self.unknown,
            "error": self.error,
            "timeout": self.timeout,
            "unreached": self.unreached,
            "solved": self.sat + self.unsat,
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
    error: int = 0
    timeout: int = 0
    unreached: int = 0
    first: str = ""
    last: str = ""
    expected: int = 0
    complete: bool = False
    file_status: str = ""

    def incremental(self) -> dict[str, object]:
        return {
            "queries": self.queries,
            "sat": self.sat,
            "unsat": self.unsat,
            "unknown": self.unknown,
            "error": self.error,
            "timeout": self.timeout,
            "unreached": self.unreached,
            "first": self.first,
            "last": self.last,
            "expected": self.expected,
            "complete": "yes" if self.complete else "no",
            "file_status": self.file_status,
        }


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

    def set_queue_expected(self, jobs: Sequence[JobSpec]) -> None:
        """Coverage denominator is the whole queue, not only completed files."""
        total = 0
        by_solver = {solver: 0 for solver in self.solvers}
        for job in jobs:
            value = job.expected if job.expected is not None else 0
            total += value
            by_solver[job.solver] = by_solver.get(job.solver, 0) + value
        self.incremental.expected = total
        for solver, value in by_solver.items():
            self.by_solver_incremental.setdefault(solver, IncrementalCounts()).expected = value

    def finish(self, result: JobResult, log_path: Path | None) -> None:
        self.running.pop(result.job.job_id, None)
        self.completed_jobs += 1
        self.outcomes[result.result] += 1
        self.by_solver[result.job.solver][result.result] += 1
        self.incremental.add(result.incremental(), result=result.result)
        self.by_solver_incremental.setdefault(result.job.solver, IncrementalCounts()).add(
            result.incremental(),
            result=result.result,
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
            "sat": result.sat,
            "unsat": result.unsat,
            "unknown": result.unknown,
            "error": result.error,
            "timeout": result.timeout,
            "unreached": result.unreached,
            "last": result.last,
            "expected": result.expected,
            "complete": "yes" if result.complete else "no",
            "file_status": result.file_status,
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
        help="directory to scan for .smt2 files; repeat for multiple folders",
    )
    parser.add_argument(
        "--files-from",
        type=Path,
        help="read a fixed .smt2 file order from a newline-delimited manifest; "
        "relative paths are resolved from the manifest directory",
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
        "--query-events",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="write one timestamped event row per check-sat outcome (default: enabled for new runs)",
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
    result = {
        "solver_binary": str(spec.binary),
        "solver_binary_sha256": sha256_path(spec.binary),
        "solver_artifacts_json": json.dumps(artifacts, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
        "solver_bundle_schema": SOLVER_BUNDLE_SCHEMA,
        "solver_bundle_sha256": solver_bundle_hash(artifacts),
        "solver_version": version,
        "solver_command_json": json.dumps(list(spec.command), ensure_ascii=False, separators=(",", ":")),
    }
    version_revision = _BUILD_HASH_RE.search(version)
    result["solver_version_revision"] = version_revision.group(1) if version_revision else ""
    cmake_cache = spec.binary.parent / "CMakeCache.txt"
    if cmake_cache.is_file():
        cache_values: dict[str, str] = {}
        for line in cmake_cache.read_text(encoding="utf-8", errors="replace").splitlines():
            key_type, separator, value = line.partition("=")
            if not separator:
                continue
            key = key_type.partition(":")[0]
            if key in {"CMAKE_BUILD_TYPE", "CMAKE_CXX_COMPILER"}:
                cache_values[key] = value
        result["solver_cmake_cache"] = str(cmake_cache.resolve())
        result["solver_cmake_cache_sha256"] = sha256_path(cmake_cache)
        result["solver_build_type"] = cache_values.get("CMAKE_BUILD_TYPE", "")
        compiler = cache_values.get("CMAKE_CXX_COMPILER", "")
        result["solver_cxx_compiler"] = compiler
        if compiler:
            compiler_version = subprocess.run(
                [compiler, "--version"], text=True, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, timeout=30, check=False,
            )
            result["solver_cxx_compiler_version"] = (
                compiler_version.stdout.splitlines()[0].strip()
                if compiler_version.stdout.splitlines() else ""
            )
    return result


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
        "runner_script_sha256": sha256_path(Path(__file__).resolve()),
        "working_directory": str(Path.cwd().resolve()),
        "platform": platform.platform(),
        "libc": " ".join(part for part in platform.libc_ver() if part),
        "python_version": platform.python_version(),
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
    outcomes = parse_outcomes(output)
    return outcomes[-1] if outcomes else "error"


def job_log_path(logs_dir: Path, job: JobSpec) -> Path:
    """Stable per-job log path: ``job_0000001.<solver>.out``."""
    solver = _LOG_NAME_RE.sub("_", job.solver) or "solver"
    return logs_dir / f"job_{job.job_id:07d}.{solver}.out"


def query_event_path(events_dir: Path, job: JobSpec) -> Path:
    """Stable per-job query-event path: ``job_0000001.<solver>.tsv``."""
    solver = _LOG_NAME_RE.sub("_", job.solver) or "solver"
    return events_dir / f"job_{job.job_id:07d}.{solver}.tsv"


def retain_job_log(log_path: Path | None, policy: str, result: str) -> Path | None:
    """Keep streamed logs according to ``--log``; delete successful jobs when policy is fail."""
    if log_path is None or policy == "none":
        return None
    if policy == "fail" and result not in {"timeout", "error"}:
        log_path.unlink(missing_ok=True)
        return None
    return log_path if log_path.is_file() else None


def _expected_for(job: JobSpec) -> int:
    return job.expected if job.expected is not None else count_check_sat(job.file_path)


def _job_result_from_outcomes(
    job: JobSpec,
    duration_sec: float,
    result: str,
    code: int | None,
    output: str,
    outcomes: Sequence[str],
) -> JobResult:
    expected = _expected_for(job)
    stats = incremental_stats(outcomes, expected, result=result, exit_ok=code == 0)
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
        error=int(stats["error"]),
        timeout=int(stats["timeout"]),
        unreached=int(stats["unreached"]),
        first=str(stats["first"]),
        last=str(stats["last"]),
        expected=expected,
        complete=str(stats["complete"]) == "yes",
        file_status=str(stats["file_status"]),
    )


def _job_result_from_output(
    job: JobSpec,
    duration_sec: float,
    result: str,
    code: int | None,
    output: str,
) -> JobResult:
    return _job_result_from_outcomes(job, duration_sec, result, code, output, parse_outcomes(output))


def _result_row(item: JobResult, log_path: Path | None) -> dict[str, str]:
    return {
        "job_id": str(item.job.job_id),
        "solver": item.job.solver,
        "file": str(item.job.file_path),
        "result": item.result,
        "time": f"{item.duration_sec:.3f}",
        "code": "" if item.code is None else str(item.code),
        "output_path": str(log_path) if log_path else "",
        **incremental_tsv_fields(item.incremental()),
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


def load_files_from(path: Path, limit: int) -> list[Path]:
    """Load a stable, de-duplicated benchmark order from a text manifest."""
    manifest = path.expanduser().resolve()
    if not manifest.is_file():
        raise FileNotFoundError(f"file manifest not found: {manifest}")
    files: list[Path] = []
    seen: set[Path] = set()
    for line_number, raw in enumerate(
        manifest.read_text(encoding="utf-8", errors="replace").splitlines(),
        start=1,
    ):
        value = raw.strip()
        if not value or value.startswith("#"):
            continue
        candidate = Path(value).expanduser()
        if not candidate.is_absolute():
            candidate = manifest.parent / candidate
        resolved = candidate.resolve()
        if not resolved.is_file() or resolved.suffix.lower() != ".smt2":
            raise ValueError(f"invalid SMT-LIB file at {manifest}:{line_number}: {value}")
        if resolved in seen:
            continue
        seen.add(resolved)
        files.append(resolved)
        if limit > 0 and len(files) >= limit:
            break
    return files


def iter_jobs(
    files: Sequence[Path],
    solvers: Sequence[str],
    expected_by_file: Mapping[Path, int],
) -> Iterable[JobSpec]:
    for job_id, (file_path, solver) in enumerate(
        ((file_path, solver) for file_path in files for solver in solvers),
        start=1,
    ):
        yield JobSpec(job_id, solver, file_path, expected_by_file[file_path])


def _count_check_sat_files(
    paths: Sequence[Path],
    progress: Callable[[int, int], None] | None = None,
) -> dict[Path, int]:
    """Count check-sat commands once per file.

    Small queues stay in-process. Larger queues use a process pool so the
    CPU-bound scanner is not serialized by the GIL.
    """
    unique = list(dict.fromkeys(paths))
    total = len(unique)
    counted: dict[Path, int] = {}
    if total == 0:
        return counted

    def record(path: Path, value: int, done: int) -> None:
        counted[path] = value
        if progress is not None and (done == total or done % max(1, total // 40) == 0):
            progress(done, total)

    if total <= 16:
        for index, path in enumerate(unique, start=1):
            record(path, count_check_sat(path), index)
        return counted

    workers = min(32, os.cpu_count() or 8, total)
    chunksize = max(1, total // (workers * 4))
    start_method = "fork" if sys.platform.startswith("linux") else "spawn"
    context = multiprocessing.get_context(start_method)
    with concurrent.futures.ProcessPoolExecutor(
        max_workers=workers,
        mp_context=context,
    ) as executor:
        try:
            for index, (path, value) in enumerate(
                zip(unique, executor.map(count_check_sat, unique, chunksize=chunksize)),
                start=1,
            ):
                record(path, value, index)
        except BaseException:
            executor.shutdown(wait=False, cancel_futures=True)
            raise
    return counted


def run_job(
    spec: SolverSpec,
    job: JobSpec,
    timeout: float,
    outer_timeout: float,
    *,
    log_path: Path | None = None,
    event_path: Path | None = None,
) -> JobResult:
    start = time.perf_counter()
    expected = _expected_for(job)
    code: int | None = None
    command = spec.render(job.file_path, timeout)
    outcomes: list[str] = []
    tail = bytearray()
    process: subprocess.Popen[str] | None = None

    consume_error: list[BaseException] = []
    event_handle: IO[str] | None = None
    event_writer: csv.DictWriter[str] | None = None
    last_event_ms = 0
    synthetic_event_written = False

    def write_event(outcome: str, source: str) -> None:
        nonlocal last_event_ms, synthetic_event_written
        if event_writer is None or event_handle is None:
            return
        elapsed_ms = max(last_event_ms, int((time.perf_counter() - start) * 1000))
        event_writer.writerow(
            {
                "ordinal": len(outcomes) + (1 if source == "synthetic" else 0),
                "elapsed_ms": elapsed_ms,
                "delta_ms": elapsed_ms - last_event_ms,
                "outcome": outcome,
                "source": source,
            }
        )
        event_handle.flush()
        last_event_ms = elapsed_ms
        synthetic_event_written = synthetic_event_written or source == "synthetic"

    def consume(stream: IO[str], log_handle: IO[str] | None) -> None:
        try:
            while True:
                line = stream.readline()
                if line == "":
                    break
                if log_handle is not None:
                    log_handle.write(line)
                    log_handle.flush()
                outcome = outcome_from_line(line)
                if outcome is not None and len(outcomes) < expected:
                    outcomes.append(outcome)
                    write_event(outcome, "solver")
                encoded = line.encode("utf-8", errors="replace")
                tail.extend(encoded)
                overflow = len(tail) - LOG_TAIL_BYTES
                if overflow > 0:
                    del tail[:overflow]
        except Exception as exc:  # noqa: BLE001 - surface reader failures after join.
            consume_error.append(exc)
        finally:
            stream.close()

    try:
        if log_path is not None:
            log_path.parent.mkdir(parents=True, exist_ok=True)
        if event_path is not None:
            event_path.parent.mkdir(parents=True, exist_ok=True)
            event_handle = event_path.open("w", encoding="utf-8", newline="", buffering=1)
            event_writer = csv.DictWriter(event_handle, fieldnames=QUERY_EVENT_FIELDS, delimiter="\t")
            event_writer.writeheader()
            event_handle.flush()
        process = subprocess.Popen(
            command,
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=(
                {
                    **os.environ,
                    "INCSMT_EVENT_FILE": str(event_path.with_suffix(".obe.jsonl")),
                    "INCSMT_SESSION_ID": str(job.file_path),
                }
                if event_path is not None else None
            ),
        )
        if process.stdout is None:
            raise RuntimeError("solver stdout pipe was not created")
        log_handle = (
            log_path.open("w", encoding="utf-8", errors="replace", buffering=1)
            if log_path is not None
            else None
        )
        try:
            reader = threading.Thread(
                target=consume,
                args=(process.stdout, log_handle),
                name=f"solver-log-{job.job_id}",
                daemon=True,
            )
            reader.start()
            timed_out = False
            try:
                code = process.wait(timeout=outer_timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                try:
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass
                code = process.wait()
            reader.join()
            if consume_error:
                raise consume_error[0]
            output = tail.decode("utf-8", errors="replace")
            duration_sec = time.perf_counter() - start
            evidence = output
            if log_path is not None:
                marker = _read_timeout_evidence(str(log_path))
                if marker and marker not in evidence:
                    evidence = marker + "\n" + evidence
            if timed_out or is_timeout_exit(code, duration_sec, timeout, evidence):
                result = "timeout"
            elif code == 0:
                result = outcomes[-1] if outcomes else "error"
            else:
                result = "error"
            if len(outcomes) < expected:
                write_event("timeout" if result == "timeout" else "error", "synthetic")
        finally:
            if log_handle is not None:
                log_handle.flush()
                try:
                    os.fsync(log_handle.fileno())
                except OSError:
                    pass
                log_handle.close()
        output = tail.decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001 - preserve launcher failures in the result stream.
        if process is not None and process.poll() is None:
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError, OSError):
                pass
            try:
                process.wait(timeout=2)
            except (subprocess.TimeoutExpired, OSError):
                pass
        output = f"{type(exc).__name__}: {exc}\n"
        if log_path is not None:
            try:
                with log_path.open("a", encoding="utf-8", errors="replace") as handle:
                    handle.write(output)
            except OSError:
                pass
        if len(outcomes) < expected and not synthetic_event_written:
            write_event("error", "synthetic")
        return _job_result_from_output(job, time.perf_counter() - start, "error", code, output)
    finally:
        if event_handle is not None:
            event_handle.flush()
            try:
                os.fsync(event_handle.fileno())
            except OSError:
                pass
            event_handle.close()
    return _job_result_from_outcomes(job, time.perf_counter() - start, result, code, output, outcomes)


def _atomic_write_text(path: Path, text: str) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        temporary.write_text(text, encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def write_progress_snapshot(output_dir: Path, *, status: str, **fields: object) -> dict[str, object]:
    """Persist a dashboard-readable progress.json, including the startup window.

    The controller writes this before ``jobs.tsv`` exists so a closed browser or
    a dashboard restart can still see a live ``starting`` card.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat()
    snapshot: dict[str, object] = {
        "format": "pair-queue-progress-v1",
        "output_dir": str(output_dir),
        "status": status,
        "phase": fields.pop("phase", "preparing" if status == "starting" else status),
        "startup_note": fields.pop("startup_note", ""),
        "started_at": fields.pop("started_at", now),
        "updated_at": now,
        "elapsed_seconds": fields.pop("elapsed_seconds", 0),
        "total_jobs": fields.get("total_jobs", 0),
        "completed_jobs": fields.get("completed_jobs", 0),
        "running_jobs": fields.get("running_jobs", 0),
        "pending_jobs": fields.get("pending_jobs", fields.get("total_jobs", 0)),
        "selected_files": fields.get("selected_files", 0),
        "counted_files": fields.get("counted_files", 0),
        "outcomes": fields.get("outcomes", {}),
        "by_solver": fields.get("by_solver", {}),
        "incremental": fields.get("incremental", {}),
    }
    snapshot.update(fields)
    _atomic_write_text(
        output_dir / "progress.json",
        json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )
    return snapshot


def clean_previous_outputs(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for name in (
        "jobs.tsv",
        "results.tsv",
        "input_hashes.tsv",
        "metadata.txt",
        "resume_history.jsonl",
        "manifest.tsv",
    ):
        (output_dir / name).unlink(missing_ok=True)
    for name in ("logs", "events", "tsv", "summary", "failures"):
        path = output_dir / name
        if path.is_symlink():
            raise ValueError(f"refusing to remove symlinked batch output path: {path}")
        if path.is_dir():
            shutil.rmtree(path)


def require_fresh_output(output_dir: Path) -> None:
    """Refuse to overwrite material data from a prior run; use --resume instead."""
    material = [
        output_dir / name
        for name in (
            "jobs.tsv", "results.tsv", "input_hashes.tsv", "metadata.txt",
            "progress.json", "resume_history.jsonl", "manifest.tsv", "logs",
            "events", "tsv", "summary", "failures",
        )
        if (output_dir / name).exists()
    ]
    if material:
        rendered = ", ".join(path.name for path in material)
        raise ValueError(
            f"refusing to overwrite existing run data in {output_dir}: {rendered}; "
            "choose a new output directory or use --resume"
        )


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
                error = _validate_result_row(row, str(job.file_path), job.solver)
                if error:
                    raise ValueError(f"row {row_number}: {error}")
                if job.expected is not None and optional_nonneg_int(row.get("expected")) != job.expected:
                    raise ValueError(f"expected count does not match jobs.tsv at {results_path}:{row_number}")
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
                stats = incremental_from_row(row)
                completed.add(job_id)
                outcomes[result] += 1
                by_solver.setdefault(solver, Counter())[result] += 1
                solved_seconds.setdefault(solver, 0.0)
                par2_seconds.setdefault(solver, 0.0)
                incremental.add(stats, result=result)
                by_solver_incremental.setdefault(solver, IncrementalCounts()).add(stats, result=result)
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
    """Resume only against the current results header; new runs write it too."""
    if append_results and results_path.is_file() and results_path.stat().st_size:
        with results_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            if not results_header_ok(reader.fieldnames):
                raise ValueError(f"invalid results header: {reader.fieldnames}")
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
    events_dir: Path
    jobs: list[JobSpec]
    remaining: list[JobSpec]
    specs: dict[str, SolverSpec]
    solvers: tuple[str, ...]
    timeout: float
    log: str
    query_events: bool
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


def _prepare_fresh(
    args: argparse.Namespace,
    progress: Callable[..., None] | None = None,
    *,
    fresh_output_reserved: bool = False,
) -> _RunPlan:
    if not args.solver or (not args.input and not args.files_from):
        raise ValueError("--solver and either --input or --files-from are required unless --resume is used")
    if args.input and args.files_from:
        raise ValueError("--input and --files-from cannot be combined")
    output_dir = args.output.expanduser().resolve()
    if not fresh_output_reserved:
        require_fresh_output(output_dir)
    if progress is not None:
        progress("config", "Loading solver configuration")
    config = load_config()
    smtbatch_branch = validate_target_branch(config)
    solvers = normalize_solvers(args.solver, config)
    specs = {name: config.solvers[name] for name in solvers}
    artifact_cache: dict[Path, dict[str, dict[str, str]]] = {}
    provenance = {
        name: solver_provenance(spec, artifact_cache) for name, spec in specs.items()
    }
    if progress is not None:
        progress("discover", "Scanning benchmark files")
    files = (
        load_files_from(args.files_from, args.limit)
        if args.files_from
        else discover_files(args.input, args.limit)
    )
    if not files:
        raise ValueError("no .smt2 files selected")
    if progress is not None:
        progress(
            "count",
            f"Counting check-sat commands in {len(files)} files",
            selected_files=len(files),
        )

    def _count_progress(done: int, total: int) -> None:
        if progress is None:
            return
        progress(
            "count",
            f"Counting check-sat commands: {done}/{total} files",
            selected_files=total,
            counted_files=done,
        )

    expected_by_file = _count_check_sat_files(files, progress=_count_progress)
    pair_count = len(files) * len(solvers)
    logs_dir = output_dir / "logs"
    events_dir = output_dir / "events"
    if progress is not None:
        progress(
            "queue",
            f"Writing job queue ({pair_count} solver-file pairs)",
            selected_files=len(files),
            counted_files=len(files),
            total_jobs=pair_count,
        )
    clean_previous_outputs(output_dir)
    jobs = list(iter_jobs(files, solvers, expected_by_file))
    write_jobs(output_dir / "jobs.tsv", jobs)
    if progress is not None:
        progress(
            "queue",
            "Job queue ready",
            selected_files=len(files),
            counted_files=len(files),
            total_jobs=pair_count,
        )
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
        "query_events": "yes" if args.query_events is not False else "no",
        "input_hashes": "input_hashes.tsv" if args.hash_inputs else "",
        "files_from": str(args.files_from.expanduser().resolve()) if args.files_from else "",
        "files_from_sha256": sha256_path(args.files_from.expanduser().resolve()) if args.files_from else "",
        "solver_config": str(config.path),
        "solver_config_sha256": sha256_path(config.path),
        "incremental": "yes",
        "target_branch": config.target_branch,
        "smtbatch_branch": smtbatch_branch,
    }
    for solver, values in provenance.items():
        for key, value in values.items():
            metadata[f"{solver}_{key}"] = value
    metadata.update(repository_provenance())
    write_metadata(output_dir, metadata)
    return _RunPlan(
        output_dir=output_dir,
        logs_dir=logs_dir,
        events_dir=events_dir,
        jobs=jobs,
        remaining=jobs,
        specs=specs,
        solvers=solvers,
        timeout=args.timeout,
        log=args.log,
        query_events=args.query_events is not False,
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
    if args.input or args.files_from or args.solver:
        raise ValueError(
            "--resume cannot be combined with --input, --files-from, or --solver "
            "(recovered from jobs.tsv)"
        )
    if args.hash_inputs:
        raise ValueError("--hash-inputs cannot be combined with --resume")
    if args.query_events is not None:
        raise ValueError("--query-events/--no-query-events cannot be combined with --resume")
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
    validate_target_branch(config)
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
        events_dir=output_dir / "events",
        jobs=jobs,
        remaining=remaining,
        specs=specs,
        solvers=solvers,
        timeout=timeout,
        log=metadata.get("log", "fail"),
        query_events=metadata.get("query_events", "no") == "yes",
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
    events_dir: Path | None = None,
    log_policy: str,
    checkpoint_every: int,
    tracker: ProgressTracker | None,
    results_path: Path,
    append_results: bool = False,
) -> Counter[str]:
    outer_timeout = timeout + max(15.0, timeout * 0.5)
    pending = iter(jobs)
    in_flight: dict[
        concurrent.futures.Future[JobResult], tuple[JobSpec, Path | None, Path | None]
    ] = {}
    outcomes: Counter[str] = Counter()
    completed = 0

    if log_policy != "none":
        logs_dir.mkdir(parents=True, exist_ok=True)
    if events_dir is not None:
        events_dir.mkdir(parents=True, exist_ok=True)
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
                log_path = job_log_path(logs_dir, job) if log_policy != "none" else None
                event_path = query_event_path(events_dir, job) if events_dir is not None else None
                with _defer_sigint():
                    future = executor.submit(
                        run_job,
                        solvers[job.solver],
                        job,
                        timeout,
                        outer_timeout,
                        log_path=log_path,
                        event_path=event_path,
                    )
                    in_flight[future] = (job, log_path, event_path)
                    if tracker is not None:
                        tracker.start(job)

        def persist_finished(done: Iterable[concurrent.futures.Future[JobResult]]) -> None:
            nonlocal completed
            with _defer_sigint():
                for future in done:
                    job, log_path, _event_path = in_flight.pop(future)
                    try:
                        item = future.result()
                    except Exception as exc:  # noqa: BLE001 - persist an unexpected worker failure.
                        item = _job_result_from_output(
                            job, 0.0, "error", None, f"{type(exc).__name__}: {exc}\n"
                        )
                    kept_log = retain_job_log(log_path, log_policy, item.result)
                    writer.writerow(_result_row(item, kept_log))
                    # Keep the per-example dashboard view current without forcing an fsync per job.
                    handle.flush()
                    completed += 1
                    if completed % checkpoint_every == 0:
                        os.fsync(handle.fileno())
                    outcomes[item.result] += 1
                    if tracker is not None:
                        tracker.finish(item, kept_log)

        interrupted = False
        output_dir = results_path.parent
        write_run_control(output_dir, RUN_CONTROL_RUNNING)
        sigint_pause = False

        def paused() -> bool:
            return read_run_control(output_dir) == RUN_CONTROL_PAUSED

        def apply_pause() -> None:
            nonlocal sigint_pause
            sigint_pause = False
            write_run_control(output_dir, RUN_CONTROL_PAUSED)
            if tracker is not None:
                tracker.status = "paused"
                tracker.render(force=True)

        def apply_running_status() -> None:
            if tracker is not None and tracker.status != "running":
                tracker.status = "running"
                tracker.render(force=True)

        def _on_sigint(_signum: int, _frame: object) -> None:
            nonlocal sigint_pause
            sigint_pause = True

        previous_handler = signal.getsignal(signal.SIGINT)
        try:
            signal.signal(signal.SIGINT, _on_sigint)
        except ValueError:
            previous_handler = None

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="solver-job") as executor:
                while True:
                    try:
                        if sigint_pause:
                            apply_pause()
                        if paused():
                            if tracker is not None and tracker.status != "paused":
                                tracker.status = "paused"
                                tracker.render(force=True)
                        else:
                            apply_running_status()
                            fill_workers(executor)
                        if not in_flight:
                            if paused():
                                interrupted = True
                            break
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
                        if tracker is not None:
                            tracker.render()
                    except KeyboardInterrupt:
                        apply_pause()
                        continue
        finally:
            if previous_handler is not None:
                signal.signal(signal.SIGINT, previous_handler)
        handle.flush()
        os.fsync(handle.fileno())
    if interrupted:
        raise KeyboardInterrupt
    return outcomes


def _main_locked(args: argparse.Namespace) -> int:
    output_dir = args.output.expanduser().resolve()
    started_monotonic = time.monotonic()
    started_at = datetime.now(timezone.utc).isoformat()

    def note(phase: str, text: str, **extra: object) -> None:
        write_progress_snapshot(
            output_dir,
            status="starting",
            phase=phase,
            startup_note=text,
            started_at=started_at,
            elapsed_seconds=round(time.monotonic() - started_monotonic, 3),
            timeout=args.timeout,
            jobs=args.jobs,
            **extra,
        )

    if not args.resume:
        try:
            require_fresh_output(output_dir)
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        note("preparing", "Preparing the job queue")
    try:
        plan = (
            _prepare_resume(args)
            if args.resume
            else _prepare_fresh(args, progress=note, fresh_output_reserved=True)
        )
    except KeyboardInterrupt:
        if not args.resume:
            write_progress_snapshot(
                output_dir,
                status="interrupted",
                phase="interrupted",
                startup_note="Interrupted during startup",
                started_at=started_at,
                elapsed_seconds=round(time.monotonic() - started_monotonic, 3),
            )
        print("[batch] interrupted during startup", file=sys.stderr)
        return 130
    except (FileNotFoundError, OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
        if not args.resume:
            write_progress_snapshot(
                output_dir,
                status="failed",
                phase="failed",
                startup_note=str(exc),
                started_at=started_at,
                elapsed_seconds=round(time.monotonic() - started_monotonic, 3),
            )
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
        started_monotonic=started_monotonic,
        started_at=started_at,
    )
    tracker.completed_jobs = plan.completed_before
    tracker.outcomes = Counter(plan.outcomes_before)
    # Merge recovered tallies into the per-solver skeleton so every solver key exists,
    # even when the interrupted run had no recorded results yet.
    tracker.by_solver = {solver: Counter(plan.by_solver_before.get(solver, ())) for solver in plan.solvers}
    tracker.solved_seconds = {solver: plan.solved_seconds_before.get(solver, 0.0) for solver in plan.solvers}
    tracker.par2_seconds = {solver: plan.par2_seconds_before.get(solver, 0.0) for solver in plan.solvers}
    tracker.incremental = replace(plan.incremental_before)
    tracker.by_solver_incremental = {
        solver: replace(plan.by_solver_incremental_before.get(solver, IncrementalCounts()))
        for solver in plan.solvers
    }
    tracker.set_queue_expected(plan.jobs)

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
            events_dir=plan.events_dir if plan.query_events else None,
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
        f"files complete={tracker.incremental.file_complete} "
        f"partial={tracker.incremental.file_partial} "
        f"timeout={tracker.incremental.file_timeout} "
        f"error={tracker.incremental.file_error} "
        f"PO sat={tracker.incremental.sat} unsat={tracker.incremental.unsat} "
        f"unknown={tracker.incremental.unknown} error={tracker.incremental.error} "
        f"timeout={tracker.incremental.timeout} unreached={tracker.incremental.unreached} "
        f"coverage={tracker.incremental.sat + tracker.incremental.unsat}/"
        f"{tracker.incremental.expected}"
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
