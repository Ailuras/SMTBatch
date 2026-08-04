"""Run a bounded parallel queue of solver-formula jobs.

Every selected SMT-LIB file is paired with every requested solver and becomes
one independently scheduled job. Results are streamed to ``results.tsv`` as
jobs complete; ``jobs.tsv`` is the immutable queue manifest. ``progress.json``
is refreshed while the queue runs for the dashboard UI.

Solver commands come from the TOML config named by SMTBATCH_CONFIG; see
smtbatch.config for the schema.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import hashlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

from .config import Config, SolverSpec, load_config
from .task import JOB_FIELDS, RESULT_FIELDS, JobSpec, write_jobs


RESULT_ORDER = ("sat", "unsat", "unknown", "timeout", "error")


@dataclass(frozen=True)
class JobResult:
    job: JobSpec
    duration_sec: float
    result: str
    code: int | None
    output: str


@dataclass
class ProgressTracker:
    output_dir: Path
    solvers: tuple[str, ...]
    total_jobs: int
    refresh_seconds: float
    recent_limit: int
    started_monotonic: float = field(default_factory=time.monotonic)
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    status: str = "running"
    completed_jobs: int = 0
    running: dict[int, JobSpec] = field(default_factory=dict)
    outcomes: Counter[str] = field(default_factory=Counter)
    by_solver: dict[str, Counter[str]] = field(default_factory=dict)
    recent_errors: deque[dict[str, object]] = field(default_factory=deque)
    _last_render: float = 0.0

    def __post_init__(self) -> None:
        self.by_solver = {solver: Counter() for solver in self.solvers}
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
        record = {
            "job_id": result.job.job_id,
            "solver": result.job.solver,
            "file": str(result.job.file_path),
            "result": result.result,
            "time": round(result.duration_sec, 3),
            "code": result.code,
            "log_path": str(log_path) if log_path else "",
        }
        if result.result in {"timeout", "error"}:
            self.recent_errors.appendleft({**record, "output": result.output[-2000:]})

    def snapshot(self) -> dict[str, object]:
        elapsed = time.monotonic() - self.started_monotonic
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
        required=True,
        help="solver name from the SMTBATCH_CONFIG file; repeat to schedule every solver-file pair",
    )
    parser.add_argument(
        "--input",
        action="append",
        type=Path,
        required=True,
        help="directory to scan for .smt2 files; repeat for multiple folders",
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


def solver_provenance(spec: SolverSpec) -> dict[str, str]:
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
    return {
        "solver_binary": str(spec.binary),
        "solver_binary_sha256": sha256_path(spec.binary),
        "solver_version": version,
    }


def repository_provenance() -> dict[str, str]:
    root = Path.cwd()
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
        "repository_commit": commit.stdout.strip() if commit.returncode == 0 else "",
        "repository_dirty": "yes" if status.returncode != 0 or status.stdout.strip() else "no",
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
    result = "error"
    for line in output.splitlines():
        parts = line.strip().split(maxsplit=1)
        if parts and parts[0] in {"sat", "unsat", "unknown"}:
            result = parts[0]
    return result


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
    return JobResult(job, time.perf_counter() - start, result, code, output)


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
    for name in ("jobs.tsv", "results.tsv", "input_hashes.tsv", "metadata.txt", "progress.json", "manifest.tsv"):
        (output_dir / name).unlink(missing_ok=True)
    for name in ("logs", "tsv", "summary", "failures"):
        path = output_dir / name
        if path.is_symlink():
            raise ValueError(f"refusing to remove symlinked batch output path: {path}")
        if path.is_dir():
            shutil.rmtree(path)


def write_metadata(output_dir: Path, metadata: dict[str, str]) -> None:
    _atomic_write_text(output_dir / "metadata.txt", "\n".join(f"{key}={value}" for key, value in metadata.items()) + "\n")


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
) -> Counter[str]:
    outer_timeout = timeout + max(15.0, timeout * 0.5)
    pending = iter(jobs)
    in_flight: dict[concurrent.futures.Future[JobResult], JobSpec] = {}
    outcomes: Counter[str] = Counter()
    completed = 0

    with results_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=RESULT_FIELDS, delimiter="\t")
        writer.writeheader()
        handle.flush()

        def fill_workers(executor: concurrent.futures.ThreadPoolExecutor) -> None:
            while len(in_flight) < workers:
                try:
                    job = next(pending)
                except StopIteration:
                    return
                if tracker is not None:
                    tracker.start(job)
                future = executor.submit(run_job, solvers[job.solver], job, timeout, outer_timeout)
                in_flight[future] = job

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="solver-job") as executor:
            fill_workers(executor)
            if tracker is not None:
                tracker.render(force=True)
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
                for future in done:
                    job = in_flight.pop(future)
                    try:
                        item = future.result()
                    except Exception as exc:  # noqa: BLE001 - persist an unexpected worker failure.
                        item = JobResult(job, 0.0, "error", None, f"{type(exc).__name__}: {exc}\n")
                    log_path = write_job_output(logs_dir, log_policy, item)
                    writer.writerow(
                        {
                            "job_id": item.job.job_id,
                            "solver": item.job.solver,
                            "file": str(item.job.file_path),
                            "result": item.result,
                            "time": f"{item.duration_sec:.3f}",
                            "code": "" if item.code is None else str(item.code),
                            "output_path": str(log_path) if log_path else "",
                        }
                    )
                    # Keep the per-example dashboard view current without forcing an fsync per job.
                    handle.flush()
                    completed += 1
                    if completed % checkpoint_every == 0:
                        os.fsync(handle.fileno())
                    outcomes[item.result] += 1
                    if tracker is not None:
                        tracker.finish(item, log_path)
                fill_workers(executor)
                if tracker is not None:
                    tracker.render()
        handle.flush()
        os.fsync(handle.fileno())
    return outcomes


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.timeout <= 0:
        print("error: --timeout must be positive", file=sys.stderr)
        return 2
    if args.jobs <= 0 or args.limit < 0 or args.checkpoint_every <= 0 or args.recent_limit <= 0:
        print("error: --jobs, --checkpoint-every, and --recent-limit must be positive; --limit must be non-negative", file=sys.stderr)
        return 2
    if args.progress_interval < 0:
        print("error: --progress-interval must be non-negative", file=sys.stderr)
        return 2

    try:
        config = load_config()
        solvers = normalize_solvers(args.solver, config)
        specs = {name: config.solvers[name] for name in solvers}
        provenance = {name: solver_provenance(spec) for name, spec in specs.items()}
        files = discover_files(args.input, args.limit)
    except (FileNotFoundError, OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if not files:
        print("error: no .smt2 files selected", file=sys.stderr)
        return 2

    pair_count = len(files) * len(solvers)
    output_dir = args.output.expanduser().resolve()
    logs_dir = output_dir / "logs"
    try:
        clean_previous_outputs(output_dir)
        write_jobs(output_dir / "jobs.tsv", iter_jobs(files, solvers))
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
        }
        for solver, values in provenance.items():
            for key, value in values.items():
                metadata[f"{solver}_{key}"] = value
        metadata.update(repository_provenance())
        write_metadata(output_dir, metadata)
    except (OSError, ValueError) as exc:
        print(f"error: cannot initialize output: {exc}", file=sys.stderr)
        return 2

    tracker = ProgressTracker(output_dir, solvers, pair_count, args.progress_interval, args.recent_limit)
    print(f"[batch] output={output_dir}")
    print(f"[batch] files={len(files)} solvers={','.join(solvers)} pairs={pair_count} workers={args.jobs}")
    print(f"[batch] progress={output_dir / 'progress.json'}")
    print("[batch] monitor with: smtbatch serve")

    try:
        outcomes = run_queue(
            iter_jobs(files, solvers),
            specs,
            timeout=args.timeout,
            workers=args.jobs,
            logs_dir=logs_dir,
            log_policy=args.log,
            checkpoint_every=args.checkpoint_every,
            tracker=tracker,
            results_path=output_dir / "results.tsv",
        )
    except KeyboardInterrupt:
        tracker.status = "interrupted"
        tracker.render(force=True)
        print("[batch] interrupted; completed results were preserved", file=sys.stderr)
        return 130
    except Exception as exc:  # noqa: BLE001 - persist progress before reporting a controller failure.
        tracker.status = "failed"
        tracker.render(force=True)
        print(f"error: batch controller failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    tracker.status = "complete"
    tracker.render(force=True)
    summary = " ".join(f"{result}={outcomes[result]}" for result in RESULT_ORDER)
    print(f"[batch] complete pairs={pair_count} {summary}")
    print(f"[batch] export with: smtbatch export {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
