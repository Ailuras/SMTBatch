"""Serve the local batch dashboard, experiment launcher, and Excel export API.

Open http://127.0.0.1:8000 after starting this command. The dashboard is
shipped as package data; all run state is discovered from progress.json files
under the results root. Solver names and commands come from the nearest
project ``smtbatch.toml``.
"""

from __future__ import annotations

import argparse
import csv
import fcntl
import heapq
import itertools
import json
import math
import mimetypes
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlparse

from .config import Config, find_config_path, load_config
from .task import (
    RESULT_FIELDS,
    VALID_TASK_RESULTS,
    classify_consistency,
    load_jobs,
    recorded_result,
    recorded_result_row,
    result_label,
    summarize_performance,
)


RUN_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}")
LOOPBACK_HOSTS = {"127.0.0.1", "localhost"}


def dashboard_path() -> Path:
    return Path(str(resources.files("smtbatch").joinpath("dashboard.html")))


def report_path() -> Path:
    return Path(str(resources.files("smtbatch").joinpath("report.html")))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        nargs="?",
        choices=("start", "stop", "restart", "status", "foreground"),
        default="start",
        help="controller action (default: start)",
    )
    parser.add_argument("--host", default="127.0.0.1", help="bind address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=None, help="bind port (default: config [defaults] port or 8000)")
    parser.add_argument("--results", type=Path, default=None, help="results root to scan (default: config [defaults] or ./results)")
    parser.add_argument("--inputs-root", type=Path, default=None, help="default benchmark root (default: config [defaults] or ./benchmarks)")
    return parser.parse_args(argv)


def resolve_roots(args: argparse.Namespace) -> tuple[Path, Path]:
    """Resolve (inputs_root, results_root): CLI flags, then config defaults, then cwd."""
    try:
        config = load_config()
        default_inputs, default_results = config.inputs_root, config.results_root
    except RuntimeError:
        default_inputs, default_results = Path("benchmarks"), Path("results")
    inputs_root = (args.inputs_root or default_inputs).expanduser().resolve()
    results_root = (args.results or default_results).expanduser().resolve()
    return inputs_root, results_root


def _resolve_port(args: argparse.Namespace) -> int:
    """Resolve the bind port: an explicit --port flag, then config [defaults], then 8000."""
    if args.port is not None:
        return args.port
    try:
        config_path = find_config_path()
    except RuntimeError:
        return 8000
    return load_config(config_path.parent).port


def _metadata(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            key, separator, value = line.partition("=")
            if separator and key and key not in values:
                values[key] = value
    except OSError:
        pass
    return values


def _within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _data_url(path: object, results_root: Path) -> str:
    if not isinstance(path, (str, Path)) or not path:
        return ""
    try:
        relative = Path(path).expanduser().resolve().relative_to(results_root)
    except (OSError, ValueError):
        return ""
    return "/data/" + quote(relative.as_posix())


@dataclass
class _PerformanceTotals:
    completed_jobs: int = 0
    solved_jobs: int = 0
    solved_seconds: float = 0.0
    par2_seconds: float = 0.0

    def summary(self) -> dict[str, object]:
        return summarize_performance(
            self.completed_jobs,
            self.solved_jobs,
            self.solved_seconds,
            self.par2_seconds,
        )


@dataclass
class _MetricsState:
    """Incremental reader state for a run created by an older batch process."""

    identity: tuple[int, int]
    timeout: float
    expected_jobs: dict[int, tuple[str, str]]
    offset: int = 0
    tail: bytes = b""
    record_buffer: bytes = b""
    in_quotes: bool = False
    header_seen: bool = False
    invalid: bool = False
    completed_ids: set[int] = field(default_factory=set)
    by_solver: dict[str, _PerformanceTotals] = field(default_factory=dict)

    def payload(self) -> dict[str, object]:
        aggregate = _PerformanceTotals()
        for totals in self.by_solver.values():
            aggregate.completed_jobs += totals.completed_jobs
            aggregate.solved_jobs += totals.solved_jobs
            aggregate.solved_seconds += totals.solved_seconds
            aggregate.par2_seconds += totals.par2_seconds
        return {
            "performance": aggregate.summary(),
            "by_solver_performance": {
                solver: totals.summary() for solver, totals in self.by_solver.items()
            },
        }


def _clamp_time(value: float, limit: float) -> float:
    """Clip a runtime onto ``[0, limit]`` so GNU-timeout overshoot still counts."""
    if not math.isfinite(value) or value < 0:
        return 0.0
    return limit if value > limit else value


class ExperimentManager:
    """Validate local UI requests and launch the regular batch CLI without a shell."""

    def __init__(self, results_root: Path, inputs_root: Path, host_cwd: Path) -> None:
        self.results_root = results_root.expanduser().resolve()
        self.inputs_root = inputs_root.expanduser().resolve()
        self.host_cwd = host_cwd
        self.processes: dict[str, subprocess.Popen[str]] = {}
        self._process_lock = threading.RLock()
        self._config: Config | None = None
        self._config_signature: tuple[str, int, int] | None = None
        self._config_error: str = ""
        self._progress_cache: dict[str, tuple[tuple[int, int], dict[str, object]]] = {}
        self._run_cache: dict[str, tuple[tuple[int, int, int, int], dict[str, object]]] = {}
        self._history_cache: tuple[float, dict[tuple[str, str], float]] | None = None
        self._smt2_cache: dict[str, tuple[float, float, list[Path]]] = {}
        self._smt2_lock = threading.Lock()
        self._metrics_lock = threading.Lock()
        self._metrics_cache: dict[str, _MetricsState] = {}
        self._purge_lock = threading.Lock()
        self._purge_threads: list[threading.Thread] = []

    def _solver_config(self) -> Config:
        """Return the current project config, reloading it when TOML changes."""
        try:
            config_path = find_config_path(self.host_cwd)
            stat = config_path.stat()
            signature = (str(config_path), stat.st_mtime_ns, stat.st_size)
        except (OSError, RuntimeError):
            signature = None

        if self._config is not None and signature == self._config_signature:
            return self._config
        try:
            config = load_config(self.host_cwd)
        except (OSError, RuntimeError) as exc:
            self._config = None
            self._config_signature = signature
            self._config_error = str(exc)
            raise ValueError(self._config_error) from None
        self._config = config
        self._config_signature = signature
        self._config_error = ""
        return config

    def config(self) -> dict[str, object]:
        try:
            config = self._solver_config()
            solvers = list(config.solvers)
            solver_options = list(config.solver_options)
            error = ""
        except ValueError as exc:
            solvers = []
            solver_options = []
            error = str(exc)
        return {
            "config_path": str(self._config.path) if self._config is not None else "",
            "config_revision": ":".join(str(value) for value in self._config_signature or ()),
            "results_root": str(self.results_root),
            "inputs_root": str(self.inputs_root),
            "solvers": solvers,
            "solver_options": solver_options,
            "config_error": error,
            "browse_available": shutil.which("osascript") is not None,
            "defaults": {"timeout": 30, "jobs": max(1, (os.cpu_count() or 2) // 2)},
            "last_run": self._read_last_run(),
        }

    @staticmethod
    def _applescript_escape(value: str) -> str:
        return value.replace("\\", "\\\\").replace('"', '\\"')

    def _last_run_path(self) -> Path:
        return self.results_root / ".last-run.json"

    def _read_last_run(self) -> dict[str, object]:
        path = self._last_run_path()
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    def _save_last_run(self, request: dict) -> None:
        """Persist the launched experiment's form values so the next session opens with them."""
        inputs = [
            value
            for value in request.get("inputs", [])
            if isinstance(value, str) and value.strip()
        ]
        if not inputs:
            fallback = request.get("input", "")
            if isinstance(fallback, str) and fallback.strip():
                inputs = [fallback]
        state: dict[str, object] = {
            "input": inputs[0] if inputs else "",
            "inputs": inputs,
            "solvers": list(dict.fromkeys(value for value in request.get("solvers", []) if isinstance(value, str))),
            "timeout": request.get("timeout"),
            "jobs": request.get("jobs"),
            "limit": request.get("limit", 0),
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }
        self.results_root.mkdir(parents=True, exist_ok=True)
        path = self._last_run_path()
        temporary = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        try:
            temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)

    def _managed_process_alive(self, run_id: str) -> bool:
        with self._process_lock:
            process = self.processes.get(run_id)
            if process is None:
                return False
            if process.poll() is None:
                return True
            self.processes.pop(run_id, None)
            return False

    def _reap_managed_processes(self) -> None:
        """Drop exited child handles in O(number of launched children), not O(history)."""
        with self._process_lock:
            for run_id, process in list(self.processes.items()):
                if process.poll() is not None:
                    self.processes.pop(run_id, None)

    def _run_is_live(self, run_id: str) -> bool:
        if self._managed_process_alive(run_id):
            return True
        run_dir = self._run_dir(run_id)
        return _read_run_pid(run_dir / ".run.pid") is not None or _run_lock_held(run_dir / ".run.lock")

    @staticmethod
    def _update_quote_state(state: _MetricsState, data: bytes) -> None:
        index = 0
        while index < len(data):
            if data[index] != ord('"'):
                index += 1
                continue
            if state.in_quotes and index + 1 < len(data) and data[index + 1] == ord('"'):
                index += 2
                continue
            state.in_quotes = not state.in_quotes
            index += 1

    @staticmethod
    def _consume_metrics_record(state: _MetricsState, record: bytes) -> None:
        try:
            decoded = record.rstrip(b"\r").decode("utf-8")
            row = next(csv.reader([decoded], delimiter="\t", strict=True))
        except (UnicodeDecodeError, csv.Error, StopIteration):
            state.invalid = True
            return
        if not state.header_seen:
            state.header_seen = True
            if row != RESULT_FIELDS:
                state.invalid = True
            return
        if len(row) != len(RESULT_FIELDS):
            state.invalid = True
            return
        values = dict(zip(RESULT_FIELDS, row, strict=True))
        try:
            job_id = int(values["job_id"])
            duration = float(values["time"])
        except ValueError:
            state.invalid = True
            return
        expected = state.expected_jobs.get(job_id)
        result = values["result"].lower()
        if (
            expected is None
            or job_id in state.completed_ids
            or expected != (values["solver"], values["file"])
            or result not in VALID_TASK_RESULTS
            or not math.isfinite(duration)
            or duration < 0
        ):
            state.invalid = True
            return
        state.completed_ids.add(job_id)
        totals = state.by_solver.setdefault(values["solver"], _PerformanceTotals())
        totals.completed_jobs += 1
        if result in {"sat", "unsat"}:
            totals.solved_jobs += 1
            totals.solved_seconds += duration
            totals.par2_seconds += duration
        else:
            totals.par2_seconds += 2 * state.timeout

    def _incremental_run_metrics(self, run_id: str) -> dict[str, object] | None:
        """Backfill metrics once, then consume only bytes appended by an old runner."""
        run_dir = self._run_dir(run_id)
        results_path = run_dir / "results.tsv"
        with self._metrics_lock:
            try:
                with results_path.open("rb") as handle:
                    stat = os.fstat(handle.fileno())
                    identity = (stat.st_dev, stat.st_ino)
                    state = self._metrics_cache.get(run_id)
                    if state is None or state.identity != identity or stat.st_size < state.offset:
                        metadata = _metadata(run_dir / "metadata.txt")
                        timeout = float(metadata.get("timeout") or "")
                        if not math.isfinite(timeout) or timeout <= 0:
                            return None
                        jobs = load_jobs(run_dir / "jobs.tsv")
                        state = _MetricsState(
                            identity,
                            timeout,
                            {job.job_id: (job.solver, str(job.file_path)) for job in jobs},
                        )
                        self._metrics_cache[run_id] = state
                    if state.invalid:
                        return None
                    handle.seek(state.offset)
                    data = state.tail + handle.read()
                    state.offset = handle.tell()
            except (OSError, UnicodeError, ValueError, csv.Error):
                return None

            physical_lines = data.split(b"\n")
            state.tail = physical_lines.pop()
            for physical_line in physical_lines:
                state.record_buffer += physical_line
                self._update_quote_state(state, physical_line)
                if state.in_quotes:
                    state.record_buffer += b"\n"
                    continue
                self._consume_metrics_record(state, state.record_buffer)
                state.record_buffer = b""
                if state.invalid:
                    return None
            return state.payload() if state.header_seen else None

    def runs(self) -> list[dict[str, Any]]:
        """Return history with stale active states repaired from actual process liveness."""
        self._reap_managed_processes()
        normalized: list[dict[str, Any]] = []
        for source in scan_runs(self.results_root):
            run = dict(source)
            run_id = str(run.get("run_id") or "")
            status = str(run.get("status") or "unknown")
            # Completed history is the common case; avoid pid/lock probes for it.
            live = status in {"running", "starting", "cancelling", "interrupted", "failed"} and self._run_is_live(run_id)
            if not live and status in {"running", "starting", "cancelling"}:
                run["stale_status"] = status
                run["status"] = "interrupted"
            elif live and status in {"interrupted", "failed"}:
                # A resume child can be live briefly before it publishes its first snapshot.
                run["status"] = "starting"
            if not isinstance(run.get("performance"), dict) and (
                live or run_id in self._metrics_cache
            ):
                metrics = self._incremental_run_metrics(run_id)
                if metrics is not None:
                    run.update(metrics)
            normalized.append(run)
        return normalized

    def pick_directory(self) -> dict[str, object]:
        """Open the macOS system folder picker and return the chosen absolute path."""
        if shutil.which("osascript") is None:
            raise RuntimeError(
                "no graphical folder picker is available on this server; "
                "type the benchmark directory path and click Add"
            )
        script = 'POSIX path of (choose folder with prompt "Select benchmark folder"'
        if self.inputs_root.is_dir():
            script += f' default location POSIX file "{self._applescript_escape(str(self.inputs_root))}"'
        script += ")"
        try:
            completed = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=300,
                check=False,
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError("folder picker timed out") from None
        if completed.returncode:
            raise ValueError("folder selection cancelled")
        chosen = Path(completed.stdout.strip()).expanduser().resolve()
        if not chosen.is_dir():
            raise ValueError(f"selection is not a directory: {chosen}")
        return {"path": str(chosen)}

    def _input_path(self, value: object) -> Path:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("input must be a benchmark directory")
        candidate = Path(value).expanduser()
        if not candidate.is_absolute():
            candidate = self.inputs_root / candidate
        resolved = candidate.resolve()
        if not resolved.is_dir():
            raise ValueError(f"input directory does not exist: {resolved}")
        return resolved

    @staticmethod
    def _positive_float(value: object, name: str) -> float:
        if isinstance(value, bool):
            raise ValueError(f"{name} must be a positive number")
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            raise ValueError(f"{name} must be a positive number") from None
        if not math.isfinite(parsed) or parsed <= 0:
            raise ValueError(f"{name} must be a positive number")
        return parsed

    @staticmethod
    def _nonnegative_int(value: object, name: str) -> int:
        if isinstance(value, bool):
            raise ValueError(f"{name} must be a non-negative integer")
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            raise ValueError(f"{name} must be a non-negative integer") from None
        if parsed < 0 or str(parsed) != str(value).strip():
            raise ValueError(f"{name} must be a non-negative integer")
        return parsed

    def _input_paths(self, request: dict[str, Any]) -> list[Path]:
        raw = request.get("inputs")
        if raw is None:
            value = request.get("input")
            raw = [value] if value not in (None, "") else []
        if isinstance(raw, str):
            raw = [raw]
        if not isinstance(raw, list) or not raw:
            raise ValueError("add at least one benchmark directory")
        paths: list[Path] = []
        seen: set[Path] = set()
        for item in raw:
            resolved = self._input_path(item)
            if resolved in seen:
                continue
            seen.add(resolved)
            paths.append(resolved)
        if not paths:
            raise ValueError("add at least one benchmark directory")
        return paths

    def _run_options(self, request: object) -> tuple[list[Path], list[str], float, int, int]:
        if not isinstance(request, dict):
            raise ValueError("request body must be a JSON object")
        input_dirs = self._input_paths(request)
        config = self._solver_config()
        values = request.get("solvers")
        if not isinstance(values, list):
            raise ValueError("solvers must be a list")
        solvers = list(dict.fromkeys(value for value in values if value in config.solvers))
        if not solvers:
            known = ", ".join(config.solvers)
            raise ValueError(f"choose at least one configured solver ({known})")
        timeout = self._positive_float(request.get("timeout", 30), "timeout")
        jobs = self._nonnegative_int(request.get("jobs", 1), "jobs")
        if jobs == 0:
            raise ValueError("jobs must be a positive integer")
        limit = self._nonnegative_int(request.get("limit", 0), "limit")
        return input_dirs, solvers, timeout, jobs, limit

    def _files(self, input_dirs: Path | list[Path], limit: int) -> list[Path]:
        if isinstance(input_dirs, Path):
            input_dirs = [input_dirs]
        files, _counts = self._files_and_counts(input_dirs)
        return files if limit == 0 else files[:limit]

    def _list_smt2(self, directory: Path) -> list[Path]:
        """Cached recursive *.smt2 listing. Home-NFS walks of UFLIA/AUFLIA are expensive."""
        resolved = directory.resolve()
        key = str(resolved)
        try:
            mtime = resolved.stat().st_mtime
        except OSError:
            return []
        now = time.monotonic()
        cached = self._smt2_cache.get(key)
        if cached is not None and cached[1] == mtime and now - cached[0] < 60.0:
            return cached[2]
        with self._smt2_lock:
            now = time.monotonic()
            try:
                mtime = resolved.stat().st_mtime
            except OSError:
                return []
            cached = self._smt2_cache.get(key)
            if cached is not None and cached[1] == mtime and now - cached[0] < 60.0:
                return cached[2]
            files = sorted(path.resolve() for path in resolved.rglob("*.smt2"))
            self._smt2_cache[key] = (now, mtime, files)
            return files

    def _files_and_counts(self, input_dirs: list[Path]) -> tuple[list[Path], list[dict[str, object]]]:
        files: list[Path] = []
        seen: set[Path] = set()
        counts: list[dict[str, object]] = []
        for input_dir in input_dirs:
            listed = self._list_smt2(input_dir)
            counts.append({"path": str(input_dir), "file_count": len(listed)})
            for resolved in listed:
                if resolved in seen:
                    continue
                seen.add(resolved)
                files.append(resolved)
        return files, counts

    def count_inputs(self, request: object) -> dict[str, object]:
        """Count .smt2 files in the given folders. Solvers are not involved."""
        if not isinstance(request, dict):
            raise ValueError("request body must be a JSON object")
        input_dirs = self._input_paths(request)
        files, input_counts = self._files_and_counts(input_dirs)
        joined = ", ".join(str(path) for path in input_dirs)
        return {
            "input": str(input_dirs[0]),
            "inputs": [str(path) for path in input_dirs],
            "input_counts": input_counts,
            "file_count": len(files),
            "input_valid": bool(files),
            "input_error": "" if files else f"no .smt2 files found in {joined}",
        }

    def preview(self, request: object) -> dict[str, object]:
        if not isinstance(request, dict):
            raise ValueError("request body must be a JSON object")
        input_dirs = self._input_paths(request)
        config = self._solver_config()
        values = request.get("solvers")
        solvers = (
            list(dict.fromkeys(value for value in values if value in config.solvers))
            if isinstance(values, list)
            else []
        )
        timeout = self._positive_float(request.get("timeout", 30), "timeout")
        jobs = self._nonnegative_int(request.get("jobs", 1), "jobs")
        if jobs == 0:
            jobs = 1
        limit = self._nonnegative_int(request.get("limit", 0), "limit")
        all_files, input_counts = self._files_and_counts(input_dirs)
        files = all_files if limit == 0 else all_files[:limit]
        total_file_count = len(all_files)
        selected_file_count = len(files)
        pairs = len(files) * len(solvers)
        batches = math.ceil(pairs / jobs) if pairs else 0
        # A job in this run can never outlast the watchdog deadline, so cap
        # historical durations (which may have been recorded under a larger
        # timeout) at it; otherwise the LPT estimate can exceed the worst-case
        # bound, which is derived from the current timeout budget.
        watchdog_seconds = timeout + max(15.0, timeout * 0.5)
        # A cold history scan walks every results.tsv under the results root
        # (including 20k-file campaigns) and blocks the dashboard. Use the
        # cache when warm; otherwise estimate from the timeout budget.
        now = time.monotonic()
        if self._history_cache is not None and now - self._history_cache[0] < 30.0:
            history = self._history_cache[1]
        else:
            history = {}
        predicted: list[float] = []
        historical_pairs = 0
        for path in files:
            file_name = str(path)
            for solver in solvers:
                key = (file_name, solver)
                duration = history.get(key)
                if duration is None:
                    predicted.append(timeout)
                else:
                    historical_pairs += 1
                    predicted.append(min(duration, watchdog_seconds))
        estimated_seconds = self._scheduled_duration(predicted, jobs)
        joined = ", ".join(str(path) for path in input_dirs)
        return {
            "input": str(input_dirs[0]),
            "inputs": [str(path) for path in input_dirs],
            "input_counts": input_counts,
            # Keep the total benchmark-set size separate from the limit-bounded
            # selection used for scheduling and runtime estimation.
            "file_count": total_file_count,
            "total_file_count": total_file_count,
            "selected_file_count": selected_file_count,
            "input_valid": bool(all_files),
            "input_error": "" if all_files else f"no .smt2 files found in {joined}",
            "solvers": solvers,
            "estimated_seconds": estimated_seconds,
            "worst_case_seconds": batches * watchdog_seconds,
            "historical_pairs": historical_pairs,
            "fallback_pairs": pairs - historical_pairs,
        }

    def launch(self, request: object) -> dict[str, object]:
        if not isinstance(request, dict):
            raise ValueError("request body must be a JSON object")
        requested_name = request.get("name", "")
        if not isinstance(requested_name, str):
            raise ValueError("name must be text")
        run_id = requested_name.strip() or datetime.now().strftime("run-%Y%m%d-%H%M%S-%f")
        if not RUN_NAME.fullmatch(run_id):
            raise ValueError("name may contain only letters, digits, '.', '_' and '-'")
        output_dir = self.results_root / run_id

        input_dirs, solvers, timeout, jobs, limit = self._run_options(request)
        if not self._files(input_dirs, limit):
            joined = ", ".join(str(path) for path in input_dirs)
            raise ValueError(f"no .smt2 files found in {joined}")

        self.results_root.mkdir(parents=True, exist_ok=True)
        command = [
            sys.executable,
            "-m",
            "smtbatch",
            "run",
            "--output",
            str(output_dir),
            "--timeout",
            f"{timeout:g}",
            "--jobs",
            str(jobs),
            "--limit",
            str(limit),
            "--log",
            "all",
        ]
        for input_dir in input_dirs:
            command.extend(("--input", str(input_dir)))
        for solver in solvers:
            command.extend(("--solver", solver))
        controller_log = self.results_root / f".{run_id}.controller.log"
        with self._process_lock:
            if output_dir.exists() or self._managed_process_alive(run_id):
                raise ValueError(f"experiment already exists: {run_id}")
            with controller_log.open("w", encoding="utf-8") as log_handle:
                process = subprocess.Popen(
                    command,
                    cwd=self.host_cwd,
                    stdin=subprocess.DEVNULL,
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    text=True,
                )
            self.processes[run_id] = process
        warning = ""
        try:
            self._save_last_run(
                {
                    "input": str(input_dirs[0]),
                    "inputs": [str(path) for path in input_dirs],
                    "solvers": solvers,
                    "timeout": timeout,
                    "jobs": jobs,
                    "limit": limit,
                }
            )
        except OSError as exc:
            warning = f"experiment started, but the last-run form could not be saved: {exc}"
        return {"run_id": run_id, "status": "starting", "warning": warning}

    def cancel_run(self, run_id: str) -> dict[str, object]:
        """Gracefully interrupt a running experiment by signalling its batch process.

        The run controller marks progress.json as interrupted and lets the small
        number of in-flight solver jobs drain, preserving their results. The pid
        comes from the ``.run.pid`` file written by the run process, so cancelling
        works even after the dashboard itself restarted.
        """
        run_dir = self._run_dir(run_id)
        pid = _read_run_pid(run_dir / ".run.pid")
        if pid is None:
            # The run may still be initializing before it writes its pid file; fall back
            # to the process handle this dashboard itself launched.
            with self._process_lock:
                process = self.processes.get(run_id)
                if process is not None and process.poll() is None:
                    pid = process.pid
        if pid is None:
            raise ValueError("experiment has no active run process")
        try:
            os.kill(pid, signal.SIGINT)
        except ProcessLookupError:
            raise ValueError("run process has already exited") from None
        return {"run_id": run_id, "status": "interrupting"}

    def delete_run(self, run_id: str) -> dict[str, object]:
        """Hide one experiment immediately, then purge its files in the background.

        History listing only looks at directories with ``progress.json``. A
        synchronous ``rmtree`` of ``logs/`` can take minutes on a 30k-file run,
        which used to freeze the confirm dialog. Renaming to a dotted trash
        path first removes the card; the slow unlink happens after the HTTP
        response.
        """
        run_dir = self._run_dir(run_id)
        if not run_dir.is_dir():
            raise ValueError("experiment not found")
        if self._run_is_live(run_id):
            raise ValueError("experiment is still running; cancel it before deleting")

        trash = self._trash_path(run_id)
        try:
            run_dir.rename(trash)
        except FileNotFoundError:
            raise ValueError("experiment not found") from None
        except OSError as exc:
            raise OSError(f"unable to move experiment out of history: {exc}") from exc

        self._progress_cache.pop(run_id, None)
        with self._metrics_lock:
            self._metrics_cache.pop(run_id, None)
        _RUN_CACHE.pop(run_dir / "progress.json", None)
        _RUN_CACHE.pop(trash / "progress.json", None)
        self._schedule_purge(trash)
        return {"run_id": run_id, "status": "deleted"}

    def _trash_path(self, run_id: str) -> Path:
        safe = re.sub(r"[^A-Za-z0-9._-]+", "_", run_id).strip("._") or "run"
        return self.results_root / f".deleting-{safe}-{time.time_ns()}"

    def _schedule_purge(self, path: Path) -> None:
        thread = threading.Thread(target=_rmtree_force, args=(path,), name=f"purge-{path.name}", daemon=True)
        with self._purge_lock:
            self._purge_threads = [item for item in self._purge_threads if item.is_alive()]
            self._purge_threads.append(thread)
        thread.start()

    def _await_purges(self, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        with self._purge_lock:
            threads = list(self._purge_threads)
        for thread in threads:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            thread.join(remaining)

    def resume(self, run_id: str, request: object) -> dict[str, object]:
        """Relaunch an interrupted or failed run, rerunning only its missing jobs."""
        if not isinstance(request, dict):
            raise ValueError("request body must be a JSON object")
        run_dir = self._run_dir(run_id)
        try:
            initial_status = self._progress(run_id).get("status")
        except ValueError as exc:
            raise ValueError(f"unable to read experiment progress: {exc}") from None
        if initial_status == "complete":
            raise ValueError("experiment is already complete")
        metadata = _metadata(run_dir / "metadata.txt")
        solvers = [name for name in (metadata.get("solvers") or "").split(",") if name]
        if not solvers:
            raise ValueError("no solver list recorded in experiment metadata")
        try:
            timeout = float(metadata.get("timeout") or "")
        except ValueError:
            raise ValueError("experiment metadata has an invalid timeout") from None
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("experiment metadata has no valid timeout")
        jobs = self._nonnegative_int(request.get("jobs"), "jobs")
        if jobs == 0:
            raise ValueError("jobs must be a positive integer")
        log = metadata.get("log", "all")
        if log not in {"all", "fail", "none"}:
            raise ValueError("experiment metadata has an invalid log policy")

        command = [
            sys.executable,
            "-m",
            "smtbatch",
            "run",
            "--resume",
            "--output",
            str(run_dir),
            "--jobs",
            str(jobs),
            "--log",
            log,
        ]
        controller_log = self.results_root / f".{run_id}.controller.log"
        with self._process_lock:
            if (
                self._managed_process_alive(run_id)
                or _read_run_pid(run_dir / ".run.pid") is not None
                or _run_lock_held(run_dir / ".run.lock")
            ):
                raise ValueError("experiment is still running")
            try:
                status = self._progress(run_id).get("status")
            except ValueError as exc:
                raise ValueError(f"unable to read experiment progress: {exc}") from None
            if status == "complete":
                raise ValueError("experiment is already complete")
            # A running/starting/cancelling snapshot without a live controller is stale
            # state left by an ungraceful exit and is therefore safe to resume.
            with controller_log.open("a", encoding="utf-8") as log_handle:
                log_handle.write(f"\n[dashboard] resume requested at {datetime.now(timezone.utc).isoformat()}\n")
                log_handle.flush()
                process = subprocess.Popen(
                    command,
                    cwd=self.host_cwd,
                    stdin=subprocess.DEVNULL,
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    text=True,
                )
            self.processes[run_id] = process
        return {"run_id": run_id, "status": "resuming"}

    @staticmethod
    def _scheduled_duration(durations: list[float], workers: int) -> float:
        """Simulate the bounded worker queue to estimate its wall-clock duration."""
        if not durations:
            return 0.0
        slots = [0.0] * min(workers, len(durations))
        heapq.heapify(slots)
        for duration in sorted(durations, reverse=True):
            earliest = heapq.heappop(slots)
            heapq.heappush(slots, earliest + duration)
        return round(max(slots), 3)

    def _historical_durations(self) -> dict[tuple[str, str], float]:
        """Average recorded durations by exact formula path and solver for preview estimates."""
        now = time.monotonic()
        if self._history_cache is not None and now - self._history_cache[0] < 30.0:
            return self._history_cache[1]
        sums: dict[tuple[str, str], tuple[float, int]] = {}
        if self.results_root.is_dir():
            for dirpath, dirnames, filenames in os.walk(self.results_root):
                dirnames[:] = [name for name in dirnames if name != "logs" and not name.startswith(".")]
                if "jobs.tsv" not in filenames or "results.tsv" not in filenames:
                    continue
                run_dir = Path(dirpath)
                try:
                    jobs = {job.job_id: job for job in load_jobs(run_dir / "jobs.tsv")}
                    with (run_dir / "results.tsv").open("r", encoding="utf-8", newline="") as handle:
                        reader = csv.DictReader(handle, delimiter="\t")
                        if reader.fieldnames != RESULT_FIELDS:
                            continue
                        for row in reader:
                            job = jobs.get(int(row.get("job_id") or ""))
                            duration = float(row.get("time") or "")
                            if job is None or not math.isfinite(duration) or duration < 0:
                                continue
                            key = (str(job.file_path), job.solver)
                            total, count = sums.get(key, (0.0, 0))
                            sums[key] = (total + duration, count + 1)
                except (OSError, ValueError, csv.Error):
                    continue
        averages = {key: total / count for key, (total, count) in sums.items() if count}
        self._history_cache = (now, averages)
        return averages

    def _run_data(self, run_id: str) -> dict[str, object]:
        run_dir = self._run_dir(run_id)
        jobs_path = run_dir / "jobs.tsv"
        results_path = run_dir / "results.tsv"
        progress_path = run_dir / "progress.json"
        try:
            jobs_stat = jobs_path.stat()
            results_stat = results_path.stat()
            progress_stat = progress_path.stat()
            cache_key: tuple[int, int, int, int] | None = (
                jobs_stat.st_mtime_ns,
                results_stat.st_mtime_ns,
                results_stat.st_size,
                progress_stat.st_mtime_ns,
            )
        except OSError:
            cache_key = None
        if cache_key is not None:
            cached = self._run_cache.get(run_id)
            if cached is not None and cached[0] == cache_key:
                return cached[1]
        try:
            jobs = load_jobs(jobs_path)
        except (OSError, ValueError) as exc:
            raise ValueError(f"unable to read experiment queue: {exc}") from None

        try:
            timeout = float(_metadata(run_dir / "metadata.txt").get("timeout") or "")
        except ValueError:
            timeout = 0.0
        if not math.isfinite(timeout) or timeout <= 0:
            timeout = 0.0

        results: dict[int, dict[str, object]] = {}
        if results_path.is_file():
            try:
                with results_path.open("r", encoding="utf-8", newline="") as handle:
                    reader = csv.DictReader(handle, delimiter="\t")
                    if reader.fieldnames != RESULT_FIELDS:
                        raise ValueError("invalid results header")
                    for row in reader:
                        job_id = int(row.get("job_id") or "")
                        duration = float(row.get("time") or "")
                        results[job_id] = {
                            "result": recorded_result_row(row, timeout),
                            "time": duration,
                            "log_url": _data_url(row.get("output_path"), self.results_root),
                        }
            except (OSError, ValueError, csv.Error) as exc:
                raise ValueError(f"unable to read experiment results: {exc}") from None

        cases: list[dict[str, object]] = []
        by_file: dict[str, dict[str, object]] = {}
        solvers: list[str] = []
        completed_pairs = 0
        for job in jobs:
            if job.solver not in solvers:
                solvers.append(job.solver)
            file_name = str(job.file_path)
            case = by_file.get(file_name)
            if case is None:
                try:
                    display_name = str(job.file_path.resolve().relative_to(self.inputs_root))
                except ValueError:
                    display_name = file_name
                case = {
                    "file": display_name,
                    "file_url": f"/api/runs/{quote(run_id, safe='')}/file?path={quote(file_name, safe='')}",
                    "results": {},
                    "times": {},
                    "state": "pending",
                    "done": 0,
                    "total": 0,
                }
                by_file[file_name] = case
                cases.append(case)
            result = results.get(job.job_id)
            case_results = case["results"]
            assert isinstance(case_results, dict)
            case_results[job.solver] = result or {"result": "pending", "time": None, "log_url": ""}
            case["total"] = int(case["total"]) + 1
            if result:
                case["done"] = int(case["done"]) + 1
                completed_pairs += 1
        for case in cases:
            case_results = case["results"]
            assert isinstance(case_results, dict)
            if int(case["done"]) < int(case["total"]):
                case["state"] = "pending"
                continue
            labels = [result_label(str(item.get("result") or "")) for item in case_results.values() if isinstance(item, dict)]
            case["state"] = classify_consistency(labels).lower()
        progress = self._progress(run_id)
        payload: dict[str, object] = {
            "run_id": run_id,
            "solvers": solvers,
            "completed_pairs": completed_pairs,
            "cases": cases,
            "progress": progress,
        }
        if cache_key is not None:
            self._run_cache[run_id] = (cache_key, payload)
        return payload

    def report_summary(self, run_id: str, state: str) -> dict[str, object]:
        data = self._run_data(run_id)
        solvers = data["solvers"]
        assert isinstance(solvers, list)
        if state not in {"all", "pending", "consistent", "conflict", "hard", "error", "other"}:
            raise ValueError("invalid formula state")
        settings = data["progress"].get("settings") or {}
        try:
            timeout = float(settings.get("timeout") or "")
        except (TypeError, ValueError):
            timeout = math.nan
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("experiment metadata has no valid timeout")
        cases = [case for case in data["cases"] if isinstance(case, dict) and (state == "all" or case.get("state") == state)]
        by_solver: dict[str, dict[str, object]] = {
            solver: {
                "completed": 0,
                "solved": 0,
                "sat_seconds": 0.0,
                "unsat_seconds": 0.0,
                "unique_solved": 0,
                "outcomes": {result: 0 for result in ("sat", "unsat", "unknown", "timeout", "error")},
                "cactus": [],
            }
            for solver in solvers
        }
        solved_times_by_solver: dict[str, list[float]] = {solver: [] for solver in solvers}
        for case in cases:
            results = case["results"]
            assert isinstance(results, dict)
            solved_solvers: set[str] = set()
            for solver in solvers:
                result = results.get(solver)
                if not isinstance(result, dict):
                    continue
                label = str(result.get("result") or "pending")
                if label not in {"sat", "unsat", "unknown", "timeout", "error"}:
                    continue
                summary = by_solver[solver]
                outcomes = summary["outcomes"]
                assert isinstance(outcomes, dict)
                summary["completed"] += 1
                outcomes[label] += 1
                value = result.get("time")
                if isinstance(value, (int, float)) and math.isfinite(value) and value >= 0:
                    if label == "sat":
                        summary["sat_seconds"] += value
                    elif label == "unsat":
                        summary["unsat_seconds"] += value
                    if label in {"sat", "unsat"}:
                        summary["solved"] += 1
                        solved_solvers.add(solver)
                        solved_times_by_solver[solver].append(value)
            if len(solved_solvers) == 1:
                unique_solver = next(iter(solved_solvers))
                by_solver[unique_solver]["unique_solved"] += 1
        for solver, summary in by_solver.items():
            outcomes = summary["outcomes"]
            solved_count = summary["solved"]
            sat_count = outcomes["sat"]
            unsat_count = outcomes["unsat"]
            solved_seconds = summary["sat_seconds"] + summary["unsat_seconds"]
            summary["avg_solved_seconds"] = round(solved_seconds / solved_count, 3) if solved_count else None
            summary["avg_sat_seconds"] = round(summary["sat_seconds"] / sat_count, 3) if sat_count else None
            summary["avg_unsat_seconds"] = round(summary["unsat_seconds"] / unsat_count, 3) if unsat_count else None
            solved_times = sorted(_clamp_time(duration, timeout) for duration in solved_times_by_solver[solver])
            limit = timeout
            cactus: list[dict[str, float]] = []
            seen = 0
            for duration, group in itertools.groupby(solved_times):
                seen += sum(1 for _ in group)
                cactus.append({"time": round(duration, 3), "solved": seen})
            if not cactus or cactus[-1]["time"] < limit:
                cactus.append({"time": round(limit, 3), "solved": seen})
            summary["cactus"] = cactus
        return {
            "run_id": run_id,
            "status": data["progress"].get("status", "unknown"),
            "updated_at": data["progress"].get("updated_at", ""),
            "case_count": len(cases),
            "completed_pairs": sum(int(case["done"]) for case in cases),
            "solvers": solvers,
            "timeout": timeout,
            "by_solver": by_solver,
        }

    def report_formulas(self, run_id: str, query: str, state: str, page: int, page_size: int) -> dict[str, object]:
        data = self._run_data(run_id)
        solvers = data["solvers"]
        assert isinstance(solvers, list)
        if state not in {"all", "pending", "consistent", "conflict", "hard", "error", "other"}:
            raise ValueError("invalid formula state")
        needle = query.strip().lower()
        cases = [
            case
            for case in data["cases"]
            if isinstance(case, dict)
            and (not needle or needle in str(case["file"]).lower())
            and (state == "all" or case.get("state") == state)
        ]
        cases.sort(key=lambda case: str(case["file"]))
        total = len(cases)
        start = (page - 1) * page_size
        page_cases = []
        for case in cases[start : start + page_size]:
            results = case["results"]
            assert isinstance(results, dict)
            page_cases.append(
                {
                    "file": case["file"],
                    "file_url": case["file_url"],
                    "state": case["state"],
                    "done": case["done"],
                    "total": case["total"],
                    "results": {solver: results.get(solver, {}) for solver in solvers},
                }
            )
        return {"solvers": solvers, "total": total, "page": page, "page_size": page_size, "cases": page_cases}

    def report_scatter(self, run_id: str, left: str, right: str, state: str) -> dict[str, object]:
        data = self._run_data(run_id)
        solvers = data["solvers"]
        assert isinstance(solvers, list)
        if left not in solvers or right not in solvers:
            raise ValueError("choose two solvers from this run")
        if state not in {"all", "pending", "consistent", "conflict", "hard", "error", "other"}:
            raise ValueError("invalid formula state")
        points: list[dict[str, object]] = []
        for case in data["cases"]:
            assert isinstance(case, dict)
            if state != "all" and case.get("state") != state:
                continue
            results = case["results"]
            assert isinstance(results, dict)
            left_result, right_result = results.get(left), results.get(right)
            if not isinstance(left_result, dict) or not isinstance(right_result, dict):
                continue
            x, y = left_result.get("time"), right_result.get("time")
            if not isinstance(x, (int, float)) or not isinstance(y, (int, float)) or x < 0 or y < 0:
                continue
            points.append({"file": case["file"], "x": x, "y": y, "left": left_result.get("result"), "right": right_result.get("result")})
        total = len(points)
        limit = 5_000
        if total > limit:
            step = math.ceil(total / limit)
            points = points[::step]
        return {"left": left, "right": right, "total_points": total, "sampled": total > len(points), "points": points}

    def example_file(self, run_id: str, path: str) -> Path:
        """Resolve a case file only if it belongs to the run's immutable queue."""
        run_dir = self._run_dir(run_id)
        try:
            jobs = load_jobs(run_dir / "jobs.tsv")
        except (OSError, ValueError) as exc:
            raise ValueError(f"unable to read experiment queue: {exc}") from None
        requested = Path(path).expanduser().resolve()
        for job in jobs:
            if job.file_path.resolve() == requested and requested.is_file():
                return requested
        raise ValueError("unknown example file")

    def _run_dir(self, run_id: str) -> Path:
        relative = Path(run_id)
        if not run_id or relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
            raise ValueError("invalid experiment name")
        candidate = (self.results_root / relative).resolve()
        if not _within(candidate, self.results_root):
            raise ValueError("invalid experiment name")
        return candidate

    def _progress(self, run_id: str) -> dict[str, object]:
        run_dir = self._run_dir(run_id)
        progress_path = run_dir / "progress.json"
        metadata_path = run_dir / "metadata.txt"
        try:
            cache_key = (progress_path.stat().st_mtime_ns, metadata_path.stat().st_mtime_ns)
        except OSError as exc:
            raise ValueError(f"unable to read experiment progress: {exc}") from None
        cached = self._progress_cache.get(run_id)
        if cached is not None and cached[0] == cache_key:
            return cached[1]
        try:
            payload = json.loads(progress_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"unable to read experiment progress: {exc}") from None
        if not isinstance(payload, dict):
            raise ValueError("invalid experiment progress payload")
        try:
            relative_id = run_dir.resolve().relative_to(self.results_root).as_posix()
        except ValueError:
            raise ValueError("invalid experiment name") from None
        _normalize_progress_outcomes(run_dir, payload)
        errors = payload.get("recent_errors")
        if isinstance(errors, list):
            normalized_errors = []
            for item in errors:
                if not isinstance(item, dict):
                    continue
                record = dict(item)
                record["log_url"] = _data_url(record.get("log_path"), self.results_root)
                normalized_errors.append(record)
            payload["recent_errors"] = normalized_errors
        payload["run_id"] = relative_id
        payload["settings"] = {
            key: value
            for key, value in _metadata(run_dir / "metadata.txt").items()
            if key in {"solvers", "timeout", "jobs", "selected_files", "solver_formula_pairs"}
        }
        self._progress_cache[run_id] = (cache_key, payload)
        return payload

    def export(self, run_id: str) -> Path:
        progress = self._progress(run_id)
        if progress.get("status") != "complete":
            raise ValueError("only complete experiments can be exported")
        run_dir = self._run_dir(run_id)
        export_path = run_dir / "report.xlsx"
        completed = subprocess.run(
            [sys.executable, "-m", "smtbatch", "export", str(run_dir), "--output", str(export_path)],
            cwd=self.host_cwd,
            capture_output=True,
            text=True,
        )
        if completed.returncode:
            message = completed.stderr.strip() or completed.stdout.strip() or "Excel export failed"
            raise RuntimeError(message)
        return export_path


def _run_timeout(run_dir: Path) -> float:
    try:
        timeout = float(_metadata(run_dir / "metadata.txt").get("timeout") or "")
    except ValueError:
        return 0.0
    return timeout if math.isfinite(timeout) and timeout > 0 else 0.0


def _normalize_progress_outcomes(run_dir: Path, payload: dict[str, Any]) -> None:
    """Recount timeout vs error so kill-after SIGKILL is not shown as a solver crash.

    Only finished runs are rescanned: live progress.json is rewritten every second,
    and a growing results.tsv must not be parsed on every poll.
    """
    if payload.get("status") not in {"complete", "interrupted", "failed"}:
        return
    outcomes = payload.get("outcomes")
    if not isinstance(outcomes, dict) or not int(outcomes.get("error") or 0):
        return
    timeout = _run_timeout(run_dir)
    results_path = run_dir / "results.tsv"
    if timeout <= 0 or not results_path.is_file():
        return
    recounted: dict[str, int] = {}
    by_solver: dict[str, dict[str, int]] = {}
    try:
        with results_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            if reader.fieldnames != RESULT_FIELDS:
                return
            for row in reader:
                solver = row.get("solver") or ""
                try:
                    float(row.get("time") or "")
                    if row.get("code"):
                        int(row["code"])
                except ValueError:
                    continue
                label = recorded_result_row(row, timeout)
                if label not in VALID_TASK_RESULTS:
                    continue
                recounted[label] = recounted.get(label, 0) + 1
                solver_counts = by_solver.setdefault(solver, {})
                solver_counts[label] = solver_counts.get(label, 0) + 1
    except (OSError, csv.Error):
        return
    if not recounted:
        return
    order = ("sat", "unsat", "unknown", "timeout", "error")
    payload["outcomes"] = {name: recounted.get(name, 0) for name in order}
    if payload.get("by_solver"):
        payload["by_solver"] = {
            solver: {name: counts.get(name, 0) for name in order} for solver, counts in by_solver.items()
        }
    errors = payload.get("recent_errors")
    if isinstance(errors, list):
        for item in errors:
            if not isinstance(item, dict):
                continue
            try:
                duration = float(item.get("time") or 0)
                raw_code = item.get("code")
                if isinstance(raw_code, str):
                    code = int(raw_code) if raw_code else None
                elif isinstance(raw_code, int):
                    code = raw_code
                else:
                    code = None
            except (TypeError, ValueError):
                continue
            item["result"] = recorded_result(
                str(item.get("result") or ""),
                code,
                duration,
                timeout,
                str(item.get("output") or ""),
            )


_RUN_CACHE: dict[Path, tuple[tuple[int, int], dict[str, Any]]] = {}


def scan_runs(results_root: Path) -> list[dict[str, Any]]:
    """Return a compact, newest-first history with mtime-cached progress parsing."""
    if not results_root.is_dir():
        return []
    root = results_root.resolve()
    runs: list[dict[str, Any]] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [name for name in dirnames if name != "logs" and not name.startswith(".")]
        if "progress.json" not in filenames or "jobs.tsv" not in filenames:
            continue
        progress_path = Path(dirpath) / "progress.json"
        try:
            stat = progress_path.stat()
            signature = (stat.st_mtime_ns, stat.st_size)
        except OSError:
            continue
        cached = _RUN_CACHE.get(progress_path)
        if cached is not None and cached[0] == signature:
            runs.append(cached[1])
            continue
        run_dir = progress_path.parent
        try:
            payload = json.loads(progress_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        try:
            run_id = run_dir.resolve().relative_to(root).as_posix()
        except ValueError:
            continue
        _normalize_progress_outcomes(run_dir, payload)
        errors = payload.get("recent_errors")
        if isinstance(errors, list):
            payload["error_count"] = len(errors)
            payload.pop("recent_errors", None)
        payload["run_id"] = run_id
        if len(_RUN_CACHE) > 512:
            _RUN_CACHE.clear()
        _RUN_CACHE[progress_path] = (signature, payload)
        runs.append(payload)
    return sorted(runs, key=lambda item: str(item.get("updated_at") or ""), reverse=True)


def handler_factory(manager: ExperimentManager) -> type[BaseHTTPRequestHandler]:
    resolved_results = manager.results_root
    dashboard = dashboard_path()
    report = report_path()

    class Handler(BaseHTTPRequestHandler):
        server_version = "SMTBatchDashboard/1"

        def log_message(self, format: str, *args: object) -> None:
            # Keep normal polling quiet; errors still go through the standard response path.
            if self.path.startswith("/api/"):
                return
            super().log_message(format, *args)

        def _send_json(self, value: object, status: HTTPStatus = HTTPStatus.OK) -> None:
            body = json.dumps(value, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_file(self, path: Path, download_name: str | None = None, content_type: str | None = None) -> None:
            if not path.is_file():
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            try:
                content_type = content_type or mimetypes.guess_type(path.name)[0] or "application/octet-stream"
                size = path.stat().st_size
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(size))
                self.send_header("Cache-Control", "no-store")
                if download_name:
                    self.send_header("Content-Disposition", f'attachment; filename="{download_name}"')
                self.end_headers()
                with path.open("rb") as handle:
                    shutil.copyfileobj(handle, self.wfile)
            except OSError:
                self.send_error(HTTPStatus.NOT_FOUND)

        def do_GET(self) -> None:  # noqa: N802 - HTTP handler API.
            request_path = unquote(urlparse(self.path).path)
            if request_path == "/api/runs":
                self._send_json(
                    {
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "results_root": str(resolved_results),
                        "runs": manager.runs(),
                    }
                )
                return
            if request_path == "/api/config":
                self._send_json(manager.config())
                return
            match = re.fullmatch(r"/api/runs/(.+)/analysis/summary", request_path)
            if match:
                try:
                    query = parse_qs(urlparse(self.path).query)
                    self._send_json(manager.report_summary(match.group(1), query.get("state", ["all"])[0]))
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, HTTPStatus.CONFLICT)
                return
            match = re.fullmatch(r"/api/runs/(.+)/analysis/formulas", request_path)
            if match:
                query = parse_qs(urlparse(self.path).query)
                try:
                    page = int(query.get("page", ["1"])[0])
                    page_size = int(query.get("page_size", ["20"])[0])
                    if page < 1 or not 1 <= page_size <= 200:
                        raise ValueError("page and page_size are out of range")
                    self._send_json(
                        manager.report_formulas(
                            match.group(1),
                            query.get("query", [""])[0],
                            query.get("state", ["all"])[0],
                            page,
                            page_size,
                        )
                    )
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
                return
            match = re.fullmatch(r"/api/runs/(.+)/analysis/scatter", request_path)
            if match:
                query = parse_qs(urlparse(self.path).query)
                try:
                    self._send_json(
                        manager.report_scatter(
                            match.group(1),
                            query.get("left", [""])[0],
                            query.get("right", [""])[0],
                            query.get("state", ["all"])[0],
                        )
                    )
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
                return
            match = re.fullmatch(r"/api/runs/(.+)/file", request_path)
            if match:
                query = parse_qs(urlparse(self.path).query)
                try:
                    file_path = manager.example_file(match.group(1), query.get("path", [""])[0])
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, HTTPStatus.NOT_FOUND)
                    return
                self._send_file(file_path, content_type="text/plain; charset=utf-8")
                return
            if request_path in {"/", "/dashboard.html"}:
                self._send_file(dashboard)
                return
            if re.fullmatch(r"/runs/.+/report", request_path):
                self._send_file(report)
                return
            if request_path == "/favicon.ico":
                self.send_response(HTTPStatus.NO_CONTENT)
                self.end_headers()
                return
            if request_path.startswith("/data/"):
                candidate = (resolved_results / request_path.removeprefix("/data/")).resolve()
                if _within(candidate, resolved_results) and "logs" in candidate.relative_to(resolved_results).parts:
                    self._send_file(candidate)
                else:
                    self.send_error(HTTPStatus.FORBIDDEN)
                return
            self.send_error(HTTPStatus.NOT_FOUND)

        def _read_json(self) -> object:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                raise ValueError("invalid Content-Length") from None
            if not 0 < length <= 65_536:
                raise ValueError("request body must be between 1 and 65536 bytes")
            try:
                return json.loads(self.rfile.read(length).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                raise ValueError("request body must be valid JSON") from None

        def do_POST(self) -> None:  # noqa: N802 - HTTP handler API.
            request_path = unquote(urlparse(self.path).path)
            if request_path == "/api/input-count":
                try:
                    response = manager.count_inputs(self._read_json())
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
                    return
                self._send_json(response)
                return
            if request_path == "/api/preview":
                try:
                    response = manager.preview(self._read_json())
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
                    return
                self._send_json(response)
                return
            if request_path == "/api/pick-directory":
                try:
                    response = manager.pick_directory()
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, HTTPStatus.CONFLICT)
                    return
                except RuntimeError as exc:
                    self._send_json({"error": str(exc)}, HTTPStatus.NOT_IMPLEMENTED)
                    return
                self._send_json(response)
                return
            if request_path == "/api/runs":
                try:
                    response = manager.launch(self._read_json())
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
                    return
                except OSError as exc:
                    self._send_json({"error": f"unable to start experiment: {exc}"}, HTTPStatus.INTERNAL_SERVER_ERROR)
                    return
                self._send_json(response, HTTPStatus.CREATED)
                return
            match = re.fullmatch(r"/api/runs/(.+)/cancel", request_path)
            if match:
                try:
                    response = manager.cancel_run(match.group(1))
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, HTTPStatus.CONFLICT)
                    return
                self._send_json(response)
                return
            match = re.fullmatch(r"/api/runs/(.+)/resume", request_path)
            if match:
                try:
                    response = manager.resume(match.group(1), self._read_json())
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, HTTPStatus.CONFLICT)
                    return
                except OSError as exc:
                    self._send_json({"error": f"unable to start resume: {exc}"}, HTTPStatus.INTERNAL_SERVER_ERROR)
                    return
                self._send_json(response, HTTPStatus.ACCEPTED)
                return
            match = re.fullmatch(r"/api/runs/(.+)/delete", request_path)
            if match:
                try:
                    response = manager.delete_run(match.group(1))
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, HTTPStatus.CONFLICT)
                    return
                except OSError as exc:
                    self._send_json({"error": f"unable to delete experiment: {exc}"}, HTTPStatus.INTERNAL_SERVER_ERROR)
                    return
                self._send_json(response)
                return
            match = re.fullmatch(r"/api/runs/(.+)/export", request_path)
            if match:
                try:
                    export_path = manager.export(match.group(1))
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, HTTPStatus.CONFLICT)
                    return
                except RuntimeError as exc:
                    self._send_json({"error": str(exc)}, HTTPStatus.INTERNAL_SERVER_ERROR)
                    return
                self._send_file(export_path, f"{Path(match.group(1)).name}.xlsx")
                return
            self._send_json({"error": "not found"}, HTTPStatus.NOT_FOUND)

    return Handler


def controller_paths(results_root: Path) -> tuple[Path, Path]:
    root = results_root.expanduser().resolve()
    return root / ".dashboard.pid", root / ".dashboard.log"


def _read_pid(path: Path) -> int | None:
    try:
        pid = int(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None
    if pid <= 0:
        return None
    try:
        os.kill(pid, 0)
    except OSError:
        return None
    # Guard against pid reuse: only accept processes running this dashboard.
    completed = subprocess.run(
        ["ps", "-p", str(pid), "-o", "command="],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0 or "smtbatch" not in completed.stdout:
        return None
    return pid


def _read_run_pid(path: Path) -> int | None:
    """Read the batch controller pid from an experiment's ``.run.pid`` file."""
    try:
        pid = int(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None
    if pid <= 0:
        return None
    try:
        os.kill(pid, 0)
    except OSError:
        return None
    # Guard against pid reuse: only accept processes running a smtbatch run controller.
    completed = subprocess.run(
        ["ps", "-p", str(pid), "-o", "command="],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0 or "smtbatch" not in completed.stdout:
        return None
    if " run " not in f" {completed.stdout} ":
        return None
    return pid


def _rmtree_force(path: Path) -> None:
    """Best-effort recursive delete; chmod and retry when a file is not writable."""

    def onerror(func: Any, err_path: str, _exc_info: object) -> None:
        try:
            os.chmod(err_path, 0o700)
            func(err_path)
        except OSError:
            return

    try:
        shutil.rmtree(path, onerror=onerror)
    except FileNotFoundError:
        return
    except OSError:
        shutil.rmtree(path, ignore_errors=True)


def _run_lock_held(path: Path) -> bool:
    """Check the crash-safe controller lock without relying on a pid or process name."""
    if not path.is_file():
        return False
    try:
        handle = path.open("a+", encoding="utf-8")
    except OSError:
        return False
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return False
    finally:
        handle.close()


def _stop_process(pid: int, timeout: float = 5.0) -> None:
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except OSError:
            return
        time.sleep(0.1)
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        pass


def _port_available(host: str, port: int) -> bool:
    server = None
    try:
        server = ThreadingHTTPServer((host, port), BaseHTTPRequestHandler)
    except OSError:
        return False
    finally:
        if server is not None:
            server.server_close()
    return True


def start_background(args: argparse.Namespace) -> int:
    inputs_root, results_root = resolve_roots(args)
    results_root.mkdir(parents=True, exist_ok=True)
    pid_path, log_path = controller_paths(results_root)
    existing = _read_pid(pid_path)
    if existing is not None:
        print(f"[dashboard] already running (pid {existing}) at http://{args.host}:{args.port}/")
        print("[dashboard] restart with: smtbatch serve restart")
        return 0
    if not _port_available(args.host, args.port):
        print(f"error: {args.host}:{args.port} is already in use; run: smtbatch serve status", file=sys.stderr)
        return 1
    command = [
        sys.executable,
        "-m",
        "smtbatch",
        "serve",
        "foreground",
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--results",
        str(results_root),
        "--inputs-root",
        str(inputs_root),
    ]
    with log_path.open("a", encoding="utf-8") as log_handle:
        process = subprocess.Popen(
            command,
            cwd=Path.cwd(),
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            text=True,
        )
    time.sleep(0.4)
    if process.poll() is not None:
        print(f"error: dashboard failed to start; see {log_path}", file=sys.stderr)
        return 1
    pid_path.write_text(f"{process.pid}\n", encoding="utf-8")
    print(f"[dashboard] running in background (pid {process.pid}) at http://{args.host}:{args.port}/")
    print("[dashboard] stop with: smtbatch serve stop")
    print(f"[dashboard] log={log_path}")
    return 0


def stop_background(args: argparse.Namespace) -> int:
    _, results_root = resolve_roots(args)
    pid_path, _ = controller_paths(results_root)
    pid = _read_pid(pid_path)
    if pid is None:
        pid_path.unlink(missing_ok=True)
        print("[dashboard] not running")
        return 0
    _stop_process(pid)
    pid_path.unlink(missing_ok=True)
    print(f"[dashboard] stopped (pid {pid})")
    return 0


def status_background(args: argparse.Namespace) -> int:
    _, results_root = resolve_roots(args)
    pid_path, log_path = controller_paths(results_root)
    pid = _read_pid(pid_path)
    if pid is None:
        pid_path.unlink(missing_ok=True)
        print(f"[dashboard] not running at http://{args.host}:{args.port}/")
        return 1
    print(f"[dashboard] running (pid {pid}) at http://{args.host}:{args.port}/")
    print(f"[dashboard] log={log_path}")
    return 0


def foreground(args: argparse.Namespace) -> int:
    inputs_root, results_root = resolve_roots(args)
    results_root.mkdir(parents=True, exist_ok=True)
    manager = ExperimentManager(results_root, inputs_root, Path.cwd())
    server = ThreadingHTTPServer((args.host, args.port), handler_factory(manager))
    url = f"http://{args.host}:{args.port}/"
    print(f"[dashboard] serving {results_root} at {url}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[dashboard] stopped")
    finally:
        server.server_close()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        args.port = _resolve_port(args)
    except (OSError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if not 1 <= args.port <= 65535:
        print("error: --port must be between 1 and 65535")
        return 2
    if not dashboard_path().is_file():
        print(f"error: dashboard file missing: {dashboard_path()}")
        return 2
    if args.host not in LOOPBACK_HOSTS:
        print("error: dashboard may only bind to 127.0.0.1 or localhost")
        return 2
    if args.action == "status":
        return status_background(args)
    if args.action == "stop":
        return stop_background(args)
    if args.action == "restart":
        stop_background(args)
        return start_background(args)
    if args.action == "foreground":
        return foreground(args)
    return start_background(args)


if __name__ == "__main__":
    raise SystemExit(main())
