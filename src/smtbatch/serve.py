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
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import parse_qs, quote, unquote, urlparse

from .config import Config, branch_status, find_config_path, load_config
from .run import write_progress_snapshot
from .task import (
    classify_consistency,
    incremental_from_row,
    load_jobs,
    result_label,
    results_header_ok,
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


def _metric_int(result: Mapping[str, object], key: str) -> int:
    value = result.get(key)
    return value if isinstance(value, int) else 0


def _query_solved(result: Mapping[str, object]) -> int:
    return _metric_int(result, "sat") + _metric_int(result, "unsat")


def _query_coverage(result: Mapping[str, object]) -> float:
    expected = _metric_int(result, "expected")
    if expected <= 0:
        return 0.0
    return _query_solved(result) / expected


def _public_result(result: object) -> dict[str, object]:
    if not isinstance(result, dict):
        return {}
    return {key: value for key, value in result.items() if not str(key).startswith("_")}


def _formula_check_sat_progress(
    case: Mapping[str, object],
    solvers: Sequence[str],
) -> tuple[int | None, int]:
    """File-level (printed check-sats, expected check-sats) for the formula list.

    Expected counts come from ``jobs.tsv``. The numerator is only defined for a
    single-solver run: how many of those commands already printed an outcome.
    """
    expected = _metric_int(case, "expected")
    results = case.get("results")
    if isinstance(results, dict):
        for solver in solvers:
            result = results.get(solver)
            if isinstance(result, dict):
                expected = max(expected, _metric_int(result, "expected"))
    queries: int | None = None
    if len(solvers) != 1 or not isinstance(results, dict):
        return queries, expected
    result = results.get(solvers[0])
    if not isinstance(result, dict):
        return queries, expected
    if str(result.get("result") or "") == "pending":
        return 0, expected
    return _metric_int(result, "queries"), expected


def _cactus_from_events(events: Sequence[tuple[float, int]], limit: float) -> list[dict[str, float]]:
    cactus: list[dict[str, float]] = []
    seen = 0
    for duration, group in itertools.groupby(sorted(events), key=lambda item: item[0]):
        if duration > limit:
            break
        seen += sum(weight for _, weight in group)
        cactus.append({"time": round(duration, 3), "solved": seen})
    if not cactus or cactus[-1]["time"] < limit:
        cactus.append({"time": round(limit, 3), "solved": seen})
    return cactus


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

    def _require_matching_branch(self) -> None:
        config = self._solver_config()
        _, valid, error = branch_status(config)
        if not valid:
            raise ValueError(error)

    def config(self) -> dict[str, object]:
        try:
            config = self._solver_config()
            solvers = list(config.solvers)
            solver_options = list(config.solver_options)
            error = ""
            branch, valid, branch_error = branch_status(config)
            target_branch = config.target_branch
        except ValueError as exc:
            solvers = []
            solver_options = []
            error = str(exc)
            branch, valid, branch_error = "", False, str(exc)
            target_branch = ""
        return {
            "config_path": str(self._config.path) if self._config is not None else "",
            "config_revision": ":".join(str(value) for value in self._config_signature or ()),
            "results_root": str(self.results_root),
            "inputs_root": str(self.inputs_root),
            "solvers": solvers,
            "solver_options": solver_options,
            "config_error": error,
            "target_branch": target_branch,
            "smtbatch_branch": branch,
            "branch_valid": valid,
            "can_launch": valid,
            "branch_error": branch_error,
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
        state: dict[str, object] = {
            "input": request.get("input", ""),
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

    def runs(self) -> list[dict[str, Any]]:
        """Return history with stale active states repaired from actual process liveness."""
        self._reap_managed_processes()
        normalized: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        for source in scan_runs(self.results_root):
            run = dict(source)
            run_id = str(run.get("run_id") or "")
            status = str(run.get("status") or "unknown")
            # Completed history is the common case; avoid pid/lock probes for it.
            live = status in {"running", "starting", "cancelling", "interrupted", "failed"} and self._run_is_live(run_id)
            if not live and status in {"running", "starting", "cancelling"}:
                # The dashboard writes starting before the child creates .run.pid.
                # Keep a fresh starting card visible across that handoff.
                if status == "starting" and _recent_timestamp(run.get("updated_at") or run.get("started_at"), now):
                    normalized.append(run)
                    continue
                run["stale_status"] = status
                run["status"] = "interrupted"
            elif live and status in {"interrupted", "failed"}:
                # A resume child can be live briefly before it publishes its first snapshot.
                run["status"] = "starting"
            normalized.append(run)
        return normalized

    def pick_directory(self) -> dict[str, object]:
        """Open the macOS system folder picker and return the chosen absolute path."""
        if shutil.which("osascript") is None:
            raise RuntimeError(
                "no graphical folder picker is available on this server; "
                "type the benchmark directory path directly into the input field"
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

    def _run_options(self, request: object) -> tuple[Path, list[str], float, int, int]:
        if not isinstance(request, dict):
            raise ValueError("request body must be a JSON object")
        input_dir = self._input_path(request.get("input"))
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
        return input_dir, solvers, timeout, jobs, limit

    @staticmethod
    def _files(input_dir: Path, limit: int) -> list[Path]:
        files = sorted(path.resolve() for path in input_dir.rglob("*.smt2"))
        return files if limit == 0 else files[:limit]

    def preview(self, request: object) -> dict[str, object]:
        input_dir, solvers, timeout, jobs, limit = self._run_options(request)
        all_files = self._files(input_dir, 0)
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
        history = self._historical_durations()
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
        return {
            "input": str(input_dir),
            # Keep the total benchmark-set size separate from the limit-bounded
            # selection used for scheduling and runtime estimation.
            "file_count": total_file_count,
            "total_file_count": total_file_count,
            "selected_file_count": selected_file_count,
            "input_valid": bool(all_files),
            "input_error": "" if all_files else f"no .smt2 files found in {input_dir}",
            "solvers": solvers,
            "estimated_seconds": estimated_seconds,
            "worst_case_seconds": batches * watchdog_seconds,
            "historical_pairs": historical_pairs,
            "fallback_pairs": pairs - historical_pairs,
        }

    def launch(self, request: object) -> dict[str, object]:
        if not isinstance(request, dict):
            raise ValueError("request body must be a JSON object")
        self._require_matching_branch()
        requested_name = request.get("name", "")
        if not isinstance(requested_name, str):
            raise ValueError("name must be text")
        run_id = requested_name.strip() or datetime.now().strftime("run-%Y%m%d-%H%M%S-%f")
        if not RUN_NAME.fullmatch(run_id):
            raise ValueError("name may contain only letters, digits, '.', '_' and '-'")
        output_dir = self.results_root / run_id

        input_dir, solvers, timeout, jobs, limit = self._run_options(request)
        if not self._files(input_dir, limit):
            raise ValueError(f"no .smt2 files found in {input_dir}")

        self.results_root.mkdir(parents=True, exist_ok=True)
        command = [
            sys.executable,
            "-m",
            "smtbatch",
            "run",
            "--input",
            str(input_dir),
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
        for solver in solvers:
            command.extend(("--solver", solver))
        controller_log = self.results_root / f".{run_id}.controller.log"
        with self._process_lock:
            if output_dir.exists() or self._managed_process_alive(run_id):
                raise ValueError(f"experiment already exists: {run_id}")
            write_progress_snapshot(
                output_dir,
                status="starting",
                phase="launching",
                startup_note="Launching controller",
                timeout=timeout,
                jobs=jobs,
                by_solver={name: {} for name in solvers},
            )
            try:
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
            except OSError as exc:
                write_progress_snapshot(
                    output_dir,
                    status="failed",
                    phase="failed",
                    startup_note=f"unable to start controller: {exc}",
                )
                raise
            self.processes[run_id] = process
        warning = ""
        try:
            self._save_last_run(
                {
                    "input": str(input_dir),
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
        """Delete one experiment directory after confirming that it is not active."""
        run_dir = self._run_dir(run_id)
        if not run_dir.is_dir():
            raise ValueError("experiment not found")
        if self._run_is_live(run_id):
            raise ValueError("experiment is still running; cancel it before deleting")

        try:
            shutil.rmtree(run_dir)
        except FileNotFoundError:
            raise ValueError("experiment not found") from None

        self._progress_cache.pop(run_id, None)
        _RUN_CACHE.pop(run_dir / "progress.json", None)
        return {"run_id": run_id, "status": "deleted"}

    def resume(self, run_id: str, request: object) -> dict[str, object]:
        """Relaunch an interrupted or failed run, rerunning only its missing jobs."""
        if not isinstance(request, dict):
            raise ValueError("request body must be a JSON object")
        self._require_matching_branch()
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
                        if not results_header_ok(reader.fieldnames):
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
            results_stat = results_path.stat() if results_path.is_file() else None
            progress_stat = progress_path.stat()
            cache_key: tuple[int, int, int, int] | None = (
                jobs_stat.st_mtime_ns,
                0 if results_stat is None else results_stat.st_mtime_ns,
                0 if results_stat is None else results_stat.st_size,
                progress_stat.st_mtime_ns,
            )
        except OSError:
            cache_key = None
        if cache_key is not None:
            cached = self._run_cache.get(run_id)
            if cached is not None and cached[0] == cache_key:
                return cached[1]
        if not jobs_path.is_file():
            progress = self._progress(run_id)
            return {
                "run_id": run_id,
                "solvers": list((progress.get("by_solver") or {})),
                "completed_pairs": 0,
                "cases": [],
                "progress": progress,
            }
        try:
            jobs = load_jobs(jobs_path)
        except (OSError, ValueError) as exc:
            raise ValueError(f"unable to read experiment queue: {exc}") from None

        results: dict[int, dict[str, object]] = {}
        if results_path.is_file():
            try:
                with results_path.open("r", encoding="utf-8", newline="") as handle:
                    reader = csv.DictReader(handle, delimiter="\t")
                    if not results_header_ok(reader.fieldnames):
                        raise ValueError("invalid results header")
                    for row in reader:
                        job_id = int(row.get("job_id") or "")
                        output_path = (row.get("output_path") or "").strip()
                        record: dict[str, object] = {
                            "result": row.get("result") or "error",
                            "time": float(row.get("time") or ""),
                            "log_url": _data_url(output_path, self.results_root),
                            **incremental_from_row(row),
                        }
                        results[job_id] = record
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
                    "source": file_name,
                    "expected": 0 if job.expected is None else job.expected,
                    "results": {},
                    "times": {},
                    "state": "pending",
                    "done": 0,
                    "total": 0,
                }
                by_file[file_name] = case
                cases.append(case)
            else:
                case["expected"] = max(int(case["expected"]), 0 if job.expected is None else job.expected)
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
        progress = data["progress"] if isinstance(data.get("progress"), dict) else {}
        settings = progress.get("settings") or {}
        try:
            timeout = float(settings.get("timeout") or progress.get("timeout") or "")
        except (TypeError, ValueError):
            timeout = math.nan
        if not math.isfinite(timeout) or timeout <= 0:
            if str(progress.get("status") or "") == "starting":
                timeout = 1.0
            else:
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
                "file_cactus": [],
                "file_complete": 0,
                "file_partial": 0,
                "file_timeout": 0,
                "file_error": 0,
                "partial_timeout": 0,
                "answers": 0,
                "expected": 0,
                "query_sat": 0,
                "query_unsat": 0,
                "query_unknown": 0,
                "query_error": 0,
                "query_timeout": 0,
                "query_unreached": 0,
                "query_solved": 0,
            }
            for solver in solvers
        }
        query_events_by_solver: dict[str, list[tuple[float, int]]] = {solver: [] for solver in solvers}
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
                summary = by_solver[solver]
                expected_count = _metric_int(result, "expected") or _metric_int(case, "expected")
                if label not in {"sat", "unsat", "unknown", "timeout", "error"}:
                    summary["expected"] += expected_count
                    continue
                outcomes = summary["outcomes"]
                assert isinstance(outcomes, dict)
                summary["completed"] += 1
                outcomes[label] += 1
                summary["file_complete"] += int(str(result.get("complete") or "") == "yes")
                file_status = str(result.get("file_status") or "")
                summary["file_partial"] += int(file_status == "partial")
                summary["file_timeout"] += int(file_status == "timeout")
                summary["file_error"] += int(file_status == "error")
                query_count = _metric_int(result, "queries")
                summary["answers"] += query_count
                summary["expected"] += expected_count
                summary["query_sat"] += _metric_int(result, "sat")
                summary["query_unsat"] += _metric_int(result, "unsat")
                summary["query_unknown"] += _metric_int(result, "unknown")
                summary["query_error"] += _metric_int(result, "error")
                summary["query_timeout"] += _metric_int(result, "timeout")
                summary["query_unreached"] += _metric_int(result, "unreached")
                query_solved = _query_solved(result)
                summary["query_solved"] += query_solved
                if label == "timeout" and query_count > 0:
                    summary["partial_timeout"] += 1
                value = result.get("time")
                if isinstance(value, (int, float)) and math.isfinite(value) and value >= 0:
                    if query_solved > 0:
                        query_events_by_solver[solver].append((value, query_solved))
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
            solved_times = sorted(solved_times_by_solver[solver])
            summary["file_cactus"] = _cactus_from_events([(duration, 1) for duration in solved_times], timeout)
            summary["cactus"] = _cactus_from_events(query_events_by_solver[solver], timeout)
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
            queries, expected = _formula_check_sat_progress(case, solvers)
            page_cases.append(
                {
                    "file": case["file"],
                    "file_url": case["file_url"],
                    "state": case["state"],
                    "done": case["done"],
                    "total": case["total"],
                    "queries": queries,
                    "expected": expected,
                    "results": {solver: _public_result(results.get(solver, {})) for solver in solvers},
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
            points.append(
                {
                    "file": case["file"],
                    "x": x,
                    "y": y,
                    "left": left_result.get("result"),
                    "right": right_result.get("result"),
                    "left_coverage": round(_query_coverage(left_result), 4),
                    "right_coverage": round(_query_coverage(right_result), 4),
                    "left_solved": _query_solved(left_result),
                    "right_solved": _query_solved(right_result),
                    "left_status": left_result.get("file_status") or "",
                    "right_status": right_result.get("file_status") or "",
                }
            )
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
            progress_mtime = progress_path.stat().st_mtime_ns
        except OSError as exc:
            raise ValueError(f"unable to read experiment progress: {exc}") from None
        try:
            metadata_mtime = metadata_path.stat().st_mtime_ns
        except OSError:
            metadata_mtime = 0
        cache_key = (progress_mtime, metadata_mtime)
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
        settings = {
            key: value
            for key, value in _metadata(run_dir / "metadata.txt").items()
            if key in {"solvers", "timeout", "jobs", "selected_files", "solver_formula_pairs"}
        }
        if "timeout" not in settings and payload.get("timeout") not in (None, ""):
            settings["timeout"] = str(payload["timeout"])
        if "jobs" not in settings and payload.get("jobs") not in (None, ""):
            settings["jobs"] = str(payload["jobs"])
        if "solvers" not in settings:
            by_solver = payload.get("by_solver")
            if isinstance(by_solver, dict) and by_solver:
                settings["solvers"] = ",".join(str(name) for name in by_solver)
        payload["settings"] = settings
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


_RUN_CACHE: dict[Path, tuple[tuple[int, int], dict[str, Any]]] = {}
_STARTUP_GRACE_SECONDS = 120.0


def _recent_timestamp(value: object, now: datetime, *, grace: float = _STARTUP_GRACE_SECONDS) -> bool:
    """True when a starting snapshot is new enough to outlive the pid handoff."""
    if not isinstance(value, str) or not value:
        return False
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return (now - stamp).total_seconds() < grace


def scan_runs(results_root: Path) -> list[dict[str, Any]]:
    """Return a compact, newest-first history with mtime-cached progress parsing."""
    if not results_root.is_dir():
        return []
    root = results_root.resolve()
    runs: list[dict[str, Any]] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [name for name in dirnames if name != "logs" and not name.startswith(".")]
        if "progress.json" not in filenames:
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
