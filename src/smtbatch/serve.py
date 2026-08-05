"""Serve the local batch dashboard, experiment launcher, and Excel export API.

Open http://127.0.0.1:8000 after starting this command. The dashboard is
shipped as package data; all run state is discovered from progress.json files
under the results root. Solver commands come from the TOML config named by
SMTBATCH_CONFIG.
"""

from __future__ import annotations

import argparse
import csv
import heapq
import json
import math
import mimetypes
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlparse

from .config import Config, load_config
from .task import RESULT_FIELDS, classify_consistency, load_jobs, result_label


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
    parser.add_argument("--port", type=int, default=8000, help="bind port (default: 8000)")
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


class ExperimentManager:
    """Validate local UI requests and launch the regular batch CLI without a shell."""

    def __init__(self, results_root: Path, inputs_root: Path, host_cwd: Path) -> None:
        self.results_root = results_root.expanduser().resolve()
        self.inputs_root = inputs_root.expanduser().resolve()
        self.host_cwd = host_cwd
        self.processes: dict[str, subprocess.Popen[str]] = {}
        self._config: Config | None = None
        self._config_error: str = ""
        self._progress_cache: dict[str, tuple[tuple[float, float], dict[str, object]]] = {}
        self._run_cache: dict[str, tuple[tuple[float, float, int, float], dict[str, object]]] = {}
        self._history_cache: tuple[float, dict[tuple[str, str], float]] | None = None

    def _solver_config(self) -> Config:
        if self._config is None:
            try:
                self._config = load_config()
                self._config_error = ""
            except RuntimeError as exc:
                self._config_error = str(exc)
                raise ValueError(self._config_error) from None
        return self._config

    def config(self) -> dict[str, object]:
        try:
            config = self._solver_config()
            solvers = sorted(config.solvers)
            error = ""
        except ValueError as exc:
            solvers = []
            error = str(exc)
        return {
            "results_root": str(self.results_root),
            "inputs_root": str(self.inputs_root),
            "solvers": solvers,
            "config_error": error,
            "defaults": {"timeout": 30, "jobs": max(1, (os.cpu_count() or 2) // 2)},
        }

    @staticmethod
    def _applescript_escape(value: str) -> str:
        return value.replace("\\", "\\\\").replace('"', '\\"')

    def pick_directory(self) -> dict[str, object]:
        """Open the macOS system folder picker and return the chosen absolute path."""
        if shutil.which("osascript") is None:
            raise RuntimeError("system folder picker is unavailable on this platform")
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
            known = ", ".join(sorted(config.solvers))
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
        files = self._files(input_dir, limit)
        pairs = len(files) * len(solvers)
        batches = math.ceil(pairs / jobs) if pairs else 0
        history = self._historical_durations()
        predicted = [history.get((str(path), solver), timeout) for path in files for solver in solvers]
        estimated_seconds = self._scheduled_duration(predicted, jobs)
        watchdog_seconds = timeout + max(15.0, timeout * 0.5)
        return {
            "input": str(input_dir),
            "file_count": len(files),
            "solvers": solvers,
            "estimated_seconds": estimated_seconds,
            "worst_case_seconds": batches * watchdog_seconds,
            "historical_pairs": sum((str(path), solver) in history for path in files for solver in solvers),
            "fallback_pairs": pairs - sum((str(path), solver) in history for path in files for solver in solvers),
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
        if output_dir.exists() or run_id in self.processes:
            raise ValueError(f"experiment already exists: {run_id}")

        input_dir, solvers, timeout, jobs, limit = self._run_options(request)

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
        return {"run_id": run_id, "status": "starting"}

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
            progress_mtime = progress_path.stat().st_mtime
            cache_key: tuple[float, float, int, float] | None = (
                jobs_stat.st_mtime,
                results_stat.st_mtime,
                results_stat.st_size,
                progress_mtime,
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

        results: dict[int, dict[str, object]] = {}
        if results_path.is_file():
            try:
                with results_path.open("r", encoding="utf-8", newline="") as handle:
                    reader = csv.DictReader(handle, delimiter="\t")
                    if reader.fieldnames != RESULT_FIELDS:
                        raise ValueError("invalid results header")
                    for row in reader:
                        job_id = int(row.get("job_id") or "")
                        results[job_id] = {
                            "result": row.get("result") or "error",
                            "time": float(row.get("time") or ""),
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
        cases = [case for case in data["cases"] if isinstance(case, dict) and (state == "all" or case.get("state") == state)]
        by_solver: dict[str, dict[str, object]] = {
            solver: {
                "completed": 0,
                "solved": 0,
                "outcomes": {result: 0 for result in ("sat", "unsat", "unknown", "timeout", "error")},
                "cactus": [],
            }
            for solver in solvers
        }
        for case in cases:
            results = case["results"]
            assert isinstance(results, dict)
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
                summary["completed"] = int(summary["completed"]) + 1
                outcomes[label] = int(outcomes[label]) + 1
                value = result.get("time")
                if isinstance(value, (int, float)) and math.isfinite(value) and value >= 0:
                    if label in {"sat", "unsat"}:
                        summary["solved"] = int(summary["solved"]) + 1
        for solver, summary in by_solver.items():
            solved_times = sorted(
                float(case["results"][solver]["time"])
                for case in cases
                if isinstance(case["results"], dict)
                and isinstance(case["results"].get(solver), dict)
                and case["results"][solver].get("result") in {"sat", "unsat"}
                and isinstance(case["results"][solver].get("time"), (int, float))
            )
            elapsed = 0.0
            cactus: list[dict[str, float]] = []
            for count, duration in enumerate(solved_times, start=1):
                elapsed += duration
                cactus.append({"time": round(elapsed, 3), "solved": count})
            summary["cactus"] = cactus
        return {
            "run_id": run_id,
            "status": data["progress"].get("status", "unknown"),
            "updated_at": data["progress"].get("updated_at", ""),
            "case_count": len(cases),
            "completed_pairs": sum(int(case["done"]) for case in cases),
            "solvers": solvers,
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
        if left not in solvers or right not in solvers or left == right:
            raise ValueError("choose two different solvers from this run")
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
            cache_key = (progress_path.stat().st_mtime, metadata_path.stat().st_mtime)
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


_RUN_CACHE: dict[Path, tuple[float, dict[str, Any]]] = {}


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
            mtime = progress_path.stat().st_mtime
        except OSError:
            continue
        cached = _RUN_CACHE.get(progress_path)
        if cached is not None and cached[0] == mtime:
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
        _RUN_CACHE[progress_path] = (mtime, payload)
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
                        "runs": scan_runs(resolved_results),
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
