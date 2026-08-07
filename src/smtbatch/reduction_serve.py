"""Serve the project-local reduction experiment launcher, monitor, and reports."""

from __future__ import annotations

import argparse
from collections import OrderedDict, Counter
import fcntl
import hashlib
import json
import os
from pathlib import Path
import random
import re
import secrets
import signal
import subprocess
import sys
import threading
import time
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlparse

from . import reduce as reduction
from .config import Config, branch_status, find_config_path, load_config, validate_target_branch


ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
LOOPBACK_HOSTS = {"127.0.0.1", "localhost"}
MAX_PAGE_SIZE = 100
DEFAULT_PAGE_SIZE = 25
MAX_TRAJECTORY_POINTS = 20000
BENCHMARK_FEATURE_PATTERNS = {
    "arrays": re.compile(rb"\barray|\bselect|\bstore", re.IGNORECASE),
    "bitvectors": re.compile(rb"bitvec|\bbv[a-z]|\(_\s*extract|\(_\s*zero_extend", re.IGNORECASE),
    "strings": re.compile(rb"str\.|string|seq\.|\bre\.", re.IGNORECASE),
    "quantifiers": re.compile(rb"\bforall\b|\bexists\b", re.IGNORECASE),
    "datatypes": re.compile(rb"declare-datatypes|declare-datatype|\bmatch\b|constructor", re.IGNORECASE),
    "floatingpoint": re.compile(rb"floatingpoint|fp\.", re.IGNORECASE),
    "reals": re.compile(rb"\breal\b|to_real|\bdiv\b", re.IGNORECASE),
    "optimization": re.compile(rb"maximize|minimize|assert-soft|check-sat-assuming", re.IGNORECASE),
}


def dashboard_path() -> Path:
    return Path(str(resources.files("smtbatch").joinpath("reduction_dashboard.html")))


def report_path() -> Path:
    return Path(str(resources.files("smtbatch").joinpath("reduction_report.html")))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        nargs="?",
        choices=("start", "stop", "restart", "status", "foreground"),
        default="start",
        help="controller action (default: start)",
    )
    parser.add_argument("--host", default="127.0.0.1")
    return parser.parse_args(argv)


def _config(start: Path | None = None) -> Config:
    return load_config(start)


def _project_root(start: Path | None = None) -> Path:
    return _config(start).path.parent.resolve()


def _resolve_port(args: argparse.Namespace) -> int:
    return _config(Path.cwd()).port


def _safe_id(value: object, label: str = "identifier") -> str:
    if not isinstance(value, str) or not ID_RE.fullmatch(value):
        raise ValueError(f"invalid {label}")
    return value


def _within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def _json_response(handler: BaseHTTPRequestHandler, value: object,
                   status: HTTPStatus = HTTPStatus.OK) -> None:
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    try:
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json; charset=utf-8")
        handler.send_header("Cache-Control", "no-store")
        handler.send_header("Content-Length", str(len(payload)))
        handler.end_headers()
        handler.wfile.write(payload)
    except (BrokenPipeError, ConnectionResetError):
        pass


def _read_json_body(handler: BaseHTTPRequestHandler) -> object:
    try:
        length = int(handler.headers.get("Content-Length", "0"))
    except ValueError:
        raise ValueError("invalid Content-Length") from None
    if length < 0 or length > 1_000_000:
        raise ValueError("request body is too large")
    data = handler.rfile.read(length)
    if not data:
        return {}
    try:
        return json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid JSON body: {exc}") from None


def _tracked(path: Path, root: Path) -> bool:
    try:
        relative = path.resolve().relative_to(root.resolve()).as_posix()
        result = subprocess.run(
            ["git", "-C", str(root), "ls-files", "--error-unmatch", "--", relative],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False,
        )
    except OSError:
        return False
    return result.returncode == 0


def _stat_signature(path: Path) -> tuple[int, int] | None:
    try:
        stat = path.stat()
    except OSError:
        return None
    return stat.st_ino, stat.st_size ^ stat.st_mtime_ns


def _benchmark_features(path: Path) -> set[str]:
    try:
        text = path.read_bytes()
    except OSError:
        return set()
    return {
        name for name, pattern in BENCHMARK_FEATURE_PATTERNS.items()
        if pattern.search(text)
    }


class ReductionManager:
    """Discover studies, launch frozen runs, and provide bounded live views."""

    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root.expanduser().resolve()
        self.config_path = self.project_root / "smtbatch.toml"
        self.results_root = self.project_root / "results"
        self.studies_root = self.project_root / "scripts" / "experiments"
        self.config: Config
        self.current_smtbatch_branch = ""
        self.branch_valid = False
        self.branch_error = ""
        self._config_signature: tuple[str, int, int] | None = None
        self._refresh_config()
        self.processes: dict[str, subprocess.Popen[str]] = {}
        self._process_lock = threading.RLock()
        self._study_cache: tuple[tuple[tuple[str, tuple[int, int] | None], ...], dict[str, dict[str, object]]] | None = None
        self._catalog_cache: tuple[tuple[object, ...], dict[str, object]] | None = None
        self._trajectory_cache: OrderedDict[tuple[str, str], tuple[object, dict[str, object]]] = OrderedDict()
        self._trajectory_lock = threading.Lock()

    def _refresh_config(self) -> None:
        try:
            config_path = find_config_path(self.project_root)
            stat = config_path.stat()
            signature = (str(config_path), stat.st_mtime_ns, stat.st_size)
        except (OSError, RuntimeError) as exc:
            raise ValueError(str(exc)) from None
        config = load_config(self.project_root)
        current_branch, branch_valid, branch_error = branch_status(config)
        if signature == self._config_signature:
            self.current_smtbatch_branch = current_branch
            self.branch_valid = branch_valid
            self.branch_error = branch_error
            return
        self._config_signature = signature
        self.config = config
        self.current_smtbatch_branch = current_branch
        self.branch_valid = branch_valid
        self.branch_error = branch_error
        self.config_path = config.path
        self.results_root = config.results_root.resolve()
        self.studies_root = config.studies_root.resolve()
        self._study_cache = None
        self._catalog_cache = None
        if hasattr(self, "_trajectory_lock"):
            with self._trajectory_lock:
                self._trajectory_cache.clear()

    def configuration(self) -> dict[str, object]:
        self._refresh_config()
        return {
            "target_branch": self.config.target_branch,
            "smtbatch_root": str(self.config.smtbatch_root),
            "smtbatch_branch": self.current_smtbatch_branch,
            # Keep current_branch as a compatibility alias for existing clients.
            "current_branch": self.current_smtbatch_branch,
            "branch_valid": self.branch_valid,
            "can_launch": self.branch_valid,
            "branch_error": self.branch_error,
            "reducers": list(self.config.reducer_options),
            "benchmark_database": str(self.config.benchmark_database),
            "benchmark_inputs": str(self.config.benchmark_inputs_root),
            "port": self.config.port,
        }

    def _benchmark_catalog(self) -> dict[str, object]:
        """Build the project benchmark catalogue from the frozen database.

        The generated manifest is kept under results as an internal source
        snapshot.  A run copies the normalized catalogue into its immutable
        study/plan artifacts, so the generated file is only a convenient
        bridge between the project database and the generic reduction engine.
        """
        self._refresh_config()
        database_path = self.config.benchmark_database
        template_path = self.config.benchmark_template
        input_signature = ()
        if self.config.benchmark_inputs_root.is_dir() and not self.config.benchmark_inputs_root.is_symlink():
            input_signature = tuple(
                (path.name, _stat_signature(path))
                for path in sorted(self.config.benchmark_inputs_root.iterdir())
                if path.is_file() and not path.is_symlink()
            )
        signature = (
            str(database_path), _stat_signature(database_path),
            input_signature,
            str(template_path) if template_path else "", _stat_signature(template_path) if template_path else None,
            str(self.config.path), _stat_signature(self.config.path),
            tuple(
                (name, spec.label, spec.description, spec.min_bytes, spec.max_bytes,
                 spec.any_features, spec.required_features, spec.forbidden_features,
                 spec.fallback)
                for name, spec in self.config.benchmark_categories.items()
            ),
        )
        if self._catalog_cache is not None and self._catalog_cache[0] == signature:
            return self._catalog_cache[1]

        base: dict[str, object] = {
            "id": "benchmark-catalog",
            "valid": False,
            "error": "",
            "database": {"path": str(database_path)},
            "categories": [],
            "total_benchmarks": 0,
        }
        if not self.config.benchmark_categories:
            base["error"] = "no benchmark categories are configured"
            self._catalog_cache = (signature, base)
            return base
        if not database_path.is_file() or database_path.is_symlink():
            base["error"] = f"benchmark database is missing: {database_path}"
            self._catalog_cache = (signature, base)
            return base
        if not self.config.benchmark_inputs_root.is_dir() or self.config.benchmark_inputs_root.is_symlink():
            base["error"] = f"benchmark inputs directory is missing: {self.config.benchmark_inputs_root}"
            self._catalog_cache = (signature, base)
            return base
        if template_path is None or not template_path.is_file() or template_path.is_symlink():
            base["error"] = f"benchmark template is missing: {template_path}"
            self._catalog_cache = (signature, base)
            return base
        try:
            database = json.loads(database_path.read_text(encoding="utf-8"))
            template = json.loads(template_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            base["error"] = f"unable to read benchmark catalogue: {exc}"
            self._catalog_cache = (signature, base)
            return base
        if not isinstance(database, dict) or not all(isinstance(key, str) for key in database):
            base["error"] = "benchmark database must be an object with string case IDs"
            self._catalog_cache = (signature, base)
            return base
        if not isinstance(template, dict):
            base["error"] = "benchmark template must be a JSON object"
            self._catalog_cache = (signature, base)
            return base

        category_cases: dict[str, list[str]] = {
            name: [] for name in self.config.benchmark_categories
        }
        benchmarks: list[dict[str, object]] = []
        errors: list[str] = []
        for filename in sorted(database):
            entry = database[filename]
            if not isinstance(entry, dict):
                errors.append(f"{filename}: database row is not an object")
                continue
            if Path(filename).name != filename or not ID_RE.fullmatch(filename):
                errors.append(f"{filename}: invalid benchmark filename")
                continue
            input_path = (self.config.benchmark_inputs_root / filename).resolve()
            if not _within(input_path, self.config.benchmark_inputs_root) or not input_path.is_file() or input_path.is_symlink():
                errors.append(f"{filename}: benchmark input is missing: {input_path}")
                continue
            features = _benchmark_features(input_path)
            matches = [
                spec for spec in self.config.benchmark_categories.values()
                if spec.matches(input_path.stat().st_size, features)
            ]
            if len(matches) != 1:
                errors.append(
                    f"{filename}: expected one category, matched "
                    f"{', '.join(spec.name for spec in matches) or 'none'}"
                )
                continue
            category = matches[0]
            mode = entry.get("match")
            if mode not in {"stderr", "stdout", "incorrect", "incorrect-unknown", "exitcode"}:
                errors.append(f"{filename}: unsupported database match mode {mode!r}")
                continue
            if mode in {"stderr", "stdout"}:
                predicate_match = {"match_stdout": "matched"}
            elif mode in {"incorrect", "incorrect-unknown"}:
                predicate_match = {"match_stdout": "different"}
            else:
                predicate_match = {}
            benchmark = {
                "id": filename,
                "input": str(input_path),
                "family": category.name,
                "theory": str(entry.get("theory", "unknown")),
                "predicate_mode": f"artifact-{mode}",
                "solver": dict(entry),
                "predicate": {
                    "command": ["scripts/solvers/ddsmt-artifact.sh", filename],
                    "match": predicate_match,
                },
            }
            category_cases[category.name].append(filename)
            benchmarks.append(benchmark)

        if errors:
            base["error"] = "benchmark catalogue validation failed: " + "; ".join(errors[:8])
            base["validation_errors"] = errors
            self._catalog_cache = (signature, base)
            return base

        database_sha256 = reduction._sha256_path(database_path)
        template_sha256 = reduction._sha256_path(template_path)
        category_payload = []
        for name, spec in self.config.benchmark_categories.items():
            category_payload.append({
                **spec.option,
                "count": len(category_cases[name]),
            })
        category_config = [
            {"id": name, **spec.option}
            for name, spec in self.config.benchmark_categories.items()
        ]
        category_config_sha256 = hashlib.sha256(
            json.dumps(category_config, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        catalog_payload = {
            "database": {"path": str(database_path), "sha256": database_sha256},
            "inputs": {"root": str(self.config.benchmark_inputs_root)},
            "template": {"path": str(template_path), "sha256": template_sha256},
            "categories": category_payload,
            "category_config_sha256": category_config_sha256,
        }
        template["schema_version"] = reduction.SCHEMA_VERSION
        template["kind"] = "reduction"
        template["study_id"] = "benchmark-catalog"
        template["root"] = str(self.project_root)
        template["benchmarks"] = benchmarks
        template["catalog"] = catalog_payload
        self.results_root.mkdir(parents=True, exist_ok=True)
        manifest_path = self.results_root / ".benchmark-catalog.json"
        manifest_bytes = (
            json.dumps(template, sort_keys=True, ensure_ascii=True, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        try:
            if not manifest_path.is_file() or manifest_path.read_bytes() != manifest_bytes:
                manifest_path.write_bytes(manifest_bytes)
            study = reduction.load_study(manifest_path)
        except (OSError, reduction.ReductionError) as exc:
            base["error"] = f"generated benchmark catalogue is invalid: {exc}"
            self._catalog_cache = (signature, base)
            return base
        record = {
            "id": "benchmark-catalog",
            "valid": True,
            "study_id": study["study_id"],
            "database": catalog_payload["database"],
            "template": catalog_payload["template"],
            "category_config_sha256": category_config_sha256,
            "categories": category_payload,
            "total_benchmarks": len(benchmarks),
            "default_timeout_seconds": study["limits"]["trial_wall_sec"],
            "default_outer_jobs": study["execution"]["outer_jobs"],
            "default_repeats": study["repeats"],
            "predicate_timeout_seconds": study["limits"]["predicate_timeout_sec"],
            "memory_mb": study["limits"]["memory_mb"],
            "schedule": study["execution"]["schedule"],
            "reducers": [
                self.config.reducers[str(item)].option
                for item in study["reducers"] if str(item) in self.config.reducers
            ],
            "manifest_path": str(manifest_path),
        }
        self._catalog_cache = (signature, {
            **record,
            "study": study,
            "category_cases": category_cases,
        })
        return self._catalog_cache[1]

    def benchmark_catalog(self) -> dict[str, object]:
        value = self._benchmark_catalog()
        return {
            key: value[key]
            for key in value
            if key not in {"study", "category_cases"}
        }

    def _study_files(self) -> list[Path]:
        self._refresh_config()
        if not self.studies_root.is_dir() or self.studies_root.is_symlink():
            return []
        studies = []
        for path in sorted(self.studies_root.rglob("*.json")):
            if not path.is_file() or path.is_symlink():
                continue
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError):
                continue
            if isinstance(raw, dict) and (
                raw.get("schema_version") == reduction.SCHEMA_VERSION
                or raw.get("kind") == "reduction"
            ):
                studies.append(path)
        return studies

    def _study_record(self, path: Path) -> dict[str, object]:
        base = {
            "path": str(path),
            "relative_path": path.relative_to(self.studies_root).as_posix(),
            "tracked": _tracked(path, self.project_root),
            "valid": False,
            "provenance_errors": [],
            "target_branch": self.config.target_branch,
            "smtbatch_branch": self.current_smtbatch_branch,
        }
        if not base["tracked"]:
            base["provenance_errors"] = ["manifest is not tracked by the project Git repository"]
            base["error"] = "manifest is not tracked"
            return base
        try:
            study = reduction.load_study(path)
            plan = reduction.build_plan(study, config=self.config)
        except (OSError, reduction.ReductionError, ValueError) as exc:
            base["error"] = str(exc)
            return base
        reducer_options = [self.config.reducers[str(item)].option for item in study["reducers"]]
        base.update({
            "valid": True,
            "study_id": study["study_id"],
            "root": study["root"],
            "source": study["source"],
            "repository": study["repository"],
            "environment": study["environment"],
            "execution": study["execution"],
            "limits": study["limits"],
            "repeats": study["repeats"],
            "benchmarks": [
                {
                    "id": item["id"], "family": item["family"],
                    "theory": item["theory"],
                    "predicate_mode": item["predicate_mode"],
                    "input": item["input"], "input_sha256": item["input_sha256"],
                    "solver": item["solver"],
                }
                for item in study["benchmarks"]
            ],
            "reducers": reducer_options,
            "comparisons": study["comparisons"],
            "trial_count": len(plan["jobs"]),
            "wave_count": max((int(job["wave"]) for job in plan["jobs"]), default=0),
            "outer_jobs": study["execution"]["outer_jobs"],
            "configured_reducers": list(self.config.reducer_options),
        })
        return base

    def studies(self) -> list[dict[str, object]]:
        files = self._study_files()
        signature = tuple((str(path), _stat_signature(path)) for path in files)
        if self._study_cache is None or self._study_cache[0] != signature:
            records = {}
            for path in files:
                record = self._study_record(path)
                identifier = record.get("study_id")
                key = str(identifier) if isinstance(identifier, str) else str(path)
                if key in records:
                    record["valid"] = False
                    record["error"] = "duplicate study_id"
                records[key] = record
            self._study_cache = (signature, records)
        return [dict(value) for value in self._study_cache[1].values()]

    def study(self, study_id: str) -> dict[str, object]:
        _safe_id(study_id, "study_id")
        for record in self.studies():
            if record.get("study_id") == study_id:
                return record
        raise ValueError(f"unknown study: {study_id}")

    def _run_dir(self, run_id: str) -> Path:
        self._refresh_config()
        _safe_id(run_id, "run_id")
        path = (self.results_root / run_id).resolve()
        if not _within(path, self.results_root):
            raise ValueError("run path escapes results root")
        return path

    def _process_alive(self, run_id: str) -> bool:
        with self._process_lock:
            process = self.processes.get(run_id)
            if process is None:
                return False
            if process.poll() is None:
                return True
            self.processes.pop(run_id, None)
            return False

    def _lock_held(self, run_dir: Path) -> bool:
        path = run_dir / ".run.lock"
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

    def _run_live(self, run_id: str, run_dir: Path) -> bool:
        if self._process_alive(run_id) or self._lock_held(run_dir):
            return True
        return False

    def _progress(self, run_dir: Path) -> dict[str, object]:
        try:
            value = json.loads((run_dir / "progress.json").read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            return {}
        return value if isinstance(value, dict) else {}

    def _command_env(self) -> dict[str, str]:
        source = Path(__file__).resolve().parents[1]
        current = os.environ.get("PYTHONPATH", "")
        return {
            **os.environ,
            "PYTHONPATH": str(source) + (os.pathsep + current if current else ""),
            "PYTHONUNBUFFERED": "1",
        }

    def _launch(self, run_id: str, run_dir: Path) -> dict[str, object]:
        log_path = run_dir / "controller.log"
        command = [
            sys.executable, "-m", "smtbatch", "reduce", "run", str(run_dir),
        ]
        with log_path.open("a", encoding="utf-8") as log:
            process = subprocess.Popen(
                command,
                cwd=self.project_root,
                env=self._command_env(),
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                text=True,
            )
        with self._process_lock:
            self.processes[run_id] = process
        return {
            "run_id": run_id, "status": "starting", "pid": process.pid,
            "cwd": str(self.project_root), "results_root": str(self.results_root),
            "log": str(log_path),
        }

    def create_run(self, body: object) -> dict[str, object]:
        self._refresh_config()
        if not self.branch_valid:
            raise ValueError(self.branch_error)
        # Keep accepting the old internal shape for already prepared scripts;
        # the dashboard and documented API use the catalogue shape below.
        legacy = {"study_id", "reducers", "timeout_seconds", "outer_jobs"}
        if isinstance(body, dict) and set(body) == legacy:
            study_id = _safe_id(body.get("study_id"), "study_id")
            study = self.study(study_id)
            if not study.get("valid"):
                raise ValueError(str(study.get("error", "invalid study")))
            selected = body.get("reducers")
            if not isinstance(selected, list) or not selected or not all(isinstance(item, str) for item in selected):
                raise ValueError("reducers must be a non-empty list of reducer IDs")
            if len(set(selected)) != len(selected):
                raise ValueError("reducers must not contain duplicates")
            timeout_seconds = body.get("timeout_seconds")
            outer_jobs = body.get("outer_jobs")
            if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)) or timeout_seconds <= 0:
                raise ValueError("timeout_seconds must be a positive number")
            if isinstance(outer_jobs, bool) or not isinstance(outer_jobs, int) or outer_jobs <= 0:
                raise ValueError("outer_jobs must be a positive integer")
            self.results_root.mkdir(parents=True, exist_ok=True)
            run_id = f"{study_id}-{time.strftime('%Y%m%d-%H%M%S', time.gmtime())}-{uuid.uuid4().hex[:8]}"
            run_dir = self._run_dir(run_id)
            if run_dir.exists():
                raise ValueError("run id collision; retry")
            reduction.prepare(
                Path(str(study["path"])), run_dir,
                reducers=selected, timeout_seconds=float(timeout_seconds), outer_jobs=outer_jobs,
            )
            return self._launch(run_id, run_dir)

        expected = {"categories", "reducers", "timeout_seconds", "outer_jobs", "max_files", "repeats"}
        if not isinstance(body, dict) or set(body) != expected:
            raise ValueError(
                "request must contain categories, reducers, timeout_seconds, outer_jobs, max_files, and repeats"
            )
        catalog = self._benchmark_catalog()
        if not catalog.get("valid"):
            raise ValueError(str(catalog.get("error", "invalid benchmark catalogue")))
        categories = body.get("categories")
        if not isinstance(categories, list) or not categories or not all(isinstance(item, str) for item in categories):
            raise ValueError("categories must be a non-empty list of category IDs")
        if len(set(categories)) != len(categories):
            raise ValueError("categories must not contain duplicates")
        category_cases = catalog["category_cases"]
        unknown_categories = [item for item in categories if item not in category_cases]
        if unknown_categories:
            raise ValueError(f"unknown benchmark category: {', '.join(unknown_categories)}")
        selected = body.get("reducers")
        if not isinstance(selected, list) or not selected or not all(isinstance(item, str) for item in selected):
            raise ValueError("reducers must be a non-empty list of reducer IDs")
        if len(set(selected)) != len(selected):
            raise ValueError("reducers must not contain duplicates")
        timeout_seconds = body.get("timeout_seconds")
        outer_jobs = body.get("outer_jobs")
        max_files = body.get("max_files")
        repeats = body.get("repeats")
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be a positive number")
        if isinstance(outer_jobs, bool) or not isinstance(outer_jobs, int) or outer_jobs <= 0:
            raise ValueError("outer_jobs must be a positive integer")
        if isinstance(max_files, bool) or not isinstance(max_files, int) or max_files <= 0:
            raise ValueError("max_files must be a positive integer")
        if isinstance(repeats, bool) or not isinstance(repeats, int) or repeats <= 0:
            raise ValueError("repeats must be a positive integer")
        candidate_ids = sorted({
            case_id for category in categories for case_id in category_cases[category]
        })
        if not candidate_ids:
            raise ValueError("selected benchmark categories contain no cases")
        seed = secrets.randbits(64)
        sampler = random.Random(seed)
        sampled_count = min(max_files, len(candidate_ids))
        selected_benchmark_ids = sorted(sampler.sample(candidate_ids, sampled_count))
        selection = {
            "categories": list(categories),
            "candidate_count": len(candidate_ids),
            "max_files": max_files,
            "sampled_count": sampled_count,
            "random_seed": seed,
            "selected_benchmark_ids": selected_benchmark_ids,
            "database_sha256": catalog["database"]["sha256"],
            "category_config_sha256": catalog["category_config_sha256"],
        }
        self.results_root.mkdir(parents=True, exist_ok=True)
        run_id = f"benchmark-catalog-{time.strftime('%Y%m%d-%H%M%S', time.gmtime())}-{uuid.uuid4().hex[:8]}"
        run_dir = self._run_dir(run_id)
        if run_dir.exists():
            raise ValueError("run id collision; retry")
        reduction.prepare(
            Path(str(catalog["manifest_path"])), run_dir,
            reducers=selected, timeout_seconds=float(timeout_seconds), outer_jobs=outer_jobs,
            benchmark_ids=selected_benchmark_ids, repeats=repeats, selection=selection,
        )
        return self._launch(run_id, run_dir)

    def _load_run(self, run_id: str) -> tuple[Path, dict[str, object]]:
        run_dir = self._run_dir(run_id)
        if not run_dir.is_dir():
            raise ValueError(f"unknown run: {run_id}")
        try:
            plan = reduction.load_plan(run_dir)
        except (OSError, reduction.ReductionError) as exc:
            raise ValueError(str(exc)) from None
        return run_dir, plan

    def stop(self, run_id: str, body: object) -> dict[str, object]:
        if not isinstance(body, dict) or set(body) != {"mode"}:
            raise ValueError("request must contain mode")
        mode = body.get("mode")
        if mode not in {"graceful", "immediate"}:
            raise ValueError("mode must be graceful or immediate")
        run_dir, _ = self._load_run(run_id)
        request = reduction.request_stop(run_dir, str(mode))
        if mode == "immediate":
            pid_path = run_dir / ".run.pid"
            try:
                pid = int(pid_path.read_text(encoding="utf-8").strip())
            except (OSError, ValueError):
                pid = None
            command_line = ""
            if pid:
                try:
                    command_line = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
                except OSError:
                    command_line = ""
            if pid and "smtbatch" in command_line and "reduce" in command_line and str(run_dir) in command_line:
                try:
                    os.kill(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        return {"run_id": run_id, **request}

    def resume(self, run_id: str, body: object) -> dict[str, object]:
        if body not in ({}, None):
            raise ValueError("resume does not accept execution parameters")
        self._refresh_config()
        if not self.branch_valid:
            raise ValueError(self.branch_error)
        run_dir, _ = self._load_run(run_id)
        state = self._progress(run_dir).get("status")
        if state == "complete":
            raise ValueError("run is already complete")
        if self._run_live(run_id, run_dir):
            raise ValueError("run is still active")
        return self._launch(run_id, run_dir) | {"status": "resuming"}

    def _trajectory(self, run_id: str, case_id: str) -> dict[str, object]:
        key = (run_id, case_id)
        run_dir, _ = self._load_run(run_id)
        signature: list[object] = []
        jobs_dir = run_dir / "jobs"
        if jobs_dir.is_dir():
            for path in sorted(jobs_dir.glob("*/attempts/*/predicate.jsonl")):
                value = _stat_signature(path)
                if value is not None:
                    signature.append((str(path), value))
                if len(signature) >= 256:
                    break
            for path in sorted(jobs_dir.glob("*/attempts/*/trace/trace.*.jsonl")):
                value = _stat_signature(path)
                if value is not None:
                    signature.append((str(path), value))
                if len(signature) >= 512:
                    break
        marker = tuple(signature)
        with self._trajectory_lock:
            cached = self._trajectory_cache.get(key)
            if cached is not None and cached[0] == marker:
                self._trajectory_cache.move_to_end(key)
                return cached[1]
        value = reduction.trajectory_for_case(run_dir, case_id)
        # Keep the HTTP payload bounded while retaining every accepted move,
        # the endpoints, and an even frontier for large journals.  Formal
        # report files remain complete and are generated by reduce.report.
        bounded = {**value, "trials": []}
        for trial in value.get("trials", []):
            item = dict(trial)
            trajectory = dict(item.get("trajectory", {}))
            points = list(trajectory.get("points", []))
            if len(points) > MAX_TRAJECTORY_POINTS:
                stride = max(1, len(points) // (MAX_TRAJECTORY_POINTS - 2))
                keep = {0, len(points) - 1}
                keep.update(index for index in range(0, len(points), stride))
                keep.update(
                    index for index, point in enumerate(points)
                    if isinstance(point, dict) and point.get("accepted")
                )
                selected = [points[index] for index in sorted(keep)]
                trajectory["points"] = selected
                warnings = list(trajectory.get("warnings", []))
                warnings.append("trajectory response was bounded; formal report retains all points")
                trajectory["warnings"] = list(dict.fromkeys(warnings))
            item["trajectory"] = trajectory
            bounded["trials"].append(item)
        value = bounded
        with self._trajectory_lock:
            self._trajectory_cache[key] = (marker, value)
            self._trajectory_cache.move_to_end(key)
            while len(self._trajectory_cache) > 128:
                self._trajectory_cache.popitem(last=False)
        return value

    @staticmethod
    def _trial_live_status(trial: dict[str, object]) -> str:
        status = trial.get("status")
        if status and status != "running":
            return str(status)
        trajectory = trial.get("trajectory")
        if isinstance(trajectory, dict) and trajectory.get("points"):
            return "running"
        return "pending"

    def _case_rows(self, run_id: str, query: dict[str, list[str]]) -> dict[str, object]:
        run_dir, plan = self._load_run(run_id)
        rows = reduction.case_rows(run_dir)
        requested_status = (query.get("status") or [""])[0]
        requested_reducer = (query.get("reducer") or [""])[0]
        requested_family = (query.get("family") or [""])[0]
        requested_theory = (query.get("theory") or [""])[0]
        requested_mode = (query.get("predicate_mode") or query.get("predicate-mode") or [""])[0]
        text = (query.get("q") or query.get("text") or [""])[0].lower()
        progress = self._progress(run_dir)
        active = progress.get("active", [])
        active_pairs = {
            (str(item.get("benchmark", "")), str(item.get("reducer", "")))
            for item in active if isinstance(item, dict)
        } if isinstance(active, list) else set()
        filtered = []
        for row in rows:
            if requested_family and row.get("family") != requested_family:
                continue
            if requested_theory and row.get("theory") != requested_theory:
                continue
            if requested_mode and row.get("predicate_mode") != requested_mode:
                continue
            if text and text not in json.dumps(row, ensure_ascii=False).lower():
                continue
            if requested_reducer and requested_reducer not in row.get("by_reducer", {}):
                continue
            selected_status = (
                row["by_reducer"][requested_reducer] if requested_reducer
                else row
            )
            planned = int(selected_status.get("planned", 0) or 0)
            completed = int(selected_status.get("completed", 0) or 0)
            case_id = str(row["case_id"])
            running = any(
                benchmark == case_id and (not requested_reducer or reducer == requested_reducer)
                for benchmark, reducer in active_pairs
            )
            derived_status = "running" if running else ("complete" if planned and completed >= planned else "pending")
            sealed_statuses = selected_status.get("statuses", {})
            if not isinstance(sealed_statuses, dict):
                sealed_statuses = {}
            verified_match = requested_status == "verified" and int(selected_status.get("verified", 0) or 0) > 0
            if requested_status and requested_status not in sealed_statuses and requested_status != derived_status and not verified_match:
                continue
            filtered.append({**row, "status": derived_status})
        try:
            page = max(1, int((query.get("page") or ["1"])[0]))
            page_size = min(MAX_PAGE_SIZE, max(1, int((query.get("page_size") or [str(DEFAULT_PAGE_SIZE)])[0])))
        except ValueError:
            raise ValueError("page and page_size must be integers") from None
        total = len(filtered)
        start = (page - 1) * page_size
        enriched = []
        for row in filtered[start:start + page_size]:
            trajectory = self._trajectory(run_id, str(row["case_id"]))
            calls = accepted = 0
            current_quality = {}
            for trial in trajectory.get("trials", []):
                if requested_reducer and trial.get("reducer_id") != requested_reducer:
                    continue
                value = trial.get("trajectory", {})
                if not isinstance(value, dict):
                    continue
                calls += int(value.get("candidate_calls", 0) or 0)
                accepted += int(value.get("accepted_moves", 0) or 0)
                current_quality[str(trial.get("reducer_id"))] = value.get("final")
            enriched.append({
                **row, "realtime_calls": calls, "realtime_accepted": accepted,
                "current_quality": current_quality,
            })
        return {
            "run_id": run_id, "page": page, "page_size": page_size,
            "total": total, "pages": (total + page_size - 1) // page_size,
            "cases": enriched,
            "study_id": plan["study_id"],
        }

    def summary(self, run_id: str) -> dict[str, object]:
        run_dir, plan = self._load_run(run_id)
        progress = self._progress(run_dir)
        provenance_errors: list[str] = []
        try:
            reduction.verify_runtime_identity(plan)
        except reduction.ReductionError as exc:
            provenance_errors.append(str(exc))
        state = str(progress.get("status", "prepared"))
        live = self._run_live(run_id, run_dir)
        if state in {"running", "starting", "stopping", "aborting"} and not live:
            stale_status = state
            state = "interrupted"
        else:
            stale_status = None
        results = reduction.completed_results(run_dir, plan)
        by_reducer: dict[str, dict[str, object]] = {}
        for reducer in plan["reducers"]:
            selected = [item for item in results if item["reducer_id"] == reducer["id"]]
            by_reducer[str(reducer["id"])] = {
                "id": reducer["id"], "label": reducer["label"],
                "planned": len(plan["benchmarks"]) * int(plan["repeats"]),
                "completed": len(selected),
                "verified": sum(bool(item.get("verified")) for item in selected),
                "evidence_ok": sum(bool(item.get("evidence_ok")) for item in selected),
                "predicate_calls": sum(int(item.get("predicate_calls", 0) or 0) for item in selected),
                "accepted_moves": sum(int(item.get("accepted_moves", 0) or 0) for item in selected),
                "statuses": dict(Counter(str(item.get("status")) for item in selected)),
            }
        active = progress.get("active", [])
        if not isinstance(active, list):
            active = []
        realtime_calls = realtime_accepted = 0
        current_quality: list[dict[str, object]] = []
        seen_cases: set[str] = set()
        for item in active[:64]:
            if not isinstance(item, dict):
                continue
            case_id = str(item.get("benchmark", ""))
            if not case_id or case_id in seen_cases:
                continue
            seen_cases.add(case_id)
            try:
                trajectory = self._trajectory(run_id, case_id)
            except (OSError, reduction.ReductionError, ValueError):
                continue
            for trial in trajectory["trials"]:
                trial_value = trial.get("trajectory", {})
                if not isinstance(trial_value, dict):
                    continue
                calls = int(trial_value.get("candidate_calls", 0) or 0)
                accepted = int(trial_value.get("accepted_moves", 0) or 0)
                realtime_calls += calls
                realtime_accepted += accepted
                current_quality.append({
                    "case_id": case_id, "reducer": trial.get("reducer_id"),
                    "repeat": trial.get("repeat"), "calls": calls,
                    "accepted": accepted, "quality": trial_value.get("final"),
                    "provisional": trial_value.get("provisional", True),
                })
        return {
            "run_id": run_id, "study_id": plan["study_id"], "status": state,
            "stale_status": stale_status, "live": live,
            "total_trials": len(plan["jobs"]), "completed_trials": len(results),
            "active_trials": len(active),
            "pending_trials": max(0, len(plan["jobs"]) - len(results) - len(active)),
            "outer_jobs": plan["execution"]["outer_jobs"],
            "schedule": plan["execution"]["schedule"],
            "wave_count": max((int(job["wave"]) for job in plan["jobs"]), default=0),
            "repeats": plan["repeats"],
            "selection": plan.get("selection", {}),
            "catalog": plan.get("catalog", {}),
            "limits": plan["limits"],
            "target_branch": self.config.target_branch,
            "smtbatch_branch": self.current_smtbatch_branch,
            "current_branch": self.current_smtbatch_branch,
            "prepared_smtbatch_branch": reduction.repository_branch(
                plan.get("repository", {}), self.config.smtbatch_root
            ) if isinstance(plan.get("repository"), dict) else None,
            "source_sha256": plan["source"]["sha256"],
            "plan_sha256": plan.get("plan_sha256"),
            "repository": plan.get("repository"),
            "provenance_errors": provenance_errors,
            "by_reducer": by_reducer,
            "comparisons": reduction.comparison_rows(plan, results),
            "realtime_calls": realtime_calls,
            "realtime_accepted": realtime_accepted,
            "current_quality": current_quality,
            "evidence_health": {
                "sealed": len(results),
                "verified": sum(bool(item.get("verified")) for item in results),
                "evidence_ok": sum(bool(item.get("evidence_ok")) for item in results),
                "warnings": sum(len(item.get("evidence_warnings", [])) for item in results),
            },
        }

    def runs(self) -> list[dict[str, object]]:
        self._refresh_config()
        self.results_root.mkdir(parents=True, exist_ok=True)
        records = []
        for path in sorted(self.results_root.iterdir()):
            if not path.is_dir() or path.is_symlink() or not (path / "plan.json").is_file():
                continue
            run_id = path.name
            try:
                record = self.summary(run_id)
            except (OSError, reduction.ReductionError, ValueError) as exc:
                record = {"run_id": run_id, "status": "invalid", "error": str(exc)}
            records.append(record)
        return records

    def export(self, run_id: str) -> dict[str, object]:
        self._refresh_config()
        run_dir, _ = self._load_run(run_id)
        path = reduction.export_xlsx(run_dir, run_dir / "report" / f"{run_id}.xlsx")
        if not _within(path, run_dir):
            raise ValueError("export path escapes run directory")
        return {"run_id": run_id, "path": str(path), "download": f"/api/runs/{quote(run_id)}/export"}


def handler_factory(manager: ReductionManager):
    class Handler(BaseHTTPRequestHandler):
        server_version = "SMTBatchReduction/2"

        def log_message(self, fmt: str, *args: object) -> None:
            sys.stderr.write("[serve] " + fmt % args + "\n")

        def _error(self, error: Exception, status: HTTPStatus = HTTPStatus.BAD_REQUEST) -> None:
            _json_response(self, {"error": str(error)}, status)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            try:
                if path == "/":
                    payload = dashboard_path().read_bytes()
                    self.send_response(HTTPStatus.OK)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                if re.fullmatch(r"/runs/[^/]+/report", path):
                    payload = report_path().read_bytes()
                    self.send_response(HTTPStatus.OK)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                if path == "/api/studies":
                    _json_response(
                        self,
                        {
                            "config": manager.configuration(),
                            "catalog": manager.benchmark_catalog(),
                            "studies": manager.studies(),
                        },
                    )
                    return
                match = re.fullmatch(r"/api/studies/([^/]+)", path)
                if match:
                    _json_response(self, manager.study(unquote(match.group(1))))
                    return
                if path == "/api/runs":
                    _json_response(self, {"runs": manager.runs()})
                    return
                match = re.fullmatch(r"/api/runs/([^/]+)/summary", path)
                if match:
                    _json_response(self, manager.summary(unquote(match.group(1))))
                    return
                match = re.fullmatch(r"/api/runs/([^/]+)/cases", path)
                if match:
                    _json_response(self, manager._case_rows(unquote(match.group(1)), parse_qs(parsed.query)))
                    return
                match = re.fullmatch(r"/api/runs/([^/]+)/cases/([^/]+)/trajectory", path)
                if match:
                    _json_response(self, manager._trajectory(unquote(match.group(1)), unquote(match.group(2))))
                    return
                raise ValueError("not found")
            except ValueError as exc:
                self._error(exc, HTTPStatus.NOT_FOUND if "not found" in str(exc) or "unknown" in str(exc) else HTTPStatus.BAD_REQUEST)
            except (OSError, reduction.ReductionError) as exc:
                self._error(exc, HTTPStatus.INTERNAL_SERVER_ERROR)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/")
            try:
                body = _read_json_body(self)
                if path == "/api/runs":
                    _json_response(self, manager.create_run(body), HTTPStatus.ACCEPTED)
                    return
                match = re.fullmatch(r"/api/runs/([^/]+)/stop", path)
                if match:
                    _json_response(self, manager.stop(unquote(match.group(1)), body))
                    return
                match = re.fullmatch(r"/api/runs/([^/]+)/resume", path)
                if match:
                    _json_response(self, manager.resume(unquote(match.group(1)), body), HTTPStatus.ACCEPTED)
                    return
                match = re.fullmatch(r"/api/runs/([^/]+)/export", path)
                if match:
                    value = manager.export(unquote(match.group(1)))
                    if parsed.query:
                        _json_response(self, value)
                    else:
                        export_path = Path(str(value["path"]))
                        if not _within(export_path, manager.results_root):
                            raise ValueError("export path escapes results root")
                        payload = export_path.read_bytes()
                        self.send_response(HTTPStatus.OK)
                        self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                        self.send_header("Content-Disposition", f"attachment; filename={export_path.name}")
                        self.send_header("Content-Length", str(len(payload)))
                        self.end_headers()
                        self.wfile.write(payload)
                    return
                raise ValueError("not found")
            except ValueError as exc:
                self._error(exc, HTTPStatus.NOT_FOUND if "not found" in str(exc) or "unknown" in str(exc) else HTTPStatus.BAD_REQUEST)
            except (OSError, reduction.ReductionError) as exc:
                self._error(exc, HTTPStatus.INTERNAL_SERVER_ERROR)

    return Handler


def controller_paths(results_root: Path) -> tuple[Path, Path]:
    root = results_root.expanduser().resolve()
    return root / ".serve.pid", root / ".serve.log"


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
    try:
        server = ThreadingHTTPServer((host, port), BaseHTTPRequestHandler)
    except OSError:
        return False
    server.server_close()
    return True


def _project_args(
    args: argparse.Namespace, *, enforce_target_branch: bool = False
) -> tuple[Config, Path]:
    config = _config(Path.cwd())
    root = config.path.parent.resolve()
    if Path.cwd().resolve() != root:
        raise RuntimeError(
            f"serve must be started from the project root {root}; current directory is {Path.cwd().resolve()}"
        )
    if enforce_target_branch:
        validate_target_branch(config)
    return config, root


def start_background(args: argparse.Namespace) -> int:
    config, root = _project_args(args)
    results_root = config.results_root.resolve()
    results_root.mkdir(parents=True, exist_ok=True)
    pid_path, log_path = controller_paths(results_root)
    existing = _read_pid(pid_path)
    if existing is not None:
        print(f"[serve] already running (pid {existing}) at http://{args.host}:{args.port}/")
        return 0
    if not _port_available(args.host, args.port):
        print(f"error: {args.host}:{args.port} is already in use", file=sys.stderr)
        return 1
    command = [
        sys.executable, "-m", "smtbatch", "serve", "foreground",
        "--host", args.host,
    ]
    current_pythonpath = os.environ.get("PYTHONPATH", "")
    child_env = {
        **os.environ,
        "PYTHONPATH": str(Path(__file__).resolve().parents[1]) + (
            os.pathsep + current_pythonpath if current_pythonpath else ""
        ),
    }
    with log_path.open("a", encoding="utf-8") as log:
        process = subprocess.Popen(
            command, cwd=root, env=child_env,
            stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
            start_new_session=True, text=True,
        )
    time.sleep(0.35)
    if process.poll() is not None:
        print(f"error: serve failed to start; see {log_path}", file=sys.stderr)
        return 1
    pid_path.write_text(f"{process.pid}\n", encoding="utf-8")
    print(f"[serve] running (pid {process.pid}) at http://{args.host}:{args.port}/")
    print(f"[serve] log={log_path}")
    return 0


def stop_background(args: argparse.Namespace) -> int:
    config, _ = _project_args(args)
    pid_path, _ = controller_paths(config.results_root)
    pid = _read_pid(pid_path)
    if pid is None:
        pid_path.unlink(missing_ok=True)
        print("[serve] not running")
        return 0
    _stop_process(pid)
    pid_path.unlink(missing_ok=True)
    print(f"[serve] stopped (pid {pid})")
    return 0


def status_background(args: argparse.Namespace) -> int:
    config, _ = _project_args(args)
    pid_path, log_path = controller_paths(config.results_root)
    pid = _read_pid(pid_path)
    if pid is None:
        pid_path.unlink(missing_ok=True)
        print(f"[serve] not running at http://{args.host}:{args.port}/")
        return 1
    print(f"[serve] running (pid {pid}) at http://{args.host}:{args.port}/")
    print(f"[serve] log={log_path}")
    return 0


def foreground(args: argparse.Namespace) -> int:
    config, root = _project_args(args)
    config.results_root.mkdir(parents=True, exist_ok=True)
    manager = ReductionManager(root)
    server = ThreadingHTTPServer((args.host, args.port), handler_factory(manager))
    print(f"[serve] serving reduction studies from {root} at http://{args.host}:{args.port}/", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[serve] stopped")
    finally:
        server.server_close()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        args.port = _resolve_port(args)
        if not 1 <= args.port <= 65535:
            raise RuntimeError("port must be between 1 and 65535")
        if args.host not in LOOPBACK_HOSTS:
            raise RuntimeError("serve may only bind to 127.0.0.1 or localhost")
        if not dashboard_path().is_file() or not report_path().is_file():
            raise RuntimeError("serve HTML assets are missing")
        if args.action == "status":
            return status_background(args)
        if args.action == "stop":
            return stop_background(args)
        if args.action == "restart":
            _project_args(args)
            stop_background(args)
            return start_background(args)
        if args.action == "foreground":
            return foreground(args)
        return start_background(args)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
