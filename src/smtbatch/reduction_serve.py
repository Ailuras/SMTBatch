"""Serve the project-local reduction experiment launcher, monitor, and reports."""

from __future__ import annotations

import argparse
from collections import OrderedDict, Counter
from datetime import datetime, timezone
import csv
import fcntl
import hashlib
import json
import os
from pathlib import Path
import random
import re
import secrets
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from urllib.parse import parse_qs, unquote, urlparse

from . import reduce as reduction
from .config import (
    BenchmarkCategorySpec,
    Config,
    branch_status,
    find_config_path,
    load_config,
    validate_target_branch,
)


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


def _byte_counts(items: list[dict[str, object]]) -> list[int]:
    sizes: list[int] = []
    for item in items:
        quality = item.get("output_quality") or {}
        if not isinstance(quality, dict):
            continue
        size = quality.get("byte_count")
        if size is not None:
            sizes.append(int(size))
    return sizes


def _stat_signature(path: Path) -> tuple[int, int] | None:
    try:
        stat = path.stat()
    except OSError:
        return None
    return stat.st_ino, stat.st_size ^ stat.st_mtime_ns


def _git_generation(root: Path) -> tuple[str, str]:
    """HEAD plus dirty fingerprint so a commit cannot reuse a stale catalog."""
    head = reduction.provenance._git(
        ["rev-parse", "HEAD"], cwd=root, check=False,
    )
    status = reduction.provenance._git(
        ["status", "--porcelain=v1", "--untracked-files=all"],
        cwd=root, check=False,
    )
    head_text = (
        head.stdout.decode("ascii", errors="replace").strip()
        if head.returncode == 0 else ""
    )
    dirty = (
        hashlib.sha256(status.stdout).hexdigest()
        if status.returncode == 0 else ""
    )
    return head_text, dirty


def _parse_iso(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _elapsed_seconds(start: object, end: object, *, live: bool) -> float | None:
    started = _parse_iso(start)
    if started is None:
        return None
    finished = datetime.now(timezone.utc) if live else _parse_iso(end)
    if finished is None:
        return None
    return max(0.0, (finished - started).total_seconds())


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
    """Build benchmark catalogs, launch frozen runs, and provide live views."""

    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root.expanduser().resolve()
        self.config_path = self.project_root / "smtbatch.toml"
        self.results_root = self.project_root / "results"
        self.config: Config
        self.current_smtbatch_branch = ""
        self.branch_valid = False
        self.branch_error = ""
        self._config_signature: tuple[str, int, int] | None = None
        self._refresh_config()
        self.processes: dict[str, subprocess.Popen[str]] = {}
        self._process_lock = threading.RLock()
        self._purge_lock = threading.Lock()
        self._purge_threads: list[threading.Thread] = []
        self._catalog_cache: tuple[tuple[object, ...], dict[str, object]] | None = None
        self._trajectory_cache: OrderedDict[tuple[str, str], tuple[object, dict[str, object]]] = OrderedDict()
        self._trajectory_lock = threading.Lock()
        self._plan_cache: OrderedDict[str, tuple[object, dict[str, object]]] = OrderedDict()
        self._results_cache: OrderedDict[str, tuple[object, list[dict[str, object]]]] = OrderedDict()
        self._cache_lock = threading.Lock()

    def _refresh_config(self) -> None:
        try:
            config_path = find_config_path(self.project_root)
            stat = config_path.stat()
            signature = (str(config_path), stat.st_mtime_ns, stat.st_size)
        except (OSError, RuntimeError) as exc:
            raise ValueError(str(exc)) from None
        config = load_config(self.project_root)
        smtbatch_branch, branch_valid, branch_error = branch_status(config)
        if signature == self._config_signature:
            self.current_smtbatch_branch = smtbatch_branch
            self.branch_valid = branch_valid
            self.branch_error = branch_error
            return
        self._config_signature = signature
        self.config = config
        self.current_smtbatch_branch = smtbatch_branch
        self.branch_valid = branch_valid
        self.branch_error = branch_error
        self.config_path = config.path
        self.results_root = config.results_root.resolve()
        self._catalog_cache = None
        if hasattr(self, "_trajectory_lock"):
            with self._trajectory_lock:
                self._trajectory_cache.clear()
        if hasattr(self, "_cache_lock"):
            with self._cache_lock:
                self._plan_cache.clear()
                self._results_cache.clear()

    def _last_run_path(self) -> Path:
        return self.results_root / ".last-run.json"

    def _read_last_run(self) -> dict[str, object]:
        path = self._last_run_path()
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    def _save_last_run(self, request: dict[str, object]) -> None:
        """Persist the latest reduction form values for the next dashboard session."""
        state: dict[str, object] = {
            "categories": list(dict.fromkeys(
                value for value in request.get("categories", []) if isinstance(value, str)
            )),
            "reducers": list(dict.fromkeys(
                value for value in request.get("reducers", []) if isinstance(value, str)
            )),
            "timeout_seconds": request.get("timeout_seconds"),
            "predicate_timeout_seconds": request.get("predicate_timeout_seconds"),
            "outer_jobs": request.get("outer_jobs"),
            "max_files": request.get("max_files", 0),
            "repeats": request.get("repeats"),
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }
        self.results_root.mkdir(parents=True, exist_ok=True)
        path = self._last_run_path()
        temporary = path.with_name(
            f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
        )
        try:
            temporary.write_text(
                json.dumps(state, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)

    def _launch_with_last_run(
        self, run_id: str, run_dir: Path, request: dict[str, object]
    ) -> dict[str, object]:
        response = dict(self._launch(run_id, run_dir))
        try:
            self._save_last_run(request)
        except OSError as exc:
            response["warning"] = f"run started, but the last-run form could not be saved: {exc}"
        return response

    def configuration(self) -> dict[str, object]:
        self._refresh_config()
        return {
            "target_branch": self.config.target_branch,
            "smtbatch_root": str(self.config.smtbatch_root),
            "smtbatch_branch": self.current_smtbatch_branch,
            "branch_valid": self.branch_valid,
            "can_launch": self.branch_valid,
            "branch_error": self.branch_error,
            "reducers": list(self.config.reducer_options),
            "benchmark_database": str(self.config.benchmark_database),
            "benchmark_inputs": str(self.config.benchmark_inputs_root),
            "port": self.config.port,
            "last_run": self._read_last_run(),
        }

    def _benchmark_catalog(self) -> dict[str, object]:
        """Build the project benchmark catalogue from the live database.

        The cache key includes the project Git generation, so a new commit
        cannot keep a serve-start identity.  A run then copies this catalogue
        into its immutable study/plan artifacts.
        """
        self._refresh_config()
        category_specs = self.config.benchmark_categories or {
            "all": BenchmarkCategorySpec(
                name="all",
                label="All benchmarks",
                description="Every benchmark in the project catalogue.",
                min_bytes=None,
                max_bytes=None,
                any_features=(),
                required_features=(),
                forbidden_features=(),
                fallback=True,
            )
        }
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
            str(self.config.benchmark_oracle),
            _stat_signature(self.config.benchmark_oracle),
            _git_generation(self.project_root),
            self.config.benchmark_identity_command,
            tuple(
                (name, spec.label, spec.description, spec.min_bytes, spec.max_bytes,
                 spec.any_features, spec.required_features, spec.forbidden_features,
                 spec.fallback)
                for name, spec in category_specs.items()
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
        if not database_path.is_file() or database_path.is_symlink():
            base["error"] = f"benchmark database is missing: {database_path}"
            self._catalog_cache = (signature, base)
            return base
        if not self.config.benchmark_inputs_root.is_dir() or self.config.benchmark_inputs_root.is_symlink():
            base["error"] = f"benchmark inputs directory is missing: {self.config.benchmark_inputs_root}"
            self._catalog_cache = (signature, base)
            return base
        if template_path is not None and (not template_path.is_file() or template_path.is_symlink()):
            base["error"] = f"benchmark template is missing: {template_path}"
            self._catalog_cache = (signature, base)
            return base
        try:
            database = json.loads(database_path.read_text(encoding="utf-8"))
            template = (
                json.loads(template_path.read_text(encoding="utf-8"))
                if template_path is not None else {}
            )
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
            name: [] for name in category_specs
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
                spec for spec in category_specs.values()
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
                    "command": [
                        sys.executable,
                        str(self.config.benchmark_oracle),
                        filename,
                    ],
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
        template_sha256 = reduction._sha256_path(template_path) if template_path is not None else None
        try:
            identity = (
                reduction.provenance.snapshot_command(
                    self.config.benchmark_identity_command,
                    cwd=self.project_root,
                    execute=True,
                )
                if self.config.benchmark_identity_command else None
            )
        except reduction.provenance.ProvenanceError as exc:
            base["error"] = f"benchmark identity validation failed: {exc}"
            self._catalog_cache = (signature, base)
            return base
        category_payload = []
        for name, spec in category_specs.items():
            category_payload.append({
                **spec.option,
                "count": len(category_cases[name]),
            })
        category_config = [
            {"id": name, **spec.option}
            for name, spec in category_specs.items()
        ]
        category_config_sha256 = hashlib.sha256(
            json.dumps(category_config, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        catalog_payload = {
            "database": {"path": str(database_path), "sha256": database_sha256},
            "inputs": {"root": str(self.config.benchmark_inputs_root)},
            "template": (
                {"path": str(template_path), "sha256": template_sha256}
                if template_path is not None else None
            ),
            "categories": category_payload,
            "category_config_sha256": category_config_sha256,
            "identity": identity,
        }
        template_limits = template.get("limits", {})
        if not isinstance(template_limits, dict):
            base["error"] = "benchmark template limits must be an object"
            self._catalog_cache = (signature, base)
            return base
        execution_template = template.get("execution", {})
        if not isinstance(execution_template, dict):
            execution_template = {}
        verification_repeats = template.get("verification_repeats", 1)
        limits = {
            "trial_wall_sec": template_limits.get("trial_wall_sec", 120),
            "predicate_timeout_sec": template_limits.get(
                "predicate_timeout_sec", self.config.predicate_timeout_sec
            ),
            "predicate_envelope_grace_sec": template_limits.get(
                "predicate_envelope_grace_sec", 3
            ),
            "memory_mb": template_limits.get("memory_mb", 8192),
            "preflight_repeats": template_limits.get("preflight_repeats", 1),
            "verification_repeats": template_limits.get(
                "verification_repeats", verification_repeats
            ),
            "termination_grace_sec": template_limits.get("termination_grace_sec", 5),
            "analysis_horizon_sec": template_limits.get(
                "analysis_horizon_sec", template_limits.get("trial_wall_sec", 120)
            ),
        }
        wrapper_script = Path(__file__).with_name("predicate.py").resolve()
        predicate_wrapper = {
            "command": [
                sys.executable,
                str(wrapper_script),
                "--log", "{journal}",
                "--phase", "{phase}",
                "--solver-timeout", "{predicate_timeout}",
                "{match_args}", "--", "{command}",
            ],
            "env": {},
        }
        configured_reducers = list(self.config.reducers)
        comparisons = [list(pair) for pair in self.config.comparisons]
        normalized_template = {
            "schema_version": reduction.SCHEMA_VERSION,
            "kind": "reduction",
            "study_id": "benchmark-catalog",
            "root": str(self.project_root),
            "execution": {
                "outer_jobs": execution_template.get("outer_jobs", 1),
                "schedule": "strict-wave",
            },
            "predicate_wrapper": predicate_wrapper,
            "benchmarks": benchmarks,
            "reducers": configured_reducers,
            "repeats": template.get("repeats", 1),
            "limits": limits,
            "comparisons": comparisons,
            "catalog": catalog_payload,
        }
        self.results_root.mkdir(parents=True, exist_ok=True)
        manifest_path = self.results_root / ".benchmark-catalog.json"
        manifest_bytes = (
            json.dumps(normalized_template, sort_keys=True, ensure_ascii=True, separators=(",", ":")) + "\n"
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
            "identity": identity,
            "category_config_sha256": category_config_sha256,
            "categories": category_payload,
            "total_benchmarks": len(benchmarks),
            "default_timeout_seconds": study["limits"]["trial_wall_sec"],
            "default_outer_jobs": study["execution"]["outer_jobs"],
            "default_repeats": study["repeats"],
            "predicate_timeout_seconds": study["limits"]["predicate_timeout_sec"],
            "predicate_envelope_grace_seconds": study["limits"][
                "predicate_envelope_grace_sec"
            ],
            "preflight_repeats": study["limits"]["preflight_repeats"],
            "verification_repeats": study["limits"]["verification_repeats"],
            "termination_grace_seconds": study["limits"]["termination_grace_sec"],
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
        required = {
            "categories", "reducers", "timeout_seconds",
            "outer_jobs", "max_files", "repeats",
        }
        optional = {"predicate_timeout_seconds"}
        if (
            not isinstance(body, dict)
            or not required <= set(body)
            or set(body) - required - optional
        ):
            raise ValueError(
                "request must contain categories, reducers, timeout_seconds, "
                "outer_jobs, max_files, and repeats"
            )
        # A new run freezes the live tree. Never reuse a serve-start catalog.
        self._catalog_cache = None
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
        predicate_timeout_seconds = body.get("predicate_timeout_seconds")
        outer_jobs = body.get("outer_jobs")
        max_files = body.get("max_files")
        repeats = body.get("repeats")
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be a positive number")
        if predicate_timeout_seconds is not None and (
            isinstance(predicate_timeout_seconds, bool)
            or not isinstance(predicate_timeout_seconds, (int, float))
            or predicate_timeout_seconds <= 0
        ):
            raise ValueError("predicate_timeout_seconds must be a positive number")
        if isinstance(outer_jobs, bool) or not isinstance(outer_jobs, int) or outer_jobs <= 0:
            raise ValueError("outer_jobs must be a positive integer")
        if isinstance(max_files, bool) or not isinstance(max_files, int) or max_files < 0:
            raise ValueError("max_files must be a non-negative integer (0 means all)")
        if isinstance(repeats, bool) or not isinstance(repeats, int) or repeats <= 0:
            raise ValueError("repeats must be a positive integer")
        candidate_ids = sorted({
            case_id for category in categories for case_id in category_cases[category]
        })
        if not candidate_ids:
            raise ValueError("selected benchmark categories contain no cases")
        seed = secrets.randbits(64)
        sampler = random.Random(seed)
        sampled_count = len(candidate_ids) if max_files == 0 else min(max_files, len(candidate_ids))
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
            reducers=selected, timeout_seconds=float(timeout_seconds),
            predicate_timeout_seconds=(
                None if predicate_timeout_seconds is None
                else float(predicate_timeout_seconds)
            ),
            outer_jobs=outer_jobs,
            benchmark_ids=selected_benchmark_ids, repeats=repeats, selection=selection,
        )
        return self._launch_with_last_run(run_id, run_dir, body)

    def _load_run(self, run_id: str) -> tuple[Path, dict[str, object]]:
        run_dir = self._run_dir(run_id)
        if not run_dir.is_dir():
            raise ValueError(f"unknown run: {run_id}")
        signature = (
            _stat_signature(run_dir / "plan.json"),
            _stat_signature(run_dir / "plan.complete.json"),
        )
        with self._cache_lock:
            cached = self._plan_cache.get(run_id)
            if cached is not None and cached[0] == signature:
                self._plan_cache.move_to_end(run_id)
                return run_dir, cached[1]
        try:
            plan = reduction.load_plan(run_dir)
        except (OSError, reduction.ReductionError) as exc:
            raise ValueError(str(exc)) from None
        with self._cache_lock:
            self._plan_cache[run_id] = (signature, plan)
            self._plan_cache.move_to_end(run_id)
            while len(self._plan_cache) > 32:
                self._plan_cache.popitem(last=False)
        return run_dir, plan

    def _results_from_tsv(self, path: Path) -> list[dict[str, object]] | None:
        try:
            with path.open(encoding="utf-8", newline="") as handle:
                rows = []
                for row in csv.DictReader(handle, delimiter="\t"):
                    def opt_int(name: str) -> int | None:
                        raw = row.get(name, "")
                        if raw in ("", None):
                            return None
                        return int(float(raw))
                    output_bytes = opt_int("output_bytes")
                    rows.append({
                        "job_id": row["job_id"],
                        "benchmark_id": row["benchmark"],
                        "reducer_id": row["reducer"],
                        "repeat": int(row["repeat"]),
                        "wave": int(row.get("wave") or 0),
                        "status": row["status"],
                        "verified": str(row.get("verified", "")).lower() == "true",
                        "evidence_ok": str(row.get("evidence_ok", "")).lower() == "true",
                        "predicate_calls": int(row.get("predicate_calls") or 0),
                        "accepted_moves": int(row.get("accepted_moves") or 0),
                        "trial_wall_sec": float(row.get("trial_wall_sec") or 0),
                        "output_quality": None if output_bytes is None else {
                            "expression_count": opt_int("output_expressions"),
                            "node_count": opt_int("output_nodes"),
                            "byte_count": output_bytes,
                        },
                    })
                return rows
        except (OSError, csv.Error, KeyError, ValueError, TypeError):
            return None

    def _completed_results(
        self, run_dir: Path, plan: dict[str, object]
    ) -> list[dict[str, object]]:
        signature = (
            _stat_signature(run_dir / "progress.json"),
            _stat_signature(run_dir / "plan.json"),
            _stat_signature(run_dir / "results.tsv"),
        )
        key = str(run_dir)
        with self._cache_lock:
            cached = self._results_cache.get(key)
            if cached is not None and cached[0] == signature:
                self._results_cache.move_to_end(key)
                return cached[1]
        indexed = self._results_from_tsv(run_dir / "results.tsv")
        progress = self._progress(run_dir)
        try:
            completed = int(progress.get("completed_jobs", -1))
        except (TypeError, ValueError):
            completed = -1
        if indexed is not None and completed == len(indexed):
            results = indexed
        else:
            results = reduction.completed_results(run_dir, plan, verify_markers=False)
        with self._cache_lock:
            self._results_cache[key] = (signature, results)
            self._results_cache.move_to_end(key)
            while len(self._results_cache) > 32:
                self._results_cache.popitem(last=False)
        return results

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

    def pause(self, run_id: str) -> dict[str, object]:
        run_dir, _ = self._load_run(run_id)
        if not self._run_live(run_id, run_dir):
            raise ValueError("experiment has no active run process")
        reduction.request_stop(run_dir, "pause")
        return {"run_id": run_id, "status": "paused"}

    def delete_run(self, run_id: str) -> dict[str, object]:
        """Hide one run immediately, then purge its files in the background."""
        run_dir = self._run_dir(run_id)
        if not run_dir.is_dir():
            raise ValueError("experiment not found")
        if self._run_live(run_id, run_dir):
            raise ValueError(
                "experiment is still running; pause it and wait for in-flight jobs to finish before deleting"
            )
        trash = self._trash_path(run_id)
        try:
            run_dir.rename(trash)
        except FileNotFoundError:
            raise ValueError("experiment not found") from None
        except OSError as exc:
            raise OSError(f"unable to move experiment out of history: {exc}") from exc
        self._drop_run_caches(run_id, run_dir, trash)
        self._schedule_purge(trash)
        return {"run_id": run_id, "status": "deleted"}

    def _trash_path(self, run_id: str) -> Path:
        safe = re.sub(r"[^A-Za-z0-9._-]+", "_", run_id).strip("._") or "run"
        return self.results_root / f".deleting-{safe}-{time.time_ns()}"

    def _drop_run_caches(self, run_id: str, *directories: Path) -> None:
        with self._cache_lock:
            self._plan_cache.pop(run_id, None)
            for directory in directories:
                self._results_cache.pop(str(directory), None)
        with self._trajectory_lock:
            for key in [item for item in self._trajectory_cache if item[0] == run_id]:
                self._trajectory_cache.pop(key, None)

    def _schedule_purge(self, path: Path) -> None:
        thread = threading.Thread(
            target=_rmtree_force, args=(path,), name=f"purge-{path.name}", daemon=True,
        )
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
            reduction.clear_control_for_resume(run_dir)
            return {"run_id": run_id, "status": "resuming"}
        return self._launch(run_id, run_dir) | {"status": "resuming"}

    @staticmethod
    def _chart_points(points: list[object]) -> list[dict[str, object]]:
        if not points:
            return []
        keep = {0, len(points) - 1}
        keep.update(
            index for index, point in enumerate(points)
            if isinstance(point, dict) and point.get("accepted")
        )
        selected = []
        for index in sorted(keep):
            point = points[index]
            if not isinstance(point, dict):
                continue
            selected.append({
                "call_index": point.get("call_index"),
                "elapsed_sec": point.get("elapsed_sec"),
                "byte_count": point.get("byte_count"),
                "accepted": point.get("accepted"),
                "mutator": point.get("mutator"),
            })
        if len(selected) > MAX_TRAJECTORY_POINTS:
            stride = max(1, len(selected) // (MAX_TRAJECTORY_POINTS - 2))
            selected = [selected[0], *selected[1:-1:stride], selected[-1]]
        return selected

    def _case_artifact_signature(
        self, run_dir: Path, plan: dict[str, object], case_id: str
    ) -> tuple[object, ...]:
        parts: list[object] = []
        for job in plan.get("jobs", []):
            if not isinstance(job, dict) or job.get("benchmark_id") != case_id:
                continue
            job_dir = run_dir / "jobs" / str(job.get("job_id", ""))
            parts.append(_stat_signature(job_dir / "job.complete.json"))
            parts.append(_stat_signature(job_dir / "result.json"))
            attempts = job_dir / "attempts"
            if not attempts.is_dir():
                continue
            for attempt in sorted(attempts.iterdir()):
                if not attempt.is_dir() or not attempt.name.isdigit():
                    continue
                parts.append(_stat_signature(attempt / "predicate.jsonl"))
                parts.append(_stat_signature(attempt / "attempt.complete.json"))
        return tuple(parts)

    def _trajectory(self, run_id: str, case_id: str) -> dict[str, object]:
        key = (run_id, case_id)
        run_dir, plan = self._load_run(run_id)
        marker = self._case_artifact_signature(run_dir, plan, case_id)
        with self._trajectory_lock:
            cached = self._trajectory_cache.get(key)
            if cached is not None and cached[0] == marker:
                self._trajectory_cache.move_to_end(key)
                return cached[1]
        value = reduction.trajectory_for_case(
            run_dir, case_id, plan=plan, include_logs=False, verify_artifacts=False,
        )
        # Dashboard charts only need the step endpoints: initial, accepted
        # moves, and the last sample.  Formal report files remain complete.
        bounded = {**value, "trials": []}
        for trial in value.get("trials", []):
            item = dict(trial)
            item.pop("logs", None)
            trajectory = dict(item.get("trajectory") or {})
            trajectory["points"] = self._chart_points(list(trajectory.get("points") or []))
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

    @staticmethod
    def _normalize_list_status(value: str) -> str:
        return "completed" if value == "complete" else value

    @staticmethod
    def _derive_case_status(
        *, running: bool, planned: int, sealed: int, statuses: dict[str, object]
    ) -> str:
        if running:
            return "running"
        if planned <= 0 or sealed <= 0 or sealed < planned:
            return "pending"
        completed = int(statuses.get("completed", 0) or 0)
        truncated = int(statuses.get("truncated", 0) or 0)
        invalid = int(statuses.get("invalid", 0) or 0)
        if completed == sealed:
            return "completed"
        if truncated:
            return "truncated"
        if invalid:
            return "invalid"
        return "completed"

    @staticmethod
    def _quality_bytes(qualities: object) -> int | None:
        values: list[int] = []
        if not isinstance(qualities, list):
            return None
        for item in qualities:
            if not isinstance(item, dict):
                continue
            size = item.get("byte_count")
            if isinstance(size, bool) or not isinstance(size, (int, float)):
                continue
            values.append(int(size))
        if not values:
            return None
        values.sort()
        return values[(len(values) - 1) // 2]

    @staticmethod
    def _reducer_outcome(statuses: object) -> str:
        if not isinstance(statuses, dict):
            return ""
        completed = int(statuses.get("completed", 0) or 0)
        truncated = int(statuses.get("truncated", 0) or 0)
        invalid = int(statuses.get("invalid", 0) or 0)
        if truncated:
            return "truncated"
        if invalid and not completed:
            return "invalid"
        if completed:
            return "completed"
        return ""

    def _case_list_view(
        self, row: dict[str, object], requested_reducer: str, running: bool
    ) -> dict[str, object]:
        by_reducer = row.get("by_reducer")
        if not isinstance(by_reducer, dict):
            by_reducer = {}
        selected = (
            {requested_reducer: by_reducer.get(requested_reducer, {})}
            if requested_reducer else by_reducer
        )
        try:
            input_bytes = int(row["input_bytes"]) if row.get("input_bytes") is not None else None
        except (TypeError, ValueError):
            input_bytes = None
        reducers = []
        best_bytes: int | None = None
        winner_ids: list[str] = []
        for reducer_id, info in selected.items():
            if not isinstance(info, dict):
                continue
            bytes_value = self._quality_bytes(info.get("final_quality"))
            ratio = (
                bytes_value / input_bytes
                if bytes_value is not None and input_bytes
                else None
            )
            outcome = self._reducer_outcome(info.get("statuses"))
            reducers.append({
                "id": reducer_id,
                "label": info.get("label") or reducer_id,
                "bytes": bytes_value,
                "ratio": ratio,
                "outcome": outcome,
            })
            if bytes_value is None:
                continue
            if best_bytes is None or bytes_value < best_bytes:
                best_bytes = bytes_value
                winner_ids = [str(reducer_id)]
            elif bytes_value == best_bytes:
                winner_ids.append(str(reducer_id))
        for item in reducers:
            item["winner"] = best_bytes is not None and item.get("bytes") == best_bytes
        selected_status = (
            by_reducer.get(requested_reducer) if requested_reducer else row
        )
        if not isinstance(selected_status, dict):
            selected_status = {}
        planned = int(selected_status.get("planned", 0) or 0)
        sealed = int(selected_status.get("completed", 0) or 0)
        statuses = selected_status.get("statuses", {})
        if not isinstance(statuses, dict):
            statuses = {}
        derived = self._derive_case_status(
            running=running, planned=planned, sealed=sealed, statuses=statuses,
        )
        return {
            **row,
            "status": derived,
            "best_bytes": best_bytes,
            "winner_ids": winner_ids,
            "reducers": reducers,
        }

    def _case_rows(self, run_id: str, query: dict[str, list[str]]) -> dict[str, object]:
        run_dir, plan = self._load_run(run_id)
        rows = reduction.case_rows(
            run_dir, plan=plan, results=self._completed_results(run_dir, plan),
        )
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
            case_id = str(row["case_id"])
            running = any(
                benchmark == case_id and (not requested_reducer or reducer == requested_reducer)
                for benchmark, reducer in active_pairs
            )
            view = self._case_list_view(row, requested_reducer, running)
            wanted = self._normalize_list_status(requested_status) if requested_status else ""
            if wanted and wanted != view["status"]:
                continue
            filtered.append(view)
        sort_key = (query.get("sort") or [""])[0]
        sort_dir = (query.get("sort_dir") or ["asc"])[0]
        reverse = sort_dir == "desc"
        if sort_key == "case":
            filtered.sort(key=lambda r: str(r["case_id"]), reverse=reverse)
        elif sort_key == "status":
            filtered.sort(key=lambda r: str(r.get("status", "")), reverse=reverse)
        elif sort_key == "bytes":
            filtered.sort(
                key=lambda r: (r.get("best_bytes") is None, r.get("best_bytes") or 0),
                reverse=reverse,
            )
        try:
            page = max(1, int((query.get("page") or ["1"])[0]))
            page_size = min(MAX_PAGE_SIZE, max(1, int((query.get("page_size") or [str(DEFAULT_PAGE_SIZE)])[0])))
        except ValueError:
            raise ValueError("page and page_size must be integers") from None
        total = len(filtered)
        start = (page - 1) * page_size
        # List rows already carry sealed call/quality counts. Parsing full
        # trajectories here made /cases hang on real journals, so the report
        # left the table empty. Live curves stay on the per-case trajectory API.
        enriched = []
        for row in filtered[start:start + page_size]:
            by_reducer = row.get("by_reducer")
            if not isinstance(by_reducer, dict):
                by_reducer = {}
            selected_items = (
                {requested_reducer: by_reducer.get(requested_reducer, {})}
                if requested_reducer else by_reducer
            )
            calls = accepted = 0
            current_quality: dict[str, object] = {}
            for reducer_id, info in selected_items.items():
                if not isinstance(info, dict):
                    continue
                calls += int(info.get("predicate_calls", 0) or 0)
                accepted += int(info.get("accepted_moves", 0) or 0)
                qualities = info.get("final_quality")
                if isinstance(qualities, list) and qualities:
                    current_quality[str(reducer_id)] = qualities[-1]
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

    def summary(self, run_id: str, query: dict[str, list[str]] | None = None) -> dict[str, object]:
        run_dir, plan = self._load_run(run_id)
        progress = self._progress(run_dir)
        state = str(progress.get("status", "prepared"))
        live = self._run_live(run_id, run_dir)
        if state in {"running", "starting", "stopping", "aborting", "paused", "resuming"} and not live:
            stale_status = state
            state = "interrupted"
        else:
            stale_status = None
        results = self._completed_results(run_dir, plan)
        repeat = ((query or {}).get("repeat") or [""])[0]
        by_reducer: dict[str, dict[str, object]] = {}
        for reducer in plan["reducers"]:
            selected = [
                item for item in results
                if item["reducer_id"] == reducer["id"]
                and (not repeat or str(item.get("repeat")) == repeat)
            ]
            completed_items = [item for item in selected if str(item.get("status")) == "completed"]
            truncated_items = [item for item in selected if str(item.get("status")) == "truncated"]
            completed_times = [float(item.get("trial_wall_sec", 0) or 0) for item in completed_items if float(item.get("trial_wall_sec", 0) or 0) > 0]
            completed_sizes = _byte_counts(completed_items)
            truncated_sizes = _byte_counts(truncated_items)
            reported_sizes = completed_sizes + truncated_sizes
            by_reducer[str(reducer["id"])] = {
                "id": reducer["id"], "label": reducer["label"],
                "completed_avg_sec": (sum(completed_times) / len(completed_times)) if completed_times else None,
                "avg_bytes": (sum(reported_sizes) / len(reported_sizes)) if reported_sizes else None,
                "completed_avg_bytes": (sum(completed_sizes) / len(completed_sizes)) if completed_sizes else None,
                "truncated_avg_bytes": (sum(truncated_sizes) / len(truncated_sizes)) if truncated_sizes else None,
                "predicate_calls": sum(int(item.get("predicate_calls", 0) or 0) for item in selected),
                "accepted_moves": sum(int(item.get("accepted_moves", 0) or 0) for item in selected),
                "statuses": dict(Counter(str(item.get("status")) for item in selected)),
            }
        compare_plan = dict(plan)
        compare_results = results
        if repeat:
            compare_results = [
                item for item in results if str(item.get("repeat")) == repeat
            ]
            compare_plan["repeats"] = 1
        labels = {str(item["id"]): item["label"] for item in plan["reducers"]}
        comparisons = [
            {
                **row,
                "left_label": labels.get(str(row["left"]), row["left"]),
                "right_label": labels.get(str(row["right"]), row["right"]),
            }
            for row in reduction.comparison_rows(compare_plan, compare_results)
        ]
        sealed_calls = sum(int(item.get("predicate_calls", 0) or 0) for item in results)
        sealed_accepted = sum(int(item.get("accepted_moves", 0) or 0) for item in results)
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
            "run_id": run_id, "study_id": plan["study_id"], "created_at": plan.get("created_at"),
            "updated_at": progress.get("updated_at"),
            "elapsed_sec": _elapsed_seconds(
                plan.get("created_at"), progress.get("updated_at"), live=live
            ),
            "status": state,
            "stale_status": stale_status, "live": live,
            "total_trials": len(plan["jobs"]), "completed_trials": len(results),
            "active_trials": len(active),
            "pending_trials": max(0, len(plan["jobs"]) - len(results) - len(active)),
            "outer_jobs": plan["execution"]["outer_jobs"],
            "schedule": plan["execution"]["schedule"],
            "wave_count": max((int(job["wave"]) for job in plan["jobs"]), default=0),
            "repeat_count": plan["repeats"],
            "selection": plan.get("selection", {}),
            "catalog": plan.get("catalog", {}),
            "limits": plan["limits"],
            "target_branch": self.config.target_branch,
            "smtbatch_branch": self.current_smtbatch_branch,
            "source_sha256": plan["source"]["sha256"],
            "plan_sha256": plan.get("plan_sha256"),
            "by_reducer": by_reducer,
            "comparisons": comparisons,
            "repeats": sorted({int(item.get("repeat", 0)) for item in results}),
            "realtime_calls": realtime_calls,
            "realtime_accepted": realtime_accepted,
            "calls": realtime_calls + sealed_calls,
            "accepted": realtime_accepted + sealed_accepted,
            "current_quality": current_quality,
        }

    def runs(self) -> list[dict[str, object]]:
        self._refresh_config()
        self.results_root.mkdir(parents=True, exist_ok=True)
        records = []
        for path in sorted(self.results_root.iterdir()):
            if (
                not path.is_dir() or path.is_symlink()
                or path.name.startswith(".")
                or not (path / "plan.json").is_file()
            ):
                continue
            run_id = path.name
            try:
                record = self.summary(run_id)
            except (OSError, reduction.ReductionError, ValueError) as exc:
                record = {"run_id": run_id, "status": "invalid", "error": str(exc)}
            records.append(record)
        return records


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
                if path == "/api/catalog":
                    _json_response(
                        self,
                        {
                            "config": manager.configuration(),
                            "catalog": manager.benchmark_catalog(),
                        },
                    )
                    return
                if path == "/api/runs":
                    _json_response(self, {"runs": manager.runs()})
                    return
                match = re.fullmatch(r"/api/runs/([^/]+)/summary", path)
                if match:
                    _json_response(self, manager.summary(unquote(match.group(1)), parse_qs(parsed.query)))
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
                match = re.fullmatch(r"/api/runs/([^/]+)/pause", path)
                if match:
                    try:
                        _json_response(self, manager.pause(unquote(match.group(1))))
                    except ValueError as exc:
                        self._error(exc, HTTPStatus.CONFLICT)
                    return
                match = re.fullmatch(r"/api/runs/([^/]+)/resume", path)
                if match:
                    try:
                        _json_response(
                            self, manager.resume(unquote(match.group(1)), body),
                            HTTPStatus.ACCEPTED,
                        )
                    except ValueError as exc:
                        self._error(exc, HTTPStatus.CONFLICT)
                    return
                match = re.fullmatch(r"/api/runs/([^/]+)/delete", path)
                if match:
                    try:
                        _json_response(self, manager.delete_run(unquote(match.group(1))))
                    except ValueError as exc:
                        self._error(exc, HTTPStatus.CONFLICT)
                    except OSError as exc:
                        self._error(exc, HTTPStatus.INTERNAL_SERVER_ERROR)
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


def _rmtree_force(path: Path) -> None:
    """Best-effort recursive delete; chmod and retry when a file is not writable."""

    def onerror(func: object, err_path: str, _exc_info: object) -> None:
        try:
            os.chmod(err_path, 0o700)
            func(err_path)  # type: ignore[operator]
        except OSError:
            return

    try:
        shutil.rmtree(path, onerror=onerror)
    except FileNotFoundError:
        return
    except OSError:
        shutil.rmtree(path, ignore_errors=True)


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
