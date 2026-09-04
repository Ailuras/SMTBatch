"""Reduction-only project configuration loaded from the nearest smtbatch.toml."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


CONFIG_NAME = "smtbatch.toml"
_PLACEHOLDER = re.compile(r"\{(\w+)\}")
_REDUCER_PLACEHOLDERS = {
    "input", "output", "workdir", "predicate_timeout",
    "predicate_envelope_timeout", "predicate",
}
_CATEGORY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


@dataclass(frozen=True)
class ReducerSpec:
    """One black-box reducer command exposed to the batch runner."""

    name: str
    label: str
    command: tuple[str, ...]
    env: dict[str, str]
    executable: Path
    provenance_paths: tuple[Path, ...]
    require_clean: bool

    @property
    def option(self) -> dict[str, str]:
        return {
            "id": self.name,
            "name": self.name,
            "label": self.label,
        }


@dataclass(frozen=True)
class BenchmarkCategorySpec:
    """A project-owned, human-readable grouping of benchmark database rows."""

    name: str
    label: str
    description: str
    min_bytes: int | None
    max_bytes: int | None
    any_features: tuple[str, ...]
    required_features: tuple[str, ...]
    forbidden_features: tuple[str, ...]
    fallback: bool

    def matches(self, size_bytes: int, features: set[str]) -> bool:
        if self.fallback:
            return True
        return (
            (self.min_bytes is None or size_bytes >= self.min_bytes)
            and (self.max_bytes is None or size_bytes <= self.max_bytes)
            and (not self.any_features or bool(set(self.any_features) & features))
            and set(self.required_features).issubset(features)
            and not (set(self.forbidden_features) & features)
        )

    @property
    def option(self) -> dict[str, object]:
        return {
            "id": self.name,
            "label": self.label,
            "description": self.description,
            "min_bytes": self.min_bytes,
            "max_bytes": self.max_bytes,
            "any_features": list(self.any_features),
            "required_features": list(self.required_features),
            "forbidden_features": list(self.forbidden_features),
        }


@dataclass(frozen=True)
class Config:
    path: Path
    reducers: dict[str, ReducerSpec]
    results_root: Path
    smtbatch_root: Path
    benchmark_database: Path
    benchmark_inputs_root: Path
    benchmark_oracle: Path
    benchmark_template: Path | None
    benchmark_identity_command: tuple[str, ...]
    benchmark_categories: dict[str, BenchmarkCategorySpec]
    comparisons: tuple[tuple[str, str], ...]
    predicate_timeout_sec: float = 25.0
    port: int = 8001
    target_branch: str = "main"

    @property
    def reducer_options(self) -> tuple[dict[str, str], ...]:
        return tuple(spec.option for spec in self.reducers.values())


def find_config_path(start: Path | None = None) -> Path:
    current = (start or Path.cwd()).expanduser().resolve()
    if current.is_file():
        current = current.parent
    for directory in (current, *current.parents):
        candidate = directory / CONFIG_NAME
        if candidate.is_file():
            return candidate
    raise RuntimeError(f"no {CONFIG_NAME} found in {current} or its parent directories")


def load_config(start: Path | None = None) -> Config:
    path = find_config_path(start)
    try:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    except tomllib.TOMLDecodeError as exc:
        raise RuntimeError(f"invalid TOML in {path}: {exc}") from None

    extras = sorted(set(data) - {"defaults", "reducers", "benchmark_catalog", "benchmark_categories"})
    if extras:
        raise RuntimeError(f"unknown top-level config tables in {path}: {', '.join(extras)}")
    defaults = data.get("defaults", {})
    if not isinstance(defaults, dict):
        raise RuntimeError(f"[defaults] must be a table in {path}")
    default_extras = sorted(
        set(defaults)
        - {"results", "port", "target_branch", "comparisons", "predicate_timeout_sec"}
    )
    if default_extras:
        raise RuntimeError(f"unknown [defaults] fields in {path}: {', '.join(default_extras)}")

    reducers_raw = data.get("reducers")
    if not isinstance(reducers_raw, dict) or not reducers_raw:
        raise RuntimeError(f"config has no [reducers] entries: {path}")
    reducers = {
        name: _parse_reducer(name, value, path)
        for name, value in reducers_raw.items()
    }

    catalog_raw = data.get("benchmark_catalog", {})
    if not isinstance(catalog_raw, dict):
        raise RuntimeError(f"[benchmark_catalog] must be a table in {path}")
    catalog_extras = sorted(
        set(catalog_raw)
        - {"database", "inputs", "oracle", "template", "identity_command"}
    )
    if catalog_extras:
        raise RuntimeError(
            f"unknown [benchmark_catalog] fields in {path}: {', '.join(catalog_extras)}"
        )
    database_path = _resolve_path(
        catalog_raw.get("database", "benchmarks/database.json"), path
    )
    inputs_path = _resolve_path(
        catalog_raw.get("inputs", "benchmarks/inputs"), path
    )
    oracle_path = _resolve_path(
        catalog_raw.get("oracle", "benchmarks/oracle.py"), path
    )
    template_value = catalog_raw.get("template")
    template_path = _resolve_path(template_value, path) if template_value is not None else None
    identity_command = _command_tuple(
        catalog_raw.get("identity_command"), "[benchmark_catalog] identity_command", path
    )
    categories_raw = data.get("benchmark_categories", {})
    if not isinstance(categories_raw, dict):
        raise RuntimeError(f"[benchmark_categories] must be a table in {path}")
    categories = {
        name: _parse_category(name, value, path)
        for name, value in categories_raw.items()
    }
    if sum(spec.fallback for spec in categories.values()) > 1:
        raise RuntimeError(f"[benchmark_categories] may contain at most one fallback category in {path}")

    comparisons = _parse_comparisons(defaults.get("comparisons", []), reducers, path)

    target_branch = defaults.get("target_branch", "main")
    if (
        not isinstance(target_branch, str)
        or not target_branch.strip()
        or target_branch != target_branch.strip()
        or any(char.isspace() for char in target_branch)
    ):
        raise RuntimeError(f"[defaults] target_branch must be a Git branch name in {path}")
    return Config(
        path=path.resolve(),
        reducers=reducers,
        results_root=_resolve_root(defaults.get("results", "results"), path),
        smtbatch_root=(path.parent / "SMTBatch").resolve(),
        benchmark_database=database_path,
        benchmark_inputs_root=inputs_path,
        benchmark_oracle=oracle_path,
        benchmark_template=template_path,
        benchmark_identity_command=identity_command,
        benchmark_categories=categories,
        comparisons=comparisons,
        predicate_timeout_sec=_parse_positive_number(
            defaults.get("predicate_timeout_sec", 25),
            "[defaults] predicate_timeout_sec",
            path,
        ),
        port=_parse_port(defaults.get("port", 8001), path),
        target_branch=target_branch,
    )


def current_git_branch(root: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "symbolic-ref", "--quiet", "--short", "HEAD"],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        )
    except OSError as exc:
        raise RuntimeError(f"unable to inspect Git branch in {root}: {exc}") from exc
    branch = result.stdout.strip()
    if result.returncode != 0 or not branch:
        raise RuntimeError(f"repository is not on a named Git branch: {root}")
    return branch


def branch_status(config: Config) -> tuple[str, bool, str]:
    try:
        branch = current_git_branch(config.smtbatch_root)
    except RuntimeError as exc:
        return "", False, str(exc)
    if branch != config.target_branch:
        return branch, False, (
            f"wrong SMTBatch branch: expected {config.target_branch!r}, found {branch!r} "
            f"in {config.smtbatch_root}"
        )
    return branch, True, ""


def validate_target_branch(config: Config) -> str:
    branch, valid, error = branch_status(config)
    if not valid:
        raise RuntimeError(error)
    return branch


def _parse_positive_number(value: object, label: str, path: Path) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise RuntimeError(f"{label} must be a positive number in {path}")
    return float(value)


def _parse_port(value: object, path: Path) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65535:
        raise RuntimeError(f"[defaults] port must be an integer between 1 and 65535 in {path}")
    return value


def _resolve_root(value: object, config_path: Path) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"project paths must be non-empty strings in {config_path}")
    root = Path(value).expanduser()
    if not root.is_absolute():
        root = config_path.parent / root
    return root.resolve()


def _resolve_path(value: object, config_path: Path) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"project paths must be non-empty strings in {config_path}")
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = config_path.parent / path
    return path.resolve()


def _string_tuple(value: object, label: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or not all(isinstance(item, str) and item.strip() for item in value):
        raise RuntimeError(f"{label} must be a list of non-empty strings")
    return tuple(item.strip() for item in value)


def _command_tuple(value: object, label: str, path: Path) -> tuple[str, ...]:
    if value is None:
        return ()
    if (
        not isinstance(value, list)
        or not value
        or not all(isinstance(item, str) and item for item in value)
    ):
        raise RuntimeError(f"{label} must be a non-empty list of strings in {path}")
    executable_token = value[0]
    executable = Path(executable_token).expanduser()
    if not executable.is_absolute() and "/" in executable_token:
        executable = path.parent / executable
    resolved = executable.resolve() if executable.exists() else None
    if resolved is None:
        located = shutil.which(executable_token)
        resolved = Path(located).resolve() if located else None
    if resolved is None or not resolved.is_file() or not os.access(resolved, os.X_OK):
        raise RuntimeError(f"{label} executable missing or not executable: {executable_token}")
    return tuple(value)


def _parse_comparisons(
    value: object, reducers: dict[str, ReducerSpec], path: Path
) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, list):
        raise RuntimeError(f"[defaults] comparisons must be a list in {path}")
    comparisons: list[tuple[str, str]] = []
    for index, pair in enumerate(value):
        if (
            not isinstance(pair, list)
            or len(pair) != 2
            or not all(isinstance(item, str) for item in pair)
            or pair[0] == pair[1]
            or any(item not in reducers for item in pair)
        ):
            raise RuntimeError(
                f"[defaults] comparisons[{index}] must reference two different configured reducers in {path}"
            )
        normalized = (pair[0], pair[1])
        if normalized not in comparisons:
            comparisons.append(normalized)
    return tuple(comparisons)


def _parse_category(name: str, value: object, path: Path) -> BenchmarkCategorySpec:
    if not isinstance(name, str) or not _CATEGORY_ID.fullmatch(name):
        raise RuntimeError(f"benchmark category names must match {_CATEGORY_ID.pattern} in {path}")
    if not isinstance(value, dict):
        raise RuntimeError(f"[benchmark_categories.{name}] must be a table in {path}")
    allowed = {
        "label", "description", "min_bytes", "max_bytes", "any_features",
        "required_features", "forbidden_features", "fallback",
    }
    extras = sorted(set(value) - allowed)
    if extras:
        raise RuntimeError(
            f"[benchmark_categories.{name}] has unknown fields: {', '.join(extras)}"
        )
    label = value.get("label", name)
    description = value.get("description", "")
    if not isinstance(label, str) or not label.strip():
        raise RuntimeError(f"[benchmark_categories.{name}] label must be a non-empty string")
    if not isinstance(description, str):
        raise RuntimeError(f"[benchmark_categories.{name}] description must be a string")
    min_bytes = value.get("min_bytes")
    max_bytes = value.get("max_bytes")
    for field, item in (("min_bytes", min_bytes), ("max_bytes", max_bytes)):
        if item is not None and (isinstance(item, bool) or not isinstance(item, int) or item < 0):
            raise RuntimeError(f"[benchmark_categories.{name}] {field} must be a non-negative integer")
    if min_bytes is not None and max_bytes is not None and min_bytes > max_bytes:
        raise RuntimeError(f"[benchmark_categories.{name}] min_bytes must not exceed max_bytes")
    any_features = _string_tuple(
        value.get("any_features"), f"[benchmark_categories.{name}] any_features"
    )
    required_features = _string_tuple(
        value.get("required_features"), f"[benchmark_categories.{name}] required_features"
    )
    forbidden_features = _string_tuple(
        value.get("forbidden_features"), f"[benchmark_categories.{name}] forbidden_features"
    )
    fallback = value.get("fallback", False)
    if not isinstance(fallback, bool):
        raise RuntimeError(f"[benchmark_categories.{name}] fallback must be boolean")
    if not fallback and min_bytes is None and max_bytes is None and not (any_features or required_features or forbidden_features):
        raise RuntimeError(
            f"[benchmark_categories.{name}] needs a matcher or fallback=true"
        )
    return BenchmarkCategorySpec(
        name=name,
        label=label.strip(),
        description=description.strip(),
        min_bytes=min_bytes,
        max_bytes=max_bytes,
        any_features=any_features,
        required_features=required_features,
        forbidden_features=forbidden_features,
        fallback=fallback,
    )


def _parse_reducer(name: str, value: object, path: Path) -> ReducerSpec:
    if not name or name != name.strip():
        raise RuntimeError(f"reducer names must be non-empty and trimmed in {path}")
    if not isinstance(value, dict):
        raise RuntimeError(f"[reducers.{name}] must be a table in {path}")
    allowed = {"label", "command", "env", "provenance_paths", "require_clean"}
    extras = sorted(set(value) - allowed)
    if extras:
        raise RuntimeError(f"[reducers.{name}] has unknown fields: {', '.join(extras)}")

    label = value.get("label", name)
    command = value.get("command")
    if not isinstance(label, str) or not label.strip():
        raise RuntimeError(f"[reducers.{name}] label must be a non-empty string")
    if not isinstance(command, list) or not command or not all(isinstance(token, str) for token in command):
        raise RuntimeError(f"[reducers.{name}] command must be a non-empty list of strings")
    for token in command:
        for match in _PLACEHOLDER.finditer(token):
            if match.group(1) not in _REDUCER_PLACEHOLDERS:
                raise RuntimeError(
                    f"[reducers.{name}] unknown placeholder {{{match.group(1)}}} in command"
                )
    for required in ("{input}", "{output}"):
        if sum(required in token for token in command) != 1:
            raise RuntimeError(f"[reducers.{name}] command must contain {required} exactly once")
    if command.count("{predicate}") != 1:
        raise RuntimeError(f"[reducers.{name}] command must contain {{predicate}} as one argv token exactly once")

    executable_token = command[0]
    executable = Path(executable_token).expanduser()
    if not executable.is_absolute() and "/" in executable_token:
        executable = path.parent / executable
    resolved = executable.resolve() if executable.exists() else None
    if resolved is None:
        located = shutil.which(executable_token)
        resolved = Path(located).resolve() if located else None
    if resolved is None or not resolved.is_file() or not os.access(resolved, os.X_OK):
        raise RuntimeError(f"[reducers.{name}] executable missing or not executable: {executable_token}")

    env = value.get("env", {})
    if not isinstance(env, dict) or not all(
        isinstance(key, str) and isinstance(item, str) for key, item in env.items()
    ):
        raise RuntimeError(f"[reducers.{name}] env must be a table of strings")
    provenance_paths_raw = _string_tuple(
        value.get("provenance_paths"), f"[reducers.{name}] provenance_paths"
    )
    provenance_paths = []
    for item in provenance_paths_raw:
        candidate = Path(item).expanduser()
        if not candidate.is_absolute():
            candidate = path.parent / candidate
        if candidate.is_symlink():
            raise RuntimeError(
                f"[reducers.{name}] provenance path must not be a symbolic link: {candidate}"
            )
        resolved_path = candidate.resolve()
        if not resolved_path.exists():
            raise RuntimeError(
                f"[reducers.{name}] provenance path is missing: {resolved_path}"
            )
        provenance_paths.append(resolved_path)
    require_clean = value.get("require_clean", False)
    if not isinstance(require_clean, bool):
        raise RuntimeError(f"[reducers.{name}] require_clean must be boolean")
    return ReducerSpec(
        name=name,
        label=label.strip(),
        command=tuple(command),
        env=dict(env),
        executable=resolved,
        provenance_paths=tuple(provenance_paths),
        require_clean=require_clean,
    )
