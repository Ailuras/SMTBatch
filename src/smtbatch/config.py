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
    "input", "output", "workdir", "trace_dir", "stats", "observe",
    "observation_dir", "observation_stats", "predicate_timeout", "trial_timeout",
    "memory_mb", "repeat", "seed", "predicate",
}


@dataclass(frozen=True)
class ReducerSpec:
    """One concrete selectable reducer version/configuration."""

    name: str
    label: str
    command: tuple[str, ...]
    env: dict[str, str]
    executable: Path
    strategy: str
    version: str
    evidence_level: str
    acceptance: str
    stats: str

    @property
    def option(self) -> dict[str, str]:
        return {
            "id": self.name,
            "name": self.name,
            "label": self.label,
            "strategy": self.strategy,
            "version": self.version,
            "evidence_level": self.evidence_level,
        }


@dataclass(frozen=True)
class Config:
    path: Path
    reducers: dict[str, ReducerSpec]
    inputs_root: Path
    results_root: Path
    studies_root: Path
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

    extras = sorted(set(data) - {"defaults", "reducers"})
    if extras:
        raise RuntimeError(f"unknown top-level config tables in {path}: {', '.join(extras)}")
    defaults = data.get("defaults", {})
    if not isinstance(defaults, dict):
        raise RuntimeError(f"[defaults] must be a table in {path}")
    default_extras = sorted(
        set(defaults) - {"inputs", "results", "studies", "port", "target_branch"}
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
        inputs_root=_resolve_root(defaults.get("inputs", "benchmarks"), path),
        results_root=_resolve_root(defaults.get("results", "results"), path),
        studies_root=_resolve_root(defaults.get("studies", "scripts/experiments"), path),
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
        raise RuntimeError(f"project root is not on a named Git branch: {root}")
    return branch


def branch_status(config: Config) -> tuple[str, bool, str]:
    try:
        branch = current_git_branch(config.path.parent.resolve())
    except RuntimeError as exc:
        return "", False, str(exc)
    if branch != config.target_branch:
        return branch, False, (
            f"wrong project branch: expected {config.target_branch!r}, found {branch!r} "
            f"in {config.path.parent.resolve()}"
        )
    return branch, True, ""


def validate_target_branch(config: Config) -> str:
    branch, valid, error = branch_status(config)
    if not valid:
        raise RuntimeError(error)
    return branch


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


def _parse_reducer(name: str, value: object, path: Path) -> ReducerSpec:
    if not name or name != name.strip():
        raise RuntimeError(f"reducer names must be non-empty and trimmed in {path}")
    if not isinstance(value, dict):
        raise RuntimeError(f"[reducers.{name}] must be a table in {path}")
    allowed = {
        "label", "command", "env", "strategy", "version", "evidence_level",
        "acceptance", "stats",
    }
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
    strings: dict[str, str] = {}
    for field, default in (("strategy", ""), ("version", "")):
        item = value.get(field, default)
        if not isinstance(item, str):
            raise RuntimeError(f"[reducers.{name}] {field} must be a string")
        strings[field] = item
    evidence_level = value.get("evidence_level", "none")
    acceptance = value.get("acceptance", "serial-inferred")
    stats = value.get("stats", "optional")
    if evidence_level not in {"none", "summary", "full"}:
        raise RuntimeError(f"[reducers.{name}] evidence_level must be none, summary, or full")
    if acceptance not in {"serial-inferred", "trace"}:
        raise RuntimeError(f"[reducers.{name}] acceptance must be serial-inferred or trace")
    if stats not in {"none", "optional", "required"}:
        raise RuntimeError(f"[reducers.{name}] stats must be none, optional, or required")
    if acceptance == "trace" and evidence_level == "none":
        raise RuntimeError(f"[reducers.{name}] trace acceptance requires summary or full evidence")
    return ReducerSpec(
        name=name,
        label=label.strip(),
        command=tuple(command),
        env=dict(env),
        executable=resolved,
        strategy=strings["strategy"],
        version=strings["version"],
        evidence_level=str(evidence_level),
        acceptance=str(acceptance),
        stats=str(stats),
    )
