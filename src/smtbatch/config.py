"""Solver and project configuration loaded from the nearest smtbatch.toml.

The config file is discovered by walking upward from the current directory,
the same way git finds its repository root: the closest ``smtbatch.toml``
wins. Each project keeps its own file, so no environment variable is needed.

Config schema:

    [defaults]
    inputs = "benchmarks"     # default benchmark root, relative to the config file
    results = "results"       # default results root, relative to the config file
    port = 8000               # optional dashboard port
    target_branch = "feat/incremental"  # SMTBatch checkout at <project>/SMTBatch


    [solvers.my-solver]
    label = "My solver"             # optional label shown in the dashboard
    binary = "/opt/my-solver/bin/my-solver"
    command = ["gtimeout", "--kill-after=1", "{timeout}", "{binary}", "{input}"]
    version_args = ["--version"]          # optional, default ["--version"]

Placeholders allowed inside ``command`` tokens:

    {binary}      absolute solver executable from the ``binary`` key
    {input}       the .smt2 file for the current job (required exactly once)
    {timeout}     per-job timeout in seconds
    {timeout_ms}  per-job timeout in whole milliseconds
"""

from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

CONFIG_NAME = "smtbatch.toml"
_PLACEHOLDER = re.compile(r"\{(\w+)\}")
_ALLOWED_PLACEHOLDERS = {"binary", "input", "timeout", "timeout_ms"}


@dataclass(frozen=True)
class SolverSpec:
    """One solver entry: identity, executable, and an argv template."""

    name: str
    binary: Path
    command: tuple[str, ...]
    version_args: tuple[str, ...]
    label: str = ""

    def render(self, input_path: Path, timeout: float) -> list[str]:
        """Substitute placeholders for one concrete job invocation."""
        timeout_s = str(int(timeout)) if float(timeout).is_integer() else repr(timeout)
        values = {
            "binary": str(self.binary),
            "input": str(input_path),
            "timeout": timeout_s,
            "timeout_ms": str(int(round(timeout * 1000))),
        }
        return [_PLACEHOLDER.sub(lambda match: values[match.group(1)], token) for token in self.command]


@dataclass(frozen=True)
class Config:
    path: Path
    solvers: dict[str, SolverSpec]
    inputs_root: Path
    results_root: Path
    smtbatch_root: Path
    port: int = 8000
    target_branch: str = "main"

    @property
    def solver_options(self) -> tuple[dict[str, str], ...]:
        """Stable, UI-safe solver metadata derived from the TOML entries."""
        return tuple({"name": spec.name, "label": spec.label or spec.name} for spec in self.solvers.values())


def find_config_path(start: Path | None = None) -> Path:
    """Return the closest smtbatch.toml at or above ``start`` (default: cwd)."""
    current = (start or Path.cwd()).expanduser().resolve()
    for directory in (current, *current.parents):
        candidate = directory / CONFIG_NAME
        if candidate.is_file():
            return candidate
    raise RuntimeError(f"no {CONFIG_NAME} found in {current} or its parent directories")


def load_config(start: Path | None = None) -> Config:
    """Load the nearest project configuration above ``start`` (or cwd)."""
    path = find_config_path(start)
    try:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    except tomllib.TOMLDecodeError as exc:
        raise RuntimeError(f"invalid TOML in {path}: {exc}") from None

    solvers_raw = data.get("solvers")
    if not isinstance(solvers_raw, dict) or not solvers_raw:
        raise RuntimeError(f"config has no [solvers] entries: {path}")
    solvers: dict[str, SolverSpec] = {}
    for name, spec in solvers_raw.items():
        solvers[name] = _parse_solver(name, spec, path)

    defaults = data.get("defaults", {})
    if not isinstance(defaults, dict):
        raise RuntimeError(f"[defaults] must be a table in {path}")
    target_branch = defaults.get("target_branch", "main")
    if (
        not isinstance(target_branch, str)
        or not target_branch.strip()
        or target_branch != target_branch.strip()
        or any(char.isspace() for char in target_branch)
    ):
        raise RuntimeError(f"[defaults] target_branch must be a Git branch name in {path}")
    inputs_root = _resolve_root(defaults.get("inputs", "benchmarks"), path)
    results_root = _resolve_root(defaults.get("results", "results"), path)
    return Config(
        path=path.resolve(),
        solvers=solvers,
        inputs_root=inputs_root,
        results_root=results_root,
        smtbatch_root=(path.parent / "SMTBatch").resolve(),
        port=_parse_port(defaults.get("port", 8000), path),
        target_branch=target_branch,
    )


def current_git_branch(root: Path) -> str:
    git_dir = root / ".git"
    try:
        if git_dir.is_file():
            payload = git_dir.read_text(encoding="utf-8").strip()
            marker = "gitdir:"
            if payload.lower().startswith(marker):
                git_dir = Path(payload[len(marker) :].strip())
                if not git_dir.is_absolute():
                    git_dir = (root / git_dir).resolve()
        text = (git_dir / "HEAD").read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise RuntimeError(f"unable to inspect Git branch in {root}: {exc}") from exc
    prefix = "ref: refs/heads/"
    if text.startswith(prefix):
        branch = text[len(prefix) :]
        if branch and not any(char.isspace() for char in branch):
            return branch
    raise RuntimeError(f"repository is not on a named Git branch: {root}")


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


def _parse_port(value: object, path: Path) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f"[defaults] port must be an integer in {path}")
    port = value
    if not 1 <= port <= 65535:
        raise RuntimeError(f"[defaults] port must be between 1 and 65535 in {path}")
    return port


def _resolve_root(value: object, config_path: Path) -> Path:
    root = Path(str(value)).expanduser()
    if not root.is_absolute():
        root = config_path.parent / root
    return root.resolve()


def _parse_solver(name: str, spec: object, path: Path) -> SolverSpec:
    if not name or name != name.strip():
        raise RuntimeError(f"solver names must be non-empty and trimmed in {path}")
    if not isinstance(spec, dict):
        raise RuntimeError(f"[solvers.{name}] must be a table in {path}")
    label = spec.get("label", name)
    if not isinstance(label, str) or not label.strip():
        raise RuntimeError(f"[solvers.{name}] label must be a non-empty string")
    binary = Path(str(spec.get("binary", ""))).expanduser()
    if not binary.is_absolute():
        binary = path.parent / binary
    binary = binary.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError(f"[solvers.{name}] binary missing or not executable: {binary}")
    command = spec.get("command")
    if not isinstance(command, list) or not command or not all(isinstance(token, str) for token in command):
        raise RuntimeError(f"[solvers.{name}] command must be a non-empty list of strings")
    if " ".join(command).count("{input}") != 1:
        raise RuntimeError(f"[solvers.{name}] command must contain {{input}} exactly once")
    for token in command:
        for match in _PLACEHOLDER.finditer(token):
            if match.group(1) not in _ALLOWED_PLACEHOLDERS:
                raise RuntimeError(f"[solvers.{name}] unknown placeholder {{{match.group(1)}}} in command")
    version_args = spec.get("version_args", ["--version"])
    if not isinstance(version_args, list) or not all(isinstance(token, str) for token in version_args):
        raise RuntimeError(f"[solvers.{name}] version_args must be a list of strings")
    return SolverSpec(name, binary, tuple(command), tuple(version_args), label.strip())
