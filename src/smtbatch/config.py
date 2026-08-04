"""Solver and project configuration loaded from the nearest smtbatch.toml.

The config file is discovered by walking upward from the current directory,
the same way git finds its repository root: the closest ``smtbatch.toml``
wins. Each project keeps its own file, so no environment variable is needed.

Config schema:

    [defaults]
    inputs = "benchmarks"     # default benchmark root, relative to the config file
    results = "results"       # default results root, relative to the config file

    [solvers.z3]
    binary = "/opt/z3/bin/z3"
    command = ["gtimeout", "--kill-after=1", "{timeout}", "{binary}", "model=true", "{input}"]
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


def find_config_path(start: Path | None = None) -> Path:
    """Return the closest smtbatch.toml at or above ``start`` (default: cwd)."""
    current = (start or Path.cwd()).expanduser().resolve()
    for directory in (current, *current.parents):
        candidate = directory / CONFIG_NAME
        if candidate.is_file():
            return candidate
    raise RuntimeError(f"no {CONFIG_NAME} found in {current} or its parent directories")


def load_config() -> Config:
    path = find_config_path()
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
    inputs_root = _resolve_root(defaults.get("inputs", "benchmarks"), path)
    results_root = _resolve_root(defaults.get("results", "results"), path)
    return Config(path.resolve(), solvers, inputs_root, results_root)


def _resolve_root(value: object, config_path: Path) -> Path:
    root = Path(str(value)).expanduser()
    if not root.is_absolute():
        root = config_path.parent / root
    return root.resolve()


def _parse_solver(name: str, spec: object, path: Path) -> SolverSpec:
    if not isinstance(spec, dict):
        raise RuntimeError(f"[solvers.{name}] must be a table in {path}")
    binary = Path(str(spec.get("binary", ""))).expanduser()
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
    return SolverSpec(name, binary.resolve(), tuple(command), tuple(version_args))
