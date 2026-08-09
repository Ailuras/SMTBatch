"""Deterministic source, executable, Git, and command identity snapshots."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
from typing import Mapping, Sequence


IGNORED_PARTS = {
    ".git", ".mypy_cache", ".pytest_cache", ".ruff_cache", ".tox",
    ".venv", "__pycache__", "build", "dist",
}
IGNORED_SUFFIXES = {".pyc", ".pyo"}


class ProvenanceError(RuntimeError):
    """Report an asset that cannot be frozen or no longer matches its snapshot."""


def _json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value, sort_keys=True, ensure_ascii=True, allow_nan=False,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_executable(token: str, cwd: Path) -> Path:
    candidate = Path(token).expanduser()
    if not candidate.is_absolute() and "/" in token:
        candidate = cwd / candidate
    resolved = candidate.resolve() if candidate.exists() else None
    if resolved is None:
        located = shutil.which(token)
        resolved = Path(located).resolve() if located else None
    if resolved is None or not resolved.is_file() or not os.access(resolved, os.X_OK):
        raise ProvenanceError(f"executable missing or not executable: {token}")
    return resolved


def _ignored(path: Path, root: Path) -> bool:
    relative = path.relative_to(root)
    return bool(set(relative.parts) & IGNORED_PARTS) or path.suffix in IGNORED_SUFFIXES


def snapshot_path(value: str | Path) -> dict[str, object]:
    """Hash one file or a recursively enumerated directory without following links."""

    candidate = Path(value).expanduser()
    if candidate.is_symlink():
        raise ProvenanceError(f"provenance path must not be a symbolic link: {candidate}")
    path = candidate.resolve()
    if not path.exists():
        raise ProvenanceError(f"provenance path is missing: {path}")
    if not path.is_file() and not path.is_dir():
        raise ProvenanceError(f"provenance path must be a file or directory: {path}")

    if path.is_file():
        candidates = [path]
        kind = "file"
        base = path.parent
    else:
        candidates = []
        kind = "directory"
        base = path
        for current, directories, filenames in os.walk(path, followlinks=False):
            current_path = Path(current)
            kept_directories = []
            for name in sorted(directories):
                child = current_path / name
                if _ignored(child, path):
                    continue
                if child.is_symlink():
                    raise ProvenanceError(
                        f"provenance tree must not contain symbolic links: {child}"
                    )
                kept_directories.append(name)
            directories[:] = kept_directories
            for name in sorted(filenames):
                child = current_path / name
                if _ignored(child, path):
                    continue
                if child.is_symlink():
                    raise ProvenanceError(
                        f"provenance tree must not contain symbolic links: {child}"
                    )
                if not child.is_file():
                    raise ProvenanceError(
                        f"provenance tree contains a non-regular file: {child}"
                    )
                candidates.append(child)

    files = []
    for item in sorted(candidates, key=lambda current: current.relative_to(base).as_posix()):
        files.append({
            "path": item.relative_to(base).as_posix(),
            "bytes": item.stat().st_size,
            "mode": stat.S_IMODE(item.stat().st_mode),
            "sha256": _sha256_path(item),
        })
    return {
        "path": str(path),
        "kind": kind,
        "file_count": len(files),
        "files": files,
        "tree_sha256": _sha256_bytes(_json_bytes(files)),
    }


def _git(arguments: Sequence[str], *, cwd: Path, check: bool = True) -> subprocess.CompletedProcess[bytes]:
    try:
        result = subprocess.run(
            ["git", *arguments], cwd=cwd, check=False,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    except OSError as exc:
        raise ProvenanceError(f"unable to inspect Git identity in {cwd}: {exc}") from exc
    if check and result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise ProvenanceError(f"Git identity command failed in {cwd}: {detail}")
    return result


def _git_root(path: Path) -> Path | None:
    cwd = path if path.is_dir() else path.parent
    result = _git(["rev-parse", "--show-toplevel"], cwd=cwd, check=False)
    if result.returncode != 0:
        return None
    value = result.stdout.decode("utf-8", errors="strict").strip()
    return Path(value).resolve() if value else None


def _git_snapshots(paths: Sequence[Path], *, require_clean: bool) -> list[dict[str, object]]:
    grouped: dict[Path, set[str]] = {}
    for path in paths:
        root = _git_root(path)
        if root is None:
            continue
        try:
            relative = path.resolve().relative_to(root).as_posix()
        except ValueError as exc:
            raise ProvenanceError(f"Git path escapes repository {root}: {path}") from exc
        grouped.setdefault(root, set()).add(relative)

    records = []
    for root in sorted(grouped, key=str):
        pathspecs = sorted(grouped[root])
        head = _git(["rev-parse", "HEAD"], cwd=root).stdout.decode("ascii").strip()
        branch_result = _git(
            ["symbolic-ref", "--quiet", "--short", "HEAD"], cwd=root, check=False
        )
        branch = (
            branch_result.stdout.decode("utf-8", errors="strict").strip()
            if branch_result.returncode == 0 else ""
        )
        status_result = _git(
            ["status", "--porcelain=v1", "--untracked-files=all", "--", *pathspecs],
            cwd=root,
        )
        diff_result = _git(["diff", "HEAD", "--binary", "--", *pathspecs], cwd=root)
        status_bytes = status_result.stdout
        diff_bytes = diff_result.stdout
        status_lines = status_bytes.decode("utf-8", errors="replace").splitlines()
        if require_clean and status_lines:
            raise ProvenanceError(
                f"provenance paths must be clean in {root}: {'; '.join(status_lines[:8])}"
            )
        records.append({
            "root": str(root),
            "head": head,
            "branch": branch,
            "pathspecs": pathspecs,
            "dirty": bool(status_lines),
            "status": status_lines,
            "status_sha256": _sha256_bytes(status_bytes),
            "diff_sha256": _sha256_bytes(diff_bytes),
        })
    return records


def snapshot_reducer(
    executable: str | Path, provenance_paths: Sequence[str | Path], *, require_clean: bool
) -> dict[str, object]:
    executable_path = Path(executable).expanduser().resolve()
    paths = [executable_path, *(Path(item).expanduser().resolve() for item in provenance_paths)]
    unique_paths = list(dict.fromkeys(paths))
    assets = [snapshot_path(path) for path in unique_paths]
    return {
        "schema_version": 1,
        "require_clean": require_clean,
        "executable": str(executable_path),
        "declared_paths": [str(path) for path in unique_paths[1:]],
        "assets": assets,
        "repositories": _git_snapshots(unique_paths, require_clean=require_clean),
    }


def resnapshot_reducer(frozen: Mapping[str, object]) -> dict[str, object]:
    executable = frozen.get("executable")
    declared = frozen.get("declared_paths")
    require_clean = frozen.get("require_clean")
    if (
        not isinstance(executable, str)
        or not isinstance(declared, list)
        or not all(isinstance(item, str) for item in declared)
        or not isinstance(require_clean, bool)
    ):
        raise ProvenanceError("malformed frozen reducer provenance")
    return snapshot_reducer(executable, declared, require_clean=require_clean)


def _command_asset_paths(command: Sequence[str], cwd: Path) -> list[Path]:
    executable = _resolve_executable(command[0], cwd)
    paths = [executable]
    for token in command[1:]:
        if "{" in token or "}" in token:
            continue
        candidate = Path(token).expanduser()
        if not candidate.is_absolute():
            candidate = cwd / candidate
        if candidate.exists() and (candidate.is_file() or candidate.is_dir()):
            paths.append(candidate.resolve())
    return list(dict.fromkeys(paths))


def snapshot_command(
    command: Sequence[str], *, cwd: str | Path, execute: bool = False,
    timeout_seconds: float = 60,
) -> dict[str, object]:
    if not command or not all(isinstance(item, str) and item for item in command):
        raise ProvenanceError("identity command must be a non-empty list of strings")
    root = Path(cwd).expanduser().resolve()
    if not root.is_dir():
        raise ProvenanceError(f"identity command cwd is not a directory: {root}")
    paths = _command_asset_paths(command, root)
    record: dict[str, object] = {
        "schema_version": 1,
        "command": list(command),
        "cwd": str(root),
        "execute": execute,
        "timeout_seconds": float(timeout_seconds),
        "assets": [snapshot_path(path) for path in paths],
        "repositories": _git_snapshots(paths, require_clean=False),
    }
    if execute:
        try:
            result = subprocess.run(
                list(command), cwd=root, check=False, timeout=timeout_seconds,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ProvenanceError(f"identity command failed: {exc}") from exc
        if result.returncode != 0:
            detail = result.stderr.decode("utf-8", errors="replace").strip()
            raise ProvenanceError(
                f"identity command returned {result.returncode}: {detail or 'no stderr'}"
            )
        record["returncode"] = result.returncode
        record["stdout"] = result.stdout.decode("utf-8", errors="replace")
        record["stdout_sha256"] = _sha256_bytes(result.stdout)
        record["stderr_sha256"] = _sha256_bytes(result.stderr)
    record["snapshot_sha256"] = _sha256_bytes(_json_bytes(record))
    return record


def resnapshot_command(frozen: Mapping[str, object]) -> dict[str, object]:
    command = frozen.get("command")
    cwd = frozen.get("cwd")
    execute = frozen.get("execute")
    timeout = frozen.get("timeout_seconds")
    if (
        not isinstance(command, list)
        or not all(isinstance(item, str) for item in command)
        or not isinstance(cwd, str)
        or not isinstance(execute, bool)
        or isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
    ):
        raise ProvenanceError("malformed frozen command provenance")
    return snapshot_command(command, cwd=cwd, execute=execute, timeout_seconds=float(timeout))


def same_snapshot(frozen: Mapping[str, object], current: Mapping[str, object]) -> bool:
    return _json_bytes(dict(frozen)) == _json_bytes(dict(current))
