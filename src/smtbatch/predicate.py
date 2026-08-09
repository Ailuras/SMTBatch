"""Run one benchmark predicate and record external reduction evidence."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid


def _json_line(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
    ) + "\n"


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _tokens(text: str):
    """Yield lightweight SMT-LIB tokens for stable size accounting."""

    index = 0
    length = len(text)
    while index < length:
        char = text[index]
        if char.isspace():
            index += 1
            continue
        if char == ";":
            newline = text.find("\n", index + 1)
            index = length if newline < 0 else newline + 1
            continue
        if char in "()":
            yield char
            index += 1
            continue
        if char == '"':
            start = index
            index += 1
            while index < length:
                if text[index] == '"':
                    if index + 1 < length and text[index + 1] == '"':
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            else:
                raise ValueError("unterminated SMT-LIB string")
            yield text[start:index]
            continue
        if char == "|":
            start = index
            index += 1
            while index < length and text[index] != "|":
                index += 1
            if index >= length:
                raise ValueError("unterminated SMT-LIB quoted symbol")
            index += 1
            yield text[start:index]
            continue
        start = index
        while index < length and not text[index].isspace() and text[index] not in "();":
            index += 1
        yield text[start:index]


def _candidate_record(path: Path) -> dict[str, object]:
    record: dict[str, object] = {
        "path": str(path),
        "bytes": None,
        "sha256": None,
        "canonical_sha256": None,
        "quality": None,
        "quality_error": None,
    }
    try:
        raw = path.read_bytes()
        text = raw.decode("utf-8")
        tokens = list(_tokens(text))
        depth = 0
        expressions = 0
        nodes = 0
        for token in tokens:
            if token == "(":
                if depth == 0:
                    expressions += 1
                depth += 1
                nodes += 1
            elif token == ")":
                depth -= 1
                if depth < 0:
                    raise ValueError("unbalanced closing parenthesis")
            else:
                if depth == 0:
                    expressions += 1
                nodes += 1
        if depth != 0:
            raise ValueError("unbalanced opening parenthesis")
        canonical = "\0".join(tokens).encode("utf-8")
        record.update({
            "bytes": len(raw),
            "sha256": _sha256_bytes(raw),
            "canonical_sha256": _sha256_bytes(canonical),
            "quality": {
                "expression_count": expressions,
                "node_count": nodes,
                "byte_count": len(canonical),
            },
        })
    except Exception as error:  # evidence retains parse failures explicitly
        record["quality_error"] = f"{type(error).__name__}: {error}"
    return record


def _read_rows(handle) -> list[dict[str, object]]:
    handle.seek(0)
    rows = []
    for line in handle:
        if not line.strip():
            continue
        value = json.loads(line)
        if isinstance(value, dict):
            rows.append(value)
    return rows


def _append(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        handle.seek(0, os.SEEK_END)
        handle.write(_json_line(value))
        handle.flush()
        os.fsync(handle.fileno())
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _append_start(
    path: Path, *, call_id: str, phase: str, candidate: Path,
) -> dict[str, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        rows = _read_rows(handle)
        starts = [row for row in rows if row.get("event") == "start"]
        reducer_starts = [row for row in starts if row.get("phase") == "reducer"]
        role = phase
        if phase == "reducer":
            role = "golden" if not reducer_starts else "candidate"
        call_seq = max(
            (
                int(row["call_seq"])
                for row in starts
                if isinstance(row.get("call_seq"), int)
                and not isinstance(row.get("call_seq"), bool)
            ),
            default=0,
        ) + 1
        event = {
            "schema_version": 2,
            "event": "start",
            "call_id": call_id,
            "call_seq": call_seq,
            "phase": phase,
            "role": role,
            "pid": os.getpid(),
            "process_group": os.getpgrp(),
            "session": os.getsid(0),
            "started_ns": time.time_ns(),
            "monotonic_ns": time.monotonic_ns(),
            "run_id": os.environ.get("SMTBATCH_RUN_ID"),
            "job_id": os.environ.get("SMTBATCH_JOB_ID"),
            "attempt": os.environ.get("SMTBATCH_ATTEMPT"),
            "reducer_id": os.environ.get("SMTBATCH_REDUCER_ID"),
            "candidate": _candidate_record(candidate),
        }
        handle.seek(0, os.SEEK_END)
        handle.write(_json_line(event))
        handle.flush()
        os.fsync(handle.fileno())
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    return event


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--phase", required=True)
    parser.add_argument("--solver-timeout", type=float, required=True)
    parser.add_argument("--ignore-stdout", action="store_true")
    parser.add_argument("--ignore-stderr", action="store_true")
    parser.add_argument("--match-stdout")
    parser.add_argument("--match-stderr")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if args.command and args.command[0] == "--":
        args.command.pop(0)
    if len(args.command) < 2:
        parser.error("expected a predicate command followed by a candidate")
    if args.solver_timeout <= 0:
        parser.error("--solver-timeout must be positive")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    call_id = uuid.uuid4().hex
    candidate = Path(args.command[-1]).resolve()
    _append_start(args.log, call_id=call_id, phase=args.phase, candidate=candidate)
    started = time.monotonic()
    returncode = 127
    stdout = b""
    stderr = b""
    error = None
    timed_out = False
    killed = False
    try:
        process = subprocess.Popen(
            [*args.command[:-1], candidate.as_posix(), f"{args.solver_timeout:g}"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True,
        )
    except OSError as exc:
        process = None
        error = f"{type(exc).__name__}: {exc}"
        stderr = (error + "\n").encode("utf-8", errors="replace")
    if process is not None:
        interrupted: dict[str, int] = {}

        def _forward(signum: int, _frame: object) -> None:
            # SMTBatch terminates the whole process group on trial timeout.
            # Forward the signal to the predicate child and stay alive long
            # enough to close this journal entry with a finish event.
            interrupted["signum"] = signum
            try:
                os.killpg(process.pid, signum)
            except ProcessLookupError:
                pass

        signal.signal(signal.SIGTERM, _forward)
        signal.signal(signal.SIGINT, _forward)
        try:
            stdout, stderr = process.communicate(timeout=args.solver_timeout + 1.0)
            returncode = process.returncode
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            stdout, stderr = process.communicate()
            returncode = 124
            error = "predicate wrapper timeout"
        if interrupted:
            killed = True
            timed_out = False
            returncode = 128 + interrupted["signum"]
            error = f"predicate interrupted by signal {interrupted['signum']}"

    _append(args.log, {
        "schema_version": 2,
        "event": "finish",
        "call_id": call_id,
        "finished_ns": time.time_ns(),
        "monotonic_ns": time.monotonic_ns(),
        "runtime_sec": time.monotonic() - started,
        "returncode": returncode,
        "timed_out": timed_out,
        "killed": killed,
        "stdout_bytes": len(stdout),
        "stderr_bytes": len(stderr),
        "stdout_sha256": _sha256_bytes(stdout),
        "stderr_sha256": _sha256_bytes(stderr),
        "stdout_match": (
            None if args.match_stdout is None
            else args.match_stdout.encode("utf-8") in stdout
        ),
        "stderr_match": (
            None if args.match_stderr is None
            else args.match_stderr.encode("utf-8") in stderr
        ),
        "ignore_stdout": args.ignore_stdout,
        "ignore_stderr": args.ignore_stderr,
        "error": error,
    })
    sys.stdout.buffer.write(stdout)
    sys.stdout.buffer.flush()
    sys.stderr.buffer.write(stderr)
    sys.stderr.buffer.flush()
    return returncode


if __name__ == "__main__":
    raise SystemExit(main())
