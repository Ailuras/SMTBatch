"""Run one benchmark predicate and record external reduction evidence."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import uuid

if __package__:
    from .oracle_protocol import decode
else:  # Direct script entry used by predicate wrappers.
    from oracle_protocol import decode


_CORRELATION_ENV = {
    "SMTBATCH_PREDICATE_ROLE",
    "SMTBATCH_PROPOSAL_ID",
    "SMTBATCH_CANDIDATE_SEQUENCE",
    "SMTBATCH_INCUMBENT_SEQUENCE",
    "SMTBATCH_CANDIDATE_RAW_SHA256",
    "SMTBATCH_STRATEGY",
    "SMTBATCH_PASS",
    "SMTBATCH_MUTATOR",
    "SMTBATCH_TASK",
}
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
# GNU timeout(1) exits 124. Python may instead see the wrapper die under
# --kill-after as SIGKILL/SIGTERM. SIGABRT is a timeout only when ForteSMT
# printed its SIGALRM marker; a plain assertion abort is still a crash.
_GNU_TIMEOUT_CODE = 124
_KILL_SIGNAL_CODES = frozenset(
    {
        -signal.SIGKILL,
        -signal.SIGTERM,
        128 + signal.SIGKILL,
        128 + signal.SIGTERM,
    }
)
_ABORT_SIGNAL_CODES = frozenset({-signal.SIGABRT, 128 + signal.SIGABRT})
_INTERNAL_TIMEOUT_MARKER = b"ForteSMT interrupted by timeout."
_LIMIT_TOLERANCE_SECONDS = 0.1


def _as_bytes(blob: bytes | str) -> bytes:
    return blob if isinstance(blob, bytes) else blob.encode("utf-8", errors="replace")


def _has_marker_line(blob: bytes | str, marker: bytes) -> bool:
    return any(line.strip() == marker for line in _as_bytes(blob).splitlines())


def _near_limit(duration_sec: float, timeout: float) -> bool:
    return (
        math.isfinite(duration_sec)
        and math.isfinite(timeout)
        and timeout > 0
        and duration_sec + _LIMIT_TOLERANCE_SECONDS >= timeout
    )


def is_timeout_exit(
    code: int | None,
    duration_sec: float = 0.0,
    timeout: float = 0.0,
    stdout: bytes | str = b"",
    stderr: bytes | str = b"",
) -> bool:
    """True when the exit carries evidence that the solver time limit fired."""
    if code is None:
        return False
    if code in _ABORT_SIGNAL_CODES:
        return _has_marker_line(stdout, _INTERNAL_TIMEOUT_MARKER) or _has_marker_line(
            stderr, _INTERNAL_TIMEOUT_MARKER
        )
    if code == _GNU_TIMEOUT_CODE:
        return True
    if code in _KILL_SIGNAL_CODES:
        return _near_limit(duration_sec, timeout)
    return False


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
        record.update(bytes=len(raw), sha256=_sha256_bytes(raw))
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
                "byte_count": len(raw),
                "normalized_byte_count": len(" ".join(tokens).encode("utf-8")),
                "token_count": len(tokens),
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


def _internal_context(
    environment: dict[str, str] | os._Environ[str] | None = None,
) -> dict[str, object] | None:
    """Read and strictly validate optional reducer-to-wrapper correlation."""

    source = os.environ if environment is None else environment
    call_id = source.get("SMTBATCH_PREDICATE_CALL_ID")
    populated = sorted(name for name in _CORRELATION_ENV if source.get(name))
    if not call_id:
        if populated:
            raise ValueError(
                "predicate correlation metadata requires "
                "SMTBATCH_PREDICATE_CALL_ID"
            )
        return None
    if not _IDENTIFIER.fullmatch(call_id):
        raise ValueError("invalid SMTBATCH_PREDICATE_CALL_ID")

    role = source.get("SMTBATCH_PREDICATE_ROLE")
    if role not in {"golden", "candidate", "cross-check", "cross-check-golden"}:
        raise ValueError("invalid SMTBATCH_PREDICATE_ROLE")
    proposal_id = source.get("SMTBATCH_PROPOSAL_ID")
    if proposal_id is not None and not _IDENTIFIER.fullmatch(proposal_id):
        raise ValueError("invalid SMTBATCH_PROPOSAL_ID")
    if role == "candidate" and proposal_id is None:
        raise ValueError("candidate correlation requires SMTBATCH_PROPOSAL_ID")

    result: dict[str, object] = {
        "source": "reducer",
        "predicate_call_id": call_id,
        "role": role,
        "proposal_id": proposal_id,
    }
    for environment_name, field in (
        ("SMTBATCH_CANDIDATE_SEQUENCE", "candidate_sequence"),
        ("SMTBATCH_INCUMBENT_SEQUENCE", "incumbent_sequence"),
    ):
        value = source.get(environment_name)
        if value is None:
            result[field] = None
            continue
        if not value.isascii() or not value.isdecimal():
            raise ValueError(f"invalid {environment_name}")
        result[field] = int(value)

    raw_sha256 = source.get("SMTBATCH_CANDIDATE_RAW_SHA256")
    if raw_sha256 is not None and not _SHA256.fullmatch(raw_sha256):
        raise ValueError("invalid SMTBATCH_CANDIDATE_RAW_SHA256")
    result["candidate_raw_sha256"] = raw_sha256
    for environment_name, field in (
        ("SMTBATCH_STRATEGY", "strategy"),
        ("SMTBATCH_PASS", "pass"),
        ("SMTBATCH_MUTATOR", "mutator"),
        ("SMTBATCH_TASK", "task"),
    ):
        value = source.get(environment_name)
        if value is not None:
            if not value or len(value) > 256 or any(
                ord(char) < 32 or ord(char) == 127 for char in value
            ):
                raise ValueError(f"invalid {environment_name}")
            result[field] = value
        else:
            result[field] = None
    return result


def _append_start(
    path: Path, *, call_id: str, phase: str, candidate: Path,
    internal: dict[str, object] | None = None,
) -> dict[str, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        rows = _read_rows(handle)
        if any(row.get("format") != "predicate" or "schema_version" in row for row in rows):
            raise ValueError("unsupported predicate journal format; start a new attempt")
        starts = [row for row in rows if row.get("event") == "start"]
        reducer_starts = [row for row in starts if row.get("phase") == "reducer"]
        role = phase
        if phase == "reducer":
            role = "golden" if not reducer_starts else "candidate"
        if internal is not None:
            internal_role = internal.get("role")
            if internal_role in {"golden", "candidate"} and internal_role != role:
                raise ValueError(
                    f"internal predicate role {internal_role!r} does not match "
                    f"external role {role!r}"
                )
        call_seq = max(
            (
                int(row["call_seq"])
                for row in starts
                if isinstance(row.get("call_seq"), int)
                and not isinstance(row.get("call_seq"), bool)
            ),
            default=0,
        ) + 1
        candidate_record = _candidate_record(candidate)
        if internal is not None:
            internal_sha256 = internal.get("candidate_raw_sha256")
            if internal_sha256 is not None and internal_sha256 != candidate_record["sha256"]:
                raise ValueError(
                    "internal candidate hash does not match wrapper candidate"
                )
        event = {
            "format": "predicate",
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
            "candidate": candidate_record,
            "internal": internal,
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
    internal = _internal_context()
    call_id = (
        str(internal["predicate_call_id"])
        if internal is not None
        else uuid.uuid4().hex
    )
    process = None
    interrupted: dict[str, int] = {}

    def stop_child() -> None:
        if process is not None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

    def on_signal(signum: int, _frame: object) -> None:
        # Install before publishing start, and retain through finish/output.
        # Local reducer deadlines can expire during startup or finalization.
        interrupted.setdefault("signum", signum)
        stop_child()

    previous_handlers = {
        signum: signal.getsignal(signum)
        for signum in (signal.SIGTERM, signal.SIGINT)
    }
    try:
        for signum in previous_handlers:
            signal.signal(signum, on_signal)
        candidate = Path(args.command[-1]).resolve()
        _append_start(
            args.log, call_id=call_id, phase=args.phase, candidate=candidate,
            internal=internal,
        )
        started = time.monotonic()
        returncode = 127
        stdout = b""
        stderr = b""
        error = None
        timed_out = False
        killed = False
        try:
            if not interrupted:
                process = subprocess.Popen(
                    [*args.command[:-1], candidate.as_posix(), f"{args.solver_timeout:g}"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True,
                )
        except OSError as exc:
            process = None
            error = f"{type(exc).__name__}: {exc}"
            stderr = (error + "\n").encode("utf-8", errors="replace")
        if process is not None:
            # A signal can arrive while Popen is returning, before process is
            # assigned. Recheck the latched signal before waiting on that child.
            if interrupted:
                stop_child()
            try:
                stdout, stderr = process.communicate(
                    timeout=args.solver_timeout + 1.0
                )
                returncode = process.returncode
                runtime_sec = time.monotonic() - started
                if is_timeout_exit(
                    returncode, runtime_sec, args.solver_timeout, stdout, stderr
                ):
                    # Same corpse as the wrapper-enforced path, so a GNU 124
                    # and a --kill-after SIGKILL still match as one timeout.
                    timed_out = True
                    returncode = _GNU_TIMEOUT_CODE
                    error = "predicate solver timeout"
            except subprocess.TimeoutExpired:
                timed_out = True
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                stdout, stderr = process.communicate()
                returncode = _GNU_TIMEOUT_CODE
                error = "predicate wrapper timeout"
        oracle = decode(stdout, returncode)
        if interrupted:
            killed = True
            timed_out = False
            returncode = 128 + interrupted["signum"]
            error = f"predicate interrupted by signal {interrupted['signum']}"
            oracle = None
        if oracle and oracle.get('outcome')=='incomplete':
            timed_out=any((oracle.get(role) or {}).get('status')=='timeout' for role in ('target','reference'))
        if "--json" in args.command and oracle is None and not timed_out and not killed:
            returncode = 2
            error = "malformed oracle response"
        _append(args.log, {
            "format": "predicate",
            "oracle": oracle,
            "solver_executions": oracle.get("solver_executions") if oracle else None,
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
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)



if __name__ == "__main__":
    raise SystemExit(main())
