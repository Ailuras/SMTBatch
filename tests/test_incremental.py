from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from smtbatch.config import SolverSpec
from smtbatch.run import classify_output, run_job
from smtbatch.task import (
    QUERY_COUNT_FIELDS,
    RESULT_CORE_FIELDS,
    RESULT_FIELDS,
    JobSpec,
    count_check_sat,
    incremental_from_row,
    incremental_stats,
    load_jobs,
    parse_answers,
    parse_outcomes,
    results_header_ok,
    write_jobs,
)


def _query_sum(stats: dict[str, object]) -> int:
    return sum(int(stats[key]) for key in QUERY_COUNT_FIELDS)


class IncrementalParsingTests(unittest.TestCase):
    def test_parse_answers_keeps_every_check_sat_line(self) -> None:
        output = "unknown\n(:added-eqs 1)\nsat\nunsat\n"
        self.assertEqual(parse_answers(output), ["unknown", "sat", "unsat"])
        self.assertEqual(parse_outcomes(output), ["unknown", "sat", "unsat"])
        self.assertEqual(classify_output(output), "unsat")

    def test_parse_outcomes_includes_smtlib_error_lines(self) -> None:
        output = 'sat\n(error "line 4: boom")\nunknown\n'
        self.assertEqual(parse_outcomes(output), ["sat", "error", "unknown"])
        self.assertEqual(parse_answers(output), ["sat", "unknown"])
        self.assertEqual(classify_output(output), "unknown")

    def test_classify_output_uses_last_error_outcome(self) -> None:
        self.assertEqual(classify_output('sat\n(error "boom")\n'), "error")

    def test_count_check_sat_ignores_comments_and_nested_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "inc.smt2"
            path.write_text(
                "; (check-sat)\n"
                "(set-logic UF)\n"
                "(check-sat)\n"
                "(assert (not true))\n"
                "( check-sat )\n"
                "(check-sat-assuming (p))\n",
                encoding="utf-8",
            )
            self.assertEqual(count_check_sat(path), 3)

    def test_results_header_requires_the_current_columns(self) -> None:
        self.assertTrue(results_header_ok(RESULT_FIELDS))
        self.assertFalse(results_header_ok(RESULT_CORE_FIELDS))
        self.assertFalse(results_header_ok(["job_id", "solver"]))
        self.assertFalse(results_header_ok([*RESULT_CORE_FIELDS, "not_a_column"]))

    def test_jobs_queue_requires_expected_counts(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            formula = root / "a.smt2"
            formula.write_text("(check-sat)\n(check-sat)\n", encoding="utf-8")
            path = root / "jobs.tsv"
            path.write_text(f"job_id\tsolver\tfile\n1\talpha\t{formula}\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "invalid jobs header"):
                load_jobs(path)
            write_jobs(path, [JobSpec(1, "alpha", formula, 2)])
            jobs = load_jobs(path)
            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0].expected, 2)


class IncrementalPartitionTests(unittest.TestCase):
    def test_complete_file_keeps_printed_unknown(self) -> None:
        stats = incremental_stats(["unknown", "sat", "unsat"], 3, result="unsat", exit_ok=True)
        self.assertEqual(stats["file_status"], "complete")
        self.assertEqual(stats["unknown"], 1)
        self.assertEqual(stats["timeout"], 0)
        self.assertEqual(stats["unreached"], 0)
        self.assertEqual(_query_sum(stats), 3)

    def test_timeout_counts_one_in_flight_query_not_the_tail(self) -> None:
        stats = incremental_stats(["sat", "unknown"], 5, result="timeout", exit_ok=False)
        self.assertEqual(stats["file_status"], "partial")
        self.assertEqual(stats["queries"], 2)
        self.assertEqual(stats["timeout"], 1)
        self.assertEqual(stats["unreached"], 2)
        self.assertEqual(stats["error"], 0)
        self.assertEqual(_query_sum(stats), 5)

    def test_empty_timeout_is_file_timeout(self) -> None:
        stats = incremental_stats([], 4, result="timeout", exit_ok=False)
        self.assertEqual(stats["file_status"], "timeout")
        self.assertEqual(stats["timeout"], 1)
        self.assertEqual(stats["unreached"], 3)
        self.assertEqual(_query_sum(stats), 4)

    def test_crash_counts_in_flight_query_as_error(self) -> None:
        stats = incremental_stats(["sat"], 3, result="error", exit_ok=False)
        self.assertEqual(stats["file_status"], "partial")
        self.assertEqual(stats["error"], 1)
        self.assertEqual(stats["timeout"], 0)
        self.assertEqual(stats["unreached"], 1)
        self.assertEqual(_query_sum(stats), 3)

    def test_early_exit_zero_is_not_complete(self) -> None:
        stats = incremental_stats(["unknown", "unsat"], 3, result="unsat", exit_ok=True)
        self.assertEqual(stats["complete"], "no")
        self.assertEqual(stats["file_status"], "partial")
        self.assertEqual(stats["error"], 1)
        self.assertEqual(stats["unreached"], 0)
        self.assertEqual(_query_sum(stats), 3)

    def test_stored_partition_is_not_reinferred(self) -> None:
        stats = incremental_from_row(
            {
                "result": "timeout",
                "queries": "2",
                "sat": "2",
                "unsat": "0",
                "unknown": "0",
                "error": "0",
                "timeout": "1",
                "unreached": "0",
                "expected": "3",
                "complete": "no",
                "file_status": "partial",
            }
        )
        self.assertEqual(stats["timeout"], 1)
        self.assertEqual(stats["unreached"], 0)
        self.assertEqual(stats["error"], 0)


class IncrementalRunJobTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.formula = root / "inc.smt2"
        self.formula.write_text("(check-sat)\n(check-sat)\n(check-sat-assuming (p))\n", encoding="utf-8")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _spec(self, script: str) -> SolverSpec:
        binary = Path(self.temp.name) / "solver"
        binary.write_text(script, encoding="utf-8")
        binary.chmod(0o755)
        return SolverSpec("alpha", binary, ("{binary}", "{input}"), ("--version",))

    def test_file_success_uses_last_answer_and_allows_unknown(self) -> None:
        item = run_job(
            self._spec("#!/bin/sh\necho unknown\necho unsat\n"),
            JobSpec(1, "alpha", self.formula),
            timeout=5,
            outer_timeout=5,
        )
        self.assertEqual(item.result, "unsat")
        self.assertFalse(item.complete)
        self.assertEqual(item.file_status, "partial")
        self.assertEqual(item.queries, 2)
        self.assertEqual(item.unknown, 1)
        self.assertEqual(item.unsat, 1)
        self.assertEqual(item.error, 1)
        self.assertEqual(item.timeout, 0)
        self.assertEqual(item.unreached, 0)
        self.assertEqual(item.first, "unknown")
        self.assertEqual(item.last, "unsat")
        self.assertEqual(item.expected, 3)
        self.assertEqual(item.sat + item.unsat + item.unknown + item.error + item.timeout + item.unreached, 3)

    def test_timeout_is_not_file_success_even_if_last_printed_sat(self) -> None:
        item = run_job(
            self._spec("#!/bin/sh\necho unknown\necho sat\nsleep 30\n"),
            JobSpec(1, "alpha", self.formula),
            timeout=1,
            outer_timeout=1.0,
        )
        self.assertEqual(item.result, "timeout")
        self.assertFalse(item.complete)
        self.assertEqual(item.file_status, "partial")
        self.assertEqual(item.queries, 2)
        self.assertEqual(item.last, "sat")
        self.assertEqual(item.timeout, 1)
        self.assertEqual(item.unreached, 0)
        self.assertEqual(item.expected, 3)
        self.assertEqual(item.sat + item.unsat + item.unknown + item.error + item.timeout + item.unreached, 3)

    def test_timeout_exit_code_overrides_printed_sat(self) -> None:
        item = run_job(
            self._spec("#!/bin/sh\necho sat\nexit 124\n"),
            JobSpec(1, "alpha", self.formula),
            timeout=5,
            outer_timeout=5,
        )
        self.assertEqual(item.result, "timeout")
        self.assertFalse(item.complete)
        self.assertEqual(item.file_status, "partial")
        self.assertEqual(item.last, "sat")
        self.assertEqual(item.queries, 1)
        self.assertEqual(item.timeout, 1)
        self.assertEqual(item.unreached, 1)

    def test_empty_timeout_leaves_remaining_queries_unreached(self) -> None:
        item = run_job(
            self._spec("#!/bin/sh\nexit 124\n"),
            JobSpec(1, "alpha", self.formula),
            timeout=5,
            outer_timeout=5,
        )
        self.assertEqual(item.result, "timeout")
        self.assertEqual(item.file_status, "timeout")
        self.assertEqual(item.queries, 0)
        self.assertEqual(item.timeout, 1)
        self.assertEqual(item.unreached, 2)

    def test_crash_after_answers_is_partial_with_in_flight_error(self) -> None:
        item = run_job(
            self._spec("#!/bin/sh\necho sat\nexit 1\n"),
            JobSpec(1, "alpha", self.formula),
            timeout=5,
            outer_timeout=5,
        )
        self.assertEqual(item.result, "error")
        self.assertEqual(item.file_status, "partial")
        self.assertEqual(item.sat, 1)
        self.assertEqual(item.error, 1)
        self.assertEqual(item.timeout, 0)
        self.assertEqual(item.unreached, 1)

    def test_timeout_streams_partial_log(self) -> None:
        log_path = Path(self.temp.name) / "job_0000001.alpha.out"
        item = run_job(
            self._spec("#!/bin/sh\necho unknown\necho sat\nsleep 30\n"),
            JobSpec(1, "alpha", self.formula),
            timeout=1,
            outer_timeout=1.0,
            log_path=log_path,
        )
        self.assertEqual(item.result, "timeout")
        self.assertTrue(log_path.is_file())
        text = log_path.read_text(encoding="utf-8")
        self.assertIn("unknown", text)
        self.assertIn("sat", text)

    def test_three_answers_and_exit_zero_is_complete(self) -> None:
        item = run_job(
            self._spec("#!/bin/sh\necho sat\necho unknown\necho unsat\n"),
            JobSpec(1, "alpha", self.formula, 3),
            timeout=5,
            outer_timeout=5,
        )
        self.assertEqual(item.result, "unsat")
        self.assertTrue(item.complete)
        self.assertEqual(item.file_status, "complete")
        self.assertEqual(item.queries, 3)
        self.assertEqual(item.unreached, 0)
        self.assertEqual(item.error, 0)
