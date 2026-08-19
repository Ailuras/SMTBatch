from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from smtbatch.config import SolverSpec
from smtbatch.run import classify_output, run_job
from smtbatch.task import (
    RESULT_CORE_FIELDS,
    RESULT_FIELDS,
    JobSpec,
    count_check_sat,
    parse_answers,
    results_header_ok,
)


class IncrementalParsingTests(unittest.TestCase):
    def test_parse_answers_keeps_every_check_sat_line(self) -> None:
        output = "unknown\n(:added-eqs 1)\nsat\nunsat\n"
        self.assertEqual(parse_answers(output), ["unknown", "sat", "unsat"])
        self.assertEqual(classify_output(output), "unsat")

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

    def test_results_header_accepts_core_or_incremental_columns(self) -> None:
        self.assertTrue(results_header_ok(RESULT_CORE_FIELDS))
        self.assertTrue(results_header_ok(RESULT_FIELDS))
        self.assertFalse(results_header_ok(["job_id", "solver"]))
        self.assertFalse(results_header_ok([*RESULT_CORE_FIELDS, "not_a_column"]))


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
        self.assertTrue(item.complete)
        self.assertEqual(item.queries, 2)
        self.assertEqual(item.unknown, 1)
        self.assertEqual(item.unsat, 1)
        self.assertEqual(item.first, "unknown")
        self.assertEqual(item.last, "unsat")
        self.assertEqual(item.expected, 3)

    def test_timeout_is_not_file_success_even_if_last_printed_sat(self) -> None:
        item = run_job(
            self._spec("#!/bin/sh\necho unknown\necho sat\nsleep 30\n"),
            JobSpec(1, "alpha", self.formula),
            timeout=1,
            outer_timeout=1.0,
        )
        self.assertEqual(item.result, "timeout")
        self.assertFalse(item.complete)
        self.assertEqual(item.queries, 2)
        self.assertEqual(item.last, "sat")
        self.assertEqual(item.expected, 3)

    def test_timeout_exit_code_overrides_printed_sat(self) -> None:
        item = run_job(
            self._spec("#!/bin/sh\necho sat\nexit 124\n"),
            JobSpec(1, "alpha", self.formula),
            timeout=5,
            outer_timeout=5,
        )
        self.assertEqual(item.result, "timeout")
        self.assertFalse(item.complete)
        self.assertEqual(item.last, "sat")
        self.assertEqual(item.queries, 1)
