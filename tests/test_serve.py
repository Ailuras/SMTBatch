from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from smtbatch.serve import ExperimentManager
from smtbatch.task import RESULT_FIELDS


class ExperimentManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.inputs = root / "inputs"
        self.results = root / "results"
        self.inputs.mkdir()
        self.results.mkdir()
        self.formula = self.inputs / "sample.smt2"
        self.formula.write_text("(check-sat)\n", encoding="utf-8")
        self.run = self.results / "sample-run"
        self.run.mkdir()
        (self.run / "jobs.tsv").write_text(
            f"job_id\tsolver\tfile\n1\tz3\t{self.formula}\n2\tcvc5\t{self.formula}\n",
            encoding="utf-8",
        )
        with (self.run / "results.tsv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=RESULT_FIELDS, delimiter="\t")
            writer.writeheader()
            writer.writerow({"job_id": 1, "solver": "z3", "file": str(self.formula), "result": "sat", "time": "2.0", "code": "0", "output_path": ""})
            writer.writerow({"job_id": 2, "solver": "cvc5", "file": str(self.formula), "result": "error", "time": "4.0", "code": "1", "output_path": ""})
        (self.run / "progress.json").write_text(
            json.dumps({"status": "complete", "updated_at": "2026-01-01T00:00:00+00:00", "total_jobs": 2, "completed_jobs": 2}),
            encoding="utf-8",
        )
        (self.run / "metadata.txt").write_text("solvers=z3,cvc5\n", encoding="utf-8")
        self.manager = ExperimentManager(self.results, self.inputs, root)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_positive_float_rejects_non_finite_values(self) -> None:
        for value in ("nan", "inf", "-inf"):
            with self.assertRaises(ValueError):
                self.manager._positive_float(value, "timeout")

    def test_report_uses_canonical_consistency_classification(self) -> None:
        page = self.manager.report_formulas("sample-run", "", "other", 1, 20)
        self.assertEqual(page["total"], 1)
        self.assertEqual(page["cases"][0]["state"], "other")

    def test_formula_pagination_and_scatter_are_bounded(self) -> None:
        page = self.manager.report_formulas("sample-run", "sample", "all", 1, 1)
        self.assertEqual(page["total"], 1)
        self.assertEqual(len(page["cases"]), 1)
        scatter = self.manager.report_scatter("sample-run", "z3", "cvc5", "all")
        self.assertEqual(scatter["total_points"], 1)
        self.assertFalse(scatter["sampled"])

    def test_historical_duration_estimate_uses_matching_jobs(self) -> None:
        history = self.manager._historical_durations()
        self.assertEqual(history[(str(self.formula), "z3")], 2.0)
        self.assertEqual(history[(str(self.formula), "cvc5")], 4.0)


if __name__ == "__main__":
    unittest.main()
