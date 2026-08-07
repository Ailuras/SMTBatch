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
        self.empty = root / "empty"
        self.inputs.mkdir()
        self.results.mkdir()
        self.empty.mkdir()
        self.bin_dir = root / "bin"
        self.bin_dir.mkdir()
        for name in ("alpha", "beta", "gamma"):
            binary = self.bin_dir / name
            binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            binary.chmod(0o755)
        self.config_path = root / "smtbatch.toml"
        self.config_path.write_text(
            """[defaults]
inputs = "inputs"
results = "results"

[solvers.alpha]
label = "Alpha engine"
binary = "bin/alpha"
command = ["{binary}", "{input}"]

[solvers.beta]
binary = "bin/beta"
command = ["{binary}", "{input}"]
""",
            encoding="utf-8",
        )
        self.formula = self.inputs / "sample.smt2"
        self.formula.write_text("(check-sat)\n", encoding="utf-8")
        self.run = self.results / "sample-run"
        self.run.mkdir()
        (self.run / "jobs.tsv").write_text(
            f"job_id\tsolver\tfile\n1\talpha\t{self.formula}\n2\tbeta\t{self.formula}\n",
            encoding="utf-8",
        )
        with (self.run / "results.tsv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=RESULT_FIELDS, delimiter="\t")
            writer.writeheader()
            writer.writerow({"job_id": 1, "solver": "alpha", "file": str(self.formula), "result": "sat", "time": "2.0", "code": "0", "output_path": ""})
            writer.writerow({"job_id": 2, "solver": "beta", "file": str(self.formula), "result": "error", "time": "4.0", "code": "1", "output_path": ""})
        (self.run / "progress.json").write_text(
            json.dumps({"status": "complete", "updated_at": "2026-01-01T00:00:00+00:00", "total_jobs": 2, "completed_jobs": 2}),
            encoding="utf-8",
        )
        (self.run / "metadata.txt").write_text("solvers=alpha,beta\n", encoding="utf-8")
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
        scatter = self.manager.report_scatter("sample-run", "alpha", "beta", "all")
        self.assertEqual(scatter["total_points"], 1)
        self.assertFalse(scatter["sampled"])

    def test_historical_duration_estimate_uses_matching_jobs(self) -> None:
        history = self.manager._historical_durations()
        self.assertEqual(history[(str(self.formula), "alpha")], 2.0)
        self.assertEqual(history[(str(self.formula), "beta")], 4.0)

    def test_preview_reports_effective_input(self) -> None:
        request = {"input": str(self.inputs), "solvers": ["alpha"], "timeout": 30, "jobs": 2, "limit": 0}
        preview = self.manager.preview(request)
        self.assertTrue(preview["input_valid"])
        self.assertEqual(preview["input_error"], "")
        self.assertEqual(preview["file_count"], 1)

    def test_preview_rejects_directory_without_smt2_files(self) -> None:
        request = {"input": str(self.empty), "solvers": ["alpha"], "timeout": 30, "jobs": 2, "limit": 0}
        preview = self.manager.preview(request)
        self.assertFalse(preview["input_valid"])
        self.assertIn("no .smt2 files", preview["input_error"])
        with self.assertRaisesRegex(ValueError, r"no \.smt2 files"):
            self.manager.launch({**request, "name": "empty-run"})

    def test_config_api_uses_arbitrary_toml_solver_names_and_labels(self) -> None:
        config = self.manager.config()
        self.assertEqual(config["solvers"], ["alpha", "beta"])
        self.assertEqual(
            config["solver_options"],
            [{"name": "alpha", "label": "Alpha engine"}, {"name": "beta", "label": "beta"}],
        )
        self.assertEqual(config["config_path"], str(self.config_path))

    def test_config_api_reloads_solver_menu_after_toml_change(self) -> None:
        self.config_path.write_text(
            """[defaults]
inputs = "inputs"
results = "results"

[solvers.alpha]
binary = "bin/alpha"
command = ["{binary}", "{input}"]

[solvers.gamma]
label = "Gamma engine"
binary = "bin/gamma"
command = ["{binary}", "{input}"]
""",
            encoding="utf-8",
        )
        config = self.manager.config()
        self.assertEqual(config["solvers"], ["alpha", "gamma"])
        self.assertEqual(config["solver_options"][1], {"name": "gamma", "label": "Gamma engine"})


if __name__ == "__main__":
    unittest.main()
