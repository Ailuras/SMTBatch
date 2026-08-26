from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from smtbatch.run import RUN_CONTROL_PAUSED, RUN_CONTROL_RUNNING, _RunLock, write_run_control
from smtbatch.serve import ExperimentManager, scan_runs
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
        self.manager._await_purges()
        self.temp.cleanup()

    def test_positive_float_rejects_non_finite_values(self) -> None:
        for value in ("nan", "inf", "-inf"):
            with self.assertRaises(ValueError):
                self.manager._positive_float(value, "timeout")

    def test_report_uses_canonical_consistency_classification(self) -> None:
        page = self.manager.report_formulas("sample-run", "", "other", 1, 20)
        self.assertEqual(page["total"], 1)
        self.assertEqual(page["cases"][0]["state"], "other")

    def test_report_summary_cactus_uses_time_budget(self) -> None:
        run_dir = self.results / "cactus-run"
        run_dir.mkdir()
        files = {index: self.inputs / f"case-{index}.smt2" for index in range(4)}
        for path in files.values():
            path.write_text("(check-sat)\n", encoding="utf-8")
        rows = [
            (1, "alpha", files[0], "sat", "2.0"),
            (2, "beta", files[0], "timeout", "30.0"),
            (3, "alpha", files[1], "sat", "8.0"),
            (4, "beta", files[1], "sat", "15.0"),
            (5, "alpha", files[2], "sat", "8.0"),
            (6, "beta", files[2], "unknown", "1.0"),
            (7, "alpha", files[3], "timeout", "30.0"),
            (8, "beta", files[3], "error", "0.5"),
        ]
        (run_dir / "jobs.tsv").write_text(
            "job_id\tsolver\tfile\n"
            + "\n".join(f"{job_id}\t{solver}\t{file}" for job_id, solver, file, _, _ in rows)
            + "\n",
            encoding="utf-8",
        )
        (run_dir / "results.tsv").write_text(
            "job_id\tsolver\tfile\tresult\ttime\tcode\toutput_path\n"
            + "\n".join(
                f"{job_id}\t{solver}\t{file}\t{result}\t{time}\t0\t"
                for job_id, solver, file, result, time in rows
            )
            + "\n",
            encoding="utf-8",
        )
        (run_dir / "progress.json").write_text(
            json.dumps(
                {
                    "status": "complete",
                    "updated_at": "2026-01-01T00:00:00+00:00",
                    "total_jobs": 8,
                    "completed_jobs": 8,
                }
            ),
            encoding="utf-8",
        )
        (run_dir / "metadata.txt").write_text("solvers=alpha,beta\ntimeout=30\njobs=2\nlog=all\n", encoding="utf-8")
        summary = self.manager.report_summary("cactus-run", "all")
        self.assertEqual(summary["timeout"], 30.0)
        alpha = summary["by_solver"]["alpha"]
        beta = summary["by_solver"]["beta"]
        self.assertEqual(
            alpha["cactus"],
            [{"time": 2.0, "solved": 1}, {"time": 8.0, "solved": 3}, {"time": 30.0, "solved": 3}],
        )
        self.assertEqual(beta["cactus"], [{"time": 15.0, "solved": 1}, {"time": 30.0, "solved": 1}])
        self.assertEqual(alpha["solved"], 3)
        self.assertEqual(alpha["unique_solved"], 2)
        self.assertEqual(alpha["avg_solved_seconds"], 6.0)
        self.assertEqual(alpha["avg_sat_seconds"], 6.0)
        self.assertIsNone(alpha["avg_unsat_seconds"])
        self.assertEqual(beta["solved"], 1)
        self.assertEqual(beta["unique_solved"], 0)
        self.assertEqual(beta["avg_solved_seconds"], 15.0)

    def test_cactus_counts_watchdog_overshoot_at_the_time_limit(self) -> None:
        run_dir = self.results / "overshoot-run"
        run_dir.mkdir()
        formula = self.inputs / "overshoot.smt2"
        formula.write_text("(check-sat)\n", encoding="utf-8")
        (run_dir / "jobs.tsv").write_text(f"job_id\tsolver\tfile\n1\talpha\t{formula}\n", encoding="utf-8")
        (run_dir / "results.tsv").write_text(
            "job_id\tsolver\tfile\tresult\ttime\tcode\toutput_path\n"
            f"1\talpha\t{formula}\tsat\t30.4\t0\t\n",
            encoding="utf-8",
        )
        (run_dir / "progress.json").write_text(
            json.dumps(
                {
                    "status": "complete",
                    "updated_at": "2026-01-01T00:00:00+00:00",
                    "total_jobs": 1,
                    "completed_jobs": 1,
                }
            ),
            encoding="utf-8",
        )
        (run_dir / "metadata.txt").write_text("solvers=alpha\ntimeout=30\njobs=1\nlog=all\n", encoding="utf-8")
        summary = self.manager.report_summary("overshoot-run", "all")
        self.assertEqual(summary["by_solver"]["alpha"]["solved"], 1)
        self.assertEqual(
            summary["by_solver"]["alpha"]["cactus"],
            [{"time": 30.0, "solved": 1}],
        )

    def test_report_summary_single_solver(self) -> None:
        run_dir = self.results / "single-solver-run"
        run_dir.mkdir()
        formula = self.inputs / "only.smt2"
        formula.write_text("(check-sat)\n", encoding="utf-8")
        (run_dir / "jobs.tsv").write_text(f"job_id\tsolver\tfile\n1\talpha\t{formula}\n", encoding="utf-8")
        (run_dir / "results.tsv").write_text(
            "job_id\tsolver\tfile\tresult\ttime\tcode\toutput_path\n"
            f"1\talpha\t{formula}\tunsat\t3.5\t0\t\n",
            encoding="utf-8",
        )
        (run_dir / "progress.json").write_text(
            json.dumps(
                {
                    "status": "complete",
                    "updated_at": "2026-01-01T00:00:00+00:00",
                    "total_jobs": 1,
                    "completed_jobs": 1,
                }
            ),
            encoding="utf-8",
        )
        (run_dir / "metadata.txt").write_text("solvers=alpha\ntimeout=30\njobs=1\nlog=all\n", encoding="utf-8")
        summary = self.manager.report_summary("single-solver-run", "all")
        alpha = summary["by_solver"]["alpha"]
        self.assertEqual(alpha["solved"], 1)
        self.assertEqual(alpha["unique_solved"], 1)
        self.assertEqual(alpha["avg_solved_seconds"], 3.5)
        self.assertEqual(alpha["avg_unsat_seconds"], 3.5)
        self.assertIsNone(alpha["avg_sat_seconds"])
        self.assertEqual(alpha["cactus"], [{"time": 3.5, "solved": 1}, {"time": 30.0, "solved": 1}])
        scatter = self.manager.report_scatter("single-solver-run", "alpha", "alpha", "all")
        self.assertEqual(scatter["total_points"], 1)
        self.assertEqual(scatter["points"][0]["x"], 3.5)
        self.assertEqual(scatter["points"][0]["y"], 3.5)

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
        self.assertEqual(preview["total_file_count"], 1)
        self.assertEqual(preview["selected_file_count"], 1)
        self.assertEqual(preview["inputs"], [str(self.inputs)])
        self.assertEqual(
            preview["input_counts"],
            [{"path": str(self.inputs), "file_count": 1}],
        )

    def test_preview_unions_multiple_input_directories(self) -> None:
        extra = self.inputs.parent / "inputs-b"
        extra.mkdir()
        (extra / "other.smt2").write_text("(check-sat)\n", encoding="utf-8")
        request = {
            "inputs": [str(self.inputs), str(extra)],
            "solvers": ["alpha"],
            "timeout": 30,
            "jobs": 2,
            "limit": 0,
        }
        preview = self.manager.preview(request)
        self.assertTrue(preview["input_valid"])
        self.assertEqual(preview["file_count"], 2)
        self.assertEqual(preview["inputs"], [str(self.inputs), str(extra)])
        self.assertEqual(
            preview["input_counts"],
            [
                {"path": str(self.inputs), "file_count": 1},
                {"path": str(extra), "file_count": 1},
            ],
        )

    def test_launch_repeats_input_flags_for_each_directory(self) -> None:
        extra = self.inputs.parent / "inputs-c"
        extra.mkdir()
        (extra / "other.smt2").write_text("(check-sat)\n", encoding="utf-8")
        request = {
            "inputs": [str(self.inputs), str(extra)],
            "solvers": ["alpha"],
            "timeout": 30,
            "jobs": 1,
            "limit": 0,
            "name": "multi-input-run",
        }
        with mock.patch("smtbatch.serve.subprocess.Popen") as popen:
            popen.return_value.poll.return_value = None
            self.manager.launch(request)
        command = popen.call_args[0][0]
        flags = [command[index + 1] for index, token in enumerate(command) if token == "--input"]
        self.assertEqual(flags, [str(self.inputs), str(extra)])
        state = self.manager._read_last_run()
        self.assertEqual(state["inputs"], [str(self.inputs), str(extra)])
        self.assertEqual(state["input"], str(self.inputs))

    def test_preview_keeps_total_count_separate_from_formula_limit(self) -> None:
        for index in range(3):
            (self.inputs / f"extra-{index}.smt2").write_text("(check-sat)\n", encoding="utf-8")
        request = {"input": str(self.inputs), "solvers": ["alpha"], "timeout": 30, "jobs": 2, "limit": 2}
        preview = self.manager.preview(request)
        self.assertEqual(preview["file_count"], 4)
        self.assertEqual(preview["total_file_count"], 4)
        self.assertEqual(preview["selected_file_count"], 2)
        self.assertEqual(preview["historical_pairs"] + preview["fallback_pairs"], 2)

    def test_count_inputs_does_not_require_solvers(self) -> None:
        counted = self.manager.count_inputs({"input": str(self.inputs)})
        self.assertTrue(counted["input_valid"])
        self.assertEqual(counted["file_count"], 1)
        self.assertEqual(
            counted["input_counts"],
            [{"path": str(self.inputs), "file_count": 1}],
        )
        empty = self.manager.count_inputs({"input": str(self.empty)})
        self.assertFalse(empty["input_valid"])
        self.assertEqual(empty["file_count"], 0)

    def test_preview_rejects_directory_without_smt2_files(self) -> None:
        request = {"input": str(self.empty), "solvers": ["alpha"], "timeout": 30, "jobs": 2, "limit": 0}
        preview = self.manager.preview(request)
        self.assertFalse(preview["input_valid"])
        self.assertIn("no .smt2 files", preview["input_error"])
        with self.assertRaisesRegex(ValueError, r"no \.smt2 files"):
            self.manager.launch({**request, "name": "empty-run"})

    def test_preview_skips_cold_history_scan(self) -> None:
        request = {"input": str(self.inputs), "solvers": ["alpha"], "timeout": 30, "jobs": 2, "limit": 0}
        with mock.patch.object(self.manager, "_historical_durations") as scanned:
            preview = self.manager.preview(request)
        scanned.assert_not_called()
        self.assertEqual(preview["historical_pairs"], 0)
        self.assertEqual(preview["fallback_pairs"], 1)
        self.assertIsNone(self.manager._history_cache)

    def test_preview_uses_warm_history_without_rescan(self) -> None:
        self.manager._historical_durations()
        request = {"input": str(self.inputs), "solvers": ["alpha"], "timeout": 30, "jobs": 2, "limit": 0}
        with mock.patch.object(self.manager, "_historical_durations") as scanned:
            preview = self.manager.preview(request)
        scanned.assert_not_called()
        self.assertEqual(preview["historical_pairs"], 1)
        self.assertEqual(preview["fallback_pairs"], 0)

    def test_preview_with_no_solvers_reports_zero_pairs(self) -> None:
        preview = self.manager.preview(
            {"input": str(self.inputs), "solvers": [], "timeout": 30, "jobs": 2, "limit": 0}
        )
        self.assertTrue(preview["input_valid"])
        self.assertEqual(preview["file_count"], 1)
        self.assertEqual(preview["historical_pairs"], 0)
        self.assertEqual(preview["fallback_pairs"], 0)
        self.assertEqual(preview["estimated_seconds"], 0.0)

    def test_files_is_instance_method_and_respects_limit(self) -> None:
        extra = self.inputs / "second.smt2"
        extra.write_text("(check-sat)\n", encoding="utf-8")
        listed = self.manager._files(self.inputs, 0)
        self.assertEqual(len(listed), 2)
        self.assertEqual(self.manager._files([self.inputs], 1), listed[:1])

    def test_list_smt2_reuses_directory_cache(self) -> None:
        first = self.manager._list_smt2(self.inputs)
        with mock.patch.object(Path, "rglob", side_effect=AssertionError("uncached rglob")):
            second = self.manager._list_smt2(self.inputs)
        self.assertEqual(first, second)
        self.assertEqual(len(first), 1)

    def test_config_api_uses_arbitrary_toml_solver_names_and_labels(self) -> None:
        config = self.manager.config()
        self.assertEqual(config["solvers"], ["alpha", "beta"])
        self.assertEqual(
            config["solver_options"],
            [{"name": "alpha", "label": "Alpha engine"}, {"name": "beta", "label": "beta"}],
        )
        self.assertEqual(config["config_path"], str(self.config_path))
        self.assertIsInstance(config["browse_available"], bool)

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

    def _interrupted_run(self, name: str) -> Path:
        run_dir = self.results / name
        run_dir.mkdir()
        (run_dir / "jobs.tsv").write_text(
            f"job_id\tsolver\tfile\n1\talpha\t{self.formula}\n2\tbeta\t{self.formula}\n",
            encoding="utf-8",
        )
        (run_dir / "results.tsv").write_text(
            "job_id\tsolver\tfile\tresult\ttime\tcode\toutput_path\n",
            encoding="utf-8",
        )
        (run_dir / "progress.json").write_text(
            json.dumps({"status": "interrupted", "updated_at": "2026-01-01T00:00:00+00:00", "total_jobs": 2, "completed_jobs": 1}),
            encoding="utf-8",
        )
        (run_dir / "metadata.txt").write_text(
            "solvers=alpha,beta\ntimeout=30\njobs=2\nlog=all\n",
            encoding="utf-8",
        )
        return run_dir

    def test_resume_rejects_complete_experiment(self) -> None:
        with self.assertRaisesRegex(ValueError, "already complete"):
            self.manager.resume("sample-run", {"jobs": 2})

    def test_resume_accepts_stale_running_experiment_without_live_pid(self) -> None:
        run_dir = self._interrupted_run("running-run")
        (run_dir / "progress.json").write_text(
            json.dumps({"status": "running", "updated_at": "2026-01-01T00:00:00+00:00"}),
            encoding="utf-8",
        )
        with mock.patch("smtbatch.serve.subprocess.Popen") as popen:
            popen.return_value.poll.return_value = None
            response = self.manager.resume("running-run", {"jobs": 2})
        self.assertEqual(response["status"], "resuming")

    def test_runs_exposes_stale_running_experiment_as_interrupted(self) -> None:
        run_dir = self._interrupted_run("stale-run")
        (run_dir / "progress.json").write_text(
            json.dumps({"status": "running", "updated_at": "2026-01-01T00:00:00+00:00"}),
            encoding="utf-8",
        )
        run = next(item for item in self.manager.runs() if item["run_id"] == "stale-run")
        self.assertEqual(run["status"], "interrupted")
        self.assertEqual(run["stale_status"], "running")

    def test_run_lock_keeps_running_state_live_without_pid_probe(self) -> None:
        run_dir = self._interrupted_run("locked-run")
        (run_dir / "progress.json").write_text(
            json.dumps({"status": "running", "updated_at": "2026-01-01T00:00:00+00:00"}),
            encoding="utf-8",
        )
        lock = _RunLock(run_dir)
        lock.acquire()
        try:
            run = next(item for item in self.manager.runs() if item["run_id"] == "locked-run")
            self.assertEqual(run["status"], "running")
            self.assertNotIn("locked-run", self.manager.processes)
            response = self.manager.resume("locked-run", {"jobs": 2})
            self.assertEqual(response["status"], "resuming")
            self.assertNotIn("locked-run", self.manager.processes)
            self.assertEqual((run_dir / ".run.control").read_text(encoding="utf-8").strip(), RUN_CONTROL_RUNNING)
        finally:
            lock.release()

    def test_runs_backfills_old_runner_metrics_incrementally(self) -> None:
        run_dir = self._interrupted_run("old-runner")
        results_path = run_dir / "results.tsv"
        results_path.write_text(
            "job_id\tsolver\tfile\tresult\ttime\tcode\toutput_path\n"
            f"1\talpha\t{self.formula}\tsat\t2.0\t0\t\n",
            encoding="utf-8",
        )
        (run_dir / "progress.json").write_text(
            json.dumps(
                {
                    "status": "running",
                    "updated_at": "2026-01-01T00:00:00+00:00",
                    "total_jobs": 2,
                    "completed_jobs": 1,
                }
            ),
            encoding="utf-8",
        )
        lock = _RunLock(run_dir)
        lock.acquire()
        try:
            first = next(item for item in self.manager.runs() if item["run_id"] == "old-runner")
            self.assertEqual(first["performance"]["average_solved_seconds"], 2.0)
            self.assertEqual(first["performance"]["par2_seconds"], 2.0)

            with results_path.open("a", encoding="utf-8") as handle:
                handle.write(f"2\tbeta\t{self.formula}\terror\t4.0\t1\t\n")
            second = next(item for item in self.manager.runs() if item["run_id"] == "old-runner")
            self.assertEqual(
                second["performance"],
                {
                    "completed_jobs": 2,
                    "solved_jobs": 1,
                    "average_solved_seconds": 2.0,
                    "par2_seconds": 31.0,
                },
            )
            self.assertEqual(second["by_solver_performance"]["beta"]["par2_seconds"], 60.0)
        finally:
            lock.release()

    def test_resume_rejects_zero_or_missing_jobs(self) -> None:
        self._interrupted_run("interrupted-run")
        with self.assertRaisesRegex(ValueError, "positive integer"):
            self.manager.resume("interrupted-run", {"jobs": 0})
        with self.assertRaisesRegex(ValueError, "non-negative integer"):
            self.manager.resume("interrupted-run", {"jobs": -1})

    def test_resume_launches_resume_command(self) -> None:
        run_dir = self._interrupted_run("interrupted-run")
        with mock.patch("smtbatch.serve.subprocess.Popen") as popen:
            popen.return_value.poll.return_value = None
            response = self.manager.resume("interrupted-run", {"jobs": 4})
        self.assertEqual(response["status"], "resuming")
        command = popen.call_args.args[0]
        self.assertIn("--resume", command)
        self.assertEqual(command[command.index("--jobs") + 1], "4")
        self.assertIn(str(run_dir), command)
        self.assertIn("--output", command)

    def test_resume_unpauses_managed_process_without_second_popen(self) -> None:
        run_dir = self._interrupted_run("interrupted-run")
        with mock.patch("smtbatch.serve.subprocess.Popen") as popen:
            popen.return_value.poll.return_value = None
            self.manager.resume("interrupted-run", {"jobs": 2})
            response = self.manager.resume("interrupted-run", {"jobs": 2})
        self.assertEqual(popen.call_count, 1)
        self.assertEqual(response["status"], "resuming")
        self.assertEqual((run_dir / ".run.control").read_text(encoding="utf-8").strip(), RUN_CONTROL_RUNNING)

    def test_cancel_writes_paused_control_without_signal(self) -> None:
        run_dir = self._interrupted_run("live-run")
        (run_dir / "progress.json").write_text(
            json.dumps({"status": "running", "updated_at": "2026-01-01T00:00:00+00:00"}),
            encoding="utf-8",
        )
        lock = _RunLock(run_dir)
        lock.acquire()
        try:
            response = self.manager.cancel_run("live-run")
            self.assertEqual(response, {"run_id": "live-run", "status": "paused"})
            self.assertEqual((run_dir / ".run.control").read_text(encoding="utf-8").strip(), RUN_CONTROL_PAUSED)
        finally:
            lock.release()

    def test_paused_live_run_stays_paused_in_history(self) -> None:
        run_dir = self._interrupted_run("paused-run")
        (run_dir / "progress.json").write_text(
            json.dumps({"status": "paused", "updated_at": "2026-01-01T00:00:00+00:00"}),
            encoding="utf-8",
        )
        lock = _RunLock(run_dir)
        lock.acquire()
        try:
            run = next(item for item in self.manager.runs() if item["run_id"] == "paused-run")
            self.assertEqual(run["status"], "paused")
        finally:
            lock.release()

    def test_stale_paused_without_pid_is_interrupted(self) -> None:
        self._interrupted_run("stale-paused")
        run_dir = self.results / "stale-paused"
        (run_dir / "progress.json").write_text(
            json.dumps({"status": "paused", "updated_at": "2026-01-01T00:00:00+00:00"}),
            encoding="utf-8",
        )
        run = next(item for item in self.manager.runs() if item["run_id"] == "stale-paused")
        self.assertEqual(run["status"], "interrupted")
        self.assertEqual(run["stale_status"], "paused")

    def test_cancel_requires_active_process(self) -> None:
        with self.assertRaisesRegex(ValueError, "no active run process"):
            self.manager.cancel_run("sample-run")

    def test_delete_run_removes_only_the_selected_results_directory(self) -> None:
        sibling = self.results / "keep-run"
        sibling.mkdir()
        (sibling / "marker.txt").write_text("keep\n", encoding="utf-8")
        launcher = self.results / ".sample-run.controller.log"
        keeper = self.results / ".keep-run.controller.log"
        launcher.write_text("sample launcher\n", encoding="utf-8")
        keeper.write_text("keep launcher\n", encoding="utf-8")

        response = self.manager.delete_run("sample-run")

        self.assertEqual(response, {"run_id": "sample-run", "status": "deleted"})
        self.assertFalse((self.results / "sample-run").exists())
        self.assertFalse(launcher.exists())
        self.assertTrue((sibling / "marker.txt").is_file())
        self.assertEqual(keeper.read_text(encoding="utf-8"), "keep launcher\n")

    def test_delete_run_hides_history_before_files_finish_removing(self) -> None:
        started = threading.Event()
        release = threading.Event()
        original = shutil.rmtree

        def blocked_rmtree(path, **kwargs):
            started.set()
            self.assertTrue(release.wait(2))
            return original(path, **kwargs)

        launcher = self.results / ".sample-run.controller.log"
        launcher.write_text("sample launcher\n", encoding="utf-8")
        with mock.patch("smtbatch.serve.shutil.rmtree", blocked_rmtree):
            response = self.manager.delete_run("sample-run")
            self.assertEqual(response["status"], "deleted")
            self.assertFalse((self.results / "sample-run").exists())
            self.assertFalse(launcher.exists())
            self.assertTrue(started.wait(2))
            self.assertTrue(any(path.name.startswith(".deleting-") for path in self.results.iterdir()))
            release.set()
        self.manager._await_purges()

    def test_delete_run_rejects_active_experiment(self) -> None:
        run_dir = self._interrupted_run("active-run")
        (run_dir / "progress.json").write_text(
            json.dumps({"status": "running", "updated_at": "2026-01-01T00:00:00+00:00"}),
            encoding="utf-8",
        )
        lock = _RunLock(run_dir)
        lock.acquire()
        try:
            with self.assertRaisesRegex(ValueError, "still running"):
                self.manager.delete_run("active-run")
            self.assertTrue(run_dir.is_dir())
        finally:
            lock.release()

    def test_delete_run_rejects_missing_or_escaping_directory(self) -> None:
        with self.assertRaisesRegex(ValueError, "not found"):
            self.manager.delete_run("missing-run")
        with self.assertRaisesRegex(ValueError, "invalid experiment name"):
            self.manager.delete_run("../outside")

    def test_config_port_defaults_to_8000_and_reads_toml(self) -> None:
        from smtbatch.config import load_config

        self.assertEqual(load_config(self.config_path.parent).port, 8000)
        self.config_path.write_text(
            self.config_path.read_text(encoding="utf-8").replace(
                '[defaults]\ninputs = "inputs"\nresults = "results"',
                '[defaults]\ninputs = "inputs"\nresults = "results"\nport = 8011',
            ),
            encoding="utf-8",
        )
        self.assertEqual(load_config(self.config_path.parent).port, 8011)

    def test_config_port_rejects_non_integer_toml_values(self) -> None:
        from smtbatch.config import load_config

        original = self.config_path.read_text(encoding="utf-8")
        for value in ("true", "8000.5"):
            self.config_path.write_text(original.replace('results = "results"', f'results = "results"\nport = {value}'), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "port must be an integer"):
                load_config(self.config_path.parent)

    def test_resolve_port_prefers_flag_over_config(self) -> None:
        from smtbatch.serve import _resolve_port

        old_cwd = Path.cwd()
        try:
            os.chdir(self.config_path.parent)
            self.assertEqual(_resolve_port(argparse.Namespace(port=None)), 8000)
            self.assertEqual(_resolve_port(argparse.Namespace(port=9000)), 9000)
        finally:
            os.chdir(old_cwd)

    def test_resolve_port_reports_invalid_config_instead_of_falling_back(self) -> None:
        from smtbatch.serve import _resolve_port

        self.config_path.write_text(
            self.config_path.read_text(encoding="utf-8").replace('results = "results"', 'results = "results"\nport = 70000'),
            encoding="utf-8",
        )
        old_cwd = Path.cwd()
        try:
            os.chdir(self.config_path.parent)
            with self.assertRaisesRegex(RuntimeError, "between 1 and 65535"):
                _resolve_port(argparse.Namespace(port=None))
        finally:
            os.chdir(old_cwd)

    def test_launch_persists_last_run_and_config_reports_it(self) -> None:
        request = {"input": str(self.inputs), "solvers": ["alpha", "beta"], "timeout": 30, "jobs": 3, "limit": 5, "name": "mem-run"}
        with mock.patch("smtbatch.serve.subprocess.Popen") as popen:
            popen.return_value.poll.return_value = None
            self.manager.launch(request)
        state = self.manager._read_last_run()
        self.assertEqual(state["input"], str(self.inputs))
        self.assertEqual(state["solvers"], ["alpha", "beta"])
        self.assertEqual(state["timeout"], 30)
        self.assertEqual(state["jobs"], 3)
        self.assertEqual(state["limit"], 5)
        self.assertEqual(self.manager.config()["last_run"]["jobs"], 3)

    def test_launch_reports_last_run_persistence_failure_as_warning(self) -> None:
        request = {"input": str(self.inputs), "solvers": ["alpha"], "timeout": 30, "jobs": 1, "limit": 0, "name": "warning-run"}
        with (
            mock.patch("smtbatch.serve.subprocess.Popen") as popen,
            mock.patch.object(self.manager, "_save_last_run", side_effect=OSError("disk full")),
        ):
            popen.return_value.poll.return_value = None
            response = self.manager.launch(request)
        self.assertEqual(response["status"], "starting")
        self.assertIn("disk full", response["warning"])

    def test_kill_after_sigkill_is_shown_as_timeout(self) -> None:
        run_dir = self.results / "kill-after-run"
        run_dir.mkdir()
        formula = self.inputs / "slow.smt2"
        formula.write_text("(check-sat)\n", encoding="utf-8")
        (run_dir / "jobs.tsv").write_text(f"job_id\tsolver\tfile\n1\talpha\t{formula}\n", encoding="utf-8")
        (run_dir / "results.tsv").write_text(
            "job_id\tsolver\tfile\tresult\ttime\tcode\toutput_path\n"
            f"1\talpha\t{formula}\terror\t302.5\t-9\t\n",
            encoding="utf-8",
        )
        (run_dir / "progress.json").write_text(
            json.dumps(
                {
                    "status": "complete",
                    "updated_at": "2026-01-01T00:00:00+00:00",
                    "total_jobs": 1,
                    "completed_jobs": 1,
                    "outcomes": {"error": 1, "timeout": 0, "sat": 0, "unsat": 0, "unknown": 0},
                    "by_solver": {"alpha": {"error": 1, "timeout": 0, "sat": 0, "unsat": 0, "unknown": 0}},
                }
            ),
            encoding="utf-8",
        )
        (run_dir / "metadata.txt").write_text("solvers=alpha\ntimeout=300\njobs=1\nlog=all\n", encoding="utf-8")
        summary = self.manager.report_summary("kill-after-run", "all")
        self.assertEqual(summary["by_solver"]["alpha"]["outcomes"]["timeout"], 1)
        self.assertEqual(summary["by_solver"]["alpha"]["outcomes"]["error"], 0)
        history = {item["run_id"]: item for item in scan_runs(self.results)}
        self.assertEqual(history["kill-after-run"]["outcomes"]["timeout"], 1)
        self.assertEqual(history["kill-after-run"]["outcomes"]["error"], 0)


if __name__ == "__main__":
    unittest.main()
