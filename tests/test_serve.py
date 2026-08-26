from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from smtbatch.run import RUN_CONTROL_PAUSED, RUN_CONTROL_RUNNING, _RunLock
from smtbatch.serve import ExperimentManager
from smtbatch.task import RESULT_FIELDS
from tests.tsvutil import jobs_tsv, result_row, write_results


def _init_smtbatch_checkout(root: Path, branch: str = "main") -> Path:
    checkout = root / "SMTBatch"
    checkout.mkdir()
    (checkout / "marker").write_text(f"{branch}\n", encoding="utf-8")
    subprocess.run(
        ["git", "init", "-b", branch],
        cwd=checkout,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    subprocess.run(["git", "add", "marker"], cwd=checkout, check=True, stdout=subprocess.DEVNULL)
    subprocess.run(
        ["git", "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "fixture"],
        cwd=checkout,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    return checkout


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
        _init_smtbatch_checkout(root)
        self.run = self.results / "sample-run"
        self.run.mkdir()
        (self.run / "jobs.tsv").write_text(
            jobs_tsv([(1, "alpha", self.formula, 1), (2, "beta", self.formula, 1)]),
            encoding="utf-8",
        )
        write_results(
            self.run / "results.tsv",
            [
                result_row(job_id=1, solver="alpha", file=self.formula, result="sat", time="2.0"),
                result_row(
                    job_id=2,
                    solver="beta",
                    file=self.formula,
                    result="error",
                    time="4.0",
                    code="1",
                    queries=0,
                    sat=0,
                    first="",
                    last="",
                    complete="no",
                    file_status="error",
                    error=1,
                    unreached=0,
                ),
            ],
        )
        (self.run / "progress.json").write_text(
            json.dumps({"status": "complete", "updated_at": "2026-01-01T00:00:00+00:00", "total_jobs": 2, "completed_jobs": 2}),
            encoding="utf-8",
        )
        (self.run / "metadata.txt").write_text("solvers=alpha,beta\ntimeout=30\n", encoding="utf-8")
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
            result_row(job_id=1, solver="alpha", file=files[0], result="sat", time="2.0"),
            result_row(
                job_id=2,
                solver="beta",
                file=files[0],
                result="timeout",
                time="30.0",
                code="124",
                queries=0,
                sat=0,
                first="",
                last="",
                complete="no",
                file_status="timeout",
                timeout=1,
            ),
            result_row(job_id=3, solver="alpha", file=files[1], result="sat", time="8.0"),
            result_row(job_id=4, solver="beta", file=files[1], result="sat", time="15.0"),
            result_row(job_id=5, solver="alpha", file=files[2], result="sat", time="8.0"),
            result_row(
                job_id=6,
                solver="beta",
                file=files[2],
                result="unknown",
                time="1.0",
                sat=0,
                unknown=1,
                first="unknown",
                last="unknown",
            ),
            result_row(
                job_id=7,
                solver="alpha",
                file=files[3],
                result="timeout",
                time="30.0",
                code="124",
                queries=0,
                sat=0,
                first="",
                last="",
                complete="no",
                file_status="timeout",
                timeout=1,
            ),
            result_row(
                job_id=8,
                solver="beta",
                file=files[3],
                result="error",
                time="0.5",
                code="1",
                queries=0,
                sat=0,
                first="",
                last="",
                complete="no",
                file_status="error",
                error=1,
            ),
        ]
        (run_dir / "jobs.tsv").write_text(
            jobs_tsv([(row["job_id"], row["solver"], Path(row["file"]), 1) for row in rows]),
            encoding="utf-8",
        )
        write_results(run_dir / "results.tsv", rows)
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

    def test_report_summary_cactus_credits_partial_check_sat(self) -> None:
        run_dir = self.results / "po-cactus-run"
        run_dir.mkdir()
        early = self.inputs / "early.smt2"
        late = self.inputs / "late.smt2"
        early.write_text("(check-sat)\n(check-sat)\n(check-sat)\n", encoding="utf-8")
        late.write_text("(check-sat)\n" * 10, encoding="utf-8")
        (run_dir / "jobs.tsv").write_text(
            jobs_tsv([(1, "alpha", early, 3), (2, "alpha", late, 10)]),
            encoding="utf-8",
        )
        with (run_dir / "results.tsv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=RESULT_FIELDS, delimiter="\t")
            writer.writeheader()
            writer.writerow(
                {
                    "job_id": 1,
                    "solver": "alpha",
                    "file": str(early),
                    "result": "unsat",
                    "time": "2.0",
                    "code": "0",
                    "output_path": "",
                    "queries": "3",
                    "sat": "0",
                    "unsat": "3",
                    "unknown": "0",
                    "error": "0",
                    "timeout": "0",
                    "unreached": "0",
                    "first": "unsat",
                    "last": "unsat",
                    "expected": "3",
                    "complete": "yes",
                    "file_status": "complete",
                }
            )
            writer.writerow(
                {
                    "job_id": 2,
                    "solver": "alpha",
                    "file": str(late),
                    "result": "timeout",
                    "time": "30.0",
                    "code": "124",
                    "output_path": "",
                    "queries": "5",
                    "sat": "4",
                    "unsat": "1",
                    "unknown": "0",
                    "error": "0",
                    "timeout": "1",
                    "unreached": "4",
                    "first": "sat",
                    "last": "unsat",
                    "expected": "10",
                    "complete": "no",
                    "file_status": "partial",
                }
            )
        (run_dir / "progress.json").write_text(
            json.dumps(
                {
                    "status": "complete",
                    "updated_at": "2026-01-01T00:00:00+00:00",
                    "total_jobs": 2,
                    "completed_jobs": 2,
                }
            ),
            encoding="utf-8",
        )
        (run_dir / "metadata.txt").write_text("solvers=alpha\ntimeout=30\njobs=1\nlog=all\n", encoding="utf-8")
        summary = self.manager.report_summary("po-cactus-run", "all")
        alpha = summary["by_solver"]["alpha"]
        self.assertEqual(alpha["query_solved"], 8)
        self.assertEqual(alpha["solved"], 1)
        self.assertEqual(
            alpha["cactus"],
            [{"time": 2.0, "solved": 3}, {"time": 30.0, "solved": 8}],
        )
        self.assertEqual(
            alpha["file_cactus"],
            [{"time": 2.0, "solved": 1}, {"time": 30.0, "solved": 1}],
        )

    def test_cactus_clips_timeout_overshoot(self) -> None:
        run_dir = self.results / "overshoot-cactus-run"
        run_dir.mkdir()
        formula = self.inputs / "overshoot.smt2"
        formula.write_text("(check-sat)\n" * 10, encoding="utf-8")
        (run_dir / "jobs.tsv").write_text(jobs_tsv([(1, "alpha", formula, 10)]), encoding="utf-8")
        write_results(
            run_dir / "results.tsv",
            [
                result_row(
                    job_id=1,
                    solver="alpha",
                    file=formula,
                    result="timeout",
                    time="30.2",
                    code="124",
                    queries=5,
                    sat=4,
                    unsat=1,
                    first="sat",
                    last="unsat",
                    expected=10,
                    complete="no",
                    file_status="partial",
                    timeout=1,
                    unreached=4,
                )
            ],
        )
        (run_dir / "progress.json").write_text(
            json.dumps({"status": "complete", "updated_at": "2026-01-01T00:00:00+00:00", "total_jobs": 1, "completed_jobs": 1}),
            encoding="utf-8",
        )
        (run_dir / "metadata.txt").write_text("solvers=alpha\ntimeout=30\njobs=1\nlog=all\n", encoding="utf-8")
        summary = self.manager.report_summary("overshoot-cactus-run", "all")
        alpha = summary["by_solver"]["alpha"]
        self.assertEqual(alpha["query_solved"], 5)
        self.assertEqual(alpha["cactus"], [{"time": 30.0, "solved": 5}])
        self.assertEqual(summary["cactus_credit"], "file-runtime")

    def test_cactus_credits_query_event_times(self) -> None:
        run_dir = self.results / "event-cactus-run"
        run_dir.mkdir()
        formula = self.inputs / "timed.smt2"
        formula.write_text("(check-sat)\n(check-sat)\n", encoding="utf-8")
        (run_dir / "jobs.tsv").write_text(jobs_tsv([(1, "alpha", formula, 2)]), encoding="utf-8")
        write_results(
            run_dir / "results.tsv",
            [
                result_row(
                    job_id=1,
                    solver="alpha",
                    file=formula,
                    result="unsat",
                    time="30.0",
                    sat=0,
                    unsat=2,
                    first="unsat",
                    last="unsat",
                    expected=2,
                )
            ],
        )
        events = run_dir / "events"
        events.mkdir()
        (events / "job_0000001.alpha.tsv").write_text(
            "ordinal\telapsed_ms\tdelta_ms\toutcome\tsource\n"
            "1\t1000\t1000\tunsat\tsolver\n"
            "2\t25000\t24000\tunsat\tsolver\n",
            encoding="utf-8",
        )
        (run_dir / "progress.json").write_text(
            json.dumps({"status": "complete", "updated_at": "2026-01-01T00:00:00+00:00", "total_jobs": 1, "completed_jobs": 1}),
            encoding="utf-8",
        )
        (run_dir / "metadata.txt").write_text("solvers=alpha\ntimeout=30\njobs=1\nlog=all\n", encoding="utf-8")
        summary = self.manager.report_summary("event-cactus-run", "all")
        alpha = summary["by_solver"]["alpha"]
        self.assertEqual(summary["cactus_credit"], "query-events")
        self.assertEqual(summary["query_cactus_status"], "ready")
        self.assertEqual(
            alpha["cactus"],
            [{"time": 1.0, "solved": 1}, {"time": 25.0, "solved": 2}, {"time": 30.0, "solved": 2}],
        )
        self.assertEqual(alpha["file_cactus"], [{"time": 30.0, "solved": 1}])

    def test_scatter_clips_time_and_keeps_coverage_outliers(self) -> None:
        from smtbatch.serve import _select_scatter_points

        cloud = [
            {
                "left_coverage": 0.1,
                "right_coverage": 0.1,
                "left_solved": 1,
                "right_solved": 1,
                "left_status": "partial",
                "right_status": "partial",
            }
        ] * 9000
        cloud.append(
            {
                "left_coverage": 0.1,
                "right_coverage": 0.9,
                "left_solved": 1,
                "right_solved": 9,
                "left_status": "timeout",
                "right_status": "partial",
            }
        )
        selected, sampled = _select_scatter_points(cloud, limit=8000)
        self.assertTrue(sampled)
        self.assertLessEqual(len(selected), 8000)
        self.assertGreater(len(selected), 1000)
        self.assertTrue(any(point["right_coverage"] == 0.9 for point in selected))

        run_dir = self.results / "scatter-clamp-run"
        run_dir.mkdir()
        formula = self.inputs / "clamp.smt2"
        formula.write_text("(check-sat)\n", encoding="utf-8")
        (run_dir / "jobs.tsv").write_text(jobs_tsv([(1, "alpha", formula, 1), (2, "beta", formula, 1)]), encoding="utf-8")
        write_results(
            run_dir / "results.tsv",
            [
                result_row(job_id=1, solver="alpha", file=formula, result="timeout", time="301.0", code="124", queries=1, sat=1, complete="no", file_status="partial", timeout=1),
                result_row(job_id=2, solver="beta", file=formula, result="sat", time="12.0"),
            ],
        )
        (run_dir / "progress.json").write_text(
            json.dumps({"status": "complete", "updated_at": "2026-01-01T00:00:00+00:00", "total_jobs": 2, "completed_jobs": 2}),
            encoding="utf-8",
        )
        (run_dir / "metadata.txt").write_text("solvers=alpha,beta\ntimeout=300\njobs=1\nlog=all\n", encoding="utf-8")
        scatter = self.manager.report_scatter("scatter-clamp-run", "alpha", "beta", "all")
        self.assertEqual(scatter["timeout"], 300.0)
        self.assertEqual(scatter["points"][0]["x"], 300.0)
        self.assertEqual(scatter["points"][0]["y"], 12.0)
        self.assertEqual(scatter["points"][0]["expected"], 1)

    def test_report_summary_single_solver(self) -> None:
        run_dir = self.results / "single-solver-run"
        run_dir.mkdir()
        formula = self.inputs / "only.smt2"
        formula.write_text("(check-sat)\n", encoding="utf-8")
        (run_dir / "jobs.tsv").write_text(jobs_tsv([(1, "alpha", formula, 1)]), encoding="utf-8")
        write_results(
            run_dir / "results.tsv",
            [
                result_row(
                    job_id=1,
                    solver="alpha",
                    file=formula,
                    result="unsat",
                    time="3.5",
                    sat=0,
                    unsat=1,
                    first="unsat",
                    last="unsat",
                )
            ],
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
        self.assertEqual(scatter["points"][0]["left_coverage"], 1.0)
        self.assertEqual(scatter["points"][0]["right_coverage"], 1.0)

    def test_formula_pagination_and_scatter_are_bounded(self) -> None:
        page = self.manager.report_formulas("sample-run", "sample", "all", 1, 1)
        self.assertEqual(page["total"], 1)
        self.assertEqual(len(page["cases"]), 1)
        scatter = self.manager.report_scatter("sample-run", "alpha", "beta", "all")
        self.assertEqual(scatter["total_points"], 1)
        self.assertFalse(scatter["sampled"])
        self.assertEqual(scatter["points"][0]["left_coverage"], 1.0)
        self.assertEqual(scatter["points"][0]["right_coverage"], 0.0)
        page = self.manager.report_formulas("sample-run", "sample", "all", 1, 1)
        alpha = page["cases"][0]["results"]["alpha"]
        self.assertEqual(alpha["queries"], 1)
        self.assertEqual(alpha["last"], "sat")
        self.assertEqual(alpha["complete"], "yes")
        self.assertEqual(alpha["file_status"], "complete")
        self.assertEqual(alpha["file_outcome"], "complete")
        self.assertEqual(alpha["sat"], 1)
        self.assertEqual(alpha["timeout"], 0)
        self.assertEqual(alpha["unreached"], 0)
        summary = self.manager.report_summary("sample-run", "all")
        self.assertEqual(summary["by_solver"]["alpha"]["file_complete"], 1)
        self.assertEqual(summary["by_solver"]["alpha"]["answers"], 1)
        self.assertEqual(summary["by_solver"]["alpha"]["partial_timeout"], 0)
        self.assertEqual(summary["by_solver"]["alpha"]["query_sat"], 1)
        self.assertEqual(summary["by_solver"]["alpha"]["query_solved"], 1)
        self.assertEqual(summary["by_solver"]["beta"]["query_error"], 1)
        self.assertEqual(summary["by_solver"]["beta"]["file_error"], 1)
        self.assertEqual(page["cases"][0]["done"], 2)
        self.assertEqual(page["cases"][0]["total"], 2)
        self.assertEqual(page["cases"][0]["expected"], 1)
        self.assertIsNone(page["cases"][0]["queries"])

    def test_formula_list_reports_check_sat_progress(self) -> None:
        run_dir = self.results / "inc-progress-run"
        run_dir.mkdir()
        formula = self.inputs / "inc.smt2"
        formula.write_text("(check-sat)\n(check-sat)\n(check-sat)\n(check-sat)\n", encoding="utf-8")
        (run_dir / "jobs.tsv").write_text(jobs_tsv([(1, "alpha", formula, 4)]), encoding="utf-8")
        with (run_dir / "results.tsv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=RESULT_FIELDS, delimiter="\t")
            writer.writeheader()
            writer.writerow(
                {
                    "job_id": 1,
                    "solver": "alpha",
                    "file": str(formula),
                    "result": "timeout",
                    "time": "300.0",
                    "code": "124",
                    "output_path": "",
                    "queries": "2",
                    "sat": "1",
                    "unsat": "0",
                    "unknown": "1",
                    "error": "0",
                    "timeout": "1",
                    "unreached": "1",
                    "first": "sat",
                    "last": "unknown",
                    "expected": "4",
                    "complete": "no",
                    "file_status": "partial",
                }
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
        (run_dir / "metadata.txt").write_text("solvers=alpha\ntimeout=300\n", encoding="utf-8")
        page = self.manager.report_formulas("inc-progress-run", "", "all", 1, 20)
        self.assertEqual(page["cases"][0]["queries"], 2)
        self.assertEqual(page["cases"][0]["expected"], 4)
        self.assertEqual(page["cases"][0]["done"], 1)
        self.assertEqual(page["cases"][0]["total"], 1)
        self.assertEqual(page["cases"][0]["results"]["alpha"]["file_outcome"], "timeout")
        truncated = self.manager.report_summary("inc-progress-run", "all")
        self.assertEqual(truncated["by_solver"]["alpha"]["file_timeout"], 1)
        self.assertEqual(truncated["by_solver"]["alpha"]["file_complete"], 0)
        self.assertEqual(truncated["by_solver"]["alpha"]["file_partial"], 0)

        pending_dir = self.results / "inc-pending-run"
        pending_dir.mkdir()
        (pending_dir / "jobs.tsv").write_text(jobs_tsv([(1, "alpha", formula, 4)]), encoding="utf-8")
        write_results(pending_dir / "results.tsv", [])
        (pending_dir / "progress.json").write_text(
            json.dumps(
                {
                    "status": "running",
                    "updated_at": "2026-01-01T00:00:00+00:00",
                    "total_jobs": 1,
                    "completed_jobs": 0,
                }
            ),
            encoding="utf-8",
        )
        (pending_dir / "metadata.txt").write_text("solvers=alpha\ntimeout=300\n", encoding="utf-8")
        pending = self.manager.report_formulas("inc-pending-run", "", "all", 1, 20)
        self.assertEqual(pending["cases"][0]["queries"], 0)
        self.assertEqual(pending["cases"][0]["expected"], 4)
        pending_summary = self.manager.report_summary("inc-pending-run", "all")
        self.assertEqual(pending_summary["by_solver"]["alpha"]["expected"], 4)

    def test_report_summary_file_outcome_uses_query_partition(self) -> None:
        run_dir = self.results / "file-outcome-run"
        run_dir.mkdir()
        decided = self.inputs / "decided.smt2"
        unknown = self.inputs / "unknown.smt2"
        decided.write_text("(check-sat)\n(check-sat)\n", encoding="utf-8")
        unknown.write_text("(check-sat)\n(check-sat)\n", encoding="utf-8")
        (run_dir / "jobs.tsv").write_text(
            jobs_tsv([(1, "alpha", decided, 2), (2, "alpha", unknown, 2)]),
            encoding="utf-8",
        )
        write_results(
            run_dir / "results.tsv",
            [
                result_row(
                    job_id=1,
                    solver="alpha",
                    file=decided,
                    result="unsat",
                    queries=2,
                    sat=1,
                    unsat=1,
                    expected=2,
                    first="sat",
                    last="unsat",
                ),
                result_row(
                    job_id=2,
                    solver="alpha",
                    file=unknown,
                    result="unknown",
                    queries=2,
                    sat=1,
                    unsat=0,
                    unknown=1,
                    expected=2,
                    first="sat",
                    last="unknown",
                    complete="yes",
                    file_status="complete",
                ),
            ],
        )
        (run_dir / "progress.json").write_text(
            json.dumps({"status": "complete", "updated_at": "2026-01-01T00:00:00+00:00", "total_jobs": 2, "completed_jobs": 2}),
            encoding="utf-8",
        )
        (run_dir / "metadata.txt").write_text("solvers=alpha\ntimeout=30\n", encoding="utf-8")
        summary = self.manager.report_summary("file-outcome-run", "all")
        alpha = summary["by_solver"]["alpha"]
        self.assertEqual(alpha["completed"], 2)
        self.assertEqual(alpha["file_complete"], 1)
        self.assertEqual(alpha["file_partial"], 1)
        self.assertEqual(alpha["file_timeout"], 0)
        self.assertEqual(alpha["file_error"], 0)
        page = self.manager.report_formulas("file-outcome-run", "", "all", 1, 20)
        outcomes = {case["file"]: case["results"]["alpha"]["file_outcome"] for case in page["cases"]}
        self.assertEqual(outcomes["decided.smt2"], "complete")
        self.assertEqual(outcomes["unknown.smt2"], "partial")

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

    def test_preview_keeps_total_count_separate_from_formula_limit(self) -> None:
        for index in range(3):
            (self.inputs / f"extra-{index}.smt2").write_text("(check-sat)\n", encoding="utf-8")
        request = {"input": str(self.inputs), "solvers": ["alpha"], "timeout": 30, "jobs": 2, "limit": 2}
        preview = self.manager.preview(request)
        self.assertEqual(preview["file_count"], 4)
        self.assertEqual(preview["total_file_count"], 4)
        self.assertEqual(preview["selected_file_count"], 2)
        self.assertEqual(preview["historical_pairs"] + preview["fallback_pairs"], 2)

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
        self.assertEqual(config["target_branch"], "main")
        self.assertEqual(config["smtbatch_branch"], "main")
        self.assertTrue(config["can_launch"])
        self.assertIsInstance(config["browse_available"], bool)

    def test_launch_rejects_wrong_smtbatch_branch(self) -> None:
        self.config_path.write_text(
            self.config_path.read_text(encoding="utf-8").replace(
                '[defaults]\ninputs = "inputs"\nresults = "results"',
                '[defaults]\ninputs = "inputs"\nresults = "results"\ntarget_branch = "feat/incremental"',
            ),
            encoding="utf-8",
        )
        config = self.manager.config()
        self.assertFalse(config["can_launch"])
        self.assertIn("expected 'feat/incremental'", config["branch_error"])
        with self.assertRaisesRegex(ValueError, "wrong SMTBatch branch"):
            self.manager.launch(
                {"input": str(self.inputs), "solvers": ["alpha"], "timeout": 30, "jobs": 1, "limit": 0, "name": "bad-branch"}
            )

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
            jobs_tsv([(1, "alpha", self.formula, 1), (2, "beta", self.formula, 1)]),
            encoding="utf-8",
        )
        write_results(run_dir / "results.tsv", [])
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
        self.manager._await_purges()

        self.assertEqual(response, {"run_id": "sample-run", "status": "deleted"})
        self.assertFalse((self.results / "sample-run").exists())
        self.assertFalse(launcher.exists())
        self.assertFalse(any(path.name.startswith(".deleting-") for path in self.results.iterdir()))
        self.assertTrue((sibling / "marker.txt").is_file())
        self.assertEqual(keeper.read_text(encoding="utf-8"), "keep launcher\n")

    def test_delete_run_hides_history_before_files_finish_removing(self) -> None:
        released = threading.Event()
        started = threading.Event()
        original = shutil.rmtree

        def blocked_rmtree(path, **kwargs):
            started.set()
            self.assertTrue(released.wait(timeout=2))
            original(path, **kwargs)

        launcher = self.results / ".sample-run.controller.log"
        launcher.write_text("sample launcher\n", encoding="utf-8")
        with mock.patch("smtbatch.serve.shutil.rmtree", blocked_rmtree):
            response = self.manager.delete_run("sample-run")
            self.assertEqual(response["status"], "deleted")
            self.assertFalse((self.results / "sample-run").exists())
            self.assertFalse(launcher.exists())
            self.assertFalse(any(run["run_id"] == "sample-run" for run in self.manager.runs()))
            self.assertTrue(started.wait(timeout=2))
            released.set()
        self.manager._await_purges()
        self.assertFalse(any(path.name.startswith(".deleting-") for path in self.results.iterdir()))

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

    def test_launch_writes_starting_card_before_jobs_exist(self) -> None:
        request = {
            "input": str(self.inputs),
            "solvers": ["alpha"],
            "timeout": 30,
            "jobs": 1,
            "limit": 0,
            "name": "startup-run",
        }
        with mock.patch("smtbatch.serve.subprocess.Popen") as popen:
            popen.return_value.poll.return_value = None
            response = self.manager.launch(request)
        self.assertEqual(response["status"], "starting")
        run_dir = self.results / "startup-run"
        self.assertTrue((run_dir / "progress.json").is_file())
        self.assertFalse((run_dir / "jobs.tsv").exists())
        payload = json.loads((run_dir / "progress.json").read_text(encoding="utf-8"))
        self.assertEqual(payload["status"], "starting")
        self.assertEqual(payload["phase"], "launching")
        listed = next(item for item in self.manager.runs() if item["run_id"] == "startup-run")
        self.assertEqual(listed["status"], "starting")
        self.assertIn("Launching", listed.get("startup_note", ""))
        summary = self.manager.report_summary("startup-run", "all")
        self.assertEqual(summary["status"], "starting")
        self.assertEqual(summary["timeout"], 30)
        self.assertEqual(summary["case_count"], 0)
        self.assertEqual(summary["solvers"], ["alpha"])

    def test_scan_runs_includes_starting_progress_without_jobs(self) -> None:
        from smtbatch.serve import scan_runs

        run_dir = self.results / "starting-only"
        run_dir.mkdir()
        (run_dir / "progress.json").write_text(
            json.dumps(
                {
                    "status": "starting",
                    "startup_note": "Counting check-sat commands",
                    "updated_at": "2099-01-01T00:00:00+00:00",
                    "started_at": "2099-01-01T00:00:00+00:00",
                }
            ),
            encoding="utf-8",
        )
        found = next(item for item in scan_runs(self.results) if item["run_id"] == "starting-only")
        self.assertEqual(found["status"], "starting")
        listed = next(item for item in self.manager.runs() if item["run_id"] == "starting-only")
        self.assertEqual(listed["status"], "starting")
        summary = self.manager.report_summary("starting-only", "all")
        self.assertEqual(summary["status"], "starting")
        self.assertEqual(summary["case_count"], 0)


if __name__ == "__main__":
    unittest.main()
