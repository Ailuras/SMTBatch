from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import signal
import tempfile
import threading
import time
import unittest
from collections import Counter
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from smtbatch.run import (
    ProgressTracker,
    JobResult,
    _RunLock,
    _load_existing_results,
    _prepare_fresh,
    _prepare_resume,
    parse_args,
    main,
    run_queue,
)
from smtbatch.task import JobSpec, load_jobs


class ResumeLogicTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.root = root
        self._old_cwd = Path.cwd()
        os.chdir(root)
        inputs = root / "inputs"
        inputs.mkdir()
        (root / "results").mkdir()
        bindir = root / "bin"
        bindir.mkdir()
        self.binary = bindir / "alpha"
        self.binary.write_text("#!/bin/sh\necho sat\n", encoding="utf-8")
        self.binary.chmod(0o755)
        self.config_path = root / "smtbatch.toml"
        self.config_path.write_text(
            '[defaults]\ninputs = "inputs"\nresults = "results"\n'
            '[solvers.alpha]\nbinary = "bin/alpha"\ncommand = ["{binary}", "{input}"]\n',
            encoding="utf-8",
        )
        self.formulas = []
        for index in range(4):
            formula = inputs / f"f{index}.smt2"
            formula.write_text("(check-sat)\n", encoding="utf-8")
            self.formulas.append(formula)

    def tearDown(self) -> None:
        os.chdir(self._old_cwd)
        self.temp.cleanup()

    def _make_run(self, name: str, completed: int, total: int = 4) -> Path:
        run_dir = self.root / "results" / name
        run_dir.mkdir()
        lines = ["job_id\tsolver\tfile"]
        for job_id in range(1, total + 1):
            lines.append(f"{job_id}\talpha\t{self.formulas[job_id - 1]}")
        (run_dir / "jobs.tsv").write_text("\n".join(lines) + "\n", encoding="utf-8")
        rows = ["job_id\tsolver\tfile\tresult\ttime\tcode\toutput_path"]
        for job_id in range(1, completed + 1):
            rows.append(f"{job_id}\talpha\t{self.formulas[job_id - 1]}\tsat\t0.5\t0\t")
        (run_dir / "results.tsv").write_text("\n".join(rows) + "\n", encoding="utf-8")
        (run_dir / "metadata.txt").write_text(
            "format=pair-queue-v1\n"
            "solvers=alpha\n"
            "timeout=10\n"
            "jobs=2\n"
            "log=all\n"
            f"solver_config={self.config_path}\n"
            f"alpha_solver_binary={self.binary}\n"
            f"alpha_solver_binary_sha256={hashlib.sha256(self.binary.read_bytes()).hexdigest()}\n"
            'alpha_solver_command_json=["{binary}","{input}"]\n',
            encoding="utf-8",
        )
        return run_dir

    def test_load_existing_results_counts_partial_results(self) -> None:
        run_dir = self._make_run("partial", completed=2)
        completed, outcomes, by_solver = _load_existing_results(
            run_dir / "results.tsv", load_jobs(run_dir / "jobs.tsv")
        )
        self.assertEqual(completed, {1, 2})
        self.assertEqual(outcomes, Counter({"sat": 2}))
        self.assertEqual(dict(by_solver["alpha"]), {"sat": 2})

    def test_load_existing_results_rejects_truncated_or_mismatched_rows(self) -> None:
        run_dir = self._make_run("corrupt", completed=0)
        jobs = load_jobs(run_dir / "jobs.tsv")
        for row in (
            "1\talpha\n",
            f"1\tbeta\t{self.formulas[0]}\tsat\t0.5\t0\t\n",
        ):
            (run_dir / "results.tsv").write_text(
                "job_id\tsolver\tfile\tresult\ttime\tcode\toutput_path\n" + row,
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "malformed|does not match"):
                _load_existing_results(run_dir / "results.tsv", jobs)

    def test_prepare_resume_selects_only_missing_jobs(self) -> None:
        run_dir = self._make_run("partial", completed=2)
        args = parse_args(["--resume", "--output", str(run_dir), "--jobs", "4", "--log", "all"])
        plan = _prepare_resume(args)
        self.assertEqual([job.job_id for job in plan.remaining], [3, 4])
        self.assertEqual(plan.completed_before, 2)
        self.assertEqual(plan.pair_count, 4)
        self.assertEqual(plan.timeout, 10.0)
        self.assertEqual(plan.log, "all")
        self.assertTrue(plan.append_results)
        self.assertEqual(plan.solvers, ("alpha",))

    def test_prepare_resume_keeps_metadata_timeout_ignoring_flag(self) -> None:
        run_dir = self._make_run("partial", completed=1)
        args = parse_args(["--resume", "--output", str(run_dir), "--timeout", "99"])
        plan = _prepare_resume(args)
        self.assertEqual(plan.timeout, 10.0)

    def test_prepare_resume_rejects_solver_binary_or_command_drift(self) -> None:
        binary_run = self._make_run("binary-drift", completed=1)
        self.binary.write_text("#!/bin/sh\necho unsat\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "binary hash changed"):
            _prepare_resume(parse_args(["--resume", "--output", str(binary_run)]))

        # Restore the binary and build a separately recorded run before changing the command.
        self.binary.write_text("#!/bin/sh\necho sat\n", encoding="utf-8")
        command_run = self._make_run("command-drift", completed=1)
        self.config_path.write_text(
            self.config_path.read_text(encoding="utf-8").replace(
                'command = ["{binary}", "{input}"]',
                'command = ["{binary}", "--new-option", "{input}"]',
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "solver command changed"):
            _prepare_resume(parse_args(["--resume", "--output", str(command_run)]))

    def test_prepare_resume_preserves_initial_metadata_and_appends_history(self) -> None:
        run_dir = self._make_run("history", completed=1)
        _prepare_resume(parse_args(["--resume", "--output", str(run_dir), "--jobs", "7"]))
        metadata = (run_dir / "metadata.txt").read_text(encoding="utf-8")
        self.assertIn("jobs=2\n", metadata)
        self.assertNotIn("jobs=7\n", metadata)
        history = [json.loads(line) for line in (run_dir / "resume_history.jsonl").read_text(encoding="utf-8").splitlines()]
        self.assertEqual(history[-1]["workers"], 7)
        self.assertEqual(history[-1]["remaining_jobs"], 3)

    def test_prepare_resume_rejects_input_and_solver_flags(self) -> None:
        run_dir = self._make_run("partial", completed=1)
        for extra in (["--input", "inputs"], ["--solver", "alpha"]):
            with self.assertRaisesRegex(ValueError, "--resume cannot be combined"):
                _prepare_resume(parse_args(["--resume", "--output", str(run_dir), *extra]))

    def test_prepare_resume_requires_job_queue(self) -> None:
        run_dir = self.root / "results" / "empty-run"
        run_dir.mkdir()
        args = parse_args(["--resume", "--output", str(run_dir)])
        with self.assertRaisesRegex(ValueError, "no job queue found"):
            _prepare_resume(args)

    def test_prepare_fresh_requires_solver_and_input(self) -> None:
        with self.assertRaisesRegex(ValueError, "required unless --resume"):
            _prepare_fresh(parse_args(["--output", "results/x"]))

    def test_resume_reruns_only_missing_jobs(self) -> None:
        run_dir = self._make_run("partial", completed=2)
        args = parse_args(["--resume", "--output", str(run_dir), "--jobs", "2", "--log", "all"])
        plan = _prepare_resume(args)
        tracker = ProgressTracker(run_dir, plan.solvers, plan.pair_count, 1.0, 5)
        tracker.completed_jobs = plan.completed_before
        tracker.outcomes = Counter(plan.outcomes_before)
        tracker.by_solver = {solver: Counter(plan.by_solver_before.get(solver, ())) for solver in plan.solvers}
        run_queue(
            plan.remaining,
            plan.specs,
            timeout=plan.timeout,
            workers=2,
            logs_dir=plan.logs_dir,
            log_policy=plan.log,
            checkpoint_every=25,
            tracker=tracker,
            results_path=run_dir / "results.tsv",
            append_results=plan.append_results,
        )
        with (run_dir / "results.tsv").open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle, delimiter="\t"))
        # Streamed results are appended as jobs complete, so the tail order is not guaranteed.
        self.assertEqual(sorted(int(row["job_id"]) for row in rows), [1, 2, 3, 4])
        self.assertEqual(tracker.completed_jobs, 4)
        self.assertEqual(dict(tracker.by_solver["alpha"]), {"sat": 4})

    def test_resume_repairs_missing_final_newline_before_append(self) -> None:
        run_dir = self._make_run("no-newline", completed=0, total=1)
        results_path = run_dir / "results.tsv"
        results_path.write_bytes(results_path.read_bytes().rstrip(b"\r\n"))
        plan = _prepare_resume(parse_args(["--resume", "--output", str(run_dir)]))
        run_queue(
            plan.remaining,
            plan.specs,
            timeout=plan.timeout,
            workers=1,
            logs_dir=plan.logs_dir,
            log_policy=plan.log,
            checkpoint_every=1,
            tracker=None,
            results_path=results_path,
            append_results=True,
        )
        with results_path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle, delimiter="\t"))
        self.assertEqual([row["job_id"] for row in rows], ["1"])

    def test_resume_main_reports_total_counts_and_releases_pid(self) -> None:
        run_dir = self._make_run("main-resume", completed=2)
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = main(["--resume", "--output", str(run_dir), "--jobs", "2"])
        self.assertEqual(exit_code, 0)
        self.assertIn("files=4 solvers=alpha pairs=4", stdout.getvalue())
        self.assertIn("complete pairs=4 sat=4", stdout.getvalue())
        self.assertFalse((run_dir / ".run.pid").exists())
        history = [json.loads(line) for line in (run_dir / "resume_history.jsonl").read_text(encoding="utf-8").splitlines()]
        self.assertEqual([item["event"] for item in history], ["resume_started", "resume_finished"])
        self.assertEqual(history[-1]["status"], "complete")

    def test_run_lock_rejects_a_second_controller(self) -> None:
        output = self.root / "results" / "locked"
        first, second = _RunLock(output), _RunLock(output)
        first.acquire()
        try:
            with self.assertRaisesRegex(ValueError, "another batch controller"):
                second.acquire()
        finally:
            first.release()
        second.acquire()
        second.release()

    def test_interrupt_drains_in_flight_results_without_scheduling_more(self) -> None:
        import smtbatch.run as run_module

        output = self.root / "results" / "cancel.tsv"
        jobs = [JobSpec(index, "alpha", self.formulas[index - 1]) for index in range(1, 4)]

        def slow_result(_spec, job, _timeout, _outer_timeout):
            time.sleep(0.15)
            return JobResult(job, 0.15, "sat", 0, "sat\n")

        timer = threading.Timer(0.03, lambda: os.kill(os.getpid(), signal.SIGINT))
        with mock.patch.object(run_module, "run_job", side_effect=slow_result):
            timer.start()
            try:
                with self.assertRaises(KeyboardInterrupt):
                    run_queue(
                        jobs,
                        {"alpha": run_module.load_config().solvers["alpha"]},
                        timeout=1,
                        workers=2,
                        logs_dir=self.root / "logs",
                        log_policy="none",
                        checkpoint_every=1,
                        tracker=None,
                        results_path=output,
                    )
            finally:
                timer.cancel()
        with output.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle, delimiter="\t"))
        self.assertEqual(sorted(int(row["job_id"]) for row in rows), [1, 2])


if __name__ == "__main__":
    unittest.main()
