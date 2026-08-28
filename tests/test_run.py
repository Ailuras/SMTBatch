from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import signal
import subprocess
import tempfile
import threading
import time
import unittest
from collections import Counter
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from smtbatch.config import SolverSpec, load_config, validate_target_branch

from smtbatch.run import (
    ProgressTracker,
    JobResult,
    RUN_CONTROL_PAUSED,
    RUN_CONTROL_RUNNING,
    _RunLock,
    _count_check_sat_files,
    _load_existing_results,
    _prepare_fresh,
    _prepare_resume,
    _results_writer_fields,
    load_files_from,
    parse_args,
    require_fresh_output,
    main,
    retain_job_log,
    run_queue,
    solver_artifacts,
    solver_bundle_hash,
    write_progress_snapshot,
    write_run_control,
    _main_locked,
)
from smtbatch.task import RESULT_FIELDS, JobSpec, is_timeout_exit, load_jobs
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
        _init_smtbatch_checkout(root)
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
        (run_dir / "jobs.tsv").write_text(
            jobs_tsv([(job_id, "alpha", self.formulas[job_id - 1], 1) for job_id in range(1, total + 1)]),
            encoding="utf-8",
        )
        write_results(
            run_dir / "results.tsv",
            [
                result_row(job_id=job_id, solver="alpha", file=self.formulas[job_id - 1])
                for job_id in range(1, completed + 1)
            ],
        )
        artifacts = solver_artifacts(self.binary)
        artifacts_json = json.dumps(artifacts, separators=(",", ":"), sort_keys=True)
        bundle_hash = solver_bundle_hash(artifacts)
        (run_dir / "metadata.txt").write_text(
            "format=pair-queue-v1\n"
            "solvers=alpha\n"
            "timeout=10\n"
            "jobs=2\n"
            "log=all\n"
            f"solver_config={self.config_path}\n"
            f"alpha_solver_binary={self.binary}\n"
            f"alpha_solver_binary_sha256={hashlib.sha256(self.binary.read_bytes()).hexdigest()}\n"
            f"alpha_solver_artifacts_json={artifacts_json}\n"
            f"alpha_solver_bundle_sha256={bundle_hash}\n"
            "alpha_solver_bundle_schema=linked-artifacts-v1\n"
            'alpha_solver_command_json=["{binary}","{input}"]\n',
            encoding="utf-8",
        )
        return run_dir

    def test_load_existing_results_counts_partial_results(self) -> None:
        run_dir = self._make_run("partial", completed=2)
        completed, outcomes, by_solver, solved_seconds, par2_seconds, incremental, by_solver_incremental = (
            _load_existing_results(run_dir / "results.tsv", load_jobs(run_dir / "jobs.tsv"), 10)
        )
        self.assertEqual(completed, {1, 2})
        self.assertEqual(outcomes, Counter({"sat": 2}))
        self.assertEqual(dict(by_solver["alpha"]), {"sat": 2})
        self.assertEqual(solved_seconds, {"alpha": 1.0})
        self.assertEqual(par2_seconds, {"alpha": 1.0})
        self.assertEqual(incremental.file_complete, 2)
        self.assertEqual(by_solver_incremental["alpha"].file_complete, 2)
        self.assertEqual(incremental.file_partial, 0)

    def test_resume_rejects_old_results_header(self) -> None:
        run_dir = self._make_run("legacy-header", completed=1)
        (run_dir / "results.tsv").write_text(
            "job_id\tsolver\tfile\tresult\ttime\tcode\toutput_path\n"
            f"1\talpha\t{self.formulas[0]}\tsat\t0.5\t0\t\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "invalid results header"):
            _results_writer_fields(run_dir / "results.tsv", True)
        with self.assertRaisesRegex(ValueError, "invalid results header"):
            _load_existing_results(run_dir / "results.tsv", load_jobs(run_dir / "jobs.tsv"), 10)

    def test_resume_rejects_old_jobs_header(self) -> None:
        run_dir = self._make_run("legacy-jobs", completed=1)
        (run_dir / "jobs.tsv").write_text(
            "job_id\tsolver\tfile\n"
            f"1\talpha\t{self.formulas[0]}\n"
            f"2\talpha\t{self.formulas[1]}\n"
            f"3\talpha\t{self.formulas[2]}\n"
            f"4\talpha\t{self.formulas[3]}\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "invalid jobs header"):
            load_jobs(run_dir / "jobs.tsv")

    def test_retain_job_log_follows_policy(self) -> None:
        logs = self.root / "logs"
        logs.mkdir()
        path = logs / "job_0000001.alpha.out"
        path.write_text("sat\n", encoding="utf-8")
        self.assertIsNone(retain_job_log(path, "fail", "sat"))
        self.assertFalse(path.exists())
        path.write_text("sat\n", encoding="utf-8")
        self.assertEqual(retain_job_log(path, "fail", "timeout"), path)
        self.assertTrue(path.is_file())
        self.assertEqual(retain_job_log(path, "all", "sat"), path)
        self.assertIsNone(retain_job_log(path, "none", "error"))

    def test_load_existing_results_rejects_truncated_or_mismatched_rows(self) -> None:
        run_dir = self._make_run("corrupt", completed=0)
        jobs = load_jobs(run_dir / "jobs.tsv")
        header = "\t".join(RESULT_FIELDS) + "\n"
        for row in (
            "1\talpha\n",
            "\t".join(
                result_row(job_id=1, solver="beta", file=self.formulas[0]).get(field, "")
                for field in RESULT_FIELDS
            )
            + "\n",
        ):
            (run_dir / "results.tsv").write_text(header + row, encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "malformed|does not match"):
                _load_existing_results(run_dir / "results.tsv", jobs, 10)

    def test_progress_tracker_reports_average_solved_time_and_par2(self) -> None:
        tracker = ProgressTracker(self.root / "results" / "metrics", ("alpha",), 2, 1.0, 5, timeout=10)
        tracker.finish(JobResult(JobSpec(1, "alpha", self.formulas[0]), 2.0, "sat", 0, ""), None)
        tracker.finish(JobResult(JobSpec(2, "alpha", self.formulas[1]), 4.0, "error", 1, ""), None)
        snapshot = tracker.snapshot()
        self.assertEqual(
            snapshot["performance"],
            {
                "completed_jobs": 2,
                "solved_jobs": 1,
                "average_solved_seconds": 2.0,
                "par2_seconds": 11.0,
            },
        )
        self.assertEqual(snapshot["by_solver_performance"], {"alpha": snapshot["performance"]})

    def test_progress_tracker_uses_queue_expected_as_coverage_denominator(self) -> None:
        tracker = ProgressTracker(self.root / "results" / "coverage", ("alpha",), 2, 1.0, 5, timeout=10)
        tracker.set_queue_expected(
            [JobSpec(1, "alpha", self.formulas[0], 3), JobSpec(2, "alpha", self.formulas[1], 7)]
        )
        tracker.finish(
            JobResult(JobSpec(1, "alpha", self.formulas[0], 3), 1.0, "sat", 0, "", sat=1, expected=3, complete=True),
            None,
        )
        snapshot = tracker.snapshot()
        incremental = snapshot["incremental"]
        assert isinstance(incremental, dict)
        self.assertEqual(incremental["expected"], 10)
        self.assertEqual(incremental["sat"], 1)

    def test_prepare_resume_selects_only_missing_jobs(self) -> None:
        run_dir = self._make_run("partial", completed=2)
        args = parse_args(["--resume", "--output", str(run_dir), "--jobs", "4", "--log", "all"])
        plan = _prepare_resume(args)
        self.assertEqual([job.job_id for job in plan.remaining], [3, 4])
        self.assertEqual(plan.completed_before, 2)
        self.assertEqual(plan.pair_count, 4)
        self.assertEqual(plan.timeout, 10.0)
        self.assertEqual(plan.log, "all")
        self.assertEqual(plan.solved_seconds_before, {"alpha": 1.0})
        self.assertEqual(plan.par2_seconds_before, {"alpha": 1.0})
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

    def test_prepare_resume_rejects_linked_artifact_bundle_drift(self) -> None:
        run_dir = self._make_run("bundle-drift", completed=1)
        changed_artifacts = {
            **solver_artifacts(self.binary),
            "libchanged.so": {"path": "/libchanged.so", "sha256": "changed"},
        }
        with mock.patch("smtbatch.run.solver_artifacts", return_value=changed_artifacts):
            with self.assertRaisesRegex(ValueError, "linked-artifact bundle changed"):
                _prepare_resume(parse_args(["--resume", "--output", str(run_dir)]))

    def test_prepare_fresh_records_linked_artifact_bundle(self) -> None:
        output = self.root / "results" / "fresh-provenance"
        plan = _prepare_fresh(
            parse_args(
                [
                    "--solver",
                    "alpha",
                    "--input",
                    str(self.root / "inputs"),
                    "--output",
                    str(output),
                ]
            )
        )
        metadata = (output / "metadata.txt").read_text(encoding="utf-8")
        artifacts = solver_artifacts(self.binary)
        self.assertIn(
            f"alpha_solver_bundle_sha256={solver_bundle_hash(artifacts)}\n",
            metadata,
        )
        self.assertIn("alpha_solver_artifacts_json=", metadata)
        self.assertIn("alpha_solver_bundle_schema=linked-artifacts-v1\n", metadata)
        self.assertIn("target_branch=main\n", metadata)
        self.assertIn("smtbatch_branch=main\n", metadata)
        self.assertIn("query_events=yes\n", metadata)
        self.assertIn(
            f"solver_config_sha256={hashlib.sha256(self.config_path.read_bytes()).hexdigest()}\n",
            metadata,
        )
        self.assertIn("runner_script_sha256=", metadata)
        self.assertIn("platform=", metadata)
        self.assertIn("python_version=", metadata)
        self.assertTrue(plan.query_events)
        self.assertEqual(plan.pair_count, 4)
        jobs = load_jobs(output / "jobs.tsv")
        self.assertEqual([job.expected for job in jobs], [1, 1, 1, 1])

    def test_prepare_fresh_refuses_to_overwrite_prior_run(self) -> None:
        output = self.root / "results" / "immutable-run"
        output.mkdir()
        metadata = output / "metadata.txt"
        metadata.write_text("sentinel=keep\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "refusing to overwrite existing run data"):
            require_fresh_output(output)
        self.assertEqual("sentinel=keep\n", metadata.read_text(encoding="utf-8"))

    def test_main_refuses_before_mutating_prior_progress(self) -> None:
        output = self.root / "results" / "immutable-main"
        output.mkdir()
        (output / "metadata.txt").write_text("sentinel=metadata\n", encoding="utf-8")
        progress = output / "progress.json"
        progress.write_text('{"sentinel":"progress"}\n', encoding="utf-8")
        exit_code = main(["--output", str(output)])
        self.assertEqual(2, exit_code)
        self.assertEqual(
            '{"sentinel":"progress"}\n',
            progress.read_text(encoding="utf-8"),
        )

    def test_files_from_preserves_order_resolves_relative_paths_and_deduplicates(self) -> None:
        manifest = self.root / "probe.txt"
        manifest.write_text(
            "# fixed probe\ninputs/f2.smt2\ninputs/f0.smt2\ninputs/f2.smt2\n",
            encoding="utf-8",
        )
        self.assertEqual(load_files_from(manifest, 0), [self.formulas[2], self.formulas[0]])
        self.assertEqual(load_files_from(manifest, 1), [self.formulas[2]])

        output = self.root / "results" / "files-from"
        plan = _prepare_fresh(
            parse_args(
                [
                    "--solver",
                    "alpha",
                    "--files-from",
                    str(manifest),
                    "--output",
                    str(output),
                ]
            )
        )
        self.assertEqual([job.file_path for job in plan.jobs], [self.formulas[2], self.formulas[0]])
        metadata = (output / "metadata.txt").read_text(encoding="utf-8")
        self.assertIn(f"files_from={manifest}\n", metadata)
        self.assertIn(f"files_from_sha256={hashlib.sha256(manifest.read_bytes()).hexdigest()}\n", metadata)

    def test_prepare_fresh_rejects_input_with_files_from(self) -> None:
        manifest = self.root / "probe.txt"
        manifest.write_text(f"{self.formulas[0]}\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "cannot be combined"):
            _prepare_fresh(
                parse_args(
                    [
                        "--solver",
                        "alpha",
                        "--input",
                        str(self.root / "inputs"),
                        "--files-from",
                        str(manifest),
                    ]
                )
            )

    def test_query_events_can_be_disabled_for_a_new_run(self) -> None:
        output = self.root / "results" / "no-query-events"
        plan = _prepare_fresh(
            parse_args(
                [
                    "--solver",
                    "alpha",
                    "--input",
                    str(self.root / "inputs"),
                    "--output",
                    str(output),
                    "--no-query-events",
                ]
            )
        )
        self.assertFalse(plan.query_events)
        self.assertIn("query_events=no\n", (output / "metadata.txt").read_text(encoding="utf-8"))

    def test_prepare_fresh_rejects_wrong_smtbatch_branch(self) -> None:
        self.config_path.write_text(
            self.config_path.read_text(encoding="utf-8").replace(
                '[defaults]\ninputs = "inputs"\nresults = "results"\n',
                '[defaults]\ninputs = "inputs"\nresults = "results"\ntarget_branch = "feat/incremental"\n',
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(RuntimeError, "wrong SMTBatch branch"):
            _prepare_fresh(
                parse_args(
                    [
                        "--solver",
                        "alpha",
                        "--input",
                        str(self.root / "inputs"),
                        "--output",
                        str(self.root / "results" / "wrong-branch"),
                    ]
                )
            )
        config = load_config(self.root)
        self.assertEqual(config.target_branch, "feat/incremental")
        with self.assertRaisesRegex(RuntimeError, "expected 'feat/incremental'"):
            validate_target_branch(config)

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
        for extra in (["--input", "inputs"], ["--files-from", "probe.txt"], ["--solver", "alpha"]):
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
        tracker = ProgressTracker(run_dir, plan.solvers, plan.pair_count, 1.0, 5, timeout=plan.timeout)
        tracker.completed_jobs = plan.completed_before
        tracker.outcomes = Counter(plan.outcomes_before)
        tracker.by_solver = {solver: Counter(plan.by_solver_before.get(solver, ())) for solver in plan.solvers}
        tracker.solved_seconds = dict(plan.solved_seconds_before)
        tracker.par2_seconds = dict(plan.par2_seconds_before)
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
        self.assertEqual(tracker.snapshot()["performance"]["solved_jobs"], 4)

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

        started = threading.Event()

        def slow_result(_spec, job, _timeout, _outer_timeout, **_kwargs):
            started.set()
            time.sleep(0.15)
            return JobResult(job, 0.15, "sat", 0, "sat\n")
            time.sleep(0.15)
            return JobResult(job, 0.15, "sat", 0, "sat\n")

        def interrupt() -> None:
            self.assertTrue(started.wait(2))
            os.kill(os.getpid(), signal.SIGINT)

        worker = threading.Thread(target=interrupt, daemon=True)
        with mock.patch.object(run_module, "run_job", side_effect=slow_result):
            worker.start()
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
                worker.join(1)
        with output.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle, delimiter="\t"))
        self.assertEqual(sorted(int(row["job_id"]) for row in rows), [1, 2])

    def test_pause_control_stops_fill_and_keeps_in_flight(self) -> None:
        import smtbatch.run as run_module

        output = self.root / "results" / "pause-fill"
        output.mkdir()
        jobs = [JobSpec(index, "alpha", self.formulas[index - 1]) for index in range(1, 4)]
        started = threading.Event()

        def slow_result(_spec, job, _timeout, _outer_timeout, **_kwargs):
            started.set()
            time.sleep(0.15)
            return JobResult(job, 0.15, "sat", 0, "sat\n")

        def pause() -> None:
            self.assertTrue(started.wait(2))
            write_run_control(output, RUN_CONTROL_PAUSED)

        worker = threading.Thread(target=pause, daemon=True)
        with mock.patch.object(run_module, "run_job", side_effect=slow_result):
            worker.start()
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
                        results_path=output / "results.tsv",
                    )
            finally:
                worker.join(1)
        with (output / "results.tsv").open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle, delimiter="\t"))
        self.assertEqual(sorted(int(row["job_id"]) for row in rows), [1, 2])

    def test_unpause_control_resumes_fill_in_same_process(self) -> None:
        import smtbatch.run as run_module

        output = self.root / "results" / "pause-resume"
        output.mkdir()
        jobs = [JobSpec(index, "alpha", self.formulas[index - 1]) for index in range(1, 4)]
        started = threading.Event()

        def slow_result(_spec, job, _timeout, _outer_timeout, **_kwargs):
            started.set()
            time.sleep(0.2)
            return JobResult(job, 0.2, "sat", 0, "sat\n")

        def pause_then_resume() -> None:
            self.assertTrue(started.wait(2))
            write_run_control(output, RUN_CONTROL_PAUSED)
            time.sleep(0.05)
            write_run_control(output, RUN_CONTROL_RUNNING)

        worker = threading.Thread(target=pause_then_resume, daemon=True)
        with mock.patch.object(run_module, "run_job", side_effect=slow_result):
            worker.start()
            try:
                run_queue(
                    jobs,
                    {"alpha": run_module.load_config().solvers["alpha"]},
                    timeout=1,
                    workers=2,
                    logs_dir=self.root / "logs",
                    log_policy="none",
                    checkpoint_every=1,
                    tracker=None,
                    results_path=output / "results.tsv",
                )
            finally:
                worker.join(1)
        with (output / "results.tsv").open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle, delimiter="\t"))
        self.assertEqual(sorted(int(row["job_id"]) for row in rows), [1, 2, 3])

    def test_pause_with_empty_inflight_exits_interrupted(self) -> None:
        import smtbatch.run as run_module

        output = self.root / "results" / "pause-empty.tsv"
        jobs = [JobSpec(index, "alpha", self.formulas[index - 1]) for index in range(1, 4)]
        with mock.patch.object(run_module, "read_run_control", return_value=RUN_CONTROL_PAUSED):
            with mock.patch.object(run_module, "run_job", side_effect=AssertionError("paused queue must not submit jobs")):
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
        with output.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle, delimiter="\t"))
        self.assertEqual(rows, [])

    def test_run_queue_streams_solver_logs(self) -> None:
        results_path = self.root / "results" / "logged.tsv"
        logs_dir = self.root / "results" / "logged-logs"
        jobs = [JobSpec(1, "alpha", self.formulas[0], 1)]
        outcomes = run_queue(
            jobs,
            {"alpha": load_config().solvers["alpha"]},
            timeout=5,
            workers=1,
            logs_dir=logs_dir,
            log_policy="all",
            checkpoint_every=1,
            tracker=None,
            results_path=results_path,
        )
        self.assertEqual(dict(outcomes), {"sat": 1})
        log_path = logs_dir / "job_0000001.alpha.out"
        self.assertTrue(log_path.is_file())
        self.assertIn("sat", log_path.read_text(encoding="utf-8"))
        with results_path.open("r", encoding="utf-8", newline="") as handle:
            row = next(csv.DictReader(handle, delimiter="\t"))
        self.assertEqual(row["output_path"], str(log_path))
        self.assertEqual(row["expected"], "1")

    def test_run_queue_keeps_query_events_when_solver_logs_are_disabled(self) -> None:
        results_path = self.root / "results" / "event-results.tsv"
        events_dir = self.root / "results" / "events"
        jobs = [JobSpec(1, "alpha", self.formulas[0], 1)]
        run_queue(
            jobs,
            {"alpha": load_config().solvers["alpha"]},
            timeout=5,
            workers=1,
            logs_dir=self.root / "unused-logs",
            events_dir=events_dir,
            log_policy="none",
            checkpoint_every=1,
            tracker=None,
            results_path=results_path,
        )
        event_path = events_dir / "job_0000001.alpha.tsv"
        self.assertTrue(event_path.is_file())
        with event_path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle, delimiter="\t"))
        self.assertEqual([(row["ordinal"], row["outcome"], row["source"]) for row in rows], [("1", "sat", "solver")])

    def test_run_queue_provides_isolated_incsmt_event_environment(self) -> None:
        self.binary.write_text(
            "#!/bin/sh\n"
            "printf '%s\\n' \"$INCSMT_SESSION_ID\" > \"$INCSMT_EVENT_FILE\"\n"
            "echo sat\n",
            encoding="utf-8",
        )
        self.binary.chmod(0o755)
        results_path = self.root / "results" / "obe-event-results.tsv"
        events_dir = self.root / "results" / "obe-events"
        job = JobSpec(1, "alpha", self.formulas[0], 1)
        run_queue(
            [job],
            {"alpha": load_config().solvers["alpha"]},
            timeout=5,
            workers=1,
            logs_dir=self.root / "unused-logs",
            events_dir=events_dir,
            log_policy="none",
            checkpoint_every=1,
            tracker=None,
            results_path=results_path,
        )
        sidecar = events_dir / "job_0000001.alpha.obe.jsonl"
        self.assertTrue(sidecar.is_file())
        self.assertEqual(
            f"{(events_dir / 'job_0000001.alpha').resolve()}::{self.formulas[0].resolve()}",
            sidecar.read_text(encoding="utf-8").strip(),
        )

    def test_run_queue_session_ids_are_unique_across_arms(self) -> None:
        self.binary.write_text(
            "#!/bin/sh\n"
            "printf '%s\\n' \"$INCSMT_SESSION_ID\" > \"$INCSMT_EVENT_FILE\"\n"
            "echo sat\n",
            encoding="utf-8",
        )
        self.binary.chmod(0o755)
        results_path = self.root / "results" / "multi-arm-event-results.tsv"
        events_dir = self.root / "results" / "multi-arm-events"
        alpha = load_config().solvers["alpha"]
        specs = {
            "alpha": alpha,
            "beta": SolverSpec(
                name="beta",
                binary=alpha.binary,
                command=alpha.command,
                version_args=alpha.version_args,
                label="beta",
            ),
        }
        jobs = [
            JobSpec(1, "alpha", self.formulas[0], 1),
            JobSpec(2, "beta", self.formulas[0], 1),
        ]
        run_queue(
            jobs,
            specs,
            timeout=5,
            workers=1,
            logs_dir=self.root / "unused-logs",
            events_dir=events_dir,
            log_policy="none",
            checkpoint_every=1,
            tracker=None,
            results_path=results_path,
        )
        sessions = {
            path.read_text(encoding="utf-8").strip()
            for path in events_dir.glob("*.obe.jsonl")
        }
        self.assertEqual(len(sessions), 2)

    def test_count_check_sat_files_matches_per_file_counts(self) -> None:
        from smtbatch.task import count_check_sat

        paths = []
        for index in range(3):
            path = self.root / "inputs" / f"count-{index}.smt2"
            path.write_text("(check-sat)\n" * (index + 1), encoding="utf-8")
            paths.append(path)
        counted = _count_check_sat_files(paths)
        self.assertEqual(counted, {path: count_check_sat(path) for path in paths})

    def test_count_check_sat_files_caps_process_workers(self) -> None:
        paths = []
        for index in range(40):
            path = self.root / "inputs" / f"pool-{index}.smt2"
            path.write_text("(check-sat)\n", encoding="utf-8")
            paths.append(path)
        fake_pool = mock.MagicMock()
        fake_pool.__enter__.return_value.map.return_value = [1] * len(paths)
        fake_pool.__exit__.return_value = False
        with (
            mock.patch("smtbatch.run.os.cpu_count", return_value=128),
            mock.patch("smtbatch.run.concurrent.futures.ProcessPoolExecutor", return_value=fake_pool) as pool,
        ):
            counted = _count_check_sat_files(paths)
        self.assertEqual(pool.call_args.kwargs["max_workers"], 32)
        self.assertEqual(counted, {path: 1 for path in paths})

    def test_resume_startup_preserves_existing_progress(self) -> None:
        run_dir = self._make_run("resume-keep-progress", completed=2)
        (run_dir / "progress.json").write_text(
            json.dumps(
                {
                    "status": "interrupted",
                    "completed_jobs": 2,
                    "total_jobs": 4,
                    "startup_note": "old",
                }
            ),
            encoding="utf-8",
        )
        with (
            mock.patch("smtbatch.run._prepare_resume", side_effect=ValueError("broken resume")),
            redirect_stdout(io.StringIO()),
        ):
            code = _main_locked(parse_args(["--resume", "--output", str(run_dir)]))
        self.assertEqual(code, 2)
        payload = json.loads((run_dir / "progress.json").read_text(encoding="utf-8"))
        self.assertEqual(payload["status"], "interrupted")
        self.assertEqual(payload["completed_jobs"], 2)

    def test_write_progress_snapshot_creates_starting_card(self) -> None:
        output = self.root / "results" / "startup"
        snapshot = write_progress_snapshot(
            output,
            status="starting",
            phase="count",
            startup_note="Counting check-sat commands: 2/3 files",
            selected_files=3,
            counted_files=2,
        )
        payload = json.loads((output / "progress.json").read_text(encoding="utf-8"))
        self.assertEqual(payload["status"], "starting")
        self.assertEqual(payload["phase"], "count")
        self.assertEqual(snapshot["counted_files"], 2)
        self.assertFalse((output / "jobs.tsv").exists())


class TimeoutClassificationTests(unittest.TestCase):
    def test_gnu_timeout_124_is_always_timeout(self) -> None:
        self.assertTrue(is_timeout_exit(124, duration_sec=10.0, timeout=10.0))
        self.assertTrue(is_timeout_exit(124, duration_sec=1.0, timeout=10.0))

    def test_kill_after_sigkill_at_limit_is_timeout(self) -> None:
        self.assertTrue(is_timeout_exit(-9, 302.5, 300.0, "ForteSMT interrupted by SIGTERM.\n"))
        self.assertTrue(is_timeout_exit(-15, 301.0, 300.0))
        self.assertFalse(is_timeout_exit(-9, 5.0, 300.0))
        self.assertFalse(is_timeout_exit(137, 1.0, 10.0))
        self.assertTrue(is_timeout_exit(137, 10.0, 10.0))

    def test_abort_requires_the_internal_timeout_marker(self) -> None:
        self.assertTrue(is_timeout_exit(-6, 300.2, 300.0, "ForteSMT interrupted by timeout.\n"))
        self.assertTrue(is_timeout_exit(134, 0.2, 300.0, "ForteSMT interrupted by timeout.\n"))
        self.assertFalse(is_timeout_exit(-6, 300.2, 300.0, ""))
        self.assertFalse(is_timeout_exit(134, 300.2, 300.0, "Fatal assertion"))


class WatchdogPlaceholderTests(unittest.TestCase):
    def test_timeout_watchdog_is_ten_seconds_after_the_job_limit(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            binary = root / "alpha"
            binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            binary.chmod(0o755)
            spec = SolverSpec(
                "alpha",
                binary,
                ("{timeout}", "{timeout_ms}", "{timeout_watchdog}", "{binary}", "{input}"),
                ("--version",),
            )
            rendered = spec.render(root / "f.smt2", 1200)
            self.assertEqual(rendered[0], "1200")
            self.assertEqual(rendered[1], "1200000")
            self.assertEqual(rendered[2], "1210")


if __name__ == "__main__":
    unittest.main()
