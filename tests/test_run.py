import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from smtbatch import cli, reduce
from smtbatch.config import load_config, validate_target_branch


class ReductionFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        (self.root / "studies").mkdir()
        (self.root / "benchmarks").mkdir()
        (self.root / "benchmarks" / "case.smt2").write_text("(check-sat)\n", encoding="utf-8")
        (self.root / "benchmarks" / "database.json").write_text(
            json.dumps({"case.smt2": {"match": "exitcode", "binary": "fixture"}}),
            encoding="utf-8",
        )
        (self.root / ".gitignore").write_text("results/\nSMTBatch/\n", encoding="utf-8")
        self.smtbatch_root = self.root / "SMTBatch"
        self.smtbatch_root.mkdir()
        (self.smtbatch_root / "branch-marker").write_text(
            "reduction\n", encoding="utf-8"
        )
        self.config_path = self.root / "smtbatch.toml"
        self.config_path.write_text(
            """[defaults]
results = "results"
port = 8001
target_branch = "feat/reduction"

[benchmark_catalog]
database = "benchmarks/database.json"
inputs = "benchmarks"
template = "studies/study.json"

[benchmark_categories.compact]
label = "Compact fixture"
description = "Fixture input size"
min_bytes = 0
max_bytes = 100

[reducers.r1]
label = "Reducer one"
command = ["/bin/true", "--r1", "{input}", "{output}", "{predicate}"]

[reducers.r2]
label = "Reducer two"
command = ["/bin/true", "--r2", "{input}", "{output}", "{predicate}"]
""",
            encoding="utf-8",
        )
        self.study_path = self.root / "studies" / "study.json"
        self.study_path.write_text(
            json.dumps({
                "schema_version": 2,
                "kind": "reduction",
                "study_id": "fixture",
                "root": "..",
                "execution": {"outer_jobs": 2, "schedule": "strict-wave"},
                "predicate_wrapper": {
                    "command": [
                        "/bin/true", "{journal}", "{phase}", "{match_args}", "{command}"
                    ],
                    "env": {},
                },
                "benchmarks": [{
                    "id": "case",
                    "input": "benchmarks/case.smt2",
                    "family": "fixture",
                    "theory": "core",
                    "predicate_mode": "exit-code",
                    "solver": {"name": "benchmark-owned"},
                    "predicate": {"command": ["/bin/true"], "match": {}},
                }],
                "reducers": ["r1", "r2"],
                "repeats": 2,
                "limits": {
                    "trial_wall_sec": 30,
                    "predicate_timeout_sec": 2,
                    "memory_mb": 128,
                    "preflight_repeats": 1,
                    "verification_repeats": 1,
                    "termination_grace_sec": 1,
                    "analysis_horizon_sec": 30,
                },
                "comparisons": [["r1", "r2"]],
            }, indent=2),
            encoding="utf-8",
        )
        subprocess.run(
            ["git", "init", "-b", "feat/reduction"],
            cwd=self.smtbatch_root, check=True, stdout=subprocess.DEVNULL,
        )
        subprocess.run(
            ["git", "add", "branch-marker"],
            cwd=self.smtbatch_root, check=True,
        )
        subprocess.run(
            [
                "git", "-c", "user.name=Test", "-c", "user.email=test@example.com",
                "commit", "-m", "fixture SMTBatch",
            ],
            cwd=self.smtbatch_root, check=True, stdout=subprocess.DEVNULL,
        )
        subprocess.run(["git", "init", "-b", "main"], cwd=self.root, check=True, stdout=subprocess.DEVNULL)
        subprocess.run(["git", "add", "."], cwd=self.root, check=True)
        subprocess.run(
            ["git", "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "fixture"],
            cwd=self.root, check=True, stdout=subprocess.DEVNULL,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()


class ConfigTests(ReductionFixture):
    def test_config_exposes_black_box_reducer_commands(self) -> None:
        config = load_config(self.root)
        self.assertEqual(config.target_branch, "feat/reduction")
        self.assertEqual(config.smtbatch_root, self.smtbatch_root.resolve())
        self.assertEqual(set(config.reducers), {"r1", "r2"})
        self.assertEqual(config.reducers["r1"].label, "Reducer one")
        self.assertEqual(config.reducers["r2"].command[0], "/bin/true")
        self.assertEqual(validate_target_branch(config), "feat/reduction")

    def test_legacy_solver_and_project_fields_are_rejected(self) -> None:
        self.config_path.write_text("[defaults]\nmode='reduction'\n[solvers.x]\nbinary='/bin/true'\n", encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "unknown top-level config tables"):
            load_config(self.root)
        self.config_path.write_text("[defaults]\ninputs='benchmarks'\nstudies='studies'\n", encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, r"unknown \[defaults\] fields"):
            load_config(self.root)

    def test_cli_exposes_only_serve_and_reduce(self) -> None:
        self.assertEqual(cli.COMMANDS, ("serve", "reduce"))


class PlanTests(ReductionFixture):
    def test_study_references_catalogue_and_subset_builds_strict_waves(self) -> None:
        study = reduce.load_study(self.study_path)
        self.assertEqual(study["reducers"], ["r1", "r2"])
        self.assertNotIn("tools", study)
        plan = reduce.build_plan(
            study, reducers=["r2"], timeout_seconds=45, outer_jobs=3
        )
        self.assertEqual([item["id"] for item in plan["reducers"]], ["r2"])
        self.assertEqual(plan["execution"], {"outer_jobs": 3, "schedule": "strict-wave"})
        self.assertEqual(plan["limits"]["trial_wall_sec"], 45)
        self.assertEqual(len(plan["jobs"]), 2)
        self.assertEqual({job["reducer_id"] for job in plan["jobs"]}, {"r2"})
        self.assertEqual(plan["wave_count"], 2)

    def test_selection_rejects_empty_unknown_and_disallowed_ids(self) -> None:
        study = reduce.load_study(self.study_path)
        with self.assertRaisesRegex(reduce.ReductionError, "at least one"):
            reduce.build_plan(study, reducers=[])
        with self.assertRaisesRegex(reduce.ReductionError, "unknown configured"):
            reduce.build_plan(study, reducers=["missing"])
        self.config_path.write_text(
            self.config_path.read_text(encoding="utf-8")
            + "\n[reducers.r3]\ncommand=['/bin/true','{input}','{output}','{predicate}']\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(reduce.ReductionError, "not allowed by study"):
            reduce.build_plan(study, config=load_config(self.root), reducers=["r3"])

    def test_prepare_freezes_selection_timeout_jobs_and_hashes(self) -> None:
        output = self.root / "results" / "prepared"
        plan = reduce.prepare(
            self.study_path, output, reducers=["r1"], timeout_seconds=61, outer_jobs=4
        )
        loaded = reduce.load_plan(output)
        self.assertEqual(plan["plan_sha256"], loaded["plan_sha256"])
        self.assertEqual([item["id"] for item in loaded["reducers"]], ["r1"])
        self.assertEqual(loaded["limits"]["trial_wall_sec"], 61)
        self.assertEqual(loaded["execution"]["outer_jobs"], 4)
        self.assertTrue((output / "plan.complete.json").is_file())
        self.assertEqual(len((output / "jobs.tsv").read_text().splitlines()), 3)
        self.assertNotIn("repository", plan)

    def test_prepare_rejects_different_options_for_existing_run(self) -> None:
        output = self.root / "results" / "prepared"
        reduce.prepare(self.study_path, output, reducers=["r1"], timeout_seconds=30, outer_jobs=1)
        with self.assertRaisesRegex(reduce.ReductionError, "non-empty output"):
            reduce.prepare(self.study_path, output, reducers=["r2"], timeout_seconds=30, outer_jobs=1)

    def test_run_lock_rejects_second_controller_and_cleans_pid(self) -> None:
        output = self.root / "results" / "locked"
        first, second = reduce._RunLock(output), reduce._RunLock(output)
        first.acquire()
        try:
            with self.assertRaisesRegex(ValueError, "another reduction controller"):
                second.acquire()
        finally:
            first.release()
        self.assertFalse((output / ".run.pid").exists())

    def test_process_timeout_cleans_the_process_group(self) -> None:
        result = reduce._run_command(
            ["/bin/sh", "-c", "sleep 2"], cwd=self.root,
            timeout=0.05, grace=0.05,
        )
        self.assertTrue(result["timed_out"])
        self.assertIsNotNone(result["returncode"])
        self.assertGreaterEqual(result["cleanup_wall_sec"], 0)

    def test_jsonl_reader_reports_unfinished_tail_without_crashing(self) -> None:
        path = self.root / "journal.jsonl"
        path.write_text('{"event":"start","call_id":"one"}\n{"event":', encoding="utf-8")
        events, truncated, error = reduce._read_jsonl(path, allow_partial=True)
        self.assertEqual(events, [{"event": "start", "call_id": "one"}])
        self.assertTrue(truncated)
        self.assertEqual(error, "")


if __name__ == "__main__":
    unittest.main()
