import http.client
import json
from http.server import ThreadingHTTPServer
from pathlib import Path
import subprocess
import threading
import unittest
from unittest import mock

from smtbatch import reduce
from smtbatch.reduction_serve import ReductionManager, handler_factory

from .test_run import ReductionFixture


class ManagerTests(ReductionFixture):
    def setUp(self) -> None:
        super().setUp()
        self.manager = ReductionManager(self.root)

    def test_catalog_exposes_reducer_defaults(self) -> None:
        catalog = self.manager.benchmark_catalog()
        self.assertTrue(catalog["valid"])
        self.assertEqual([item["id"] for item in catalog["reducers"]], ["r1", "r2"])
        self.assertEqual(catalog["default_timeout_seconds"], 30)
        self.assertEqual(catalog["default_outer_jobs"], 2)
        self.assertEqual(catalog["default_repeats"], 2)
        self.assertEqual(catalog["total_benchmarks"], 1)
        self.assertEqual(catalog["identity"]["stdout"], "fixture-identity-v1\n")
        identity_paths = [Path(item["path"]).resolve() for item in catalog["identity"]["assets"]]
        self.assertTrue(identity_paths)
        for path in identity_paths:
            path.relative_to(self.root.resolve())
        self.assertTrue(any(path.name == "identity.sh" for path in identity_paths))

    def test_catalog_identity_failure_blocks_launch(self) -> None:
        self.config_path.write_text(
            self.config_path.read_text(encoding="utf-8").replace(
                'identity_command = ["./identity.sh"]',
                'identity_command = ["/bin/false"]',
            ),
            encoding="utf-8",
        )
        manager = ReductionManager(self.root)
        catalog = manager.benchmark_catalog()
        self.assertFalse(catalog["valid"])
        self.assertIn("benchmark identity validation failed", catalog["error"])

    def test_catalog_run_samples_categories_and_freezes_four_parameters(self) -> None:
        with mock.patch.object(self.manager, "_launch", return_value={"run_id": "catalog-launched"}):
            result = self.manager.create_run({
                "categories": ["compact"], "reducers": ["r2"],
                "timeout_seconds": 90, "outer_jobs": 5,
                "max_files": 0, "repeats": 3,
            })
        self.assertEqual(result["run_id"], "catalog-launched")
        run_dirs = [path for path in (self.root / "results").iterdir() if path.is_dir()]
        self.assertEqual(len(run_dirs), 1)
        plan = reduce.load_plan(run_dirs[0])
        self.assertEqual(plan["selection"]["categories"], ["compact"])
        self.assertEqual(plan["selection"]["max_files"], 0)
        self.assertEqual(plan["selection"]["sampled_count"], 1)
        self.assertEqual(plan["repeats"], 3)
        self.assertEqual(plan["limits"]["trial_wall_sec"], 90)
        self.assertEqual(plan["execution"]["outer_jobs"], 5)
        self.assertEqual(len(plan["benchmarks"]), 1)
        self.assertEqual(len(plan["jobs"]), 3)
        last_run = self.manager.configuration()["last_run"]
        self.assertEqual(last_run["categories"], ["compact"])
        self.assertEqual(last_run["reducers"], ["r2"])
        self.assertEqual(last_run["timeout_seconds"], 90)
        self.assertEqual(last_run["outer_jobs"], 5)
        self.assertEqual(last_run["max_files"], 0)
        self.assertEqual(last_run["repeats"], 3)
        self.assertIn("saved_at", last_run)

    def test_create_run_validates_exact_request(self) -> None:
        invalid = [
            {"study_id": "fixture"},
            {"study_id": "fixture", "reducers": ["r1"], "timeout_seconds": 1, "outer_jobs": 1},
            {"categories": [], "reducers": ["r1"], "timeout_seconds": 1, "outer_jobs": 1, "max_files": 1, "repeats": 1},
            {"categories": ["missing"], "reducers": ["r1"], "timeout_seconds": 1, "outer_jobs": 1, "max_files": 1, "repeats": 1},
            {"categories": ["compact"], "reducers": ["r1"], "timeout_seconds": 0, "outer_jobs": 1, "max_files": 1, "repeats": 1},
            {"categories": ["compact"], "reducers": ["r1"], "timeout_seconds": 1, "outer_jobs": 0, "max_files": 1, "repeats": 1},
            {"categories": ["compact"], "reducers": ["r1"], "timeout_seconds": 1, "outer_jobs": 1, "max_files": -1, "repeats": 1},
        ]
        for body in invalid:
            with self.subTest(body=body), self.assertRaises(ValueError):
                self.manager.create_run(body)

    def test_project_branch_does_not_control_smtbatch_gate(self) -> None:
        subprocess.run(["git", "switch", "-c", "wrong"], cwd=self.root, check=True, stdout=subprocess.DEVNULL)
        self.manager._refresh_config()
        config = self.manager.configuration()
        self.assertTrue(config["can_launch"])
        self.assertEqual(config["smtbatch_branch"], "feat/reduction")
        self.assertTrue(self.manager.benchmark_catalog()["valid"])

    def test_wrong_smtbatch_branch_keeps_history_visible_but_blocks_mutations(self) -> None:
        subprocess.run(
            ["git", "switch", "-c", "wrong"],
            cwd=self.smtbatch_root, check=True, stdout=subprocess.DEVNULL,
        )
        self.manager._refresh_config()
        config = self.manager.configuration()
        self.assertFalse(config["can_launch"])
        self.assertIn("expected 'feat/reduction'", config["branch_error"])
        self.assertIn("SMTBatch", config["branch_error"])
        self.assertTrue(self.manager.benchmark_catalog()["valid"])
        with self.assertRaisesRegex(ValueError, "expected 'feat/reduction'"):
            self.manager.create_run({
                "categories": ["compact"], "reducers": ["r1"],
                "timeout_seconds": 30, "outer_jobs": 1, "max_files": 0, "repeats": 1,
            })

    def test_prepared_summary_and_cases_use_frozen_values(self) -> None:
        output = self.root / "results" / "prepared"
        reduce.prepare(self.study_path, output, reducers=["r1"], timeout_seconds=70, outer_jobs=3)
        summary = self.manager.summary("prepared")
        self.assertEqual(summary["status"], "prepared")
        self.assertIsNotNone(summary["created_at"])
        self.assertIsNotNone(summary["updated_at"])
        self.assertIsInstance(summary["elapsed_sec"], float)
        self.assertGreaterEqual(summary["elapsed_sec"], 0)
        self.assertEqual(summary["total_trials"], 2)
        self.assertEqual(set(summary["by_reducer"]), {"r1"})
        self.assertNotIn("reducers", summary)
        self.assertEqual(summary["outer_jobs"], 3)
        self.assertEqual(summary["limits"]["trial_wall_sec"], 70)
        self.assertNotIn("prepared_smtbatch_branch", summary)
        self.assertNotIn("repository", summary)
        cases = self.manager._case_rows("prepared", {"page": ["1"], "page_size": ["25"]})
        self.assertEqual(cases["total"], 1)
        self.assertEqual(cases["cases"][0]["status"], "pending")
        self.assertEqual(summary["comparisons"], [])
        self.assertIsNone(summary["by_reducer"]["r1"]["completed_avg_bytes"])

    def test_path_traversal_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.manager._run_dir("../outside")

    def test_case_pagination_does_not_parse_trajectories(self) -> None:
        rows = [{
            "case_id": f"case-{index}", "family": "f", "theory": "t",
            "predicate_mode": "exit", "planned": 1, "completed": 0,
            "verified": 0, "statuses": {}, "by_reducer": {"r1": {
                "planned": 1, "completed": 0, "verified": 0, "statuses": {},
                "predicate_calls": index, "accepted_moves": 1,
                "final_quality": [{"byte_count": index}],
            }},
        } for index in range(200)]
        with (
            mock.patch.object(self.manager, "_load_run", return_value=(self.root, {"study_id": "fixture", "jobs": []})),
            mock.patch.object(self.manager, "_progress", return_value={}),
            mock.patch("smtbatch.reduction_serve.reduction.case_rows", return_value=rows),
            mock.patch.object(self.manager, "_trajectory") as parse,
        ):
            result = self.manager._case_rows("fixture", {"page": ["2"], "page_size": ["5"]})
        self.assertEqual(result["total"], 200)
        self.assertEqual(len(result["cases"]), 5)
        self.assertEqual([row["case_id"] for row in result["cases"]], [f"case-{index}" for index in range(5, 10)])
        self.assertEqual(result["cases"][0]["realtime_calls"], 5)
        self.assertEqual(result["cases"][0]["current_quality"]["r1"]["byte_count"], 5)
        parse.assert_not_called()

    def test_summary_exposes_paired_comparisons_and_completed_size(self) -> None:
        output = self.root / "results" / "paired"
        reduce.prepare(
            self.study_path, output, reducers=["r1", "r2"], timeout_seconds=70, outer_jobs=1,
        )
        summary = self.manager.summary("paired")
        self.assertEqual(len(summary["comparisons"]), 1)
        pair = summary["comparisons"][0]
        self.assertEqual((pair["left"], pair["right"]), ("r1", "r2"))
        self.assertEqual(pair["left_label"], "Reducer one")
        self.assertEqual(pair["paired_cases"], 0)
        self.assertIsNone(summary["by_reducer"]["r1"]["completed_avg_bytes"])
        self.assertIn("completed_avg_bytes", summary["by_reducer"]["r1"])

    def test_case_list_status_and_winner_bytes(self) -> None:
        rows = [
            {
                "case_id": "done", "family": "f", "theory": "t", "predicate_mode": "exit",
                "input_bytes": 100, "planned": 2, "completed": 2,
                "statuses": {"completed": 2},
                "by_reducer": {
                    "r1": {
                        "label": "One", "planned": 1, "completed": 1,
                        "statuses": {"completed": 1},
                        "final_quality": [{"byte_count": 10}],
                    },
                    "r2": {
                        "label": "Two", "planned": 1, "completed": 1,
                        "statuses": {"completed": 1},
                        "final_quality": [{"byte_count": 40}],
                    },
                },
            },
            {
                "case_id": "cut", "family": "f", "theory": "t", "predicate_mode": "exit",
                "input_bytes": 100, "planned": 2, "completed": 2,
                "statuses": {"truncated": 2},
                "by_reducer": {
                    "r1": {
                        "label": "One", "planned": 1, "completed": 1,
                        "statuses": {"truncated": 1},
                        "final_quality": [{"byte_count": 80}],
                    },
                    "r2": {
                        "label": "Two", "planned": 1, "completed": 1,
                        "statuses": {"truncated": 1},
                        "final_quality": [{"byte_count": 90}],
                    },
                },
            },
        ]
        with (
            mock.patch.object(
                self.manager, "_load_run",
                return_value=(self.root, {"study_id": "fixture", "jobs": []}),
            ),
            mock.patch.object(self.manager, "_progress", return_value={}),
            mock.patch("smtbatch.reduction_serve.reduction.case_rows", return_value=rows),
        ):
            completed = self.manager._case_rows(
                "fixture", {"page": ["1"], "page_size": ["10"], "status": ["completed"]},
            )
            alias = self.manager._case_rows(
                "fixture", {"page": ["1"], "page_size": ["10"], "status": ["complete"]},
            )
            truncated = self.manager._case_rows(
                "fixture", {"page": ["1"], "page_size": ["10"], "status": ["truncated"]},
            )
            by_bytes = self.manager._case_rows(
                "fixture", {"page": ["1"], "page_size": ["10"], "sort": ["bytes"], "sort_dir": ["asc"]},
            )
        self.assertEqual([row["case_id"] for row in completed["cases"]], ["done"])
        self.assertEqual([row["case_id"] for row in alias["cases"]], ["done"])
        self.assertEqual(completed["cases"][0]["status"], "completed")
        self.assertEqual(completed["cases"][0]["best_bytes"], 10)
        self.assertEqual(completed["cases"][0]["winner_ids"], ["r1"])
        self.assertTrue(completed["cases"][0]["reducers"][0]["winner"])
        self.assertEqual([row["case_id"] for row in truncated["cases"]], ["cut"])
        self.assertEqual([row["case_id"] for row in by_bytes["cases"]], ["done", "cut"])

    def test_http_trajectory_keeps_a_light_chart_payload(self) -> None:
        plan = {
            "jobs": [{"job_id": "j1", "benchmark_id": "case-a"}],
            "study_id": "fixture",
        }
        payload = {
            "case": {"id": "case-a"},
            "trials": [{
                "logs": {"stdout": {"text": "noise"}},
                "trajectory": {"points": [
                    {"call_index": 0, "elapsed_sec": 0, "byte_count": 10, "accepted": True, "extra": 1},
                    {"call_index": 1, "elapsed_sec": 1, "byte_count": 10, "accepted": False},
                    {"call_index": 2, "elapsed_sec": 2, "byte_count": 4, "accepted": True, "mutator": "foo"},
                ]},
            }],
        }
        with (
            mock.patch.object(self.manager, "_load_run", return_value=(self.root, plan)),
            mock.patch(
                "smtbatch.reduction_serve.reduction.trajectory_for_case",
                return_value=payload,
            ) as parse,
        ):
            result = self.manager._trajectory("fixture", "case-a")
        self.assertEqual(parse.call_args.kwargs.get("include_logs"), False)
        self.assertEqual(parse.call_args.kwargs.get("verify_artifacts"), False)
        self.assertIs(parse.call_args.kwargs.get("plan"), plan)
        trial = result["trials"][0]
        self.assertNotIn("logs", trial)
        self.assertEqual(
            [point["call_index"] for point in trial["trajectory"]["points"]],
            [0, 2],
        )
        self.assertNotIn("extra", trial["trajectory"]["points"][0])


class HttpTests(ReductionFixture):
    def setUp(self) -> None:
        super().setUp()
        self.manager = ReductionManager(self.root)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler_factory(self.manager))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        super().tearDown()

    def request(self, method: str, path: str, body: object | None = None):
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port)
        payload = json.dumps(body) if body is not None else None
        headers = {"Content-Type": "application/json"} if payload is not None else {}
        connection.request(method, path, payload, headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, response.getheader("Content-Type"), data

    def test_main_page_is_launcher_monitor_only(self) -> None:
        status, _, data = self.request("GET", "/")
        html = data.decode("utf-8")
        self.assertEqual(status, 200)
        self.assertIn("Reduction Console", html)
        self.assertIn("Per-case timeout", html)
        self.assertIn("Parallel jobs", html)
        self.assertIn("Maximum benchmark files", html)
        self.assertIn("Repeats", html)
        self.assertIn("Controller connected", html)
        self.assertIn('id="categories"', html)
        self.assertIn('id="reducers"', html)
        self.assertIn("FREEZE RUN PLAN", html)
        self.assertIn("run-card", html)
        self.assertIn("run-card-body", html)
        self.assertIn("branch_error", html)
        self.assertIn("Launch gated", html)
        self.assertNotIn('<a class="run-card"', html)
        self.assertIn("/report", html)
        self.assertNotIn("Open report", html)
        self.assertIn("calculated workload", html)
        self.assertIn("Estimated completion", html)
        self.assertIn("Worst-case completion", html)
        self.assertIn("/api/catalog", html)
        self.assertNotIn("/api/studies", html)
        self.assertIn("run.created_at", html)
        self.assertIn("run.elapsed_sec", html)
        self.assertIn("run.by_reducer", html)
        self.assertNotIn("run.started_at", html)
        self.assertNotIn("run.reducers", html)
        self.assertNotIn("Candidate pool", html)
        self.assertIn("0 = all selected cases", html)
        self.assertIn("last_run", html)
        self.assertNotIn("Read-only execution facts", html)
        self.assertNotIn("Reduction / Observation", html)
        self.assertNotIn("Study conditions", html)
        self.assertNotIn("Cactus", html)
        self.assertNotIn("PAR-2", html)

    def test_report_is_a_separate_route_with_trajectory_charts(self) -> None:
        status, _, data = self.request("GET", "/runs/anything/report")
        html = data.decode("utf-8")
        self.assertEqual(status, 200)
        self.assertIn("Size over time", html)
        self.assertIn("Accepted moves over time", html)
        self.assertIn("case-layout", html)
        self.assertIn("trajSeq", html)
        self.assertIn("state.summary.live", html)
        self.assertIn("Paired comparisons", html)
        self.assertIn("completed_avg_bytes", html)
        self.assertIn('data-sort="bytes"', html)
        self.assertIn("case-reducers", html)
        self.assertIn("truncated", html)
        self.assertIn("invalid", html)
        self.assertNotIn("TRIAL_BUCKET", html)
        self.assertNotIn("timeout_verified", html)
        self.assertNotIn("String(s.categories||[]).join", html)
        self.assertNotIn("Reduction / Observation", html)

    def test_catalog_api_exposes_branch_gate_and_catalogue(self) -> None:
        status, _, data = self.request("GET", "/api/catalog")
        payload = json.loads(data)
        self.assertEqual(status, 200)
        self.assertTrue(payload["config"]["can_launch"])
        self.assertEqual(payload["config"]["smtbatch_branch"], "feat/reduction")
        self.assertNotIn("current_branch", payload["config"])
        self.assertEqual({item["id"] for item in payload["config"]["reducers"]}, {"r1", "r2"})
        self.assertTrue(payload["catalog"]["valid"])
        self.assertEqual(payload["catalog"]["total_benchmarks"], 1)
        self.assertEqual(payload["catalog"]["categories"][0]["id"], "compact")
        self.assertEqual(payload["config"]["last_run"], {})

    def test_runs_api_rejects_old_study_shape(self) -> None:
        status, _, data = self.request(
            "POST", "/api/runs",
            {"study_id": "fixture", "reducers": ["r1"], "timeout_seconds": 1, "outer_jobs": 1},
        )
        self.assertEqual(status, 400)
        self.assertIn("categories", json.loads(data)["error"])

    def test_unknown_route_is_not_found(self) -> None:
        for path in ("/api/unknown", "/api/studies", "/api/studies/fixture"):
            with self.subTest(path=path):
                status, _, _ = self.request("GET", path)
                self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
