import http.client
import json
from http.server import ThreadingHTTPServer
from pathlib import Path
import subprocess
import tempfile
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

    def test_study_lists_concrete_reducers_and_editable_defaults(self) -> None:
        record = self.manager.study("fixture")
        self.assertTrue(record["valid"])
        self.assertEqual([item["id"] for item in record["reducers"]], ["r1", "r2"])
        self.assertEqual(record["limits"]["trial_wall_sec"], 30)
        self.assertEqual(record["outer_jobs"], 2)
        self.assertEqual(record["trial_count"], 4)
        self.assertEqual(record["wave_count"], 4)

    def test_create_run_freezes_requested_subset_and_resources(self) -> None:
        with mock.patch.object(self.manager, "_launch", return_value={"run_id": "launched"}):
            result = self.manager.create_run({
                "study_id": "fixture", "reducers": ["r2"],
                "timeout_seconds": 90, "outer_jobs": 5,
            })
        self.assertEqual(result["run_id"], "launched")
        run_dirs = [path for path in (self.root / "results").iterdir() if path.is_dir()]
        self.assertEqual(len(run_dirs), 1)
        plan = reduce.load_plan(run_dirs[0])
        self.assertEqual([item["id"] for item in plan["reducers"]], ["r2"])
        self.assertEqual(plan["limits"]["trial_wall_sec"], 90)
        self.assertEqual(plan["execution"]["outer_jobs"], 5)

    def test_catalog_run_samples_categories_and_freezes_four_parameters(self) -> None:
        with mock.patch.object(self.manager, "_launch", return_value={"run_id": "catalog-launched"}):
            result = self.manager.create_run({
                "categories": ["compact"], "reducers": ["r2"],
                "timeout_seconds": 90, "outer_jobs": 5,
                "max_files": 1, "repeats": 3,
            })
        self.assertEqual(result["run_id"], "catalog-launched")
        run_dirs = [path for path in (self.root / "results").iterdir() if path.is_dir()]
        self.assertEqual(len(run_dirs), 1)
        plan = reduce.load_plan(run_dirs[0])
        self.assertEqual(plan["selection"]["categories"], ["compact"])
        self.assertEqual(plan["selection"]["sampled_count"], 1)
        self.assertEqual(plan["repeats"], 3)
        self.assertEqual(plan["limits"]["trial_wall_sec"], 90)
        self.assertEqual(plan["execution"]["outer_jobs"], 5)
        self.assertEqual(len(plan["benchmarks"]), 1)
        self.assertEqual(len(plan["jobs"]), 3)

    def test_create_run_validates_exact_request(self) -> None:
        invalid = [
            {"study_id": "fixture"},
            {"study_id": "fixture", "reducers": [], "timeout_seconds": 1, "outer_jobs": 1},
            {"study_id": "fixture", "reducers": ["r1"], "timeout_seconds": 0, "outer_jobs": 1},
            {"study_id": "fixture", "reducers": ["r1"], "timeout_seconds": 1, "outer_jobs": 0},
            {"categories": [], "reducers": ["r1"], "timeout_seconds": 1, "outer_jobs": 1, "max_files": 1, "repeats": 1},
            {"categories": ["missing"], "reducers": ["r1"], "timeout_seconds": 1, "outer_jobs": 1, "max_files": 1, "repeats": 1},
            {"categories": ["compact"], "reducers": ["r1"], "timeout_seconds": 1, "outer_jobs": 1, "max_files": 0, "repeats": 1},
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
        self.assertEqual(self.manager.studies()[0]["study_id"], "fixture")

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
        self.assertEqual(self.manager.studies()[0]["study_id"], "fixture")
        with self.assertRaisesRegex(ValueError, "expected 'feat/reduction'"):
            self.manager.create_run({
                "study_id": "fixture", "reducers": ["r1"],
                "timeout_seconds": 30, "outer_jobs": 1,
            })

    def test_prepared_summary_and_cases_use_frozen_values(self) -> None:
        output = self.root / "results" / "prepared"
        reduce.prepare(self.study_path, output, reducers=["r1"], timeout_seconds=70, outer_jobs=3)
        summary = self.manager.summary("prepared")
        self.assertEqual(summary["status"], "prepared")
        self.assertEqual(summary["total_trials"], 2)
        self.assertEqual(summary["outer_jobs"], 3)
        self.assertEqual(summary["limits"]["trial_wall_sec"], 70)
        self.assertEqual(summary["prepared_smtbatch_branch"], "feat/reduction")
        cases = self.manager._case_rows("prepared", {"page": ["1"], "page_size": ["25"]})
        self.assertEqual(cases["total"], 1)
        self.assertEqual(cases["cases"][0]["status"], "pending")

    def test_path_traversal_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.manager._run_dir("../outside")

    def test_case_pagination_reads_trajectories_only_for_returned_page(self) -> None:
        rows = [{
            "case_id": f"case-{index}", "family": "f", "theory": "t",
            "predicate_mode": "exit", "planned": 1, "completed": 0,
            "verified": 0, "statuses": {}, "by_reducer": {"r1": {
                "planned": 1, "completed": 0, "verified": 0, "statuses": {},
            }},
        } for index in range(200)]
        trajectory = {"trials": []}
        with (
            mock.patch.object(self.manager, "_load_run", return_value=(self.root, {"study_id": "fixture"})),
            mock.patch.object(self.manager, "_progress", return_value={}),
            mock.patch("smtbatch.reduction_serve.reduction.case_rows", return_value=rows),
            mock.patch.object(self.manager, "_trajectory", return_value=trajectory) as parse,
        ):
            result = self.manager._case_rows("fixture", {"page": ["2"], "page_size": ["5"]})
        self.assertEqual(result["total"], 200)
        self.assertEqual(len(result["cases"]), 5)
        self.assertEqual(parse.call_count, 5)


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
        self.assertIn("SMTBatch Reduce", html)
        self.assertIn("Total trial timeout", html)
        self.assertIn("Parallel jobs", html)
        self.assertIn("Maximum benchmark files", html)
        self.assertIn("Repeats", html)
        self.assertIn("SMTBatch branch", html)
        self.assertIn('id="categories"', html)
        self.assertIn('id="reducers"', html)
        self.assertIn("Confirm reduction experiment", html)
        self.assertIn("View results", html)
        self.assertNotIn("Reduction / Observation", html)
        self.assertNotIn("Study conditions", html)
        self.assertNotIn("Cactus", html)
        self.assertNotIn("PAR-2", html)

    def test_report_is_a_separate_route_with_trajectory_charts(self) -> None:
        status, _, data = self.request("GET", "/runs/anything/report")
        html = data.decode("utf-8")
        self.assertEqual(status, 200)
        self.assertIn("Top-level expressions", html)
        self.assertIn("AST nodes", html)
        self.assertIn("Serialized bytes", html)
        self.assertIn("Predicate calls", html)
        self.assertNotIn("Reduction / Observation", html)

    def test_studies_api_exposes_branch_gate_and_catalogue(self) -> None:
        status, _, data = self.request("GET", "/api/studies")
        payload = json.loads(data)
        self.assertEqual(status, 200)
        self.assertTrue(payload["config"]["can_launch"])
        self.assertEqual(payload["config"]["smtbatch_branch"], "feat/reduction")
        self.assertEqual({item["id"] for item in payload["config"]["reducers"]}, {"r1", "r2"})
        self.assertEqual(payload["studies"][0]["study_id"], "fixture")
        self.assertTrue(payload["catalog"]["valid"])
        self.assertEqual(payload["catalog"]["total_benchmarks"], 1)
        self.assertEqual(payload["catalog"]["categories"][0]["id"], "compact")

    def test_runs_api_rejects_old_study_only_shape(self) -> None:
        status, _, data = self.request("POST", "/api/runs", {"study_id": "fixture"})
        self.assertEqual(status, 400)
        self.assertIn("reducers", json.loads(data)["error"])

    def test_unknown_route_is_not_found(self) -> None:
        status, _, _ = self.request("GET", "/api/unknown")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
