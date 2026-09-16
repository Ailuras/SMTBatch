"""Build real catalog manifests without invoking project-owned predicates."""

import json
from pathlib import Path
import sys

from smtbatch.reduction_serve import ReductionManager


def make_catalog(tmp_path, entries):
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    for filename in entries:
        (inputs / filename).write_text("(check-sat)\n")
    (tmp_path / "database.json").write_text(json.dumps(entries))
    (tmp_path / "oracle.py").write_text(
        "raise RuntimeError('catalog construction must not execute the oracle')\n"
    )
    command = [sys.executable, "-c", "pass", "{input}", "{output}", "{predicate}"]
    (tmp_path / "smtbatch.toml").write_text(
        '[benchmark_catalog]\n'
        'database = "database.json"\n'
        'inputs = "inputs"\n'
        'oracle = "oracle.py"\n'
        '[reducers.test]\n'
        f'command = {json.dumps(command)}\n'
    )
    return ReductionManager(tmp_path).benchmark_catalog()


def test_model_failure_is_retained_with_other_catalog_cases(tmp_path):
    entries = {
        f"{mode}.smt2": {"match": mode, "solver": "test-solver", "args": []}
        for mode in (
            "stderr", "stdout", "incorrect", "incorrect-unknown", "exitcode",
            "invalid-model",
        )
    }
    entries["invalid-model.smt2"].update(
        args=["model_validate=true"],
        model_policy="z3-model-validation-single-query-v1",
    )
    catalog = make_catalog(tmp_path, entries)
    assert catalog["valid"], catalog["error"]
    assert catalog["total_benchmarks"] == len(entries)
    manifest = json.loads(Path(catalog["manifest_path"]).read_text())
    cases = {row["id"]: row for row in manifest["benchmarks"]}
    assert set(cases) == set(entries)
    model = cases["invalid-model.smt2"]
    assert model["solver"] == entries["invalid-model.smt2"]
    assert model["predicate_mode"] == "artifact-invalid-model"
    assert model["predicate"]["command"] == [
        sys.executable, str(tmp_path / "oracle.py"), "--json", "invalid-model.smt2",
    ]


def test_unknown_match_mode_still_rejects_the_catalog(tmp_path):
    catalog = make_catalog(tmp_path, {"case.smt2": {"match": "invalid_model"}})
    assert not catalog["valid"]
    assert "case.smt2: unsupported database match mode 'invalid_model'" in catalog["error"]
    assert "manifest_path" not in catalog
