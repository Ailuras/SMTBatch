# SMTBatch

SMTBatch is an independent, reduction-only experiment launcher, monitor, and evidence report. Each consuming project owns its `smtbatch.toml`, tracked study manifests, benchmark predicates, inputs, and results.

## Install

```bash
uv pip install git+ssh://git@github.com/Ailuras/SMTBatch.git
# development checkout
uv pip install -e /path/to/SMTBatch
```

SMTBatch discovers the closest `smtbatch.toml` while walking upward from the current directory. The service itself must be started from that project root.

## Project configuration

Every `[reducers.<id>]` entry is one concrete, independently selectable reducer version/configuration. Different strategies, versions, and evidence levels use different IDs.

```toml
[defaults]
inputs = "benchmarks"
results = "results"
studies = "scripts/experiments"
port = 8001
target_branch = "feat/reduction" # SMTBatch checkout branch; defaults to main

[reducers.ddmin-summary]
label = "D3SMT ddmin · observer summary"
version = "project"
strategy = "ddmin"
evidence_level = "summary"
acceptance = "trace"
stats = "required"
command = ["python3", "-m", "src", "--strategy", "ddmin", "--observe", "summary", "--observation-dir", "{observation_dir}", "--observation-stats", "{observation_stats}", "-j", "1", "--timeout", "{predicate_timeout}", "--ignore-output", "{input}", "{output}", "{predicate}"]
```

A reduction-v2 study contains benchmark predicates, default resource limits, repeats, comparisons, and a list of allowed reducer IDs. Reducer commands are not duplicated in the study. When a run is created, SMTBatch freezes the selected reducer definitions, resolved command/file hashes, timeout, outer jobs, repository identity, and strict-wave job matrix into the run plan. Resume always uses that immutable plan.

The SMTBatch checkout at `<project-root>/SMTBatch` must match `[defaults] target_branch` to launch or resume work. The consuming project branch is independent. A mismatched SMTBatch branch may still start the service and inspect historical runs.

## Commands

```bash
smtbatch serve
smtbatch serve status
smtbatch serve restart
smtbatch serve stop
smtbatch serve foreground

smtbatch reduce prepare scripts/experiments/smoke.json \
  --output results/smoke \
  --reducers ddsmt-stock ddmin-summary \
  --timeout 3600 \
  --jobs 4
smtbatch reduce run results/smoke
smtbatch reduce status results/smoke
smtbatch reduce report results/smoke --xlsx
```

The dashboard main page launches experiments and monitors active/history runs. Opening a result navigates to `/runs/<run-id>/report`, where reducer summaries, paired comparisons, evidence health, paginated cases, and three-dimensional predicate-call trajectories are shown.

Each run stores an immutable `plan.json` and `jobs.tsv`, append-only resume history, per-attempt evidence, sealed job markers, a rebuildable `results.tsv`, and report exports. Graceful stop drains in-flight trials without scheduling more. Immediate stop terminates active reducer process groups and preserves unsealed partial attempts for the next resume attempt.
