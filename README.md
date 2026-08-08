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

Every `[reducers.<id>]` entry is one black-box reducer command. SMTBatch only supplies generic input/output/predicate arguments and records generic reduction measurements; it does not interpret the reducer's strategy or version.

```toml
[defaults]
results = "results"
port = 8001
target_branch = "feat/reduction" # SMTBatch checkout branch; defaults to main

[reducers.ddsmt]
label = "ddSMT"
command = ["/home/hanrui/ddsmt/bin/ddsmt", "-j", "1", "--timeout", "{predicate_timeout}", "--ignore-output", "{input}", "{output}", "{predicate}"]

[reducers.d3smt]
label = "D3SMT"
command = ["/usr/bin/python3", "-m", "src", "-j", "1", "--timeout", "{predicate_timeout}", "--ignore-output", "{input}", "{output}", "{predicate}"]
```

The default evidence path is external: SMTBatch wraps every predicate call, records the
candidate size vector, file hashes, solver outcome, elapsed time, and reducer stdout/stderr.
The report therefore gives a generic size-decline curve for every reducer. D3SMT's captured
logs may be inspected for white-box diagnosis, but no baseline must implement a D3SMT-specific
observer.

A reduction-v2 study contains benchmark predicates, default resource limits, repeats, comparisons, and a list of allowed reducer IDs. Reducer commands are not duplicated in the study. When a run is created, SMTBatch freezes the selected reducer definitions, resolved command/file hashes, timeout, outer jobs, repository identity, and strict-wave job matrix into the run plan. Resume always uses that immutable plan.

The SMTBatch checkout at `<project-root>/SMTBatch` must match `[defaults] target_branch` to launch or resume work. The consuming project branch is independent. A mismatched SMTBatch branch may still start the service and inspect historical runs.

## Commands

```bash
smtbatch serve
smtbatch serve status
smtbatch serve restart
smtbatch serve stop
smtbatch serve foreground

smtbatch reduce prepare results/.benchmark-catalog.json \
  --output results/smoke \
  --reducers ddsmt d3smt \
  --timeout 3600 \
  --jobs 4
smtbatch reduce run results/smoke
smtbatch reduce status results/smoke
smtbatch reduce report results/smoke --xlsx
```

The dashboard main page launches experiments and monitors active/history runs. Opening a result navigates to `/runs/<run-id>/report`, where reducer summaries, paired comparisons, evidence health, paginated cases, and three-dimensional predicate-call trajectories are shown.

Each run stores an immutable `plan.json` and `jobs.tsv`, append-only resume history, per-attempt evidence, sealed job markers, a rebuildable `results.tsv`, and report exports. Graceful stop drains in-flight trials without scheduling more. Immediate stop terminates active reducer process groups and preserves unsealed partial attempts for the next resume attempt.
