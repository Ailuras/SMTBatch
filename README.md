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

Every `[reducers.<id>]` entry is one black-box reducer command. SMTBatch only supplies generic input/output/predicate arguments and records generic reduction measurements; it does not interpret the reducer's strategy.

Replace the example reducer commands below with your project's executables.
Relative executable and provenance paths are resolved from the configuration
directory. Reducers run with the study's project root as their working directory.

```toml
[defaults]
results = "results"
port = 8001
target_branch = "feat/reduction" # SMTBatch checkout branch; defaults to main
comparisons = [["baseline", "candidate"]]

[benchmark_catalog]
database = "benchmarks/database.json"
inputs = "benchmarks/inputs"
identity_command = ["/usr/bin/python3", "benchmarks/oracle.py", "--identity"]

[reducers.baseline]
label = "Baseline"
command = ["./tools/baseline", "{input}", "{output}", "{predicate}"]
provenance_paths = ["tools/baseline"]
require_clean = true

[reducers.candidate]
label = "Candidate"
command = ["./tools/candidate", "{input}", "{output}", "{predicate}"]
provenance_paths = ["tools/candidate"]
require_clean = true
```

The default evidence path is external: SMTBatch wraps every predicate call, records the
candidate size vector, file hashes, solver outcome, elapsed time, and reducer stdout/stderr.
The report therefore gives a generic size-decline curve for every reducer.
Reducer logs may be inspected for implementation-specific diagnosis. Optional
`SMTBATCH_*` correlation fields associate a reducer's own call and proposal IDs
with external predicate events; reducers without these fields work normally.
SMTBatch does not import the consuming project's modules or require its observer.

A reduction study contains benchmark predicates, default resource limits, repeats, comparisons, and a list of allowed reducer IDs. Reducer commands are not duplicated in the study. When a run is created, SMTBatch freezes the selected reducer definitions, executable and declared-source hashes, relevant Git identities and scoped dirty state, predicate-wrapper assets, input hashes, and an optional executed benchmark identity command together with the timeout, outer jobs, and strict-wave job matrix. Prepare rejects a dirty reducer marked `require_clean`; resume re-snapshots every frozen asset and rejects any drift before starting jobs. Only the current data format is supported; old experiment files must not be reused.

The SMTBatch checkout selected by `[defaults] smtbatch_root` (default:
`<project-root>/SMTBatch`) must match `target_branch` to launch or resume work.
It can reside outside the consuming project. Each repository keeps its own Git
history and branch. A mismatched SMTBatch branch may still start the service
and inspect existing runs.

## Commands

```bash
smtbatch serve
smtbatch serve status
smtbatch serve restart
smtbatch serve stop
smtbatch serve foreground

smtbatch reduce prepare results/.benchmark-catalog.json \
  --output results/smoke \
  --reducers baseline candidate \
  --timeout 3600 \
  --jobs 4
smtbatch reduce run results/smoke
smtbatch reduce status results/smoke
smtbatch reduce report results/smoke --xlsx
```

The dashboard main page launches experiments and monitors active/history runs. Opening a result navigates to `/runs/<run-id>/report`, where reducer summaries, paired comparisons, evidence health, paginated cases, and three-dimensional predicate-call trajectories are shown.

Each run stores an immutable `plan.json` and `jobs.tsv`, append-only resume history, per-attempt evidence, sealed job markers, a rebuildable `results.tsv`, and report exports. Graceful stop drains in-flight trials without scheduling more. Immediate stop terminates active reducer process groups and preserves unsealed partial attempts for the next resume attempt.

## Data format

Study manifests, plans, and result records use `"format": "reduction"`. Predicate
journal events use `"format": "predicate"`. There are no numbered data formats,
migration paths, or historical field aliases. Old plans are rejected by prepare,
run, status, and report commands. New runs must be prepared from the current
project configuration. Package and Python versions still identify software builds.

JSON benchmark predicates emit `"schema": "smt-oracle"`, with an `outcome`,
`reason`, and matching `signature`. The four outcomes are `interesting` (exit 0),
`not_interesting` (exit 1), `incomplete` (exit 3), and `error` (exit 2). A repeated
timeout or execution failure is not a preserved signal. Ordinary black-box
predicates without JSON remain supported as a separate invocation mode.

Quality records contain expression count, AST node count, raw byte count,
normalized byte count, and token count. Paired comparisons rank AST nodes first,
then normalized bytes. `preserving_calls` counts positive predicate checks; it
does not claim that the reducer adopted each candidate. The best observed
preserving candidate and the verified final output are reported separately.
