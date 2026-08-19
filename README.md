# SMTBatch

Local SMT-LIB batch experiment console. One tool, shared by every project:

- `smtbatch run` — bounded parallel queue of solver × formula jobs with streaming state.
- `smtbatch serve` — local dashboard (submit experiments, live run state, on-demand analysis, Excel download) that runs in the background by default.
- `smtbatch export` — Excel workbook for completed runs (log paths only, never raw output).
- `smtbatch collect` — copy cases matching a cross-solver consistency class.

## Install

```bash
uv pip install git+ssh://git@github.com/Ailuras/SMTBatch.git
# or, for development:
uv pip install -e /path/to/SMTBatch
```

## Solver configuration

Each project keeps a `smtbatch.toml` in its root. The tool finds it
automatically — the closest file walking upward from the current directory
wins, so no environment variable is ever needed.

```toml
[defaults]
inputs = "benchmarks"    # default benchmark root, relative to the config file
results = "results"      # default results root, relative to the config file
port = 8000              # dashboard port (optional)
target_branch = "Incremental"  # SMTBatch checkout at <project-root>/SMTBatch

[solvers.my-solver]
label = "My solver"             # optional label shown in the dashboard
binary = "/opt/my-solver/bin/my-solver"
command = ["gtimeout", "--kill-after=1", "{timeout}", "{binary}", "{input}"]

[solvers.another-solver]
binary = "/opt/another-solver/bin/another-solver"
command = ["gtimeout", "--kill-after=1", "{timeout}", "{binary}", "{input}"]
```

Each solver has a name, an executable, and an argv template controlling exactly
where the timeout goes and which extra flags are passed. Placeholders:
`{binary}`, `{input}` (required exactly once), `{timeout}` (seconds),
`{timeout_ms}` (milliseconds). `version_args` defaults to `["--version"]`.
Relative `binary` paths are resolved from the directory containing
`smtbatch.toml`.
The TOML table is the complete solver menu: the dashboard exposes every
configured entry and refreshes the menu when the file changes. Solver labels
are optional; when omitted, the table key is shown.
See `smtbatch.toml`.

The SMTBatch checkout at `<project-root>/SMTBatch` must match `[defaults] target_branch`
to run, launch, or resume work. The consuming project branch is independent. A
mismatched SMTBatch branch may still start the dashboard and inspect historical
runs. `target_branch` defaults to `main` when omitted.

## Usage

Run from anywhere inside a project; the config, benchmarks, and results roots
all resolve from the project's `smtbatch.toml`:

```bash
smtbatch run --solver my-solver --solver another-solver --input benchmarks --output results/baseline --timeout 30 --jobs 8
smtbatch run --resume --output results/baseline --jobs 8

smtbatch serve            # start the dashboard in the background (default)
smtbatch serve restart    # stop, then start (use after upgrades)
smtbatch serve status     # pid, URL, log path
smtbatch serve stop
smtbatch serve foreground # debug in the current terminal

smtbatch export results/baseline --output exports/baseline.xlsx
```

The dashboard lives at <http://127.0.0.1:8000/>. Experiments submitted from the
page run in independent background sessions; closing the browser does not stop
them. Running experiments can be cancelled after their in-flight jobs drain,
and interrupted, failed, or stale runs can be resumed from their durable queue.
Options: `--host`, `--port`, `--inputs-root`, `--results`. An explicit `--port`
overrides `[defaults] port`.

Resume treats `jobs.tsv` as immutable and accepts only complete result rows that
match it exactly. It verifies the recorded solver binary, command template, and,
for new runs, a content hash over every file-backed dependency reported by
`ldd` before appending results. This linked-artifact bundle prevents an
unchanged launcher from resuming against silently rebuilt shared libraries. A
per-run OS lock prevents concurrent controllers from writing the same result
stream.

Select a run from the history to load its separate report page. The report
loads its summary first; scatter data and server-paginated formula rows load
only when requested, keeping large experiments responsive.

Every run directory contains `jobs.tsv` (immutable queue), `results.tsv`
(streaming results), `progress.json` (live progress), `metadata.txt`
(immutable initial provenance), and `logs/` (one output file per job, kept for
debugging). Resumed runs additionally contain append-only
`resume_history.jsonl`, preserving each resume attempt and its worker count.
Solver provenance in `metadata.txt` includes the resolved artifact inventory
and a path-independent bundle hash. It also records the commit and dirty state
of both the project being measured and the SMTBatch runner repository. The
Excel export contains only log paths.

## Incremental SMT-LIB files

This branch scores incremental files (many `push` / `check-sat` / `pop` in one
`.smt2`) at **file** granularity:

- The solver process must finish the whole file (`complete=yes`, exit 0).
  Timeout, SIGKILL, or a non-zero exit is not a file success, even if stdout
  already contains `sat` or `unsat`.
- Only the **last** `check-sat` answer is the file result. Intermediate
  `unknown` is allowed and does not fail the file.
- `result` in `results.tsv` stays process-level: exit 0 uses that last answer;
  a kill is still `timeout`.

New runs append these columns after the original seven:

`queries  sat  unsat  unknown  first  last  expected  complete`

`expected` is the number of top-level `check-sat` / `check-sat-assuming`
commands in the source file. `queries` is how many answers were printed.
`complete=yes` means the process exited 0. Dashboard and Excel report the same
fields, including `partial_timeout` (timed out after printing at least one
answer). Resume still accepts the original 7-column `results.tsv`.
