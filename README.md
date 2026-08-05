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

[solvers.z3]
binary = "/opt/z3/bin/z3"
command = ["gtimeout", "--kill-after=1", "{timeout}", "{binary}", "model=true", "{input}"]

[solvers.cvc5]
binary = "/opt/cvc5/bin/cvc5"
command = ["gtimeout", "--kill-after=1", "{timeout}", "{binary}", "--lang", "smt2.6", "--strings-exp", "--produce-models", "{input}"]
version_args = ["--version"]
```

Each solver has a name, an executable, and an argv template controlling exactly
where the timeout goes and which extra flags are passed. Placeholders:
`{binary}`, `{input}` (required exactly once), `{timeout}` (seconds),
`{timeout_ms}` (milliseconds). `version_args` defaults to `["--version"]`.
See `smtbatch.toml`.

## Usage

Run from anywhere inside a project; the config, benchmarks, and results roots
all resolve from the project's `smtbatch.toml`:

```bash
smtbatch run --solver z3 --solver cvc5 --input benchmarks --output results/baseline --timeout 30 --jobs 8

smtbatch serve            # start the dashboard in the background (default)
smtbatch serve restart    # stop, then start (use after upgrades)
smtbatch serve status     # pid, URL, log path
smtbatch serve stop
smtbatch serve foreground # debug in the current terminal

smtbatch export results/baseline --output exports/baseline.xlsx
```

The dashboard lives at <http://127.0.0.1:8000/>. Experiments submitted from the
page run in independent background sessions; closing the browser does not stop
them. Options: `--host`, `--port`, `--inputs-root`, `--results`.

Select a run from the history to load its separate report page. The report
loads its summary first; scatter data and server-paginated formula rows load
only when requested, keeping large experiments responsive.

Every run directory contains `jobs.tsv` (immutable queue), `results.tsv`
(streaming results), `progress.json` (live progress), `metadata.txt`
(provenance), and `logs/` (one output file per job, kept for debugging). The
Excel export contains only log paths.
