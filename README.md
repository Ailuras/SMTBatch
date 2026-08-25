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
target_branch = "feat/incremental"  # SMTBatch checkout at <project-root>/SMTBatch

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

Every run directory contains `jobs.tsv` (immutable queue, including the expected
check-sat count per file), `results.tsv` (streaming results), `progress.json`
(live progress), `metadata.txt` (immutable initial provenance), and, by default,
`events/` (one timestamped check-sat event stream per job). `logs/` contains one
solver-output file per retained job. Resumed runs additionally contain append-only
`resume_history.jsonl`, preserving each resume attempt and its worker count.
Solver provenance in `metadata.txt` includes the resolved artifact inventory
and a path-independent bundle hash. It also records the commit and dirty state
of both the project being measured and the SMTBatch runner repository. The
Excel export contains only log paths.

## Incremental SMT-LIB files

This branch scores incremental files (many `push` / `check-sat` / `pop` in one
`.smt2`) at two layers.

**Query counts** partition every expected `check-sat` / `check-sat-assuming`:

`sat + unsat + unknown + error + timeout + unreached = expected`

- `sat` / `unsat` — definite answers.
- `unknown` — printed `unknown` (including a solver's own per-query budget).
- `error` — an `(error "...")` line, or the in-flight query if the process crashed.
- `timeout` — bookkeeping only: at most one query, the check-sat that was running when GNU timeout / SIGKILL fired. The analysis UI does not treat this as a check-sat status; it is folded into `unreached`.
- `unreached` — later queries that never started after the process died.

**File labels** on the dashboard and report are derived from those counts, not from the process-level `file_status` column in `results.tsv`:

- `complete` — every expected check-sat is `sat` or `unsat`.
- `partial` — the session printed every check-sat, but some are `unknown` or `error`.
- `timeout` — the session did not finish because of the file wall.
- `error` — the session did not finish for any other reason.

`results.tsv` still stores a separate process-lifecycle `file_status` (`complete` = exit 0 and every check-sat printed an outcome, including `unknown`). Resume and the live progress snapshot keep that column unchanged so a running experiment is not rewritten.

`result` stays process-level: exit 0 uses the last printed outcome; a kill is still
`timeout`. Dashboard, analysis charts, and Excel lead with PO coverage
`(sat+unsat)/expected`. Check-sat summaries show sat/unsat/unknown/error/unreached.
The formula list gives each solver a check-sat column (PO tag plus
`sat+unsat/expected`) and a files column (file label plus runtime). The cactus
plot credits each decided check-sat at the time it was answered (`events/`
elapsed_ms) when those files exist, and clips GNU-timeout overshoot onto the
file wall so truncated files still count. If a run has many event files, the
query-time curve is built once, cached as `query_cactus.json`, and the first
report view may briefly show the file-runtime fallback. Switch the cactus to
files for the old last-answer curve. The scatter plot defaults to per-file
coverage, colored by coverage difference, with point size from the expected
check-sat count.

`jobs.tsv` records `expected` (the number of `check-sat` / `check-sat-assuming`
commands) when the queue is built, so pending files do not need to be re-parsed.
For repeatable probe sets, pass `--files-from probe.txt` instead of `--input`.
The manifest is newline-delimited, ignores blank lines and `#` comments, resolves
relative paths from its own directory, preserves order, and is hashed into run
metadata.

`results.tsv` always has:

`job_id solver file result time code output_path queries sat unsat unknown error timeout unreached first last expected complete file_status`

`queries` is how many outcomes were printed. Resume requires this header and
rejects older result streams. `jobs.tsv` must include `expected`; queues without
that column are rejected rather than re-parsed. Solver stdout is streamed into
`logs/job_<id>.<solver>.out` while the job runs, so a timeout still leaves a
partial log. `--log fail` deletes the file afterwards when the process succeeded.
Independently of that log policy, `events/job_<id>.<solver>.tsv` records
`ordinal elapsed_ms delta_ms outcome source` as answers arrive. A killed or
failed in-flight query gets one final `source=synthetic` timeout/error event;
later queries remain `unreached` in `results.tsv`. Use `--no-query-events` to
disable these files for a new run.
