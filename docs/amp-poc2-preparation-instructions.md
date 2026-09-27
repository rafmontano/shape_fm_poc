# Historical brief: POC2 single entry point and acceptance baseline

> **Historical document.** This was the original AMP preparation brief before
> the final source cutover. It is retained only as design and acceptance-history
> evidence. `README.md`, `docs/architecture.md`, `docs/code-standards.md`, and
> `docs/local-execution.md` define the current interface and structure.

## Objective

Establish the single ShapeFM entry point and the authoritative 100-series
GIFT-Eval acceptance case over the existing POC1 implementation.

This task creates the stable interface and baseline needed for later structural
refactoring. It does not yet reorganise the existing implementation and does
not import anything from `m4_tsc_fmts_2026`.

## Working location and authority

Work only in:

```text
/Users/monta/Documents/Projects/shape_fm_poc
```

Read these documents before changing code:

```text
docs/code-standards.md
docs/architecture.md
docs/data-contract.md
docs/poc1.md
docs/local-execution.md
README.md
```

`docs/code-standards.md` defines the agreed target interface. Existing
architecture and data contracts continue to define scientific behaviour,
database authority, provenance, and restart semantics.

## Roles

- The researcher is the Chief Developer and approves interfaces, scientific
  scope, remote execution, and acceptance evidence.
- ChatGPT is the architect and reviews implementation and evidence.
- AMP is the developer/tester and implements the bounded task described here.

## Non-negotiable constraints

- Do not create a repository, worktree, project copy, or POC-specific source
  tree.
- Do not install or upgrade packages, tools, environments, or dependencies.
- Do not add dependencies when the existing standard library or locked
  environment is sufficient.
- Do not import code from `m4_tsc_fmts_2026`.
- Do not change cleaning, transformation, forecasting, combination,
  evaluation, metric, or data-selection mathematics.
- Do not change pinned data, package, model, or GIFT-Eval revisions.
- Do not change deterministic scientific identities, seeds, transaction
  boundaries, retry rules, or restart rules.
- Preserve the single writable DuckDB coordinator.
- Do not overwrite `data/shapefm.duckdb`, immutable source data, or accepted
  results.
- Do not run the full 4,227-series M4 Daily experiment.
- Do not commit, push, switch branches, or synchronize Ubuntu until the Chief
  Developer explicitly approves the remote-execution checkpoint.
- Preserve unrelated work, including the two untracked documentation files.
- Do not treat `.amp/` artifacts as project source.

## Agreed architecture

The only researcher-facing entry point is:

```text
src/python/00_main.py
```

Python owns orchestration, Dask, the single DuckDB writer, task state,
provenance, restart, GIFT-Eval integration, and foundation-model execution.

R is invoked by Python only for specialised R calculations. R workers receive
ordinary inputs, return ordinary results, and never open the writable database.

There is no target `src/python/shapefm/` folder and no long-term requirement to
preserve `import shapefm` or the POC1 console commands. Those interfaces remain
temporarily available only while the new entry point establishes the baseline.

## Scope of this AMP task

Implement only:

1. `src/python/00_main.py` as the single intended entry point.
2. The smallest general deterministic series-limit capability needed to select
   exactly 100 M4 Daily series end to end.
3. One end-to-end acceptance action invoked through `00_main.py`.
4. The acceptance evidence and report.

Do not move, rename, split, consolidate, or delete the current implementation,
tests, workflows, shell files, console commands, R files, or Python package in
this task. They are temporary implementation dependencies behind the new entry
point until the baseline is accepted.

## Entry-point contract

Use Python's standard library for command parsing. Do not add a CLI dependency.

The new entry point must:

- show help and exit without mutation when no action is supplied;
- reject unknown actions and invalid values clearly;
- resolve the repository root independently of the current working directory;
- record the invoked action, arguments, repository revision, environment, and
  database path;
- return a non-zero exit status on failure;
- never install or synchronize an environment automatically.

For this task, implement the actions required for the acceptance path and
read-only inspection. Do not add placeholder actions that report success but
perform no work.

The acceptance command must be:

```text
.tools/uv/uv run --locked python src/python/00_main.py test
```

The researcher must not need to call a POC1 command, R file, shell launcher, or
Python module directly.

## Deterministic 100-series selection

The current planner exposes a ten-instance smoke scope and full M4 Daily scope.
Add the smallest general capability necessary to pass a series limit through
the existing import, plan, execution, and evaluation path.

Requirements:

- Use a general parameter such as `series_limit`; do not hard-code a POC2-only
  scientific branch.
- The acceptance action fixes the value at `100`.
- Select the first 100 official M4 Daily forecast instances in pinned
  GIFT-Eval order.
- Assert exactly 100 distinct series identifiers and exactly 100 forecast
  instances. The current pinned configuration has one official window per
  selected series.
- Record requested and actual counts in invocation provenance.
- Default existing behaviour remains unchanged when a limit is not supplied.
- Preserve deterministic scope expansion and restart behaviour.
- Reject non-positive, non-integer, unavailable, or inconsistent limits.
- Do not create a configuration file solely to store the number 100.

This selection control is the only scientific-path capability that may be
added in this task. It must not alter forecasting mathematics.

## Acceptance database and isolation

The test uses a fresh isolated DuckDB database created specifically for the
acceptance run. It must not reuse an experiment containing earlier Mac MPS or
full-data forecasts.

The test must not write to:

```text
data/shapefm.duckdb
```

Record the isolated database path in the report. Generated databases, logs,
model caches, and raw results remain uncommitted.

## Expected scientific path

The single acceptance action executes the complete current grid:

```text
01 import
02 preprocess
03 transform
04 forecast
05 combine
06 official GIFT-Eval evaluation
```

Expected counts for 100 selected series under the current accepted POC1 grid:

```text
unique series / forecast instances: 100
process 02 tasks:                   200
process 03 tasks:                   400
process 04 tasks:                   800
process 05 tasks:                 1,200
process 06 tasks:                    12
candidate forecast rows:          1,200
official evaluation rows:            12
```

Derive and report the actual import task count separately. If the current
accepted configuration produces different downstream counts, stop and explain
the discrepancy rather than changing the assertions silently.

## Local implementation checkpoint

Before remote execution:

1. Confirm the existing locked environments are present; do not install or
   synchronize them.
2. Implement the entry point and deterministic limit with focused checks.
3. Run a read-only or dry plan for exactly 100 series.
4. Confirm the expected series, task, forecast, and evaluation counts.
5. Confirm the entry point can create and address a fresh isolated database.
6. Run only inexpensive local checks needed to establish that remote execution
   is ready.
7. Report the diff, commands, and dry-plan evidence.

Stop for Chief Developer approval before committing, pushing, changing remote
state, or synchronizing Ubuntu.

## Two-machine acceptance checkpoint

After explicit approval, synchronize the exact approved revision to Ubuntu and
run the acceptance action from the Mac.

The Mac is the coordinator and Dask scheduler. Register exactly:

```text
Mac:       5 CPU workers
Ubuntu:   15 CPU workers + 15 logical CUDA/GPU workers on one physical GPU
Total:    35 Dask workers
```

The scheduler and coordinator do not count as workers. Each logical GPU worker
advertises one Chronos slot while all fifteen share the same physical RTX 5090.

Each CPU worker uses one thread. Chronos-2 requests the Ubuntu
`CHRONOS_GPU_SLOT=1` resource. Official GIFT-Eval evaluation remains controlled
by the Mac. Only the coordinator writes to DuckDB.

Before execution, verify and record:

- the exact same approved revision on Mac and Ubuntu;
- clean working trees except for approved generated evidence;
- matching pinned GIFT-Eval revisions;
- existing locked environments on both machines;
- five registered Mac workers;
- thirty registered Ubuntu workers;
- exactly fifteen Ubuntu workers advertising `CHRONOS_GPU_SLOT=1`;
- no Mac worker advertising CUDA.

Do not install, upgrade, or synchronize dependencies as part of the test. If a
required existing environment is unavailable, stop and report it.

## Resource safety

The requested worker topology is a validation target, not prior evidence of
safety. Use bounded per-worker memory and preserve coordinator and operating-
system headroom.

Monitor and record:

- available memory and swap on both machines;
- worker memory limits;
- Dask spilling;
- worker restarts;
- task failures and retries;
- GPU memory and out-of-memory recovery;
- task contribution by host and resource type;
- wall-clock and process runtimes.

Stop and retain diagnostics if there is sustained memory pressure, persistent
spilling, a worker restart, machine instability, or risk to the authoritative
database. Do not silently change worker counts to make the run pass.

## Restart check

After the first successful acceptance run, execute the same acceptance action
again against the same isolated database.

The second run must:

- select no already completed scientific task;
- create no duplicate scientific row;
- preserve forecast and evaluation counts;
- preserve scientific fingerprints;
- record the new invocation and skipped counts.

## Acceptance criteria

The baseline is accepted only when all of the following are demonstrated:

1. The researcher invokes only `src/python/00_main.py`.
2. Exactly 100 unique M4 Daily series and forecast instances are selected.
3. All expected tasks complete with no pending, running, or failed tasks.
4. Forecast and official-evaluation counts match the dry plan.
5. Both Mac and Ubuntu CPU workers contribute tasks.
6. Every Chronos-2 task runs on the dedicated Ubuntu GPU worker.
7. The coordinator is the only DuckDB writer.
8. No scientific rows are duplicated.
9. The restart run skips completed work and preserves fingerprints.
10. No worker restart, persistent Dask spilling, or unhandled out-of-memory
    event occurs.
11. The report contains enough provenance to repeat and compare the same test
    after refactoring.

Do not claim acceptance if a machine was not used, worker counts differed, the
selection was not exactly 100, or official GIFT-Eval evaluation did not finish.

## Generated evidence

Write one concise generated acceptance report under `results/`. It should
include:

- entry-point command;
- repository revision and working-tree state;
- source and model revisions;
- isolated database path;
- requested and actual series counts;
- task, forecast, and evaluation counts;
- task contribution by host;
- Chronos worker and accelerator provenance;
- runtime and resource observations;
- failures and retries;
- restart evidence;
- scientific fingerprints;
- final pass/fail decision and limitations.

Do not commit generated databases, logs, caches, model weights, or raw result
artifacts.

## AMP report at the local checkpoint

Before requesting approval for remote execution, report:

1. Files added or changed.
2. The exact `00_main.py` interface.
3. How the existing POC1 implementation is invoked internally.
4. The minimal changes made for `series_limit`.
5. Dry-plan and local-check commands and outcomes.
6. Expected two-machine topology and command.
7. Risks, limitations, or unresolved questions.

Then stop for Chief Developer approval. Do not begin the broader structural
refactor in this task.
