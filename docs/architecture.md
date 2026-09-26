# ShapeFM POC architecture

## One authoritative database

`data/shapefm.duckdb` is the authoritative experimental store. It contains
canonical series, evaluation boundaries, source and configuration provenance,
run history, task state, and attempt history. There is no authoritative Parquet
layer or disposable catalog.

Original GIFT-Eval files remain separate and immutable under
`data/source/gift_eval/`. The adapter reads the pinned Arrow stream in bounded
batches and does not modify it. A deterministic dataset ID includes the logical
dataset, pinned source revision and hashes, and material import configuration;
changed input therefore creates a new dataset version.

## Coordinator and workers

Only a coordinator opens the database for writing. `ImportCoordinator` streams source rows,
creates deterministic per-series tasks, and sends ordinary task objects to a
pure worker function. POC 1 uses the same rule through `POC1Coordinator` for
Stages 2–6. Workers return result objects without database access.

The execution abstraction supports `sequential`, `local`, and `dask`.
Sequential calls the same worker functions one batch at a time. Local uses
bounded machine-local processes or threads. Dask submits the existing bounded
batches to CPU workers on the Mac and Ubuntu, while Chronos-2 batches require a
dedicated Ubuntu worker advertising `GPU=1`. Execution mode, scheduler address,
timeouts, retry count, and worker count are invocation controls and never enter
scientific identity.

The Dask scheduler and coordinator run on the Mac. Dask workers receive only
serializable task batches and return ordinary result dictionaries; worker code
does not import or open DuckDB. The coordinator validates every returned task
ID set, commits each result and task completion atomically, then releases that
future. Only a bounded window of futures exists, rather than one future per
scientific task. Official Stage 6 GIFT-Eval evaluation remains on the Mac.

The Dask scheduler is transient and never authoritative. DuckDB completion
state controls restart: committed tasks are skipped, interrupted tasks return
to pending, and only unfinished batches are submitted. A lost worker cannot
invalidate committed results. The GPU worker lazily owns one persistent
Chronos subprocess; a restarted worker reloads the pinned model and retries
only unfinished work.

Worker count is an execution choice and does not affect dataset identity or
scientific output. Likewise, `max_series` limits one invocation but is not part
of dataset identity; the full invocation extends the smoke run's existing tasks.

For each successful result, one transaction inserts or validates the series,
inserts or validates its evaluation window, completes the task, and completes
the attempt. A failure rolls back all scientific writes and is recorded as a
failed attempt. On rerun, completed tasks are skipped and failed or interrupted
tasks are retried. `runs` stores the logical restartable import, while
`run_invocations` preserves every call, including all-skipped calls, with its
scope, worker count, environment, status, and summary.

## Schema evolution

Schema version 1 contains only `schema_versions`, `datasets`, `series`,
`evaluation_windows`, `runs`, `run_invocations`, `tasks`, and `task_attempts`.

Schema version 2 preserves all version 1 rows and adds:

- benchmark and experiment identity: `benchmark_configurations`, `experiments`,
  `experiment_variants`, and `forecast_instances`;
- restart state: `experiment_invocations`, `experiment_tasks`, and
  `experiment_task_attempts`;
- scientific results: `preprocessed_series`, `transformed_series`, `forecasts`,
  and `forecast_components`;
- official outputs: `official_evaluations` and `submission_exports`.

Schema version 4 adds the exact evaluation input count and deterministic
forecast-input fingerprint to each official evaluation. Scope expansion keeps
upstream scientific rows and task IDs, while invalidating only Stage 6 and
scope-dependent evaluation/export records.

Arrays stay in DuckDB list columns; model weights and temporary worker data do
not. POC 2 will address Mantis, MOMENT, and training architecture through later
explicit migrations, without pre-creating speculative tables here.
