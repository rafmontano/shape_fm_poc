# Foundation Stage 1 architecture

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

Only `ImportCoordinator` opens the database for writing. It streams source rows,
creates deterministic per-series tasks, and sends ordinary task objects to a
pure worker function. Workers return result objects without database access.

`workers = 1` calls the same worker function sequentially. `workers > 1` uses
local processes but retains one coordinator and one writer. Worker count is an
execution choice and does not affect dataset identity or scientific output.
Likewise, `max_series` limits one invocation but is not part of dataset identity;
the full invocation extends the smoke run's existing tasks.

For each successful result, one transaction inserts or validates the series,
inserts or validates its evaluation window, completes the task, and completes
the attempt. A failure rolls back all scientific writes and is recorded as a
failed attempt. On rerun, completed tasks are skipped and failed or interrupted
tasks are retried. `runs` stores the logical restartable import, while
`run_invocations` preserves every call, including all-skipped calls, with its
scope, worker count, environment, status, and summary.

## Scope and evolution

Schema version 1 contains only `schema_versions`, `datasets`, `series`,
`evaluation_windows`, `runs`, `run_invocations`, `tasks`, and `task_attempts`.
Future research tables will be introduced by explicit migrations when those stages are built.
POC 2 will address Mantis, MOMENT, and training architecture. The complete
six-stage experiment is intentionally outside this POC.
