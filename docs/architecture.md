# ShapeFM architecture

## Research architecture

![ShapeFM research architecture](images/shapefm_research_architecture.png)

The research view has seven layers: Research Interface, Experiment Management,
Evaluation & Benchmarking, Forecast Decision, Modelling & Representation,
Time-Series Engineering, and Research Data. Across those layers, the Experiment
Grid is the experiment component, ShapeFM Decision is the scientific function,
Benchmark Evaluation is the evaluation service, and Research Feedback closes
the iteration loop.

The persistent research objects are a Time-Series Record (data object), an
Experiment (research object), an Evidence Report (information object), and the
Research Schema (data structure). They move through one six-process cycle:

```text
01 Import → 02 Clean → 03 Transform → 04 Forecast → 05 Combine → 06 Evaluate
     ▲                                                                    │
     └──────────────────────────── Iterate ────────────────────────────────┘
```

This structure supports reproducible, traceable, scalable research while
avoiding full reruns, data sprawl, and manual orchestration.

## Technical layers

![ShapeFM technical layers](images/shapefm_technical_layers.png)

The technical view has three layers:

1. **DuckDB data layer.** One database stores canonical series, evaluation
   boundaries, scientific results, task/attempt state, and provenance.
2. **Python/R application layer.** Python coordinates and is the sole writable
   database owner. R receives ordinary JSON jobs for specialised `tsclean` and
   AutoARIMA computation and returns ordinary JSON results.
3. **Dask/concurrent.futures execution layer.** Bounded local or distributed
   queues execute serializable work. Workers never open writable DuckDB.

## Identity, transactions, and restart

Dataset identity is derived from pinned source revisions and material import
configuration. Experiment, variant, task, forecast, and evaluation identities
remain independent of worker placement and concurrency. Each successful result
and task completion is committed atomically by the coordinator. Failed or
interrupted work remains retryable; completed work is skipped on restart.

The Dask scheduler is transient and non-authoritative. The Mac hosts the
coordinator and scheduler. The approved Objective 1 acceptance topology has one
Mac CPU worker and one Ubuntu GPU worker; the scheduler is not a worker.
Chronos work requires `CHRONOS_GPU_SLOT=1` on the one physical Ubuntu GPU.

## Schema evolution

Schema versions preserve prior rows while adding experiment identity, task and
attempt state, scientific forecasts, and official evaluation provenance.
Arrays remain in DuckDB list columns. Execution addresses, transient model
weights, logs, and telemetry are not authoritative database content.

## Experiment configuration authority

This decision begins POC2 Phase 2 Import. Each DuckDB file represents one
experiment and grows as its six processes are completed. The same experiment
may execute Processes 01–03 first and resume Processes 04–06 later without
repeating completed valid work.

Before the database exists, one human-readable JSON file defines the
experiment. It must include these reference fields:

- `date`: the experiment creation date in ISO `YYYY-MM-DD` format;
- `description`: a concise statement of the experiment objective.

For example:

```json
{
  "experiment": {
    "name": "poc2_m4_daily_100",
    "date": "2026-09-28",
    "description": "Validate the central ShapeFM workflow on 100 M4 Daily series using AutoARIMA and Chronos-2."
  },
  "reproducibility": {"seed": 1234}
}
```

The Python entry point validates the complete JSON before creating the
database. Database creation stores both the original JSON and its resolved
configuration. From that point, DuckDB is the experiment configuration's
single source of truth; resuming the experiment must not depend on the original
JSON file.

A central Python configuration interface reads the stored configuration and
provides the coordinator with typed values. The coordinator passes each
process or task only the values it requires. Process and worker modules do not
duplicate global settings or issue independent configuration queries.

Scientific settings cannot change silently after database creation. Changing
the dataset, selected series, transformations, forecast horizon, models, or
evaluation method requires another experiment database. Operational settings,
such as worker allocation, may change when resuming, but the values actually
used are appended to the database execution history.

The database records each execution request, including requested processes,
timestamp, code revision, worker allocation, participating hosts, status, and
failure details. These records describe the progressive execution history of
one experiment; they are not separate scientific experiments.

The governing principle is:

> JSON defines a new experiment. DuckDB preserves and controls the experiment.
> The coordinator gives every process and task only the information it needs.

### Evolution across POCs

This is a continuing ShapeFM architecture decision, not a one-time POC2
refactor. Every later POC must begin by reviewing whether it introduces or
changes experiment settings. When it does, the experiment JSON contract,
validation, DuckDB schema or stored configuration, central configuration
interface, affected process and worker inputs, tests, and documentation must
evolve together.

New workflow settings must enter through this configuration path. They must not
be introduced as independent script globals, hidden defaults, or a second
configuration source. Existing experiment databases remain interpretable
through explicit configuration and schema versions; an incompatible scientific
change creates a new experiment database rather than silently changing an
existing experiment.

### POC2 implementation

Configuration version 1 is implemented by the typed
`ExperimentConfiguration` interface. New-database creation validates the whole
document before opening the target path, builds a temporary sibling database,
stores the original and resolved JSON plus required metadata/hash, creates the
six process rows transactionally, and atomically renames the file. Invalid
documents cannot leave a partial target database.

`experiment_configuration` is the immutable authority record.
`experiment_processes` records the current ordered Process 01–06 state.
`execution_events` is append-only execution evidence containing requested
processes, effective operational configuration, code/host identity, status,
summary, and failure. Scientific task/result tables retain their existing
stable identities and transaction boundaries.

The complete field contract and evolution procedure are documented in
[`experiment-configuration.md`](experiment-configuration.md). The exhaustive
disposition of former globals, machine settings, protocol constants, and test
expectations is in [`configuration-inventory.md`](configuration-inventory.md).
