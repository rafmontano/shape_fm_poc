# ShapeFM architecture

ShapeFM is one research workflow, not a collection of model-specific runners.
The architecture is designed to be simple, restartable, parallel where useful,
and transparent to the researcher.

![ShapeFM research architecture](images/shapefm_research_architecture.png)

## Six processes

```text
01 Import → 02 Preprocess → 03 Transform → 04 Forecast → 05 Combine → 06 Evaluate
```

| Process | Owns | Durable completion |
| --- | --- | --- |
| 01 Import | Read canonical GIFT-Eval records and store canonical series/window boundaries. | Accepted series items in `experiment.duckdb`. |
| 02 Preprocess | Apply the selected cleaning mode to historical observations. | Accepted prepared series in `experiment.duckdb`. |
| 03 Transform | Fit/apply transformations; create windows, split membership, labels and reusable features. | Accepted high-volume items in `windows.duckdb`, linked to the parent. |
| 04 Forecast | Run forecast providers and train/load/predict expensive meta-learners. | Forecasts, directional predictions and model-artifact metadata. |
| 05 Combine | Resolve stored component forecasts and apply an approved combination. | Combined forecast plus component identities and weights. |
| 06 Evaluate | Calculate approved metrics, sensitivity, tables and derived exports. | Evaluations/tables in DuckDB; figures/CSVs under `results/`. |

All models plug into this workflow. DTW, Mantis, Chronos, R forecasts and
combinations do not own separate experiment runners.

## Workflow hierarchy

![ShapeFM research workflow hierarchy](images/shapefm_research_workflow_hierarchy.png)

```text
Experiment
  └─ Process
      └─ meaningful substep, only when it has its own prerequisite and output
          └─ bounded batch/work group, when useful
              └─ atomic scientific item
```

The hierarchy is semantic, not a requirement to add layers. A process with one
cohesive operation has no artificial substeps. Internal helper functions are not
reported as research progress.

## Four technical layers

![ShapeFM technical layers](images/shapefm_technical_layers.png)

1. **Prefect / process layer** expresses the experiment, process dependencies,
   meaningful substeps, operational attempts and failures.
2. **Dask / execution layer** schedules eligible bounded work locally or across
   configured machines. Scientific objects do not know where they execute.
3. **Python/R application layer** contains cohesive source, domain, provider and
   repository objects plus small scientific functions.
4. **DuckDB data layer** is the authoritative scientific state and restart
   boundary. One configured coordinator writes it.

ShapeFM inherits orchestration, scheduling and transactions from these building
blocks. It does not build a second scheduler, queue, retry system, transaction
protocol or validation framework around them.

## Normal item path

```text
repository reads pending identities
  → application object forms ordinary input
  → local or Dask execution calls the same scientific function/provider
  → ordinary result returns to coordinator
  → repository commits result and completion atomically
```

Dask placement, network transport and worker discovery are infrastructure
details. Workers never write the research databases.

## Storage and authority

One experiment uses fixed paths:

```text
config/experiment.json          creation-time bootstrap
results/experiment.duckdb       parent: configuration, canonical data, state,
                                forecasts and evaluations
results/windows.duckdb          child: high-volume windows, membership,
                                transformed inputs, labels and features
results/tables/                 derived human-readable tables
results/figures/                derived figures
models/...                      fitted DTW/Random-Forest artifacts
```

The two DuckDB files are one experiment. Process 01 creates the parent; Process
03 creates the required child. No third scientific database, experiment-named
database or evidence database is part of the architecture. Names, dates and
scientific identity live inside DuckDB, not in filenames.

Generated results are disposable and untracked. Git stores code, configuration,
locks and living documentation. External backups are optional researcher
snapshots, not a runtime dependency.

## Restart and progress

DuckDB item commits are restart authority. A restart submits only missing or
failed identities; completed results are not recalculated merely to verify them.
Prefect records operational attempts but does not replace scientific state.

Researcher status is a read-only interpretation of stored state. It reports the
experiment, current process/substep, completed/failed/pending counts, latest
recorded error and known totals. It does not rerun validation or infer a live
worker from a stale `running` row.

## Validation boundaries

Normal execution validates only:

- external configuration/source input;
- conversion into the canonical object;
- native provider output returning to ShapeFM;
- database transaction/integrity; and
- final expected completion count or dependency.

Trusted internal objects are not repeatedly revalidated. Historical parity,
environment qualification, corruption audits and cross-version comparisons are
explicit QA/diagnostic actions, not part of every experiment.

## Source ownership

```text
src/python/
├── 00_main.py
├── 01_import/
├── 02_preprocess/
├── 03_transform/
├── 04_forecast/
├── 05_combine/
├── 06_evaluate/
├── util/            only code used by at least two processes
└── tests/
```

Each process folder contains its real Prefect `process.py` flow and code used
only by that process. Empty parent wrappers, forwarding aliases and process code
hidden in large `shared_*` modules are not the target. R keeps process entry
scripts under `src/r/` and reusable scientific functions under `src/r/util/`.

## Architecture qualities

- **Simple and pragmatic:** every layer owns a necessary responsibility.
- **Efficient:** no duplicate whole-data passes or routine recalculation.
- **Scalable:** bounded work can move from local to distributed execution.
- **Agnostic:** scientific code does not know host, scheduler or database path.
- **Resilient:** Prefect exposes failures; DuckDB commits accepted items
  atomically; restart resumes the delta.
- **Observable:** progress follows the research hierarchy.
- **Scientifically stable:** operational topology never changes scientific
  identity or results.

The active simplification work is governed by
[POC2-X01](poc2-x01-architecture-transformation.md).
