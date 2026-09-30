# Experiment configuration reference

## Authority and lifecycle

`config/experiments/poc2_m4_daily_100.json` preserves the complete historical
version-1, period-7 experiment. New default runs use
`config/experiments/poc2_m4_daily_100_resolved_period.json`, the version-2
contract whose R period is resolved in the pinned GIFT-Eval environment. A new
database requires one complete configuration file.
ShapeFM validates the entire document before writing, creates a temporary
DuckDB, stores the original and resolved documents, creates six process-state
rows, commits, and atomically renames the file. Invalid input leaves no database.

After creation, the database is authoritative. Resume does not read the JSON
and rejects a supplied `--configuration`. `experiment_configuration` stores the
contract version, name, date, description, seed, original JSON, resolved JSON,
scientific fingerprint, and configuration-integrity fingerprint.
`experiment_processes` stores ordered process state.
`execution_events` appends each request and its effective operational settings,
host/code provenance, result, or failure.

## Version 1 compatibility and version 2 period policy

Version 1 coupled `data.benchmark.seasonality = 7` to R preprocessing, R
forecasting, and scoring. Stored v1 documents remain valid and keep that
interpretation. They are not rewritten.

Version 2 removes seasonality from `data.benchmark` and adds
`pipeline.r_period_override`. `null` (the committed default) calls pinned
GluonTS `get_seasonality()` using the stored frequency; the current Daily result
is 1. A positive integer takes precedence for R preprocessing and R forecasting.
Set it to 7 only for an explicit legacy-cleaning comparison. Evaluation remains
on the pinned GluonTS benchmark convention and is not changed by this override.
Both the resolved R period and evaluation seasonality are retained in existing
benchmark metadata JSON; no schema column was added. The historical
`official_seasonality` metadata/column names remain compatibility aliases for
the R period.

## Configuration fields

| Field | Meaning and current value | Consumer |
|---|---|---|
| `configuration_version` | `1` for historical semantics; `2` for independently resolved R period | validator and DuckDB loader |
| `experiment.name` | v2 default `poc2_m4_daily_100_resolved_period`; v1 name retained | experiment metadata and status |
| `experiment.date` | required ISO date; v2 default `2026-09-30` | DuckDB experiment metadata |
| `experiment.description` | required research objective | DuckDB experiment metadata |
| `reproducibility.seed` | validated non-negative integer `1234` | scientific identity and future stochastic workers |
| `data.dataset_name` | `m4_daily` | import and GIFT-Eval payloads |
| `data.source` | GIFT-Eval system, pinned revision, local directory, three SHA-256 files | import integrity and provenance |
| `data.benchmark` | `m4_daily/D/short`, `Econ/Fin`, one variate, daily frequency, 14-step horizon, one window, zero-based end-exclusive boundaries | import, planning, workers, evaluation |
| `data.selection` | deterministic `first_official`, count `100` | import and plan |
| `pipeline.processes` | ordered IDs/names 01–06 | process state, CLI selection, status |
| `pipeline.preprocessing` | modes `standard`, `robust`; default `robust` | Process 02 task creation and R payload |
| `pipeline.transformations.methods` | `identity`, `minmax_then_standardize` | Process 03 task creation and worker payload |
| `pipeline.adjustment` | `identity` | variant identity |
| `pipeline.combination` | equal weight; AutoARIMA `0.5`, Chronos-2 `0.5` | Process 05 payload and provenance |
| `models.auto_arima` | R `forecast` package and all `auto.arima`/interval settings | Process 04 R payload |
| `models.chronos_2` | repository, revision, package version, float32, nine quantiles, and batch/cross-learning policy | Process 04 Python/Dask payload and preflight |
| `evaluation.method` | `gift_eval` | Process 06 selection |
| `evaluation.gift_eval` | code revision, locked environment, submodule directory | bridge launch and distributed preflight |
| `data.benchmark.frequency` | official GIFT-Eval frequency (`D`); v1 also retains historical `seasonality = 7` | import and period resolver |
| `pipeline.r_period_override` | v2-only `null` or positive R preprocessing/forecast period override | planning, Processes 02 and 04 |
| `evaluation.options` | axis, invalid-label, NaN, and versioned scoring-seasonality policy | Process 06 payload |
| `evaluation.provisional_candidate` | development candidate selector | result export logic |
| `evaluation.submission_metadata` | explicit draft/non-submittable fields | export validation only |
| `execution.default` | mode, process/import workers, batch/in-flight/retry/timeout/thread controls, memory floors, one writer | ordinary `run` and execution provenance |
| `execution.final_acceptance` | one Mac CPU + one Ubuntu GPU worker, one physical RTX 5090, memory ceilings and safety thresholds | `00_main.py test` |
| `execution.paths` | project, Chronos and R worker paths | coordinator, Dask workers, acceptance preflight |
| `execution.restart` | skip completed; retry failed/interrupted | coordinator selection policy |

Resolution derives four variants, three candidates, task counts
100/200/400/800/1,200/12 for Processes 01–06, 1,200 forecast rows, and 12
official evaluations. These values are not independent configuration fields.

The scientific fingerprint covers data and selection, the scientific pipeline,
models and intentionally controlled model settings, combination, evaluation,
and seed. It excludes experiment metadata and execution controls. The
configuration-integrity fingerprint covers the complete resolved document.

## Consumer flow

```text
creation JSON → validated original/resolved JSON → DuckDB
       DuckDB → ExperimentConfiguration → coordinator
              → process selection → explicit task payload → Python or R worker
```

R receives JSON through stdin and returns JSON through stdout. A shared R input
utility creates `stats::ts` from unchanged values and the resolved period for
both preprocessing and the nine-method pool. R never reads
the experiment JSON or DuckDB. Distributed Python workers receive ordinary
serialized arguments. The Mac coordinator remains the only DuckDB writer.

## Adding a field in a later POC

1. Add the field to the single experiment JSON contract.
2. Increment `configuration_version` when interpretation changes and retain an
   explicit decoder for older stored versions.
3. Update validation and deterministic resolution.
4. Update DuckDB schema/stored configuration if queryable columns are needed.
5. Expose the field through `ExperimentConfiguration`.
6. Pass only the required subset through coordinator, process, and task payload.
7. Update every Python or R consumer.
8. Add validation, persistence, payload, restart, and scientific regression tests.
9. Update architecture, this reference, and researcher instructions.

An incompatible scientific change requires a new database. Never reinterpret
or silently modify scientific settings in an existing database.
