# Experiment configuration reference

## Authority and lifecycle

`config/experiments/poc2_m4_daily_100.json` preserves the complete historical
version-1, period-7 experiment. New default runs use
`config/experiments/poc2_m4_daily_100_resolved_period.json`, the version-2
contract whose R period is resolved in the pinned GIFT-Eval environment. A new
database requires one complete configuration file.
The optional 100-series AutoARIMA/ETS tuning experiment is
`config/experiments/poc2_m4_daily_100_period_tuning.json` and uses version 3.
The gate-local standardisation experiment is
`config/experiments/poc2_m4_daily_100_standardised.json` and uses version 4.
The opt-in rolling-window/S1 experiment is
`config/experiments/poc2_m4_daily_100_rolling_windows.json` and uses version 5.
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

The [execution policy](execution-policy.md) now governs heavy-run profile
selection and host availability. Recovery implemented an explicit operational
profile override without changing stored experiment identity. The
[safeguard follow-up](amp-poc2-execution-safeguards-instructions.md) now rejects
ordinary heavy tuning when the profile is omitted, removes hidden routing/limit
constants, monitors active owned R processes and implements eight Mac CPU
workers. Preserve stored creation documents and integrity hashes; do not reload
or rewrite scientific configuration.

## Versions 1–5

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

Version 3 preserves the version-2 resolver and adds opt-in Gate 4 period-policy
tuning. It compares baseline against `forecast::findfrequency()` over three
historical folds and records `tsfeatures` seasonal strength as diagnostics. The
fixed Gate 2 period and independent Gate 6 seasonality do not change. The v3
acceptance models are AutoARIMA and ETS. Approved heavy execution now uses both
CPU hosts under the execution policy. The original one-Mac v3 snapshot remains
readable; an explicit operational profile enabled recovery. It cannot silently
authorise a new heavy local run; only an explicit recorded researcher-approved
exception can do so. Chronos is intentionally outside this R-only acceptance.

Version 4 preserves the version-2 period resolver and normal
AutoARIMA/Chronos model route, registers `identity` plus
`standardise_sample_v1`, and requires an explicit positive
`pipeline.window_preparation.context_length`. The committed v4 document uses 64.
That window setting is validated, stored and exposed through the central
configuration interface, but current model adapters continue to consume full
histories. A v4 configuration can optionally include the approved v3 tuning
policy; absence of that policy is the normal v4 route.

The committed v4 `execution.final_acceptance` is an evidence snapshot of the
acceptance actually performed: sequential mode, Processes 1–3, and one Mac CPU
worker. It is not a scheduler profile and does not authorise heavy local work.
Any heavy execution continues to use the approved named profile with 8 Mac and
15 Ubuntu CPU workers under the execution policy.

`standardise_sample_v1` fits on history only using sample SD (n−1). Its exact
portable state is `recipe`, `version`, `centre`, `scale`, `count`, and
`constant`. Exact constants and singletons use scale 1, so apply and inverse
remain positive affine maps for additional values. Configuration versions 1–3
retain `minmax_then_standardize` exactly as stored and are never silently
reinterpreted as the new recipe.

Version 5 preserves version-4 transformation and period behavior and adds the
combined ID 011/016 contract under `pipeline.window_preparation`. It defines all
ten approved frequency W/H pairs, derives stride as W+H, selects frequencies
explicitly, protects official training boundaries, and fixes preprocessing,
transformation and S1 settings. The production document selects only Daily; the
frequency table is a reusable resolver contract, not a claim that every
GIFT-Eval dataset is production-ready. The exact S1 membership is generated and
persisted once, then controls resume without rerunning random allocation.

## Configuration fields

| Field | Meaning and current value | Consumer |
|---|---|---|
| `configuration_version` | `1` historical; `2` independently resolved R period; `3` opt-in period tuning; `4` portable sample standardisation; `5` rolling windows and persisted S1 | validator and DuckDB loader |
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
| `pipeline.transformations.methods` | v1–v3: `identity`, `minmax_then_standardize`; v4: `identity`, `standardise_sample_v1` | Process 03 task creation and worker payload |
| `pipeline.window_preparation.context_length` | v4 positive integer; committed value `64`; capability not yet consumed by current model adapters | central configuration and future selected window consumers |
| `pipeline.window_preparation.frequencies` | v5 approved W/H definitions for 10S, 5T, 10T, 15T, H, D, W, M, Q and Y; resolved stride is W+H | rolling-window coordinator and child definitions |
| `pipeline.window_preparation.split` | v5 S1 by original series, fraction 0.80, seed 123, pinned tsai generator; persisted membership controls resume | eligible-cohort allocation and child membership |
| `pipeline.window_preparation.block_policy` | v5 complete non-overlapping blocks at segment start; no padding, partial block, random offset or protected-boundary crossing | tsai window creation |
| `pipeline.adjustment` | `identity` | variant identity |
| `pipeline.combination` | equal weight; v1/v2 AutoARIMA + Chronos-2, v3 AutoARIMA + ETS | Process 05 payload and provenance |
| `models.auto_arima` | R `forecast` package and all `auto.arima`/interval settings | Process 04 R payload |
| `models.chronos_2` | repository, revision, package version, float32, nine quantiles, and batch/cross-learning policy | Process 04 Python/Dask payload and preflight |
| `models.ets` | v3 R `forecast` ETS with registered `opt.crit = "mae"` settings | Process 04 R payload |
| `evaluation.method` | `gift_eval` | Process 06 selection |
| `evaluation.gift_eval` | code revision, locked environment, submodule directory | bridge launch and distributed preflight |
| `data.benchmark.frequency` | official GIFT-Eval frequency (`D`); v1 also retains historical `seasonality = 7` | import and period resolver |
| `pipeline.r_period_override` | v2/v3 `null` or positive baseline R preprocessing/forecast period override | planning, Processes 02 and 04 |
| `pipeline.seasonal_period_tuning` | v3-only approved three-fold MAE policy; baseline retained for ties/inconclusive results | Process 04 tuning |
| `evaluation.options` | axis, invalid-label, NaN, and versioned scoring-seasonality policy | Process 06 payload |
| `evaluation.provisional_candidate` | development candidate selector | result export logic |
| `evaluation.submission_metadata` | explicit draft/non-submittable fields | export validation only |
| `execution.default` | mode, process/import workers, batch/in-flight/retry/timeout/thread controls, memory floors, one writer | ordinary `run` and execution provenance |
| `execution.final_acceptance` | versioned evidence snapshot; v4 records sequential Processes 1–3 on one Mac worker; legacy snapshots remain readable and heavy runs use the named execution profile | acceptance execution and preflight |
| `execution.paths` | project, Chronos and R worker paths | coordinator, Dask workers, acceptance preflight |
| `execution.restart` | skip completed; retry failed/interrupted | coordinator selection policy |

Resolution derives four variants, three candidates, task counts
100/200/400/800/1,200/12 for Processes 01–06, 1,200 forecast rows, and 12
official evaluations. These values are not independent configuration fields.
For the gate-local v4 acceptance, only Processes 01–03 were run: 100 imports,
200 preprocessing tasks, and 400 transformation tasks.

The scientific fingerprint covers data and selection, the scientific pipeline,
models and intentionally controlled model settings, combination, evaluation,
and seed. It excludes experiment metadata and execution controls. The
configuration-integrity fingerprint covers the complete resolved document.

Version 5's resolved configuration stores derived strides. Its parent schema
adds stable numeric frequency/dataset/series lookups and preparation-run state;
the normalized child schema stores definitions, S1 membership, tasks and
prepared windows. Raw arrays remain in the parent and future values are resolved
by zero-based, end-exclusive positions.

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
