# ShapeFM

ShapeFM is one traceable, restartable time-series research system. Its only
researcher-facing experiment entry point is:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py --help
```

`--no-sync` is required when using the already prepared locked environments;
the command does not install or update dependencies. The public actions are
`plan`, `run`, `prepare-windows`, the opt-in `prepare-features`, `status`,
`results`, and `test`.

The [working and communication agreement](docs/working-agreement.md) defines
roles, approvals and effective collaboration. It prioritises productivity and
clarity, not word limits.

The [Python file naming standard](docs/code-standards.md#python-utility-file-organisation)
links process helpers to their parent with `pNN_MM_` and identifies shared code
with `shared_`. Its approved migration changes organisation, not results. Any
code held for possible later reuse goes in tracked `tmp/inactive/`, not disposable
temporary storage. Follow the [current AMP handoff](docs/amp-poc2-workflow-orchestration-instructions.md)
for implementation and checkpoint publication status.
The naming pass is implemented for review; see the
[old-to-new map and verification](docs/poc2-workflow-orchestration-acceptance.md#python-organisation-implementation).
Public commands and stored experiment definitions are unchanged. Historical
source-bound window databases are not automatically rebound to renamed source.

Heavy testing must follow the [approved execution policy](docs/execution-policy.md).
The [execution safeguard instructions](docs/amp-poc2-execution-safeguards-instructions.md)
record the implemented eight Mac CPU workers and enforcement fixes. The focused
8+15 CPU validation passed without repeating the completed seasonal test.
Migration ID 010 subsequently implemented the portable
`standardise_sample_v1` Gate 3 recipe and completed its focused, gate-local
100-series acceptance; see the
[standardisation evidence](docs/poc2-standardisation-evidence.md).

## Installation

From the repository root, install the complete R and Python dependency superset,
submodules, datasets, and pinned model assets with:

```sh
scripts/setup.sh
```

This is equivalent to `scripts/setup.sh install all`. It is non-destructive,
reuses valid existing environments and downloads, and validates the complete
installation. Selective installation is intentionally unsupported. Run the
read-only audit at any time with:

```sh
scripts/setup.sh verify
```

For a clean reconstruction, `scripts/setup.sh rebuild all --confirm-delete`
moves every managed folder to an `original-folder-name_DDMMYY` sibling before
installing. It never deletes the old folders and stops before any move if a
dated destination already exists. See the [environment guide](docs/environment.md)
for prerequisites, all six Python environments, R and M4 handling, accelerator
behavior, recovery, download safeguards, and installation reports.

Setup-managed environments, generated data/models/results, local tool and
download caches, temporary downloads, and their dated rebuild archives are
ignored by narrowly scoped repository-relative rules. Environment manifests,
lockfiles, source, configuration, documentation, and tests remain trackable.

## Research workflow

The [approved workflow standard and software-layer diagram](docs/poc2-workflow-orchestration-decision.md)
govern this and future workflow development: Prefect coordinates, Dask schedules
compute, and the coordinator selected by the execution profile remains the sole
DuckDB writer. The tracked [machine inventory](config/machines.json) supplies
stable host facts and service ports; normal distributed execution derives its
Prefect and Dask endpoints without a manually exported Mac IP or
`PREFECT_API_URL`. The researcher closed
Stage 1 on 3 October 2026 with known limitations and authorised pragmatic Stage 2
consolidation and the reviewed GitHub checkpoint. See the
[acceptance record](docs/poc2-workflow-orchestration-acceptance.md). Prefect records
local operational history without persisting scientific task results, while
DuckDB remains authoritative for accepted work. Follow the [implementation
instructions](docs/amp-poc2-workflow-orchestration-instructions.md) and
[code standards](docs/code-standards.md); do not introduce a parallel workflow
framework or silently change the approved execution policy.

Stage 1 is **accepted for progression with documented limitations**. Runtime
profile v3 supports workload-dependent GPU launch and shared-GPU safeguards. Bounded
normal-entry forecasting, retrieval, post-commit restart and sequential comparison
passed. Native CPU overlap and a fresh normal-route scheduler-loss check are
carried into Stage 2, not marked as passed. Ordinary AutoARIMA currently runs
on Ubuntu under the large-fit rule. Do not bypass memory limits to force Mac
participation or repeat full campaigns for internal refactoring. See the
[closure evidence and manual QA instructions](docs/poc2-workflow-orchestration-acceptance.md#stage-1-closure-follow-up-handoff)
for fixture limitations and the current acceptance at the top of that record.
Stage 2 prioritises necessary workflows, already-approved previous-project
methods and consolidation of utilities under the
[pragmatic code standard](docs/code-standards.md#pragmatic-implementation).

Stage 2's opt-in `config/experiments/poc2_m4_daily_100_r_pool.json` connects the
nine native R methods to normal forecasting and retrieval. It does not replace
the default experiment or alter historical model settings. Review its bounded
evidence and limitations in the acceptance record before launching a full run.
The normal `run --processes 2-3` and `5-6` routes now accept the same explicit
approved execution profile as forecasting. Use inclusive ranges, not comma lists.

The bounded all-model acceptance configuration is
`config/experiments/poc2_m4_daily_100_all_models.json` (version 8). It selects
one robust/standardised variant and plans 100 Daily series × ten base models
(the nine registered R methods plus Chronos-2), followed by the existing
equal-weight combination: exactly 1,100 stored forecasts and 11 evaluations.
It uses the explicit `poc2_seasonal_recovery` profile and does not activate the
unregistered ID 018 point methods or its future M4 Comb recipe.
The [bounded execution evidence](docs/poc2-all-model-execution-evidence.md)
records 1,100 valid stored forecasts and successful two-host recovery, but does
not claim that every requested model fitted: all period-1 STL-AR requests used
the existing visible seasonal-naïve fallback. That scientific disposition
remains for researcher decision.

The follow-on forecast-contract configuration is
`config/experiments/poc2_m4_daily_100_forecast_contract_m4_comb.json` (version
9). It uses the same 100 Daily series and one preparation variant, routes nine R
probabilistic methods, Chronos-2, and four official M4 point methods through one
common request/result contract, then calculates `m4_comb` in Process 05 from the
stored SES, Holt, and Damped means. Its expected graph is 1,400 base forecasts,
100 combinations, 300 component links, 1,500 total forecasts, and 15
capability-appropriate evaluations.

The [version-9 execution evidence](docs/poc2-forecast-contract-m4-combination-execution-evidence.md)
records the completed graph, exact lineage, two-host contribution, resource
safety and an immutable scientific restart without interpreting model accuracy.

The ID 021 directional-only configuration is
`config/experiments/poc2_m4_daily_100_directional_dtw.json` (version 10). It
reuses bounded rolling-window preparation and strict labels, calibrates one
direct-aeon 1NN-DTW width per horizon, writes 1,400 binary directional
predictions, records Process 05 as deterministic no-work, and evaluates fourteen
horizons in Process 06. It writes no forecast rows. See the
[closed architecture](docs/poc2-id021-dtw-baseline.md) and
[accepted execution evidence](docs/poc2-id021-dtw-execution-evidence.md).
ID 021 was accepted and approved for publication on 6 October 2026; ID 022 was
not started.

For independent Stage 2 QA, inspect the disposable fixture named in the acceptance
record with `status` and `results`; distinguish its labelled synthetic forecast
rows from native model evidence. Check one stored result per R method, requested
versus executed method/fallback metadata, original-scale arrays and component
lineage. Repeat a completed normal `run` only with the documented approved profile
and verify that accepted forecast hashes/timestamps and task attempt counts stay
unchanged. Do not run the fault injector against accepted research databases.
Window/S1 QA still checks immutable membership and raw-parent ownership; tuning
QA still checks fold boundaries, policy selection and visible fallback provenance.
These checks do not establish Mantis training or Objective 2 reproduction.

The six ordered processes are:

```text
01 import → 02 preprocess → 03 transform → 04 forecast → 05 combine → 06 evaluate
```

Python coordinates every process and is the sole DuckDB writer. R is a
specialised worker for standard/robust preprocessing and an allowlisted nine-method
forecast pool. The pool's common probabilistic contract and explicit
seasonal-naive fallback are documented in
[`docs/forecast-methods.md`](docs/forecast-methods.md).
Import missingness and the two preprocessing modes are documented in
[`docs/preprocessing.md`](docs/preprocessing.md).
New standardised experiments use configuration v4 and keep v1–v3 interpretation
unchanged. The committed v4 acceptance configuration is
`config/experiments/poc2_m4_daily_100_standardised.json`.
Configuration v5 adds the historical optional rolling-window preparation outside
Processes 01–06. It persists one deterministic S1 train/test membership by
original series and complete transformed input windows in a separate child
DuckDB; raw inputs and futures remain only in the parent. The committed v5
configuration is `config/experiments/poc2_m4_daily_100_rolling_windows.json`.
Configuration v6 corrects R-compatible split rounding, parent/child validation,
bounded preparation and configurable W/H validation in a fresh experiment:
`config/experiments/poc2_m4_daily_100_rolling_windows_corrected.json`.
ID 013 adds strict actual directional labels when an exact original-scale
prepared reference exists. ID 014 adds optional reusable `fforma_base_v1`
features through an explicit `prepare-features` command; neither capability
makes feature extraction mandatory for Mantis or ordinary forecasts. See the
[directional-label](docs/poc2-directional-labels.md) and
[optional-feature](docs/poc2-features.md) decisions.
Completed task state and scientific provenance are stored transactionally, so
rerunning the same experiment skips completed work without changing experiment,
task, forecast, or evaluation identity.

Inspect the deterministic first-100-series plan:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py plan \
  --configuration config/experiments/poc2_m4_daily_100_resolved_period.json
```

Create one experiment database and execute Processes 01–03:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py run \
  --database results/poc2_m4_daily_100.duckdb \
  --configuration config/experiments/poc2_m4_daily_100_resolved_period.json \
  --processes 1-3
```

Resume Processes 04–06 from DuckDB alone. Do not pass the original JSON again:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py run \
  --database results/poc2_m4_daily_100.duckdb --processes 4-6
```

An existing database rejects `--configuration`. Repeating the second command
records another execution event but skips completed scientific work.

The historical Objective 1 two-worker acceptance command is shown below for
its specific scope; it is not the current heavy-test profile or the command to
recover the interrupted Gate 4 tuning run:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py test \
  --database results/poc2_final_acceptance.duckdb \
  --report results/poc2_final_acceptance_report.json
```

Read status and official evaluation results:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py status \
  --database results/poc2_final_acceptance.duckdb
.tools/uv/uv run --locked --no-sync python src/python/00_main.py results \
  --database results/poc2_final_acceptance.duckdb
```

Read one stored forecast by supplying all three selectors:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py results \
  --database results/poc2_final_acceptance.duckdb \
  --variant-id VARIANT_ID --series-id SERIES_ID --candidate CANDIDATE
```

After completing Process 01 for the v6 experiment, create or resume its window
database explicitly. Heavy acceptance uses the approved named 8-Mac/15-Ubuntu
CPU profile:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py prepare-windows \
  --database results/poc2_m4_daily_100_rolling_windows_corrected.duckdb \
  --windows-database results/poc2_m4_daily_100_rolling_windows_corrected.windows.duckdb \
  --execution-profile poc2_seasonal_recovery
```

A local focused check must declare enforced bounds instead of silently running
an unbounded local workload. The coordinator enforces the same contract for
direct calls: positive limits no greater than 100 series and 200 windows.

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py prepare-windows \
  --database PATH_TO_SMALL_PARENT.duckdb \
  --windows-database PATH_TO_SMALL_CHILD.duckdb \
  --local-max-series 2 --local-max-windows 20
```

Read one prepared window without writing either database:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py results \
  --database results/poc2_m4_daily_100_rolling_windows_corrected.duckdb \
  --windows-database results/poc2_m4_daily_100_rolling_windows_corrected.windows.duckdb \
  --dataset-id DATASET_ID --series-id SERIES_ID --window-ordinal 0
```

The optional 100-series AutoARIMA/ETS period-tuning test completed all 800
forecasts; see the [acceptance record](docs/poc2-seasonal-period-tuning-results.md).
Preserve that completed database. The
[safeguard follow-up](docs/amp-poc2-execution-safeguards-instructions.md) was
validated with focused tests and a small isolated distributed check, not another
full acceptance run. Chronos and GPU computation were not used for the R-only check.

Source `src/r/qa/inspect_seasonal_period_tuning.R` to leave one series' folds,
candidates, diagnostics, validation scores, selection, and final forecast in
`selected_tuning_series`. Validation actuals in this QA object are scoring data
only and were never supplied to preparation or model fitting.

For the standardisation acceptance, set
`STANDARDISATION_QA_DATABASE <- ".amp/in/id010_standardisation_acceptance.duckdb"`
and source `src/r/qa/inspect_standardisation.R` in a repository-root R session.
It leaves `selected_standardisation`, original/model/standardised/restored
histories, the fitted/stored state, and validation checks available for manual
inspection. The script opens DuckDB read-only and uses future actuals for QA
display only, never fitting.

For rolling-window QA, set `ROLLING_QA_PARENT_DATABASE`,
`ROLLING_QA_WINDOWS_DATABASE`, and `ROLLING_QA_DATASET_ID`, optionally set
`ROLLING_QA_SERIES_ID` and `ROLLING_QA_WINDOW_ORDINAL`, then source
`src/r/qa/inspect_rolling_window.R`. It leaves `selected_rolling_window`, raw
input/future, transformed input, fitted state, and inverse-restored cleaned
input in the R workspace. Both DuckDB files are opened read-only.

Generated databases and reports remain ignored; `test` refuses to overwrite
`data/shapefm.duckdb`.

## Final source structure

```text
src/python/00_main.py                    single researcher interface
src/python/01_import.py                  Process 01 wrapper
src/python/02_preprocess.py              Process 02 wrapper
src/python/03_transform.py               Process 03 wrapper
src/python/04_forecast.py                Process 04 wrapper
src/python/04_02_forecast_chronos.py     Chronos forecasting substep
src/python/04_04_directional_dtw.py      direct-aeon directional DTW executable
src/python/05_combine.py                 Process 05 wrapper
src/python/06_evaluate.py                Process 06 wrapper
src/python/06_01_evaluate_gift_eval.py   official evaluation substep
src/python/util/p00_01_researcher_actions.py researcher action dispatch
src/python/util/p01_01_import_flow.py     import coordinating flow
src/python/util/p04_01_forecast_flow.py   forecasting coordinating flow
src/python/util/p05_01_forecast_combination.py combination calculation
src/python/util/shared_experiment_execution.py cross-gate coordinator
src/python/util/shared_machine_environment.py machine inventory and topology resolver
src/python/util/shared_workflow_orchestration.py Prefect flows, tasks, writer locks
config/machines.json                    stable machine identities and service ports
src/python/util/window_preparation.py    rolling-window/S1 coordinator and retrieval
src/python/tests/                        unit, integration, and acceptance tests
src/r/02_01_preprocess_series.R          R preprocessing substep
src/r/04_01_forecast_auto_arima.R        R AutoARIMA substep
src/r/04_01_forecast_r_methods.R         bounded AutoARIMA/ETS tuning substep
src/r/util/forecast_methods.R             shared registered R forecast pool
src/r/util/transformations.R               portable fitted standardisation
src/r/util/window_preparation.R            explicit trailing-context preparation
src/r/util/labels.R                        strict direction-label utility
src/r/util/seasonal_period.R              shared period diagnostics
src/r/util/time_series_input.R           shared R time-series input contract
src/r/qa/inspect_rolling_window.R         read-only interactive window inspection
src/r/tests/                              focused R contract tests
```

## POC2 phases and topology

POC2 has two phases. **Preparation** established the stable structure, single
entry point, distributed acceptance, traceability, and restart proof. **Import**
uses a versioned JSON experiment definition for database creation and then uses
the stored DuckDB configuration exclusively for every resume.

The accepted historical end-to-end baseline used 5 Mac CPU workers, 15 Ubuntu
CPU workers, and 1 Ubuntu GPU worker (21 Dask workers). A subsequent isolated
calibration found that 15 logical Chronos worker processes sharing one physical
Ubuntu RTX 5090 were scientifically equivalent, faster, and resource-safe.
The Preparation topology was 5 Mac CPU + 15 Ubuntu CPU + 15 logical Ubuntu GPU
workers = 35 Dask workers. One physical GPU and 15 logical execution slots are
distinct facts. The complete 100-series pipeline and its restart passed with
this topology; measured evidence is recorded in
[`docs/poc2-preparation-completion.md`](docs/poc2-preparation-completion.md).
Objective 1's separate import acceptance used a two-worker target: one Mac CPU
and one Ubuntu GPU worker. That limited case does not replace the approved
heavy-test capacities. The current [execution policy](docs/execution-policy.md)
defines profile selection, memory-safe admission, Ubuntu synchronisation and
explicit user-approved exceptions when the laptop is away from Ubuntu.

See the [research vision](docs/research-vision.md),
[architecture](docs/architecture.md), [code standards](docs/code-standards.md),
[experiment configuration reference](docs/experiment-configuration.md),
[configuration inventory](docs/configuration-inventory.md), [data contract](docs/data-contract.md),
and [local execution contract](docs/local-execution.md).

## Research notes

[Transformation objects and fable](docs/research-notes-transformations.md)
records future implementation options and source evidence. It is research only,
not an approved change, scheduled release item or instruction for current work.

[Historical Table 1 forecast adjustments](docs/poc2-forecast-adjustment-reference.md)
preserves the Mantis scaling and SMYL–Oracle definitions for later migration,
including the researcher's clarification about older Mac results.
