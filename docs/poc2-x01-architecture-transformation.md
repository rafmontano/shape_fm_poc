# POC2-X01 five-layer MVP architecture transformation

## Objective

Transform the active six-process implementation so the code reflects the
approved five-layer architecture: DuckDB authority, Python/R science, Dask work
groups, Prefect orchestration and top-layer MLflow research tracking. Preserve
approved scientific definitions and interpretation, not exact numerical identity
with retired historical implementations. X01 remains open for researcher review.

This is a transformation, not a rewrite. It is complete only when the active
path is materially simpler; moving the same code into new folders is not enough.

## Required result

```text
experiment.py Experiment flow
  ├─ p01_import/flow.py
  ├─ p02_preprocess/flow.py
  ├─ p03_transform/flow.py
  ├─ p04_forecast/flow.py
  ├─ p05_combine/flow.py
  └─ p06_evaluate/flow.py

process flow → cohesive source/provider/repository objects
             → bounded local or Dask scientific calls
             → coordinator DuckDB transaction
```

Shared adapters are `util/runtime.py`, `util/distributed_execution.py` and
`util/research_tracking.py`. `util/` contains only code used by at least two
active processes. The mandatory hierarchy is Experiment → Process → meaningful
subflow → work group → atomic item, with exactly the ordered subflows in
[architecture](architecture.md). There
are no empty parent scripts, forwarding facades, generic gate engines, duplicate
completion state machines or model-specific experiment runners.

## Transformation rule for each process

1. Map the active call path, objects, functions, validations, lines and stores.
2. Preserve approved process inputs, meaningful subflows/order, atomic items and
   durable outputs.
3. Preserve accepted mathematics and settings, including the fixed current DTW
   baseline in [directional models](directional-models.md).
4. Replace custom orchestration/scheduling/transactions with Prefect, Dask and
   DuckDB responsibilities.
5. Remove pass-through layers, duplicate checks and obsolete compatibility
   paths/tests.
6. Place process-only code in its importable process folder and genuinely shared
   code in `util/`; audit all retained active Python/R documentation against
   [code standards](code-standards.md), not merely new files.
7. Show the before/after call path and objective file/function/class/line counts.
8. Validate with focused tests and bounded 100-series evidence through only the
   changed process, then prove restart at its smallest durable unit. E/F continue
   the accepted Process 04 evidence; do not restart from zero or rerun Forecast.

Do not run later processes “for completeness.” Do not create a branch per
process. Work continues on `poc2-x01-architecture-transformation` until the
six-process result is reviewed.

## Process 01 reference

Target:

```text
Experiment → 01 Import
  → 01.01 Register source population
  → 01.02 Materialise canonical series
  → 01.03 Accept canonical dataset
```

01.01 preserves canonical source order, applies the configured population limit
and stores stable dataset/frequency/source-series keys and source locations.
01.02 plans only missing canonical records, calculates pure series items through
the shared executor and commits on the coordinator. 01.03 checks registered and
accepted counts/keys match and stores the dataset reference. This is the only
Import acceptance check; it does not repeat source/value validation. Accepted
subflows return stored references; accepted series are never rescheduled.

The import path must not contain M4comp2018 reconciliation, a second source
authority, custom multiprocessing, a generic gate state machine, artificial
status substeps or repeated whole-source/table fingerprints.

Process 01 is accepted only when:

- the active call path is understandable from the experiment and process flow;
- the process-owned implementation is materially smaller than its baseline;
- normal operation retains only real trust-boundary and minimal completion checks;
- a fresh 100-series run produces exactly 100 canonical series and no later
  process work;
- an interrupted/partial restart computes only missing items;
- a completed restart performs no source/scientific work; and
- its three subflows are visible in Prefect and the single resumable MLflow run.

## Remaining processes

The same structure applies with process-specific science:

- Process 02: prepared-series items.
- Process 03: rolling modelling inputs in the fixed child; membership and
  reusable features in the parent. Process 04 representations also use the parent.
- Process 04: provider forecasts and expensive fitted-model lifecycle.
- Process 05: stored component dependency resolution and combinations.
- Process 06: stored metrics, sensitivity, tables and read-only exports.

## Acceptance of X01

X01 closes only after all six active paths conform, the researcher can follow
the workflow/progress, final produced tables/figures are presented to the researcher,
fixed storage paths/restart preserve accepted work, and obsolete code,
tests, configuration and documentation have been removed. The researcher makes
the final acceptance decision.

## Approved MVP scope and planned increments

Unlisted behaviour is absent by default. No calibration, automatic tuning,
speculative validation/conversion, benchmark/environment probes, compatibility
wrappers, monitoring frameworks or worker/nested MLflow runs. Normal assertions
and trust-boundary failures are allowed; new capabilities require approval.
Exactly `results/experiment.duckdb` and `results/windows.duckdb` are scientific
databases; the child is created only when Process 03 needs it. Configuration,
subflow state and accepted atomic identities belong in DuckDB. Generated database
migrations/backward compatibility are not required.

The coordinator-only `MLflowResearchTracking` uses supported SQLite under
`.runtime/mlflow/`, one `ShapeFM` experiment and one resumable run whose ID is
stored in the parent. It logs milestones, approved small parameters/tags and
curated final metrics/CSV/PNG/PDF artifacts. Failures warn and never invalidate
science. Periodic progress/ETA is off unless a stored setting explicitly enables
it for a named long-running subflow; no autologging/system metrics/model registry.

Increments A–C retain their recorded status. Increment D (`94e12b3`) is accepted
by the researcher. E/F have completed their bounded continuations; the complete
candidate awaits researcher review. This does not close X01.

| Increment | Scope and bounded acceptance |
| --- | --- |
| A | Shared runtime/executor/tracking, experiment and Process 01; from no scientific DBs, 100 series through Import only; three visible subflows, one resumable run, restart schedules no accepted series. |
| B | Process 02; fresh 100 series through Preprocess only; prepared histories and database-only restart. |
| C | Process 03; fresh 100 series through Transform only; child ownership, shared configured cap, modelling inputs/features and atomic restart. |
| D | Accepted Process 04 without calibration; completed 100-series distributed run on Mac CPU, Ubuntu CPU and Ubuntu GPU; required outputs, cached assets, accepted-work restart and targeted TF32 tests. |
| E | Process 05-only continuation of accepted 100-series Process 04 evidence; stored components and explicitly enabled deployable adjustments, excluding Oracle/sensitivity diagnostics; no rerun of Process 04. |
| F | Process 06-only continuation after E; complete candidate-restartable surface, selected-only diagnostics, approved tables/figures and MLflow publication; no-new-work restart performs no scientific recalculation; no rerun of Process 04. |

Remove each superseded active path/tests after its bounded acceptance; do not
retain aliases. Measure before/after production files/folders/physical lines,
longest files, test files/lines and every direct Prefect/Dask/MLflow import
location; include elapsed times, restart and assigned-worker evidence. Focused
tests protect flow order/restart, shared-executor execution, scientific formulas and
one resumable tracking run/publication. The completed bounded runs below are
sufficient: final completion checks are non-scientific compilation, mocked
interface tests, documentation links and whitespace, not another calculation,
restart or publication. Do not run scientific test suites to repeat accepted work.

Calculate each scientific atomic item once on its assigned worker and resume
accepted DuckDB state. Hardware/backend are operational provenance, not scientific
identity or grounds for recalculation. Ordinary valid floating-point differences
are accepted; there is no parity gate, parity workload or numerical tolerance
gate. Provider boundaries check output shape and scientifically usable values.
Keep the TF32 correction that preserves Mantis float32 calculations.

Final review presents the produced Table 1, Daily Table 2, Figure 2 and standalone
CD outputs to the researcher. Do not run automatic historical comparison or a
second scientific workload for review. Stop for a material final-result issue
only when the researcher identifies it. No calibration or tuning to prior results.

## Pre-implementation checkpoint — 14 October 2026

Recorded cleanup checkpoint: `c498dc6`. Preserve it and this baseline; do not
rerun cleanup as part of the documentation reconciliation.

The approved documentation/configuration/data/root cleanup is preserved before
the five-layer increments. This checkpoint is not acceptance of Increments A–F.
The approved first-level flows and second-level pseudocode are reconciled into
the targets above; they are not evidence of implementation acceptance.

The structural baseline counts all production `.py` files under `src/python`,
including package initializers, and excludes tests, caches and package metadata.
Physical lines include comments and blank lines. Imports are counted throughout
the AST, including imports inside functions.

| Measure | Before Increment A |
| --- | ---: |
| Production Python files | 59 |
| Production source folders, including root | 2 |
| Production physical lines | 26,419 |
| Test Python files | 37 |
| Test physical lines | 14,776 |
| Files directly importing Prefect/Dask integrations | 11 |
| Physical lines in those integration files | 14,084 |
| Files importing MLflow | 0 |

The longest production files are `shared_experiment_execution.py` (5,270),
`shared_distributed_execution.py` (2,205), `window_preparation.py` (1,651),
`shared_configuration.py` (1,577) and `shared_database.py` (1,314 physical lines).

Cleanup validation: 95 focused tests passed; offline setup readiness reported
zero missing prerequisites; changed Python syntax, shell syntax, living-document
relative links and whitespace checks passed. A fresh isolated run through
Process 01 accepted 100 series with observations exactly equal to the first
100 canonical Arrow records in 6.784 seconds including CLI startup. The second
request, after removing its bootstrap JSON, completed in 6.960 seconds with all
100 accepted task attempts unchanged. Processes 02–06 remained pending and no
child database was created. This measures the starting implementation only.

At the checkpoint, the bootstrap/generated parent described version 12 with
horizon-specific DTW calibration and an uncapped cohort. That is historical,
not the active scientific baseline. The approved current baseline is direct
aeon one-NN, float64 standardised length 64, fixed proportion 0.10/effective
width 6 across horizons 1–14, one neighbour's labels reused and shared cap
10,000. Do not run calibration comparison or require exact v12 equivalence.

## Increment A — shared foundation and Import

The active path is `experiment.py → p01_import/flow.py → ImportApplication →
shared WorkGroupExecutor → coordinator Repository`. Its three approved subflows
replace the former CLI/actions/import-flow/store stack. Canonical acquisition
remains a setup capability in `p01_import/acquisition.py`, not an import-time
second source. Superseded researcher actions, progress/CLI/request modules,
Process 01 conversion/source/store/flow and their compatibility tests are removed.
Later-process scientific modules remain only until their owning migrations.

Fresh isolated acceptance from no scientific databases: exactly 100 canonical
series, matching the first 100 Arrow observations and official boundaries,
in 9.368 seconds including Prefect startup. Database-only restart after removing
bootstrap JSON took 2.067 seconds; all 200 population/canonical accepted rows
and timestamps were unchanged. No child database or later process was created.
Prefect exposed 01 and 01.01–01.03. MLflow recorded one resumable run,
`fee7e3bb99134022998e14b561c016ea`, with completed milestones and 100/100
completion progress. Periodic progress remained disabled.

Public-flow tests cover partial group failure with preserved earlier acceptance,
database-only resume, a single tracking run, and sequential/local-Dask execution
with mixed-frequency source rows. Together with retained setup tests, 12 passed.
Retained legacy storage/table/export tests passed (28); five focused
feature/manifest tests passed. Acquisition verified the real pinned source;
syntax, lock consistency and whitespace checks passed. No calibration or
later-process acceptance run was performed.

## Increment B — Preprocess

The active path is `02 Preprocess → 02.01 Materialise prepared histories →
02.02 Accept prepared dataset`. Prepared histories exclude the official test
future, preserve original scale and record native method/package provenance.
Daily's modelling period resolves to 1 from the pinned GluonTS adapter; no
statistical seasonality inference or scientific override was introduced.

The shared executor supports the native provider's bounded batch interface,
avoiding one R startup per series. Initial per-series startup took 148.372
seconds; the accepted fresh 100-series run through Process 02 took 31.530
seconds including experiment startup (the preparation calculation/acceptance
portion was 18.113 seconds). All 100 histories matched an independent direct
`forecast::tsclean(stats::ts(history, frequency=1))` calculation. No child or
later-process output was created. Database-only restart took 3.066 seconds,
with all 300 population/canonical/prepared rows and timestamps unchanged.
MLflow retained one run, `d21a6dc047894a11a473104133e6cfa0`, with completed
02/02.01/02.02 milestones and 100/100 completion progress; periodic tracking
remained disabled.

Six focused tests passed, including native standard/robust formulas, distinct
held-out futures, canonical preservation, no-work restart and local-Dask native
batch execution. Legacy planning/storage/native-R checks passed (62), retained
workflow tests passed (10), and legacy Process 03 fixture tests passed (6).
The old Process 02 wrapper, coordinator handler/planning/dispatch, Prefect
cleaning task, repeated storage validation and obsolete orchestration tests
were removed. R documentation changed without changing parsed scientific
expressions. No new/old schema compatibility bridge was added.

## Increment C — Transform

The approved four subflows assign series-level S1 membership, materialise
modelling origins, materialise reusable features and accept the modelling
dataset. Rolling inputs clean raw training blocks independently; official
directional inputs clean the raw trailing 64 observations independently of
full-history point inputs. Sample-SD state and strict future labels retain the
accepted definitions. Membership/features/common reference membership are
parent-owned; origin inputs/labels are child-owned.

Fresh 100-series acceptance through Process 03 took 281.298 seconds including
startup. It accepted 732 modelling inputs (632 rolling and 100 official), a
495-window S1 training cohort under the configured shared maximum 10,000, and
595 reusable 42-feature vectors. Native calculation/acceptance took 127.667
seconds for inputs and 114.709 seconds for features. All 732 raw boundaries,
block-local R-cleaned inputs, sample-SD states and strict labels passed an
independent read/QA calculation. The capped fixture exercises actual stratified
selection rather than only the below-cap case; evaluation inputs remain uncapped.

Database-only full requested-range restart took 4.065 seconds. A separate
Process 03-only restart after legacy retirement took 4.893 seconds: all 996
parent and 732 child rows and acceptance timestamps remained unchanged.
The final canonical databases supersede intermediate isolated evidence copies.
MLflow used one run, `4456cf7e55a14684a431c5435df278af`, with completed
03/03.01–03.04 milestones and completion progress. No Process 04 work or
calibration ran.

Sixteen focused import/preprocess/transform/numerical tests passed after
retirement. Native feature/cohort/transformation R checks passed; audited R
expressions remained identical. Superseded Process 03 wrapper, coordinator
handlers/planning/dispatch, window/feature orchestration, repeated storage
validation, compatibility transformation branches and their obsolete tests
were removed. No schema bridge or wrapper was introduced.

## Increment D — Forecast accepted

The researcher accepted `94e12b3`. The five Forecast subflows use pure native
R/Python calculations, coordinator-only result/artifact acceptance and cached
foundation-model sessions. A–C retain their status; E/F have completed their
bounded continuations. X01 remains open for final researcher review.

Accepted evidence is the completed 100-series distributed Process 04 run using
Mac CPU, Ubuntu CPU and Ubuntu GPU. Required outputs are 1,600 individual
forecasts, 595 embeddings, 15 fitted artifacts and 200 complete directional
vectors. Restart preserves accepted DuckDB work and fitted artifacts; cached
assets are reused. No calibration or Process 05/06 acceptance is part of D.

Targeted TF32 tests support the retained correction: CUDA matmul/convolution
TF32 is disabled to preserve the specified Mantis float32 calculation. Ordinary
valid backend floating-point differences are accepted without parity comparison
or tolerance gates. Continue E/F from this accepted evidence without rerunning 04.

## Increment E — Combine

Process 05 continued the accepted distributed 100-series experiment with no
Process 04 invocation. It accepted 100 complete M4 Comb horizons from stored
SES/Holt/Damped means at equal thirds, preserving component keys and weights.
The configured identity adjustment creates no extra candidates; Oracle and
sensitivity remain Process 06 diagnostics. The run took 8.608 seconds and the
no-new-work restart 2.058 seconds. All prior atomic rows remained unchanged;
restart preserved all accepted rows and timestamps. The focused public-flow
combination/lineage/restart test passed. Process 06 had not run at this boundary.

## Increment F — completed candidate for researcher review

Process 06-only continuation completed the accepted 100-series experiment in
231.647 seconds: 1,700 capability-aware forecast evaluations, 1,900 directional
evaluations, all 1,057 diagnostic candidates and three selected-only diagnostic
outputs. It assembled and published five CSVs and three PNG/PDF figure pairs.
The 2.051-second no-new-work restart preserved every accepted row and timestamp.
No Process 04, scientific comparison or calibration workload ran.

Accepted scientific state and outputs now reside at canonical root locations.
All three PNG figures were inspected: labels, legends and dependent-horizon
caveat are readable and unclipped. No presentation defect was found; no output
was regenerated. This does not constitute final researcher acceptance.

The final wiring fixes restore the process-owned period-provider CLI, move the
unchanged JSON digest to Evaluate, serialize the standalone export summary and
remove unused operational configuration. All retained active Python/R modules
and public contracts were audited, including formula meaning and Prefect
hierarchy/restart ownership. The technical illustration now shows exactly the
five approved layers. No new scientific calculation, restart or comparison was
performed during completion.

## Canonical delivery and read-only validation

The authoritative experiment is `287166f8-48ce-4bc2-b5de-584f51752775`; its
existing MLflow run is `e73fc135fdc54cbca127dc0069ee1942`.

- Both `results/experiment.duckdb` and `results/windows.duckdb` open read-only,
  have the same single owner, and retain all atomic plans/results. All 30
  milestones (six processes and 24 approved subflows) remain accepted.
- All 15 relative semantic `models/<model>/daily/...` references resolve with their
  stored size/SHA. Five CSVs in `results/tables/` and six PNG/PDF files in
  `results/figures/` are byte-identical to the accepted outputs.
- The existing MLflow run remains `FINISHED`, with 28 curated scientific metrics
  plus 60 operational completion/progress fields, five parameters, completed
  milestone tags and 11 published artifacts. Its old
  `last_error` tag is operational history, not a current failure or scientific
  state. Public read APIs resolve all artifacts under `.runtime/mlflow/`.
- Promotion replaced, rather than merged, the stale root database. Every table
  row was checked against the accepted package. The sole DuckDB change is the
  operational publication-path list in `06.06/published`; accepted timestamps,
  identities, source data and scientific values are unchanged. The windows
  database is byte-identical.
- MLflow has no public artifact-location setter in the installed version. One
  bounded transaction using its installed SQLAlchemy store/ORM relocated only
  the existing experiment `artifact_location` and run `artifact_uri`. All other
  SQLite rows and artifact bytes are unchanged. No experiment/run was created
  and no metric/artifact was relogged.
- The normal researcher command defaults directly to canonical repository-root
  paths. There are no symlinks, permanent promotion helpers or dependencies on
  the isolated review package.

The original accepted package and recoverable stale-root backup were preserved
through validation and the published canonical checkpoint. They and the obsolete
generated experiment/parity copies, one-off scripts and scratch material have
now been removed. Project bytecode and stale package-build metadata were removed;
Numba caches were preserved under `.cache/`. Source data, environments, package/
model caches, Git history and IDE state are preserved. Post-cleanup read-only
checks confirmed both stores, fitted references and all MLflow artifact links
remain usable without the review package. No task-owned writer/worker/service is
left active. Ubuntu synchronization is operational only and remains the final
delivery step when that host is available; it must not run science.

## Final structural measurements

Baseline is cleanup checkpoint `c498dc6`; final counts include retained active
first-party source only. Physical lines include blank lines/comments. Folder
counts are directories directly containing counted source; Python functions
include nested definitions. Vendored/generated code and environments are excluded.

| Measure | Before | Candidate |
| --- | ---: | ---: |
| Production Python files / folders | 59 / 2 | 34 / 8 |
| Production Python lines | 26,419 | 4,452 |
| Python functions / classes | 682 / 77 | 254 / 25 |
| Python test files / lines | 37 / 14,776 | 13 / 1,120 |
| Production R files / folders | 22 / 3 | 13 / 5 |
| Production R lines | 3,582 | 1,988 |
| R test files / lines | 9 / 1,703 | 7 / 1,122 |

Longest retained Python files: `p04_forecast/application.py` 308,
`util/runtime.py` 307, `p06_evaluate/application.py` 285,
`p01_import/acquisition.py` 262, `p04_forecast/mantis.py` 250 lines. Longest R:
`p04_forecast/forecast_methods.R` 992, `p04_forecast/forecast_worker.R` 157,
`p06_evaluate/metrics.R` 151, `p06_evaluate/publication_worker.R` 123,
`p03_transform/features.R` 108 lines. Before, the longest R files were respectively
`util/forecast_methods.R` 997, `qa/get_m4_daily_series.R` 300,
`qa/inspect_rolling_window.R` 277, `qa/inspect_standardisation.R` 210 and
`util/transformations.R` 188 lines; Python baseline maxima are recorded above.

The active Python production tree is:

```text
src/python/
  experiment.py
  p01_import/       acquisition.py application.py flow.py
  p02_preprocess/   application.py flow.py period.py
  p03_transform/   application.py flow.py inputs.py labels.py
  p04_forecast/    application.py checkpoint.py chronos.py
                   contract.py flow.py mantis.py native.py providers.py
  p05_combine/     application.py flow.py science.py
  p06_evaluate/    application.py flow.py gift.py publication.py reports.py
  util/            __init__.py distributed_cluster.py distributed_execution.py
                   providers.py research_tracking.py runtime.py
                   transformations.py
```

The old path was numbered entry scripts → generic coordinator/handler/planner
dispatch → process adapters → repeated storage validation/custom scheduling.
The new path is `experiment.py` → process `flow.py` meaningful subflows →
process `application.py` and pure scientific providers → shared bounded executor
→ coordinator repository transaction. The process owns its science and durable
atomic result; Prefect owns attempts, Dask schedules eligible work, MLflow tracks
one research run and DuckDB alone decides restart.

Retired families include all superseded numbered Python entry scripts, generic
experiment/workflow/execution-event/process-store infrastructure, duplicate
provider/model-storage wrappers, window/feature orchestration, researcher CLI
facades, DTW/seasonal/execution calibration, legacy schema QA, standalone R
AutoARIMA, unused R label/transformation/padding/diagnostic helpers and their
obsolete tests. Scientific parity-only diagnostics/tests were removed rather
than retained as a second acceptance framework. Git preserves the full removal
inventory and earlier implementations; no compatibility aliases remain.

All direct production integration imports (paths relative to `src/python`;
line numbers refer to this candidate):

| Integration | Locations |
| --- | --- |
| Prefect | `experiment.py:22`; `p01_import/flow.py:9`; `p02_preprocess/flow.py:9`; `p03_transform/flow.py:9`; `p04_forecast/flow.py:11`; `p05_combine/flow.py:7`; `p06_evaluate/flow.py:11`; `util/distributed_cluster.py:24`; `util/distributed_execution.py:15–17`; `util/research_tracking.py:16` |
| Dask/distributed | `util/distributed_cluster.py:22–23`; `util/distributed_execution.py:139` |
| prefect-dask | `util/distributed_execution.py:18` |
| MLflow | `util/research_tracking.py:15` |

The sole test MLflow import is `tests/test_mvp_import.py:15`. Baseline integration
imports were in `util/execution_calibration.py:841`, `feature_extraction.py:22,24–28`,
`p00_01_researcher_actions.py:452,599`, `p01_01_import_flow.py:5`,
`p04_01_forecast_flow.py:20–24`, `p04_02_forecast_provider.py:101`,
`p04_04_seasonal_period_tuning.py:27–33`, `shared_distributed_execution.py:33–36`,
`shared_experiment_execution.py:1558`, `shared_workflow_orchestration.py:36–42`
and `window_preparation.py:31–36`; there were no MLflow imports.

## Final non-scientific checks and review limits

Three mocked researcher-interface tests and four setup-interface tests passed.
Every retained Python file compiled in memory; all 20 retained R files parsed
without executing expressions. Living-document relative links, source-script
references and `git diff --check` passed. Scientific configuration sections are
unchanged by final cleanup. Canonical `status` was read-only; no scientific suite,
export, process, restart, calibration or parity comparison was run.

Distributed evidence remains the completed D run: 2,128.915 seconds through
Forecast only, Mac CPU plus Ubuntu CPU/GPU participation, accepted outputs and
accepted-work restart. Its recorded task stream contained 406 scheduler events
across six worker addresses on the two hosts. Hardware provenance does not enter
scientific identity. This evidence is not a request for another distributed run.

Final researcher review uses the canonical produced tables and figures, not a
second workload or historical-equivalence exercise. X01 remains open; neither
the completed candidate nor publication authorizes merging to `main` or closing
the transformation.

## Bounded source and workspace normalization

Safety checkpoint: `af53500f5a1a07b1d7bccb21d393da4d2cc2c4e0`. This increment
normalizes ownership, naming and operational paths only. It does not redesign
the five layers, change scientific settings or calculate any scientific item.
All six processes and 24 subflows retain their accepted state.

### Source and root before/after

Both languages now use `pNN_<process_name>/`, descriptive `snake_case` files and
central `tests/`. Python retains all six process folders and the `util`/`tests`
package markers; process folders are implicit namespace packages. R has no
Import or Combine directory because those processes have no R code.

| Authored source | Safety checkpoint files / lines | Normalized files / lines |
| --- | ---: | ---: |
| Python production | 35 / 4,442 | 34 / 4,452 |
| Python tests | 13 / 1,060 | 13 / 1,120 |
| R production | 13 / 1,988 | 13 / 1,988 |
| R tests | 7 / 1,122 | 7 / 1,122 |

The before layout had eight flat R workers, four process-only helpers in R
`util/`, the shared time-series adapter, and central tests. The exact source
move inventory below names checkpoint paths, not active aliases. All twelve
R files retain their parsed expressions, except literal `source()` paths.

```text
Before src/r/                     Normalized src/r/
eight flat workers                p02_preprocess/ preprocess_worker.R
                                  p03_transform/ feature_worker.R features.R training_cohort.R
                                  p04_forecast/ forecast_worker.R archived_forecast_worker.R
                                                forecast_methods.R
                                  p06_evaluate/ evaluation_worker.R diagnostic_worker.R
                                                publication_worker.R adjustments.R metrics.R
util/ five helpers                util/ time_series_input.R
tests/ seven files                tests/ same seven central files
qa/ generated history             removed

Before src/python/                Normalized src/python/
p01_import/ ... p06_evaluate/      same six process folders, without p04 package marker
util/ shared transformations      util/ transformations.py; other shared modules retained
tests/ thirteen files             tests/ same thirteen files, shared fixture marker retained
```

| Checkpoint path (under `src/`) | Active path (under `src/`) |
| --- | --- |
| `python/util/shared_transformations.py` | `python/util/transformations.py` |
| `r/02_01_preprocess_series.R` | `r/p02_preprocess/preprocess_worker.R` |
| `r/feature_provider.R` | `r/p03_transform/feature_worker.R` |
| `r/util/features.R` | `r/p03_transform/features.R` |
| `r/03_02_select_training_cohort.R` | `r/p03_transform/training_cohort.R` |
| `r/04_01_forecast_r_methods.R` | `r/p04_forecast/forecast_worker.R` |
| `r/04_03_forecast_m4_reference.R` | `r/p04_forecast/archived_forecast_worker.R` |
| `r/util/forecast_methods.R` | `r/p04_forecast/forecast_methods.R` |
| `r/06_02_evaluate_paper_tables.R` | `r/p06_evaluate/evaluation_worker.R` |
| `r/06_04_retain_diagnostics.R` | `r/p06_evaluate/diagnostic_worker.R` |
| `r/06_03_plot_directional_results.R` | `r/p06_evaluate/publication_worker.R` |
| `r/util/forecast_adjustments.R` | `r/p06_evaluate/adjustments.R` |
| `r/util/paper_table_metrics.R` | `r/p06_evaluate/metrics.R` |

```text
Before root                    Normalized root
.prefect/                      .runtime/prefect/
.runtime/dask/ stale scratch    .runtime/dask/ (empty operational boundary)
.runtime/mlflow/               .runtime/mlflow/ (unchanged accepted run)
models/<stored experiment ID>/  models/directional_dtw/daily/model.pickle
                               models/directional_mantis_rf/daily/horizon_01..14.pickle
.idea/ .Rproj.user/             removed
.DS_Store .Rhistory             removed
.amp/                          removed after task scratch is no longer needed
results/ data/ config/ src/     retained (authored source normalized above)
docs/ scripts/ external/        retained
.cache/ .tools/ .venv/          retained
renv/ environments/             retained
.Renviron .Renviron.example     retained
.Rprofile shape_fm_poc.Rproj     retained
.git/ and root manifests/locks  retained
```

No compatibility copy, forwarding alias, empty source directory or QA script
remains. Removed source is the documentation-only Python Process 04 package
marker. R `qa/` contained generated history only and is removed. Local filesystem
metadata was removed, including `.DS_Store` inside the vendored checkout; no
vendored source was changed. Generated bytecode/pytest caches and packaging
debris were removed from authored workspace locations. Installed dependency
metadata and protected environments/caches are not authored source and remain
intact. `src/` contains exactly the authored Python/R modules and central tests.

Python utility ownership was audited: `runtime`, `distributed_execution`,
`distributed_cluster` and `research_tracking` serve Processes 01–06;
`providers` serves 02/03/04/06; `transformations` serves 03/04. R `util` contains
only `time_series_input.R`, used by 02/03/04. The single-process period transport
moved unchanged into `p02_preprocess/application.py`.

The process-owned preprocessing script constant is reused by Transform. Its
obsolete bootstrap execution-path field was removed; the accepted stored
configuration is untouched and no old-path compatibility mapping is needed.
Prefect home is set before imports at the researcher boundary, propagated with
worker-local roots over SSH, and Dask scratch stays under `.runtime/dask/`.
Setup removes generated source egg-info immediately after environment sync;
the cleanup is one shell command, not a packaging or cleanup framework.

### Model-byte and scientific-state preservation

Before moving files, their original filenames, sizes and SHA-256 values were
recorded and checked against stored model references. Fifteen existing files
were renamed, not refitted or deserialized. The following final manifest has
the same sizes and hashes before and after. DTW is
`models/directional_dtw/daily/model.pickle`; every RF row is under
`models/directional_mantis_rf/daily/`.

| Final file | Bytes | SHA-256 (unchanged) |
| --- | ---: | --- |
| DTW `model.pickle` | 297706 | `ded4b1111328aca91af3e81ee760ad94ee3729091c78fa8e450f33ed3f6ab80f` |
| `horizon_01.pickle` | 1735200 | `02ff12e1e888ac637a26a6c4c365374368aa10978a34be20690d7a0f6cd193fe` |
| `horizon_02.pickle` | 1775040 | `b6e5afc9d65f3e8ee35c5a45939f451cba422709b344208bf74edc3dae87e281` |
| `horizon_03.pickle` | 1763840 | `c72be6ff881eee43caf85c80066ac799dcaa5ab15fd539408d5f82d12d4ad4a4` |
| `horizon_04.pickle` | 1748800 | `ae876471060bbe394271eeee14b57529fc432c1cdb68afcf407c03568bdccc5d` |
| `horizon_05.pickle` | 1731840 | `087bbfbf045765fae2f9e766a444f04215b09fe94b12c462eb585e42268ce05d` |
| `horizon_06.pickle` | 1774240 | `b3a08215a1a0f12d39ce01fd929f4eb79bb8e386a1b15a502101dfd9a1b3589e` |
| `horizon_07.pickle` | 1738240 | `9057842ee437cc69d9ecd8a198a350c4f7a8fe5bf8eab370085f6396ba960ef4` |
| `horizon_08.pickle` | 1756640 | `fc52a83bb1bba214b1bffaa35014843b250216bdd494206661e6091aa402815b` |
| `horizon_09.pickle` | 1778880 | `a72a787eeb12789e83555300fed328f3599932b53802ea743811369e6b86e7a2` |
| `horizon_10.pickle` | 1755040 | `ffee2a0df4f4940a42c74678d79d7f5f4773b06752e8d2f70be582ad96e24402` |
| `horizon_11.pickle` | 1778400 | `3fbdd0ea3b63eba2d042beb2e8fcd1a1eced1ac80cfbb24a241702627cbdccc6` |
| `horizon_12.pickle` | 1769600 | `a31c5e04421e25395629c45fb9a87bb8c7e044da59744ef702ab4707cf971ad7` |
| `horizon_13.pickle` | 1780800 | `a92cbdbdb972e0a1152f5114839e5c19f47eca1e0bcfd130aed4b9ec7495c67d` |
| `horizon_14.pickle` | 1778880 | `9ad63c95c2cfebe8f60b32f88e46f846f12d78bb1e0b3cc6f2b07d9e62b137d5` |

Every row in both databases was checked read-only against the pre-move snapshot.
Only fifteen `04.03` result `path` fields changed. All other fields, table rows,
plans, accepted identities, scientific values, configurations, completion state
and timestamps are unchanged. No MLflow metadata change was needed: models are
not tracking artifacts, and the accepted tracking database/artifact bytes are
unchanged. The same existing run remains `FINISHED` with resolving links. All
five CSVs, six figures and canonical source-data bytes are unchanged.

### Exact additional authored edits and bounded checks

Besides the moves/removal above, edits are confined to:

- `config/experiment.json`, `scripts/setup.sh`, `src/python/experiment.py`;
- Python `p02_preprocess/application.py`, `p03_transform/application.py`,
  `p04_forecast/application.py`, `p06_evaluate/application.py` and
  `p06_evaluate/publication.py`;
- Python `util/providers.py`, `util/distributed_cluster.py`,
  `util/distributed_execution.py`;
- Python tests `test_preprocessing.py`, `test_researcher_interface.py`,
  `test_setup.py`, `test_transformations.py`;
- R tests `test_directional_result_plots.R`, `test_features.R`,
  `test_forecast_adjustments.R`, `test_forecast_methods.R`,
  `test_paper_table_metrics.R`, `test_paper_table_worker.R`,
  `test_training_cohort.R` (moved source/worker path references only);
- `README.md` and living docs `architecture.md`, `code-standards.md`,
  `environment.md`, `experiment-configuration.md`, `directional-models.md`
  and this X01 document.

Ten focused interface/path/setup tests passed using mocked calculation calls,
opaque temporary bytes and a stubbed installer, not models or scientific
processes. Python compiled in memory; retained application/flow/shared modules
and the renamed transformation-test imports resolved. All twenty R files parsed;
all twelve moved production files retained their parsed scientific expressions.
Source/config paths, Markdown links, whitespace and active stale-path searches
passed. No new scientific output, restart, parity workload, dependency change or
lockfile edit occurred. Ubuntu synchronization is revision-only, attempted once
at the end; an unavailable host leaves synchronization pending, not science.
