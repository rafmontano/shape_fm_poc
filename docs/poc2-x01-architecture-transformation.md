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
- All 15 relative `models/<experiment-id>/...` references resolve with their
  stored size/SHA. Five CSVs in `results/tables/` and six PNG/PDF files in
  `results/figures/` are byte-identical to the accepted outputs.
- The existing MLflow run remains `FINISHED`, with 28 curated metrics, five
  parameters, completed milestone tags and 11 published artifacts. Its old
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
through validation. Removal of obsolete generated copies and scratch material
follows the canonical checkpoint; source data, environments, package/model caches,
Git history and IDE state are preserved. No task-owned writer/worker/service is
left active. Ubuntu synchronization is operational only and remains the final
delivery step when that host is available; it must not run science.

## Final structural measurements

Baseline is cleanup checkpoint `c498dc6`; final counts include retained active
first-party source only. Physical lines include blank lines/comments. Folder
counts are directories directly containing counted source; Python functions
include nested definitions. Vendored/generated code and environments are excluded.

| Measure | Before | Candidate |
| --- | ---: | ---: |
| Production Python files / folders | 59 / 2 | 35 / 8 |
| Production Python lines | 26,419 | 4,442 |
| Python functions / classes | 682 / 77 | 254 / 25 |
| Python test files / lines | 37 / 14,776 | 13 / 1,060 |
| Production R files / folders | 22 / 3 | 13 / 2 |
| Production R lines | 3,582 | 1,988 |
| R test files / lines | 9 / 1,703 | 7 / 1,122 |

Longest retained Python files: `util/runtime.py` 307,
`p04_forecast/application.py` 304, `p06_evaluate/application.py` 285,
`p01_import/acquisition.py` 262, `p04_forecast/mantis.py` 250 lines. Longest R:
`util/forecast_methods.R` 992, `04_01_forecast_r_methods.R` 157,
`util/paper_table_metrics.R` 151, `06_03_plot_directional_results.R` 123,
`util/features.R` 108 lines. Before, the longest R files were respectively
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
  p04_forecast/    __init__.py application.py checkpoint.py chronos.py
                   contract.py flow.py mantis.py native.py providers.py
  p05_combine/     application.py flow.py science.py
  p06_evaluate/    application.py flow.py gift.py publication.py reports.py
  util/            __init__.py distributed_cluster.py distributed_execution.py
                   providers.py research_tracking.py runtime.py
                   shared_transformations.py
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
| Prefect | `experiment.py:18`; `p01_import/flow.py:9`; `p02_preprocess/flow.py:9`; `p03_transform/flow.py:9`; `p04_forecast/flow.py:11`; `p05_combine/flow.py:7`; `p06_evaluate/flow.py:11`; `util/distributed_cluster.py:24`; `util/distributed_execution.py:15–17`; `util/research_tracking.py:16` |
| Dask/distributed | `util/distributed_cluster.py:22–23`; `util/distributed_execution.py:138` |
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
