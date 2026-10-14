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
8. Validate from an empty generated workspace with 100 series only through the
   changed process, then prove restart at its smallest durable unit.

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
the workflow/progress, scientific comparisons support the accepted interpretation, fixed
storage paths/restart work from a clean generated workspace, and obsolete code,
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

Increments A–C have passed bounded developer acceptance and are submitted for
researcher review; D–F remain planned. This does not close X01.

| Increment | Scope and bounded acceptance |
| --- | --- |
| A | Shared runtime/executor/tracking, experiment and Process 01; from no scientific DBs, 100 series through Import only; three visible subflows, one resumable run, restart schedules no accepted series. |
| B | Process 02; fresh 100 series through Preprocess only; prepared histories and database-only restart. |
| C | Process 03; fresh 100 series through Transform only; child ownership, shared configured cap, modelling inputs/features and atomic restart. |
| D | Process 04 without calibration; fresh 100 series through Forecast only; local then bounded two-machine same-current-code execution parity, cached assets and atomic restart. |
| E | Process 05; fresh 100 series through Combine only; stored components and explicitly enabled deployable adjustments, excluding Oracle/sensitivity diagnostics. |
| F | Process 06/final publication; fresh 100-series end-to-end run; complete candidate-restartable surface, selected-only diagnostics, approved tables/figures and MLflow publication; no-new-work restart performs no scientific recalculation. |

Remove each superseded active path/tests after its bounded acceptance; do not
retain aliases. Measure before/after production files/folders/physical lines,
longest files, test files/lines and every direct Prefect/Dask/MLflow import
location; include elapsed times, restart and local/two-machine evidence. Focused
tests protect flow order/restart, shared-executor parity, scientific formulas and
one resumable tracking run/publication. Run the bounded final suite once after F.

Same-current-code local/distributed parity verifies execution integrity with the
same inputs/settings and approved numerical policy. Historical aggregate
comparison instead asks whether Table 1, Daily Table 2, Figure 2 and standalone
CD outputs are approximately similar and support the same interpretation. Do not
calibrate, compare calibration settings or force exact historical v12 equivalence.
Record ordinary differences and continue; only differences materially affecting
rankings, conclusions or interpretations require researcher review. POC3 follows
this approximate historical-comparison policy too.

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
database-only resume, a single tracking run, and sequential/local-Dask parity
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
Evidence databases are retained in the isolated review artifact workspace.
MLflow used one run, `4456cf7e55a14684a431c5435df278af`, with completed
03/03.01–03.04 milestones and completion progress. No Process 04 work or
calibration ran.

Sixteen focused import/preprocess/transform/numerical tests passed after
retirement. Native feature/cohort/transformation R checks passed; audited R
expressions remained identical. Superseded Process 03 wrapper, coordinator
handlers/planning/dispatch, window/feature orchestration, repeated storage
validation, compatibility transformation branches and their obsolete tests
were removed. The unmigrated Process 04 retains only its existing stored-cohort
reader until D; no schema bridge or wrapper was introduced.

## Increment D — Forecast candidate; acceptance blocked

The candidate implements the approved five Forecast subflows with pure native
R/Python calculations, coordinator-only result/artifact acceptance and cached
foundation-model sessions. D is not accepted: its superseded implementation,
calibration-only code/configuration/tests and remaining documentation audit have
not yet been retired. E and F have not started. A–C remain accepted and pushed.

The isolated local 100-series workspace accepted 1,600 individual forecasts,
595 embeddings, 15 fitted artifacts and 200 complete directional vectors;
no Process 05/06 work ran. A first attempt failed after 488 forecasts; the native
failure's cause was not established. A Process 04-only resume preserved those
forecasts and completed the pool. Completed restart took 4.917 seconds with all
3,406 parent rows, 732 child rows, timestamps and artifact bytes unchanged.
Independent direct-aeon and pinned RF calculations verified all 2,800 directional
labels. Deleting one fitted artifact in a disposable copy rebuilt only that
unit in 12.410 seconds, with identical bytes and all other accepted rows intact.

A bounded two-machine Process 04-only run reused accepted Process 01–03 inputs
and finished in 2,128.915 seconds with the same counts. Dask's standard task
stream recorded scientific groups on Mac CPU, Ubuntu CPU and Ubuntu GPU.
Each isolated scientific experiment retained one coordinator MLflow run:
local `54b1cafba2be435f8a45057ddb9da5b7`, distributed
`e73fc135fdc54cbca127dc0069ee1942`. Completion status does not establish parity.

DTW neighbour identities, distances and all 1,400 labels matched exactly.
The initial CUDA Mantis run differed in 35 RF prediction vectors. A failing
eight-input batch reproduced cuDNN's default TF32 reduction. Disabling TF32
matmul/convolution preserves the specified float32 calculation; a bounded
595-input diagnostic plus independent 14-horizon RF fits then produced all
1,400 local labels exactly. Residual embeddings differed by at most
0.0003509521484375; 524 of 152,320 coordinates exceeded the diagnostic floating
check. This corrected candidate has not passed full distributed acceptance.

NNETAR and AutoARIMA still differ across hosts: 183 forecast array-fields
exceeded the legacy diagnostic `atol=rtol=1e-5`, with maximum original-scale
absolute difference 5.2710900185 and reported relative difference 0.002242269.
Identical direct native calls reproduced two failing series without Dask,
inversion or storage; each host's direct means reproduced its stored means
exactly after the accepted inverse transformation. R/package versions and
recorded scientific settings agree. Native BLAS libraries differ, but BLAS has
not been established as the cause. Seven focused A–D tests passed before this
numerical investigation; they do not override the failed acceptance check.
After the TF32 correction, the three focused Forecast tests passed again
(4.893 seconds), and the corrected native CUDA session passed the originally
failing eight-input batch (maximum embedding difference 0.0000057220458984375).

The living contracts refer to an approved numerical policy without specifying
floating-output thresholds. The legacy diagnostic threshold is not established
as researcher-approved. Researcher review must specify same-current-code
floating acceptance/precision for forecasts and embeddings before D can be
accepted. Do not widen tolerances, round outputs, alter optimizers/models,
replace numerical dependencies or concentrate work on one host to hide these
differences. Historical approximate similarity remains a separate final check.
Evidence is retained in the isolated review artifact workspaces; X01 is open.
