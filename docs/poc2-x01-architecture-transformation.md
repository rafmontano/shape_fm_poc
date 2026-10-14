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

Increment A has passed bounded developer acceptance and is submitted for
researcher review; B–F remain planned. This does not close X01.

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
