# POC2-X01 validation architecture transformation

## Objective

Transform the active six-process implementation so the code reflects the
approved architecture: readable Prefect processes, transparent Dask execution,
cohesive application objects, DuckDB restart authority and validation only at
real trust boundaries. Preserve accepted scientific behaviour.

This is a transformation, not a rewrite. It is complete only when the active
path is materially simpler; moving the same code into new folders is not enough.

## Required result

```text
00_main Experiment flow
  ├─ 01_import/process.py
  ├─ 02_preprocess/process.py
  ├─ 03_transform/process.py
  ├─ 04_forecast/process.py
  ├─ 05_combine/process.py
  └─ 06_evaluate/process.py

process flow → cohesive source/provider/repository objects
             → bounded local or Dask scientific calls
             → coordinator DuckDB transaction
```

`util/` contains only cohesive code used by at least two active processes. There
are no empty parent scripts, forwarding facades, generic gate engines, duplicate
completion state machines or model-specific experiment runners.

## Transformation rule for each process

1. Map the active call path, objects, functions, validations, lines and stores.
2. Define the process's inputs, meaningful substeps (if any), atomic items and
   durable outputs before editing.
3. Keep scientific functions/providers unchanged unless a separately approved
   scientific defect exists.
4. Replace custom orchestration/scheduling/transactions with Prefect, Dask and
   DuckDB responsibilities.
5. Remove pass-through layers, duplicate checks and obsolete compatibility
   paths/tests.
6. Place process-only code in its numbered folder and genuinely shared code in
   `util/`.
7. Show the before/after call path and objective file/function/class/line counts.
8. Validate from an empty generated workspace with 100 series only through the
   changed process, then prove restart at its smallest durable unit.

Do not run later processes “for completeness.” Do not create a branch per
process. Work continues on `poc2-x01-architecture-transformation` until the
six-process result is reviewed.

## Process 01 reference

Target:

```text
researcher command → Experiment flow → ImportProcess.run()
  → GiftEvalSource → canonical conversion
  → ImportRepository transaction → results/experiment.duckdb
```

Process 01 has no justified independent scientific substep. Import acceptance is
internal finalization. A fresh run validates the configured external source,
converts selected items and commits them. A partial restart reads completed item
identities and converts only missing/failed items. A completed restart returns
the stored summary without reopening the source or rehashing tables.

The import path must not contain M4comp2018 reconciliation, a second source
authority, custom multiprocessing, a generic gate state machine, artificial
status substeps or repeated whole-source/table fingerprints.

Process 01 is accepted only when:

- the active call path is understandable from the experiment and process flow;
- the process-owned implementation is materially smaller than its baseline;
- normal operation retains only source, conversion, transaction and completion
  checks;
- a fresh 100-series run produces exactly 100 canonical series and no later
  process work;
- an interrupted/partial restart computes only missing items;
- a completed restart performs no source/scientific work; and
- human/JSON status is read-only and reports Process 01 completion accurately.

## Remaining processes

The same structure applies with process-specific science:

- Process 02: prepared-series items.
- Process 03: windows/membership/labels/features in the fixed child database.
- Process 04: provider forecasts and expensive fitted-model lifecycle.
- Process 05: stored component dependency resolution and combinations.
- Process 06: stored metrics, sensitivity, tables and read-only exports.

## Acceptance of X01

X01 closes only after all six active paths conform, the researcher can follow
the workflow/progress, accepted scientific results remain unchanged, fixed
storage paths/restart work from a clean generated workspace, and obsolete code,
tests, configuration and documentation have been removed. The researcher makes
the final acceptance decision.

## Pre-implementation checkpoint — 14 October 2026

The approved documentation/configuration/data/root cleanup is preserved before
the five-layer increments. This checkpoint is not acceptance of Increments A–F.
The new approved first-level flows and second-level pseudocode supersede the
earlier workflow targets above; those targets will be reconciled during the
implementation, not treated as an alternative orchestration design.

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

The tracked bootstrap and current generated parent database still describe
version 12 with horizon-specific DTW width calibration and an uncapped cohort.
The living directional contract describes fixed proportion 0.10 and a shared
10,000-window cap. Scientific comparison must distinguish those settings;
topology parity cannot establish their scientific equivalence.
