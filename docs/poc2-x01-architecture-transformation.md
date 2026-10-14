# POC2-X01 five-layer MVP architecture transformation

## Objective

Make the active six-process implementation reflect the approved five-layer
architecture, with materially simpler ownership and no duplicate orchestration.
Preserve approved scientific definitions and interpretation, not exact numerical
identity with historical implementations. X01 remains open for researcher review.

## Approved architecture and process hierarchy

The five layers, bottom to top, are DuckDB scientific state and restart,
Python/R scientific applications, Dask bounded execution, Prefect orchestration,
and coordinator-only MLflow research tracking. The selected coordinator alone
writes DuckDB; workers return results without storage or tracking clients.

```text
Experiment → Process → meaningful subflow → work group → atomic item
  01 Import → 02 Preprocess → 03 Transform → 04 Forecast → 05 Combine → 06 Evaluate

experiment.py → process flow.py → application/scientific providers
              → shared bounded executor → coordinator repository transaction
```

The six processes and their 24 ordered subflows are defined in
[architecture](architecture.md#workflow-hierarchy). DuckDB owns accepted atomic
identities and completion; Prefect owns operational attempts and Dask schedules
eligible work. Combinations consume stored component forecasts and preserve
lineage. Evaluate consumes stored results without refitting models.

## MVP boundaries

- Exactly two scientific databases: `results/experiment.duckdb` and
  `results/windows.duckdb`. Process 03 creates the child when needed.
- Configuration enters once and is stored in DuckDB. Accepted configuration,
  scientific identities, values and timestamps remain authoritative on resume;
  historical execution paths in stored configuration are not rewritten.
- Fixed DTW baseline: direct aeon one-nearest-neighbour on standardised length-64
  input, warping proportion 0.10/effective width 6 for horizons 1–14, one
  neighbour's labels reused, and shared maximum 10,000 reference windows. See
  [directional models](directional-models.md).
- Calculate each atomic item once on its assigned worker. Hardware/backend are
  operational provenance, not scientific identity or grounds for recalculation.
  Valid floating-point differences are accepted without parity workloads or
  equality/tolerance gates. Provider boundaries check required shape and usable
  values. Retain the tested CUDA TF32 correction for declared float32 calculation.
- No calibration, tuning, speculative validation, compatibility layers, second
  scheduler/transaction protocol, custom monitoring or model-specific runners.
- MLflow uses one `ShapeFM` experiment and one resumable research run, logging
  milestones and curated final metrics/artifacts. No worker/item/nested runs,
  autologging, system metrics or model registry. Tracking never governs science.

## Final current structure

Both languages use descriptive `snake_case` files and `pNN_<process_name>/`
ownership, with central tests. `util/` means code used by two or more processes;
no empty process folders, compatibility copies or forwarding aliases remain.

```text
src/python/
  experiment.py
  p01_import/ ... p06_evaluate/  process flows, applications and scientific code
  util/                        shared runtime, executor, providers, tracking,
                               transformations and cluster transport
  tests/
src/r/
  p03_transform/               features and training-cohort provider
  p04_forecast/                forecast providers and methods
  p06_evaluate/                metrics, diagnostics and publication
  util/                       preprocess_worker.R, time_series_input.R
  tests/
results/
  experiment.duckdb
  windows.duckdb
  tables/                     five CSVs
  figures/                    three PNG/PDF pairs
models/
  directional_dtw/daily/model.pickle
  directional_mantis_rf/daily/horizon_01.pickle ... horizon_14.pickle
.runtime/
  prefect/                    Prefect home set at the researcher entry point
  dask/                       operational scratch boundary
  mlflow/                     existing tracking state and published artifacts
```

`src/r/util/preprocess_worker.R` is shared because Process 02 prepares full
histories while Process 03 performs independent block-local cleaning without
future leakage. `src/r/util/time_series_input.R` provides common input conversion
for Processes 02–04. The single worker-path constant lives beside `run_r` in
`src/python/util/providers.py`; Process 03 does not import Process 02's application.
The exact source ownership rules are in [code standards](code-standards.md).

Researcher-facing model/output paths use semantic names. Internal identities
belong in DuckDB and MLflow; tool-managed IDs stay inside hidden runtime state.
Normal researcher execution writes directly to canonical root locations, with
no promotion subsystem, symlinks or review-workspace dependency. Temporary test
roots clean up after themselves. Source contains authored modules/tests, not
generated history, packaging metadata or caches.

## Before/after measurements

Baseline is the [cleanup checkpoint](https://github.com/rafmontano/shape_fm_poc/commit/c498dc6);
candidate counts cover retained active
first-party source only. Physical lines include comments and blanks. Folders
directly contain counted source; Python functions include nested definitions.
Vendored/generated code and environments are excluded.

| Measure | Before | Candidate |
| --- | ---: | ---: |
| Production Python files / folders | 59 / 2 | 34 / 8 |
| Production Python lines | 26,419 | 4,451 |
| Python functions / classes | 682 / 77 | 254 / 25 |
| Python test files / lines | 37 / 14,776 | 13 / 1,122 |
| Production R files / folders | 22 / 3 | 13 / 4 |
| Production R lines | 3,582 | 1,989 |
| R test files / lines | 9 / 1,703 | 7 / 1,122 |

The former numbered scripts, generic coordinator/handler/planner dispatch,
duplicate provider/store wrappers and custom scheduling are retired. The current
process path owns its science and atomic results while shared integrations own
transport, scheduling and tracking. Git preserves implementation history.

## Final acceptance evidence

The canonical experiment is `287166f8-48ce-4bc2-b5de-584f51752775`; its existing
finished MLflow run is `e73fc135fdc54cbca127dc0069ee1942`.

- Both stores open read-only with one matching experiment owner. All six
  processes and 24 subflows are accepted.
- The accepted 100-series experiment contains 732 modelling inputs, 495 cohort
  references, 595 feature vectors and 595 embeddings, 1,600 forecasts, 15 fitted artifacts,
  200 directional vectors and 100 combinations. Evaluate accepted 1,700 forecast
  evaluations, 1,900 directional evaluations, 1,057 diagnostic candidates and
  three retained diagnostic outputs.
- Distributed Forecast completed in 2,128.915 seconds with Mac CPU, Ubuntu CPU
  and Ubuntu GPU participation. Required outputs and accepted-work restart were
  accepted; focused TF32 tests passed. No scientific calibration was performed.
- The completed 2.051-second no-new-work restart preserved accepted rows and
  timestamps. It is sufficient evidence and must not be repeated for final review.
- All 15 fitted references resolve with stored sizes and SHA-256 hashes. Semantic
  path normalization changed only operational model paths, not model bytes,
  settings, results, identities, configuration or accepted timestamps.
- Five CSVs and six figure files are present. The three PNG figures were
  inspected with readable, unclipped labels/legends and the dependent-horizon
  caveat; no presentation defect or regeneration was required.
- The existing MLflow run is `FINISHED`, with 28 curated metrics, 60 operational
  completion/progress fields, five parameters and 11 resolving published
  artifacts. It remains the same experiment/run; no metrics or artifacts were
  relogged. Models remain outside MLflow artifacts.
- Final non-scientific checks cover Python compilation/imports, retained R
  parsing, mocked interface/path tests, read-only database/model/output
  resolution, living-document links, stale active paths and whitespace. The
  shared-worker ownership correction does not alter accepted experiment files.

## Current status and remaining action

The complete candidate is presented for researcher review, not X01 closure.
Review the canonical Table 1, Daily Table 2, Figure 2 and standalone CD outputs;
do not regenerate outputs or run a second scientific workload or automatic
historical comparison. Only the researcher decides whether a final result is
materially problematic and whether X01 may close.

Keep `poc2-x01-architecture-transformation` committed and published without
merging `main`. Ubuntu source synchronization is revision-only: fast-forward
when reachable, preserving stashes and verifying a clean worktree and stopped
services. An unavailable host leaves source synchronization pending, not science.
