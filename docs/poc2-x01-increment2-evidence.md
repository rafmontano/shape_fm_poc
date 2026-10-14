# POC2-X01 Increment 2 implementation evidence

14 October 2026. **Implemented and locally validated; researcher acceptance is
pending.** Scope and authority: [X01 transformation](poc2-x01-architecture-transformation.md).
This is not authority to begin Increment 3 or run Processes 02–06.

## Implementation and boundaries

Normal commands are `python src/python/00_main.py run --processes 1` and
`python src/python/00_main.py status --format human`. `ActivePaths` owns fixed
bootstrap/parent/child paths; alternate layouts are restricted to temporary test
roots. Bootstrap stores original/resolved configuration; existing parents are
authoritative and cannot be silently overwritten. An orphan child blocks bootstrap.

`results/experiment.duckdb` owns configuration, canonical data, process/task state
and later scientific outputs. Process 01 creates this parent only.
`results/windows.duckdb` is the same experiment's required Process 03 child for
rolling-window preparation, membership, transformed inputs and labels/features.
Status reads both logical roles and stored linkage metadata, without repairing,
recalculating or validating scientific outputs. No third scientific store exists.
Process 03 fixed-child execution wiring remains deferred.

Schema 14 adds concise `process_substeps` checkpoints. `ImportState` owns state,
counts, attempts, timestamps, failure location and persisted-output fingerprints.
Thin Prefect subflows sequence existing source/storage objects. Substep 01.01
commits canonical items; 01.02 persists import acceptance. Completed boundaries
skip work; incomplete canonical imports retain atomic item commits. Storage reads
check fingerprints on restart; ordinary status does not. No new scheduler or
duplicate scientific payload is introduced. Small test helpers and thin Prefect
functions are the existing practical OOP exceptions, not new object frameworks.

GIFT-Eval is the sole canonical authority. Both import routes no longer acquire
M4comp2018 references or compare histories. The inactive R adapter/test are retained
only for explicitly requested research reproduction. Existing reference schema
and downstream readers remain unchanged. Historical configuration fields remain
unchanged for identity compatibility but do not gate import. No scientific
calculations, dependencies or pinned settings were changed.

## Focused verification

The first candidate's synthetic fixture failed at the former M4 history gate.
The researcher explicitly authorised removing that production comparison.
After correction:

- 35 tests passed in 73.486 seconds: `tests.test_progress`,
  `tests.test_active_import`, `tests.test_import`, `tests.test_table_configuration`.
  Coverage includes bootstrap, fixed/test-only paths, no overwrite, JSON-free
  restart, durable counts, item and acceptance failures/retry, corrupted persisted
  output rejection, legacy states, human/JSON compatibility and byte preservation.
  Table-configuration checks use fixtures/mocks; no later scientific process ran.
- Nine adapted stored-reference fixture tests passed separately. They preserve
  reader contracts without invoking the retired adapter or scientific workflows.
- All 14 changed/new Python files compiled. Changed-path whitespace and local
  Markdown link checks passed. No historical full acceptance matrix was run.
- Two-file fixtures distinguish pending child, missing required child and
  incompatible linkage, and preserve both database files across status requests.

## Fresh pinned 100-series run and restart

Before execution, neither active database nor active JSON existed, and no named
ShapeFM/Prefect/Dask/R/Chronos execution was running. The unchanged template
`config/experiments/poc2_m4_daily_100_paper_tables.json` was copied to the ignored
`config/experiment.json`. The ordinary entry point ran **Process 01 only**.

GIFT-Eval source revision: `30841734ac5cfddbd0c3bad6d09d2b6b32becbb0`.
Scientific hash: `c2635a31d638f70f384355f8df542c8b96ad9f36d310d904d6e46ed887433a22`.
Configuration integrity: `81f93eecf2f8d9563bb62c558def2fd055fdc479e5b4e9fdafd6ebff30ac6bc7`.
The run tested the uncommitted Increment 2 candidate atop published Increment 1;
stored source revision therefore records the Increment 1 base, not the later
delivery commit. Subsequent changes were documentation only.

Observed Process 01 start/completion: 14:17:32–14:17:38 +11:00.
Exactly 100 canonical series and 100 completed item tasks were stored; zero M4
reference rows were imported. Both substeps recorded 100 planned, 100 completed,
zero failed items and one attempt. Processes 02–06 remained pending.

```text
Imported series: 100
Main database: results/experiment.duckdb — available
Windows database: results/windows.duckdb — not created (Process 03 pending)
Process 01 Import: completed
  01.01 Import canonical GIFT-Eval dataset: completed
  01.02 Persist and accept canonical import: completed
Process 02 Preprocess: pending
Process 03 Transform/window preparation: pending
Process 04 Forecast: pending
Process 05 Combine: pending
Process 06 Evaluate: pending
Latest recorded error (record timestamp): not recorded
ETA: not recorded
```

The bootstrap JSON was temporarily moved away, then the same Process 01 request
returned `skipped_completed`; JSON was restored afterward. Full ordered snapshots
of `series`, `evaluation_windows`, `task_attempts`, `process_substeps` and
`experiment_processes` were identical before/after. Restart appended operational
execution history, but did no new scientific work.

SHA-256 before/after repeated JSON and human status (identical pairs):

| Inspection | Before = after |
| --- | --- |
| Fresh run | `0ac4de1b21f2d518b58a3cfa98408bdb728a87e35e9fcd213ebec09d4b5586a7` |
| After database-only restart | `3a18507abb0bce51de41aa67f765bf27a19fcb50b271a1097c268131944d569b` |

Repeated fresh-run human output was byte-identical and JSON status objects were
equal. The restart hash differs because execution history legitimately changed.
The child remained absent; `results/` contained only `experiment.duckdb`. No
experiment-named output/evidence directory or root CSV/PDF/PNG was produced.

## Cleanup and retained workspace

Earlier approved cleanup removed obsolete experiment databases/WALs, results and
evidence, experiment-specific models, Prefect/Dask runtime and old acceptance
scratch on Mac and Ubuntu, after checking writers were stopped. Ubuntu source
remains at accepted Part B and its five existing stashes were preserved; no source
synchronisation occurred. Final local cleanup removes the newly generated Prefect
runtime and duplicate/raw test snapshots, retaining concise review logs only in
ignored `.amp/in/artifacts/` (not a researcher-facing result/evidence directory).

Remaining Mac top-level directories:
`.amp`, `.cache`, `.git`, `.idea`, `.Rproj.user`, `.tools`, `.venv`, `config`,
`data`, `docs`, `environments`, `external`, `models`, `renv`, `results`, `scripts`,
`src`, `tests`, `tmp`, `workflows`.

Retained generated/cache locations and reasons:

- `results/`: the new active parent database for review; `models/` is empty.
- `data/source/gift_eval/`: pinned authoritative source data, not disposable output.
- `.cache/`: downloaded Hugging Face/Mantis/Chronos assets and download caches.
- `.amp/in/aeon-numba-mac/` and `.amp/in/directional-worker-cache/numba/`:
  reusable compiled DTW caches. Ubuntu's `numba-cache-id021-dependency` was retained.
- `.amp/in/artifacts/`: concise local verification records, ignored by Git.
- `.venv`, `.tools`, `environments`, `renv`, `external`: installed toolchains,
  environments and pinned dependencies/source; not rebuilt or deleted.
- `.idea`, `.Rproj.user`: preserved IDE state. Ordinary Python bytecode caches
  inside retained source/environments are incidental caches, not experiment data.
- `tmp/inactive/`: tracked historical reproduction source, not generated residue.

## Exact delivery paths and limitations

Changed/new paths:

```text
.gitignore
README.md
docs/architecture.md
docs/archived-m4-forecasts.md
docs/experiment-configuration.md
docs/local-execution.md
docs/poc2-x01-architecture-transformation.md
docs/poc2-x01-increment2-evidence.md
src/python/tests/test_active_import.py
src/python/tests/test_m4_reference.py
src/python/tests/test_progress.py
src/python/tests/test_table_configuration.py
src/python/util/p00_01_researcher_actions.py
src/python/util/p00_02_researcher_cli.py
src/python/util/p00_04_progress.py
src/python/util/p01_01_import_flow.py
src/python/util/p01_02_import_execution.py
src/python/util/p01_04_import_state.py
src/python/util/shared_active_paths.py
src/python/util/shared_configuration.py
src/python/util/shared_database.py
src/python/util/shared_process_storage.py
src/r/01_02_import_m4comp2018.R → tmp/inactive/01_02_import_m4comp2018.R
src/r/tests/test_import_m4comp2018.R → tmp/inactive/test_import_m4comp2018.R
```

The continuing delivery branch is `poc2-x01-architecture-transformation`.
Main and safety branch remain at accepted Part B; no merge, tag or new branch.
Only the above source/tests/docs and ignore rule belong in the commit, never
bootstrap JSON, databases, logs, models, caches or results.

Deferred: Process 02 transformation; Process 03 production fixed-child wiring
and source-link lifecycle; later model/export paths; separately authorised M4
reproduction-reference provisioning. The later legacy state model has no durable
substep progress, heartbeat or ETA. Stored experiment state is not recorded until
later processes; recorded running is not evidence of live execution. No claim is
made about later scientific execution or comparative accuracy. Stop for review.
