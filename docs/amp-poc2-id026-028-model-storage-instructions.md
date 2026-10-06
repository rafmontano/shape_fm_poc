# AMP instructions: close POC2 IDs 026, 027 and 028

Status: Approved implementation and acceptance instructions, 6 October 2026.

## Objective

Complete the existing uncommitted version-11 ID 027/028 candidate by adding the
small ID 026 fitted-model save/load layer. Treat IDs 026, 027 and 028 as one
integrated delivery. This is an evolution of the current candidate, not a new
architecture or another checkpoint series.

## Starting state

Preserve the complete current candidate based on `52b33a8` and all approved
documentation. Do not reset, restore, stash or discard it. Verify the starting
manifest and stop only for unrelated changes or a different base revision.

Read `AGENTS.md`, the code standards, execution policy, ID 021 decision, ID 026
requirements/design, and ID 027/028 decision before editing.

Retain the existing version-11 work: common Process 03 inputs, normalized
model-neutral predictions/evaluations, provider lineage, correct scientific
IDs, separate operational provenance, independently scheduled horizons and
version-10 compatibility.

## Implement the MVP

Add `src/python/util/shared_model_storage.py` with one small documented
`ModelStorage` class providing `exists`, `save` and `load` for centrally
resolved experiment/model/frequency/horizon paths.

- Use trusted project-owned `joblib` files.
- Default to `overwrite=false`.
- Save to a temporary file, validate by loading, then atomically replace.
- Existing plus no overwrite reports `skipped_existing` and does not call fit.
- Missing trains and saves.
- Explicit overwrite retrains and safely replaces.
- Missing or unreadable files fail prediction with the exact logical model
  scope.
- Do not add a sidecar catalogue, new registry service, generations, archive,
  quota or user interface.

Use the approved external paths:

```text
models/<experiment>/directional_dtw/<frequency>/model_all_horizons.joblib
models/<experiment>/directional_mantis_rf/<frequency>/model_hNN.joblib
```

The model root and experiment namespace come through central configuration;
scientific scripts do not construct paths.

## Integrate the Process 04 flow

Use these executable responsibilities and update every active reference:

```text
04_04_train_directional_dtw.py
04_05_encode_mantis.py
04_06_train_random_forest.py
04_07_predict_directional_dtw.py
04_08_predict_random_forest.py
```

Preserve and refactor the current worker/component code; do not duplicate it.
Prefect enforces this dependency:

```text
Process 03
  |-- train DTW
  `-- encode Mantis -> train fourteen Random Forests

all fitted model files available
  |-- predict DTW
  `-- predict Mantis/Random Forest
```

Prediction code must contain no fit, calibration or fallback.

## DTW

Persist one complete trusted `joblib` object per frequency containing the
shared reference identities, finite float64 reference matrix, fourteen label
columns and fourteen selected widths. Do not duplicate the reference library
per horizon. Prediction loads that object and uses the accepted direct aeon
1.6.0 calculation.

Do not change any accepted ID 021 scientific setting or version-10 behavior.
With an existing model file and overwrite disabled, skip calibration.

## Mantis/Random Forest

Keep Mantis frozen and preserve the current representation contract. Train and
save one Random Forest per horizon with 200 trees, seed 42, one internal thread
and the complete S1 membership. Prediction loads those files and never fits.

Do not persist the Mantis provider cache as an experiment model. Do not persist
ordinary forecast-pool fitted objects or modify ID 018 calculations.

## DuckDB, overwrite and distributed files

Use existing task, execution and scientific records to capture trained,
overwritten, skipped, loaded, missing or unreadable outcomes plus relative path,
size and checksum where the existing evidence contract permits. Do not create a
new global or experiment model registry merely for ID 026.

Actual file existence controls availability. Explicit overwrite resets only the
selected model scope and its dependent prediction/evaluation records. Adding a
new model preserves completed unrelated work.

Mac remains authoritative. Ubuntu training writes a staged file; consolidate
it to the corresponding Mac model path with the existing secured transfer.
After all training files are available on Mac, synchronize the experiment model
directory to Ubuntu once, then run distributed prediction. Transfer files, not
model bytes in JSON.

## Validation and delivery

Use temporary local roots for focused tests and prove:

- save/load prediction parity for DTW and Random Forest;
- existing-file skip invokes no fit/calibration;
- safe overwrite and failed-overwrite preservation;
- manual deletion is detected;
- missing model blocks prediction;
- one missing RF horizon does not retrain the other thirteen;
- DTW reference data is stored once;
- prediction contains no training;
- version-10 DTW behavior and existing forecast behavior remain unchanged;
- no generated artifacts are tracked.

Run the complete relevant locked/no-sync local suites. If they pass, proceed in
the same delivery:

1. create the integrated implementation commit;
2. transfer that exact revision to the clean Ubuntu `main` without touching its
   existing stashes;
3. verify source, locks, environments, data, ports and process state;
4. run a fresh two-machine first-100 M4 Daily experiment with DTW and
   Mantis/Random Forest;
5. require one DTW fitted file, fourteen RF files, 1,400 predictions per model,
   28 evaluations, zero accepted failures/duplicates, and eligible contribution
   from both machines;
6. rerun with overwrite disabled and prove zero calibration/fitting plus
   unchanged scientific results;
7. update the existing decisions and one concise execution-evidence/closure
   record;
8. commit the closure documentation separately;
9. push `main` by normal fast-forward; and
10. fast-forward/verify Mac `main` = Ubuntu `main` = `origin/main`, with both
    worktrees clean.

Stop and preserve evidence if implementation, preflight or scientific execution
fails. Do not force-push, amend, tag, install unapproved dependencies, alter
lockfiles, commit generated models/databases/logs/caches, or disturb Ubuntu
stashes.

The final report must be concise: revisions, test results, task/result counts,
model-file counts and sizes, first-run/restart behavior, host contribution,
resource/failure summary, documentation updated, exclusions confirmed, and
three-way revision equality.
