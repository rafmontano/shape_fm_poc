# POC2 ID 026 fitted meta-learner storage

Status: Architecture approved by the Researcher and Chief Architect on
6 October 2026. Implementation and acceptance remain pending.

## Decision

ShapeFM will persist expensive fitted meta-learners outside DuckDB through one
small shared storage object. ID 026 works with IDs 027 and 028 as one evolution
of the current directional-model candidate, not as a separate subsystem.

The initial managed models are:

- directional DTW: one fitted multi-horizon object per frequency; and
- Mantis/Random Forest: one fitted Random Forest per frequency and horizon.

Ordinary forecast-pool methods remain bounded fit-and-forecast operations. ID
026 does not persist their per-series fitted objects.

The historical `src/python/model_io.py` is replaced by design, not imported.
Its useful save/load behavior is retained while centralizing paths, respecting
frequency, supporting skip or overwrite, and separating training from
prediction.

## Simple storage layout

```text
models/
`-- <experiment>/
    |-- directional_dtw/
    |   `-- D/
    |       `-- model_all_horizons.joblib
    `-- directional_mantis_rf/
        `-- D/
            |-- model_h01.joblib
            |-- ...
            `-- model_h14.joblib
```

The model root and experiment namespace are centrally configured. Scientific
scripts never construct these paths. The directory remains ignored by Git.

## Shared model-storage object

`src/python/util/shared_model_storage.py` owns one documented `ModelStorage`
class with the minimum operations:

```text
exists(model, frequency, horizon_scope)
save(model_object, model, frequency, horizon_scope, overwrite=false)
load(model, frequency, horizon_scope)
```

`exists` checks the real file. `save` writes to a temporary file, proves that
the provider can load it, and then replaces the final path atomically. `load`
returns the fitted object or raises a clear missing/unreadable-model error with
model, frequency and horizon scope.

`overwrite=false` is the default:

- existing file: return `skipped_existing` and do not fit;
- missing file: fit and save;
- overwrite requested: fit and replace only after the new file is valid.

The current MVP uses trusted project-owned `joblib` files for both Python
meta-learners. No sidecar catalogue, model generations, promotion workflow or
new artifact service is required. A checksum, file size and relative path may
be recorded in existing execution evidence after save/load; they do not require
a new model-registry schema.

## Process flow

The current ID 027/028 version-11 candidate remains the base. Training and
prediction become explicit Process 04 substeps while the six-process workflow
remains unchanged:

```text
03 common directional preparation
 |
 |-- 04.04 train DTW
 |-- 04.05 encode Mantis
 `-- 04.06 train Random Forest

all requested fitted meta-learners available
 |
 |-- 04.07 predict DTW
 `-- 04.08 predict Random Forest

05 directional no-work
06 evaluate
```

Use these human-readable executable names:

- `04_04_train_directional_dtw.py`;
- `04_05_encode_mantis.py`;
- `04_06_train_random_forest.py`;
- `04_07_predict_directional_dtw.py`; and
- `04_08_predict_random_forest.py`.

The existing Process 04 and shared utility components retain the scientific
logic. This is a focused split of responsibilities, not a new orchestration
path.

## DTW object

DTW has no conventional estimator, but its reusable fitted state is clear:

- complete S1 reference identities;
- finite float64 length-64 reference arrays;
- fourteen binary-label columns; and
- the selected effective width for each of the fourteen horizons.

Store that state once in `model_all_horizons.joblib`; do not duplicate the
reference library per horizon. Calibration scores and accepted lineage remain
in DuckDB. Prediction loads this object and performs the accepted direct aeon
1.6.0 neighbour calculation without recalibration.

ID 026 changes DTW lifecycle only. It does not change membership, cleaning,
standardisation, labels, width candidates, local cost, calibration exclusion,
tie rules, distance engine or final prediction mathematics.

## Mantis/Random-Forest objects

The frozen Mantis checkpoint remains a pinned provider dependency and is not
copied per experiment. Persisted representations remain reusable training and
prediction inputs.

Train and save one Random Forest per horizon using the already approved
settings: 200 trees, seed 42, one internal thread and complete S1 training
membership. Prediction loads the fourteen saved classifiers and never calls
`fit`.

## DuckDB and restart

DuckDB continues to own configuration, task state, scientific lineage,
predictions and evaluations. The fitted objects remain ordinary files outside
DuckDB. Existing execution/task evidence records whether an artifact was
trained, overwritten, skipped, loaded, missing or unreadable; a separate global
or experiment model catalogue is not required for this MVP.

Actual file existence takes precedence over an old completion row. If the
researcher deletes a file, training recreates it and prediction reports it
missing until training succeeds. If overwrite is requested, reset only that
model scope and its dependent predictions/evaluations. Adding another model to
unchanged data skips existing files and calculates only the new delta.

## Mac and Ubuntu

Mac remains the authoritative model-file and DuckDB owner. A model trained on
Ubuntu is saved to a known staging path and copied to the same experiment/model
relative location on Mac using the existing secured machine-transfer mechanism.
After every required training file is consolidated on Mac, synchronize that
experiment's model directory to Ubuntu once before distributed prediction.

Both machines then resolve the same logical relative paths through
`ModelStorage`. Model bytes are transferred as files, not embedded in the JSON
scientific contract. No new cache service or per-model materialization protocol
is required.

## Configuration

Add only:

- the model root/experiment namespace through the existing centralized
  configuration and execution-profile boundary; and
- `overwrite=false`, with an explicit bounded override when retraining is
  requested.

The storage layer does not infer whether changed data invalidates a model. A
material data change uses explicit overwrite, researcher deletion or a fresh
experiment namespace, as already approved in the requirements.

## MVP acceptance and closure

The implementation must prove:

1. missing objects train and save;
2. existing objects with overwrite disabled skip fitting;
3. overwrite safely replaces the selected object;
4. a failed save preserves the prior object;
5. saved and loaded DTW and Random-Forest objects reproduce their in-memory
   predictions;
6. a missing file blocks prediction and is recreated by training;
7. the five Process 04 substeps and dependency barrier execute through Prefect
   and Dask;
8. the first 100 M4 Daily series produce one DTW model file, fourteen Random-
   Forest files, 1,400 predictions per directional model and 28 evaluations;
9. a restart performs no DTW calibration or Random-Forest fitting and preserves
   the accepted scientific outputs; and
10. existing ID 018 forecast results and ID 021 DTW mathematics remain
    unchanged.

Complete the implementation, local tests and real Mac/Ubuntu 100-series run as
one gated delivery. If all evidence passes, update the acceptance documentation,
commit the integrated ID 026/027/028 change, fast-forward Mac, Ubuntu and
GitHub `main`, and confirm clean equality. No generated model, database, cache,
log or report is committed.

Successful evidence formally closes ID 026 and IDs 027/028. ID 021 remains
scientifically closed with this accepted storage-lifecycle addendum.
