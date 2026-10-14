# Directional models and fitted artifacts

DTW and Mantis/Random Forest are comparable directional black boxes. They use
the same canonical Process 03 inputs, S1 membership, labels, horizons and
Process 06 output contract. Provider-specific conversion begins only after that
common boundary.

## Common input and output

- Daily input: finite `standardise_sample_v1` vector of length 64.
- Training membership: the persisted S1 training cohort.
- Evaluation inputs: official inputs, uncapped.
- Horizons: 1–14.
- Label: `directional_strict_v1` (`1` when future > reference, otherwise `0`).
- Output: one binary prediction per input/horizon with model and input identity.
- No future actual is present in a Process 04 prediction request.

The current full-Daily comparison caps the shared training cohort at 10,000
windows when more are eligible. Selection is deterministic with seed 1234 and
the approved first-horizon stratification. DTW and Mantis must reference the
same persisted cohort; test/evaluation inputs remain uncapped.

## DTW baseline

DTW is direct one-nearest-neighbour classification using
`aeon.distances.dtw_distance` from the pinned classifiers environment:

- float64 length-64 standardized inputs;
- squared-Euclidean local cost;
- fixed Sakoe-Chiba proportion `0.10` (effective width 6), identical across
  horizons 1–14;
- one nearest reference per evaluation input;
- the neighbour's stored labels reused for horizons 1–14; and
- equal distances resolved by the lowest stable reference identity.

There is no width calibration in the current design, no probability output and
no alternative DTW engine or fallback. Dask distributes bounded CPU work; the
worker holds no database connection and the coordinator commits results.

DTW's fitted artifact contains the reference identities/arrays/labels and fixed
width once per frequency. Prediction loads it and never recalibrates.

This is the authoritative active POC2/POC3 baseline, with the shared configured
maximum of 10,000 reference windows. Horizon-calibrated, uncapped version 12 is
historical only, preserved through Git history; it is not an active alternative.
Do not run calibration, tune for prior results or force historical equivalence.
Calculate each atomic item once on its assigned worker and reuse accepted DuckDB
state. Ordinary valid floating-point differences are accepted, with no parity
workload or tolerance gate. Final produced outputs go to the researcher, without
automatic historical comparison or a second scientific review workload.

## Mantis/Random Forest baseline

The researcher-facing model is `directional_mantis_rf`, composed from:

```text
frozen Mantis representation → reusable Random Forest classifier
```

- Pinned `paris-noah/Mantis-8M` checkpoint.
- Linear resize from 64 to 512, `align_corners=false`.
- Legacy-compatible final-transformer-layer CLS representation.
- Frozen float32 representation of dimension 256.
- CUDA TF32 matmul/convolution disabled to preserve the float32 calculation.
- One independent scikit-learn Random Forest per horizon.
- 200 trees, seed 42 and `n_jobs=1`.

Mantis does not fine-tune. The classifier is model-neutral and does not depend
on the Mantis package. The Mantis environment owns representation inference;
the classifiers environment owns Random Forest serialization and prediction.
Dask, not scikit-learn, owns parallelism.

## Fitted model lifecycle

Ordinary forecast models are not persisted. Expensive directional fitted state
is stored under the fixed `models/` root through one model-storage interface.
Provider environments save/load their own trusted artifacts. The coordinator
verifies the logical relative path, size, checksum and lineage without importing
provider libraries.

- Existing valid artifact + overwrite disabled: skip training.
- Missing artifact: train and publish atomically.
- Explicit overwrite: replace only after the new artifact is valid.
- Missing artifact at prediction: stop and require training; do not fit inside
  prediction.

DTW stores one multi-horizon artifact per frequency. Random Forest stores one
classifier per frequency/horizon. Adding a model calculates only the new delta;
deleting an artifact makes that scope eligible for training again.

The canonical Daily paths are:

```text
models/directional_dtw/daily/model.pickle
models/directional_mantis_rf/daily/horizon_01.pickle ... horizon_14.pickle
```

Scientific experiment IDs and lineage live in DuckDB, not visible model paths.
Path normalization moves these fifteen artifacts byte-for-byte, without training
or prediction, and updates only operational model-path fields in the accepted
database. Scientific settings, stored configuration, identities and checksums
are preserved. Models are not MLflow artifacts; none exist there, so no MLflow
metadata changes are needed for this normalization.

## Parallel and restart behavior

Prefect preserves Process 04's approved order: individual forecasts,
representations, decision-model fitting, directional predictions and forecast-pool
acceptance. Membership/features/representations are stored in the parent
`experiment.duckdb`; rolling modelling inputs are in `windows.duckdb`, created
only when Process 03 needs it. Dask places
eligible work across configured machines. Completed artifacts and predictions
are skipped on restart. Worker/host/batch/hardware/backend are operational
provenance, not model identity or grounds to recalculate accepted predictions.
The coordinator alone writes DuckDB and owns the authoritative
model-file copy.
