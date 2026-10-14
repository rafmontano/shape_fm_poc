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
- Sakoe-Chiba proportion `0.10` (effective width 6);
- one nearest reference per evaluation input;
- the neighbour's stored labels reused for horizons 1–14; and
- equal distances resolved by the lowest stable reference identity.

There is no width calibration in the current design, no probability output and
no alternative DTW engine or fallback. Dask distributes bounded CPU work; the
worker holds no database connection and the coordinator commits results.

DTW's fitted artifact contains the reference identities/arrays/labels and fixed
width once per frequency. Prediction loads it and never recalibrates.

## Mantis/Random Forest baseline

The researcher-facing model is `directional_mantis_rf`, composed from:

```text
frozen Mantis representation → reusable Random Forest classifier
```

- Pinned `paris-noah/Mantis-8M` checkpoint.
- Linear resize from 64 to 512, `align_corners=false`.
- Legacy-compatible final-transformer-layer CLS representation.
- Frozen float32 representation of dimension 256.
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

## Parallel and restart behavior

Prefect orders preparation, representation/training and prediction. Dask places
eligible work across configured machines. Completed artifacts and predictions
are skipped on restart. Worker/host/batch changes do not change model identity or
predictions. The coordinator alone writes DuckDB and owns the authoritative
model-file copy.
