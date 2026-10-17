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
is stored under the fixed `models/` root. The fitting provider serializes its
native object to a temporary local file and returns its path, host and provider
metadata. The coordinator moves a local fit or retrieves a remote fit with
rsync over SSH, publishes the complete canonical file, then commits model
completion and lineage in DuckDB. Successful publication removes the temporary
fit. No model bytes are carried inside JSON messages.

- An accepted training item is reused, never silently reopened.
- Only pending training items are fitted and published.
- Missing worker copies may be recopied from the coordinator.
- Loss of an authoritative accepted model stops execution and requires researcher
  direction before retraining; prediction never fits models.

DTW stores one multi-horizon artifact per frequency. Random Forest stores one
classifier per frequency/horizon. Adding an approved model calculates only its
pending delta; deleting a file does not delete its completed training record.

The canonical Daily paths are:

```text
models/directional_dtw/daily/model.pickle
models/directional_mantis_rf/daily/horizon_01.pickle ... horizon_14.pickle
```

Scientific experiment IDs and lineage live in DuckDB, not visible model paths.
Models are not MLflow artifacts. The downloaded frozen Mantis checkpoint stays
in the setup-owned pretrained-model cache and is loaded offline by its native
library; runtime does not qualify package versions or hash checkpoint contents.

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

Before pending 04.04 work is submitted, the coordinator prepares required model
files once on eligible CPU hosts using incremental rsync over SSH, retaining the
same relative paths under each project root. Rsync publishes complete files
through its ordinary temporary-file rename; there is no bidirectional merge,
checksum gate, registration protocol or prepared-host ledger.

Prediction requests contain input values and local model paths. Each worker
process owns one classifier session per environment, reused across work groups.
A lock prevents concurrent callers from interleaving requests and responses on
its stream. The provider loads each local file once and keeps the model in
memory until that session ends. JSON carries ordinary requests/results only.
Copy/load failures retain the native error and path/host context. Ordinary file
operations and completed prerequisites are trusted; ShapeFM does not separately
attest that an internal file remained unchanged between saving and loading.
