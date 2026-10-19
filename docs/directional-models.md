# Directional models and fitted artifacts

DTW and Mantis/Random Forest are comparable directional black boxes. They use
the same canonical Process 03 inputs, S1 membership, labels, horizons and
Process 06 output contract. Provider-specific conversion begins only after that
common boundary. Shared input semantics do not require identical model-specific
training populations.

## Common input and output

Use the previous project's clean-once, then window, then transform sequence as
specified in the data contract. Per-window cleaning is prohibited. Replacing
the platform must not silently change scientific preparation. The researcher
has approved restoration of the historical model-specific training populations
and historical DTW calculation below, delivered with dtw-python while retaining
the current tsai S1 membership.

- Daily input: finite `standardise_sample_v1` vector of length 64.
- Training membership: the persisted S1 training cohort.
- Evaluation inputs: official inputs, uncapped.
- Horizons: 1–14.
- Label: `directional_strict_v1` (`1` when future > reference, otherwise `0`).
- Output: one binary prediction per input/horizon with model and input identity.
- No future actual is present in a Process 04 prediction request.

Mantis/Random Forest uses all eligible rolling windows in the S1 training
partition, without a training cap. DTW alone uses at most 10,000 such windows.
When capping is needed, reuse the previous project's R first-horizon stratified
sampling procedure with seed 123, including its quota allocation, sampling and
final shuffle. At or below the cap, retain all eligible rows in their input order.
Persist model-specific ordered references; do not sort away the selected order
before fitting or prediction. Test/evaluation inputs remain uncapped.

Process 04 materialises Mantis representations for its full uncapped training
population and official evaluation inputs, not just the DTW subset. Resource
limits control execution batches, not the scientific training population. The
reference implementation is `m4_tsc_fmts_2026/src/r/08_split_index.R` and the
uncapped Mantis selection in `src/python/r_bridge_sktime.py`. Since S1 membership
remains tsai-based, this does not promise identical historical sample identities.

## DTW baseline

DTW is direct one-nearest-neighbour classification using `dtw-python`, the
[direct Python counterpart of R dtw](https://dynamictimewarping.github.io/).
Preserve the previous calculation from `src/r/13_baseline_dtw_real.R`, not its
implementation language. The Python package is `dtw-python` (import `dtw`),
not another similarly named DTW package. Pin a compatible version in the
existing classifiers environment; no new environment or R subprocess is needed.

- float64 length-64 standardized inputs;
- `dist_method="euclidean"` and `step_pattern="symmetric2"`;
- `distance_only=True`, `open_begin=False`, `open_end=False`; use `.distance`,
  not normalized distance;
- `window_type="sakoechiba"`, `window_args={"window_size": width}`; base integer width is
  `max(1, round(maximum_input_length * 0.10))`, capped for each pair by both
  lengths minus one, with minimum 1, exactly as in the previous helper;
- for Daily length 64, width 6 is reused across horizons 1–14;
- one nearest reference per evaluation input;
- the neighbour's stored labels reused for horizons 1–14; and
- equal distances resolved by the first minimum in the persisted reference
  order, as with R `which.min`, not by a newly sorted identity order.

There is no width calibration in the current design, no probability output and
no alternative scientific DTW engine. Preserve the previous helper's handling
of unusable pair distances; no usable neighbour fails the item rather than
inventing a prediction. Dask distributes bounded CPU work; the
worker holds no database connection and the coordinator commits results.

DTW's fitted artifact contains the reference identities/arrays/labels and fixed
width once per frequency. Prediction loads it and never recalibrates.

This is the researcher-confirmed reference baseline for this run, replacing aeon
DTW. Historical-method reproducibility and researcher trust take precedence over
introducing a new baseline. Bakeoff reconsideration is deferred to later research.
The [2017 univariate TSC Bakeoff](https://doi.org/10.1007/s10618-016-0483-9)
selects a DTW window through training-data cross-validation. This fixed-width
historical baseline is not claimed to reproduce that method. Moving to Bakeoff
requires an explicit decision on that scientific change, including the distance
definition and window selection for the horizon-specific labels. Do not add
tuning or a second baseline implicitly. Horizon-calibrated, uncapped version 12
is historical only, not an active alternative.
Do not run calibration, tune for prior results or force historical equivalence.
Calculate each atomic item once on its assigned worker and reuse accepted DuckDB
state. Ordinary valid floating-point differences are accepted, with no parity
workload or tolerance gate. The researcher-authorized saved-result comparison
for this correction follows the evaluation contract; it does not rerun the old
experiment or tune the new results to match it.

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
