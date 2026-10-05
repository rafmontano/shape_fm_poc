# POC2 ID 021 directional DTW baseline

Status: Approved architecture for implementation on 5 October 2026. The direct
`aeon.distances.dtw_distance` engine at aeon 1.6.0 supersedes the withdrawn
sktime candidate. Implementation and local validation do not constitute
scientific acceptance or authorise GitHub publication.

## Scientific definition

ID 021 is one one-nearest-neighbour directional classifier. Each input is the
64-point `standardise_sample_v1` result from one independently bounded robust R
preprocessing request. Labels are `directional_strict_v1` at horizons 1–14.
Training references are every S1 training window; there is no cap or sample.

Calibration evaluates candidate Sakoe-Chiba proportions 0.00–0.99 by 0.01.
For equal-length 64-point inputs, the effective width is `int(window * 64)`, so
the 100 configured proportions map to 64 distinct widths and each effective
width is calculated once. Distance is direct aeon DTW with float64 inputs and
squared Euclidean local cost. Calibration excludes every reference from the
query's source series. Equal accuracy selects the smallest effective width;
equal distance selects the lowest stable reference-window identity. One width
is selected independently for each horizon.

Final prediction uses the complete S1 training library without same-series
exclusion. Horizons sharing a selected width share one neighbour search. The
neighbour's corresponding binary label is copied with its stable identity,
finite distance and effective width. ID 021 does not calculate probabilities,
serialize a fitted model, or write forecast rows.

## Preparation and leakage boundary

Process 03 creates or resumes the existing rolling-window/S1 preparation and
its centralized labels; it does not create a DTW-specific copy. Each official
input follows the same scientific worker path:

1. Select exactly the final 64 raw observations before the official origin.
2. Send only those observations to the pinned R robust-preprocessing worker.
3. Fit and apply `standardise_sample_v1` to that bounded cleaned result.
4. Retain the bounded cleaned final value as the strict-label reference.
5. Store input, cleaning and transformation hashes with worker provenance.

The fourteen official future values are used on the Mac only to persist
protected actual labels for Process 06. They are never sent in a Process 04
payload. Slicing the tail from a separately cleaned full history is not this
contract.

## Workflow and execution

```text
Process 03                    Process 04                    Process 05/06
bounded R clean + standardise calibration + prediction     no-work + accuracy
          |                         |                              |
          v                         v                              v
S1 child + official inputs -> direct aeon DTW blocks -> directional parent tables
                                    |
                        Mac and Ubuntu CPU workers
                        one computational thread each
```

`src/python/04_04_directional_dtw.py` is the only DTW-specific executable. It
owns calibration and prediction internally and calls aeon distance directly;
no aeon classifier, sktime, R DTW, Java, tsml, second engine or fallback is
permitted. The classifiers lock pins aeon 1.6.0. Actual aeon, NumPy, Numba and
scikit-learn versions, the lock digest, relevant calculation-source digests,
worker digest, repository revision, float64 dtype, preparation digest and
reference-library digest form the implementation provenance. Documentation
does not enter scientific identity.

Prefect owns the Process 04 flow and Dask schedules CPU blocks. A calibration
block is one distinct width × validation-query block; a prediction block is one
distinct selected width × evaluation-query block. The complete immutable
reference library is installed once through a content-addressed worker-local
cache and verified from values, source identities and labels before use. Worker
subprocesses use an explicit writable Numba cache and one internal thread. They
receive compact blocks, open no DuckDB connection and retain no complete
distance matrix. The Mac coordinator validates results and is the sole writer.

## Storage and restart

The parent DuckDB stores bounded official inputs, protected actual labels, one
model definition, 896 calibration scores, fourteen selected widths, 1,400
predictions, fourteen directional evaluations and one deterministic Process 05
no-work row. The linked windows DuckDB remains the normalized source of S1
references and labels. Every new directional record uses insert-or-verify:
existing identical content is accepted, while any conflict fails before its
task can complete. Scientific identity is independent of block size, host,
worker address and completion order.

Completed calibration, prediction and evaluation identities are skipped on
restart. A completed 100-series run must retain the same widths, predictions,
nearest-neighbour lineage, distances, counts and fingerprints with zero new
scientific work.

## Validation and acceptance boundary

The deterministic bounded fixture must agree exactly in sequential, local-Dask
and Mac/Ubuntu distributed modes for width mapping, calibration scores,
selected widths, neighbours, distances, predictions, evaluations and scientific
fingerprints. A bounded complete-reference timing and memory pilot precedes the
full run.

Complete acceptance uses the first 100 official M4 Daily series, every S1
training reference, all 64 effective widths, horizons 1–14, and 23 CPU workers:
8 Mac and 15 Ubuntu. Both hosts must perform calibration and prediction work.
It must produce 100 official inputs and actual-label rows, 896 scores, fourteen
widths, exactly 1,400 predictions, one Process 05 no-work row, fourteen
evaluations and zero DTW forecast rows. Accuracy is recorded but is not a pass
threshold. Resource, retry, failure, elapsed-time and host-contribution evidence
is required, followed by a no-new-work restart.

An unpublished local implementation checkpoint may be transferred directly to
Ubuntu for this test. GitHub publication, a closure record and scientific
acceptance require later Researcher and Chief Developer approval. Abandoned
`id021-100-series*` and `id021-bounded-*` artifacts from the withdrawn engine
must not be reused.
