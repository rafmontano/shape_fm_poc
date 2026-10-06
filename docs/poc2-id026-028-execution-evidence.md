# POC2 IDs 026–028 execution evidence and closure

Status: Implementation and two-machine acceptance approved; IDs 026, 027 and
028 closed by the Researcher on 7 October 2026. The machine-environment
acceptance condition is satisfied by this same campaign.

Accepted implementation revision:
[`e1cc469974048155cc51ad7b45c307c397bc4e4d`](https://github.com/rafmontano/shape_fm_poc/commit/e1cc469974048155cc51ad7b45c307c397bc4e4d).
The fresh version-11 experiment selected the first 100 official M4 Daily series
and compared the existing `directional_dtw` provider with `directional_mantis_rf`.

## Accepted execution

| Evidence | Result |
| --- | --- |
| First run | 304.97 seconds |
| Restart | 19.92 seconds |
| Completed Process 01–06 task counts | 100 / 100 / 100 / 1,430 / 1 / 28 |
| Fitted artifacts | One multi-horizon DTW model and fourteen horizon-specific Random Forest models |
| Predictions | 1,400 DTW and 1,400 Mantis/Random-Forest |
| Evaluations | Fourteen per model; twenty-eight total |
| Mac contribution | 515 host-attributed attempts |
| Ubuntu contribution | 900 host-attributed attempts |
| Mantis representation | Ubuntu CUDA |
| Failed attempts, retries, duplicates, missing results | Zero |

Both prediction providers successfully loaded their fitted objects in the
classifiers environment. All fifteen model SHA-256 values matched between Mac
and Ubuntu after synchronization. Provider environments own model serialization
and deserialization; the main coordinator verifies logical paths, file metadata,
worker evidence and byte integrity only. Mac owns the authoritative model
directory and the sole DuckDB writer. See the
[ID 026 storage decision](poc2-id026-fitted-model-storage.md) and
[ID 027/028 scientific contracts](poc2-id027-028-mantis-directional.md).

Prefect and Dask endpoints were derived from the
[machine inventory](machine-environment.md), without manually exporting an IP
address or `PREFECT_API_URL`: `http://RMMacbookPro.local:4200/api` and
`tcp://RMMacbookPro.local:8786`. The run used eight Mac CPU workers, fifteen
Ubuntu CPU workers and one Ubuntu GPU worker, one thread each. All owned
services and workers were stopped afterward.

## Restart and identity

Restart used the same database and model namespace, without the bootstrap JSON.
It skipped all six completed processes and added no task attempts. There was
no DTW recalibration or Random Forest retraining. All fifteen fitted-model
checksums, sizes and modification times remained unchanged, as did all twenty
scientific-table snapshots, predictions, evaluations and scientific fingerprints.
Restart reused the existing artifacts and results; it did not repeat prediction.

- Scientific fingerprint:
  `e5dc6a5b7a5d614d2cc0a365a17f1c4b224cb3b80d828f5713281b2f748af1b3`
- Configuration integrity fingerprint:
  `eeeeaf3dc7f55382eda6f2c4fe62bc550a82358a18d8111bc9f9da62560b6102`

## Acceptance boundary

This accepts execution, coordination, fitted-model persistence, reuse and
restart behaviour. It does **not** claim comparative scientific accuracy or
full-dataset performance. ID 021 remains closed; its DTW mathematics were not
changed. Earlier failed runs remain diagnostic evidence, not acceptance runs.
Raw databases, fitted models, logs, caches and generated reports are excluded
from Git; this document is the concise publication record.

The [7 October architecture snapshots](architecture.md#implementation-architecture-snapshot--7-october-2026)
are dated, descriptive references, not mandatory standards or new architecture
authorities.
