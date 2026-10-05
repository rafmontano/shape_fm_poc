# POC2 ID 021 directional DTW execution evidence

Status: Accepted by the Researcher and Chief Architect on 6 October 2026. ID
021 is closed and approved for publication. Accuracy was recorded as a research
result, not used as an acceptance threshold. ID 022 was not started.

## Accepted source and environments

The implementation commit is
`5614b54673e7890677fb68dc22853df726eaf2ce`; the prediction-block provenance
correction is `53f4ff5d0b7c9a19c72c06106ca6d94f7dcc863b`. The latter exact revision
was synchronized directly between the clean Mac and Ubuntu worktrees before the
accepted run. The source manifest contained 119 files and had fingerprint
`79c86b8904b3e65a792eddf595e28f642fa784cc5dec42e8e367d53355440b95`.

Both hosts used the classifiers lock with SHA-256
`46eba332a4408d5a13e23887c8d4fb67b6afa96ec0c21ff7d51167d4bab6d46a`.
The numerical packages were aeon 1.6.0, NumPy 2.0.2, Numba 0.61.2 and
scikit-learn 1.7.2. The Mac used Python 3.12.14 on ARM64; Ubuntu used Python
3.12.3 on x86-64. Direct aeon DTW numerical fixtures agreed on both hosts.

The accepted model definition recorded these fingerprints:

| Item | SHA-256 |
| --- | --- |
| Scientific calculation source | `367cc5430641a92f88a09e636aba5ba233be3ecc723363077e095c369128f686` |
| DTW worker | `6f2a4f4de3010daa67090c2e2794d39acde13914d13a430d54d0f8feac9f3a27` |
| Reference library | `70156096b5fe130d3f6bc1edd0251ef8920a2f0e638ef3034d2e05a3690ec519` |
| Preparation | `5406fe61f631e5968e3ff60176e67420fb21246a533557e21a40f64a4bec678d` |
| Numerical implementation | `3ad1188e83ada416f58b52acaed20c78d57218e0ea4062e3d70ac36ee6634151` |

## Validation and execution

The focused numerical, directional-pipeline, bounded-preprocessing parity,
insert-or-verify conflict, provenance, reference-cache and fresh-Process-03
tests passed. The established locked fast, routing and applicable integration
suites also passed. A bounded end-to-end fixture produced exactly equal
effective-width mappings, calibration scores, selected widths, neighbours,
distances, predictions, evaluations and scientific fingerprints in sequential,
Mac-local Dask and Mac/Ubuntu distributed modes. In the distributed fixture,
Mac and Ubuntu completed respectively 24 and 40 calibration blocks and one and
six prediction blocks.

The accepted fresh experiment ran through Processes 01–06 using 8 Mac and 15
Ubuntu CPU workers, one computational thread per worker, no GPU, and the Mac as
the sole DuckDB writer. It used all 495 eligible S1 training references without
a cap or sample and all 64 effective candidate widths. Its stored result was:

- 100 official evaluation inputs and 100 protected actual-label rows;
- one DTW model definition and 896 calibration scores;
- 14 selected-width rows and exactly 1,400 binary predictions;
- one deterministic Process 05 no-work record and 14 evaluations; and
- zero DTW forecast rows, duplicate scientific identities, retries, failed
  invocations or accepted failures.

Calibration completed 15,389,696 distance calculations in 1,024 blocks: Mac
completed 350 and Ubuntu 674. Prediction completed 445,500 distances in 63
blocks: Mac completed 23 blocks and 164,340 distances; Ubuntu completed 40
blocks and 281,160 distances. Total DTW work was 15,835,196 distance
calculations in 1,087 blocks, with both hosts contributing to both phases.

Process 04 elapsed time was 103.793 seconds and total Process 01–06 elapsed time
was 237.968 seconds. Minimum available memory was 4,526,981,120 bytes on the Mac
and 125,427,179,520 bytes on Ubuntu. Peak child RSS was 259,063,808 bytes and
279,969,792 bytes respectively. Neither host recorded swap growth or Dask spill;
there were no worker removals, replacements, resource-threshold events, Prefect
failures or Dask failures.

## Scientific result

| Horizon | Selected width | Correct / evaluated | Accuracy |
| ---: | ---: | ---: | ---: |
| 1 | 4 | 56 / 100 | 0.56 |
| 2 | 3 | 55 / 100 | 0.55 |
| 3 | 25 | 56 / 100 | 0.56 |
| 4 | 3 | 59 / 100 | 0.59 |
| 5 | 0 | 56 / 100 | 0.56 |
| 6 | 11 | 54 / 100 | 0.54 |
| 7 | 13 | 57 / 100 | 0.57 |
| 8 | 13 | 54 / 100 | 0.54 |
| 9 | 11 | 52 / 100 | 0.52 |
| 10 | 11 | 62 / 100 | 0.62 |
| 11 | 11 | 57 / 100 | 0.57 |
| 12 | 10 | 60 / 100 | 0.60 |
| 13 | 9 | 57 / 100 | 0.57 |
| 14 | 1 | 42 / 100 | 0.42 |

## Restart and rejected preliminary attempts

The supported restart loaded the authoritative configuration from the accepted
DuckDB and reported all six processes as `skipped_completed`. It created zero
new task attempts, calibration work, predictions or evaluations. Counts,
timestamps, selected widths, predictions, nearest-neighbour identities and
distances, scientific fingerprints and complete stored-row hashes were
unchanged; duplicate counts remained zero.

The first bounded cache-barrier pilot was safely rejected because its harness
attempted reference-cache installation before all 23 workers had joined. The
corrected pilot waited for all workers and completed 31,616 distances in 9.245
seconds without spill or swap growth. Separately, a literal restart command
that repeated `--configuration` was rejected by CLI validation because an
existing database accepts only its stored authoritative configuration. The
supported restart omitted that creation-only argument and passed. Neither
rejected preliminary attempt changed accepted scientific state.

Abandoned `id021-100-series*` and `id021-bounded-*` artifacts from the withdrawn
sktime candidate were not reused as source, input or evidence for this aeon
run.
