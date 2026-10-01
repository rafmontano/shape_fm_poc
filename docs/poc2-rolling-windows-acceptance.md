# POC2 rolling-window and S1 acceptance record

Date: 1 October 2026
Scope: approved IDs 011 and 016 only

## Implementation status

Configuration version 5 implements the approved combined preparation workflow:
ten centrally validated frequency definitions, derived W+H strides, official
training-boundary protection, complete tsai windows, one persisted S1 allocation
by namespaced original-series identity, robust Gate 2 cleaning, independent
per-window `standardise_sample_v1`, normalized parent/child DuckDB persistence,
idempotent resume, read-only retrieval, CLI opt-in and interactive R QA.

The production configuration selects M4 Daily only. Features, labels, Mantis
adaptation/training/prediction and other GIFT-Eval datasets were not implemented.

## Validation completed on Mac

- 54 focused Python tests passed. They cover all ten settings, the exact Daily
  L=156 example, exact/short/remainder/offset boundaries, missingness and
  infinity, constant inputs, S1 counts and namespaced identities, placement
  independence, real R cleaning, configuration persistence, retrieval,
  source-manifest rejection, interrupted parent reconciliation and
  duplicate-free resume. Distributed workers also use the profile-owned
  admission budget and continuous owned-R-process memory monitor.
- All focused R standardisation tests passed, including sample-SD agreement,
  constants, inversion and explicit context behavior.
- Python compilation, R QA-script parsing and `git diff --check` passed.
- The broader Python discovery run passed 172 tests and could not import one
  GIFT-Eval semantics module from the general project environment; that module's
  dependency is isolated in the pinned GIFT-Eval environment and was not changed.
- A fresh isolated import read the first 100 official M4 Daily series: 57,235
  observations and 100 archived reference means. No accepted database changed.
- A bounded live calculation over those official training boundaries found 100
  eligible series, zero short series, 632 complete windows and 5,139 unused
  trailing observations. S1 produced 80 train series/493 windows and 20 test
  series/139 windows. Two real windows from series 0 were robust-cleaned and
  independently standardized; boundaries were `[0,64)→[64,78)` and
  `[78,142)→[142,156)`.
- A bounded persisted integration wrote all 12 windows for official series 0 to
  an isolated child, retrieved window 0 through the R QA script, and confirmed
  its raw input/future, S1 partition, transformed input, fitted state and inverse.

No compatible historical membership artifact with a verified canonical
dataset/series mapping was imported. This run therefore records
`generated_new_allocation`: tsai 1.0.1 receives the sorted namespaced cohort,
explicit test count 20 and seed 123. It is not claimed bit-identical to R's
`sample()` allocation. ShapeFM maps tsai's two-way held-out output to S1 test;
no validation partition is persisted.

The local calculation proves the scientific boundary and worker contracts; it
is not the required distributed 100-series persistence acceptance.

## Outstanding distributed acceptance

Ubuntu hostname `WSUbuntu1.local` did not resolve during implementation. Under
the execution policy, no heavy Mac-only substitute was run. Ubuntu dependency
synchronization, matching two-host source manifest, actual 8-Mac/15-Ubuntu task
contribution, overlap/resource evidence, full child persistence/retrieval on the
100 official series and published-revision Ubuntu synchronization therefore
remain outstanding. GPU workers are not needed for this CPU-only workflow.

The new direct dependency is pinned as `tsai==1.0.1`. Its required fastai,
PyTorch, NumPy and scikit-learn stack materially enlarges the general Python
environment; this is a current packaging limitation rather than duplicated data.
Lock regeneration added the required graph without changing or removing any
previously locked package version.

## Manual QA

After the distributed child database exists, run from a repository-root R
session:

```r
ROLLING_QA_PARENT_DATABASE <- "results/poc2_m4_daily_100_rolling_windows.duckdb"
ROLLING_QA_WINDOWS_DATABASE <- "results/poc2_m4_daily_100_rolling_windows.windows.duckdb"
ROLLING_QA_DATASET_ID <- "gift_eval/m4_daily/REPLACE_WITH_STORED_ID"
ROLLING_QA_SERIES_ID <- "0"
ROLLING_QA_WINDOW_ORDINAL <- 0L
source("src/r/qa/inspect_rolling_window.R")
```

Inspect `selected_rolling_window`, `rolling_raw_input`, `rolling_raw_future`,
`rolling_transformed_input`, `rolling_transformation_state` and
`rolling_restored_cleaned_input`. The script opens both databases read-only and
leaves these objects in the interactive workspace.
