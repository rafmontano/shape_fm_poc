# POC2 rolling-window and S1 acceptance record

Initial evidence: 1 October 2026. Corrected acceptance: 2 October 2026.
Scope: approved IDs 011 and 016 only

## Current closure status

The full 100-series v6 acceptance below was published in 22e41bb. Subsequent
review found two implementation misses: direct coordinator calls could omit
local limits, and the actual bounded path manually sliced windows after tsai was
tested only in a helper. Both are now corrected within the agreed design and a
fresh final-manifest two-host acceptance passed. IDs 011 and 016 are recommended
for closure after this reviewed revision is published and both hosts are safely
synchronised. Configuration v6 and all scientific settings remain unchanged;
earlier version-5 evidence is preserved.

## Implementation status

The initial configuration-version-5 implementation at 9728fce includes:
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

These are historical observations for the initial implementation, not acceptance
of the reviewed gaps. Direct review checks of the legacy R expression gave test
counts 1, 19 and 845 for eligible cohort sizes 10, 100 and 4227; the Decimal
implementation gave 2, 20 and 845. Preserve the reported 80/20 allocation above
as historical and use a fresh experiment for the approved rounding correction.
The count of 632 windows is not the required distributed 100-series persistence
acceptance; the live stored integration covered only the 12 windows of series 0.

## Corrected implementation and distributed acceptance

Configuration v6 uses `r_double_floor_v1`, validates supported/configured
frequency keys and positive W/H values, derives stride once, and accepts valid
changed W/H settings in a fresh experiment. The coordinator reads only a small
series index, retrieves bounded target slices, emits at most 16 windows per job,
and keeps one job per distributed task. Local execution requires explicit
`--local-max-series` and `--local-max-windows`; omission and exceeded bounds fail
before child or parent preparation writes. Retrieval and R QA validate parent
configuration, preparation/run linkage, membership, per-series source hash and
window boundaries.

The final safeguard applies inside `WindowPreparationCoordinator.run()` as well
as the CLI. Direct local calls require exactly two positive integer limits, with
policy maximums of 100 series and 200 windows. Missing, malformed, excessive and
falsely distributed calls fail before series selection, parent lookup writes,
child creation or worker launch. Distributed calls require the approved
`poc2_seasonal_recovery` profile and reject local-limit substitution.

The final bounded path reads at most one configured job chunk of complete W+H
blocks, invokes tsai 1.0.1 `SlidingWindow`, preserves its returned inputs and
absolute positions, and then filters already persisted ordinals. Future values
exist only transiently at the generator boundary and are not sent to R or stored.

Mac and Ubuntu tested identical runtime/lock fingerprints at source-manifest
`f39d4c9c8e832758c01dfc75a78fac6a0709deffb50e543b14f83e2868a7d221`,
GIFT-Eval submodule `4d5ab3fa0fe7451bbf59bb1ff6dd76e6e414d64a`, and tsai 1.0.1. The same 49
focused Python tests and R parse/rounding checks passed on each host. The pinned
GIFT-Eval semantics suite passed 2 tests on each host. The recorded renv
out-of-sync warning remains unchanged; required R versions were R 4.6.1,
forecast 8.24.0, jsonlite 2.0.0 and tsfeatures 1.1.1.

The fresh v6 parent imported 100 series and 57,235 observations. Independent
parent reconciliation and child persistence both produced 632 windows and 5,139
unused trailing observations, with zero short series. Membership is disjoint:
81 train series/495 windows/4,137 unused observations and 19 test series/137
windows/1,002 unused observations. All 632 window IDs and
`(series_key, window_ordinal)` pairs are unique; all 100 tasks completed once;
boundary reconciliation found zero violations.

The approved profile supplied 8 Mac and 15 Ubuntu CPU workers (23 active tuning
slots); no GPU worker was launched. Mac completed 21 jobs/148 windows and Ubuntu
79 jobs/484 windows. Their persisted completion ranges overlapped from
09:47:04 to 09:47:25 local time. Continuous memory evidence recorded no safety
response or swap growth; minimum available memory during work was 3.64 GiB on
Mac and 118.36 GiB on Ubuntu. A sequential rerun of one window from each host
matched the persisted transformed hashes exactly. A full resume reused the
membership, submitted no jobs, retained 632 windows and did not increment any
task attempt.

Python retrieval and the R QA script both read official series 0, window 0:
64 transformed inputs, 14 unchanged future values and test membership. Focused
tests also reject a different parent containing the same dataset/series keys but
changed observations, enforce local limits before writes, preserve configurable
settings on database-only resume, and restore an interrupted long-series chunk
without duplicate or changed transformed hashes.

The new direct dependency is pinned as `tsai==1.0.1`. Its required fastai,
PyTorch, NumPy and scikit-learn stack materially enlarges the general Python
environment; this is a current packaging limitation rather than duplicated data.
Lock regeneration added the required graph without changing or removing any
previously locked package version.

## Final closure acceptance

Mac and Ubuntu tested the final reviewed source manifest
`1a8f22a907d2036ed5588f9c6f84ecc71e81abee7d56218e483b18ec291e6c8e` with
byte-identical affected runtime/tests, unchanged locks, GIFT-Eval submodule
`4d5ab3fa0fe7451bbf59bb1ff6dd76e6e414d64a`, tsai 1.0.1, R 4.6.1,
forecast 8.24.0, jsonlite 2.0.0 and tsfeatures 1.1.1. The same 52 focused Python
tests passed on each host. They now prove direct-coordinator rejection before
writes/worker calls, permitted bounded CLI forwarding, all ten configured
frequency paths, nonzero offsets, three chunks per frequency, partial resume,
and persistence of a deliberately altered tsai-returned input. R transformation
tests and QA parsing also passed on each host. The recorded renv completeness
warning remains outside this scope.

The authoritative fresh pair is
`.amp/in/id011016-closure-parent.duckdb` and
`.amp/in/id011016-closure-windows.duckdb`. It imported the same 100 M4
Daily series and 57,235 observations. The corrected path persisted 632 unique
windows with zero duplicate IDs or `(series_key, window_ordinal)` pairs:
81 train series/495 windows/4,137 unused observations and 19 test series/137
windows/1,002 unused observations. Every task completed once. Mac contributed
30 series/192 windows and Ubuntu 70 series/440 windows, with completion intervals
overlapping from 11:00:39.606705 to 11:00:58.403142 local time. Minimum available
memory during work was 4.35 GiB on Mac and 118.38 GiB on Ubuntu; no safety
response or swap growth was recorded. No GPU worker was launched.

Against the preserved 22e41bb acceptance, all 100 parent identities and series
memberships, all 632 boundaries, and all 632 raw-input hashes matched exactly.
Of 632 transformed arrays, 630 were byte-identical; two windows assigned from
Mac to Ubuntu had robust-cleaning floating-point differences no larger than
2.67 × 10⁻¹⁵. One fitted-state scale differed by 1.42 × 10⁻¹⁴; all other state
fields and all other fitted states were byte-identical. The two
cleaned/transformed hashes consequently differ. This is a recorded cross-host
numerical limitation, not a split, window or settings change. Python and R both
retrieved official series 0/window 0 with 64 transformed inputs and 14 unchanged
future observations. A full approved-profile resume reused persisted membership,
submitted zero jobs, retained 632 unique windows and left all 100 task attempt
counts at one.

## Manual QA

For the retained final acceptance artifacts, run from a repository-root R
session:

```r
ROLLING_QA_PARENT_DATABASE <- ".amp/in/id011016-closure-parent.duckdb"
ROLLING_QA_WINDOWS_DATABASE <- ".amp/in/id011016-closure-windows.duckdb"
ROLLING_QA_DATASET_ID <- "gift_eval/m4_daily/2919659809a2c1c5e5ccb2eb"
ROLLING_QA_SERIES_ID <- "0"
ROLLING_QA_WINDOW_ORDINAL <- 0L
source("src/r/qa/inspect_rolling_window.R")
```

Inspect `selected_rolling_window`, `rolling_raw_input`, `rolling_raw_future`,
`rolling_transformed_input`, `rolling_transformation_state` and
`rolling_restored_cleaned_input`. The script opens both databases read-only and
leaves these objects in the interactive workspace.

Equivalent command-line QA is:

```sh
Rscript src/r/qa/inspect_rolling_window.R \
  .amp/in/id011016-closure-parent.duckdb \
  .amp/in/id011016-closure-windows.duckdb \
  gift_eval/m4_daily/2919659809a2c1c5e5ccb2eb 0 0
```

The `.amp/in` databases and JSON evidence are deliberately ignored review
artifacts, not published experiment databases.
