# AMP instructions: implement POC2 ID 033 directional forecast metrics

Status: Approved implementation and closure instructions, 7 October 2026. The
implementation passed Chief Architect review and received Researcher approval;
the closure authorization below supersedes the initial uncommitted handoff.

## Objective

Implement the approved dormant ID 033 R utility from
[`poc2-id033-directional-forecast-metrics.md`](poc2-id033-directional-forecast-metrics.md).
Preserve the useful calculations from the historical
`m4_tsc_fmts_2026/src/r/forecast_methods3.R` without importing its duplicated
time-series conversion or forecast wrappers and without activating a workflow.

This is a small functional import, not a forecast-model, feature-pipeline,
database or orchestration increment.

## Starting review

Before editing, verify the current branch, revision and worktree. Preserve any
unrelated work and stop if the worktree contains changes outside the approved
ID 033 documentation candidate.

Read completely:

- `AGENTS.md`;
- `docs/code-standards.md`;
- `docs/research-vision.md`;
- `docs/poc2-features.md`;
- `docs/poc2-id033-directional-forecast-metrics.md`; and
- the historical `m4_tsc_fmts_2026/src/r/forecast_methods3.R`.

Inspect current R time-series construction, forecast methods, feature provider
and their tests before coding. Report any conflict with the approved design
rather than resolving it by expanding scope.

## Implement only the dormant utility

Add:

```text
src/r/util/directional_forecast_metrics.R
src/r/tests/test_directional_forecast_metrics.R
```

Implement these documented functions without numeric suffixes:

```text
legacy_mean_directional_accuracy
legacy_mean_directional_value
legacy_mean_directional_percentage_value
legacy_pt_statistic
legacy_pt_p_value
calculate_directional_forecast_metrics
```

The five kernels preserve the historical finite-input arithmetic. The public
aggregator validates the approved contract and calculates the five results for
each member of a named forecast list. Keep the code concise and readable. Use
the documented bounded functional exception; do not add a class around pure
stateless calculations.

Follow the source-documentation standard for the file and every function:
purpose, meaningful inputs, outputs and unavailable behavior. State that the
file is imported and not run directly.

## Required behavior

Use these exact code constants:

```text
metric_set_id = legacy_directional_forecast_v1
comparison_definition = within_future_path_changes_v1
```

Require:

- finite numeric `actual` with length at least two;
- a non-empty named list of candidates with non-empty, unique names;
- every forecast finite, numeric, on the caller-provided common scale, and the
  same length as `actual`; and
- finite scalar reward and penalty values.

Return the decision document's ordinary R-list envelope, with one named model
result per candidate. Values are MDA, MDV, MDPV, legacy PT statistic and legacy
PT p-value. Represent unavailable numeric values as `NA_real_` and include a
stable concise reason in the same model result.

Preserve the historical semantics:

- calculations compare only `diff(actual)` with `diff(forecast)`;
- two zero changes count as an MDA match;
- MDV is in original series units;
- MDPV divides by `actual[-1]` and is unavailable if a destination is zero;
- legacy PT removes zero directions and is unavailable for no comparisons or
  a non-positive/non-finite denominator; and
- the p-value is the historical two-sided normal calculation from that legacy
  statistic.

Do not silently replace unavailable values with zero, one or the old real-run
fallbacks. Do not change the formulas to a canonical PT implementation under
the same names.

## Reuse and exclusions

Do not copy or implement:

- `vec_to_ts2` or another frequency/`stats::ts` converter;
- `auto_arima_forec`, `ets_forec` or any model fitting;
- `library(forecast)` or another new dependency in the metric utility;
- an R worker, JSON protocol, CLI action, process wrapper or provider;
- configuration, DuckDB tables, planning, storage or result retrieval;
- Prefect or Dask integration; or
- activation of `DISABLED_DIRECTIONAL_FEATURES` or changes to `FEATURE_SCHEMA`.

Do not source the new utility from `feature_provider.R`, the normal six-process
path, DTW, Mantis or any other production entry. Existing forecast models and
their accepted settings remain the only forecast authority.

If a minimal documentation cross-reference requires correction after the code
exists, update only the ID 033 decision or the existing ID 014 feature note. Do
not create an acceptance document before evidence is reviewed.

## Tests

The focused R test must be self-contained and must not source files from the
previous repository at runtime. Use deterministic fixtures with frozen expected
values and cover all eight acceptance points in the decision document.

In addition, prove by inspection/search that:

- the new production file contains no `forecast::`, `library(forecast)`, model
  fitting, DuckDB, Prefect or Dask call;
- no active source file refers to the new utility; and
- the existing disabled feature names and 42-feature schema remain unchanged.

Run the new focused R test, existing R feature tests and the smallest existing
locked/no-sync suite needed to show that normal feature and forecast contracts
remain intact. Parse all changed R files and run `git diff --check`. Do not run
a heavy, distributed, GPU, full-pipeline or 100-series experiment for this
dormant utility.

## Initial delivery boundary

Leave the implementation uncommitted and unstaged for Chief Developer and
Chief Architect review. Do not contact Ubuntu, install or update dependencies,
change lockfiles, commit, push, tag, amend, reset, stash or synchronize machines.

This boundary governed the initial implementation handoff and was satisfied.
The accepted candidate may now proceed under the closure authorization below.

Report concisely:

1. exact files changed;
2. function and output-contract mapping;
3. historical parity fixtures and results;
4. invalid/unavailable cases and their explicit reasons;
5. focused and regression test results;
6. proof of no model fitting, dependency or pipeline activation;
7. documentation and code-standard conformance; and
8. any remaining limitation or decision required.

## Approved closure and publication

The Researcher and Chief Architect accepted the implementation on 7 October
2026. Close ID 033 without adding another acceptance document or running a
100-series, distributed, GPU or accuracy experiment.

First verify this exact candidate scope on Mac `main`:

```text
AGENTS.md
docs/amp-poc2-id033-directional-forecast-metrics-instructions.md
docs/poc2-features.md
docs/poc2-id033-directional-forecast-metrics.md
src/r/tests/test_directional_forecast_metrics.R
src/r/util/directional_forecast_metrics.R
```

The expected published parent is `e5580f628da151239f2bba69b3d335c3fc80ad99`.
Stop before modifying Git history if the parent, candidate paths, Mac branch or
Ubuntu state differs, or if either machine contains unrelated work. Preserve
all Ubuntu stashes unchanged.

Before commit, rerun only the proportionate local closure checks:

- the focused ID 033 test under `Rscript --vanilla`;
- the existing R feature and forecast-method tests;
- parse both new R files;
- confirm no production source references the dormant utility;
- confirm dependency locks and feature schemas are unchanged; and
- run link, whitespace and exact-path checks.

If they pass:

1. stage exactly the six paths above;
2. inspect the complete staged content and exclude generated artifacts;
3. create one normal commit with subject
   `feat: add dormant directional forecast metrics`;
4. fetch GitHub and prove `origin/main` is an ancestor of local `main`;
5. push `main` by normal fast-forward without a tag;
6. on clean Ubuntu `main`, fetch and merge `origin/main` with `--ff-only`;
7. run only the focused dependency-free ID 033 R test on Ubuntu; and
8. verify Mac `main` = Ubuntu `main` = `origin/main`, both worktrees clean.

Do not install or update dependencies, touch stashes, amend, reset, force-push,
start Prefect/Dask services, or commit databases, models, reports, logs, caches
or other generated evidence. The final report must give the commit, branch,
three-way revision equality, exact committed paths, Mac/Ubuntu focused-test
results, relevant local regression results, clean-worktree confirmation and the
continuing dormant/non-accuracy boundary.
