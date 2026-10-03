# AMP Code optional-feature implementation instructions

Status: the ID 014 bounded-memory correction below was approved by the
researcher on 3 October 2026. Continue from the existing uncommitted ID 013/014
implementation; do not restart either item. ID 013 technical review found no new
blocker and requests no further scientific change.

## Retained contract

Implement the scientific, provenance and storage contract in the
[optional-feature decision](poc2-features.md). Keep calculation in one R
provider using the native input adapter and centrally resolved period. Connect
it only through an explicit `prepare-features` request and the existing
Prefect/Dask/Mac-writer architecture. Preserve prepared inputs, labels, futures,
S1 membership and accepted parent/child databases.

Verify unchanged-legacy parity, schema order, scaling isolation, constants,
short inputs, periods, job failure, individual/batch equivalence, persistence,
retrieval, provenance-aware restart and the not-requested path. Use disposable
data and focused Mac/safely synchronized Ubuntu checks. Do not run Mantis, a
forecast campaign, GPU work or a new scientific feature strategy. Leave the
implementation uncommitted and unpushed for technical review and independent
researcher QA.

## Approved bounded-memory correction

Technical review confirmed that `FeatureExtractionCoordinator._window_jobs()`
fetches every prepared input into Python before cache filtering, local-limit
checks and batching. Bounded worker submission alone does not bound coordinator
memory. Correct this existing path without adding another workflow or framework.

1. Validate local limits and count the relevant windows before reading input
   arrays. Preserve the existing limit semantics: reject an oversized local
   request, rather than silently selecting a smaller experiment.
2. Select pending work using the existing complete provenance/reuse rules and
   read its arrays in deterministic bounded pages. Avoid full-window Python
   lists and unbounded accepted-row sets. Use database filtering/counts or
   equivalent bounded reads. Paging must not skip rows as commits change the
   pending set, and must permit incremental commits between pages.
3. Retain only the configured pages and in-flight batches needed for current
   work; release consumed payloads. Completion counts and fully reused restarts
   must not reload all arrays. Use the existing batch and in-flight authorities,
   not new hard-coded resource settings.

Keep small readable methods in the current coordinator. Preserve the 42-feature
calculation, names/order, per-series scaling, fallbacks, exact cache identity,
failure reporting, retrieval and sole Mac writer. Do not change cleaning,
labels, windows, S1 membership, dependency versions or accepted databases. The
eight directional features remain disabled. ID 015 and Mantis are not added.

## Focused acceptance

- Add a disposable regression fixture larger than one page. Demonstrate that
  limit rejection happens before array loading, live payloads stay bounded as
  window count grows, every pending row is processed exactly once, and restart
  reads no accepted input arrays or resubmits accepted work. Do not rely only on
  the previous two-window memory check. Keep failure/provenance/retrieval checks
  and verify unchanged inputs, labels, S1 membership and accepted feature rows.
- Safely synchronise the reviewed source/test/document overlay to Ubuntu first.
  Inspect both worktrees and jobs; preserve unrelated work and verify actual
  file hashes, not just Git HEAD. Never copy environments, databases or results.
  Run affected focused checks on both hosts, reusing unchanged scientific tests.
- Run one small real `prepare-features` integration through the normal
  researcher entry point, Prefect/Dask, native R provider, Mac persistence and
  retrieval. Use the centrally resolved `poc2_seasonal_recovery` v3 CPU profile:
  8 Mac and 15 Ubuntu workers, existing admission/thread/in-flight limits and
  memory floors. Do not launch GPU workers. Use a fixed disposable fixture with
  enough bounded batches to demonstrate completed, overlapping work on both
  hosts; record source/dependencies, host task counts and resource evidence.
  Registration alone is insufficient. Compare a small sequential reference
  using declared tolerances, and prove a second run reuses the accepted rows.
- No full dataset, forecasting, training or accuracy campaign is needed. If
  Ubuntu is unavailable or safe synchronisation fails, leave distributed
  acceptance pending and report it; do not substitute a heavy Mac-only run or
  repeatedly enlarge/retry the test. Follow the existing execution policy.

## Handoff boundary

Follow the [working agreement](working-agreement.md), code standards and
execution policy linked from `AGENTS.md`. Update the existing feature acceptance
record with the correction, focused evidence, actual peak payload/memory bounds,
per-host work, limitations and before/after script and line counts. Correct
affected documentation without rewriting historical evidence or creating a new
review log. Report concise manual QA steps and stop for technical/researcher
review, leaving changes uncommitted and unpushed. This approval does not include
Git publication or a spreadsheet update.

Preserve the [historical forecast-adjustment reference](poc2-forecast-adjustment-reference.md)
and its README link. Older Mac Table 1 results are not a blocker; do not pursue
that comparison or implement adjustment/Oracle logic within this correction.
