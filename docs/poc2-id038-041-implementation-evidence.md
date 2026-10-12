# IDs 038/041 integrated implementation checkpoint

## Day-end failed-run technical checkpoint, 12 October 2026

The researcher authorised publication and two-host synchronisation of the existing
implementation commits plus this separate documentation-only record, followed by
owned-service shutdown and clean Ubuntu OS shutdown. This is **failed-run technical
checkpoint publication**, not successful Part B acceptance or POC2 scientific
closure. No experiment continuation, retry, historical cleanup, tag, dependency
change or rewrite of earlier commits is authorised. Earlier publication restrictions
below describe their historical increments, not this day-end authorisation.

The supported resume of `poc2_m4_daily_full_paper_tables_ulp_20261008` used
`5635e57e1a2be6443c9fe5f619d543f445d14280` and skipped completed Processes 01–03.
Its command exited **1** after **143,862.7578 seconds**, approximately **39 hours
58 minutes** (39:57:42.758). The recorded execution began at 10:13:02 AEDT on
9 October and ended at 02:10:23 AEDT on 11 October. Authoritative stored state:

| Process/result | Count and final state |
| --- | --- |
| Process 01 import | Completed: 4,227 series and forecast instances; 8,454 archived forecasts |
| Process 02 preprocessing | Completed: 4,227 records |
| Process 03 transformations | Completed: 4,227 records; 124,706 prepared windows |
| Process 04 forecasts/models | Failed during distributed DTW calibration; zero generated forecasts, stored calibration scores, selected widths or directional predictions |
| Process 05 combinations | Not run; pending |
| Process 06 evaluation/sensitivity | Not run; zero official evaluations, sensitivity candidates, retained diagnostics or paper reports |

Read-only comparison with the preserved pre-recovery database confirms all
Process 01–03 series, forecast-instance, archived-forecast, preprocessing and
transformation rows remain unchanged. Source-provider reconciliation remains the
accepted informational result: 4,227 series, 10,023,836 observations, maximum
normalized difference 0.000058736088095187654%, zero flagged series and zero
actual-direction changes. The previously accepted ULP evidence is preserved.

The last trustworthy scheduler snapshot was at **12:45 AEDT on 9 October**:
**11,645 of 199,232 DTW calibration batches, approximately 5.8%**, 23 active CPU
workers plus one registered GPU worker, and zero scheduler task errors at that
time. **This snapshot is stale and is not necessarily the final percentage.**
Later CPU activity is not a final completion count; no final batch percentage
has been established. The 118,386 failed Process 04 planned task records reflect
the process-level abort, not 118,386 independently executed model failures.

At 02:08:47 AEDT on 11 October, Ubuntu's worker log records the native
`environments/classifiers/.venv/bin/python` subprocess running
`src/python/04_04_train_directional_dtw.py calibration` terminating with
**`Signals.SIGSEGV: 11`**. The same `CalledProcessError` is retained in DuckDB
task-attempt evidence. **The exact underlying native-library cause remains
unconfirmed.** Shutdown-time communication errors in the coordinator log do not
establish that native cause. This failure is distinct from the earlier swap-growth
guard interruption; no scientific formula or dependency is changed to explain it.

The continuation controller recorded `recovery_failed` and did not invoke export,
alternative-source aggregate comparison or restart validation. No final tables or
figures were produced. Main/window databases remain local under `results/`;
their researcher evidence is under the experiment's `evidence/`, including
`pre-resource-recovery/`, original summaries/logs, resource samples,
`resource-recovery-status.json`, `resource-recovery-completion.json` and the
stale `resource-recovery-progress.json`. Ubuntu CPU/GPU logs were additionally
copied there as `ubuntu-dtw-cpu-failed-run.log` and `ubuntu-gpu-failed-run.log`.
Original worker/service logs, caches, prior evidence and any existing model
artifacts are retained on their respective machines; none is committed.

The outstanding requirement before another full run is to **diagnose and
checkpoint DTW native execution**, with reproducible bounded validation and
reviewed evidence. Do not infer scientific acceptance from the completed import,
preprocessing, transformation or earlier Part A acceptance.

## Part B operational recovery, 9 October 2026

The researcher classified the preserved Process 04 stop as an operational
interruption, not a scientific failure. The
[resource-policy amendment](execution-policy.md#part-b-operational-recovery-amendment-9-october-2026)
retains configured measurements/thresholds but records swap/headroom breaches
as warnings. The DTW subprocess still fails on native execution errors,
timeouts and invalid cache/response identity. Existing continuous R/Chronos
monitoring records threshold warning counts without killing useful work;
failed safety probes remain fatal. No scientific settings, admission budgets,
topology, worker counts or dependency locks changed.

DTW returns host/process, RAM/swap before/after, delta, unchanged thresholds,
spill, native exit status and retry provenance. Calibration persists warning
batches with per-host extrema through existing task-attempt evidence. Focused
tests demonstrate warning/continue behaviour and fatal native OOM/SIGKILL and
altered-cache failures. The 70 focused execution/safety/window tests, 11 pinned
DTW tests and three pinned GIFT-Eval semantics tests passed. Eight relevant R
scripts passed, including explicit device cleanup and standalone figures; all
R source files parse. Changed-document file links and `git diff --check` pass.

Schema version 12 is authoritative in the existing initializer/migration; the
stale window-fixture expectation was corrected from 11 to 12 without changing
schema code. Broad discovery ran 353 entries with one skip and one import error:
GIFT-Eval integration was mistakenly loaded in the project environment. Its
three tests subsequently passed in the existing dedicated pinned environment.
The correctly separated project suite passed all **345 tests**. Resumed execution
evidence will be recorded after completion; this paragraph does not claim Part B
success. Before restart, copies of both interrupted databases and its summary
were preserved in `evidence/pre-resource-recovery/`; database hashes exactly
match the failed-run hashes recorded below.
The preceding failed-run addendum and generated evidence remain preserved.
Publication and historical cleanup remain outside this recovery increment.

## First authorised Process 03 cross-host correction

The researcher first authorised this policy after the stopped run documented
below. The accepted source-provider reconciliation is unchanged. The
[separate integrity/ULP contract](architecture.md#process-03-cross-host-numerical-validation)
is implemented in `ProcessStorage`: exact linked identities, input and payload
hashes, configured metadata, lineage and finite shapes remain mandatory.
Stored worker values and fitted state are authoritative. Numerical comparison
uses IEEE-754 ULP distance, with signed zero equivalent: up to four ULPs continue
as computationally equivalent; larger finite differences continue as warnings.
Fitted centre/scale are compared numerically, while recipe/version/count and
constant metadata remain exact. Stored fitted state must reproduce its own
payload. No transformation or observation formula changed.

The existing process action persists per-record operational evidence in the
process summary; normal execution-event validation and read-only export retain
it without changing scientific configuration or report fingerprints. Measured
directional effects are checked from identical futures/origins under both
states. Explicit downstream-impact flags escalate exported evidence to
`researcher_review_required`; they do not discard outputs. Unassessed effects
are not claimed unchanged. Focused tests exercise both ULP and scientific-impact
boundaries, stale hashes, identity/lineage/shape corruption and operational-only
evidence persistence, with bounded functional fixtures as the normal test
exception to OOP.

Validation against automatically removed copies of both failed databases passed
all **4,227** transformations. The same three affected records have fitted scale
distance **one ULP** and transformed-value distance at most **two ULPs**;
there are **zero** warnings above four ULPs and **zero** directional-label
changes. Original and copied database bytes, stored Process 01–03 rows, vectors
and hashes remained unchanged. The failed experiment was neither resumed nor
repaired. The new full-Daily configuration selects
`poc2_m4_daily_full_paper_tables_ulp_20261008` and keeps the entire approved
population, model settings, seed, thresholds and workflow. Publication and
historical cleanup are not authorised in this increment.

Local checks passed **22** focused ProcessStorage/export tests and the earlier
**75** combined ProcessStorage/export/main/table/configuration checks. All eight
relevant R scripts passed, including the nine-method forecast suite and five
plotting groups; **11** DTW tests and **3** GIFT-Eval semantics tests passed in
their existing pinned provider environments. The final broader project run
executed **344 tests: 343 passed, one failed**. Its sole failure is the unchanged
window-fixture assertion expecting schema version 11 while the current,
unchanged schema initializer creates version 12. It is not a regression from
this correction; the stale unrelated assertion was not edited or suppressed.
Earlier discovery used the wrong environment for GIFT-Eval, and an inline
test launcher could not support multiprocessing spawn; those harness mistakes
were corrected by the dedicated environment and normal `python -m unittest`
launcher. Dependency and lock files remain unchanged. The broader suite is not
reported as wholly green, and its remaining fixture limitation accompanies the
fresh execution evidence rather than being confused with an experiment
structural failure.

### Fresh full-Daily ULP run: Process 03 passed; Process 04 resource stop

The fresh `poc2_m4_daily_full_paper_tables_ulp_20261008` run used identical,
clean Mac/Ubuntu revision `c20223a68930924575aaaafe9b933a0ce98bca0a` and the
unchanged approved 8 Mac CPU/15 Ubuntu CPU/one Ubuntu GPU topology. Its Prefect
workflow ran from 21:29:38 AEDT on 8 October to 00:19:39 on 9 October 2026;
the command exited nonzero after **10,223.20 seconds**. The normal cluster
preflight passed. Dependencies, model locks, observations and scientific
parameters were not changed.

Processes 01–03 completed and passed validation for **4,227** series each.
There are **8,454** archived forecasts, **4,227** forecast instances and
**124,706** prepared windows with directional labels. Source reconciliation
again classified all 10,023,836 paired observations as **informational**:
maximum normalized difference 0.000058736088095187654%, zero flagged/material
series and zero changed actual directional labels. The producing worker's
transformation values and hashes were retained. Four records (zero-based IDs
124, 670, 2606 and 2739, produced on Ubuntu) differ from Mac recomputation:
fitted scales by one ULP and transformed values by at most two ULPs, with maximum
absolute transformed difference 4.440892098500626e-16. All 4,227 comparisons
are computationally equivalent; **zero** exceed four ULPs and **zero** measured
directional labels change. This fresh result validates the correction beyond
the earlier failed-database copy check; it does not establish unmeasured
counterfactual model/selection/rank effects.

Process 04 stopped in its distributed DTW calibration subflow with
`directional DTW swap-growth safety limit was breached`. The unchanged native
batch wrapper compares local host-wide swap before/after the classifier
subprocess against **0.25 GiB**; this is a resource safety stop, not a numerical
warning, hash failure or source-reconciliation rejection. The process-level
failure propagated to all **118,386** planned Process 04 task/attempt records,
whose failed records have no runtime/resource payload. Those records do not
represent 118,386 independently executed model failures. No fitted model,
generated forecast, directional prediction/evaluation, official evaluation or
sensitivity result was persisted. Processes 05–06 remain pending; their planned
counts are 63,405 and 46, not completed-result counts.

The 46 periodic samples recorded minimum available memory **3.182 GiB** on
Mac and **116.485 GiB** on Ubuntu. Sampled Mac swap decreased from approximately
0.420 to 0.373 GiB before calibration. A post-run Mac check measured **2.244
GiB** swap (approximately 1.871 GiB above the final periodic sample); Ubuntu
showed zero used swap and zero cumulative swap-in/out counters. The sampling
interval missed the final calibration interval, and the exception does not
retain the failed batch's host or before/after values. Consequently these
observations support Mac pressure but do not measure the offending subprocess's
individual memory peak or exact batch swap delta. Completed recorded task
contributions are **2,192 Mac** and **6,262 Ubuntu**; window-batch work has
separate operational logging.

The failed database, window database, logs, source reconciliation and numerical
records are preserved under `results/poc2_m4_daily_full_paper_tables_ulp_20261008*`.
Read-only extraction generated `evidence/execution-summary.json` without
changing database bytes. Main/window SHA-256 values are respectively
`793391e684773a35ac15167dc06cd34c5ad8d1122c60f4e46b964d164c55b2bc` and
`8703c104c07f753786e298e485b97291ec3858c2bfc71a0b6cd3147f4d8214ca`.
No restart, aggregate alternative-source comparison, result tables, figure
export or historical numerical comparison was performed because the required
scientific results do not exist. Owned services stopped and ports
4200/8786/8787/8725 were closed. Neither host's repository root contains
CSV/PDF/PNG or `Rplots.pdf`. Both source HEADs remain the unpublished correction
checkpoint; Ubuntu's five stashes and the published Part A are unchanged.
This factual failure addendum remains unstaged on Mac. No push, historical
cleanup, retry, resource-guard bypass or scientific closure is claimed.

## Part B cross-source reconciliation, 8 October 2026

Part A remains accepted, published and closed. The first full-Daily attempt
imported every series but its older strict reference adapter rejected legitimate
cross-provider numeric differences. That preserved rejection is not evidence
that ShapeFM architecture or the experiment population is invalid. The
researcher approved the explicit [cross_source_v1 policy](experiment-configuration.md#cross-source-reconciliation-approved-8-october-2026)
for a fresh full-Daily experiment; the previous database is not reconfigured.

Read-only application to the preserved sources compared 4,227 series and
10,023,836 observations. Exactly unequal representations occur at 8,225,949
positions: 3,056 histories and 3,045 futures contain differences. The largest
per-series normalized difference is **0.000058736088095187654%**, below 0.01%.
No series is flagged or material, and no strict directional label changes among
59,178 comparisons. Status is **informational**. Per-series measurements and
one-based discrepancy locations are retained under the preserved experiment's
`evidence/source-reconciliation/`; observations are neither copied into that
report nor corrected. The database SHA-256 remained unchanged.

The existing R reference adapter owns the pure reconciliation kernels; the
existing `ImportCoordinator` owns report publication, coordinator-only evidence
and the before-Process-02 review barrier. Central configuration validates and
stores the versioned opt-in thresholds; absent policy preserves older meaning.
The full tracked configuration selects the fresh
`poc2_m4_daily_full_paper_tables_reconciled_20261008` namespace, all 4,227 series,
seed 1234 and unchanged models/workflow. Small stateless R kernels and focused
fixtures use the existing pragmatic functional exception. No dependency, model
parameter, forecast formula or accepted Part A configuration changed.

Focused R fixtures exercise representation differences, constant/near-zero
series, exact/above population boundaries, material differences, directional
ties/changes and structural failures. Python import/reference/configuration
checks passed **36 tests**, including explicit resolved-policy storage and
fingerprinting, report publication, review-before-reference storage and unchanged
canonical values. The combined locked Python regression suite passed **253
tests**; R adapter/reconciliation checks, syntax, documentation links and
whitespace checks passed. Dependencies and locks are unchanged.

### Fresh full-Daily execution: stopped at Process 03 validation

Mac and Ubuntu ran the identical unpublished source checkpoint
`115cf050943abf3e8497b53ffccd15f96261d452`, with clean worktrees before execution.
The fresh experiment ran 17:07:11–19:56:07 AEDT on 8 October 2026, exiting
nonzero after 10,136.49 seconds. Process 01 imported all 4,227 series and 8,454
archived forecasts; its stored reconciliation reproduced the informational
measurements above. Process 02 completed and validated all 4,227 items.
Process 03 stored 124,706 rolling windows and all 4,227 transformations, but
its final validation failed with
`stored transformation failed value, hash or lineage validation`.
Processes 04–06 did not execute: no fitted models, generated forecasts,
evaluations or sensitivity candidates were produced. Completed task rows do
not mean Process 03 passed validation.

Read-only diagnosis isolated zero-based series IDs **670, 2606 and 2739**, all
transformed on Ubuntu. Model recipe, input/output hashes, lengths and both
parent/preprocessing lineage checks pass exactly. On each record, the fitted
scale differs from Mac recomputation by **4.547473508864641e-13** (one ULP);
the maximum absolute transformed-value difference is
**4.440892098500626e-16**. Recomputing those identical stored inputs on Ubuntu
reproduces both stored parameters and output fingerprints exactly. Both hosts
use Python 3.12.14, but Mac is ARM64/macOS and Ubuntu is x86_64/Linux. This is
a cross-host exact-equality validator failure, not a source-reconciliation
threshold breach. No transformation formula, tolerance, stored value or
fingerprint was changed to bypass it.

The failed database, window database, logs, resource samples, reconciliation,
execution summary and read-only cross-host diagnostics are preserved under
`results/poc2_m4_daily_full_paper_tables_reconciled_20261008*`. Evidence extraction
left the experiment database SHA-256 unchanged:
`2a70490f2addf9e8fbf9750b851a99a01b0846f92f5b6aa8d96c1ad6f5ff8c1d`.
Stored completed attempts identify 3,288 Mac and 5,166 Ubuntu contributions.
The 23 later operational samples show minimum available memory of 4.354 GiB
on Mac and 116.776 GiB on Ubuntu. Earlier import monitoring observed Mac swap
growth to approximately 0.779 GiB, subsequently decreasing; Ubuntu swap
remained zero. These samples are not continuous-run minima or evidence of
zero resource pressure. Prefect event-queue overflow and shutdown cancellation
warnings also remain in the log; they do not replace the recorded validation
exception as the stopping failure.

Owned Prefect/Dask/native services stopped; ports 4200/8786/8787 and the
temporary Prefect port 8385 were closed, and neither root contained CSV/PDF/PNG
or `Rplots.pdf`. Ubuntu's five stashes remain unchanged. The checkpoint was
not pushed; no failed outputs were removed. Full execution, restart, tables,
figures and alternative-source aggregate-metric comparison remain outstanding.
There are no completed scientific aggregate results from which to assess
changes to reported conclusions. Resume requires addressing the Process 03
cross-host validation contract separately; the data-provider policy must not
be silently expanded to waive transformation-integrity checks.

## Fresh corrected Part A technical acceptance, 8 October 2026

The fresh `poc2_m4_daily_100_paper_tables_corrected_20261008` experiment passed
on source revision `7a98a3e798aa1fbcef303963eaf287cb1c52d085`, shared cleanly
by Mac and Ubuntu before execution. This factual technical result supersedes
the pending execution statements in the historical checkpoint sections below;
it is not historical numerical reproduction or researcher scientific closure.
The original implementation commit was not amended. Dependencies, locks,
scientific formulas/settings and the six-process architecture were unchanged.

The initial execution ran 08:30:55–08:46:50 AEDT. Processes 01–06 completed
**100/100/100/2830/1500/46** items. All 4,576 experiment-task attempts completed,
with no failed attempts: 1,641 identify Mac, 1,476 Ubuntu and 1,459 have no
worker hostname because their coordinator-local evidence uses another shape.
There are 1,500 forecasts (100 for each of 15 methods including M4 Comb),
200 archived reference forecasts, 2,800 directional predictions, 28 directional
evaluations and 17 official evaluations. The complete paper surface contains
1,050 unique sensitivity candidates plus seven baseline/direct results;
nine selected results, 300 retained diagnostic vectors and 126 horizon rows
pass the authoritative storage validator. Mac contributed 15 paper batches
(303 candidates); Ubuntu contributed 34 (754). The coordinator alone wrote
DuckDB; distributed jobs used the existing storage-free native boundaries.
One DTW all-horizon artifact and 14 fitted Random Forest artifacts were created
and retrieved through the accepted model store.

The actual topology was 8 Mac CPU, 15 Ubuntu CPU and one Ubuntu GPU worker.
All 100 Chronos forecasts identify CUDA on RTX 5090, one Dask GPU worker,
generation 1 and model-load count 1. The exact pinned local snapshot was used;
no download fallback exists. A separate real two-batch native-process probe
also matched consecutive forecasts and the preserved forecast exactly, recorded
zero external network calls using `strace`, and verified shutdown cleanup.
Recorded R-fit minimum available memory was 4.524 GiB on Mac and 116.439 GiB
on Ubuntu; Chronos recorded at least 117.978 GiB host and 29.829 GiB GPU
headroom. These monitors recorded zero swap use and zero safety responses.

Restart completed at 08:48:25 AEDT, skipping all six completed processes. Task
attempts, forecasts, diagnostic vectors and paper batch/candidate counts did not
increase; forecast, candidate, diagnostic and report fingerprints were unchanged.
The unchanged fitted artifacts were reused as completed validated work, rather
than retrained. Re-export preserved every scientific CSV and PDF/PNG byte;
only the operational manifest timestamp and execution-summary restart evidence
changed. Both exports left database SHA-256 unchanged. Manifest hashes verify
all 12 declared payloads; the manifest is the thirteenth declared file and is
not self-hashed.

The horizon PNG and both separate standalone CD PNGs were visually inspected:
four horizon traces and five CD methods are readable, with no clipping or
combined panels. Research/paper CD versions are byte-identical and display the
dependent-horizon descriptive limitation. These actual renderings supersede
the preliminary combined-panel rendering evidence below. The full result tree,
tables, figures, manifest and execution summary are under
`results/poc2_m4_daily_100_paper_tables_corrected_20261008/`; its database and
window database are under `results/`, fitted artifacts under `models/`.
No repository-root CSV, PDF, PNG or `Rplots.pdf` was created. Owned services
and native children stopped, and ports 4200/8786/8787 were closed on both hosts.
Ubuntu's five pre-existing stashes were unchanged.

Scientific configuration fingerprint:
`c2635a31d638f70f384355f8df542c8b96ad9f36d310d904d6e46ed887433a22`.
Stored report result fingerprint:
`337f1ae72d2027e7074de3f9d6ed9f328e6b9558542e024a77ef410083bed4a1`.
Selected SMYL lambdas were (1.005, 0.985), Chronos lambdas (1.015, 0.975).
These are 100-series technical results, not comparisons with full-population
published targets. Complete 4,227-series Part B and researcher review remain
outstanding. Generated outputs and raw execution evidence are not Git content.

## Authorized recovery correction, 8 October 2026

The reboot cleared the observed Chronos native preflight crash: the direct
fault-handled probe and unchanged 24-worker cluster preflight passed, and the
fresh reboot experiment completed Processes 01–05, including RTX 5090 forecasts.
This establishes recovery, not the underlying cause of the earlier segmentation
fault. No dependency, driver, lockfile, scientific formula or model setting changed.

That fresh experiment failed in selected diagnostic retention. Isolated native
R recalculations on Mac and Ubuntu matched every selected identity, lambda,
experiment/input/source/grid fingerprint, membership and discrete count exactly.
Mac-stored Chronos–Mantis MASE was 19.88743462958401; Ubuntu recalculated
19.887434629583925 (absolute difference 8.526512829121202e-14, relative
4.28738697973513e-15). Oracle MASE/OWA also exhibited machine-level rounding;
directional counts and sMAPE were unchanged. The measured discrepancy is confined
to floating-point aggregates, not scientific consistency or selection.

`TableEvaluation.validate_retained` now verifies all nonmetric identity and count
fields exactly, checks DA exactly and permits only `math.isclose` with absolute
tolerance **1e-13** and relative tolerance **5e-15** for sMAPE, MASE and OWA.
These bounds narrowly cover the measured discrepancy and remain below the
authorised 1e-12 ceiling. The first stored surface remains authoritative: its
values, candidate, deterministic selection and fingerprints are never replaced.
The selected recalculation only supplies retained vectors linked to that original
candidate. Material metric differences and all identity/count differences fail.

Copy-only recovery also found two of 300 vectors whose integral JSON values
were fingerprinted as integers but stored as DuckDB `DOUBLE[]`. Reconstructing
those integer representations reproduced the mismatch exactly. Retained vectors
are now normalized to Python floats before storage and hashing, preserving their
numerical values and original candidate lineage. No checksum tolerance is used.
Process 06 recovery passed against a second disposable copy; the original failed
database and evidence were not modified.

Chronos previously restarted on every batch because monitor object identity
triggered replacement and successful-batch cleanup always closed the child.
The corrected worker owns one persistent model/monitor pair across batches and
idle intervals. Admission is refreshed per call; safety extrema and swap baseline
remain lifetime-scoped. Configuration changes, errors, unsafe state, worker
replacement and shutdown release that ownership. A worker cleanup plugin and
process-exit hook close the child and monitor without changing scheduling.

Preflight and native loading share the standard-library
`shared_chronos_checkpoint.py` resolver. It requires the exact pinned snapshot,
readable configuration and complete local safetensors data. The native provider
loads that local path with `local_files_only=True`, `HF_HUB_OFFLINE=1` and
`TRANSFORMERS_OFFLINE=1`; no repository-name download fallback is permitted.
The small stateless resolver uses the documented bounded functional exception.

Original failed databases and evidence are preserved unchanged. Recovery checks
and field-level comparison records are derived files under `results/`, never
committed scientific records. Fresh acceptance and restart remain required.

Local validation passed 225 combined focused/regression tests, followed by the
updated 10-test Process 06 suite (including integral JSON storage/retrieval).
The copy recovery preserved all 1,057 surface identities/fingerprints and
produced 17 official evaluations. Locked R metric/worker/adjustment/rendering
checks, 11 DTW numerical tests, Python parsing, documentation links and whitespace
checks passed. Dependencies/locks are unchanged. Controlled Chronos tests prove
two batches reuse a real protocol child with load count one, preserve monitoring
and close on unsafe/configuration/shutdown boundaries; real GPU validation follows
source synchronization and is not yet claimed by these local tests.

## Current Part A / Part B implementation status

The researcher subsequently authorised the complete Part A/Part B sequence.
The checkpoint-only restrictions and reduced-pool counts below describe the
earlier candidate, not the current execution authorisation. Part A publication,
cleanup and full Part B remain conditional on successful fresh 100-series
two-machine acceptance and restart. No scientific acceptance is inferred from
the synthetic checks.

The corrected v12 candidate retains the complete accepted v9 ordinary forecast
pool and M4 Comb alongside the accepted v11 DTW and Mantis models. GIFT-Eval
receives the unchanged official options, independently of the paper metric
profile. Expected Process 01–06 counts are **100/100/100/2830/1500/46**:
1,500 forecasts, 2,800 directional predictions, 17 official evaluations,
28 directional evaluations and one paper-table task. The 1,050 sensitivity
candidates remain bounded CPU batches, not additional durable process tasks.

`config/experiments/poc2_m4_daily_full_paper_tables.json` selects all **4,227**
official Daily series, seed 1234 and the current approved execution profile.
Import and official planning independently reject an unexpected source count.
It has no sampled cohort; expected process counts are
**4227/4227/4227/118386/63405/46**. Full execution remains gated on Part A.

Additional complete-scope paths are `src/python/tests/test_complete_pipeline.py`
and `src/python/util/p01_03_gift_eval_source.py`. Configuration, source guards,
planning, ordinary forecasting and official scoring share the existing owners;
there is no second runner or calculation.

Corrected local validation:

- The combined Python focused/regression suite passed **220 tests**. Expected
  retry/worker-loss errors are intentional test inputs, not experiment failures.
- R passed **5 rendering**, **4 metric**, **3 JSON-worker**, **9 adjustment** and
  **12 directional-metric** groups. The renderer verifies installed locked
  `scmamp` and opens/closes explicit devices, including on draw errors.
- All **13 declared files** are exported in the mandatory results tree. One
  authoritative CD matrix and tied-rank result supplies two separate,
  byte-identical standalone CD presentations. Both have the dependent-horizon
  disclaimer; there is no combined-panel output.
- The horizon PNG and both standalone CD PNGs were visually inspected in an
  automatically removed temporary directory. Labels, axes and disclaimers were
  readable without clipping. These are synthetic rendering checks, not historical
  Figure 2 reproduction.
- Export checks prove read-only DuckDB access, unchanged database bytes, exact
  stored CSV parity, deterministic regeneration, constrained custom destinations,
  traversal/symlink rejection and no publication of invalid/incomplete evidence.
  No evaluation, model, Prefect or Dask computation is called during export.
- The updated configuration suite passed **9 tests**, including equal official
  evaluator options and unsliced Arrow source-count rejection on both sides of
  the expected count. The classifiers environment passed **11 DTW numerical
  tests**; the accepted R forecast suite passed for all **9 registered methods**
  and the four approved M4 point methods. Python/R parsing, local documentation
  file links and `git diff --check` passed. Dependency and lock files are unchanged;
  no repository-root CSV, PDF, PNG or `Rplots.pdf` was present.

No generated test output is retained as a scientific record. Actual two-machine
counts, revision identity, restart and numerical comparisons must be recorded
after execution; none is claimed here.

## Delivery state and authority

Local candidate for Chief Developer and Chief Architect review, **not researcher
acceptance or numerical reproduction**. Implements the
[approved decision](poc2-id038-041-table-reproduction.md) and
[implementation instructions](amp-poc2-id038-041-table-reproduction-instructions.md).
The starting branch is `main`, at the published
[ID 037 revision](https://github.com/rafmontano/shape_fm_poc/commit/b5e3df01cde610d96fed3bdd21913de01c868939).
The candidate remains uncommitted and unstaged. No Ubuntu contact, installation,
dependency synchronisation, scientific experiment, publication or tag occurred.
Existing research databases, model artifacts and accepted configurations were not
modified. Test databases were temporary synthetic stores only.

## Exact path and responsibility map

Pre-existing user documentation retained:

- `AGENTS.md`
- `README.md`
- `docs/architecture.md`
- `docs/code-standards.md`
- `docs/poc2-forecast-adjustment-reference.md`
- `docs/research-vision.md`
- `docs/amp-poc2-id038-041-table-reproduction-instructions.md`
- `docs/poc2-id038-041-table-reproduction.md`

Implementation additions/edits:

| Responsibility | Exact paths |
| --- | --- |
| Fresh scientific configuration and operational settings | `config/experiments/poc2_m4_daily_100_paper_tables.json`, `src/python/util/shared_configuration.py`, `src/python/util/shared_execution_profiles.py` |
| Mixed upstream planning, existing providers, Process 06 dispatch and read-only status/results | `src/python/util/shared_experiment_execution.py`, `src/python/util/p00_01_researcher_actions.py` |
| Additive schema and existing process validation/preflight | `src/python/util/shared_database.py`, `src/python/util/shared_process_storage.py`, `src/python/util/shared_distributed_cluster.py` |
| Existing Prefect CPU compute dispatch | `src/python/util/shared_workflow_orchestration.py` |
| Cohort cache, scientific identities and bounded batch/selection barriers | `src/python/util/p06_01_table_flow.py` |
| Coordinator-only storage, lineage/integrity/restart validation and diagnostic/profile retrieval | `src/python/util/p06_02_table_storage.py` |
| One selected-result table builder, six-frequency aggregation and descriptive ranks | `src/python/util/p06_03_table_reports.py` |
| Shared R adjustment and historical metric calculation, serial JSON worker | `src/r/util/forecast_adjustments.R`, `src/r/util/paper_table_metrics.R`, `src/r/06_02_evaluate_paper_tables.R` |
| Focused configuration, source SQL, real local-Dask, persistence/restart and science tests | `src/python/tests/test_table_configuration.py`, `src/python/tests/test_paper_tables.py`, `src/r/tests/test_paper_table_metrics.R`, `src/r/tests/test_paper_table_worker.R`, `src/r/tests/test_forecast_adjustments.R` |
| Configuration/researcher instructions, workflow diagram and checkpoint evidence | `docs/experiment-configuration.md`, `docs/poc2-workflow-orchestration-decision.md`, `docs/poc2-id038-041-implementation-evidence.md` |

Small cohesive `TableEvaluation`, `TableStorage` and `TableReports` objects own
science/identity, coordinator persistence and presentation respectively. Existing
Prefect/Dask objects own scheduling. The stateless R kernels and synthetic tests
use the documented bounded functional exception; classes would add no useful
responsibility to their arithmetic/assertions.

## Contracts and cardinalities

Configuration v12 stores original/resolved documents and scientific fingerprints.
It retains accepted preprocessing, S1 membership, DTW, frozen Mantis/Random
Forest, Chronos-2, native Naive2 and official SMYL/FFORMA archive definitions.
Expected Process 01–06 tasks for 100 series are
**100 / 100 / 100 / 1,630 / 200 / 29**. Process 06 has the accepted 28 directional
tasks plus one `paper_tables` task, not one durable task per lambda or series.

The exact grid is 25 upward by 21 downward values: **525 per base, 1,050 total**.
There are seven additional baseline/direct aggregate results, hence **1,057**
aggregate rows. With the default batch bound 25, the two sensitivity surfaces
alone use **42 batches**. All 1,057 aggregates use **49 model-grouped batches**
(a baseline can share its base model's batch); selected retention uses three
additional batches. Maximum in-flight work is the existing execution setting;
batch size, addresses, host, runtime and topology do not enter science.

Schema v12 adds `paper_metric_candidates`, `paper_selected_results`,
`paper_diagnostic_means`, `paper_directional_results`, `paper_table_reports` and
`paper_execution_batches`. Scientific results and operational batch evidence
are separate. Nine selected model identities supply **126 Daily horizon rows**.
Only selected SMYL–Mantis, Chronos–Mantis and SMYL-Oracle diagnostic vectors are
retained: six vectors for the two-series fixture, 300 for the configured future
100-series execution. No diagnostic is inserted into ordinary `forecasts` or
`official_evaluations`.

Historical `m4_paper_tables_v1` uses raw canonical context origins, Daily MASE
lag 1, per-series full-horizon components then cohort averages, and OWA from
ratios of aggregated components against Naive2. The accepted directional
evaluations retain their cleaned bounded-input origins separately. GIFT-Eval,
accepted labels, forecast model means/settings, combination, Mantis and DTW
mathematics were not changed.

## Local validation evidence

- **14 focused Python tests passed** in `tests.test_paper_tables` and
  `tests.test_table_configuration`. Two asymmetric synthetic series exercise the
  full 1,057-result surface through the real R subprocess and real Prefect
  local-Dask CPU tasks. Scientific records and selected pairs equal sequential
  results exactly; worker/runtime changes do not change identity/content.
- Grid endpoints/cardinality, both disagreement directions, strict terminal
  ties and complete-vector scaling are checked against frozen historical
  arithmetic. The R metric suite passes **4 groups**; the real JSON worker
  passes **3 groups**, including horizon-one arrays and invalid wire types.
- Frozen independent rational fixtures distinguish Daily MASE lag 1 from lag 7,
  ratio-of-component-means OWA from averaging per-series OWA, and direct
  directional predictions from point-vector directions. All selection tie
  levels are exercised, including stable final lambda ordering.
- Real temporary DuckDB migration is idempotent. An interruption after a
  completion-order batch leaves aggregates durable; retry submits only missing
  aggregate identities. Completed restart submits no work. Transaction failure
  rolls back and tampered scientific fingerprints are rejected. Full paper
  selection/report validation passes on the synthetic store; source SQL tests
  separately prove canonical raw context/original mean/prediction joins.
- Table 1 contains **7 rows**. Daily Table 2 contains **8 / 7 / 7 cells** in
  Panels A/B/C, exactly equal to shared Table 1 DA/OWA/SMYL-relative improvement.
  Terminal DA is selected from the common stored horizon results. Tied average
  ranks, duplicate rejection and explicit incomplete-model handling pass.
- Daily-only `All` is rejected. Synthetic complete-six-frequency tests weight
  components and recalculate OWA (independent expected value **1.25**, not the
  fixture's deliberately wrong OWA 99). Figure 5-shaped retrieval returns
  **107** synthetic horizon rows spanning all six frequencies without POC3 data.
- **163 accepted Python regression tests passed** across configuration,
  execution, Process 06/workflow, forecast flow/safety, directional preparation
  and comparison, Mantis components, fitted storage and labels. **11 DTW
  numerical tests passed** in the existing classifiers environment. After the
  final validator integration, **15 ProcessStorage/directional-comparison tests
  passed again**, alongside the focused suite.
- Accepted R checks passed: **ID 037 9 groups**, **ID 033 12 groups**, and the
  existing forecast-method suite for **9 registered methods**. ID 037's
  dependency-free calculation is unchanged; its isolation assertion now permits
  only the explicitly approved paper-profile callers/source fingerprint.

Commands use existing environments only:

```sh
PYTHONPATH=src/python .venv/bin/python -m unittest tests.test_paper_tables tests.test_table_configuration
PYTHONPATH=src/python .venv/bin/python -m unittest tests.test_configuration tests.test_execution tests.test_experiment_execution tests.test_process_storage tests.test_workflow_orchestration tests.test_forecast_flow tests.test_forecast_safety tests.test_directional_pipeline tests.test_directional_comparison_pipeline tests.test_directional_mantis_components tests.test_model_storage tests.test_labels
PYTHONPATH=src/python environments/classifiers/.venv/bin/python -m unittest tests.test_directional_dtw
Rscript --vanilla src/r/tests/test_forecast_adjustments.R
Rscript --vanilla src/r/tests/test_paper_table_worker.R
Rscript --vanilla src/r/tests/test_directional_forecast_metrics.R
Rscript --vanilla src/r/tests/test_forecast_methods.R
```

Python compilation, R parsing, changed-document relative file links and
`git diff --check` are also checked. Logs are local review artifacts under
`.amp/in/`; they contain synthetic operational evidence, not research scores.

## Remaining review and execution boundary

Chief Developer/Chief Architect review and researcher acceptance come next.
The separately authorised two-machine 100-series run must show Mac and Ubuntu
CPU batch contribution, all 1,050 unique sensitivity aggregates, exact scientific
parity with the reference path, safe resources, sole-writer commits and restart
with no completed candidates rescheduled. This checkpoint did not contact Ubuntu
and cannot claim that evidence.

Complete M4 Daily Table 1 numerical reproduction, Daily Table 2 validation,
approved Figure 2 evidence and final POC2 closure remain open. POC3/POC4,
all-frequency scientific execution, missing historical classifiers, legacy
best-window selection and significance claims remain excluded. Future frequency
scope is configuration-version work over the same calculation, common result
boundary and guarded `All` builder, not permission to execute it now.

## Integrated IDs 058/062 read-only reporting checkpoint

This extension preserves the complete calculation/storage candidate above. The
complete unstaged candidate now contains **37 paths**: the 31 paths in the
earlier exact map plus these six additions:

- `src/python/util/p06_04_result_export.py`
- `src/r/06_03_plot_directional_results.R`
- `src/python/tests/test_result_export.py`
- `src/r/tests/test_directional_result_plots.R`
- `src/python/util/p00_02_researcher_cli.py`
- `src/python/tests/test_main.py`

The export action is registered through the existing CLI/action owners; the
single `src/python/00_main.py` entry point remains unchanged. `TableStorage`
validates the complete stored Process 06 snapshot through a read-only connection.
`TableEvaluation` retains the configured figure profiles using the existing
`TableReports` rank calculation. `ResultExport` flattens stored cells, stages
derived files, invokes only the stateless R renderer and publishes the manifest
last. It never writes DuckDB or calls evaluation/scheduling. The two CLI mock
regressions now use valid temporary configuration-bearing databases instead of
empty files, matching the existing results-routing contract.

Central v12 configuration explicitly selects Mantis, Chronos-2, 1-NN DTW and
SMYL for the horizon figure. One authoritative CD matrix uses Mantis, 1-NN DTW,
FFORMA, Chronos-2 and SMYL. The locked `scmamp` 0.3.2 revision
`3cf4d8b9759769cdf20771afa0efc33a5265c7f9` renders that matrix with alpha 0.05,
reverse TRUE and cex 0.75; PDF uses useDingbats FALSE. Installed package
versions/revision are checked against the unchanged `renv.lock`.

The [central export contract](experiment-configuration.md#selective-figure-2-and-read-only-export-ids-058062)
owns the exact five CSV/six figure/one manifest filenames, the mandatory
`results/<experiment>/` directory, constrained optional output and manifest
fields. The two standalone CD versions currently use the same stored matrix and
mean ranks; no combined-panel file is permitted. Only declared derived files
are replaceable; unrelated files remain untouched. Payload hashes are recorded,
not a circular manifest self-hash. Absolute paths and the operational export
timestamp do not enter scientific identity.

### Researcher correction before Part A acceptance

The initial local renderer combined focused/applicable CD views in one device.
That presentation is rejected and its local test evidence below is preliminary.
Part A must first implement and inspect the two standalone CD versions, enforce
that every persistent database/CSV/PDF/PNG/manifest/evidence file is under
top-level `results/`, and prove that no root-level `Rplots.pdf` is created. Final
evidence must replace the preliminary output counts and rendering assertions.

Final local evidence, using only existing environments and temporary stores:

- **19 focused Python tests passed**: 8 paper-table, 7 configuration and 4 export
  tests. These retain sequential/local-Dask parity and prove exact Table 1,
  Daily Table 2 and long directional CSV parity, Daily_1 through Daily_14 ordering,
  explicit model subsets and independently expected tied average ranks.
- **197 Python regressions passed**, including the earlier 163 accepted tests
  and 34 researcher-entry tests. **11 DTW numerical tests passed**.
- Real R checks passed: **3 plotting groups**, **4 paper metric groups**,
  **3 paper worker groups**, **9 ID 037 groups**, **12 ID 033 groups**, and the
  accepted forecast suite for **9 methods**.
- The actual entry-point export opens every observed DuckDB connection read-only;
  database bytes are identical before and after export and regeneration. Missing
  configured horizons, tampered fingerprints and pending Process 06 state fail
  before rendering/publication and preserve a previous manifest.
- Export runs with scientific evaluation and Prefect compute dispatch patched to
  fail if called. The renderer's isolation assertions exclude RDS readers,
  database access, scientific workers, model fitting and nested orchestration.
  Scientific fixture calculation occurs only while seeding the synthetic store,
  not during export.
- Deleting/recreating the output yields identical CSV, PDF and PNG bytes and
  identical manifest content after removing only `exported_at`. All declared
  payload hashes verify. Renderer failure publishes no partial final output;
  unrelated user files survive replacement.
- The initial synthetic PNGs were inspected before the researcher correction.
  Their combined CD panels are not accepted Part A evidence. The corrected
  standalone research and paper CD files require new visual inspection and
  must retain the dependent-horizon disclaimer.

Review logs and the inspected synthetic figures are under the locally excluded
`.amp/in/artifacts/id038-041-058-062/`. They are presentation/test evidence, not
historical Figure 2 reproduction or authoritative research records. Python
compilation, R parsing, documentation links and whitespace checks are included
in final checkpoint validation; dependency/lock files and the index are unchanged.

Recommendation: Chief Developer and Chief Architect review this one integrated
candidate before separately authorising the consolidated Mac/Ubuntu 100-series
acceptance. No Ubuntu contact, dependency installation, heavy experiment,
staging or publication occurred. Complete Daily scientific reproduction, the
applicable-model Figure 2 acceptance, POC2 closure and later POC3/POC4 execution
remain open. CD bars/grouping describe dependent horizons, not independent-sample
significance. ID 061, excluded historical classifiers and extra legacy figures
remain outside scope.
