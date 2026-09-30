# AMP instructions for distributed tuning recovery

Historical handoff: this five-Mac-worker recovery finished with 800 forecasts.
Do not repeat it. The researcher has now approved eight Mac CPU workers and
three enforcement corrections; follow the
[current safeguard instructions](amp-poc2-execution-safeguards-instructions.md)
and [execution policy](execution-policy.md). Counts and steps below describe
the earlier recovery, not the current task or runtime capacity.

Approved on 30 September 2026. Finish the interrupted seasonal-period test
safely under the [execution policy](execution-policy.md). Implement its runtime
checks; do not merely change documentation or worker numbers. The researcher
approved safe Ubuntu synchronisation and bounded recovery testing. Do not begin
ID 010 afterwards: report closure and wait for the researcher to return to it.

Read the execution policy, [seasonal decision](poc2-seasonal-period-tuning.md),
[seasonal implementation instructions](amp-poc2-seasonal-period-tuning-instructions.md),
[research vision](research-vision.md), [code standards](code-standards.md),
[architecture](architecture.md), [configuration reference](experiment-configuration.md)
and [local execution guide](local-execution.md). This recovery supersedes older
Mac-only or smaller-profile interpretations, not the approved science.

## Preserve and inspect first

Recheck Git status and active processes on both hosts. Preserve AMP's existing
uncommitted implementation, researcher documents and QA scripts. No concurrent
editors or coordinators on this task, resets, cleanup deletions, force pushes,
destructive synchronisation or unrelated refactoring.

The read-only review found the following state; revalidate it before use:

- Mac: `/Users/monta/Documents/Projects/shape_fm_poc`, HEAD
  `94a3f09607f55bdf16e987aebc9be1e4b19f706b`, with uncommitted tuning work.
- Ubuntu: `/home/rafmontano/Documents/PhD/2026/projects/shape_fm_poc`,
  clean at `ef2b5a2a99d656965bd9b7e4b982e648e6f115cc`, eight commits behind Mac.
- `WSUbuntu1.local` did not resolve, but SSH to the previously recorded
  `192.168.1.44` succeeded with strict host-key verification and the hostname
  alias. Use verified machine configuration, not a hardcoded source-code IP.
  A separate AMP runner is not necessary when approved SSH works.
- Ubuntu had R 4.6.1 and forecast 8.24.0, but lacked `tsfeatures`; the Mac lock
  pins tsfeatures 1.1.1. Python locks matched; R locks differed.
- `results/poc2_m4_daily_100_period_tuning.duckdb` contained 408 forecasts,
  408 selections, 615 folds and 2,448 validation rows. Gate 4 had 408 completed
  and 392 stale running tasks; no matching Mac forecasting process was alive.
  A `.duckdb.wal` file was present. Other gates are not this recovery's scope.

Before database writes, ensure no active writer and make a uniquely dated,
consistent recoverable backup in an ignored location. Preserve the WAL with its
database if recovery requires it; never delete it as cleanup or copy a live
database/WAL pair inconsistently. Verify backup readability. Capture counts,
scientific/configuration hashes and fingerprints of completed forecasts and
tuning evidence for preservation checks.

## Repair the shared execution path

1. Resolve one approved profile centrally: Mac CPU 5, Ubuntu CPU 15, Ubuntu
   logical GPU capacity 15 on one physical GPU. This R-only test uses CPU pools,
   not GPU jobs. Reuse existing structures, not a second scheduler or universal
   framework. Document the exact profile owner and precedence.
2. Wire the resolved profile through `00_main.py`, numbered wrappers,
   coordinator, validators, worker startup, task admission and test harnesses.
   Preserve old stored documents and hashes; add an explicit audited execution
   override for resume. Do not rewrite the experiment or assume editing its
   original JSON changes the database configuration.
3. Remove tuning's Dask rejection and serial-only orchestration. Schedule real
   preparation, diagnostics and R fits through the existing bounded scheduler.
   Preserve compatible fold/candidate sharing, deduplication, task identities,
   per-result persistence and restart. Workers never receive the coordinator
   or a DuckDB connection; Python on Mac alone writes.
4. Make preflight workload-aware, not merely Gate-4-aware: AutoARIMA/ETS must
   not require Chronos settings, its cache or GPU workers. Verify actual R/Python
   dependencies and code loaded by CPU workers; reject mismatches before fits.
5. Implement memory-aware admission through existing controls. Keep approved
   pools, one thread per R fit and host memory floors. Include R child memory;
   five 4–5 GiB fits do not fit on Mac, and a 2 GiB worker budget does not cover
   them. Report resolved budgets, placement and throttling. Safety responses
   must be visible, bounded and restartable.
6. Recover orphaned task/attempt/invocation state through normal recovery,
   recording interruption rather than fabricated completion. Claim work when
   dispatched where feasible, so queued work is not misleadingly called active.
   Preserve timeout/fallback evidence. Infrastructure failures must not be
   presented as scientific model failures or successful candidate validation.

Keep the 100 series, four existing preparation variants, AutoARIMA/ETS,
three folds, MAE rule, model settings and 30-minute R timeout unchanged. Do not
implement ID 010, change transformations, add models, run 4,227 series or run
Gates 1–6 merely to validate recovery. Document all changed files and functions
with human-readable comments.

## Bring Ubuntu to the exact tested source

Inspect both worktrees again before synchronising. Use the established
non-destructive workflow to include reviewed implementation, tests,
configuration, documentation and locks, including relevant uncommitted files.
An old GitHub checkout or matching HEAD alone is insufficient. Preserve new
remote work; stop on conflicts instead of overwriting or stashing it.

Verify matching tested source manifests and relevant submodule/lock identities.
Restore required missing pinned packages and dependencies locally on Ubuntu and
test that they load. Do not transfer Mac environments, compiled libraries,
secrets, databases or results. No reset/from-zero installation. Recheck source
after further edits and restart only this test's workers.

This handoff authorises machine synchronisation, not blanket staging or
publication. If the chosen workflow needs an unapproved commit/push, request
that permission; otherwise use verified non-destructive source synchronisation.
Do not bypass clean-tree checks silently: implement and test an explicit source
manifest verification path if needed for current uncommitted work. Report the
tested identity accurately.

## Validate before the expensive resume

Run lightweight checks first: central profile propagation, no hidden serial
fallback, workload-aware preflight, old-database loading and identity preservation,
interruption recovery, memory admission and the sole-writer boundary. Negative
tests must reject wrong worker counts, unavailable hosts, stale source and
missing packages. Simulate an approved local exception separately without
granting one for this run.

Next run a small isolated real-data forecast check through the same execution
path. Demonstrate AutoARIMA/ETS computation on both hosts with overlapping work;
compare a small sequential reference with identical prepared inputs, settings
and controlled seeds. Declare tolerances; compare forecasts, scores and policy
choices. A parallel selection helper or connected-worker count is insufficient.
Include restart and safe interruption coverage without altering saved acceptance
results.

Report preflight and small-test evidence before the long resume. When approved
checks pass, proceed without requesting another worker-count redesign. On
safety, compatibility or connectivity failure, stop heavy work and report the
specific issue; never silently substitute a long Mac-only run.

## Resume and close the seasonal test

Resume Gate 4 against the existing database with the explicit approved execution
override. Revalidate the remainder: the review observed 392 tasks, not a fresh
800-task run. Reuse completed folds and validations as well as final forecasts.
Do not replay historical timeouts to erase their recorded outcomes.

Provide periodic concise progress: completed/remaining tasks, active work and
completed contributions per host, memory/safety state, failures and slow fits.
Give an observed-throughput ETA range only when useful; do not promise linear
speedup from worker counts or leave a long stall unexplained. If a host
disappears, preserve results and follow the availability policy.

Closure requires 800 final forecasts and their selections, or an explicit
unresolved failure report instead of a pass. Reconcile fold/validation counts
and report policy choices, MAEs, substitutions, timeouts and inconclusive cases.
Verify the original 408 forecasts and compatible completed evidence are
unchanged. Repeat Gate 4 resume: no completed scientific computation or duplicate
rows, unchanged result fingerprints; a new execution event is expected.

Return changed files, exact preflight/resume/QA commands, profile owner and
effective settings, source identities, Ubuntu synchronisation/dependency
evidence, real host contributions/overlap, reference tolerances/results,
resource observations, failure/retry counts, preservation/restart proof and Git
publication status. Record dated results and limitations in the repository.
Do not claim full-pipeline acceptance. Stop here: leave ID 010 queued for the
researcher's next instruction and do not update the spreadsheet.
