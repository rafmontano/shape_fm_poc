# AMP instructions for eight Mac workers and execution safeguards

Status update, 1 October 2026: this task was completed and published as commit
`9579fc9`. Its instruction to leave ID 010 queued records the boundary at that
time; the researcher subsequently reviewed the result and authorised ID 010 in
the [standardisation decision](poc2-standardisation.md).

Approved on 30 September 2026. Implement the three reviewed execution fixes
and increase Mac CPU capacity from five to eight workers. Ubuntu remains at
15 CPU workers and 15 logical GPU workers on one physical GPU. Follow the
[execution policy](execution-policy.md), repository code standards and research
vision. This is a focused operational follow-up, not new forecasting research.

The 800-forecast seasonal test is complete. Preserve its
[acceptance record](poc2-seasonal-period-tuning-results.md), database, backup and
all scientific results. Do not repeat the 69-minute acceptance or run Gates 1–6.
ID 010 remains queued; do not implement its transformations or start it after
this task without the researcher's next instruction.

## Inspect and preserve

Inspect both worktrees and active processes. Preserve all existing uncommitted
implementation, documents, QA scripts and unrelated work. Do not run concurrent
editors/coordinators, reset, stash, delete or overwrite conflicts. Capture the
completed database's counts, scientific/configuration hashes and result
fingerprints read-only; use an existing verified backup or make a safe backup
before any necessary database write. The intended test work is isolated.

Read the policy, this handoff, code-standards.md, architecture.md,
experiment-configuration.md, configuration-inventory.md, local-execution.md,
the seasonal decision and acceptance record before editing. The older
recovery instructions describe the completed five-worker run, not the current
capacity or an instruction to resume 392 tasks again.

## Implement the approved corrections

1. **Make the execution guard unavoidable for heavy work.** The current optional
   profile flag leaves a route to serial tuning. At the shared entry/coordinator
   boundary, reject heavy execution without the approved distributed profile or
   an explicit recorded local exception authorised by the researcher. Apply this
   when the profile flag is omitted as well as supplied; direct wrappers must not
   bypass it. A scoped lightweight-test mode is allowed, but is not a blanket
   exception for the normal 100-series experiment. Read-only operations remain
   available. An unavailable Ubuntu host is not local-run approval.
2. **Use one central profile and eight useful Mac CPU workers.** Evolve the
   existing profile/configuration path; do not add another scheduler or duplicate
   framework. Configure Mac CPU 8, Ubuntu CPU 15, Ubuntu GPU capacity 15.
   R-only checks use 23 CPU workers and no GPU jobs; 38 is full CPU/GPU capacity,
   not a requirement to launch unused GPU workers. Record the effective profile
   version or digest. Preserve existing stored experiment documents, hashes and
   historical execution snapshots.
3. **Remove hidden allocation and concurrency constants.** In
   `src/python/util/seasonal_period_tuning.py`, remove the every-twentieth-ETS
   quota and fixed Mac in-flight limit of one. Host floors, fit budgets,
   concurrency and any model-placement rules belong in the resolved profile,
   not independent literals. Use bounded, resource-aware scheduling so eligible
   work continues on Mac whenever headroom permits. Do not replace the quota
   with another fixed contribution percentage or launch extra idle-only workers
   merely to report eight. Heavy R concurrency may be lower than CPU capacity,
   but its limit and measured reason must be explicit.
4. **Monitor memory throughout the work.** The observed approximately 12 GiB R
   child shows that the 5 GiB AutoARIMA estimate is not a reliable upper bound.
   Set defensible explicit admission budgets and monitor host memory and the
   owned R process tree during fits, not only before dispatch. Preserve the
   3 GiB Mac and 16 GiB Ubuntu host floors, and one thread per R fit. Include
   coordinator/scheduler and worker overhead in the Mac budget. Suspend
   admission on pressure; if needed, safely stop owned work with bounded
   cleanup and retryable state. Never kill unrelated processes or label an
   infrastructure/resource interruption as a scientific model fallback.
5. **Keep the implementation small and shared.** Use existing Dask, profile,
   subprocess, provenance and recovery mechanisms. Python on Mac remains the
   sole DuckDB writer. Do not pass database connections to workers or change
   models, periods, preprocessing, transformations, folds, seeds, scoring or
   the approved 30-minute model timeout to make checks faster. Document each
   changed file/function and report actual limitations.

## Synchronise safely before distributed checks

Recheck Ubuntu connectivity, code and dependencies rather than assuming the
previous synchronisation is current. Sync the exact reviewed code, tests,
configuration, relevant documentation and locks non-destructively. Include
relevant uncommitted files; matching HEAD alone is insufficient. Verify source
manifests, lock identities and loaded dependency versions, then restart only
test-owned workers. Repeat after any subsequent code edits.

Do not copy Mac environments, compiled R libraries, secrets, databases, results
or machine-specific settings onto Ubuntu. Restore only required pinned missing
dependencies through the existing non-destructive approach. On conflicts or
unavailable Ubuntu, stop distributed/heavy work and report the issue; independent
lightweight checks may continue. Retain strict SSH host-key verification.

## Focused acceptance without repeating the full run

- Add negative tests proving omitted profiles, wrong topology, missing Ubuntu,
  stale source and incompatible dependencies cannot silently launch a heavy
  local run. Test an explicit bounded local exception separately; the tests
  do not authorise one for this task's distributed checks.
- Test profile propagation into actual scheduling. On safe synthetic
  low-memory tasks, demonstrate that all eight Mac CPU workers can execute
  work, including overlapping execution. Cover a queue long enough to expose
  the removed quota; Mac dispatch must not stop after every-twentieth sampling.
  Verify configured in-flight limits rather than just counting connections.
- Simulate memory growth above the admitted estimate, host-floor breaches,
  swap growth and worker loss. Verify admission pauses, bounded cancellation
  of owned processes where required, truthful retryable state and preservation
  of completed results. Do not allocate many GiB or force a real OOM to test it.
- Run a small isolated real AutoARIMA/ETS check with both hosts contributing
  concurrently through the corrected execution path. Demonstrate useful Mac
  concurrency above the old one-task queue where safe; explain observed limits.
  Include fresh fold preparation/validation, not only cached selections.
  Compare a small sequential reference with identical inputs, settings and
  declared tolerances; report forecasts, scores, policies and periods.
- Exercise interruption and zero-work restart on the isolated case. Recheck
  the completed 800-forecast database read-only: no changed scientific rows,
  counts or fingerprints. Do not refit its forecasts or repeat its timeouts.

Report requested/started workers, peak active tasks and completed tasks by host,
overlap intervals, admission waits, R/host memory peaks and minima, safety
responses, failures/retries and comparison results. Eight connected workers is
not sufficient evidence of utilisation. If the checks cannot demonstrate safe
use, report the limitation rather than weakening them or forcing unsafe fits.

## Documentation and GitHub checkpoint

Correct any claim that all policy enforcement is already complete until these
checks pass. Keep the original 5+15 recovery results, timings and fingerprints
unchanged; append separate dated eight-worker evidence and the source identity
tested. Do not present historical runs as evidence of the new configuration.

After focused acceptance passes, create the reviewed code/documentation GitHub
checkpoint approved with this follow-up. Inspect the exact changes and stage
only explicit paths belonging to the approved seasonal/execution work and
supporting documentation. Preserve unrelated and queued ID 010 work; do not
publish databases, backups, results, logs, environments, secrets or model files.
Verify the existing remote/branch and divergence; use a normal push, never a
force push or history rewrite. Stop for conflicting remote work or credentials
rather than overwriting it. Confirm the remote commit and synchronise Ubuntu
to that reviewed source identity safely.

Return changed files, effective profile and limits, tests executed/skipped,
source/dependency and utilisation evidence, memory-safety proof, unchanged
800-result fingerprints, exact commands, unresolved limitations and commit/push
status. Stop after this handoff; do not update the spreadsheet or start ID 010.
