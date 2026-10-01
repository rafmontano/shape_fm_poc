# AMP Code Instructions for POC2 Standardisation

Implement approved migration ID 010 in `/Users/monta/Documents/Projects/shape_fm_poc` using the [decision](poc2-standardisation.md). The decision is the scientific contract; the [evidence record](poc2-standardisation-evidence.md) explains why and distinguishes completed probes from tests still required.

## Sequence and authority

Start authorised on 1 October 2026. The seasonal test and its
[execution safeguard follow-up](amp-poc2-execution-safeguards-instructions.md)
are complete at published commit `9579fc9`. Mac, Ubuntu and `origin/main` were
reported clean and synchronised at that commit. The researcher reviewed the
result and explicitly authorised ID 010. The [execution policy](execution-policy.md)
governs any heavy tests, including explicit approval for a laptop-only
exception.

Confirm that the reported clean `9579fc9` baseline and stopped worker state
still hold before editing. If either host has subsequently changed, preserve
that work and resolve the difference safely rather than resetting, stashing or
overwriting it. Do not start a second concurrent editor/agent on shared files.

After that boundary, inspect the actual worktree and re-read the completed code. File names and configuration versions mentioned here are observations, not assumptions about the final previous-task result. Preserve all unrelated changes, original data, accepted results and existing QA scripts. Do not reset, stash, delete or overwrite another task's work.

Read the decision and evidence, research-vision.md, code-standards.md, architecture.md, experiment-configuration.md, configuration-inventory.md, preprocessing.md, forecast-methods.md and the completed seasonal-period decision before editing. The approved gate-local checks here do not require the full two-machine end-to-end acceptance described elsewhere.

## Synchronise Ubuntu before use

Ubuntu was synchronised to the published `9579fc9` baseline during the
prerequisite task. Verify that baseline before editing; do not repeat a transfer
merely for appearance. Preserve uncommitted and unrelated work on both hosts;
resolve any overlapping edits with the researcher rather than overwriting them.

After ID 010 implementation, synchronise its changes again before distributed tests. Repeat this check whenever the code under test changes. Include the required source, tests, shared configuration and relevant dependency declarations, not just the last committed revision if the tested Mac code has uncommitted changes. Do not copy Mac virtual environments, compiled R libraries, secrets, databases, results or machine-specific settings onto Ubuntu.

Verify and record matching tested revisions, submodule revisions where applicable, and source fingerprints for any relevant uncommitted files. Check each host's existing compatible environment separately. Restart only workers owned by this test when necessary to load updated code; do not interrupt another active task. Do not run distributed acceptance with stale code or treat matching Git HEAD alone as sufficient when working trees differ. If safe synchronisation cannot be completed, report the blocker and leave distributed validation outstanding.

## Implement the small shared contract

1. Evolve the existing `src/python/util/transformations.py`. Add a small R counterpart under `src/r/util/transformations.R`, following repository naming rather than introducing a `utils` folder. Both implement fit, apply and inverse locally; do not transfer data between R and Python solely for scaling. Avoid duplicate mathematical implementations inside either language.
2. For finite nonconstant history use centre=mean(x), scale=sample SD with n−1, forward(y)=(y−centre)/scale and inverse(z)=centre+scale*z. In Python select the sample convention explicitly; use double precision in both languages.
3. Detect exactly constant nonempty history before calculating SD, including length one. Store centre=x[1], effective scale=1 and constant=true. Apply y−centre and invert centre+z for every supplied value. Do not zero arbitrary future values or force every forecast to the historical constant. Nearly constant nonconstant input is not silently clipped to constant.
4. Validate inputs, fitted state, supported versions and finite results. Empty/nonfinite histories, invalid state and numerical failures produce clear structured errors through existing failure handling, not fabricated zero-series success. Missing-value repair belongs to Gate 2. Invalid test actuals remain subject to existing evaluation rules, not imputation by the transformer.
5. Register the approved recipe as `standardise_sample_v1` in both languages.
Use the same JSON-serialisable state keys: `recipe`, `version`, `centre`,
`scale`, `count` and `constant`. Do not silently accept aliases with different
semantics. Applying/inverting a fitted state must not refit. State produced by
one language must be usable by the other without reinterpretation.
6. Retain only the small ordered-step interface `identity` and
`standardise_sample_v1`. Compose registered steps, not executable formula
strings or model-specific branches. Apply in order, invert in reverse. Do not
add new min–max, nonlinear or automatic transformation-selection capabilities.

## Integrate without changing the preceding experiment

7. Audit all actual callers after the preceding task completes: normal Gate 3 preparation, Gate 4 output inversion, historical tuning-fold preparation/scoring, final tuned forecasts, calibration, task planning, retrieval and tests. Replace copied scaling logic with the shared contract where applicable. Preserve each fold's raw training boundary; never fit on full-history preparation and then slice it into earlier folds.
8. Route one declared preparation per selected consumer/variant. Do not force all models to standardise or apply the same step twice across adapters. Preserve the current no-transformation route. Do not modify Mantis internal normalisation, resizing, model weights or forecast-method internals as part of this task.
9. Evolve the current configuration version and stored original/resolved contract together as needed. Introduce the new recipe for new experiments; do not repurpose `minmax_then_standardize` or relabel old outputs. Preserve existing identity/schema semantics required to retrieve and resume old experiments. Minimal existing-version compatibility is not a new researcher-selectable legacy mode. Use a fresh isolated database for scientifically changed acceptance.
10. Include recipe/version, fitted-input/window identity and required preparation provenance in relevant scientific/cache identities. Reuse current transformed-output/state storage; do not duplicate raw datasets or introduce another scheduler. Python remains the sole DuckDB writer. Completed compatible work must survive resume; old/new or different-fold results must not collide.
11. Invert only numeric forecasts actually supplied by the provider, including mean and any median/quantiles. Positive affine inversion preserves their interpretation and ordering. Do not fabricate probabilistic forecasts or invert class labels/embeddings. Do not change archived Smyl/FFORMA original-scale forecasts.

## Migrate the selected utility responsibilities

- Consolidate `standardise_vec` and `scale_pair_std` into the same fit/apply implementation per language; a convenience pair operation may reuse it without copying the calculation.
- Preserve `compute_label_vector` as the strict rule future > last history gives 1, otherwise 0, in a cohesive label utility rather than transformations. Compute labels from original observations or consistently transformed values using the same positive affine state; do not reuse zeroed legacy futures. Ties remain 0.
- Replace `get_window_size_from_h` with explicit, validated context-length configuration in the appropriate window-preparation responsibility. Preserve required old choices for replication (the old M4 Daily default was 64) without treating them as universal sampling-frequency rules or deriving them from forecast horizon. Do not shorten every model's history to 64 or change the preceding tuning experiment's expanding histories.
- Where a selected utility has no production consumer yet, provide a documented, tested capability and state that fact; do not invent a feature-training or rolling-window pipeline merely to call it. If fulfilling the selected scope requires a larger new workflow, report the dependency for review.
- Do not migrate the functions marked Not needed. Do not delete old scripts, remove unrelated compatibility or implement other vision gaps.

## Validate before reporting completion

Until the researcher advises otherwise, use both the Mac and Ubuntu for computationally heavy tests through the existing bounded distributed execution. The researcher reports that Ubuntu is now on; verify readiness before use. Lightweight unit and numerical-fixture checks may remain local. This supersedes the earlier Mac-only testing guidance, not the requirement to finish the preceding task before starting ID 010.

For heavy tests, complete and verify the Ubuntu code synchronisation above before scheduling work:

- Verify connectivity, matching tested code revisions or source fingerprints, compatible declared environments, worker capabilities and available resources on both hosts. Use existing execution profiles and safe synchronisation; preserve changes on both machines. An Ubuntu GPU worker is not automatically evidence that the relevant Python or R CPU workload is supported there.
- Run actual eligible work on both machines, with bounded overlapping execution. Keep the Mac coordinator and Python sole-writer DuckDB architecture, existing resource limits and restart behaviour. Do not add nested parallelism or a second scheduler.
- Record host and worker identities, completed task counts per host, task start/end times demonstrating overlap, runtime, failures/retries and relevant resource observations. Two connected workers alone do not prove parallelism.
- Compare a small representative subset against sequential execution using the same inputs and controlled seeds where applicable, with declared numerical tolerances. Do not repeat an entire expensive workload solely to obtain a sequential reference, or claim speedup merely because both machines ran.
- If either host or its required environment is unavailable, report the limitation and leave the two-machine check outstanding. Continue safe independent lightweight checks where useful; do not silently substitute a Mac-only heavy run or report distributed acceptance as passed.

This changes execution placement, not scientific scope. Keep the existing 100-series limit and gate-local tests. Do not add model downloads, dependency upgrades, a full-dataset run or full import-to-evaluation run. Use GPU acceleration only where an already approved workload needs it; CPU standardisation does not require GPU computation.

The prerequisite run reported that `renv` considers the project lock not fully
synchronised, while all required pinned R dependencies loaded successfully.
Treat that as a recorded repository-maintenance limitation, not permission to
upgrade packages or regenerate the whole lock during ID 010. If ID 010 genuinely
requires a new dependency, stop and report it for review; prefer the existing
base R/Python numerical facilities.

1. Add shared numerical fixtures for both languages: positive/nonconstant, negative, exactly constant, one observation, small real variation, invalid empty/nonfinite input, malformed state, values beyond training range, and finite inverse recovery. Establish explicit absolute/relative tolerances appropriate to double precision; report maximum discrepancies rather than claiming bitwise equality.
2. Check new R versus new Python values and parameters. Fit in R then apply/invert in Python, and vice versa, including JSON state round trips. Do not use either new implementation as the sole expected-value oracle; include hand-calculated fixtures such as x=(10,20,30) giving (-1,0,1).
3. Verify constants: fit (5,5,5), obtain zero history, apply(7)=2, inverse(2)=7, inverse(1.5)=6.5, and retain upward label 1. Verify ties and zero/negative constants. Test that the inverse does not collapse distinct supplied forecast values.
4. Compare to the previous R recipe on identical nonconstant vectors, without refitting on held-out targets. Explain expected denominator differences against old Python and corrected-constant differences against both old implementations. Account for FLOAT storage precision independently of formula differences.
5. Exercise the existing normal and tuned output paths with focused tests, using lightweight/stub forecasts where sufficient. Vary only held-out future values and verify fitted transformation state and training inputs do not change. Test per-fold state ownership and original-scale scoring/inversion.
6. Test configuration rejection/acceptance, old stored experiment semantics, new recipe identity separation, state persistence, resume without repeating completed work, and sequential versus bounded-parallel equivalence. For computationally heavy checks, use both hosts and retain the contribution/overlap evidence above. Do not silently weaken existing tests to accept changed historical results.
7. Run gate-local preparation/inspection on at most the first 100 official M4 Daily series in an isolated experiment or read-only source workflow. Keep the preceding task's database and outputs untouched. Full model refits or accuracy comparisons are not required to prove the transformation contract.
8. Add a small source()-friendly R QA script under `src/r/qa`, following existing QA style. Select a dataset/series/window, retrieve stored history and fitted state read-only where available, leave inspectable original/transformed/restored objects and checks in the R session, and clearly distinguish stored results from local recomputation. Do not overwrite existing QA scripts or change their contracts.

## Documentation and handoff

Document all changed/new files and functions using the repository's human-readable comment standard. After the preceding task is complete, update relevant README/architecture/configuration/forecast documentation and link the approved decision and evidence. Preserve historical evidence; append implementation results with dates/revisions rather than rewriting prior observations as new successes.

Return: changed files; final configuration and recipe versions; R/Python contract and state fields; runnable gate-local and manual-QA instructions; tests executed and skipped; numerical differences; constant counts and corrected labels; preservation/resume checks; and any unresolved dependencies. For heavy tests, include evidence of actual contributions from both Mac and Ubuntu, overlapping execution, reference-result agreement and failures/retries. Report any selected function that is tested but not yet connected to a consumer.

Keep implementation and acceptance status distinct. Do not mark ID 010 closed while required checks are outstanding, and do not update the spreadsheet yourself. This task does not instruct a commit, push or other publication; report Git status and preserve the documents for the user's normal publication workflow.
