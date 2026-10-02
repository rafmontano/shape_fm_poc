# AMP Code implementation instructions for IDs 011 and 016

Status: Original scope approved on 1 October 2026; the researcher approved the
review corrections and closure work on 2 October 2026. Scoped GitHub publication
and safe Ubuntu synchronisation remain authorised. The final coordinator guard,
actual tsai workflow, focused checks and fresh two-host acceptance are complete;
publication and final revision synchronisation remain. The work continued from
22e41bb without changing the approved architecture or scientific settings. See
[`poc2-rolling-windows-acceptance.md`](poc2-rolling-windows-acceptance.md).

Complete the combined
[rolling-window and S1 split decision](poc2-window-training-architecture.md).
Read its linked standards and the [configuration contract](experiment-configuration.md),
[standardisation decision](poc2-standardisation.md) and
[execution policy](execution-policy.md). The decision owns the exact settings;
do not maintain another independently editable table here.

## Final closure work

This section identifies the remaining work after 22e41bb. The later sections
remain the governing contracts, not instructions to redo completed work.

Implementation and acceptance completed on 2 October 2026. The requirements
below are retained as the review record; see the acceptance document for actual
counts, host contributions, numerical comparison and QA commands.

1. **Enforce the coordinator safeguard.** A direct local call to
   `WindowPreparationCoordinator.run()` must reject absent or invalid explicit
   limits before preparation writes or worker launches, just as the entry point
   does. Enforce the workload bounds and existing heavy-run approval policy;
   supplying arbitrary large limits is not approval for heavy local testing.
   Update focused fixtures to declare their bounds rather than bypass the guard.
   Add direct-coordinator and command-line regression tests for missing,
   invalid and exceeded limits, plus a permitted small run. Preserve the
   approved distributed route and its memory safeguards.
2. **Use tsai in the actual experiment workflow.** Retain bounded DuckDB reads
   and batches, but pass each bounded source block through the pinned
   `SlidingWindow` helper. Do not keep a separate custom window generator for
   the actual workflow while testing tsai elsewhere. Align batches to complete
   W+H blocks, preserve absolute offsets and stable global window ordinals,
   and retain no-padding, protected-boundary and partial-resume behaviour.
   Future values may be read transiently for window generation; they must not
   enter cleaning/standardisation or be copied into the child database.

Test the real coordinator-to-window-generator path, proving tsai is called and
its returned inputs are the ones processed and stored. Compare values and
boundaries with an independent reference across all ten configured frequency
fixtures, multiple chunks, nonzero offsets, short/tail cases and partial resume.
A helper-only test is insufficient. Keep the approved defaults, S1 membership,
R rounding, cleaning and transformation semantics unchanged; do not introduce
new libraries, split strategies, model work or unrelated refactoring.

Recheck Mac, Ubuntu and GitHub at the current baseline, preserving these
documentation edits and any unrelated work. Synchronise the corrected runtime
files and verify their actual manifest, locks and native dependencies before
running affected lightweight checks on both machines. Then validate the changed
window path with the full first-100-series M4 Daily preparation using the
approved 8-Mac/15-Ubuntu CPU profile. Use a fresh isolated acceptance pair so the
existing accepted artifacts remain untouched; do not bypass source fingerprints
to reuse a child built by different code. No GPU or unrelated earlier-gate or
seasonal-tuning acceptance is required.

Reconcile the persisted result against the previous 632 windows, 81/19 series
and 495/137 train/test windows. Compare per-series membership, positions and
transformed values/hashes rather than only totals; investigate any unexpected
change without changing scientific settings. New preparation/run identifiers
may differ and are not themselves a scientific mismatch. Verify real work and
overlap on both hosts, Python/R retrieval, and a resume with zero recomputation
and no duplicate windows. Retain the already passed GIFT-Eval evidence unless
affected code or dependencies require its checks to be rerun.

Update the decision, acceptance record and affected researcher documentation
with the actual final path, evidence and manual QA commands. Review and publish
only scoped code/tests/documentation with the authorised normal GitHub push,
then safely synchronise Ubuntu and verify the three revisions and source
manifest. Preserve all databases, results and unrelated changes. Report each of
the two fixes and its tests, final persisted counts, commit and host status.
Recommend IDs 011/016 ready for closure only when every requirement passes;
otherwise report the precise outstanding item. Do not update the spreadsheet.

## Scope and preflight

One preparation workflow: identify eligible series, assign the split, generate
windows, standardise, store and retrieve. Do not implement labels, features,
Mantis adaptation/embeddings, training, predictions, alternative split
strategies, model-specific training caps or unrelated script removal.

Inspect current work and active tasks; finish/report any preceding active task
before starting and preserve unrelated edits. Use a fresh isolated experiment
for new scientific settings; retain all existing databases, results and QA
scripts. Commit and normal push are explicitly authorised for the reviewed
changes required by IDs 011 and 016, including these decision/instruction
documents. This is not permission to publish unrelated pending work.

## Approved review corrections

Complete these corrections before the final distributed acceptance. The
scientific window defaults, stride W+H, S1 strategy, seed and scope are unchanged.

1. **Execution safeguard.** Enforce the existing heavy-run policy through the
   entry point and coordinator, including when no profile flag is supplied.
   A local focused check must have explicit, enforced workload bounds; the name
   `sequential_focused` is not a safeguard. Reject unbounded local preparation
   without recorded researcher approval. Test the rejection before work starts.
2. **Database pairing.** Validate parent, child, preparation and source identity
   before returning a window or its future, in both Python retrieval and R QA.
   Matching dataset/series keys alone is insufficient. Reuse persisted hashes
   and lineage; reject mismatched or ambiguous pairs and invalid boundaries.
   Test a wrong parent containing the same dataset/series identifiers but
   different observations, as well as the correct pair.
3. **Historical split rounding.** Restore the previous R double-arithmetic
   expression `max(1, floor((1 - TRAIN_FRAC) * N))`, not Decimal rounding.
   At TRAIN_FRAC=0.80, verify against R: N=10 gives 1 test series, N=100 gives
   19, and N=4227 gives 845. Thus the corrected 100-series run uses 81 train
   and 19 test series. Keep tsai's disjoint two-way allocation and its recorded
   new-allocation provenance; matching counts does not reproduce R sampling.
   Version the corrected split semantics and create a fresh isolated experiment
   and child for acceptance. Do not rewrite old 80/20 memberships or evidence,
   or silently reinterpret earlier version-5 experiments during resume.
4. **Bounded preparation.** Retain the small identity/membership index, but read
   observations and generate windows progressively in bounded batches. Do not
   first collect every target or window in memory. Bound the inputs, returned
   arrays and pending writes, including a single long series; reuse the existing
   scheduler and sole writer. Demonstrate limits with synthetic fixtures and
   prove partial-batch interruption/resume without duplicates or changed values.
5. **Central settings.** Keep the approved defaults in their configuration
   owner, removing the second exact-value window table from Python validation.
   Validate supported keys, numeric types, positive lengths and consistent
   policies; derive stride once and persist it. Test that a valid changed window
   length works in a fresh experiment and resumes from stored settings without
   a code edit. Keep live acceptance at the approved M4 Daily defaults; do not
   add other dataset importers or new split strategies.

## Implementation

1. **Central settings.** Carry the decision's window table, stride rule,
   S1 policy, training proportion 0.80 and split seed 123 through creation-time
   JSON, validation, resolved configuration, DuckDB and worker payloads.
   Persist calculated strides without another editable stride setting.
   Reconcile existing context-length/seed fields; distinguish split seed from
   other existing seeds rather than changing them globally. Version the
   configuration coherently, preserve older stored interpretations and resume
   without requiring the original JSON. Keep window horizons separate from
   benchmark evaluation horizons.

2. **Eligibility and membership.** Use permitted historical boundaries and
   calculate eligible original series before splitting. Within each frequency
   pool, use a deterministic ordering of canonical dataset/series identities;
   equal source IDs in different datasets must not collide. Persist the eligible
   cohort and exact membership before scheduling window jobs. Preserve the
   previous S1 test-count rule, max(1, floor((1 - training_fraction) * N));
   explicitly test rounding against the old R expression and require nonempty
   training and test partitions. Report pools with fewer than two eligible
   series. Never split by window row or apply stratification, balancing or caps.

   Where compatible historical membership is available, validate its source,
   cohort and row-to-series mapping before reuse. R row indices alone are not
   stable identifiers. Otherwise generate membership once using a pinned tsai
   splitting utility on the eligible series-ID list, with seed 123, the explicit
   test count and stratification/balancing disabled. Record generator/version,
   source ordering and membership fingerprint. Identify this as a new allocation,
   not bit-identical R reproduction. Declare reused versus generated membership
   at creation as provenance, not another scientific split strategy. Persisted
   membership, not rerunning the random generator, governs resume.

3. **Windows and preparation.** For each permitted contiguous segment, start
   at its first observation, take W inputs and H future observations, then
   advance W + H. Use explicit tsai SlidingWindow settings and retain zero-based,
   end-exclusive positions. No partial blocks, random offsets, padding or
   crossing series/protected boundaries. Report zero-window series and unused
   tails; do not borrow the trailing-context helper's padding policy.
   Preserve approved cleaning policy and provenance. Fit standardise_sample_v1
   on each input only; do not slice full-series-standardised arrays or fit on
   future values. Report any upstream preparation boundary conflict rather
   than silently altering preprocessing. Persist transformed inputs and state;
   future values remain in the parent, referenced by position. Pin required
   dependencies narrowly; report broader environment incompatibilities.

4. **Normalised persistence.** Add integer lookup mappings to existing
   dataset/series/frequency identities in the new parent experiment without
   replacing string primary keys throughout the project. One child DuckDB
   covers all selected frequencies and uses matching parent keys. Store
   definitions/descriptions once. Store series membership once per split
   definition in the child; windows inherit it through the series key.
   Do not duplicate train/test data arrays. Window identities must distinguish
   source version, preparation definition and positions; record split identity
   separately so membership cannot silently change.

   The coordinator alone writes either database. Workers receive bounded
   serialisable inputs and return results. Make window writes and completion
   idempotent; reconcile interrupted parent/child updates without assuming
   cross-file atomic transactions. Validate parent identity and membership
   fingerprints on resume and refuse mismatches.

5. **Researcher access.** Expose the combined preparation as an explicit opt-in
   capability through the existing entry point/coordinator, not mandatory
   Gate 3 or forecast processing. Reuse helpers and scheduling. Provide read-only
   retrieval by dataset/series/window, with inherited train/test membership.
   Extend or add a small R QA script under src/r/qa for manual inspection;
   leave existing QA contracts intact. Add human-readable comments and update
   affected configuration, architecture and researcher documentation.

## GitHub and machine synchronisation

1. Inspect Mac, Ubuntu and the verified GitHub remote/branch, including local
   changes, unpublished commits and active jobs. Fetch before deciding how to
   synchronise. Preserve unrelated work; stop for guidance on conflicts or
   unreviewed outgoing commits rather than publishing them implicitly.
   The latest reported shared baseline is 22e41bb. Verify the live state and
   fast-forward only where safe; do not repeat the older bc50945 recovery or
   move a newer checkout backwards.
2. Before distributed tests, synchronise the reviewed task source and required
   locks to Ubuntu safely. Verify the actual tested file manifest on both
   machines, including relevant uncommitted files and submodules; matching HEAD
   alone is insufficient. Keep native environments local and restore missing
   dependencies non-destructively from approved locks. Do not transfer databases,
   results, environments, secrets or machine-specific settings.
3. After validation, review the diff and stage explicit task paths only,
   including updated documentation and manual QA scripts. Exclude generated
   data, model weights, results, environments, caches, credentials and editor
   histories. Review every outgoing commit. Create scoped commits, fetch/recheck
   remote ancestry, and push normally to the verified upstream. Never force-push,
   discard local changes or rewrite history to resolve a conflict.
4. Bring Ubuntu safely to the published revision without overwriting local
   work. Verify Mac HEAD, live GitHub branch tip and Ubuntu HEAD, plus the tested
   source manifest. Report commit IDs and remaining local changes separately.
   If code changed after testing, rerun affected checks; do not repeat unchanged
   expensive work solely for publication.

If Ubuntu is unavailable, safe GitHub publication may still proceed with
incomplete testing clearly documented, but Ubuntu synchronisation and
distributed acceptance remain outstanding. Do not claim full completion or
substitute a heavy Mac-only run.

## Validation and completion report

Use small fixtures for all ten frequencies and at most the first 100 official
M4 Daily series for live acceptance. Do not download all datasets or train models.

Verify:

- Daily W=64, H=14, stride=78: L=156 permitted observations gives inputs
  [0,64), [78,142) and futures [64,78), [142,156). Compare with the old R S1
  generator and an independent reference.
- Exact-fit, short and remainder cases; protected boundaries; transformation
  values/state, including constants and cross-language agreement.
- S1 count/rounding; empty/singleton eligible pools; unequal series lengths;
  namespaced identities; disjoint train/test series and inherited membership
  for every window. Changing worker placement must not change membership.
- Historical membership compatibility or explicitly new membership provenance;
  configuration persistence; retrieval; bounded execution; interruption between
  file updates; duplicate-free resume without JSON, resampling or data changes.

Heavy checks require matching tested source on Mac and Ubuntu and the approved
8-Mac/15-Ubuntu CPU profile/safety limits. Show completed tasks on both hosts,
overlap and agreement with a small sequential reference. No GPU work is needed.
If Ubuntu is unavailable, leave distributed acceptance outstanding.

## Closure acceptance

Run affected lightweight Python and R checks on both machines after the final
fixes. The pinned GIFT-Eval semantics suite already passed on both hosts at
22e41bb; retain that evidence unless affected code or dependencies change.

Then execute and persist the full first-100-series M4 Daily preparation in the
fresh corrected experiment using the approved 8-Mac/15-Ubuntu CPU profile.
Verify the corrected source manifest and dependencies on both hosts immediately
before execution. Record real task contributions and overlap, available and
active worker counts, resource limits and throttling. Use sufficient bounded
work units to expose useful parallelism without artificial participation quotas
or unsafe simultaneous workloads. No GPU or model training is required.

Independently reconcile the number of planned and stored windows, boundaries,
tails and partitions. The 22e41bb run persisted 632 windows, unlike the earlier
count-only check. Preserve that evidence and verify the final code path itself;
investigate unexplained changes without adjusting settings to force a total.
Verify disjoint series membership, actual Python/R retrieval,
per-window standardisation, a small sequential reference and duplicate-free
resume that skips completed work. Supply exact manual QA commands with real
stored identifiers, not placeholders.

Preserve the earlier Mac evidence as historical. Update documentation with the
five fixes, tests, complete persisted counts, scientific/source identities and
final GitHub/Mac/Ubuntu revisions. Recommend closure only when these checks and
publication/synchronisation succeed; otherwise list the precise remaining gap.

Report changed files, resolved settings, source/dependency identities, checks
passed/skipped, and usable-length, series and window counts by frequency and
partition. Include zero-window/trailing counts, membership provenance,
preservation/resume evidence and exact researcher/manual-QA commands. Include
published commit IDs and verified Mac/GitHub/Ubuntu synchronisation status.
Distinguish implementation, validation and publication. Do not update the
spreadsheet.
