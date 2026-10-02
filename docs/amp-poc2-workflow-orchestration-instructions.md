# AMP Code Stage 2 implementation instructions

Status: Approved by the researcher on 3 October 2026. Stage 1 is closed and
accepted for progression with documented limitations. Stage 2 and the scoped
GitHub checkpoint below are authorised. This document replaces the accumulated
Stage 1 handoffs; their evidence remains in the
[acceptance record](poc2-workflow-orchestration-acceptance.md).

## Outcome and authority

Prioritise completion of POC2 Objective 2 over further polishing of temporary
infrastructure. Retain the readable main and gate flows. Consolidate only the
workflows and already-approved previous-project methods that the research needs.

Read [AGENTS.md](../AGENTS.md), the [working agreement](working-agreement.md),
[pragmatic code standard](code-standards.md#pragmatic-implementation),
[workflow decision](poc2-workflow-orchestration-decision.md#pragmatic-stage-2-amendment),
[execution policy](execution-policy.md) and
[research vision](research-vision.md). Item-specific scientific decisions still
govern calculations, inputs, outputs, fallbacks and comparisons. Approval of
Stage 2 is not approval to invent missing scientific choices.

## First publish the accepted checkpoint

Git commit and normal push are explicitly authorised for the reviewed Stage 1
implementation, its source/tests/configuration/locks and approved documentation,
including this pragmatic amendment. Do this before Stage 2 source changes.

1. Inspect Mac, Ubuntu and the remote for active processes, divergence and
   unrelated changes. Verify the reviewed 81-script / 31,112-line snapshot
   against its preserved manifest; account for subsequent approved documentation
   changes separately. A dirty worktree is expected, not a reason to stop.
2. Stage explicit reviewed paths only. Inspect the index for unintended changes,
   secrets and generated artifacts; run whitespace and relevant lightweight
   checks. Preserve existing test evidence instead of rerunning model campaigns
   for publication. Exclude databases, results, environments, caches, local
   Prefect/Dask history and generated diagnostics.
3. Create a clear checkpoint commit and push normally to the configured GitHub
   remote after ancestry/conflict checks. No force push, destructive reset or
   discarding another person's work.
4. Safely synchronise Ubuntu to the published checkpoint. If it has an identical
   uncommitted overlay, verify its bytes and preserve it while advancing; if
   there are conflicting changes, stop only the affected sync and report them.
   Do not copy environments, databases or secrets between hosts.
5. Verify and report the Mac, Ubuntu and remote revisions plus actual source
   identity. If the remote or Ubuntu is unavailable, preserve the local work
   and report the unsynchronised state; do not claim publication or two-host
   readiness. Follow the standing availability policy for further testing.

This is publication authority for the accepted checkpoint, not blanket approval
to publish unrelated or future unreviewed Stage 2 changes. Continue Stage 2 once
the checkpoint is safely established; do not ask again for this approval.

## Then implement Stage 2 pragmatically

Start with a short plan in the existing acceptance record: retained workflow,
required approved methods, modules to consolidate/remove, and the focused test.
Do not create another planning framework or wait for approval of routine details.

- Migrate the remaining required Gates 1–6, tuning and window/S1 execution paths
  into the established Prefect/Dask pattern. Reuse existing scientific code and
  native objects. Do not refactor an obsolete script merely to preserve it.
- Complete the already-approved nine-method R pool integration through shared
  configuration, task planning, generic R execution, persistence and retrieval.
  Use [the forecast contracts](forecast-methods.md) and preserve explicit
  requested/executed model and fallback provenance. Address it with the retained
  forecasting workflow, rather than deferring it behind more platform polish.
- Continue the agreed component-by-component migration. The Objective 2 target
  remains selected previous-project reproduction, including the Mantis
  directional-accuracy work. Do not add unapproved training/split strategies,
  feature sets, datasets or scientific result targets to this orchestration task;
  identify any such missing decision precisely for the next item review.
- Consolidate utilities by coherent responsibility. Remove duplicate internal
  checks, forwarding layers and speculative branches where the controlled
  contract already establishes the invariant. Keep necessary validation at real
  input, provider, storage and restart boundaries. No new scheduler, contract
  framework or universal translation layer.
- Remove obsolete current-repository implementations after tracing production,
  stored-path, test and manual-QA callers and validating the replacement.
  Preserve genuinely used compatibility and requested QA. If external/manual
  use is uncertain, report that specific candidate rather than deleting it.
  Do not delete previous-project repositories, accepted data or historical
  evidence. Finish with an alignment review of the retained scripts against
  POC2 and the research vision.

Small reusable functions remain appropriate within cohesive objects. Keep main
and the gate workflows easy to read. Document true constants; do not turn every
fixed internal detail into a configuration option. Scientific and execution
settings still have their single authoritative configuration.

## Carry the remaining checks into Stage 2

These are no longer Stage 1 blockers or publication prerequisites:

| Remaining item | Stage 2 treatment |
| --- | --- |
| Native CPU work on both hosts | Verify with a suitable retained workload during its Stage 2 integration. Current ordinary AutoARIMA is Ubuntu-only under the large-fit resource rule; idle Mac workers do not constitute a pass. Preserve memory floors and fit budgets. |
| Scheduler-loss recovery | Add a small interruption/resume check to the retained normal execution path, preserving accepted rows. Existing post-commit recovery is valid evidence, but is not scheduler-loss evidence. |
| Utility duplication and excessive defensiveness | Simplify with the affected retained module; test the real supported contract, not hypothetical platforms or unsupported inputs. |

Keep these items visible in the existing acceptance record. If a path is
removed, explain which retained path now owns the requirement and its test.
Do not silently drop a still-relevant capability or relabel untested work as
passed. No extra standalone Stage 1 correction cycle is required.

## Proportionate verification and handoff

Test each changed contract and one bounded connected path where relevant.
Reuse accepted results and existing evidence. Group related corrections with
their Stage 2 workflow; do not repeat a full end-to-end or fault campaign for
every internal helper. Reserve full Objective 2 reproduction for the agreed
scientific acceptance point, not routine architecture checks.

Retain scientific correctness, boundary identity/shape/scale checks, fallback
visibility, single-writer transactions, restart integrity, source/version
traceability and approved resource limits. Safe two-machine source sync precedes
distributed tests. No silent heavy Mac-only fallback or intentional memory
exhaustion. Distinguish synthetic fixtures, real model work and research results.

Update the existing architecture, affected source contracts, researcher guidance
and acceptance record with the code. Report only material changes, evidence,
remaining research items and decisions genuinely needed. OOP and documentation
serve readable, maintainable research code; do not add files or elaborate
machinery merely to satisfy a structural checklist.

Return the bounded Stage 2 result for technical/researcher review. Do not claim
Objective 2 complete merely because orchestration is migrated. The researcher
maintains the spreadsheet.

## Script and line count comparison

Retain all four earlier manifests and the accepted 81-script / 31,112-line
snapshot. Use the same repository-wide first-party R/Python scope and physical
line counting as the acceptance record: include tests and manual QA, exclude
vendor/environments/data/results/caches/generated artifacts, count each file
once, including a final unterminated line. Record source hashes.

Update the full per-script before/after table and reconciled language/role
totals at the Stage 2 handoff, not after every small edit. Include unchanged,
added and removed scripts and explain merges. Compare with the original
67-script / 28,513-line baseline and the accepted checkpoint. Do not remove
necessary documentation/tests or hide code to obtain a lower count.

## Approved Stage 1 closure follow-up

Historical reference only. The 3 October closure implementation and its
verification limits are preserved in the
[Stage 1 closure handoff](poc2-workflow-orchestration-acceptance.md#stage-1-closure-follow-up-handoff).
The researcher has now closed that checkpoint; the Stage 2 instructions above
supersede the earlier stop and publication restrictions.

## Approved Stage 1 correction pass

Historical reference only. The original six findings and evidence remain in
the [correction handoff](poc2-workflow-orchestration-acceptance.md#stage-1-correction-handoff-3-october-2026).
Do not restart that completed correction cycle.
