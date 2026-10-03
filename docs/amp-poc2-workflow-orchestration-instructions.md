# AMP Code Python file organisation instructions

Status: Approved by the researcher on 3 October 2026. Checkpoint the reviewed
Stage 2 implementation, then apply the standing file-naming standard without
changing behaviour. This is the current assignment, replacing the completed
Stage 2 implementation handoff. Earlier evidence remains in the
[acceptance record](poc2-workflow-orchestration-acceptance.md).

## Outcome and scope

Make the live Python source understandable by filename, parent process and
purpose, using the [approved standard](code-standards.md#python-utility-file-organisation).
Read [AGENTS.md](../AGENTS.md), the [working agreement](working-agreement.md)
and [execution policy](execution-policy.md). Follow the existing architecture.

This is an organisation-only change: preserve calculations, model settings,
fallbacks, inputs/outputs, identities, storage schema, retries, resource limits,
workflow order and public commands. Keep existing classes/functions intact.
Do not split large cross-gate modules, redesign objects, change dependencies,
add new wrappers/loaders, or resume unrelated migration work in this assignment.
Current scope is Python utilities and their references, not R file renaming.

## First publish the reviewed checkpoint

Commit and normal GitHub push of the reviewed Stage 2 source/tests/configuration
and approved documentation are explicitly authorised. Do this before renaming.

- Check current Mac/Ubuntu/remote state and active jobs. Verify the reviewed
  81-script / 31,200-line source against the preserved Stage 2 manifest; account
  for the approved documentation edits separately and preserve unrelated work.
- Stage explicit reviewed paths only; exclude databases, results, environments,
  secrets, caches and generated local evidence. Reuse the existing test evidence.
  Inspect the staged diff, check ancestry and push normally, without force push
  or destructive reset. Preserve any conflicting or additional work.
- Safely advance Ubuntu to that checkpoint and verify actual source identity,
  not only Git HEAD. Preserve an existing overlay before any potentially
  conflicting update; do not copy environments, databases or secrets.
- Report the checkpoint and verified revisions. Do not claim publication or
  two-host readiness if a host/remote is unavailable. Follow the availability
  policy; do not substitute heavy Mac-only work.

The previous Stage 1 checkpoint is already published as d032f20. Do not recreate
it or repeat Stage 1/2 acceptance campaigns. This new authorisation covers the
reviewed Stage 2 baseline and approved documentation, not unreviewed renamed code.

## Organise the same implementation

1. Record one concise old-to-new mapping in the existing acceptance record:
   filename, owner, purpose and active/inactive status. Trace imports plus setup,
   subprocess, configuration, dynamic/stored paths, tests and manual QA.
2. Rename existing process-owned utilities to `pNN_MM_current_descriptive_name.py`.
   Use `pNN_01_...` for the existing coordinating flow where one exists.
   Keep shared components under `shared_...`; retain meaningful names for
   independent workflows without an approved numeric parent. No artificial gate
   number for window preparation. Keep the current flat `src/python/util/`.
3. Update ordinary imports, runtime string references, shell/module launchers,
   source-manifest discovery, test patch targets and current documentation.
   Update affected headers with owner/purpose. Keep historical evidence intact.
   Do not extend the numbered-wrapper loader to these importable helpers.
4. Do not change scientific function/class names merely to match filenames.
   Grouping means naming the existing cohesive modules, not generating a file
   for each object or extracting mixed responsibilities during this pass.
5. Move only confirmed inactive code into tracked `tmp/inactive/`, preserving
   its relative path and contents. Follow the holding-area standard and create
   its short index only if something is moved. This is a pool for possible reuse,
   not deletion. Never park live setup, diagnostic, compatibility or requested QA
   code merely because no ordinary import was found. If none qualifies, say so.

Use these agreed examples; resolve the remaining mapping from actual ownership:

| Current utility | New name |
| --- | --- |
| import_flow.py | p01_01_import_flow.py |
| import_execution.py | p01_02_import_execution.py |
| forecast_flow.py | p04_01_forecast_flow.py |
| forecast_provider.py | p04_02_forecast_provider.py |
| forecast_storage.py | p04_03_forecast_storage.py |
| forecast_combination.py | p05_01_forecast_combination.py |
| configuration.py | shared_configuration.py |
| database.py | shared_database.py |

Numbers guide browsing, not execution. Keep `00_main.py`, numbered gate/worker
interfaces, Python special filenames and test-discovery names stable. Necessary
compatibility for a demonstrated stored/external reference may use a minimal
alias; do not create a duplicate facade for every renamed module.

## Preserve outcomes and verify proportionately

Renaming changes source/module paths and fingerprints, not scientific experiment
meaning. Preserve original configurations, source manifests, accepted data and
results. Record the new operational source identity; never rewrite a historical
hash, disable a mismatch guard or recompute accepted work to make a rename pass.
If an existing persistent reference requires more than a narrow compatible path
resolution, report that affected case before expanding scope.

Safely synchronise the rename set and matching references to Ubuntu, then use
fresh test-owned workers so old imported modules are not mistaken for new code.
Check import/discovery, unchanged public CLI actions, setup references and the
affected focused suites on both hosts. Include a small connected path and a
bounded distributed task to verify renamed-module serialisation/imports; reuse
fixtures and deterministic stubs where appropriate. No full scientific campaign,
new fault matrix, GPU rerun or model training is required solely for renaming.
Any heavy work still requires the approved two-host profile and safeguards.

Use disposable fixtures/copies for mutation checks. Confirm relevant result
values, identities, saved transformation state and restart behaviour are unchanged;
accepted research databases remain untouched. Do not present source hashes as
unchanged after renaming or reuse old evidence as if it tested new import paths.

## Script and line count comparison

Preserve prior manifests and comparisons. Add the current old-to-new mapping
and reconciled before/after counts using the same first-party R/Python physical
line scope. Report active source, tests/QA and inactive holdings separately,
plus total retained code. Moving code out of `src` is not deleting it or reducing
total retained lines. Explain any additions; no arbitrary reduction target.

## Handoff and review

Update the architecture/current process map and guidance with actual filenames.
Report the published baseline commit, both tested source identities, moved files,
inactive holdings or none, focused checks and any compatibility exceptions.
Keep the report concise; place detailed evidence in the existing acceptance record.
Do not claim Objective 2 complete or update the researcher's spreadsheet.

Return the organisation result for technical and researcher review before its
own commit/push. No permanent deletions, result transformations or additional
architecture refactor are authorised by this assignment.

## Approved Stage 1 closure follow-up

Historical reference only: see the preserved
[Stage 1 closure handoff](poc2-workflow-orchestration-acceptance.md#stage-1-closure-follow-up-handoff).
Do not restart that completed correction cycle.

## Approved Stage 1 correction pass

Historical reference only: see the
[correction evidence](poc2-workflow-orchestration-acceptance.md#stage-1-correction-handoff-3-october-2026).
Current approval and publication boundaries are stated above.
