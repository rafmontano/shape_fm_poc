# POC2 workflow orchestration acceptance

Status: Stage 1 closed and accepted for progression by the researcher on
3 October 2026, with known limitations carried into Stage 2. Stage 2 and the
reviewed Stage 1 GitHub checkpoint are authorised. Full migration and scientific
Objective 2 completion are not claimed.

## Researcher acceptance and pragmatic Stage 2 direction

The researcher has accepted the readable Stage 1 execution structure and directed
the team to stop the separate correction cycle, consolidate the remaining work
into Stage 2, and prioritise required previous-project methods and Objective 2.
The accepted source snapshot is 81 scripts / 31,112 lines; the
[closure handoff](#stage-1-closure-follow-up-handoff) identifies its manifests,
144 Mac/75 Ubuntu test evidence and bounded native forecast/recovery results.

Known limitations remain explicit:

- Native CPU forecast overlap across hosts was not demonstrated. Ordinary
  AutoARIMA currently uses Ubuntu-only large-fit resources; this is not merely
  temporary Mac memory pressure. Verify appropriate retained CPU work in Stage 2
  without weakening memory limits or presenting idle workers as computation.
- Current normal-route recovery evidence covers post-commit lost acknowledgement,
  not a fresh scheduler-loss test. Complete that bounded check with the retained
  Stage 2 path; do not rerun Stage 1 merely to close it separately.
- Overall code reduction and full workflow migration remain unfinished. Consolidate
  required utilities and remove obsolete code as replacements are validated.

These limitations do not block the accepted checkpoint or Stage 2. Acceptance
does not manufacture missing evidence or record an unperformed QA procedure.
Implementation follows the [current AMP instructions](amp-poc2-workflow-orchestration-instructions.md)
and [pragmatic architecture amendment](poc2-workflow-orchestration-decision.md#pragmatic-stage-2-amendment).
Commit and normal push of the reviewed checkpoint and approved documentation,
plus safe Ubuntu synchronisation, are explicitly authorised; publication is
pending until the implementer reports verified revisions.

The reports and previous approval restrictions below are retained as dated
history. This acceptance supersedes their Stage 1 pause/publication restrictions,
not their evidence limits. Add Stage 2 results separately; do not rewrite old
tests as broader successes.

## Initial implementation reported by AMP

`src/python/00_main.py` remains the only researcher entry point. Its `run`,
`prepare-windows`, and historical `test` writer routes now execute under Prefect.
Each executed Gate 1–6 child flow records three meaningful tasks: inspect the
authoritative input/prerequisite state, execute through the existing coordinator,
and validate the committed DuckDB output. The optional rolling-window/S1 flow
uses the same inspect/execute/validate handoff. `plan`, `status`, and `results`
remain direct read-only utilities.

Prefect 3.8.7 stores operational history in coordinator-local `/.prefect/`
SQLite and explicitly disables task caching and result persistence. Dask
2026.8.0 remains the compute scheduler. Existing R/Python functions and adapters
still calculate results, and only the Mac coordinator opens writable DuckDB.
The writer lock rejects overlapping workflows by resolved database path.
Prefect owns bounded gate retries; inner Dask retries are zero on Prefect-owned
paths. Scientific fallback, memory admission, child-process monitoring, Chronos
OOM splitting, and repeat-safe DuckDB commits remain ShapeFM responsibilities.

AutoARIMA remains implemented once in `src/r/util/forecast_methods.R`.
`src/r/04_01_forecast_auto_arima.R` is retained as a delegating compatibility
adapter for historical configurations.

## Initial coverage reported by AMP

| Capability | Evidence | Result |
| --- | --- | --- |
| Existing workflow coverage | Public `run` completed fresh Gates 1–3; all Gate 1–6 wrapper dispatch uses the same experiment/gate flow; historical acceptance and window/S1 entry routes are Prefect flows; read-only routes remain direct | Passed |
| Hierarchy and dependencies | Real Prefect parent/child runs contain named inspect/execute/validate tasks; permanent execution or validation failure blocks the downstream gate and parent success | Passed |
| Load balancing | Approved 8 Mac + 15 Ubuntu CPU cluster validated 23 workers; targeted tasks completed on both hosts with 2.959 s overlap; a resource-blocked future stayed pending while eligible work completed | Passed |
| CPU/GPU routing | Real Ubuntu AutoARIMA returned `[25, 26, 27]`; real pinned Chronos-2 ran on the RTX 5090 and returned one three-step probabilistic forecast; official GIFT-Eval semantic tests passed | Passed |
| Safety and preflight | Exact dirty-tree source manifest, topology and pinned Python/Dask/R versions passed on every CPU worker; stale source/profile/topology, resource pressure, and invalid settings have rejecting tests | Passed |
| Failure handling | Prefect transient fault retried only its gate; permanent and invalid-output faults failed visibly; Dask worker-loss/retry and partial-batch preservation tests passed | Passed |
| Restart and storage | Fresh Gates 1–3 accepted 100/200/400 tasks and restart skipped all three; scheduler interruption cancelled in-flight work and a fresh scheduler completed new work; coordinator/interrupted-invocation, partial-batch, window child-commit-before-parent-ack, and flow commit-before-ack recovery tests passed | Passed |
| Output validity | Post-gate DuckDB summaries are compared to returned handoffs; parent/child identities and row counts are validated; missing/mismatched output tests and second-writer refusal passed | Passed |
| Operational service | An unreachable Prefect API failed before the scientific runner started; removing the outage recovered successfully; durable local SQLite contains experiment, child-flow, task, success and failure history | Passed |
| Readability/integration | One 288-line integration module owns Prefect flows/tasks and writer locking; scientific modules and wrappers remain directly readable | Passed |
| Simplification evidence | Complete reproducible 69-script comparison below | Passed, with an explicitly reviewed line increase |

No full 100-series Gate 4–6 rerun or accepted 800-forecast seasonal-tuning rerun
was performed. That is intentional under the bounded-test policy: real R,
Chronos/GPU and official-evaluator boundaries were exercised separately while
existing contract tests covered combination, persistence, restart and recovery.

## Tested identity, machines, and evidence

- Source revision on both machines before testing:
  `b4da3fcda7e1bbc5bc412d9633d64bd5e8e35096`; GIFT-Eval submodule
  `4d5ab3fa0fe7451bbf59bb1ff6dd76e6e414d64a`.
- Synchronized runtime manifest during distributed evidence:
  `8d1a743ae8926f4d45da5a4bac87d467833c65dd9092e289ba9750f50842b383`
  across 74 runtime/config/lock files. After the final inspect/execute/validate
  refinement, both machines matched the final 74-file fingerprint
  `9f96e99be731117b478b94b5dac01079f3150a0d3d85781afd6441b8a2f379ef`.
- Both hosts: Python 3.12.14, Dask/distributed 2026.8.0, Prefect 3.8.7,
  prefect-dask 0.3.7 and DuckDB 1.5.5. Tuning workers also reported R 4.6.1,
  forecast 8.24.0, jsonlite 2.0.0 and tsfeatures 1.1.1.
- Actual contributors: `RMMacbookPro.local` (8 CPU workers) and `WSUbuntu1`
  (15 CPU workers); Chronos used `NVIDIA GeForce RTX 5090` with CUDA 13.0,
  torch 2.14.0+cu130 and chronos-forecasting 2.2.2.
- Distributed artifacts are under
  `.amp/in/artifacts/workflow-orchestration/`: `managed-cluster-evidence.json`,
  `ubuntu-autoarima-evidence.json`, `ubuntu-chronos-evidence.json`,
  `dask-scheduler-interruption-recovery.json`, and
  `prefect-outage-recovery.json`.
- Real entry artifacts are `gates-1-3-substeps.json` and
  `gates-1-3-substeps-restart.json`. No accepted database, result, model,
  environment, secret or cache was copied or changed.

The first broad Mac discovery run executed 187 tests: 186 passed and one
collection failed because a GIFT-Eval-only test was deliberately invoked from
the core environment. Rerunning it from its pinned environment passed both
tests; that environment separation is not an application failure. The broad
pass then completed 182 top-level Python tests, 8 separated integration tests,
all 3 R test scripts, 19 setup-verifier tests, and 70 focused Ubuntu tests. After
the final completed-output inspection refinement, its 34 changed-area tests
passed on both Mac and Ubuntu, and the real Gates 1–3 restart validated all
stored output before skipping. `git diff --check` and compilation also passed.

The full setup audit also reported pre-existing local installation gaps unrelated
to this migration: missing model-directory copies on Mac, and optional
Mantis/classifier/TensorFlow/R-superset folders on Ubuntu. The exact dependencies
used by this migration passed independent worker preflight, including Chronos
CUDA and its pinned checkpoint. A future complete `scripts/setup.sh verify`
requires restoring those optional local assets; this does not weaken the tested
workflow paths.

## Manual QA

1. Run `scripts/setup.sh verify`. If it reports the known optional local-asset
   gaps above, restore them through normal non-destructive setup; do not copy an
   environment between machines.
2. Create a fresh ignored database and run a bounded non-forecast route:

   ```sh
   .tools/uv/uv run --locked --no-sync python src/python/00_main.py run \
     --database .amp/in/manual-workflow.duckdb \
     --configuration config/experiments/poc2_m4_daily_100_standardised.json \
     --processes 1-3
   ```

3. Confirm the JSON contains a top-level `prefect_flow_run_id`; each completed
   process contains `prefect_gate_flow_run_id`, all three `prefect_substeps`, and
   `validation.output_validated: true`.
4. Repeat the command without `--configuration`. Confirm all three gates report
   `skipped_completed`, then inspect state with `00_main.py status`.
5. Inspect `/.prefect/prefect.db` locally or start Prefect's local UI if desired.
   It is operational history only; compare scientific completion against DuckDB.
6. For rolling-window QA, use the existing focused `prepare-windows` command and
   `src/r/qa/inspect_rolling_window.R` instructions in the README. Keep both
   databases read-only during manual inspection.

Do not use the historical full `test` command merely to inspect orchestration;
it is a real 100-series acceptance workload.

## Script-count method and reconciliation

Both inventories used Git-visible tracked plus nonignored untracked `.py`, `.R`
and `.r` files. They exclude `external`, `renv`, `data`, `results`, `.amp`,
`.cache`, every managed `.venv`, caches and generated artifacts. `awk NR` counts
physical lines, including comments, blanks and an unterminated final line.
`src/python/tests` and `src/r/tests` are automated tests, any `qa` directory is
manual QA, and all remaining scripts are production/support. Each path is
counted once, independent of machine.

Baseline: 67 scripts, 28,513 lines; manifest
`dd4c42cd634fc513d7668da25e8b618a9ba4aaf81affc51c313259d2ffbc0a22`,
captured before source edits at the revision above with the pre-existing
documentation worktree recorded in `baseline-identity.txt`.

Final: 69 scripts, 29,418 lines; manifest
`358385f86ea2412e1d81b301312c345c635ed6e50acb96e7f6426502c8e3f5f0`,
recorded in `final-identity.txt`. The detailed manifests are
`baseline-scripts.tsv` and `final-scripts.tsv` in the evidence directory.

| Language and role | Scripts before | Scripts after | Lines before | Lines after | Line change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Python — production/support | 29 | 30 | 16,930 | 17,483 | +553 |
| Python — automated test | 20 | 21 | 8,191 | 8,543 | +352 |
| Python subtotal | 49 | 51 | 25,121 | 26,026 | +905 |
| R — production/support | 11 | 11 | 1,801 | 1,801 | 0 |
| R — automated test | 3 | 3 | 655 | 655 | 0 |
| R — manual QA | 4 | 4 | 936 | 936 | 0 |
| R subtotal | 18 | 18 | 3,392 | 3,392 | 0 |
| Production/support subtotal | 40 | 41 | 18,731 | 19,284 | +553 |
| Automated-test subtotal | 23 | 24 | 8,846 | 9,198 | +352 |
| Manual-QA subtotal | 4 | 4 | 936 | 936 | 0 |
| **All scripts** | **67** | **69** | **28,513** | **29,418** | **+905** |

| Script before | Script after | Language | Role | Lines before | Lines after | Change | Status or reason |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| `scripts/setup_r.R` | `scripts/setup_r.R` | R | production/support | 150 | 150 | +0 | Unchanged |
| `scripts/setup_support.py` | `scripts/setup_support.py` | Python | production/support | 706 | 710 | +4 | Changed — verify pinned Prefect dependencies |
| `src/python/00_main.py` | `src/python/00_main.py` | Python | production/support | 763 | 1013 | +250 | Changed — Prefect routing and completed-output validation |
| `src/python/01_import.py` | `src/python/01_import.py` | Python | production/support | 31 | 31 | +0 | Unchanged |
| `src/python/02_preprocess.py` | `src/python/02_preprocess.py` | Python | production/support | 32 | 32 | +0 | Unchanged |
| `src/python/03_transform.py` | `src/python/03_transform.py` | Python | production/support | 32 | 32 | +0 | Unchanged |
| `src/python/04_02_forecast_chronos.py` | `src/python/04_02_forecast_chronos.py` | Python | production/support | 281 | 281 | +0 | Unchanged |
| `src/python/04_03_forecast_m4_submission.py` | `src/python/04_03_forecast_m4_submission.py` | Python | production/support | 29 | 29 | +0 | Unchanged |
| `src/python/04_forecast.py` | `src/python/04_forecast.py` | Python | production/support | 43 | 43 | +0 | Unchanged |
| `src/python/05_combine.py` | `src/python/05_combine.py` | Python | production/support | 32 | 32 | +0 | Unchanged |
| `src/python/06_01_evaluate_gift_eval.py` | `src/python/06_01_evaluate_gift_eval.py` | Python | production/support | 462 | 462 | +0 | Unchanged |
| `src/python/06_evaluate.py` | `src/python/06_evaluate.py` | Python | production/support | 34 | 34 | +0 | Unchanged |
| `src/python/tests/__init__.py` | `src/python/tests/__init__.py` | Python | automated test | 10 | 10 | +0 | Unchanged |
| `src/python/tests/acceptance.py` | `src/python/tests/acceptance.py` | Python | automated test | 1784 | 1871 | +87 | Changed — route acceptance gates through Prefect |
| `src/python/tests/integration/__init__.py` | `src/python/tests/integration/__init__.py` | Python | automated test | 10 | 10 | +0 | Unchanged |
| `src/python/tests/integration/test_gift_eval_evaluation.py` | `src/python/tests/integration/test_gift_eval_evaluation.py` | Python | automated test | 189 | 189 | +0 | Unchanged |
| `src/python/tests/integration/test_gift_eval_semantics.py` | `src/python/tests/integration/test_gift_eval_semantics.py` | Python | automated test | 98 | 98 | +0 | Unchanged |
| `src/python/tests/integration/test_import_pipeline.py` | `src/python/tests/integration/test_import_pipeline.py` | Python | automated test | 151 | 151 | +0 | Unchanged |
| `src/python/tests/integration/test_preprocessing_regression.py` | `src/python/tests/integration/test_preprocessing_regression.py` | Python | automated test | 72 | 72 | +0 | Unchanged |
| `src/python/tests/test_acceptance.py` | `src/python/tests/test_acceptance.py` | Python | automated test | 457 | 457 | +0 | Unchanged |
| `src/python/tests/test_configuration.py` | `src/python/tests/test_configuration.py` | Python | automated test | 297 | 297 | +0 | Unchanged |
| `src/python/tests/test_execution.py` | `src/python/tests/test_execution.py` | Python | automated test | 893 | 893 | +0 | Unchanged |
| `src/python/tests/test_experiment_execution.py` | `src/python/tests/test_experiment_execution.py` | Python | automated test | 972 | 972 | +0 | Unchanged |
| `src/python/tests/test_gpu_concurrency_calibration.py` | `src/python/tests/test_gpu_concurrency_calibration.py` | Python | automated test | 197 | 197 | +0 | Unchanged |
| `src/python/tests/test_import.py` | `src/python/tests/test_import.py` | Python | automated test | 400 | 400 | +0 | Unchanged |
| `src/python/tests/test_m4_reference.py` | `src/python/tests/test_m4_reference.py` | Python | automated test | 354 | 354 | +0 | Unchanged |
| `src/python/tests/test_main.py` | `src/python/tests/test_main.py` | Python | automated test | 757 | 790 | +33 | Changed — internal-path and missing-output coverage |
| `src/python/tests/test_preprocessing.py` | `src/python/tests/test_preprocessing.py` | Python | automated test | 109 | 109 | +0 | Unchanged |
| `src/python/tests/test_seasonal_period_tuning.py` | `src/python/tests/test_seasonal_period_tuning.py` | Python | automated test | 124 | 124 | +0 | Unchanged |
| `src/python/tests/test_setup.py` | `src/python/tests/test_setup.py` | Python | automated test | 394 | 394 | +0 | Unchanged |
| `src/python/tests/test_transformations.py` | `src/python/tests/test_transformations.py` | Python | automated test | 258 | 258 | +0 | Unchanged |
| `src/python/tests/test_window_preparation.py` | `src/python/tests/test_window_preparation.py` | Python | automated test | 665 | 665 | +0 | Unchanged |
| `src/python/tests/test_workflow_orchestration.py` | `src/python/tests/test_workflow_orchestration.py` | Python | automated test | 0 | 232 | +232 | Added — orchestration/retry/recovery tests |
| `src/python/util/__init__.py` | `src/python/util/__init__.py` | Python | production/support | 10 | 10 | +0 | Unchanged |
| `src/python/util/configuration.py` | `src/python/util/configuration.py` | Python | production/support | 1043 | 1043 | +0 | Unchanged |
| `src/python/util/database.py` | `src/python/util/database.py` | Python | production/support | 962 | 962 | +0 | Unchanged |
| `src/python/util/distributed_cluster.py` | `src/python/util/distributed_cluster.py` | Python | production/support | 343 | 343 | +0 | Unchanged |
| `src/python/util/distributed_execution.py` | `src/python/util/distributed_execution.py` | Python | production/support | 1288 | 1288 | +0 | Unchanged |
| `src/python/util/execution_calibration.py` | `src/python/util/execution_calibration.py` | Python | production/support | 1141 | 1141 | +0 | Unchanged |
| `src/python/util/execution_profiles.py` | `src/python/util/execution_profiles.py` | Python | production/support | 564 | 564 | +0 | Unchanged |
| `src/python/util/experiment_execution.py` | `src/python/util/experiment_execution.py` | Python | production/support | 3126 | 3126 | +0 | Unchanged |
| `src/python/util/forecast_combination.py` | `src/python/util/forecast_combination.py` | Python | production/support | 52 | 52 | +0 | Unchanged |
| `src/python/util/gift_eval_acquisition.py` | `src/python/util/gift_eval_acquisition.py` | Python | production/support | 294 | 294 | +0 | Unchanged |
| `src/python/util/gift_eval_source.py` | `src/python/util/gift_eval_source.py` | Python | production/support | 150 | 150 | +0 | Unchanged |
| `src/python/util/gpu_concurrency_calibration.py` | `src/python/util/gpu_concurrency_calibration.py` | Python | production/support | 1386 | 1386 | +0 | Unchanged |
| `src/python/util/import_execution.py` | `src/python/util/import_execution.py` | Python | production/support | 986 | 986 | +0 | Unchanged |
| `src/python/util/m4_submission.py` | `src/python/util/m4_submission.py` | Python | production/support | 121 | 121 | +0 | Unchanged |
| `src/python/util/provenance.py` | `src/python/util/provenance.py` | Python | production/support | 55 | 55 | +0 | Unchanged |
| `src/python/util/seasonal_period_tuning.py` | `src/python/util/seasonal_period_tuning.py` | Python | production/support | 1614 | 1614 | +0 | Unchanged |
| `src/python/util/transformations.py` | `src/python/util/transformations.py` | Python | production/support | 263 | 263 | +0 | Unchanged |
| `src/python/util/window_preparation.py` | `src/python/util/window_preparation.py` | Python | production/support | 1087 | 1098 | +11 | Changed — transfer retry ownership to Prefect |
| `src/python/util/workflow_orchestration.py` | `src/python/util/workflow_orchestration.py` | Python | production/support | 0 | 288 | +288 | Added — Prefect integration and writer lock |
| `src/r/01_02_import_m4comp2018.R` | `src/r/01_02_import_m4comp2018.R` | R | production/support | 209 | 209 | +0 | Unchanged |
| `src/r/02_01_preprocess_series.R` | `src/r/02_01_preprocess_series.R` | R | production/support | 81 | 81 | +0 | Unchanged |
| `src/r/04_01_forecast_auto_arima.R` | `src/r/04_01_forecast_auto_arima.R` | R | production/support | 86 | 86 | +0 | Unchanged |
| `src/r/04_01_forecast_r_methods.R` | `src/r/04_01_forecast_r_methods.R` | R | production/support | 103 | 103 | +0 | Unchanged |
| `src/r/qa/get_m4_daily_series.R` | `src/r/qa/get_m4_daily_series.R` | R | manual QA | 300 | 300 | +0 | Unchanged |
| `src/r/qa/inspect_rolling_window.R` | `src/r/qa/inspect_rolling_window.R` | R | manual QA | 277 | 277 | +0 | Unchanged |
| `src/r/qa/inspect_seasonal_period_tuning.R` | `src/r/qa/inspect_seasonal_period_tuning.R` | R | manual QA | 149 | 149 | +0 | Unchanged |
| `src/r/qa/inspect_standardisation.R` | `src/r/qa/inspect_standardisation.R` | R | manual QA | 210 | 210 | +0 | Unchanged |
| `src/r/tests/test_forecast_methods.R` | `src/r/tests/test_forecast_methods.R` | R | automated test | 440 | 440 | +0 | Unchanged |
| `src/r/tests/test_import_m4comp2018.R` | `src/r/tests/test_import_m4comp2018.R` | R | automated test | 97 | 97 | +0 | Unchanged |
| `src/r/tests/test_transformations.R` | `src/r/tests/test_transformations.R` | R | automated test | 118 | 118 | +0 | Unchanged |
| `src/r/util/forecast_methods.R` | `src/r/util/forecast_methods.R` | R | production/support | 797 | 797 | +0 | Unchanged |
| `src/r/util/labels.R` | `src/r/util/labels.R` | R | production/support | 21 | 21 | +0 | Unchanged |
| `src/r/util/seasonal_period.R` | `src/r/util/seasonal_period.R` | R | production/support | 79 | 79 | +0 | Unchanged |
| `src/r/util/time_series_input.R` | `src/r/util/time_series_input.R` | R | production/support | 57 | 57 | +0 | Unchanged |
| `src/r/util/transformations.R` | `src/r/util/transformations.R` | R | production/support | 188 | 188 | +0 | Unchanged |
| `src/r/util/window_preparation.R` | `src/r/util/window_preparation.R` | R | production/support | 30 | 30 | +0 | Unchanged |

The total increased by 905 lines rather than decreasing. Exactly 553 lines are
production/support integration and 352 are automated tests. The production
increase is the explicit Prefect boundary (288), public-entry inspection,
completed-output validation, identity and writer integration (250), retry
ownership transfer (11), and setup verification (4). Tests add orchestration
hierarchy, retry, missing-output, writer and commit-before-ack recovery coverage
plus acceptance routing.
No R science, documentation, tests or safeguards were removed or moved to hide
complexity.

The removed complexity is behavioural rather than a net line reduction: the
entry point's custom gate loop and active-gate failure bookkeeping were replaced
by Prefect parent/child state, task retries and operational history; Prefect-owned
paths no longer stack Dask retries. Retained custom code is domain-specific:
scientific validation/fallback, external-process timeout/termination, memory
admission, GPU routing, repeat-safe DuckDB transactions and parent/child storage
recovery. The line increase is the transparent initial integration cost and
should be reviewed as a trade-off; it is not presented as code-volume
simplification.

## Initial handoff claim, retained for history

Initially reported by AMP as accepted (not researcher acceptance): migration
code, pinned dependencies, Gates 1–6 and window/acceptance
routing, real Gates 1–3 storage path, two-host CPU work, real Ubuntu R and GPU
adapters, official evaluator semantics, bounded retry/failure/recovery, writer
exclusion and count reconciliation.

Retried by design: one injected transient Prefect gate and Dask worker-loss test.
Failed visibly by design: permanent child failure, stored-output mismatch,
unreachable Prefect API and interrupted Dask scheduler. Each had a successful
bounded recovery path.

Pending operational housekeeping: restore optional local setup assets if a full
superset audit is required. Pending delivery: Git commit and push require
separate scoped researcher approval. The spreadsheet was not changed.

## Stage 1 implementation review snapshot — 2 October 2026

The approved Stage 1 refactor is implemented locally for review, but is **not
accepted or ready for unrestricted production use**. Required corrections and
verification gaps below remain part of Stage 1, not permission to start Stage 2.
No commit or push was made. Existing accepted research data and environments
were preserved. The historical claims above are not evidence that these gaps
have been closed.

### Implemented responsibilities and contracts

The current [architecture responsibility map](architecture.md#stage-1-working-implementation-boundaries)
identifies the objects, small operations, configuration authority and tests.
Review the actual source together with this record:

- `00_main.py` obtains a `ResearcherRequest`, dispatches through
  `ResearcherActions` and presents output/errors through `ResearcherCLI`.
  Parsing, invocation provenance and process/event SQL no longer live in main.
  Help and read-only actions do not start Prefect or Dask.
- `gate1_import_flow` loads `ExperimentConfiguration` from DuckDB, constructs
  `ConfiguredGiftEvalSource`, streams bounded records, computes named series
  tasks, commits locally and verifies source/membership before accepting the run.
  The callback-to-legacy-loop route was removed. No repeated JSON resolution is
  used on resume. Raw Arrow remains necessary for source hashes and null/NaN
  distinction; the official reader is also lazy but converts that validity data.
- `ordinary_forecast_flow` prepares pending jobs through `ForecastStorage`,
  submits named AutoARIMA/Chronos tasks, validates identities/shapes/finite values,
  restores original scale and commits on the Mac. Dask uses an explicitly supplied
  existing scheduler with CPU/GPU resource annotations and bounded in-flight
  groups. Local CPU submissions also honor configured concurrency and overlap.
- Flow parameters contain paths, identities, settings and job data, not storage
  handles. This matters because prefect-dask serializes parent parameters in
  task context. An initial distributed serialization failure exposed this and
  was fixed before the successful runs; its evidence remains preserved.
- Ordinary Gate 4 application retries belong to Prefect compute tasks; Dask
  retries and the enclosing Gate 4 retry are zero. The task count tests prove
  zero retries means one attempt and two retries means three attempts. Existing
  scientific fallback remains inside the native R adapter.
- Expected Gate 4 identities come from experiment instances, variants and
  configured models, rather than surviving task/result counts. Missing both a
  task and its result is rejected. Failed fresh/predecessor validation leaves
  the process failed, not reusable as completed. Stored forecasts are checked
  for shape, finite values, original scale, lineage and content hash.

The old ordinary Gate 4 dispatch body was removed rather than relocated intact.
The generic gate wrapper, direct historical `import_m4_daily`, tuning dispatch,
other scientific gates, and window/S1 internals remain explicit Stage 2 paths.
The delegating AutoARIMA R adapter remains unchanged. Native process timeouts,
local Chronos OOM subdivision, transaction rollback and writer locking remain
domain/safety code, not a second application retry controller. No scientific
logic was moved to configuration or another language to reduce line counts.

### Bounded evidence and actual delivery state

Evidence is under `.amp/in/artifacts/workflow-stage1/`. These files are local
review artifacts, not published results. All diagnostic fixtures are isolated;
no production model settings or dataset selection contract was weakened.

| Check | Observed evidence | Limit |
| --- | --- | --- |
| Combined contract regression | `final-contract-tests.log`: 88 tests passed | Precedes the final local concurrency/provider refinement |
| Final regression | `final-90-tests.log`: all 90 tests passed; `final-forecast-regression.log` and `ubuntu-final-forecast-tests.log`: 27 forecasting/regression tests passed on each host | The preceding v2 run found two stale test hooks; corrected provider-boundary fault injection retained the assertions |
| Real configuration → import → storage | `import-final.json`, `import-final-resume.json`: 100 approved series, 57,235 observations; resume without creation JSON skips completed Gate 1 | Import-only workload, not a 100-series forecasting run |
| Real two-host CPU forecasts | `distributed-forecast.json`: two R jobs, context 24, horizon 2, in-flight 2, retries 0; one job on each host, 1.497-second overlap | Small eligible work, not memory-pressure acceptance |
| Real R plus Chronos/GPU flow | `distributed-forecast-gpu.json` and final log: two R and two pinned Chronos forecasts reach DuckDB through Prefect/Dask | Two logical GPU tasks, not saturation of 15 slots |
| Same-experiment scheduler recovery | `same-experiment-scheduler-resume.json`: scheduler stopped after first real commit; flow exited 1; restarted scheduler completed the remaining task; second resume did no work | Two stub forecasts on one explicitly bounded local worker; complements, not replaces, real two-host tests |
| Commit before acknowledgement | `test_forecast_flow`: first accepted row/hash/timestamp unchanged; resume invokes only outstanding identity | Synthetic provider, real Prefect/storage/retrieval |
| Local task concurrency and serialization | `test_forecast_flow`: two-task barrier and provider pickle round-trip | Bounded CPU probe; no heavy local substitution |

The same-experiment scheduler diagnostic preserved the first forecast ID,
content hash and creation timestamp exactly. Its failure propagated as Dask
`scheduler-connection-lost`, rather than success from an unrelated new task.
Diagnostic functions are a bounded practical exception: one fixture and one
owned service lifetime do not justify a reusable object framework.

Source was conflict-checked against the previously synchronized Ubuntu hashes
before copying runtime files. Mac and Ubuntu lock files and source manifests
matched; environments/databases/results were not copied. Both hosts use Python
3.12.14, Prefect 3.8.7, prefect-dask 0.3.7, Dask/distributed 2026.8.0 and DuckDB
1.5.5. GIFT-Eval remains pinned to `4d5ab3fa0fe7451bbf59bb1ff6dd76e6e414d64a`.
Worker preflight records native package versions, host resources and source
identity in the distributed JSON. The final real GPU run used runtime manifest
`f7bfab5e1b8a12f90e6a9532d34a9279685300ebf8a33b30227888f5b46409c5`.
Subsequent changes to regression injection code are test-only and separately
identified by the final script/runtime manifests. The final review runtime
fingerprint is `4e9990a7c59905f4f50c8aa9d9568b229263235774d06d2dabb958de55ad6284`
on both hosts (`mac-review-runtime.json`, `ubuntu-review-runtime.json`).
Compilation and `git diff --check` passed. Test-owned services and workers were
stopped after their runs.

Reproduction commands (from repository root; diagnostics are retained under
`.amp/in/` and are not public researcher entry points):

```sh
PYTHONPATH=src/python .venv/bin/python -m unittest \
  tests.test_main tests.test_process_storage tests.test_configuration \
  tests.test_import tests.test_experiment_execution \
  tests.test_workflow_orchestration tests.test_forecast_flow
PYTHONPATH=src/python .venv/bin/python .amp/in/stage1_scheduler_resume.py
STAGE1_GPU=1 PYTHONPATH=src/python .venv/bin/python .amp/in/stage1_distributed.py
.venv/bin/python .amp/in/stage1_inventory.py
```

### Required corrections and remaining verification

1. Ordinary distributed AutoARIMA still calls the existing `autoarima_batch`
   adapter without the continuous R child-memory reservation/monitor used by
   the tuning path. The small successful runs do not prove host-floor safety
   for large fits. Wire and test the approved safety policy before heavy use.
2. Distributed Chronos currently propagates OOM into the bounded Prefect retry;
   it does not yet reproduce the local provider's adaptive OOM subdivision.
   Shared-GPU floor/pressure admission and contention still need explicit
   migrated-path evidence. The local provider now starts/closes its persistent
   bridge per Prefect batch; retain a reusable bridge lifetime if profiling
   confirms material startup overhead, without passing live handles to tasks.
3. Gate 1 process-level skip validation and Gates 2/3 predecessor validation
   still lack complete independent value/hash/lineage validation (the import
   execution itself verifies source membership and accepted raw rows). Counts
   and joins alone do not close every corruption case.
4. Retry tests cover the migrated task budget and enclosing retry suppression,
   but a real stored-default-versus-explicit-profile override comparison and
   sequential numerical comparison with declared tolerances remain outstanding.
5. Full worker/coordinator/Prefect-service interruption combinations and the
   connected tuning/window/S1/combination/evaluation coverage belong to the
   remaining acceptance matrix. Historical adapter evidence is not a substitute
   for those actual migrated workflows. Complete object-contract documentation
   and review method size/readability before declaring OOP compliance.

These are required follow-up items, not implicit exceptions to safeguards or
researcher acceptance. At this snapshot, both reviews were pending. The
subsequent technical review and correction approval are recorded below;
Stage 2 has not begun.

### Technical review and correction approval — recorded 3 October 2026

ChatGPT checked the 80-file source manifest and saved test evidence and
recommended retaining the implementation while keeping Stage 1 open. The
researcher approved the bounded correction pass, not implementation acceptance.
The authoritative assignment is the
[approved Stage 1 correction pass](amp-poc2-workflow-orchestration-instructions.md#approved-stage-1-correction-pass).

In addition to the gaps above, review identified the ordinary preflight's
clean-worktree requirement (bypassed by the direct-flow diagnostic), incorrect
stored-versus-effective retry selection, and fixed-wave dispatch that can block
eligible work. The retry probe resolved zero retries but passed the stored
value of two to the task. A consistent provider contract, public import-storage
operations and complete affected-file/method documentation are also required.
See the linked assignment for correction scope and acceptance checks.

No correction implementation or new runtime verification is claimed by this
approval record. Preserve the dated evidence and inventory below; add separately
identified correction results rather than replacing them. Stage 1 remains open
for technical re-review and the researcher's independent QA. Stage 2 and Git
publication remain unauthorised by this correction approval.

### Script comparison method and interpretation

The original 67-script / 28,513-line manifest and reviewed 69-script /
29,418-line manifest remain distinct. `before-scripts.tsv` matched the reviewed
snapshot exactly before this refactor; its SHA-256 is
`358385f86ea2412e1d81b301312c345c635ed6e50acb96e7f6426502c8e3f5f0`.
The inventory command above uses `git ls-files --cached --others
--exclude-standard -z`, de-duplicates paths, excludes third-party/submodule,
environment, cache, data/result and `.amp` trees, and counts every included
`.py`/`.R`/`.r` physical line, including final unterminated lines. It preserves
the original language/role rules. SHA-256 of each actual file is in
`final-scripts.tsv`; HEAD is not represented as the dirty source identity.

Final inventory: **80 scripts / 29,670 lines**, SHA-256
`0189f8108f7ab697f763f7cfc52504a3a9c2d1dae01a8c95e7eb0751e42167b8`.
This is **+252 lines** against the reviewed partial tree and **+1,157 lines**
against the original pre-migration tree. Production/support is 19,590 lines,
automated tests 9,144 and manual QA 936. `00_main.py` is 35 lines versus 1,013
in the reviewed partial tree and 763 in the original baseline; the moved
responsibilities are fully counted in the new modules below.

There are no file renames. Main's responsibilities were split across request,
CLI, action and process/event storage objects; forecasting is split across flow,
provider and storage. These are many-to-many responsibility moves, not claimed
line deletions. New validation and tests offset removal of ordinary dispatch.
CLI test setup/scenarios were consolidated; compare the retained assertions,
not just the reduced file size. The total still grows against both baselines;
this is a review trade-off, not the requested overall line-reduction outcome.

The following generated tables include every unchanged file and reconcile
production/support, automated-test, manual-QA and language subtotals against
both preserved manifests.
### Stage 1 compared with the original pre-migration snapshot

| Script before | Script after | Language | Role | Lines before | Lines after | Change | Status or reason |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| scripts/setup_r.R | scripts/setup_r.R | R | production/support | 150 | 150 | +0 | Unchanged |
| scripts/setup_support.py | scripts/setup_support.py | Python | production/support | 706 | 710 | +4 | Changed; see responsibility map |
| src/python/00_main.py | src/python/00_main.py | Python | production/support | 763 | 35 | -728 | Changed; see responsibility map |
| src/python/01_import.py | src/python/01_import.py | Python | production/support | 31 | 30 | -1 | Changed; see responsibility map |
| src/python/02_preprocess.py | src/python/02_preprocess.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/03_transform.py | src/python/03_transform.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/04_02_forecast_chronos.py | src/python/04_02_forecast_chronos.py | Python | production/support | 281 | 281 | +0 | Unchanged |
| src/python/04_03_forecast_m4_submission.py | src/python/04_03_forecast_m4_submission.py | Python | production/support | 29 | 29 | +0 | Unchanged |
| src/python/04_forecast.py | src/python/04_forecast.py | Python | production/support | 43 | 43 | +0 | Unchanged |
| src/python/05_combine.py | src/python/05_combine.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/06_01_evaluate_gift_eval.py | src/python/06_01_evaluate_gift_eval.py | Python | production/support | 462 | 462 | +0 | Unchanged |
| src/python/06_evaluate.py | src/python/06_evaluate.py | Python | production/support | 34 | 34 | +0 | Unchanged |
| src/python/tests/__init__.py | src/python/tests/__init__.py | Python | automated test | 10 | 10 | +0 | Unchanged |
| src/python/tests/acceptance.py | src/python/tests/acceptance.py | Python | automated test | 1784 | 1871 | +87 | Changed; see responsibility map |
| src/python/tests/integration/__init__.py | src/python/tests/integration/__init__.py | Python | automated test | 10 | 10 | +0 | Unchanged |
| src/python/tests/integration/test_gift_eval_evaluation.py | src/python/tests/integration/test_gift_eval_evaluation.py | Python | automated test | 189 | 189 | +0 | Unchanged |
| src/python/tests/integration/test_gift_eval_semantics.py | src/python/tests/integration/test_gift_eval_semantics.py | Python | automated test | 98 | 98 | +0 | Unchanged |
| src/python/tests/integration/test_import_pipeline.py | src/python/tests/integration/test_import_pipeline.py | Python | automated test | 151 | 151 | +0 | Unchanged |
| src/python/tests/integration/test_preprocessing_regression.py | src/python/tests/integration/test_preprocessing_regression.py | Python | automated test | 72 | 72 | +0 | Unchanged |
| src/python/tests/test_acceptance.py | src/python/tests/test_acceptance.py | Python | automated test | 457 | 457 | +0 | Unchanged |
| src/python/tests/test_configuration.py | src/python/tests/test_configuration.py | Python | automated test | 297 | 297 | +0 | Unchanged |
| src/python/tests/test_execution.py | src/python/tests/test_execution.py | Python | automated test | 893 | 893 | +0 | Unchanged |
| src/python/tests/test_experiment_execution.py | src/python/tests/test_experiment_execution.py | Python | automated test | 972 | 985 | +13 | Changed; see responsibility map |
| — | src/python/tests/test_forecast_flow.py | Python | automated test | 0 | 197 | +197 | Added; responsibility split or contract test |
| src/python/tests/test_gpu_concurrency_calibration.py | src/python/tests/test_gpu_concurrency_calibration.py | Python | automated test | 197 | 197 | +0 | Unchanged |
| src/python/tests/test_import.py | src/python/tests/test_import.py | Python | automated test | 400 | 429 | +29 | Changed; see responsibility map |
| src/python/tests/test_m4_reference.py | src/python/tests/test_m4_reference.py | Python | automated test | 354 | 354 | +0 | Unchanged |
| src/python/tests/test_main.py | src/python/tests/test_main.py | Python | automated test | 757 | 437 | -320 | Changed; see responsibility map |
| src/python/tests/test_preprocessing.py | src/python/tests/test_preprocessing.py | Python | automated test | 109 | 109 | +0 | Unchanged |
| — | src/python/tests/test_process_storage.py | Python | automated test | 0 | 60 | +60 | Added; responsibility split or contract test |
| src/python/tests/test_seasonal_period_tuning.py | src/python/tests/test_seasonal_period_tuning.py | Python | automated test | 124 | 124 | +0 | Unchanged |
| src/python/tests/test_setup.py | src/python/tests/test_setup.py | Python | automated test | 394 | 394 | +0 | Unchanged |
| src/python/tests/test_transformations.py | src/python/tests/test_transformations.py | Python | automated test | 258 | 258 | +0 | Unchanged |
| src/python/tests/test_window_preparation.py | src/python/tests/test_window_preparation.py | Python | automated test | 665 | 665 | +0 | Unchanged |
| — | src/python/tests/test_workflow_orchestration.py | Python | automated test | 0 | 232 | +232 | Added; responsibility split or contract test |
| src/python/util/__init__.py | src/python/util/__init__.py | Python | production/support | 10 | 10 | +0 | Unchanged |
| src/python/util/configuration.py | src/python/util/configuration.py | Python | production/support | 1043 | 1043 | +0 | Unchanged |
| src/python/util/database.py | src/python/util/database.py | Python | production/support | 962 | 962 | +0 | Unchanged |
| src/python/util/distributed_cluster.py | src/python/util/distributed_cluster.py | Python | production/support | 343 | 343 | +0 | Unchanged |
| src/python/util/distributed_execution.py | src/python/util/distributed_execution.py | Python | production/support | 1288 | 1288 | +0 | Unchanged |
| src/python/util/execution_calibration.py | src/python/util/execution_calibration.py | Python | production/support | 1141 | 1141 | +0 | Unchanged |
| — | src/python/util/execution_event_storage.py | Python | production/support | 0 | 125 | +125 | Added; responsibility split or contract test |
| src/python/util/execution_profiles.py | src/python/util/execution_profiles.py | Python | production/support | 564 | 564 | +0 | Unchanged |
| src/python/util/experiment_execution.py | src/python/util/experiment_execution.py | Python | production/support | 3126 | 2756 | -370 | Changed; see responsibility map |
| src/python/util/forecast_combination.py | src/python/util/forecast_combination.py | Python | production/support | 52 | 52 | +0 | Unchanged |
| — | src/python/util/forecast_flow.py | Python | production/support | 0 | 120 | +120 | Added; responsibility split or contract test |
| — | src/python/util/forecast_provider.py | Python | production/support | 0 | 209 | +209 | Added; responsibility split or contract test |
| — | src/python/util/forecast_storage.py | Python | production/support | 0 | 147 | +147 | Added; responsibility split or contract test |
| src/python/util/gift_eval_acquisition.py | src/python/util/gift_eval_acquisition.py | Python | production/support | 294 | 294 | +0 | Unchanged |
| src/python/util/gift_eval_source.py | src/python/util/gift_eval_source.py | Python | production/support | 150 | 209 | +59 | Changed; see responsibility map |
| src/python/util/gpu_concurrency_calibration.py | src/python/util/gpu_concurrency_calibration.py | Python | production/support | 1386 | 1386 | +0 | Unchanged |
| src/python/util/import_execution.py | src/python/util/import_execution.py | Python | production/support | 986 | 1097 | +111 | Changed; see responsibility map |
| — | src/python/util/import_flow.py | Python | production/support | 0 | 52 | +52 | Added; responsibility split or contract test |
| src/python/util/m4_submission.py | src/python/util/m4_submission.py | Python | production/support | 121 | 121 | +0 | Unchanged |
| — | src/python/util/process_storage.py | Python | production/support | 0 | 151 | +151 | Added; responsibility split or contract test |
| src/python/util/provenance.py | src/python/util/provenance.py | Python | production/support | 55 | 55 | +0 | Unchanged |
| — | src/python/util/researcher_actions.py | Python | production/support | 0 | 509 | +509 | Added; responsibility split or contract test |
| — | src/python/util/researcher_cli.py | Python | production/support | 0 | 148 | +148 | Added; responsibility split or contract test |
| — | src/python/util/researcher_request.py | Python | production/support | 0 | 24 | +24 | Added; responsibility split or contract test |
| src/python/util/seasonal_period_tuning.py | src/python/util/seasonal_period_tuning.py | Python | production/support | 1614 | 1614 | +0 | Unchanged |
| src/python/util/transformations.py | src/python/util/transformations.py | Python | production/support | 263 | 263 | +0 | Unchanged |
| src/python/util/window_preparation.py | src/python/util/window_preparation.py | Python | production/support | 1087 | 1098 | +11 | Changed; see responsibility map |
| — | src/python/util/workflow_orchestration.py | Python | production/support | 0 | 288 | +288 | Added; responsibility split or contract test |
| src/r/01_02_import_m4comp2018.R | src/r/01_02_import_m4comp2018.R | R | production/support | 209 | 209 | +0 | Unchanged |
| src/r/02_01_preprocess_series.R | src/r/02_01_preprocess_series.R | R | production/support | 81 | 81 | +0 | Unchanged |
| src/r/04_01_forecast_auto_arima.R | src/r/04_01_forecast_auto_arima.R | R | production/support | 86 | 86 | +0 | Unchanged |
| src/r/04_01_forecast_r_methods.R | src/r/04_01_forecast_r_methods.R | R | production/support | 103 | 103 | +0 | Unchanged |
| src/r/qa/get_m4_daily_series.R | src/r/qa/get_m4_daily_series.R | R | manual QA | 300 | 300 | +0 | Unchanged |
| src/r/qa/inspect_rolling_window.R | src/r/qa/inspect_rolling_window.R | R | manual QA | 277 | 277 | +0 | Unchanged |
| src/r/qa/inspect_seasonal_period_tuning.R | src/r/qa/inspect_seasonal_period_tuning.R | R | manual QA | 149 | 149 | +0 | Unchanged |
| src/r/qa/inspect_standardisation.R | src/r/qa/inspect_standardisation.R | R | manual QA | 210 | 210 | +0 | Unchanged |
| src/r/tests/test_forecast_methods.R | src/r/tests/test_forecast_methods.R | R | automated test | 440 | 440 | +0 | Unchanged |
| src/r/tests/test_import_m4comp2018.R | src/r/tests/test_import_m4comp2018.R | R | automated test | 97 | 97 | +0 | Unchanged |
| src/r/tests/test_transformations.R | src/r/tests/test_transformations.R | R | automated test | 118 | 118 | +0 | Unchanged |
| src/r/util/forecast_methods.R | src/r/util/forecast_methods.R | R | production/support | 797 | 797 | +0 | Unchanged |
| src/r/util/labels.R | src/r/util/labels.R | R | production/support | 21 | 21 | +0 | Unchanged |
| src/r/util/seasonal_period.R | src/r/util/seasonal_period.R | R | production/support | 79 | 79 | +0 | Unchanged |
| src/r/util/time_series_input.R | src/r/util/time_series_input.R | R | production/support | 57 | 57 | +0 | Unchanged |
| src/r/util/transformations.R | src/r/util/transformations.R | R | production/support | 188 | 188 | +0 | Unchanged |
| src/r/util/window_preparation.R | src/r/util/window_preparation.R | R | production/support | 30 | 30 | +0 | Unchanged |

| Language and role | Scripts before | Scripts after | Lines before | Lines after | Line change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Python — production/support | 29 | 39 | 16930 | 17789 | +859 |
| Python — automated test | 20 | 23 | 8191 | 8489 | +298 |
| Python — manual QA | 0 | 0 | 0 | 0 | +0 |
| R — production/support | 11 | 11 | 1801 | 1801 | +0 |
| R — automated test | 3 | 3 | 655 | 655 | +0 |
| R — manual QA | 4 | 4 | 936 | 936 | +0 |
| Python subtotal | 49 | 62 | 25121 | 26278 | +1157 |
| R subtotal | 18 | 18 | 3392 | 3392 | +0 |
| production/support subtotal | 40 | 50 | 18731 | 19590 | +859 |
| automated test subtotal | 23 | 26 | 8846 | 9144 | +298 |
| manual QA subtotal | 4 | 4 | 936 | 936 | +0 |
| All scripts | 67 | 80 | 28513 | 29670 | +1157 |

### Stage 1 compared with the reviewed partial implementation

| Script before | Script after | Language | Role | Lines before | Lines after | Change | Status or reason |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| scripts/setup_r.R | scripts/setup_r.R | R | production/support | 150 | 150 | +0 | Unchanged |
| scripts/setup_support.py | scripts/setup_support.py | Python | production/support | 710 | 710 | +0 | Unchanged |
| src/python/00_main.py | src/python/00_main.py | Python | production/support | 1013 | 35 | -978 | Changed; see responsibility map |
| src/python/01_import.py | src/python/01_import.py | Python | production/support | 31 | 30 | -1 | Changed; see responsibility map |
| src/python/02_preprocess.py | src/python/02_preprocess.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/03_transform.py | src/python/03_transform.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/04_02_forecast_chronos.py | src/python/04_02_forecast_chronos.py | Python | production/support | 281 | 281 | +0 | Unchanged |
| src/python/04_03_forecast_m4_submission.py | src/python/04_03_forecast_m4_submission.py | Python | production/support | 29 | 29 | +0 | Unchanged |
| src/python/04_forecast.py | src/python/04_forecast.py | Python | production/support | 43 | 43 | +0 | Unchanged |
| src/python/05_combine.py | src/python/05_combine.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/06_01_evaluate_gift_eval.py | src/python/06_01_evaluate_gift_eval.py | Python | production/support | 462 | 462 | +0 | Unchanged |
| src/python/06_evaluate.py | src/python/06_evaluate.py | Python | production/support | 34 | 34 | +0 | Unchanged |
| src/python/tests/__init__.py | src/python/tests/__init__.py | Python | automated test | 10 | 10 | +0 | Unchanged |
| src/python/tests/acceptance.py | src/python/tests/acceptance.py | Python | automated test | 1871 | 1871 | +0 | Unchanged |
| src/python/tests/integration/__init__.py | src/python/tests/integration/__init__.py | Python | automated test | 10 | 10 | +0 | Unchanged |
| src/python/tests/integration/test_gift_eval_evaluation.py | src/python/tests/integration/test_gift_eval_evaluation.py | Python | automated test | 189 | 189 | +0 | Unchanged |
| src/python/tests/integration/test_gift_eval_semantics.py | src/python/tests/integration/test_gift_eval_semantics.py | Python | automated test | 98 | 98 | +0 | Unchanged |
| src/python/tests/integration/test_import_pipeline.py | src/python/tests/integration/test_import_pipeline.py | Python | automated test | 151 | 151 | +0 | Unchanged |
| src/python/tests/integration/test_preprocessing_regression.py | src/python/tests/integration/test_preprocessing_regression.py | Python | automated test | 72 | 72 | +0 | Unchanged |
| src/python/tests/test_acceptance.py | src/python/tests/test_acceptance.py | Python | automated test | 457 | 457 | +0 | Unchanged |
| src/python/tests/test_configuration.py | src/python/tests/test_configuration.py | Python | automated test | 297 | 297 | +0 | Unchanged |
| src/python/tests/test_execution.py | src/python/tests/test_execution.py | Python | automated test | 893 | 893 | +0 | Unchanged |
| src/python/tests/test_experiment_execution.py | src/python/tests/test_experiment_execution.py | Python | automated test | 972 | 985 | +13 | Changed; see responsibility map |
| — | src/python/tests/test_forecast_flow.py | Python | automated test | 0 | 197 | +197 | Added; responsibility split or contract test |
| src/python/tests/test_gpu_concurrency_calibration.py | src/python/tests/test_gpu_concurrency_calibration.py | Python | automated test | 197 | 197 | +0 | Unchanged |
| src/python/tests/test_import.py | src/python/tests/test_import.py | Python | automated test | 400 | 429 | +29 | Changed; see responsibility map |
| src/python/tests/test_m4_reference.py | src/python/tests/test_m4_reference.py | Python | automated test | 354 | 354 | +0 | Unchanged |
| src/python/tests/test_main.py | src/python/tests/test_main.py | Python | automated test | 790 | 437 | -353 | Changed; see responsibility map |
| src/python/tests/test_preprocessing.py | src/python/tests/test_preprocessing.py | Python | automated test | 109 | 109 | +0 | Unchanged |
| — | src/python/tests/test_process_storage.py | Python | automated test | 0 | 60 | +60 | Added; responsibility split or contract test |
| src/python/tests/test_seasonal_period_tuning.py | src/python/tests/test_seasonal_period_tuning.py | Python | automated test | 124 | 124 | +0 | Unchanged |
| src/python/tests/test_setup.py | src/python/tests/test_setup.py | Python | automated test | 394 | 394 | +0 | Unchanged |
| src/python/tests/test_transformations.py | src/python/tests/test_transformations.py | Python | automated test | 258 | 258 | +0 | Unchanged |
| src/python/tests/test_window_preparation.py | src/python/tests/test_window_preparation.py | Python | automated test | 665 | 665 | +0 | Unchanged |
| src/python/tests/test_workflow_orchestration.py | src/python/tests/test_workflow_orchestration.py | Python | automated test | 232 | 232 | +0 | Unchanged |
| src/python/util/__init__.py | src/python/util/__init__.py | Python | production/support | 10 | 10 | +0 | Unchanged |
| src/python/util/configuration.py | src/python/util/configuration.py | Python | production/support | 1043 | 1043 | +0 | Unchanged |
| src/python/util/database.py | src/python/util/database.py | Python | production/support | 962 | 962 | +0 | Unchanged |
| src/python/util/distributed_cluster.py | src/python/util/distributed_cluster.py | Python | production/support | 343 | 343 | +0 | Unchanged |
| src/python/util/distributed_execution.py | src/python/util/distributed_execution.py | Python | production/support | 1288 | 1288 | +0 | Unchanged |
| src/python/util/execution_calibration.py | src/python/util/execution_calibration.py | Python | production/support | 1141 | 1141 | +0 | Unchanged |
| — | src/python/util/execution_event_storage.py | Python | production/support | 0 | 125 | +125 | Added; responsibility split or contract test |
| src/python/util/execution_profiles.py | src/python/util/execution_profiles.py | Python | production/support | 564 | 564 | +0 | Unchanged |
| src/python/util/experiment_execution.py | src/python/util/experiment_execution.py | Python | production/support | 3126 | 2756 | -370 | Changed; see responsibility map |
| src/python/util/forecast_combination.py | src/python/util/forecast_combination.py | Python | production/support | 52 | 52 | +0 | Unchanged |
| — | src/python/util/forecast_flow.py | Python | production/support | 0 | 120 | +120 | Added; responsibility split or contract test |
| — | src/python/util/forecast_provider.py | Python | production/support | 0 | 209 | +209 | Added; responsibility split or contract test |
| — | src/python/util/forecast_storage.py | Python | production/support | 0 | 147 | +147 | Added; responsibility split or contract test |
| src/python/util/gift_eval_acquisition.py | src/python/util/gift_eval_acquisition.py | Python | production/support | 294 | 294 | +0 | Unchanged |
| src/python/util/gift_eval_source.py | src/python/util/gift_eval_source.py | Python | production/support | 150 | 209 | +59 | Changed; see responsibility map |
| src/python/util/gpu_concurrency_calibration.py | src/python/util/gpu_concurrency_calibration.py | Python | production/support | 1386 | 1386 | +0 | Unchanged |
| src/python/util/import_execution.py | src/python/util/import_execution.py | Python | production/support | 986 | 1097 | +111 | Changed; see responsibility map |
| — | src/python/util/import_flow.py | Python | production/support | 0 | 52 | +52 | Added; responsibility split or contract test |
| src/python/util/m4_submission.py | src/python/util/m4_submission.py | Python | production/support | 121 | 121 | +0 | Unchanged |
| — | src/python/util/process_storage.py | Python | production/support | 0 | 151 | +151 | Added; responsibility split or contract test |
| src/python/util/provenance.py | src/python/util/provenance.py | Python | production/support | 55 | 55 | +0 | Unchanged |
| — | src/python/util/researcher_actions.py | Python | production/support | 0 | 509 | +509 | Added; responsibility split or contract test |
| — | src/python/util/researcher_cli.py | Python | production/support | 0 | 148 | +148 | Added; responsibility split or contract test |
| — | src/python/util/researcher_request.py | Python | production/support | 0 | 24 | +24 | Added; responsibility split or contract test |
| src/python/util/seasonal_period_tuning.py | src/python/util/seasonal_period_tuning.py | Python | production/support | 1614 | 1614 | +0 | Unchanged |
| src/python/util/transformations.py | src/python/util/transformations.py | Python | production/support | 263 | 263 | +0 | Unchanged |
| src/python/util/window_preparation.py | src/python/util/window_preparation.py | Python | production/support | 1098 | 1098 | +0 | Unchanged |
| src/python/util/workflow_orchestration.py | src/python/util/workflow_orchestration.py | Python | production/support | 288 | 288 | +0 | Changed; see responsibility map |
| src/r/01_02_import_m4comp2018.R | src/r/01_02_import_m4comp2018.R | R | production/support | 209 | 209 | +0 | Unchanged |
| src/r/02_01_preprocess_series.R | src/r/02_01_preprocess_series.R | R | production/support | 81 | 81 | +0 | Unchanged |
| src/r/04_01_forecast_auto_arima.R | src/r/04_01_forecast_auto_arima.R | R | production/support | 86 | 86 | +0 | Unchanged |
| src/r/04_01_forecast_r_methods.R | src/r/04_01_forecast_r_methods.R | R | production/support | 103 | 103 | +0 | Unchanged |
| src/r/qa/get_m4_daily_series.R | src/r/qa/get_m4_daily_series.R | R | manual QA | 300 | 300 | +0 | Unchanged |
| src/r/qa/inspect_rolling_window.R | src/r/qa/inspect_rolling_window.R | R | manual QA | 277 | 277 | +0 | Unchanged |
| src/r/qa/inspect_seasonal_period_tuning.R | src/r/qa/inspect_seasonal_period_tuning.R | R | manual QA | 149 | 149 | +0 | Unchanged |
| src/r/qa/inspect_standardisation.R | src/r/qa/inspect_standardisation.R | R | manual QA | 210 | 210 | +0 | Unchanged |
| src/r/tests/test_forecast_methods.R | src/r/tests/test_forecast_methods.R | R | automated test | 440 | 440 | +0 | Unchanged |
| src/r/tests/test_import_m4comp2018.R | src/r/tests/test_import_m4comp2018.R | R | automated test | 97 | 97 | +0 | Unchanged |
| src/r/tests/test_transformations.R | src/r/tests/test_transformations.R | R | automated test | 118 | 118 | +0 | Unchanged |
| src/r/util/forecast_methods.R | src/r/util/forecast_methods.R | R | production/support | 797 | 797 | +0 | Unchanged |
| src/r/util/labels.R | src/r/util/labels.R | R | production/support | 21 | 21 | +0 | Unchanged |
| src/r/util/seasonal_period.R | src/r/util/seasonal_period.R | R | production/support | 79 | 79 | +0 | Unchanged |
| src/r/util/time_series_input.R | src/r/util/time_series_input.R | R | production/support | 57 | 57 | +0 | Unchanged |
| src/r/util/transformations.R | src/r/util/transformations.R | R | production/support | 188 | 188 | +0 | Unchanged |
| src/r/util/window_preparation.R | src/r/util/window_preparation.R | R | production/support | 30 | 30 | +0 | Unchanged |

| Language and role | Scripts before | Scripts after | Lines before | Lines after | Line change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Python — production/support | 30 | 39 | 17483 | 17789 | +306 |
| Python — automated test | 21 | 23 | 8543 | 8489 | -54 |
| Python — manual QA | 0 | 0 | 0 | 0 | +0 |
| R — production/support | 11 | 11 | 1801 | 1801 | +0 |
| R — automated test | 3 | 3 | 655 | 655 | +0 |
| R — manual QA | 4 | 4 | 936 | 936 | +0 |
| Python subtotal | 51 | 62 | 26026 | 26278 | +252 |
| R subtotal | 18 | 18 | 3392 | 3392 | +0 |
| production/support subtotal | 41 | 50 | 19284 | 19590 | +306 |
| automated test subtotal | 24 | 26 | 9198 | 9144 | -54 |
| manual QA subtotal | 4 | 4 | 936 | 936 | +0 |
| All scripts | 69 | 80 | 29418 | 29670 | +252 |

## Stage 1 correction handoff: 3 October 2026

**Not checkpoint acceptance.** The independent corrections below are implemented
in the uncommitted workspace. The corrected ordinary distributed GPU route is
fail-closed, so normal distributed entry-to-result/retrieval, interrupted restart
and the small sequential numerical comparison remain unverified. Earlier direct
flow and recovery evidence above is preserved, not relabelled as new-route proof.
No Stage 2 work, commit, push or expensive forecast/tuning campaign was performed.

### Six findings and their evidence

Evidence paths in this section are relative to
`.amp/in/artifacts/workflow-stage1-corrections/`.

| Finding | Implemented correction | Verification and remaining limits |
| --- | --- | --- |
| Distributed memory safeguards | Profile-derived `ForecastSafetyPolicy` connects ordinary R calls to existing reservations and continuous child monitoring. Independent Chronos batches have bounded OOM subdivision; singleton/joint-learning OOM is terminal to preserve semantics. | Controlled pressure, release, terminal/OOM tests pass. GPU headroom/admission remains unresolved; ordinary protected Chronos rejects before model startup. No intentional memory exhaustion. |
| Normal-entry preflight | `run_process` passes actual runtime manifest to worker validation; matching dirty source is allowed, missing/extra/stale files rejected. Locks/dependencies and legacy clean-tree checks remain. | Exact Mac/Ubuntu runtime manifests match; rejection tests pass. Actual entry validates/skips Gates 1–3 and rejects corrupted predecessors. Corrected distributed result/retrieval path is blocked, not verified. |
| Effective retries | Gate 4 consumes effective settings rather than stored defaults; Prefect compute tasks own retries, enclosing gate/Dask retries remain zero. | Real coordinator-dispatch tests: stored one retry gives two attempts; explicit zero gives one; explicit two gives three. |
| Stored-data validation | Gate 1 checks pinned source/identities/values/hashes/windows/splits; Gate 2 checks hashes, values and preprocessing contracts; Gate 3 recomputes values/state/hash and validates lineage. | Altered values, lineage and missing task/output tests pass. Public-entry copied-data corruption leaves Gate 1 failed, Gate 4 pending and zero forecasts; historical hash preserved. Valid historical Gates 1–3 resume successfully. |
| Bounded load balancing | Prefect completion-order refill replaces fixed waves; no-overlap phases remain. Submission/result failures stop admission and drain submitted futures. | Real local Prefect futures and DuckDB tests prove eligible CPU refill while GPU work is blocked, caps and phase ordering. This is controlled orchestration evidence, not corrected two-host placement proof. |
| Object boundaries/documentation | Providers share `forecast(model, batch)`; public import storage operations own attempt/result/failure bookkeeping. Affected headers/contracts and architecture map updated. | Focused contract tests pass. Human readability/OOP acceptance remains for ChatGPT and researcher review; broader legacy migration remains Stage 2. |

The normal historical restart first exposed R/jsonlite integral JSON versus
DuckDB DOUBLE-array hash encoding: 190/200 valid preprocessing rows were rejected.
Validation now accepts the historical integral encoding as well as the exact
double encoding, without rewriting stored hashes or scientific values. The first
failure is retained in `predecessor-resume.log`; the successful corrected run is
`predecessor-resume-v2.json` and its log.

### Bounded checks and source identity

- `final-regression.log`: **128 passed**, using the command below on Mac.
- `ubuntu-focused.log`: **60 passed**, covering process storage, import, forecast
  safety, forecast flow and execution on Ubuntu.
- `import-resume.json`: public-entry reuse of the existing 100-series import.
- `predecessor-resume-v2.json`: public-entry validation/skip of Gates 1–3 using
  `.amp/in/workflow-stage1-correction-predecessors-v2.duckdb`, a disposable copy
  of the prior fixture, not the accepted source database.
- `corrupt-entry.log`, `corrupt-result.json`: public-entry rejection after one raw
  value was altered in a separate disposable copy; original hash retained,
  affected gate failed, forecast count zero.
- `gpu-policy-block.log`: normal entry with `--processes 4 --execution-profile
  poc2_seasonal_recovery` exits with failure before cluster startup.
- Python compilation and `git diff --check` passed. No new scientific results
  or numerical equivalence claim is made by these checks.

Source synchronization checked Ubuntu files against prior recorded hashes before
copying authorised runtime files; no environment, accepted data or results were
copied. Final `mac-final-runtime.json` and `ubuntu-final-runtime.json` agree on
source/configuration/support files and locks. Canonical runtime identity:
`ed7dc5f79db0df6b1e4160416d300fde2ee758e105dc82b7b474ce0131b67b40`.
Both hosts report Python 3.12.14, Prefect 3.8.7, prefect-dask 0.3.7,
Dask/distributed 2026.8.0 and DuckDB 1.5.5; native R 4.6.1,
forecast 8.24.0 and jsonlite 2.0.0. See the host environment and R-package JSONs.
This demonstrates matching runtime inputs, not successful GPU integration.

### Profile and lifecycle blocker at handoff

The managed `poc2_seasonal_recovery` v2 profile supplies the approved distributed
R safety controls but sets accelerator headroom to zero, conflicting with the
execution policy's 4 GiB GPU floor. `ManagedTuningCluster` currently launches CPU
pools only; ordinary profile-based forecasting needs the GPU topology too. Other
GPU profiles lack the distributed R admission controls. The tuning guard also
explicitly requires v2. Silently changing these coupled controls would alter the
approved runtime contract. A global inference lock was rejected because it would
impose a hidden one-batch GPU cap without solving transient pressure admission.

**Proposed at handoff; subsequently approved below, not yet implemented:**
introduce runtime profile v3 with the
policy's 4 GiB GPU floor, extend the existing managed lifecycle to launch/validate
the configured 15 GPU workers when required, and reconcile the tuning guard.
Retain 8 Mac/15 Ubuntu CPU capacity, 12 GiB R fit budget, 3/16 GiB host floors,
thread/batch limits and no CPU/GPU overlap; preserve historical profile snapshots
and all scientific settings. Complete shared-GPU admission using existing safety
primitives, then perform only the outstanding bounded normal-route/restart and
sequential-comparison checks. Do not weaken preflight to obtain a passing run.

### Technical review and closure follow-up approval

On 3 October 2026, ChatGPT verified that all 81 scripts matched the recorded
hashes and that the saved logs supported the reported 128 Mac and 60 Ubuntu
test passes. The saved host runtime manifests matched. This was source/artifact
review, not a rerun or new live two-host acceptance.

The review confirmed the profile/lifecycle blocker and these execution gaps:

- The normal entry still expects one GPU worker instead of consuming the
  configured GPU capacity. Launch and validation counts must agree.
- An isolated simulation of the actual refill branch with two slots, three CPU
  batches and two blocked GPU batches completed the first two CPU batches, then
  left the third unsubmitted behind the two GPU futures. The existing one-GPU
  regression did not cover this case.
- The ordinary flow receives the overall in-flight bound but does not enforce
  the profile's AutoARIMA-specific cap of 8. Memory admission alone is not that
  concurrency contract.

The researcher approved profile v3/lifecycle reconciliation and completion of
these remaining safeguards. The policy change is recorded in
[execution policy](execution-policy.md#approved-profile-reconciliation); the
authoritative implementation assignment is the
[Stage 1 closure follow-up](amp-poc2-workflow-orchestration-instructions.md#approved-stage-1-closure-follow-up).
Keep the correction evidence above as a dated snapshot and add separately
identified follow-up results. No implementation, successful GPU run or checkpoint
acceptance is claimed by this approval. Stage 1 remains open; Stage 2 and Git
publication remain unauthorised.

### Manual QA at this review boundary

Run the focused regression command from the repository root:

```sh
PYTHONPATH=src/python .venv/bin/python -m unittest \
  tests.test_main tests.test_configuration tests.test_process_storage \
  tests.test_import tests.test_experiment_execution tests.test_forecast_flow \
  tests.test_forecast_safety tests.test_execution tests.test_workflow_orchestration
```

For independent read-only inspection and bounded restart validation of the
already copied fixture (never substitute an accepted research database):

```sh
.venv/bin/python src/python/00_main.py status \
  --database .amp/in/workflow-stage1-correction-predecessors-v2.duckdb
.venv/bin/python src/python/00_main.py run \
  --database .amp/in/workflow-stage1-correction-predecessors-v2.duckdb --processes 1 2 3
```

Expect all three gates to validate and report `skipped_completed`. Inspect the
corruption artifacts rather than modifying this clean fixture. Do not run the
full historical acceptance command as correction QA. After implementing the
approved profile change,
review fixture bounds, attempt/time budgets and source/dependency identities
before the outstanding two-host run; require actual host contributions, common
retrieval, unchanged accepted rows on restart and declared numerical tolerances.

For code review, follow `00_main → ResearcherCLI/ResearcherActions → Prefect gate`;
Gate 1 composes configured source records with public import storage operations;
Gate 4 composes pending-job preparation, named provider tasks, bounded completion
refill, Mac-only commits and final validation. See the architecture responsibility
map and `forecast_flow.py`, `forecast_provider.py`, `import_flow.py`,
`import_execution.py`, `process_storage.py`. The one-off inventory and diagnostic
scripts use simple functions because they have no reusable production state;
this bounded exception does not apply to production workflow orchestration.

### Complete correction script/line comparison

The following generated tables retain every unchanged script and the original,
partial and reviewed baselines. Counts use the same physical-line/Git-visible
first-party R/Python rules as the earlier snapshot (`.amp/in/stage1_inventory.py`).
The correction grows from **80 / 29,670 to 81 / 30,341** scripts/lines:
production/support +335 and automated tests +336, total +671. Final categories
are 19,925 production/support, 9,480 automated-test and 936 manual-QA lines.
Relative growth is +923 versus the partial baseline and +1,828 versus original.
The main stays 35 lines. This is responsibility separation and stronger validation,
not an overall code-size reduction; profile admission/monitoring remains custom
safety code rather than being delegated to Prefect retries or Dask scheduling.

Preserved `before-scripts.tsv` SHA-256:
`0189f8108f7ab697f763f7cfc52504a3a9c2d1dae01a8c95e7eb0751e42167b8`.
Correction `final-scripts.tsv` SHA-256:
`adb2426675391fc01e8ac3cf7bb9347cbc0c988be6a03ae459536d12b307b2a5`.

### Stage 1 compared with the original 67-script baseline

| Script before | Script after | Language | Role | Lines before | Lines after | Change | Status or reason |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| scripts/setup_r.R | scripts/setup_r.R | R | production/support | 150 | 150 | +0 | Unchanged |
| scripts/setup_support.py | scripts/setup_support.py | Python | production/support | 706 | 710 | +4 | Changed; see responsibility map |
| src/python/00_main.py | src/python/00_main.py | Python | production/support | 763 | 35 | -728 | Changed; see responsibility map |
| src/python/01_import.py | src/python/01_import.py | Python | production/support | 31 | 30 | -1 | Changed; see responsibility map |
| src/python/02_preprocess.py | src/python/02_preprocess.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/03_transform.py | src/python/03_transform.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/04_02_forecast_chronos.py | src/python/04_02_forecast_chronos.py | Python | production/support | 281 | 281 | +0 | Unchanged |
| src/python/04_03_forecast_m4_submission.py | src/python/04_03_forecast_m4_submission.py | Python | production/support | 29 | 29 | +0 | Unchanged |
| src/python/04_forecast.py | src/python/04_forecast.py | Python | production/support | 43 | 43 | +0 | Unchanged |
| src/python/05_combine.py | src/python/05_combine.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/06_01_evaluate_gift_eval.py | src/python/06_01_evaluate_gift_eval.py | Python | production/support | 462 | 462 | +0 | Unchanged |
| src/python/06_evaluate.py | src/python/06_evaluate.py | Python | production/support | 34 | 34 | +0 | Unchanged |
| src/python/tests/__init__.py | src/python/tests/__init__.py | Python | automated test | 10 | 10 | +0 | Unchanged |
| src/python/tests/acceptance.py | src/python/tests/acceptance.py | Python | automated test | 1784 | 1871 | +87 | Changed; see responsibility map |
| src/python/tests/integration/__init__.py | src/python/tests/integration/__init__.py | Python | automated test | 10 | 10 | +0 | Unchanged |
| src/python/tests/integration/test_gift_eval_evaluation.py | src/python/tests/integration/test_gift_eval_evaluation.py | Python | automated test | 189 | 189 | +0 | Unchanged |
| src/python/tests/integration/test_gift_eval_semantics.py | src/python/tests/integration/test_gift_eval_semantics.py | Python | automated test | 98 | 98 | +0 | Unchanged |
| src/python/tests/integration/test_import_pipeline.py | src/python/tests/integration/test_import_pipeline.py | Python | automated test | 151 | 151 | +0 | Unchanged |
| src/python/tests/integration/test_preprocessing_regression.py | src/python/tests/integration/test_preprocessing_regression.py | Python | automated test | 72 | 72 | +0 | Unchanged |
| src/python/tests/test_acceptance.py | src/python/tests/test_acceptance.py | Python | automated test | 457 | 457 | +0 | Unchanged |
| src/python/tests/test_configuration.py | src/python/tests/test_configuration.py | Python | automated test | 297 | 297 | +0 | Unchanged |
| src/python/tests/test_execution.py | src/python/tests/test_execution.py | Python | automated test | 893 | 931 | +38 | Changed; see responsibility map |
| src/python/tests/test_experiment_execution.py | src/python/tests/test_experiment_execution.py | Python | automated test | 972 | 985 | +13 | Changed; see responsibility map |
| — | src/python/tests/test_forecast_flow.py | Python | automated test | 0 | 265 | +265 | Added; responsibility split or contract test |
| — | src/python/tests/test_forecast_safety.py | Python | automated test | 0 | 127 | +127 | Added; responsibility split or contract test |
| src/python/tests/test_gpu_concurrency_calibration.py | src/python/tests/test_gpu_concurrency_calibration.py | Python | automated test | 197 | 197 | +0 | Unchanged |
| src/python/tests/test_import.py | src/python/tests/test_import.py | Python | automated test | 400 | 467 | +67 | Changed; see responsibility map |
| src/python/tests/test_m4_reference.py | src/python/tests/test_m4_reference.py | Python | automated test | 354 | 354 | +0 | Unchanged |
| src/python/tests/test_main.py | src/python/tests/test_main.py | Python | automated test | 757 | 437 | -320 | Changed; see responsibility map |
| src/python/tests/test_preprocessing.py | src/python/tests/test_preprocessing.py | Python | automated test | 109 | 109 | +0 | Unchanged |
| — | src/python/tests/test_process_storage.py | Python | automated test | 0 | 125 | +125 | Added; responsibility split or contract test |
| src/python/tests/test_seasonal_period_tuning.py | src/python/tests/test_seasonal_period_tuning.py | Python | automated test | 124 | 124 | +0 | Unchanged |
| src/python/tests/test_setup.py | src/python/tests/test_setup.py | Python | automated test | 394 | 394 | +0 | Unchanged |
| src/python/tests/test_transformations.py | src/python/tests/test_transformations.py | Python | automated test | 258 | 258 | +0 | Unchanged |
| src/python/tests/test_window_preparation.py | src/python/tests/test_window_preparation.py | Python | automated test | 665 | 665 | +0 | Unchanged |
| — | src/python/tests/test_workflow_orchestration.py | Python | automated test | 0 | 232 | +232 | Added; responsibility split or contract test |
| src/python/util/__init__.py | src/python/util/__init__.py | Python | production/support | 10 | 10 | +0 | Unchanged |
| src/python/util/configuration.py | src/python/util/configuration.py | Python | production/support | 1043 | 1043 | +0 | Unchanged |
| src/python/util/database.py | src/python/util/database.py | Python | production/support | 962 | 962 | +0 | Unchanged |
| src/python/util/distributed_cluster.py | src/python/util/distributed_cluster.py | Python | production/support | 343 | 343 | +0 | Unchanged |
| src/python/util/distributed_execution.py | src/python/util/distributed_execution.py | Python | production/support | 1288 | 1327 | +39 | Changed; see responsibility map |
| src/python/util/execution_calibration.py | src/python/util/execution_calibration.py | Python | production/support | 1141 | 1141 | +0 | Unchanged |
| — | src/python/util/execution_event_storage.py | Python | production/support | 0 | 125 | +125 | Added; responsibility split or contract test |
| src/python/util/execution_profiles.py | src/python/util/execution_profiles.py | Python | production/support | 564 | 564 | +0 | Unchanged |
| src/python/util/experiment_execution.py | src/python/util/experiment_execution.py | Python | production/support | 3126 | 2762 | -364 | Changed; see responsibility map |
| src/python/util/forecast_combination.py | src/python/util/forecast_combination.py | Python | production/support | 52 | 52 | +0 | Unchanged |
| — | src/python/util/forecast_flow.py | Python | production/support | 0 | 143 | +143 | Added; responsibility split or contract test |
| — | src/python/util/forecast_provider.py | Python | production/support | 0 | 267 | +267 | Added; responsibility split or contract test |
| — | src/python/util/forecast_storage.py | Python | production/support | 0 | 147 | +147 | Added; responsibility split or contract test |
| src/python/util/gift_eval_acquisition.py | src/python/util/gift_eval_acquisition.py | Python | production/support | 294 | 294 | +0 | Unchanged |
| src/python/util/gift_eval_source.py | src/python/util/gift_eval_source.py | Python | production/support | 150 | 209 | +59 | Changed; see responsibility map |
| src/python/util/gpu_concurrency_calibration.py | src/python/util/gpu_concurrency_calibration.py | Python | production/support | 1386 | 1386 | +0 | Unchanged |
| src/python/util/import_execution.py | src/python/util/import_execution.py | Python | production/support | 986 | 1144 | +158 | Changed; see responsibility map |
| — | src/python/util/import_flow.py | Python | production/support | 0 | 54 | +54 | Added; responsibility split or contract test |
| src/python/util/m4_submission.py | src/python/util/m4_submission.py | Python | production/support | 121 | 121 | +0 | Unchanged |
| — | src/python/util/process_storage.py | Python | production/support | 0 | 292 | +292 | Added; responsibility split or contract test |
| src/python/util/provenance.py | src/python/util/provenance.py | Python | production/support | 55 | 55 | +0 | Unchanged |
| — | src/python/util/researcher_actions.py | Python | production/support | 0 | 528 | +528 | Added; responsibility split or contract test |
| — | src/python/util/researcher_cli.py | Python | production/support | 0 | 148 | +148 | Added; responsibility split or contract test |
| — | src/python/util/researcher_request.py | Python | production/support | 0 | 24 | +24 | Added; responsibility split or contract test |
| src/python/util/seasonal_period_tuning.py | src/python/util/seasonal_period_tuning.py | Python | production/support | 1614 | 1614 | +0 | Unchanged |
| src/python/util/transformations.py | src/python/util/transformations.py | Python | production/support | 263 | 263 | +0 | Unchanged |
| src/python/util/window_preparation.py | src/python/util/window_preparation.py | Python | production/support | 1087 | 1098 | +11 | Changed; see responsibility map |
| — | src/python/util/workflow_orchestration.py | Python | production/support | 0 | 288 | +288 | Added; responsibility split or contract test |
| src/r/01_02_import_m4comp2018.R | src/r/01_02_import_m4comp2018.R | R | production/support | 209 | 209 | +0 | Unchanged |
| src/r/02_01_preprocess_series.R | src/r/02_01_preprocess_series.R | R | production/support | 81 | 81 | +0 | Unchanged |
| src/r/04_01_forecast_auto_arima.R | src/r/04_01_forecast_auto_arima.R | R | production/support | 86 | 86 | +0 | Unchanged |
| src/r/04_01_forecast_r_methods.R | src/r/04_01_forecast_r_methods.R | R | production/support | 103 | 103 | +0 | Unchanged |
| src/r/qa/get_m4_daily_series.R | src/r/qa/get_m4_daily_series.R | R | manual QA | 300 | 300 | +0 | Unchanged |
| src/r/qa/inspect_rolling_window.R | src/r/qa/inspect_rolling_window.R | R | manual QA | 277 | 277 | +0 | Unchanged |
| src/r/qa/inspect_seasonal_period_tuning.R | src/r/qa/inspect_seasonal_period_tuning.R | R | manual QA | 149 | 149 | +0 | Unchanged |
| src/r/qa/inspect_standardisation.R | src/r/qa/inspect_standardisation.R | R | manual QA | 210 | 210 | +0 | Unchanged |
| src/r/tests/test_forecast_methods.R | src/r/tests/test_forecast_methods.R | R | automated test | 440 | 440 | +0 | Unchanged |
| src/r/tests/test_import_m4comp2018.R | src/r/tests/test_import_m4comp2018.R | R | automated test | 97 | 97 | +0 | Unchanged |
| src/r/tests/test_transformations.R | src/r/tests/test_transformations.R | R | automated test | 118 | 118 | +0 | Unchanged |
| src/r/util/forecast_methods.R | src/r/util/forecast_methods.R | R | production/support | 797 | 797 | +0 | Unchanged |
| src/r/util/labels.R | src/r/util/labels.R | R | production/support | 21 | 21 | +0 | Unchanged |
| src/r/util/seasonal_period.R | src/r/util/seasonal_period.R | R | production/support | 79 | 79 | +0 | Unchanged |
| src/r/util/time_series_input.R | src/r/util/time_series_input.R | R | production/support | 57 | 57 | +0 | Unchanged |
| src/r/util/transformations.R | src/r/util/transformations.R | R | production/support | 188 | 188 | +0 | Unchanged |
| src/r/util/window_preparation.R | src/r/util/window_preparation.R | R | production/support | 30 | 30 | +0 | Unchanged |

| Language and role | Scripts before | Scripts after | Lines before | Lines after | Line change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Python — production/support | 29 | 39 | 16930 | 18124 | +1194 |
| Python — automated test | 20 | 24 | 8191 | 8825 | +634 |
| Python — manual QA | 0 | 0 | 0 | 0 | +0 |
| R — production/support | 11 | 11 | 1801 | 1801 | +0 |
| R — automated test | 3 | 3 | 655 | 655 | +0 |
| R — manual QA | 4 | 4 | 936 | 936 | +0 |
| Python subtotal | 49 | 63 | 25121 | 26949 | +1828 |
| R subtotal | 18 | 18 | 3392 | 3392 | +0 |
| production/support subtotal | 40 | 50 | 18731 | 19925 | +1194 |
| automated test subtotal | 23 | 27 | 8846 | 9480 | +634 |
| manual QA subtotal | 4 | 4 | 936 | 936 | +0 |
| All scripts | 67 | 81 | 28513 | 30341 | +1828 |

### Stage 1 compared with the partial 69-script implementation

| Script before | Script after | Language | Role | Lines before | Lines after | Change | Status or reason |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| scripts/setup_r.R | scripts/setup_r.R | R | production/support | 150 | 150 | +0 | Unchanged |
| scripts/setup_support.py | scripts/setup_support.py | Python | production/support | 710 | 710 | +0 | Unchanged |
| src/python/00_main.py | src/python/00_main.py | Python | production/support | 1013 | 35 | -978 | Changed; see responsibility map |
| src/python/01_import.py | src/python/01_import.py | Python | production/support | 31 | 30 | -1 | Changed; see responsibility map |
| src/python/02_preprocess.py | src/python/02_preprocess.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/03_transform.py | src/python/03_transform.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/04_02_forecast_chronos.py | src/python/04_02_forecast_chronos.py | Python | production/support | 281 | 281 | +0 | Unchanged |
| src/python/04_03_forecast_m4_submission.py | src/python/04_03_forecast_m4_submission.py | Python | production/support | 29 | 29 | +0 | Unchanged |
| src/python/04_forecast.py | src/python/04_forecast.py | Python | production/support | 43 | 43 | +0 | Unchanged |
| src/python/05_combine.py | src/python/05_combine.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/06_01_evaluate_gift_eval.py | src/python/06_01_evaluate_gift_eval.py | Python | production/support | 462 | 462 | +0 | Unchanged |
| src/python/06_evaluate.py | src/python/06_evaluate.py | Python | production/support | 34 | 34 | +0 | Unchanged |
| src/python/tests/__init__.py | src/python/tests/__init__.py | Python | automated test | 10 | 10 | +0 | Unchanged |
| src/python/tests/acceptance.py | src/python/tests/acceptance.py | Python | automated test | 1871 | 1871 | +0 | Unchanged |
| src/python/tests/integration/__init__.py | src/python/tests/integration/__init__.py | Python | automated test | 10 | 10 | +0 | Unchanged |
| src/python/tests/integration/test_gift_eval_evaluation.py | src/python/tests/integration/test_gift_eval_evaluation.py | Python | automated test | 189 | 189 | +0 | Unchanged |
| src/python/tests/integration/test_gift_eval_semantics.py | src/python/tests/integration/test_gift_eval_semantics.py | Python | automated test | 98 | 98 | +0 | Unchanged |
| src/python/tests/integration/test_import_pipeline.py | src/python/tests/integration/test_import_pipeline.py | Python | automated test | 151 | 151 | +0 | Unchanged |
| src/python/tests/integration/test_preprocessing_regression.py | src/python/tests/integration/test_preprocessing_regression.py | Python | automated test | 72 | 72 | +0 | Unchanged |
| src/python/tests/test_acceptance.py | src/python/tests/test_acceptance.py | Python | automated test | 457 | 457 | +0 | Unchanged |
| src/python/tests/test_configuration.py | src/python/tests/test_configuration.py | Python | automated test | 297 | 297 | +0 | Unchanged |
| src/python/tests/test_execution.py | src/python/tests/test_execution.py | Python | automated test | 893 | 931 | +38 | Changed; see responsibility map |
| src/python/tests/test_experiment_execution.py | src/python/tests/test_experiment_execution.py | Python | automated test | 972 | 985 | +13 | Changed; see responsibility map |
| — | src/python/tests/test_forecast_flow.py | Python | automated test | 0 | 265 | +265 | Added; responsibility split or contract test |
| — | src/python/tests/test_forecast_safety.py | Python | automated test | 0 | 127 | +127 | Added; responsibility split or contract test |
| src/python/tests/test_gpu_concurrency_calibration.py | src/python/tests/test_gpu_concurrency_calibration.py | Python | automated test | 197 | 197 | +0 | Unchanged |
| src/python/tests/test_import.py | src/python/tests/test_import.py | Python | automated test | 400 | 467 | +67 | Changed; see responsibility map |
| src/python/tests/test_m4_reference.py | src/python/tests/test_m4_reference.py | Python | automated test | 354 | 354 | +0 | Unchanged |
| src/python/tests/test_main.py | src/python/tests/test_main.py | Python | automated test | 790 | 437 | -353 | Changed; see responsibility map |
| src/python/tests/test_preprocessing.py | src/python/tests/test_preprocessing.py | Python | automated test | 109 | 109 | +0 | Unchanged |
| — | src/python/tests/test_process_storage.py | Python | automated test | 0 | 125 | +125 | Added; responsibility split or contract test |
| src/python/tests/test_seasonal_period_tuning.py | src/python/tests/test_seasonal_period_tuning.py | Python | automated test | 124 | 124 | +0 | Unchanged |
| src/python/tests/test_setup.py | src/python/tests/test_setup.py | Python | automated test | 394 | 394 | +0 | Unchanged |
| src/python/tests/test_transformations.py | src/python/tests/test_transformations.py | Python | automated test | 258 | 258 | +0 | Unchanged |
| src/python/tests/test_window_preparation.py | src/python/tests/test_window_preparation.py | Python | automated test | 665 | 665 | +0 | Unchanged |
| src/python/tests/test_workflow_orchestration.py | src/python/tests/test_workflow_orchestration.py | Python | automated test | 232 | 232 | +0 | Unchanged |
| src/python/util/__init__.py | src/python/util/__init__.py | Python | production/support | 10 | 10 | +0 | Unchanged |
| src/python/util/configuration.py | src/python/util/configuration.py | Python | production/support | 1043 | 1043 | +0 | Unchanged |
| src/python/util/database.py | src/python/util/database.py | Python | production/support | 962 | 962 | +0 | Unchanged |
| src/python/util/distributed_cluster.py | src/python/util/distributed_cluster.py | Python | production/support | 343 | 343 | +0 | Unchanged |
| src/python/util/distributed_execution.py | src/python/util/distributed_execution.py | Python | production/support | 1288 | 1327 | +39 | Changed; see responsibility map |
| src/python/util/execution_calibration.py | src/python/util/execution_calibration.py | Python | production/support | 1141 | 1141 | +0 | Unchanged |
| — | src/python/util/execution_event_storage.py | Python | production/support | 0 | 125 | +125 | Added; responsibility split or contract test |
| src/python/util/execution_profiles.py | src/python/util/execution_profiles.py | Python | production/support | 564 | 564 | +0 | Unchanged |
| src/python/util/experiment_execution.py | src/python/util/experiment_execution.py | Python | production/support | 3126 | 2762 | -364 | Changed; see responsibility map |
| src/python/util/forecast_combination.py | src/python/util/forecast_combination.py | Python | production/support | 52 | 52 | +0 | Unchanged |
| — | src/python/util/forecast_flow.py | Python | production/support | 0 | 143 | +143 | Added; responsibility split or contract test |
| — | src/python/util/forecast_provider.py | Python | production/support | 0 | 267 | +267 | Added; responsibility split or contract test |
| — | src/python/util/forecast_storage.py | Python | production/support | 0 | 147 | +147 | Added; responsibility split or contract test |
| src/python/util/gift_eval_acquisition.py | src/python/util/gift_eval_acquisition.py | Python | production/support | 294 | 294 | +0 | Unchanged |
| src/python/util/gift_eval_source.py | src/python/util/gift_eval_source.py | Python | production/support | 150 | 209 | +59 | Changed; see responsibility map |
| src/python/util/gpu_concurrency_calibration.py | src/python/util/gpu_concurrency_calibration.py | Python | production/support | 1386 | 1386 | +0 | Unchanged |
| src/python/util/import_execution.py | src/python/util/import_execution.py | Python | production/support | 986 | 1144 | +158 | Changed; see responsibility map |
| — | src/python/util/import_flow.py | Python | production/support | 0 | 54 | +54 | Added; responsibility split or contract test |
| src/python/util/m4_submission.py | src/python/util/m4_submission.py | Python | production/support | 121 | 121 | +0 | Unchanged |
| — | src/python/util/process_storage.py | Python | production/support | 0 | 292 | +292 | Added; responsibility split or contract test |
| src/python/util/provenance.py | src/python/util/provenance.py | Python | production/support | 55 | 55 | +0 | Unchanged |
| — | src/python/util/researcher_actions.py | Python | production/support | 0 | 528 | +528 | Added; responsibility split or contract test |
| — | src/python/util/researcher_cli.py | Python | production/support | 0 | 148 | +148 | Added; responsibility split or contract test |
| — | src/python/util/researcher_request.py | Python | production/support | 0 | 24 | +24 | Added; responsibility split or contract test |
| src/python/util/seasonal_period_tuning.py | src/python/util/seasonal_period_tuning.py | Python | production/support | 1614 | 1614 | +0 | Unchanged |
| src/python/util/transformations.py | src/python/util/transformations.py | Python | production/support | 263 | 263 | +0 | Unchanged |
| src/python/util/window_preparation.py | src/python/util/window_preparation.py | Python | production/support | 1098 | 1098 | +0 | Unchanged |
| src/python/util/workflow_orchestration.py | src/python/util/workflow_orchestration.py | Python | production/support | 288 | 288 | +0 | Changed; see responsibility map |
| src/r/01_02_import_m4comp2018.R | src/r/01_02_import_m4comp2018.R | R | production/support | 209 | 209 | +0 | Unchanged |
| src/r/02_01_preprocess_series.R | src/r/02_01_preprocess_series.R | R | production/support | 81 | 81 | +0 | Unchanged |
| src/r/04_01_forecast_auto_arima.R | src/r/04_01_forecast_auto_arima.R | R | production/support | 86 | 86 | +0 | Unchanged |
| src/r/04_01_forecast_r_methods.R | src/r/04_01_forecast_r_methods.R | R | production/support | 103 | 103 | +0 | Unchanged |
| src/r/qa/get_m4_daily_series.R | src/r/qa/get_m4_daily_series.R | R | manual QA | 300 | 300 | +0 | Unchanged |
| src/r/qa/inspect_rolling_window.R | src/r/qa/inspect_rolling_window.R | R | manual QA | 277 | 277 | +0 | Unchanged |
| src/r/qa/inspect_seasonal_period_tuning.R | src/r/qa/inspect_seasonal_period_tuning.R | R | manual QA | 149 | 149 | +0 | Unchanged |
| src/r/qa/inspect_standardisation.R | src/r/qa/inspect_standardisation.R | R | manual QA | 210 | 210 | +0 | Unchanged |
| src/r/tests/test_forecast_methods.R | src/r/tests/test_forecast_methods.R | R | automated test | 440 | 440 | +0 | Unchanged |
| src/r/tests/test_import_m4comp2018.R | src/r/tests/test_import_m4comp2018.R | R | automated test | 97 | 97 | +0 | Unchanged |
| src/r/tests/test_transformations.R | src/r/tests/test_transformations.R | R | automated test | 118 | 118 | +0 | Unchanged |
| src/r/util/forecast_methods.R | src/r/util/forecast_methods.R | R | production/support | 797 | 797 | +0 | Unchanged |
| src/r/util/labels.R | src/r/util/labels.R | R | production/support | 21 | 21 | +0 | Unchanged |
| src/r/util/seasonal_period.R | src/r/util/seasonal_period.R | R | production/support | 79 | 79 | +0 | Unchanged |
| src/r/util/time_series_input.R | src/r/util/time_series_input.R | R | production/support | 57 | 57 | +0 | Unchanged |
| src/r/util/transformations.R | src/r/util/transformations.R | R | production/support | 188 | 188 | +0 | Unchanged |
| src/r/util/window_preparation.R | src/r/util/window_preparation.R | R | production/support | 30 | 30 | +0 | Unchanged |

| Language and role | Scripts before | Scripts after | Lines before | Lines after | Line change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Python — production/support | 30 | 39 | 17483 | 18124 | +641 |
| Python — automated test | 21 | 24 | 8543 | 8825 | +282 |
| Python — manual QA | 0 | 0 | 0 | 0 | +0 |
| R — production/support | 11 | 11 | 1801 | 1801 | +0 |
| R — automated test | 3 | 3 | 655 | 655 | +0 |
| R — manual QA | 4 | 4 | 936 | 936 | +0 |
| Python subtotal | 51 | 63 | 26026 | 26949 | +923 |
| R subtotal | 18 | 18 | 3392 | 3392 | +0 |
| production/support subtotal | 41 | 50 | 19284 | 19925 | +641 |
| automated test subtotal | 24 | 27 | 9198 | 9480 | +282 |
| manual QA subtotal | 4 | 4 | 936 | 936 | +0 |
| All scripts | 69 | 81 | 29418 | 30341 | +923 |

### Stage 1 compared with the reviewed 80-script Stage 1 snapshot

| Script before | Script after | Language | Role | Lines before | Lines after | Change | Status or reason |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| scripts/setup_r.R | scripts/setup_r.R | R | production/support | 150 | 150 | +0 | Unchanged |
| scripts/setup_support.py | scripts/setup_support.py | Python | production/support | 710 | 710 | +0 | Unchanged |
| src/python/00_main.py | src/python/00_main.py | Python | production/support | 35 | 35 | +0 | Unchanged |
| src/python/01_import.py | src/python/01_import.py | Python | production/support | 30 | 30 | +0 | Unchanged |
| src/python/02_preprocess.py | src/python/02_preprocess.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/03_transform.py | src/python/03_transform.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/04_02_forecast_chronos.py | src/python/04_02_forecast_chronos.py | Python | production/support | 281 | 281 | +0 | Unchanged |
| src/python/04_03_forecast_m4_submission.py | src/python/04_03_forecast_m4_submission.py | Python | production/support | 29 | 29 | +0 | Unchanged |
| src/python/04_forecast.py | src/python/04_forecast.py | Python | production/support | 43 | 43 | +0 | Unchanged |
| src/python/05_combine.py | src/python/05_combine.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/06_01_evaluate_gift_eval.py | src/python/06_01_evaluate_gift_eval.py | Python | production/support | 462 | 462 | +0 | Unchanged |
| src/python/06_evaluate.py | src/python/06_evaluate.py | Python | production/support | 34 | 34 | +0 | Unchanged |
| src/python/tests/__init__.py | src/python/tests/__init__.py | Python | automated test | 10 | 10 | +0 | Unchanged |
| src/python/tests/acceptance.py | src/python/tests/acceptance.py | Python | automated test | 1871 | 1871 | +0 | Unchanged |
| src/python/tests/integration/__init__.py | src/python/tests/integration/__init__.py | Python | automated test | 10 | 10 | +0 | Unchanged |
| src/python/tests/integration/test_gift_eval_evaluation.py | src/python/tests/integration/test_gift_eval_evaluation.py | Python | automated test | 189 | 189 | +0 | Unchanged |
| src/python/tests/integration/test_gift_eval_semantics.py | src/python/tests/integration/test_gift_eval_semantics.py | Python | automated test | 98 | 98 | +0 | Unchanged |
| src/python/tests/integration/test_import_pipeline.py | src/python/tests/integration/test_import_pipeline.py | Python | automated test | 151 | 151 | +0 | Unchanged |
| src/python/tests/integration/test_preprocessing_regression.py | src/python/tests/integration/test_preprocessing_regression.py | Python | automated test | 72 | 72 | +0 | Unchanged |
| src/python/tests/test_acceptance.py | src/python/tests/test_acceptance.py | Python | automated test | 457 | 457 | +0 | Unchanged |
| src/python/tests/test_configuration.py | src/python/tests/test_configuration.py | Python | automated test | 297 | 297 | +0 | Unchanged |
| src/python/tests/test_execution.py | src/python/tests/test_execution.py | Python | automated test | 893 | 931 | +38 | Changed; see responsibility map |
| src/python/tests/test_experiment_execution.py | src/python/tests/test_experiment_execution.py | Python | automated test | 985 | 985 | +0 | Unchanged |
| src/python/tests/test_forecast_flow.py | src/python/tests/test_forecast_flow.py | Python | automated test | 197 | 265 | +68 | Changed; see responsibility map |
| — | src/python/tests/test_forecast_safety.py | Python | automated test | 0 | 127 | +127 | Added; responsibility split or contract test |
| src/python/tests/test_gpu_concurrency_calibration.py | src/python/tests/test_gpu_concurrency_calibration.py | Python | automated test | 197 | 197 | +0 | Unchanged |
| src/python/tests/test_import.py | src/python/tests/test_import.py | Python | automated test | 429 | 467 | +38 | Changed; see responsibility map |
| src/python/tests/test_m4_reference.py | src/python/tests/test_m4_reference.py | Python | automated test | 354 | 354 | +0 | Unchanged |
| src/python/tests/test_main.py | src/python/tests/test_main.py | Python | automated test | 437 | 437 | +0 | Unchanged |
| src/python/tests/test_preprocessing.py | src/python/tests/test_preprocessing.py | Python | automated test | 109 | 109 | +0 | Unchanged |
| src/python/tests/test_process_storage.py | src/python/tests/test_process_storage.py | Python | automated test | 60 | 125 | +65 | Changed; see responsibility map |
| src/python/tests/test_seasonal_period_tuning.py | src/python/tests/test_seasonal_period_tuning.py | Python | automated test | 124 | 124 | +0 | Unchanged |
| src/python/tests/test_setup.py | src/python/tests/test_setup.py | Python | automated test | 394 | 394 | +0 | Unchanged |
| src/python/tests/test_transformations.py | src/python/tests/test_transformations.py | Python | automated test | 258 | 258 | +0 | Unchanged |
| src/python/tests/test_window_preparation.py | src/python/tests/test_window_preparation.py | Python | automated test | 665 | 665 | +0 | Unchanged |
| src/python/tests/test_workflow_orchestration.py | src/python/tests/test_workflow_orchestration.py | Python | automated test | 232 | 232 | +0 | Unchanged |
| src/python/util/__init__.py | src/python/util/__init__.py | Python | production/support | 10 | 10 | +0 | Unchanged |
| src/python/util/configuration.py | src/python/util/configuration.py | Python | production/support | 1043 | 1043 | +0 | Unchanged |
| src/python/util/database.py | src/python/util/database.py | Python | production/support | 962 | 962 | +0 | Unchanged |
| src/python/util/distributed_cluster.py | src/python/util/distributed_cluster.py | Python | production/support | 343 | 343 | +0 | Unchanged |
| src/python/util/distributed_execution.py | src/python/util/distributed_execution.py | Python | production/support | 1288 | 1327 | +39 | Changed; see responsibility map |
| src/python/util/execution_calibration.py | src/python/util/execution_calibration.py | Python | production/support | 1141 | 1141 | +0 | Unchanged |
| src/python/util/execution_event_storage.py | src/python/util/execution_event_storage.py | Python | production/support | 125 | 125 | +0 | Unchanged |
| src/python/util/execution_profiles.py | src/python/util/execution_profiles.py | Python | production/support | 564 | 564 | +0 | Unchanged |
| src/python/util/experiment_execution.py | src/python/util/experiment_execution.py | Python | production/support | 2756 | 2762 | +6 | Changed; see responsibility map |
| src/python/util/forecast_combination.py | src/python/util/forecast_combination.py | Python | production/support | 52 | 52 | +0 | Unchanged |
| src/python/util/forecast_flow.py | src/python/util/forecast_flow.py | Python | production/support | 120 | 143 | +23 | Changed; see responsibility map |
| src/python/util/forecast_provider.py | src/python/util/forecast_provider.py | Python | production/support | 209 | 267 | +58 | Changed; see responsibility map |
| src/python/util/forecast_storage.py | src/python/util/forecast_storage.py | Python | production/support | 147 | 147 | +0 | Unchanged |
| src/python/util/gift_eval_acquisition.py | src/python/util/gift_eval_acquisition.py | Python | production/support | 294 | 294 | +0 | Unchanged |
| src/python/util/gift_eval_source.py | src/python/util/gift_eval_source.py | Python | production/support | 209 | 209 | +0 | Unchanged |
| src/python/util/gpu_concurrency_calibration.py | src/python/util/gpu_concurrency_calibration.py | Python | production/support | 1386 | 1386 | +0 | Unchanged |
| src/python/util/import_execution.py | src/python/util/import_execution.py | Python | production/support | 1097 | 1144 | +47 | Changed; see responsibility map |
| src/python/util/import_flow.py | src/python/util/import_flow.py | Python | production/support | 52 | 54 | +2 | Changed; see responsibility map |
| src/python/util/m4_submission.py | src/python/util/m4_submission.py | Python | production/support | 121 | 121 | +0 | Unchanged |
| src/python/util/process_storage.py | src/python/util/process_storage.py | Python | production/support | 151 | 292 | +141 | Changed; see responsibility map |
| src/python/util/provenance.py | src/python/util/provenance.py | Python | production/support | 55 | 55 | +0 | Unchanged |
| src/python/util/researcher_actions.py | src/python/util/researcher_actions.py | Python | production/support | 509 | 528 | +19 | Changed; see responsibility map |
| src/python/util/researcher_cli.py | src/python/util/researcher_cli.py | Python | production/support | 148 | 148 | +0 | Unchanged |
| src/python/util/researcher_request.py | src/python/util/researcher_request.py | Python | production/support | 24 | 24 | +0 | Unchanged |
| src/python/util/seasonal_period_tuning.py | src/python/util/seasonal_period_tuning.py | Python | production/support | 1614 | 1614 | +0 | Unchanged |
| src/python/util/transformations.py | src/python/util/transformations.py | Python | production/support | 263 | 263 | +0 | Unchanged |
| src/python/util/window_preparation.py | src/python/util/window_preparation.py | Python | production/support | 1098 | 1098 | +0 | Unchanged |
| src/python/util/workflow_orchestration.py | src/python/util/workflow_orchestration.py | Python | production/support | 288 | 288 | +0 | Unchanged |
| src/r/01_02_import_m4comp2018.R | src/r/01_02_import_m4comp2018.R | R | production/support | 209 | 209 | +0 | Unchanged |
| src/r/02_01_preprocess_series.R | src/r/02_01_preprocess_series.R | R | production/support | 81 | 81 | +0 | Unchanged |
| src/r/04_01_forecast_auto_arima.R | src/r/04_01_forecast_auto_arima.R | R | production/support | 86 | 86 | +0 | Unchanged |
| src/r/04_01_forecast_r_methods.R | src/r/04_01_forecast_r_methods.R | R | production/support | 103 | 103 | +0 | Unchanged |
| src/r/qa/get_m4_daily_series.R | src/r/qa/get_m4_daily_series.R | R | manual QA | 300 | 300 | +0 | Unchanged |
| src/r/qa/inspect_rolling_window.R | src/r/qa/inspect_rolling_window.R | R | manual QA | 277 | 277 | +0 | Unchanged |
| src/r/qa/inspect_seasonal_period_tuning.R | src/r/qa/inspect_seasonal_period_tuning.R | R | manual QA | 149 | 149 | +0 | Unchanged |
| src/r/qa/inspect_standardisation.R | src/r/qa/inspect_standardisation.R | R | manual QA | 210 | 210 | +0 | Unchanged |
| src/r/tests/test_forecast_methods.R | src/r/tests/test_forecast_methods.R | R | automated test | 440 | 440 | +0 | Unchanged |
| src/r/tests/test_import_m4comp2018.R | src/r/tests/test_import_m4comp2018.R | R | automated test | 97 | 97 | +0 | Unchanged |
| src/r/tests/test_transformations.R | src/r/tests/test_transformations.R | R | automated test | 118 | 118 | +0 | Unchanged |
| src/r/util/forecast_methods.R | src/r/util/forecast_methods.R | R | production/support | 797 | 797 | +0 | Unchanged |
| src/r/util/labels.R | src/r/util/labels.R | R | production/support | 21 | 21 | +0 | Unchanged |
| src/r/util/seasonal_period.R | src/r/util/seasonal_period.R | R | production/support | 79 | 79 | +0 | Unchanged |
| src/r/util/time_series_input.R | src/r/util/time_series_input.R | R | production/support | 57 | 57 | +0 | Unchanged |
| src/r/util/transformations.R | src/r/util/transformations.R | R | production/support | 188 | 188 | +0 | Unchanged |
| src/r/util/window_preparation.R | src/r/util/window_preparation.R | R | production/support | 30 | 30 | +0 | Unchanged |

| Language and role | Scripts before | Scripts after | Lines before | Lines after | Line change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Python — production/support | 39 | 39 | 17789 | 18124 | +335 |
| Python — automated test | 23 | 24 | 8489 | 8825 | +336 |
| Python — manual QA | 0 | 0 | 0 | 0 | +0 |
| R — production/support | 11 | 11 | 1801 | 1801 | +0 |
| R — automated test | 3 | 3 | 655 | 655 | +0 |
| R — manual QA | 4 | 4 | 936 | 936 | +0 |
| Python subtotal | 62 | 63 | 26278 | 26949 | +671 |
| R subtotal | 18 | 18 | 3392 | 3392 | +0 |
| production/support subtotal | 50 | 50 | 19590 | 19925 | +335 |
| automated test subtotal | 26 | 27 | 9144 | 9480 | +336 |
| manual QA subtotal | 4 | 4 | 936 | 936 | +0 |
| All scripts | 80 | 81 | 29670 | 30341 | +671 |

## Stage 1 closure follow-up handoff

Implementation is ready for ChatGPT technical review and researcher independent
QA, **not Stage 1 acceptance**. This continues the reviewed correction snapshot;
the earlier 128-Mac/60-Ubuntu test evidence and all preceding snapshots remain
intact. No Stage 2, spreadsheet update, commit or push was performed.

### Findings, implementation and verification limits

| Finding | Current result | Evidence / limitation |
| --- | --- | --- |
| Profile/lifecycle reconciliation | Implemented and bounded-route verified | Runtime v3 has 4 GiB GPU headroom; one profile-derived topology supplies launch and validation counts. CPU-only work launches 23 workers; pending GPU work launches 38 (23 CPU + 15 logical GPU). Exact tuning version/fingerprint checks remain. Historical experiment hashes/settings are unchanged; effective settings and fingerprint are execution-event metadata. |
| Shared-GPU protection | Implemented; controlled faults and real startup/inference verified | Existing monitor checks host memory, swap and GPU headroom during owned child work. Startup-only process lock takes fresh samples under lock and after readiness; release precedes inference. Tests cover pressure/probe failure, child-registration race, release, startup failure and OOM replacement. Models close after protected batches. This is reactive protection, not a predictive per-model memory reservation or a guarantee against instantaneous OOM. |
| AutoARIMA-specific concurrency | Implemented and controlled-test verified | Eight submitted/retrying AutoARIMA futures maximum, independent of the total 23 bound. Existing R admission/monitoring remains. Ordinary R requests CPU plus the existing Ubuntu `AUTOARIMA_R_SLOT`, consistent with tuning's large-fit placement. No new worker cap or relaxed memory budget. |
| Blocked-GPU refill | Implemented and regression verified | Two slots, three CPU batches and two blocked GPU batches: first CPU waits for GPU startup; third CPU releases GPU. CPU progresses beyond the first two without exceeding the cap. Existing no-overlap/failure/drain checks pass; approved runtime overlap remains false. |
| Normal entry/preflight | Verified on Mac/Ubuntu | Actual matching uncommitted source, locks and dependencies accepted; mismatches still rejected. Public main invokes configuration, Prefect, Dask, native providers, Mac storage and common results retrieval. Historical v4 GPU-name omission is supported without dropping CUDA validation. |
| Retry, stored-data and object-boundary corrections | Retained and regression verified | Effective retry settings, corrupt/missing predecessor rejection, cohesive provider operation and import-owned transactions remain covered. Main stays 35 lines. No second scheduler, retry owner or scientific implementation was added. |
| Real CPU work on both hosts | **Unresolved acceptance criterion** | Mac available memory was approximately 5–6 GiB, below the preserved 12 GiB fit budget + 3 GiB floor. The initial Mac fit waited safely without starting R; only that test-owned future was cancelled. Final native R and Chronos jobs ran on Ubuntu; Mac coordinated and wrote DuckDB. Registered Mac workers are not evidence of native CPU overlap. |

The closure extends existing `ExecutionProfile`, `ManagedTuningCluster`,
`ForecastSafetyPolicy`, `TuningMemoryMonitor`, `PersistentChronosWorker` and
`ProcessStorage` responsibilities rather than introducing another infrastructure
layer. `ProcessStorage.forecast_requires_gpu` derives workload need from expected
and pending work, including missing tasks. The architecture's object map and
entry/import/forecast compositions remain the review map. Small test fixtures,
fault functions and one-off evidence/synchronisation scripts use functions:
their bounded duties do not benefit from additional classes. This is the approved
practical exception, not a procedural production-pipeline exception.

### Source identity and bounded test evidence

Evidence root: `.amp/in/artifacts/workflow-stage1-closure/` (local, excluded from
Git). `mac-runtime.json` and `ubuntu-runtime.json` contain the identical 104-file
runtime/configuration/lock manifest. Its canonical fingerprint is
`9de58e3ba23921fb834773efa4fa5f93cd0339dd8049ad6c43aa06e3a076c4f0`.
The source is uncommitted; a Git revision alone does not identify this snapshot.
`stage1_closure_sync.py` compared remote bytes with the last verified manifest,
rejected conflicts and transferred only authorised changed runtime files. No
environment, data or result tree was copied. Transfer-generated AppleDouble
sidecars were identified and removed; subsequent transfers suppress xattrs.

Both hosts reported Python 3.12.14, Prefect 3.8.7, prefect-dask 0.3.7,
Dask/distributed 2026.8.0 and DuckDB 1.5.5. Normal worker preflight checked native
dependencies, including R 4.6.1, forecast 8.24.0 and jsonlite 2.0.0, and the GPU
worker CUDA/device contract. Environment reports and entry logs retain details.

| Check | Result | Artifact |
| --- | --- | --- |
| Final Mac regression (nine modules below) | 144 passed, 132.995 seconds | `final-regression.log` |
| Ubuntu process-storage/import/safety/flow/execution regressions | 75 passed, 35.626 seconds | `ubuntu-focused.log` |
| Safety coverage within those suites | 14 tests, including spawned-process startup-lock serialization with permitted inference overlap and simulated pressure | `test_forecast_safety.py`, test logs |
| Public entry, injected post-commit failure, same-experiment resume, second resume | Exit codes 1, 0, 0; second resume preserves forecasts and attempts | `entry-outcome.json`, `entry-*-snapshot.json`; repeated on final routing in `routing-final/` |
| Common public retrieval of four real forecasts | Passed | `retrieval-0.json` through `retrieval-3.json` |
| Sequential same-input native comparison | Four jobs, 616 values; maximum absolute difference 0; declared rtol=1e-5 and atol=1e-5 | `sequential-native.json`, `sequential-comparison.json` |
| Final routing fixture preservation | All 800 content hashes equal prior completed fixture; 798 retained rows keep hashes/timestamps; scientific/configuration hashes unchanged | `final-verification.json` |

Mac regression command (run from repository root):

```bash
PYTHONPATH=src/python .venv/bin/python -m unittest \
  tests.test_main tests.test_configuration tests.test_process_storage \
  tests.test_import tests.test_experiment_execution tests.test_forecast_flow \
  tests.test_forecast_safety tests.test_execution tests.test_workflow_orchestration
```

The normal-route fixture is a disposable copy of valid Gates 1–3 in
`.amp/in/workflow-stage1-closure.duckdb`. It preserves the production 100-series
configuration and validation contracts; **796 of its 800 forecast rows are
explicitly synthetic fixture rows**, seeded through storage to leave only two
AutoARIMA and two Chronos jobs. Real jobs use 97 observations and horizon 14.
This is four-job integration evidence, not an 800-forecast scientific campaign.
No compatible completed historical AutoARIMA+Chronos database could be reused
without changing its contract. Accepted databases/results were not modified.

The public-entry harness `.amp/in/stage1_closure_entry.py` owns a private-LAN
Prefect service and invokes the real `00_main.main`/CLI. Its only injection
raises once after a real Mac commit to simulate a lost acknowledgement; it does
not substitute providers, flow, configuration or validation. Each entry invocation
has a 1,200-second harness timeout and the effective profile's zero task retries.
The sequential reference uses the same four inputs and native adapters on Ubuntu,
with safeguards retained and only execution provenance labelled sequential.

After the final routing/pending-GPU correction, a separate disposable copy
`.amp/in/workflow-stage1-closure-routing.duckdb` retained 798 rows and removed
only one R and one GPU fixture result for the affected rerun. The same public
fault/resume/second-resume sequence passed. Needed-GPU runs recorded 38 workers;
the completed second resume recorded 23 CPU workers and zero launched GPU
workers. No full seasonal, full M4 or 100-series forecasting experiment was run.
Pre-model launcher and historical-GPU-field failures remain under
`launch-failure/` and `historical-gpu-field-failure/`, not counted as successes.

The new interruption evidence is **post-commit lost acknowledgement**, not a
fresh scheduler-kill recovery test. Earlier scheduler-loss evidence remains
supporting evidence with its original scope; it is not relabelled as a new
normal-route scheduler-kill result. Controlled concurrency tests demonstrate
absence of an inference-long lock; the small real fixture does not establish
15 simultaneous resident models, inference throughput or peak-load safety.
All test-owned Dask/Chronos processes were absent on both hosts after completion.

### Independent manual QA without repeating model experiments

Inspect the manifests, logs, outcome/snapshot JSON and four retrieval artifacts
above first. Read-only commands below start no Prefect services or Dask workers:

```bash
.venv/bin/python src/python/00_main.py status \
  --database .amp/in/workflow-stage1-closure-routing.duckdb
.venv/bin/python src/python/00_main.py results \
  --database .amp/in/workflow-stage1-closure-routing.duckdb \
  --series-id 73 --variant-id variant/ba6d2937d756c45996be2661 \
  --candidate auto_arima
```

For an independently requested second-resume check, use `run --processes 4
--execution-profile poc2_seasonal_recovery` against that disposable routing
database, after normal availability/source checks. Expect no new native forecasts,
unchanged forecast hashes/timestamps and task attempts, plus a new execution
event with the 23-worker CPU-only topology. This still launches the managed CPU
cluster; do not describe it as read-only. Do not rerun the fault harness against
the completed fixture: it intentionally requires pending work. Recreating fault
evidence needs a new disposable copy and a bounded test plan, never deletion of
accepted research rows. The retained fixture/entry/sequential scripts document
the preparation and execution; they are review evidence, not new researcher APIs.

Review remaining CPU-overlap and scheduler-loss evidence gaps explicitly. Do not
lower resource floors, add an unapproved model, manufacture Mac activity or treat
worker registration as computation to close them. Stage 1 acceptance remains
with the researcher after ChatGPT review and independent QA.

### Complete closure script and line comparison

The following four complete comparisons retain the original 67-script baseline,
partial 69-script baseline, reviewed 80-script checkpoint and reviewed 81-script
correction snapshot. The final snapshot is **81 scripts / 31,112 physical lines**:
20,239 production/support, 9,937 automated tests and 936 manual QA. Relative to
the correction snapshot this is **+771 lines: +314 production and +457 tests**;
relative to the 80-script checkpoint +1,442, partial baseline +1,694 and original
baseline +2,599. This is growth, not an overall simplification claim.

Most closure production growth is shared-GPU admission/monitoring (+192 lines in
`distributed_execution.py`), profile/topology (+41), lifecycle (+25), bounded
refill/cap (+31) and pending-work/routing integration. Tests add pressure,
cross-process admission, topology, pending-work and asymmetric refill coverage.
No new production script was added in this closure. Earlier main/forecast
dispatch reductions and retained Stage 2 compatibility logic remain documented
above; no scientific logic moved into another language to hide line growth.

Counting reuses `.amp/in/stage1_inventory.py`: unique tracked and nonignored
first-party `.py`, `.R`, `.r` paths from `git ls-files --cached --others
--exclude-standard -z`; exclude external/vendor, renv/environments, data/results,
cache, tooling and generated `.amp` artifacts. Count newline bytes plus a final
unterminated line, including comments/docstrings/blanks; classify tests and QA
by path. The same source scope and baselines as earlier comparisons are used.
`before-scripts.tsv` SHA256:
`adb2426675391fc01e8ac3cf7bb9347cbc0c988be6a03ae459536d12b307b2a5`.
`final-scripts.tsv` SHA256:
`80b4458810b8e730bf43ac201dd037ddfb6b369b4baf78e0908fdb283be21fd7`.
The full generated tables are also retained as `script-comparison.md` in the
closure evidence directory; each table includes unchanged files and reconciled
language/role totals. Responsibility changes map to the architecture table and
the findings above rather than treating extracted modules as removed logic.

### Stage 1 compared with original67-script baseline

| Script before | Script after | Language | Role | Lines before | Lines after | Change | Status or reason |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| scripts/setup_r.R | scripts/setup_r.R | R | production/support | 150 | 150 | +0 | Unchanged |
| scripts/setup_support.py | scripts/setup_support.py | Python | production/support | 706 | 710 | +4 | Changed; see responsibility map |
| src/python/00_main.py | src/python/00_main.py | Python | production/support | 763 | 35 | -728 | Changed; see responsibility map |
| src/python/01_import.py | src/python/01_import.py | Python | production/support | 31 | 30 | -1 | Changed; see responsibility map |
| src/python/02_preprocess.py | src/python/02_preprocess.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/03_transform.py | src/python/03_transform.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/04_02_forecast_chronos.py | src/python/04_02_forecast_chronos.py | Python | production/support | 281 | 281 | +0 | Unchanged |
| src/python/04_03_forecast_m4_submission.py | src/python/04_03_forecast_m4_submission.py | Python | production/support | 29 | 29 | +0 | Unchanged |
| src/python/04_forecast.py | src/python/04_forecast.py | Python | production/support | 43 | 43 | +0 | Unchanged |
| src/python/05_combine.py | src/python/05_combine.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/06_01_evaluate_gift_eval.py | src/python/06_01_evaluate_gift_eval.py | Python | production/support | 462 | 462 | +0 | Unchanged |
| src/python/06_evaluate.py | src/python/06_evaluate.py | Python | production/support | 34 | 34 | +0 | Unchanged |
| src/python/tests/__init__.py | src/python/tests/__init__.py | Python | automated test | 10 | 10 | +0 | Unchanged |
| src/python/tests/acceptance.py | src/python/tests/acceptance.py | Python | automated test | 1784 | 1871 | +87 | Changed; see responsibility map |
| src/python/tests/integration/__init__.py | src/python/tests/integration/__init__.py | Python | automated test | 10 | 10 | +0 | Unchanged |
| src/python/tests/integration/test_gift_eval_evaluation.py | src/python/tests/integration/test_gift_eval_evaluation.py | Python | automated test | 189 | 189 | +0 | Unchanged |
| src/python/tests/integration/test_gift_eval_semantics.py | src/python/tests/integration/test_gift_eval_semantics.py | Python | automated test | 98 | 98 | +0 | Unchanged |
| src/python/tests/integration/test_import_pipeline.py | src/python/tests/integration/test_import_pipeline.py | Python | automated test | 151 | 151 | +0 | Unchanged |
| src/python/tests/integration/test_preprocessing_regression.py | src/python/tests/integration/test_preprocessing_regression.py | Python | automated test | 72 | 72 | +0 | Unchanged |
| src/python/tests/test_acceptance.py | src/python/tests/test_acceptance.py | Python | automated test | 457 | 457 | +0 | Unchanged |
| src/python/tests/test_configuration.py | src/python/tests/test_configuration.py | Python | automated test | 297 | 297 | +0 | Unchanged |
| src/python/tests/test_execution.py | src/python/tests/test_execution.py | Python | automated test | 893 | 1038 | +145 | Changed; see responsibility map |
| src/python/tests/test_experiment_execution.py | src/python/tests/test_experiment_execution.py | Python | automated test | 972 | 985 | +13 | Changed; see responsibility map |
| — | src/python/tests/test_forecast_flow.py | Python | automated test | 0 | 338 | +338 | Added; responsibility split or contract test |
| — | src/python/tests/test_forecast_safety.py | Python | automated test | 0 | 374 | +374 | Added; responsibility split or contract test |
| src/python/tests/test_gpu_concurrency_calibration.py | src/python/tests/test_gpu_concurrency_calibration.py | Python | automated test | 197 | 197 | +0 | Unchanged |
| src/python/tests/test_import.py | src/python/tests/test_import.py | Python | automated test | 400 | 467 | +67 | Changed; see responsibility map |
| src/python/tests/test_m4_reference.py | src/python/tests/test_m4_reference.py | Python | automated test | 354 | 354 | +0 | Unchanged |
| src/python/tests/test_main.py | src/python/tests/test_main.py | Python | automated test | 757 | 457 | -300 | Changed; see responsibility map |
| src/python/tests/test_preprocessing.py | src/python/tests/test_preprocessing.py | Python | automated test | 109 | 109 | +0 | Unchanged |
| — | src/python/tests/test_process_storage.py | Python | automated test | 0 | 135 | +135 | Added; responsibility split or contract test |
| src/python/tests/test_seasonal_period_tuning.py | src/python/tests/test_seasonal_period_tuning.py | Python | automated test | 124 | 124 | +0 | Unchanged |
| src/python/tests/test_setup.py | src/python/tests/test_setup.py | Python | automated test | 394 | 394 | +0 | Unchanged |
| src/python/tests/test_transformations.py | src/python/tests/test_transformations.py | Python | automated test | 258 | 258 | +0 | Unchanged |
| src/python/tests/test_window_preparation.py | src/python/tests/test_window_preparation.py | Python | automated test | 665 | 665 | +0 | Unchanged |
| — | src/python/tests/test_workflow_orchestration.py | Python | automated test | 0 | 232 | +232 | Added; responsibility split or contract test |
| src/python/util/__init__.py | src/python/util/__init__.py | Python | production/support | 10 | 10 | +0 | Unchanged |
| src/python/util/configuration.py | src/python/util/configuration.py | Python | production/support | 1043 | 1043 | +0 | Unchanged |
| src/python/util/database.py | src/python/util/database.py | Python | production/support | 962 | 962 | +0 | Unchanged |
| src/python/util/distributed_cluster.py | src/python/util/distributed_cluster.py | Python | production/support | 343 | 368 | +25 | Changed; see responsibility map |
| src/python/util/distributed_execution.py | src/python/util/distributed_execution.py | Python | production/support | 1288 | 1519 | +231 | Changed; see responsibility map |
| src/python/util/execution_calibration.py | src/python/util/execution_calibration.py | Python | production/support | 1141 | 1141 | +0 | Unchanged |
| — | src/python/util/execution_event_storage.py | Python | production/support | 0 | 125 | +125 | Added; responsibility split or contract test |
| src/python/util/execution_profiles.py | src/python/util/execution_profiles.py | Python | production/support | 564 | 605 | +41 | Changed; see responsibility map |
| src/python/util/experiment_execution.py | src/python/util/experiment_execution.py | Python | production/support | 3126 | 2765 | -361 | Changed; see responsibility map |
| src/python/util/forecast_combination.py | src/python/util/forecast_combination.py | Python | production/support | 52 | 52 | +0 | Unchanged |
| — | src/python/util/forecast_flow.py | Python | production/support | 0 | 174 | +174 | Added; responsibility split or contract test |
| — | src/python/util/forecast_provider.py | Python | production/support | 0 | 271 | +271 | Added; responsibility split or contract test |
| — | src/python/util/forecast_storage.py | Python | production/support | 0 | 147 | +147 | Added; responsibility split or contract test |
| src/python/util/gift_eval_acquisition.py | src/python/util/gift_eval_acquisition.py | Python | production/support | 294 | 294 | +0 | Unchanged |
| src/python/util/gift_eval_source.py | src/python/util/gift_eval_source.py | Python | production/support | 150 | 209 | +59 | Changed; see responsibility map |
| src/python/util/gpu_concurrency_calibration.py | src/python/util/gpu_concurrency_calibration.py | Python | production/support | 1386 | 1386 | +0 | Unchanged |
| src/python/util/import_execution.py | src/python/util/import_execution.py | Python | production/support | 986 | 1144 | +158 | Changed; see responsibility map |
| — | src/python/util/import_flow.py | Python | production/support | 0 | 54 | +54 | Added; responsibility split or contract test |
| src/python/util/m4_submission.py | src/python/util/m4_submission.py | Python | production/support | 121 | 121 | +0 | Unchanged |
| — | src/python/util/process_storage.py | Python | production/support | 0 | 304 | +304 | Added; responsibility split or contract test |
| src/python/util/provenance.py | src/python/util/provenance.py | Python | production/support | 55 | 55 | +0 | Unchanged |
| — | src/python/util/researcher_actions.py | Python | production/support | 0 | 534 | +534 | Added; responsibility split or contract test |
| — | src/python/util/researcher_cli.py | Python | production/support | 0 | 148 | +148 | Added; responsibility split or contract test |
| — | src/python/util/researcher_request.py | Python | production/support | 0 | 24 | +24 | Added; responsibility split or contract test |
| src/python/util/seasonal_period_tuning.py | src/python/util/seasonal_period_tuning.py | Python | production/support | 1614 | 1614 | +0 | Unchanged |
| src/python/util/transformations.py | src/python/util/transformations.py | Python | production/support | 263 | 263 | +0 | Unchanged |
| src/python/util/window_preparation.py | src/python/util/window_preparation.py | Python | production/support | 1087 | 1098 | +11 | Changed; see responsibility map |
| — | src/python/util/workflow_orchestration.py | Python | production/support | 0 | 288 | +288 | Added; responsibility split or contract test |
| src/r/01_02_import_m4comp2018.R | src/r/01_02_import_m4comp2018.R | R | production/support | 209 | 209 | +0 | Unchanged |
| src/r/02_01_preprocess_series.R | src/r/02_01_preprocess_series.R | R | production/support | 81 | 81 | +0 | Unchanged |
| src/r/04_01_forecast_auto_arima.R | src/r/04_01_forecast_auto_arima.R | R | production/support | 86 | 86 | +0 | Unchanged |
| src/r/04_01_forecast_r_methods.R | src/r/04_01_forecast_r_methods.R | R | production/support | 103 | 103 | +0 | Unchanged |
| src/r/qa/get_m4_daily_series.R | src/r/qa/get_m4_daily_series.R | R | manual QA | 300 | 300 | +0 | Unchanged |
| src/r/qa/inspect_rolling_window.R | src/r/qa/inspect_rolling_window.R | R | manual QA | 277 | 277 | +0 | Unchanged |
| src/r/qa/inspect_seasonal_period_tuning.R | src/r/qa/inspect_seasonal_period_tuning.R | R | manual QA | 149 | 149 | +0 | Unchanged |
| src/r/qa/inspect_standardisation.R | src/r/qa/inspect_standardisation.R | R | manual QA | 210 | 210 | +0 | Unchanged |
| src/r/tests/test_forecast_methods.R | src/r/tests/test_forecast_methods.R | R | automated test | 440 | 440 | +0 | Unchanged |
| src/r/tests/test_import_m4comp2018.R | src/r/tests/test_import_m4comp2018.R | R | automated test | 97 | 97 | +0 | Unchanged |
| src/r/tests/test_transformations.R | src/r/tests/test_transformations.R | R | automated test | 118 | 118 | +0 | Unchanged |
| src/r/util/forecast_methods.R | src/r/util/forecast_methods.R | R | production/support | 797 | 797 | +0 | Unchanged |
| src/r/util/labels.R | src/r/util/labels.R | R | production/support | 21 | 21 | +0 | Unchanged |
| src/r/util/seasonal_period.R | src/r/util/seasonal_period.R | R | production/support | 79 | 79 | +0 | Unchanged |
| src/r/util/time_series_input.R | src/r/util/time_series_input.R | R | production/support | 57 | 57 | +0 | Unchanged |
| src/r/util/transformations.R | src/r/util/transformations.R | R | production/support | 188 | 188 | +0 | Unchanged |
| src/r/util/window_preparation.R | src/r/util/window_preparation.R | R | production/support | 30 | 30 | +0 | Unchanged |

| Language and role | Scripts before | Scripts after | Lines before | Lines after | Line change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Python — production/support | 29 | 39 | 16930 | 18438 | +1508 |
| Python — automated test | 20 | 24 | 8191 | 9282 | +1091 |
| Python — manual QA | 0 | 0 | 0 | 0 | +0 |
| R — production/support | 11 | 11 | 1801 | 1801 | +0 |
| R — automated test | 3 | 3 | 655 | 655 | +0 |
| R — manual QA | 4 | 4 | 936 | 936 | +0 |
| Python subtotal | 49 | 63 | 25121 | 27720 | +2599 |
| R subtotal | 18 | 18 | 3392 | 3392 | +0 |
| production/support subtotal | 40 | 50 | 18731 | 20239 | +1508 |
| automated test subtotal | 23 | 27 | 8846 | 9937 | +1091 |
| manual QA subtotal | 4 | 4 | 936 | 936 | +0 |
| All scripts | 67 | 81 | 28513 | 31112 | +2599 |

### Stage 1 compared with partial69-script baseline

| Script before | Script after | Language | Role | Lines before | Lines after | Change | Status or reason |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| scripts/setup_r.R | scripts/setup_r.R | R | production/support | 150 | 150 | +0 | Unchanged |
| scripts/setup_support.py | scripts/setup_support.py | Python | production/support | 710 | 710 | +0 | Unchanged |
| src/python/00_main.py | src/python/00_main.py | Python | production/support | 1013 | 35 | -978 | Changed; see responsibility map |
| src/python/01_import.py | src/python/01_import.py | Python | production/support | 31 | 30 | -1 | Changed; see responsibility map |
| src/python/02_preprocess.py | src/python/02_preprocess.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/03_transform.py | src/python/03_transform.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/04_02_forecast_chronos.py | src/python/04_02_forecast_chronos.py | Python | production/support | 281 | 281 | +0 | Unchanged |
| src/python/04_03_forecast_m4_submission.py | src/python/04_03_forecast_m4_submission.py | Python | production/support | 29 | 29 | +0 | Unchanged |
| src/python/04_forecast.py | src/python/04_forecast.py | Python | production/support | 43 | 43 | +0 | Unchanged |
| src/python/05_combine.py | src/python/05_combine.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/06_01_evaluate_gift_eval.py | src/python/06_01_evaluate_gift_eval.py | Python | production/support | 462 | 462 | +0 | Unchanged |
| src/python/06_evaluate.py | src/python/06_evaluate.py | Python | production/support | 34 | 34 | +0 | Unchanged |
| src/python/tests/__init__.py | src/python/tests/__init__.py | Python | automated test | 10 | 10 | +0 | Unchanged |
| src/python/tests/acceptance.py | src/python/tests/acceptance.py | Python | automated test | 1871 | 1871 | +0 | Unchanged |
| src/python/tests/integration/__init__.py | src/python/tests/integration/__init__.py | Python | automated test | 10 | 10 | +0 | Unchanged |
| src/python/tests/integration/test_gift_eval_evaluation.py | src/python/tests/integration/test_gift_eval_evaluation.py | Python | automated test | 189 | 189 | +0 | Unchanged |
| src/python/tests/integration/test_gift_eval_semantics.py | src/python/tests/integration/test_gift_eval_semantics.py | Python | automated test | 98 | 98 | +0 | Unchanged |
| src/python/tests/integration/test_import_pipeline.py | src/python/tests/integration/test_import_pipeline.py | Python | automated test | 151 | 151 | +0 | Unchanged |
| src/python/tests/integration/test_preprocessing_regression.py | src/python/tests/integration/test_preprocessing_regression.py | Python | automated test | 72 | 72 | +0 | Unchanged |
| src/python/tests/test_acceptance.py | src/python/tests/test_acceptance.py | Python | automated test | 457 | 457 | +0 | Unchanged |
| src/python/tests/test_configuration.py | src/python/tests/test_configuration.py | Python | automated test | 297 | 297 | +0 | Unchanged |
| src/python/tests/test_execution.py | src/python/tests/test_execution.py | Python | automated test | 893 | 1038 | +145 | Changed; see responsibility map |
| src/python/tests/test_experiment_execution.py | src/python/tests/test_experiment_execution.py | Python | automated test | 972 | 985 | +13 | Changed; see responsibility map |
| — | src/python/tests/test_forecast_flow.py | Python | automated test | 0 | 338 | +338 | Added; responsibility split or contract test |
| — | src/python/tests/test_forecast_safety.py | Python | automated test | 0 | 374 | +374 | Added; responsibility split or contract test |
| src/python/tests/test_gpu_concurrency_calibration.py | src/python/tests/test_gpu_concurrency_calibration.py | Python | automated test | 197 | 197 | +0 | Unchanged |
| src/python/tests/test_import.py | src/python/tests/test_import.py | Python | automated test | 400 | 467 | +67 | Changed; see responsibility map |
| src/python/tests/test_m4_reference.py | src/python/tests/test_m4_reference.py | Python | automated test | 354 | 354 | +0 | Unchanged |
| src/python/tests/test_main.py | src/python/tests/test_main.py | Python | automated test | 790 | 457 | -333 | Changed; see responsibility map |
| src/python/tests/test_preprocessing.py | src/python/tests/test_preprocessing.py | Python | automated test | 109 | 109 | +0 | Unchanged |
| — | src/python/tests/test_process_storage.py | Python | automated test | 0 | 135 | +135 | Added; responsibility split or contract test |
| src/python/tests/test_seasonal_period_tuning.py | src/python/tests/test_seasonal_period_tuning.py | Python | automated test | 124 | 124 | +0 | Unchanged |
| src/python/tests/test_setup.py | src/python/tests/test_setup.py | Python | automated test | 394 | 394 | +0 | Unchanged |
| src/python/tests/test_transformations.py | src/python/tests/test_transformations.py | Python | automated test | 258 | 258 | +0 | Unchanged |
| src/python/tests/test_window_preparation.py | src/python/tests/test_window_preparation.py | Python | automated test | 665 | 665 | +0 | Unchanged |
| src/python/tests/test_workflow_orchestration.py | src/python/tests/test_workflow_orchestration.py | Python | automated test | 232 | 232 | +0 | Unchanged |
| src/python/util/__init__.py | src/python/util/__init__.py | Python | production/support | 10 | 10 | +0 | Unchanged |
| src/python/util/configuration.py | src/python/util/configuration.py | Python | production/support | 1043 | 1043 | +0 | Unchanged |
| src/python/util/database.py | src/python/util/database.py | Python | production/support | 962 | 962 | +0 | Unchanged |
| src/python/util/distributed_cluster.py | src/python/util/distributed_cluster.py | Python | production/support | 343 | 368 | +25 | Changed; see responsibility map |
| src/python/util/distributed_execution.py | src/python/util/distributed_execution.py | Python | production/support | 1288 | 1519 | +231 | Changed; see responsibility map |
| src/python/util/execution_calibration.py | src/python/util/execution_calibration.py | Python | production/support | 1141 | 1141 | +0 | Unchanged |
| — | src/python/util/execution_event_storage.py | Python | production/support | 0 | 125 | +125 | Added; responsibility split or contract test |
| src/python/util/execution_profiles.py | src/python/util/execution_profiles.py | Python | production/support | 564 | 605 | +41 | Changed; see responsibility map |
| src/python/util/experiment_execution.py | src/python/util/experiment_execution.py | Python | production/support | 3126 | 2765 | -361 | Changed; see responsibility map |
| src/python/util/forecast_combination.py | src/python/util/forecast_combination.py | Python | production/support | 52 | 52 | +0 | Unchanged |
| — | src/python/util/forecast_flow.py | Python | production/support | 0 | 174 | +174 | Added; responsibility split or contract test |
| — | src/python/util/forecast_provider.py | Python | production/support | 0 | 271 | +271 | Added; responsibility split or contract test |
| — | src/python/util/forecast_storage.py | Python | production/support | 0 | 147 | +147 | Added; responsibility split or contract test |
| src/python/util/gift_eval_acquisition.py | src/python/util/gift_eval_acquisition.py | Python | production/support | 294 | 294 | +0 | Unchanged |
| src/python/util/gift_eval_source.py | src/python/util/gift_eval_source.py | Python | production/support | 150 | 209 | +59 | Changed; see responsibility map |
| src/python/util/gpu_concurrency_calibration.py | src/python/util/gpu_concurrency_calibration.py | Python | production/support | 1386 | 1386 | +0 | Unchanged |
| src/python/util/import_execution.py | src/python/util/import_execution.py | Python | production/support | 986 | 1144 | +158 | Changed; see responsibility map |
| — | src/python/util/import_flow.py | Python | production/support | 0 | 54 | +54 | Added; responsibility split or contract test |
| src/python/util/m4_submission.py | src/python/util/m4_submission.py | Python | production/support | 121 | 121 | +0 | Unchanged |
| — | src/python/util/process_storage.py | Python | production/support | 0 | 304 | +304 | Added; responsibility split or contract test |
| src/python/util/provenance.py | src/python/util/provenance.py | Python | production/support | 55 | 55 | +0 | Unchanged |
| — | src/python/util/researcher_actions.py | Python | production/support | 0 | 534 | +534 | Added; responsibility split or contract test |
| — | src/python/util/researcher_cli.py | Python | production/support | 0 | 148 | +148 | Added; responsibility split or contract test |
| — | src/python/util/researcher_request.py | Python | production/support | 0 | 24 | +24 | Added; responsibility split or contract test |
| src/python/util/seasonal_period_tuning.py | src/python/util/seasonal_period_tuning.py | Python | production/support | 1614 | 1614 | +0 | Unchanged |
| src/python/util/transformations.py | src/python/util/transformations.py | Python | production/support | 263 | 263 | +0 | Unchanged |
| src/python/util/window_preparation.py | src/python/util/window_preparation.py | Python | production/support | 1098 | 1098 | +0 | Unchanged |
| src/python/util/workflow_orchestration.py | src/python/util/workflow_orchestration.py | Python | production/support | 288 | 288 | +0 | Changed; see responsibility map |
| src/r/01_02_import_m4comp2018.R | src/r/01_02_import_m4comp2018.R | R | production/support | 209 | 209 | +0 | Unchanged |
| src/r/02_01_preprocess_series.R | src/r/02_01_preprocess_series.R | R | production/support | 81 | 81 | +0 | Unchanged |
| src/r/04_01_forecast_auto_arima.R | src/r/04_01_forecast_auto_arima.R | R | production/support | 86 | 86 | +0 | Unchanged |
| src/r/04_01_forecast_r_methods.R | src/r/04_01_forecast_r_methods.R | R | production/support | 103 | 103 | +0 | Unchanged |
| src/r/qa/get_m4_daily_series.R | src/r/qa/get_m4_daily_series.R | R | manual QA | 300 | 300 | +0 | Unchanged |
| src/r/qa/inspect_rolling_window.R | src/r/qa/inspect_rolling_window.R | R | manual QA | 277 | 277 | +0 | Unchanged |
| src/r/qa/inspect_seasonal_period_tuning.R | src/r/qa/inspect_seasonal_period_tuning.R | R | manual QA | 149 | 149 | +0 | Unchanged |
| src/r/qa/inspect_standardisation.R | src/r/qa/inspect_standardisation.R | R | manual QA | 210 | 210 | +0 | Unchanged |
| src/r/tests/test_forecast_methods.R | src/r/tests/test_forecast_methods.R | R | automated test | 440 | 440 | +0 | Unchanged |
| src/r/tests/test_import_m4comp2018.R | src/r/tests/test_import_m4comp2018.R | R | automated test | 97 | 97 | +0 | Unchanged |
| src/r/tests/test_transformations.R | src/r/tests/test_transformations.R | R | automated test | 118 | 118 | +0 | Unchanged |
| src/r/util/forecast_methods.R | src/r/util/forecast_methods.R | R | production/support | 797 | 797 | +0 | Unchanged |
| src/r/util/labels.R | src/r/util/labels.R | R | production/support | 21 | 21 | +0 | Unchanged |
| src/r/util/seasonal_period.R | src/r/util/seasonal_period.R | R | production/support | 79 | 79 | +0 | Unchanged |
| src/r/util/time_series_input.R | src/r/util/time_series_input.R | R | production/support | 57 | 57 | +0 | Unchanged |
| src/r/util/transformations.R | src/r/util/transformations.R | R | production/support | 188 | 188 | +0 | Unchanged |
| src/r/util/window_preparation.R | src/r/util/window_preparation.R | R | production/support | 30 | 30 | +0 | Unchanged |

| Language and role | Scripts before | Scripts after | Lines before | Lines after | Line change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Python — production/support | 30 | 39 | 17483 | 18438 | +955 |
| Python — automated test | 21 | 24 | 8543 | 9282 | +739 |
| Python — manual QA | 0 | 0 | 0 | 0 | +0 |
| R — production/support | 11 | 11 | 1801 | 1801 | +0 |
| R — automated test | 3 | 3 | 655 | 655 | +0 |
| R — manual QA | 4 | 4 | 936 | 936 | +0 |
| Python subtotal | 51 | 63 | 26026 | 27720 | +1694 |
| R subtotal | 18 | 18 | 3392 | 3392 | +0 |
| production/support subtotal | 41 | 50 | 19284 | 20239 | +955 |
| automated test subtotal | 24 | 27 | 9198 | 9937 | +739 |
| manual QA subtotal | 4 | 4 | 936 | 936 | +0 |
| All scripts | 69 | 81 | 29418 | 31112 | +1694 |

### Stage 1 compared with reviewed80-script baseline

| Script before | Script after | Language | Role | Lines before | Lines after | Change | Status or reason |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| scripts/setup_r.R | scripts/setup_r.R | R | production/support | 150 | 150 | +0 | Unchanged |
| scripts/setup_support.py | scripts/setup_support.py | Python | production/support | 710 | 710 | +0 | Unchanged |
| src/python/00_main.py | src/python/00_main.py | Python | production/support | 35 | 35 | +0 | Unchanged |
| src/python/01_import.py | src/python/01_import.py | Python | production/support | 30 | 30 | +0 | Unchanged |
| src/python/02_preprocess.py | src/python/02_preprocess.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/03_transform.py | src/python/03_transform.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/04_02_forecast_chronos.py | src/python/04_02_forecast_chronos.py | Python | production/support | 281 | 281 | +0 | Unchanged |
| src/python/04_03_forecast_m4_submission.py | src/python/04_03_forecast_m4_submission.py | Python | production/support | 29 | 29 | +0 | Unchanged |
| src/python/04_forecast.py | src/python/04_forecast.py | Python | production/support | 43 | 43 | +0 | Unchanged |
| src/python/05_combine.py | src/python/05_combine.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/06_01_evaluate_gift_eval.py | src/python/06_01_evaluate_gift_eval.py | Python | production/support | 462 | 462 | +0 | Unchanged |
| src/python/06_evaluate.py | src/python/06_evaluate.py | Python | production/support | 34 | 34 | +0 | Unchanged |
| src/python/tests/__init__.py | src/python/tests/__init__.py | Python | automated test | 10 | 10 | +0 | Unchanged |
| src/python/tests/acceptance.py | src/python/tests/acceptance.py | Python | automated test | 1871 | 1871 | +0 | Unchanged |
| src/python/tests/integration/__init__.py | src/python/tests/integration/__init__.py | Python | automated test | 10 | 10 | +0 | Unchanged |
| src/python/tests/integration/test_gift_eval_evaluation.py | src/python/tests/integration/test_gift_eval_evaluation.py | Python | automated test | 189 | 189 | +0 | Unchanged |
| src/python/tests/integration/test_gift_eval_semantics.py | src/python/tests/integration/test_gift_eval_semantics.py | Python | automated test | 98 | 98 | +0 | Unchanged |
| src/python/tests/integration/test_import_pipeline.py | src/python/tests/integration/test_import_pipeline.py | Python | automated test | 151 | 151 | +0 | Unchanged |
| src/python/tests/integration/test_preprocessing_regression.py | src/python/tests/integration/test_preprocessing_regression.py | Python | automated test | 72 | 72 | +0 | Unchanged |
| src/python/tests/test_acceptance.py | src/python/tests/test_acceptance.py | Python | automated test | 457 | 457 | +0 | Unchanged |
| src/python/tests/test_configuration.py | src/python/tests/test_configuration.py | Python | automated test | 297 | 297 | +0 | Unchanged |
| src/python/tests/test_execution.py | src/python/tests/test_execution.py | Python | automated test | 893 | 1038 | +145 | Changed; see responsibility map |
| src/python/tests/test_experiment_execution.py | src/python/tests/test_experiment_execution.py | Python | automated test | 985 | 985 | +0 | Unchanged |
| src/python/tests/test_forecast_flow.py | src/python/tests/test_forecast_flow.py | Python | automated test | 197 | 338 | +141 | Changed; see responsibility map |
| — | src/python/tests/test_forecast_safety.py | Python | automated test | 0 | 374 | +374 | Added; responsibility split or contract test |
| src/python/tests/test_gpu_concurrency_calibration.py | src/python/tests/test_gpu_concurrency_calibration.py | Python | automated test | 197 | 197 | +0 | Unchanged |
| src/python/tests/test_import.py | src/python/tests/test_import.py | Python | automated test | 429 | 467 | +38 | Changed; see responsibility map |
| src/python/tests/test_m4_reference.py | src/python/tests/test_m4_reference.py | Python | automated test | 354 | 354 | +0 | Unchanged |
| src/python/tests/test_main.py | src/python/tests/test_main.py | Python | automated test | 437 | 457 | +20 | Changed; see responsibility map |
| src/python/tests/test_preprocessing.py | src/python/tests/test_preprocessing.py | Python | automated test | 109 | 109 | +0 | Unchanged |
| src/python/tests/test_process_storage.py | src/python/tests/test_process_storage.py | Python | automated test | 60 | 135 | +75 | Changed; see responsibility map |
| src/python/tests/test_seasonal_period_tuning.py | src/python/tests/test_seasonal_period_tuning.py | Python | automated test | 124 | 124 | +0 | Unchanged |
| src/python/tests/test_setup.py | src/python/tests/test_setup.py | Python | automated test | 394 | 394 | +0 | Unchanged |
| src/python/tests/test_transformations.py | src/python/tests/test_transformations.py | Python | automated test | 258 | 258 | +0 | Unchanged |
| src/python/tests/test_window_preparation.py | src/python/tests/test_window_preparation.py | Python | automated test | 665 | 665 | +0 | Unchanged |
| src/python/tests/test_workflow_orchestration.py | src/python/tests/test_workflow_orchestration.py | Python | automated test | 232 | 232 | +0 | Unchanged |
| src/python/util/__init__.py | src/python/util/__init__.py | Python | production/support | 10 | 10 | +0 | Unchanged |
| src/python/util/configuration.py | src/python/util/configuration.py | Python | production/support | 1043 | 1043 | +0 | Unchanged |
| src/python/util/database.py | src/python/util/database.py | Python | production/support | 962 | 962 | +0 | Unchanged |
| src/python/util/distributed_cluster.py | src/python/util/distributed_cluster.py | Python | production/support | 343 | 368 | +25 | Changed; see responsibility map |
| src/python/util/distributed_execution.py | src/python/util/distributed_execution.py | Python | production/support | 1288 | 1519 | +231 | Changed; see responsibility map |
| src/python/util/execution_calibration.py | src/python/util/execution_calibration.py | Python | production/support | 1141 | 1141 | +0 | Unchanged |
| src/python/util/execution_event_storage.py | src/python/util/execution_event_storage.py | Python | production/support | 125 | 125 | +0 | Unchanged |
| src/python/util/execution_profiles.py | src/python/util/execution_profiles.py | Python | production/support | 564 | 605 | +41 | Changed; see responsibility map |
| src/python/util/experiment_execution.py | src/python/util/experiment_execution.py | Python | production/support | 2756 | 2765 | +9 | Changed; see responsibility map |
| src/python/util/forecast_combination.py | src/python/util/forecast_combination.py | Python | production/support | 52 | 52 | +0 | Unchanged |
| src/python/util/forecast_flow.py | src/python/util/forecast_flow.py | Python | production/support | 120 | 174 | +54 | Changed; see responsibility map |
| src/python/util/forecast_provider.py | src/python/util/forecast_provider.py | Python | production/support | 209 | 271 | +62 | Changed; see responsibility map |
| src/python/util/forecast_storage.py | src/python/util/forecast_storage.py | Python | production/support | 147 | 147 | +0 | Unchanged |
| src/python/util/gift_eval_acquisition.py | src/python/util/gift_eval_acquisition.py | Python | production/support | 294 | 294 | +0 | Unchanged |
| src/python/util/gift_eval_source.py | src/python/util/gift_eval_source.py | Python | production/support | 209 | 209 | +0 | Unchanged |
| src/python/util/gpu_concurrency_calibration.py | src/python/util/gpu_concurrency_calibration.py | Python | production/support | 1386 | 1386 | +0 | Unchanged |
| src/python/util/import_execution.py | src/python/util/import_execution.py | Python | production/support | 1097 | 1144 | +47 | Changed; see responsibility map |
| src/python/util/import_flow.py | src/python/util/import_flow.py | Python | production/support | 52 | 54 | +2 | Changed; see responsibility map |
| src/python/util/m4_submission.py | src/python/util/m4_submission.py | Python | production/support | 121 | 121 | +0 | Unchanged |
| src/python/util/process_storage.py | src/python/util/process_storage.py | Python | production/support | 151 | 304 | +153 | Changed; see responsibility map |
| src/python/util/provenance.py | src/python/util/provenance.py | Python | production/support | 55 | 55 | +0 | Unchanged |
| src/python/util/researcher_actions.py | src/python/util/researcher_actions.py | Python | production/support | 509 | 534 | +25 | Changed; see responsibility map |
| src/python/util/researcher_cli.py | src/python/util/researcher_cli.py | Python | production/support | 148 | 148 | +0 | Unchanged |
| src/python/util/researcher_request.py | src/python/util/researcher_request.py | Python | production/support | 24 | 24 | +0 | Unchanged |
| src/python/util/seasonal_period_tuning.py | src/python/util/seasonal_period_tuning.py | Python | production/support | 1614 | 1614 | +0 | Unchanged |
| src/python/util/transformations.py | src/python/util/transformations.py | Python | production/support | 263 | 263 | +0 | Unchanged |
| src/python/util/window_preparation.py | src/python/util/window_preparation.py | Python | production/support | 1098 | 1098 | +0 | Unchanged |
| src/python/util/workflow_orchestration.py | src/python/util/workflow_orchestration.py | Python | production/support | 288 | 288 | +0 | Unchanged |
| src/r/01_02_import_m4comp2018.R | src/r/01_02_import_m4comp2018.R | R | production/support | 209 | 209 | +0 | Unchanged |
| src/r/02_01_preprocess_series.R | src/r/02_01_preprocess_series.R | R | production/support | 81 | 81 | +0 | Unchanged |
| src/r/04_01_forecast_auto_arima.R | src/r/04_01_forecast_auto_arima.R | R | production/support | 86 | 86 | +0 | Unchanged |
| src/r/04_01_forecast_r_methods.R | src/r/04_01_forecast_r_methods.R | R | production/support | 103 | 103 | +0 | Unchanged |
| src/r/qa/get_m4_daily_series.R | src/r/qa/get_m4_daily_series.R | R | manual QA | 300 | 300 | +0 | Unchanged |
| src/r/qa/inspect_rolling_window.R | src/r/qa/inspect_rolling_window.R | R | manual QA | 277 | 277 | +0 | Unchanged |
| src/r/qa/inspect_seasonal_period_tuning.R | src/r/qa/inspect_seasonal_period_tuning.R | R | manual QA | 149 | 149 | +0 | Unchanged |
| src/r/qa/inspect_standardisation.R | src/r/qa/inspect_standardisation.R | R | manual QA | 210 | 210 | +0 | Unchanged |
| src/r/tests/test_forecast_methods.R | src/r/tests/test_forecast_methods.R | R | automated test | 440 | 440 | +0 | Unchanged |
| src/r/tests/test_import_m4comp2018.R | src/r/tests/test_import_m4comp2018.R | R | automated test | 97 | 97 | +0 | Unchanged |
| src/r/tests/test_transformations.R | src/r/tests/test_transformations.R | R | automated test | 118 | 118 | +0 | Unchanged |
| src/r/util/forecast_methods.R | src/r/util/forecast_methods.R | R | production/support | 797 | 797 | +0 | Unchanged |
| src/r/util/labels.R | src/r/util/labels.R | R | production/support | 21 | 21 | +0 | Unchanged |
| src/r/util/seasonal_period.R | src/r/util/seasonal_period.R | R | production/support | 79 | 79 | +0 | Unchanged |
| src/r/util/time_series_input.R | src/r/util/time_series_input.R | R | production/support | 57 | 57 | +0 | Unchanged |
| src/r/util/transformations.R | src/r/util/transformations.R | R | production/support | 188 | 188 | +0 | Unchanged |
| src/r/util/window_preparation.R | src/r/util/window_preparation.R | R | production/support | 30 | 30 | +0 | Unchanged |

| Language and role | Scripts before | Scripts after | Lines before | Lines after | Line change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Python — production/support | 39 | 39 | 17789 | 18438 | +649 |
| Python — automated test | 23 | 24 | 8489 | 9282 | +793 |
| Python — manual QA | 0 | 0 | 0 | 0 | +0 |
| R — production/support | 11 | 11 | 1801 | 1801 | +0 |
| R — automated test | 3 | 3 | 655 | 655 | +0 |
| R — manual QA | 4 | 4 | 936 | 936 | +0 |
| Python subtotal | 62 | 63 | 26278 | 27720 | +1442 |
| R subtotal | 18 | 18 | 3392 | 3392 | +0 |
| production/support subtotal | 50 | 50 | 19590 | 20239 | +649 |
| automated test subtotal | 26 | 27 | 9144 | 9937 | +793 |
| manual QA subtotal | 4 | 4 | 936 | 936 | +0 |
| All scripts | 80 | 81 | 29670 | 31112 | +1442 |

### Stage 1 compared with reviewed81-script correction snapshot

| Script before | Script after | Language | Role | Lines before | Lines after | Change | Status or reason |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| scripts/setup_r.R | scripts/setup_r.R | R | production/support | 150 | 150 | +0 | Unchanged |
| scripts/setup_support.py | scripts/setup_support.py | Python | production/support | 710 | 710 | +0 | Unchanged |
| src/python/00_main.py | src/python/00_main.py | Python | production/support | 35 | 35 | +0 | Unchanged |
| src/python/01_import.py | src/python/01_import.py | Python | production/support | 30 | 30 | +0 | Unchanged |
| src/python/02_preprocess.py | src/python/02_preprocess.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/03_transform.py | src/python/03_transform.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/04_02_forecast_chronos.py | src/python/04_02_forecast_chronos.py | Python | production/support | 281 | 281 | +0 | Unchanged |
| src/python/04_03_forecast_m4_submission.py | src/python/04_03_forecast_m4_submission.py | Python | production/support | 29 | 29 | +0 | Unchanged |
| src/python/04_forecast.py | src/python/04_forecast.py | Python | production/support | 43 | 43 | +0 | Unchanged |
| src/python/05_combine.py | src/python/05_combine.py | Python | production/support | 32 | 32 | +0 | Unchanged |
| src/python/06_01_evaluate_gift_eval.py | src/python/06_01_evaluate_gift_eval.py | Python | production/support | 462 | 462 | +0 | Unchanged |
| src/python/06_evaluate.py | src/python/06_evaluate.py | Python | production/support | 34 | 34 | +0 | Unchanged |
| src/python/tests/__init__.py | src/python/tests/__init__.py | Python | automated test | 10 | 10 | +0 | Unchanged |
| src/python/tests/acceptance.py | src/python/tests/acceptance.py | Python | automated test | 1871 | 1871 | +0 | Unchanged |
| src/python/tests/integration/__init__.py | src/python/tests/integration/__init__.py | Python | automated test | 10 | 10 | +0 | Unchanged |
| src/python/tests/integration/test_gift_eval_evaluation.py | src/python/tests/integration/test_gift_eval_evaluation.py | Python | automated test | 189 | 189 | +0 | Unchanged |
| src/python/tests/integration/test_gift_eval_semantics.py | src/python/tests/integration/test_gift_eval_semantics.py | Python | automated test | 98 | 98 | +0 | Unchanged |
| src/python/tests/integration/test_import_pipeline.py | src/python/tests/integration/test_import_pipeline.py | Python | automated test | 151 | 151 | +0 | Unchanged |
| src/python/tests/integration/test_preprocessing_regression.py | src/python/tests/integration/test_preprocessing_regression.py | Python | automated test | 72 | 72 | +0 | Unchanged |
| src/python/tests/test_acceptance.py | src/python/tests/test_acceptance.py | Python | automated test | 457 | 457 | +0 | Unchanged |
| src/python/tests/test_configuration.py | src/python/tests/test_configuration.py | Python | automated test | 297 | 297 | +0 | Unchanged |
| src/python/tests/test_execution.py | src/python/tests/test_execution.py | Python | automated test | 931 | 1038 | +107 | Changed; see responsibility map |
| src/python/tests/test_experiment_execution.py | src/python/tests/test_experiment_execution.py | Python | automated test | 985 | 985 | +0 | Unchanged |
| src/python/tests/test_forecast_flow.py | src/python/tests/test_forecast_flow.py | Python | automated test | 265 | 338 | +73 | Changed; see responsibility map |
| src/python/tests/test_forecast_safety.py | src/python/tests/test_forecast_safety.py | Python | automated test | 127 | 374 | +247 | Changed; see responsibility map |
| src/python/tests/test_gpu_concurrency_calibration.py | src/python/tests/test_gpu_concurrency_calibration.py | Python | automated test | 197 | 197 | +0 | Unchanged |
| src/python/tests/test_import.py | src/python/tests/test_import.py | Python | automated test | 467 | 467 | +0 | Unchanged |
| src/python/tests/test_m4_reference.py | src/python/tests/test_m4_reference.py | Python | automated test | 354 | 354 | +0 | Unchanged |
| src/python/tests/test_main.py | src/python/tests/test_main.py | Python | automated test | 437 | 457 | +20 | Changed; see responsibility map |
| src/python/tests/test_preprocessing.py | src/python/tests/test_preprocessing.py | Python | automated test | 109 | 109 | +0 | Unchanged |
| src/python/tests/test_process_storage.py | src/python/tests/test_process_storage.py | Python | automated test | 125 | 135 | +10 | Changed; see responsibility map |
| src/python/tests/test_seasonal_period_tuning.py | src/python/tests/test_seasonal_period_tuning.py | Python | automated test | 124 | 124 | +0 | Unchanged |
| src/python/tests/test_setup.py | src/python/tests/test_setup.py | Python | automated test | 394 | 394 | +0 | Unchanged |
| src/python/tests/test_transformations.py | src/python/tests/test_transformations.py | Python | automated test | 258 | 258 | +0 | Unchanged |
| src/python/tests/test_window_preparation.py | src/python/tests/test_window_preparation.py | Python | automated test | 665 | 665 | +0 | Unchanged |
| src/python/tests/test_workflow_orchestration.py | src/python/tests/test_workflow_orchestration.py | Python | automated test | 232 | 232 | +0 | Unchanged |
| src/python/util/__init__.py | src/python/util/__init__.py | Python | production/support | 10 | 10 | +0 | Unchanged |
| src/python/util/configuration.py | src/python/util/configuration.py | Python | production/support | 1043 | 1043 | +0 | Unchanged |
| src/python/util/database.py | src/python/util/database.py | Python | production/support | 962 | 962 | +0 | Unchanged |
| src/python/util/distributed_cluster.py | src/python/util/distributed_cluster.py | Python | production/support | 343 | 368 | +25 | Changed; see responsibility map |
| src/python/util/distributed_execution.py | src/python/util/distributed_execution.py | Python | production/support | 1327 | 1519 | +192 | Changed; see responsibility map |
| src/python/util/execution_calibration.py | src/python/util/execution_calibration.py | Python | production/support | 1141 | 1141 | +0 | Unchanged |
| src/python/util/execution_event_storage.py | src/python/util/execution_event_storage.py | Python | production/support | 125 | 125 | +0 | Unchanged |
| src/python/util/execution_profiles.py | src/python/util/execution_profiles.py | Python | production/support | 564 | 605 | +41 | Changed; see responsibility map |
| src/python/util/experiment_execution.py | src/python/util/experiment_execution.py | Python | production/support | 2762 | 2765 | +3 | Changed; see responsibility map |
| src/python/util/forecast_combination.py | src/python/util/forecast_combination.py | Python | production/support | 52 | 52 | +0 | Unchanged |
| src/python/util/forecast_flow.py | src/python/util/forecast_flow.py | Python | production/support | 143 | 174 | +31 | Changed; see responsibility map |
| src/python/util/forecast_provider.py | src/python/util/forecast_provider.py | Python | production/support | 267 | 271 | +4 | Changed; see responsibility map |
| src/python/util/forecast_storage.py | src/python/util/forecast_storage.py | Python | production/support | 147 | 147 | +0 | Unchanged |
| src/python/util/gift_eval_acquisition.py | src/python/util/gift_eval_acquisition.py | Python | production/support | 294 | 294 | +0 | Unchanged |
| src/python/util/gift_eval_source.py | src/python/util/gift_eval_source.py | Python | production/support | 209 | 209 | +0 | Unchanged |
| src/python/util/gpu_concurrency_calibration.py | src/python/util/gpu_concurrency_calibration.py | Python | production/support | 1386 | 1386 | +0 | Unchanged |
| src/python/util/import_execution.py | src/python/util/import_execution.py | Python | production/support | 1144 | 1144 | +0 | Unchanged |
| src/python/util/import_flow.py | src/python/util/import_flow.py | Python | production/support | 54 | 54 | +0 | Unchanged |
| src/python/util/m4_submission.py | src/python/util/m4_submission.py | Python | production/support | 121 | 121 | +0 | Unchanged |
| src/python/util/process_storage.py | src/python/util/process_storage.py | Python | production/support | 292 | 304 | +12 | Changed; see responsibility map |
| src/python/util/provenance.py | src/python/util/provenance.py | Python | production/support | 55 | 55 | +0 | Unchanged |
| src/python/util/researcher_actions.py | src/python/util/researcher_actions.py | Python | production/support | 528 | 534 | +6 | Changed; see responsibility map |
| src/python/util/researcher_cli.py | src/python/util/researcher_cli.py | Python | production/support | 148 | 148 | +0 | Unchanged |
| src/python/util/researcher_request.py | src/python/util/researcher_request.py | Python | production/support | 24 | 24 | +0 | Unchanged |
| src/python/util/seasonal_period_tuning.py | src/python/util/seasonal_period_tuning.py | Python | production/support | 1614 | 1614 | +0 | Unchanged |
| src/python/util/transformations.py | src/python/util/transformations.py | Python | production/support | 263 | 263 | +0 | Unchanged |
| src/python/util/window_preparation.py | src/python/util/window_preparation.py | Python | production/support | 1098 | 1098 | +0 | Unchanged |
| src/python/util/workflow_orchestration.py | src/python/util/workflow_orchestration.py | Python | production/support | 288 | 288 | +0 | Unchanged |
| src/r/01_02_import_m4comp2018.R | src/r/01_02_import_m4comp2018.R | R | production/support | 209 | 209 | +0 | Unchanged |
| src/r/02_01_preprocess_series.R | src/r/02_01_preprocess_series.R | R | production/support | 81 | 81 | +0 | Unchanged |
| src/r/04_01_forecast_auto_arima.R | src/r/04_01_forecast_auto_arima.R | R | production/support | 86 | 86 | +0 | Unchanged |
| src/r/04_01_forecast_r_methods.R | src/r/04_01_forecast_r_methods.R | R | production/support | 103 | 103 | +0 | Unchanged |
| src/r/qa/get_m4_daily_series.R | src/r/qa/get_m4_daily_series.R | R | manual QA | 300 | 300 | +0 | Unchanged |
| src/r/qa/inspect_rolling_window.R | src/r/qa/inspect_rolling_window.R | R | manual QA | 277 | 277 | +0 | Unchanged |
| src/r/qa/inspect_seasonal_period_tuning.R | src/r/qa/inspect_seasonal_period_tuning.R | R | manual QA | 149 | 149 | +0 | Unchanged |
| src/r/qa/inspect_standardisation.R | src/r/qa/inspect_standardisation.R | R | manual QA | 210 | 210 | +0 | Unchanged |
| src/r/tests/test_forecast_methods.R | src/r/tests/test_forecast_methods.R | R | automated test | 440 | 440 | +0 | Unchanged |
| src/r/tests/test_import_m4comp2018.R | src/r/tests/test_import_m4comp2018.R | R | automated test | 97 | 97 | +0 | Unchanged |
| src/r/tests/test_transformations.R | src/r/tests/test_transformations.R | R | automated test | 118 | 118 | +0 | Unchanged |
| src/r/util/forecast_methods.R | src/r/util/forecast_methods.R | R | production/support | 797 | 797 | +0 | Unchanged |
| src/r/util/labels.R | src/r/util/labels.R | R | production/support | 21 | 21 | +0 | Unchanged |
| src/r/util/seasonal_period.R | src/r/util/seasonal_period.R | R | production/support | 79 | 79 | +0 | Unchanged |
| src/r/util/time_series_input.R | src/r/util/time_series_input.R | R | production/support | 57 | 57 | +0 | Unchanged |
| src/r/util/transformations.R | src/r/util/transformations.R | R | production/support | 188 | 188 | +0 | Unchanged |
| src/r/util/window_preparation.R | src/r/util/window_preparation.R | R | production/support | 30 | 30 | +0 | Unchanged |

| Language and role | Scripts before | Scripts after | Lines before | Lines after | Line change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Python — production/support | 39 | 39 | 18124 | 18438 | +314 |
| Python — automated test | 24 | 24 | 8825 | 9282 | +457 |
| Python — manual QA | 0 | 0 | 0 | 0 | +0 |
| R — production/support | 11 | 11 | 1801 | 1801 | +0 |
| R — automated test | 3 | 3 | 655 | 655 | +0 |
| R — manual QA | 4 | 4 | 936 | 936 | +0 |
| Python subtotal | 63 | 63 | 26949 | 27720 | +771 |
| R subtotal | 18 | 18 | 3392 | 3392 | +0 |
| production/support subtotal | 50 | 50 | 19925 | 20239 | +314 |
| automated test subtotal | 27 | 27 | 9480 | 9937 | +457 |
| manual QA subtotal | 4 | 4 | 936 | 936 | +0 |
| All scripts | 81 | 81 | 30341 | 31112 | +771 |
