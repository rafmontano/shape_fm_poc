# IDs 038/041 integrated implementation checkpoint

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
