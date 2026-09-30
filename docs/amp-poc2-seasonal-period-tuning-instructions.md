# AMP Code instructions for seasonal period tuning

Implement the approved [seasonal period tuning decision](poc2-seasonal-period-tuning.md).
Read it in full alongside research-vision.md, poc2-object-adaptation.md and the
current architecture, configuration and forecast contracts. The decision is the
scientific acceptance contract. Keep implementation limited to 100 M4 Daily
series and AutoARIMA/ETS initially.

This original scientific implementation and its recovery are complete. For
current work, follow the [safeguard instructions](amp-poc2-execution-safeguards-instructions.md)
and [execution policy](execution-policy.md). Preserve all 800 forecasts; the
original implementation/acceptance steps below do not authorise repeating the
completed run. The new eight-worker capacity requires only focused validation.

## Preparation

- Inspect Git status and current code before editing. Preserve user changes,
  existing databases and src/r/qa/get_m4_daily_series.R. No script deletions or
  unrelated fixes from the vision-alignment review.
- Reuse existing code, environments, shared R adapters, task execution and
  persistence wherever possible. No universal adapter registry or new scheduler.
- Verify both machines for heavy acceptance under the execution policy. This
  R-only workload needs CPU workers, not Chronos or GPU computation. If Ubuntu
  is unavailable, stop heavy execution and request direction; do not silently
  use a long Mac-only run.

## Implementation

1. Add opt-in tuning through the existing experiment configuration mechanism.
   Defaults for this experiment are three validation windows, horizon h,
   origin spacing h, expanding training histories, minimum three seasonal
   cycles, MAE selection, and baseline retention for ties/inconclusive results.
   Persist through existing original/resolved configuration authority. Preserve
   v1/v2 untuned semantics; version only where compatibility requires it.
2. Resolve the baseline from the existing period policy. Keep the preprocessing
   period fixed. Introduce a distinct selected model-period value in the Gate 4
   job so it cannot alter cleaning or official evaluation seasonality.
3. Add a small shared R utility for findfrequency and tsfeatures diagnostics.
   Reuse registered model wrappers. Do not add a seasonal-strength cutoff or
   additional candidate search without approval. Check model support and history
   sufficiency before fitting; record why candidates are rejected or substituted.
4. Generate raw historical fold slices strictly before the official test.
   Refit existing cleaning/transformation functions within each fold. Compare
   inverse-transformed forecasts to unmodified validation observations. Exclude
   missing validation labels consistently across policies; persist valid counts.
   All-missing windows make selection inconclusive. Neither future actuals nor
   full-history fitted preparation may enter fold computation.
5. Integrate ETS alongside AutoARIMA in configuration, planning, execution,
   persistence and retrieval as needed for this approved tuning task. Preserve
   registered model settings and requested/executed identities. This authorizes
   those two models for tuning, not implementation of the other seven R methods.
6. Schedule preparation/candidate tasks by series, preparation variant and fold;
   validation tasks by model, fold and policy. Share only identical compatible
   inputs and deduplicate equal periods. Reuse bounded workers, retry semantics,
   and existing concurrency controls. Avoid nested model parallelism.
7. Select baseline or estimated policy using mean MAE across all three folds.
   A model fallback is retained as evidence but cannot win tuning on behalf of
   the requested model. Incomplete comparisons retain baseline with a reason.
   On final history, resolve the winning policy again, enforce eligibility, and
   invoke the normal final forecast/result adapter. Preserve its probabilistic
   contract and documented fallback behaviour.
8. Reuse existing storage and add only necessary versioned research-result
   structures. Keep candidate diagnostics, validation predictions/scores,
   selection and final forecasts linked and independently inspectable. Keep
   validation outputs out of official forecast queries and Gate 6. Python alone
   writes DuckDB. Historical databases must remain interpretable.
9. Make task identities include scientific tuning settings, input/preparation
   identity, window and model settings as applicable. Resume completed tasks;
   do not reuse results from different inputs or versions. Control random seeds
   independently of task scheduling order where stochastic code is used.
10. Update human-readable code comments and documentation. Explain that this
    searches forecasting policies per series/model, not an immutable true
    seasonal period, and that seasonal-strength features are diagnostic here.

## Validation and manual QA

Run focused synthetic/contract tests before the 100-series acceptance. Verify:

- exact fold boundaries and isolation of held-out values from preprocessing,
  transformations, period estimation and model fitting;
- baseline/estimated selection, ties, insufficient history, duplicate periods,
  unsupported periods, missing labels and model failures/fallbacks;
- different models can select different policies without changing preprocessing;
- original-scale scoring and final-period re-estimation;
- stored validation forecasts never appear as official forecasts;
- candidate sharing and restart without recomputing completed work;
- sequential/parallel agreement under controlled seeds and numeric tolerance;
- existing untuned configuration, forecast adapters and archived retrieval
  regressions remain valid.

Provide a small documented QA script under src/r/qa for inspecting one series'
windows, candidates, seasonal strengths, validation scores, selected policy and
final period. Reuse read-only retrieval and the user's source()-based workflow;
leave inspectable objects in the R session. Do not alter the existing M4 QA
object's ten-component contract. Clearly label validation actuals as QA data,
never model inputs.

Resume the approved 100-series tuning case for AutoARIMA and ETS with the
verified distributed CPU configuration, after the recovery checks. Preserve
completed work. Report policy selections, comparable MAEs, skipped and
inconclusive cases, failures/fallbacks, wall-clock overhead and restart evidence.
Do not require every series to improve and do not describe selection-window
improvement as independent test-set evidence. Do not run 4,227-series tuning,
foundation-model downloads or a full GPU pipeline for this acceptance.

## Handoff

Return changed files, exact configuration/run/QA instructions, tests actually
executed, the 100-series summary, unresolved limitations and any decisions that
would require scope expansion. Mark implementation/acceptance status accurately
in the decision document. Do not change the spreadsheet, remove scripts or fix
unrelated vision gaps. Report Git commit/publication status explicitly; preserve
unrelated work and never force-push or rewrite history.

After reporting seasonal-test closure, stop and wait for the researcher to
return to ID 010. Its prior design approval is not an instruction to start it
automatically as part of this recovery.
