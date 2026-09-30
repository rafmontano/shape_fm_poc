# POC2 seasonal period tuning decision

Status on 30 September 2026: implementation and the bounded 100-series
distributed acceptance are complete. See the dated
[acceptance and recovery record](poc2-seasonal-period-tuning-results.md) for
results, preservation proof and limitations. Expansion remains subject to
researcher review.

The completed scientific test remains accepted. Execution-policy closure is
separate: follow the [execution policy](execution-policy.md) and
[eight-worker safeguard instructions](amp-poc2-execution-safeguards-instructions.md)
for the approved remaining corrections. Do not repeat the completed acceptance
or change the procedure below. ID 010 stays queued until the follow-up is closed
and the researcher instructs its resumption.

This decision extends the [POC2 adapter decision](poc2-object-adaptation.md)
within the [ShapeFM research vision](research-vision.md). It adds optional,
individual forecasting-period tuning in Gate 4. The initial acceptance scope is
100 M4 Daily series, AutoARIMA and ETS. Expansion to all 4,227 series and further
models follows review of that evidence.

## Ownership and scope

Gate 4 coordinates candidate preparation, historical validation, policy
selection and final forecasting. The shared R utility estimates periods and
features. Model adapters construct the objects required by their models.
Python coordinates parallel work and is the sole DuckDB writer.

Gate 2 cleaning settings remain fixed during this experiment. Gate 6 retains
its independent official scoring policy. A selected forecasting period must
not replace either setting. Existing experiments and untuned execution remain
reproducible. Chronos-2 retains its current inputs and does not enter this search.

## Scientific procedure

For each series, preparation variant and selected model:

1. Create three historical validation windows, each of length h, where h is the
   dataset forecast horizon. All validation observations precede the official
   test boundary. Use expanding training histories; the latest validation
   window ends at that boundary, and earlier origins are separated by h.
2. Compare two policies: the existing resolved forecasting period (baseline)
   and a period estimated with forecast::findfrequency().
3. For each window, fit cleaning and transformations on its training history
   only. Estimate the alternative period from that prepared training input.
   Use tsfeatures to measure seasonal strength at the proposed period as
   diagnostic evidence, without imposing a feature threshold for selection.
4. Require at least three complete cycles for a seasonal candidate and ensure
   it is supported by the model. An unusable estimate falls back to the baseline
   policy for that window, with a reason. If both policies resolve to the same
   period, reuse the computation instead of fitting twice. Period 1 denotes
   the non-seasonal case; do not fabricate seasonal-strength evidence for it.
5. Forecast the full h steps, invert transformations and compare with untouched
   validation observations on the original scale. Select using the arithmetic
   mean of the three window MAEs. A tie retains the baseline.
6. If history is insufficient for the three windows, retain the baseline and
   record that tuning was skipped. Missing validation actuals are excluded;
   a window with no valid actuals makes selection inconclusive.
7. If either policy cannot complete the required validation successfully,
   retain the baseline and flag selection as inconclusive. Preserve documented
   seasonal-naive model fallback, but do not count a fallback as successful
   validation of the requested model.
8. Select a policy, not a single fixed period from a validation window. If the
   estimated policy wins, estimate its final period using the complete prepared
   historical input, apply the same eligibility checks, then fit and forecast.
   Persist the actual final period and any baseline substitution reason.

The official test actuals never enter estimation, preparation or selection.
Do not create folds by slicing values cleaned or transformed using the full
history. Source raw training slices and reuse the existing preparation routines.
Record preparation/model failures distinctly from an ineligible period.

## Parallel execution and restart

Use the approved CPU pools on both machines for heavy acceptance. A laptop-only
exception requires the researcher's explicit direction; an unavailable Ubuntu
host must not silently trigger a long local run. Verify effective configuration,
tested source, dependencies and actual worker capabilities before scheduling.

Reuse the existing bounded scheduler and worker environments. Preparation and
candidate analysis are tasks per series, preparation variant and validation
window; compatible models share these results. Model validation tasks are per
model, window and policy. Selection runs after its dependencies complete, then
the existing final forecasting path consumes the selected period.

Avoid nested R parallelism and a second scheduler. Compatibility for sharing
requires identical training boundaries, input values, preparation settings and
relevant software versions. Deduplicate equal-period fits without losing the
two policy identities. Successful tasks survive restart and are not repeated.

## Persistence

Reuse existing experiment, task, forecast and provenance structures where
appropriate. Add narrowly scoped versioned storage only for research results
that existing structures cannot represent. Persist:

- candidate periods, seasonal-strength diagnostics, eligibility and reasons;
- validation forecasts, original-scale MAE, valid-label counts, requested and
  executed model, status and fallback provenance;
- selected policy, comparison scores, status and reason per series/model/variant;
- final resolved period and a link from the final forecast to its selection.

Reference existing data and configuration instead of copying raw observations
or dataset metadata. Distinguish validation forecasts from official final
forecasts so retrieval and evaluation cannot mix them. Changing scientific
tuning settings creates a new experiment; existing results are not overwritten.

## Acceptance and research limits

Validate three-window boundaries, leakage prevention, candidate sharing,
model-specific selection, inspectable stored results, restart and sequential
versus parallel equivalence. Control seeds for stochastic execution. Compare
robust preparation against the same existing routine at the fixed cleaning
period, not at the tuned model period.

Report the 100-series results: per-model policy choices, validation error
changes, skip/fallback/failure counts and runtime overhead. Validation improvement
does not prove improved official test performance. Do not run the full dataset
or expand the model pool before review.

This extends forecasting research and does not reopen ID 9 cleaning or authorize
a general translation framework, broader vision fixes, or script deletion.
