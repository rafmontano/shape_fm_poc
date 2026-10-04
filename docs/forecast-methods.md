# R forecast-method pool

The researcher approved the M4 benchmark extension in
[`poc2-m4-benchmark-methods.md`](poc2-m4-benchmark-methods.md) on 4 October
2026. The bounded implementation adds Naive2, SES, Holt and Damped to this single
R pool. Configuration v9 activates them through a separate capability-aware
allowlist without altering the established nine-method registry.

## Scope and boundary

Gate 4 has an allowlisted R forecast pool in
`src/r/util/forecast_methods.R`. It receives an already prepared numeric
context from Gate 3 and produces independent probabilistic base forecasts.
It does not fit or interpret transformations, restore scales, combine
forecasts, evaluate forecasts, read actual future observations, or write to
DuckDB. Python remains the coordinator and only database writer.

The initial method order preserves the original FFORMA pool:

1. `auto_arima_forec`
2. `ets_forec`
3. `nnetar_forec`
4. `tbats_forec`
5. `stlm_ar_forec`
6. `rw_drift_forec`
7. `thetaf_forec`
8. `naive_forec`
9. `snaive_forec`

`M4_forec_methods()` returns this order. `forecast_method_registry()` returns
the same identifiers mapped to explicit function objects. The generic runner
indexes only this validated registry; JSON cannot supply executable R code.

## M4 point methods

The same source file now also provides four library-only point methods:

- `naive2_forec`
- `ses_forec`
- `holt_forec`
- `damped_forec`

They receive one finite `stats::ts` plus a positive horizon and return only a
finite numeric mean vector. The private `.m4_seasonality_test` implements the
official 90% autocorrelation rule. The shared private `.m4_point_forecast`
validates input, avoids decomposition for period one and nonseasonal series,
performs classical multiplicative adjustment when selected, invokes the chosen
point method, restores future seasonal factors once, and validates the output.

These functions do not appear in `M4_forec_methods()` or
`forecast_method_registry()`, so the original nine names, order, and callables
remain exact. Configuration v9 selects them through the separate
`R_POINT_METHODS` allowlist and the normal Process 04 R worker. Their common
result capability is `mean_only`: `mean` is populated while `median`,
`quantile_levels`, and `quantiles` are null. They do not use fallback. The
researcher's historical R module and the official M4 R benchmark definition are
the scientific authorities for ID 018.

M4 Comb is this Process 05 recipe in configuration v9:

```text
m4_comb:
  ses:     1/3
  holt:    1/3
  damped:  1/3
```

Process 04 calculates and stores SES, Holt and Damped independently. After
verifying all three stored component forecasts, Process 05 reads them,
calculates the equal-weight result, stores it as a fourth forecast, and records the
three component forecast IDs, names and weights through `forecast_components`.
The components remain independently retrievable, evaluable, restartable and
reusable. No R `comb_forec()` model-refitting function exists; the existing
Process 05 calculation remains the sole combination implementation.

## Request and result

A common `forecast-v1` request carries exact experiment, task, instance,
variant, dataset, series, model, capability, scale and seed identities plus a
non-empty finite `context`, positive `horizon`, stored frequency, resolved
seasonal period, explicit model settings, and capability-appropriate quantile
levels. It never contains future actual observations. Probabilistic v9 requests
use q0.025, q0.1 through q0.9, and q0.975; mean-only requests use null levels.

All nine methods receive a `stats::ts` created by the shared
`time_series_from_values()` boundary. It preserves the prepared values and
attaches the period resolved by experiment planning; individual methods do not
independently infer a period or construct their own production input object.

Every successful common result contains:

- exact contract, experiment, task, instance, and variant identity;
- `requested_model_id`, `executed_model_id`, capability, scale, and horizon;
- `mean`, `median`, `quantile_levels`, and a levels-by-horizon `quantiles`
  matrix, with the last three null for mean-only results;
- `fallback_used` and `fallback_reason`;
- method, package-version, distribution, settings, and R provenance;
- `status = "success"`.

The coordinator validator rejects field drift, identity or capability mismatch,
wrong dimensions, non-finite values, crossing quantiles, incorrect q0.5 medians,
and inconsistent fallback metadata before restoring scale and storing the result.

## Probabilistic outputs

The interval adapters request the central intervals implied by the desired
quantiles. For the current levels, 95%, 80%, 60%, 40%, and 20% lower bounds
become q0.025, q0.1, q0.2, q0.3, and q0.4; the corresponding upper bounds become
q0.975, q0.9, q0.8, q0.7, and q0.6. These adapters do not request an internal Box-Cox
transformation, so the `forecast` package's symmetric input-scale intervals use
the point forecast as the median and q0.5.

| Method | Mean and median | Other quantiles |
|---|---|---|
| AutoARIMA | package point forecast; Gaussian mean = median | `forecast()` central intervals |
| ETS | package point forecast; input-scale mean = median | `forecast()` central intervals; `opt.crit = "mae"` |
| NNETAR | standard `forecast(fit, h)$mean`; empirical predictive median | empirical type-8 quantiles simulated from the same single fit |
| TBATS | package point forecast used as q0.5 and median | `forecast()` central intervals from `tbats(x, use.parallel = FALSE)` |
| STL-AR | package point forecast; input-scale mean = median | `forecast()` central intervals from `stlm(..., modelfunction = stats::ar)` |
| Random walk with drift | Gaussian mean = median | `rwf()` central intervals |
| Theta | package point forecast; symmetric mean = median | `thetaf()` central intervals |
| Naïve | Gaussian mean = median | `naive()` central intervals |
| Seasonal naïve | Gaussian mean = median | `snaive()` central intervals |

NNETAR is nonlinear, so its point forecast is not assumed to be its predictive
median. The adapter fits once under a local deterministic seed and preserves the
standard fitted-model point forecast as `mean`; simulations from that same fit
provide the predictive median and requested quantiles.
Defaults are seed 1234, 20 fits, 1,000 paths, and non-bootstrap innovations;
all are recorded in provenance. The caller may supply reviewed settings.

The forecast-pool profile preserves the original FFORMA scientific defaults:
AutoARIMA uses `stepwise = FALSE` and `approximation = FALSE`, while TBATS keeps
its automatic internal component selection. The existing configured POC worker
may explicitly request its historical `stepwise = TRUE` AutoARIMA profile; that
choice is recorded in result provenance and does not change the pool default.

The old FFORMA STL-AR function silently changed to AutoARIMA on failure. This
implementation removes that hidden model substitution: every fitting,
forecasting, or output-validation error enters the common visible fallback.

## Seasonal-naïve fallback

If a requested method fails, `run_forecast_method()` captures the original
error and tries `snaive_forec` once with the same scientific input. A successful
fallback emits a warning and returns one result with the original method in
`requested_method_id`, `snaive_forec` in `executed_method_id`,
`fallback_used = true`, and the original error in `fallback_reason`.

Direct seasonal-naïve failure never recurses. If both methods fail, execution
terminates with one error containing both failures. A fallback result confirms
that a forecast was produced; it does not claim that the requested model fitted.
The [version-8 execution evidence](poc2-all-model-execution-evidence.md) is a
concrete example: all 100 period-1 STL-AR requests stored valid seasonal-naïve
fallbacks, so those candidate rows must not be reported as successful STL-AR
fits.

## Stage 2 production integration

Configuration v7 (`poc2_m4_daily_100_r_pool.json`) now connects these nine
methods through the retained normal Gate 4 route. `R_MODEL_METHODS` maps stable
Python model IDs to the native allowlisted IDs. The generic R worker receives
the prepared context, resolved period and configured settings for each job.
The provider returns requested/executed method, fallback reason and provenance;
`ForecastStorage` validates these before the single-writer commit. Retrieval
uses the configured model ID and never refits a method.

Chronos-2 produces the configured quantiles through its pinned native pipeline.
Because its independently estimated levels can cross, the Python provider sorts
quantile values per horizon step, derives the result median from the rearranged
q0.5 row, preserves the model mean, and records whether this monotone
rearrangement occurred. The coordinator still rejects any crossing result, and
the evaluator never repairs one.

Configuration v9
(`poc2_m4_daily_100_forecast_contract_m4_comb.json`) retains that registry and
adds the four point methods through `R_POINT_METHODS`. All fourteen base methods
use the same serialized request/success/error fields, coordinator validation,
inverse transformation, Dask routing, and single-writer storage path. Process 05
then creates `m4_comb` only from the three stored original-scale component means.
Process 06 selects the full probabilistic metric profile for ten candidates and
the eight-metric `mean_based_v1` profile for the four point methods and M4 Comb.

Historical configurations and their AutoARIMA worker path remain supported.
The adapter delegates to the same native implementation; it is not a second
AutoARIMA algorithm. The nine-model equal-weight combination is a transparent
baseline over stored outputs, not the FFORMA meta-learner. Stage 2 implementation
and bounded evidence remain subject to review in the workflow acceptance record.

## Generic example

```r
source("src/r/util/forecast_methods.R")

request <- list(
  task_id = "task-1",
  dataset_id = "m4_daily",
  series_id = "D1",
  context = as.numeric(1:56),
  horizon = 3L,
  frequency = 7L,
  method_id = "naive_forec",
  settings = list(),
  quantile_levels = seq(0.1, 0.9, by = 0.1)
)

results <- run_forecast_methods(
  request,
  method_ids = c("auto_arima_forec", "naive_forec", "snaive_forec")
)
```

Gate 3 now persists fitted transformation state for `standardise_sample_v1`.
Gate 4 applies registered preparation steps in forward order and inverses the
forecast `mean` and any supplied `median` or quantiles in reverse order; it does
not fabricate absent probabilistic fields. Historical
`minmax_then_standardize` inversion remains available only for configuration
v1–v3 compatibility. The v4 context-window utility is configured and tested,
but current forecast adapters still receive complete prepared histories.
