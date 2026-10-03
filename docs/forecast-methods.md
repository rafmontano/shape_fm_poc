# R forecast-method pool

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

## Request and result

A request contains optional task/run identity, dataset and series IDs, a
non-empty finite numeric `context`, positive integer `horizon`, positive integer
resolved R `frequency`/period (or `seasonality`), a registered `method_id`,
method `settings`, and
ordered unique `quantile_levels` strictly between zero and one. It must not
contain future actual observations. Current GIFT-Eval forecasts request levels
0.1 through 0.9.

All nine methods receive a `stats::ts` created by the shared
`time_series_from_values()` boundary. It preserves the prepared values and
attaches the period resolved by experiment planning; individual methods do not
independently infer a period or construct their own production input object.

Every successful result contains:

- dataset, series, and optional task/run identity;
- `requested_method_id` and `executed_method_id`;
- `horizon`, resolved `r_period`, `mean`, `median`, `quantile_levels`, and a
  levels-by-horizon
  `quantiles` matrix;
- `fallback_used` and `fallback_reason`;
- method, package-version, distribution, settings, and R provenance;
- `status = "success"`.

The validator rejects wrong dimensions, non-finite values, crossing quantiles,
incorrect q0.5 medians, inconsistent fallback metadata, and unregistered
methods. `forecast_result_to_json()` writes the quantile matrix in row-major
order with 17-digit numeric precision.

## Probabilistic outputs

The interval adapters request the central intervals implied by the desired
quantiles. For the current levels, 80%, 60%, 40%, and 20% lower bounds become
q0.1, q0.2, q0.3, and q0.4; the corresponding upper bounds become q0.9, q0.8,
q0.7, and q0.6. These adapters do not request an internal Box-Cox
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

## Stage 2 production integration

Configuration v7 (`poc2_m4_daily_100_r_pool.json`) now connects these nine
methods through the retained normal Gate 4 route. `R_MODEL_METHODS` maps stable
Python model IDs to the native allowlisted IDs. The generic R worker receives
the prepared context, resolved period and configured settings for each job.
The provider returns requested/executed method, fallback reason and provenance;
`ForecastStorage` validates these before the single-writer commit. Retrieval
uses the configured model ID and never refits a method.

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
