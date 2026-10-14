# Forecast model contract

Every forecast provider is a black box behind the same ShapeFM boundary. Native
R/Python objects stay inside their provider; callers see one request and one
result.

## Request

A request identifies the experiment, dataset/series, prepared input, frequency,
horizon, requested model/variant, approved settings, scale, seed and requested
output capability. It contains past observations only—never future actuals.

## Result

A successful result identifies requested and executed model, horizon and scale,
and returns:

- a finite mean vector of exactly the requested horizon;
- median and ordered quantiles when the model capability provides them;
- null probabilistic fields for a point-only model;
- explicit fallback use/reason where the method contract permits one; and
- the provider/model/package provenance needed to interpret the result.

All providers use equivalent semantic fields regardless of language. Adapters
may convert native objects, but they do not change the scientific calculation.
Transformation inversion applies the stored fitted state to every supplied
location/quantile and never fabricates unavailable quantiles.

## Forecast pool

The FFORMA-compatible R pool is:

| Model | Required scientific setting |
| --- | --- |
| AutoARIMA | `stepwise=FALSE`, `approximation=FALSE` |
| ETS | `opt.crit="mae"` |
| NNETAR | native default fit; deterministic experiment seed; simulations only for probabilistic outputs |
| TBATS | `use.parallel=FALSE` (Dask owns parallelism) |
| STL-AR | `stlm(..., modelfunction=stats::ar)`; on fitting error only, `auto.arima(d=0,D=0)` |
| Random walk with drift | `rwf(..., drift=TRUE)` |
| Theta | `thetaf()` |
| Naive | `naive()` |
| Seasonal naive | last seasonal cycle |
| SES, Holt, Damped, Naive2 | official M4 point-method definitions; mean-only |

There is no pool-wide fallback. A method without its approved fallback fails
visibly. Chronos-2 uses its pinned local checkpoint and native probabilistic
provider; ordinary execution is offline and does not download the model for each
job.

Given identical input, frequency, horizon, seed and model settings, the FFORMA
methods must produce the same individual point means as the original
implementation. ShapeFM's envelope, storage and quantile fields are additional
contracts around those calculations.

## Storage and combination

Process 04 stores every individual forecast before combination. Process 05
reads stored component identities, requires them all, applies the approved
formula and stores a new forecast with component links and weights. Missing
components are calculated by Process 04, not silently refitted inside the
combination.

M4 Comb is the equal-weight point combination of SES, Holt and Damped. The
component means and combination remain independently retrievable. A general
weighted mean accepts two or more comparable forecasts, requires weights that
sum to one and combines each supported field only by an approved mathematically
valid rule; quantiles are not automatically treated as a joint distribution.

## Evaluation capability

Evaluation selects metrics compatible with the result. Mean-only methods do not
receive fabricated probabilistic scores. Probabilistic forecasts preserve their
levels and are evaluated through the configured GIFT-Eval profile.
