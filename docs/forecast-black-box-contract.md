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

### Chronos-2 paper baseline

The approved baseline supplies original canonical GIFT-Eval history strictly
before the forecast origin, without Process 02 cleaning or external Process 03
standardisation. Chronos owns its native normalization; outputs are already on
the original scale and must not be externally inverse-transformed. Retain the
existing pinned checkpoint, precision, quantile levels and independent-series
inference settings. The paper point forecast is the median (0.5 quantile).
In the pinned provider, the return field named `mean` is that median, not an
estimated expectation; document this meaning without redesigning the envelope.
The [2.2.2 provider implementation](https://github.com/amazon-science/chronos-forecasting/blob/v2.2.2/src/chronos/chronos2/pipeline.py)
selects the native 0.5 channel for that return. Context truncation stays inside
the provider and follows the loaded checkpoint's context limit.

This follows the previous paper project's sensitivity path:
`src/r/sensitivity/03_add_chronos_forecasts.R` calls
`src/python/chronos_forecast.py` on original histories. Its loader did not
explicitly pin the checkpoint revision or dtype. Inspect retained historical
provenance if available; otherwise record that limitation and retain the current
pin. Do not guess historical settings or tune to reproduce a score.
The retained `environments/foundation-models.yml` pins `chronos-forecasting`
2.2.2, but the checkpoint revision and precision used for the saved forecasts
could not be established. A currently cached snapshot does not establish which
weights ran historically. ShapeFM retains its configured revision and float32.

Other models retain their approved preparation. A cleaned-input Chronos variant
is separate research requiring approval, not an implicit baseline replacement.
The earlier project's source is reference material, not authorized for editing.

Archived M4 forecasts are imported by Process 01 as defined in the
[data contract](data-contract.md#archived-forecast-inputs). Process 04 reads them
from the same forecast repository used for calculated forecasts; it does not
launch R or reopen M4comp2018. Missing required archives are reported as an
import prerequisite, not silently fetched by Forecast.

A query by experiment, dataset and series returns AutoARIMA, Chronos, FFORMA,
SMYL and other stored methods through one interface. Forecast origin/window,
horizon and method distinguish comparable results. Imported versus calculated
is provenance, not a separate researcher access path. Archived point forecasts
retain null probabilistic fields and are not reused for other rolling origins.
Preserve existing forecast identities and accepted rows. Process 04 calculates
only missing calculated forecasts and includes imported rows in pool completion;
it does not copy their vectors into another result store.

The common repository namespace is `04.01`. Resolve dataset/series identity from
the canonical `01.02` row and select forecasts by its official-window `input_key`
and horizon, regardless of provider. Native results need not duplicate canonical
dataset/series fields. `01.04` completion rows contain references to those same
forecast keys, not another forecast vector.

FFORMA methods preserve the approved native scientific definitions and settings,
not exact floating-point identity across hardware/backends or retired code.
Provider boundaries check output shape and scientifically usable values;
ordinary valid floating-point differences are accepted without parity workloads
or tolerance gates. ShapeFM's envelope, storage and quantile fields are additional
contracts around those calculations.

## Storage and combination

The paper Naive2 reference is an explicit raw-history input policy within the
existing Process 04 provider path, not a separate runner. Supply original
canonical observations strictly before the forecast origin, with no Process 02
cleaning or Process 03 standardisation and no inverse transform of its output.
Preserve method/series/window identity and record the actual original input/output
scale. See [evaluation](evaluation.md#naive2-reference-for-paper-owa) for its
scientific authority and correction scope. Other models retain their approved
input preparation; archived SMYL/FFORMA values are never recalculated.

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
