# Gate 1 import and Gate 2 preprocessing

## Gate 1 preserves observations

Gate 1 streams the pinned GIFT-Eval Arrow source and stores its canonical raw
target in `series.target`. Finite values are not cleaned or transformed. Arrow
nulls and IEEE NaNs are accepted as missing observations and their positions are
preserved; DuckDB represents both forms as missing list elements. Empty targets,
malformed records, identity/frequency mismatches, and positive or negative
infinity fail clearly.

The official frequency comes from the GIFT-Eval dataset (`Dataset.freq`). The
approved M4 Daily benchmark metadata records a seasonality of 7, representing
the weekly cycle, and that same value is passed to preprocessing, live
forecasting, and evaluation. It is stored with its provenance rather than
derived from the former hidden period lookup. Gate 1 does not impute data.

## Gate 2 modes

Researchers select preprocessing through the existing experiment JSON:

```json
"preprocessing": {
  "default": "robust",
  "modes": ["standard", "robust"]
}
```

These are the only user-selectable modes:

- **`standard`** calls `forecast::na.interp()` with the official seasonality.
  It fills only missing positions and must leave every finite input observation
  unchanged.
- **`robust`** is the default and reproduces the previous cleaning expression:
  `as.numeric(forecast::tsclean(stats::ts(x, frequency = seasonality)))`.
  It may replace missing values and detected outliers.

Both modes receive historical context only—never future/test observations—and
must return a finite vector with the original length. A result records the
dataset/series lineage through its forecast instance, mode, official frequency
and seasonality, R and `forecast` versions, input/output hashes, success status,
missing counts before and after, and whether values changed. Failures remain in
the restartable task/attempt tables with their error. The raw `series.target`
row is never overwritten or duplicated in each preprocessing result.

The database retains the historical column name `cleaning_method` for safe
schema compatibility. New configuration, APIs, and documentation call the
values preprocessing modes.

## Evaluation and official references

Forecast vectors must be finite. Actual observations are different: missing
future labels remain missing in the official source and GIFT-Eval receives
`mask_invalid_label = true`, so they are excluded from metrics rather than
imputed, treated as zero, or reported as forecast failures.

`official_reference` is an internal source designation for imported Smyl and
FFORMA M4 point forecasts. It is not a third preprocessing mode. Those archived
means were produced from the original competition data, bypass ShapeFM
preprocessing, remain `mean_only`, and never receive fabricated quantiles.
