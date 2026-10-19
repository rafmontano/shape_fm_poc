# ShapeFM data contract

## Canonical source

GIFT-Eval is the sole canonical time-series source. The active M4 Daily source is:

```text
data/gift_eval/m4_daily/
├── data-00000-of-00001.arrow
├── dataset_info.json
└── state.json
```

The source revision and file hashes are pinned in `config/experiment.json` and
`config/dependencies/gift_eval.json`. Process 01 does not compare GIFT-Eval
histories with M4comp2018 or another source before accepting them. Secondary
source-comparison adapters are inactive unless the researcher explicitly
requests a comparison.

## Series identity

`dataset_id` identifies the exact imported dataset version. Within it,
`(dataset_id, series_id)` identifies a series. The canonical row stores the full
target array, source/frequency metadata, observation count and required lineage.
Finite observations are preserved. Source null/NaN positions remain missing;
infinity, empty targets and malformed identity/frequency fail.

M4 Daily uses the official GIFT-Eval item identity and prediction length 14.
Array boundaries are zero-based and end-exclusive. For length `N`:

```text
history / training-window source [0, N-14)
official test                   [N-14, N)
```

Use all available pre-test history as in the previous project. Do not reserve
another horizon from that history for window construction: `train_end` and
`test_start` both equal `N-horizon`. This observation boundary is distinct from
S1 series-level membership. Keep the current tsai S1 split, seed 123 and 80/20
policy; replacing it with the previous R membership generator is not approved.

## Process 01 — Import

Process 01 reads the selected GIFT-Eval records, converts each to the canonical
series object and commits it atomically to `results/experiment.duckdb`. It does
not clean, transform or infer statistical seasonality. Restart reads committed
series identities and imports only missing work.

### Archived forecast inputs

When archived methods are configured, subflow 01.04 imports their precalculated
forecasts after canonical dataset acceptance. GIFT-Eval remains the sole source
of observations; M4comp2018 supplies only configured SMYL/FFORMA forecasts and
their provenance for the selected series and original competition forecast window.
Do not import its histories, actuals, unused submissions or unrelated datasets.

One coordinator-host R invocation loads the pinned archive and materialises the
required numeric vectors. The coordinator commits each forecast through the
common forecast repository and atomic completion boundary. Do not persist R
promises, create a parallel reference store, duplicate vectors in Process 04,
or add a database/intermediate export. A restarted import reads stored forecast
identities and extracts only missing records; it does not open the archive when
none are missing. Initial package loading must still succeed; this design does
not assert that the native loading fault has been diagnosed.

Check series/method/window identity, horizon and usable numeric values once at
ingestion. Source package/version and submission identity are provenance. Source
retrieval is not model fitting or a comparison against GIFT-Eval histories.

Process 01 completion requires its configured subflows, including archived
import. Resume preserves accepted canonical data and imports only missing
forecast items; it does not reset the experiment.

## Process 02 — Preprocess

Preprocessing uses historical observations only and never changes the canonical
target. The supported modes are:

- `standard`: `forecast::na.interp()` fills missing observations;
- `robust`: `forecast::tsclean()` performs the approved missing/outlier handling.

The R period is resolved once from configured frequency unless an explicit
experiment override is approved. It is an R modelling convention, not a
seasonality test and not GIFT-Eval's scoring seasonality.

## Process 03 — Transform and windows

Cleaning occurs once per series in Process 02, on its complete available history
strictly before the official test origin. Process 03 slices that persisted
prepared history; it must not call a cleaning/interpolation provider per window.
This restores the previous project's clean-once, then window, then transform
sequence. Raw canonical observations remain intact alongside prepared history.
Chronos-2 and Naive2 consume original uncleaned history, without external
standardisation or inverse transformation; they do not consume prepared history.

For the historical directional workflow, rolling input and target blocks are
sliced from the prepared history, ending no later than the official test origin.
Official evaluation inputs use the prepared historical tail, but official test
actuals stay raw and never enter cleaning. Standardisation remains fitted on
each input only and applied consistently to its associated targets where needed;
standardisation is not cleaning. Full-history preparation is retrospective for
internal rolling origins: later historical observations may influence earlier
cleaned values. Do not claim origin-local causal cleaning or add repeated
per-origin cleaning to eliminate this property. Final test observations remain
excluded. The platform must preserve this approved scientific sequence.

`standardise_sample_v1` fits on the finite prepared historical input. A
nonconstant input uses sample standard deviation (`n-1`); constant/singleton
inputs use scale 1. Store the fitted state so forecasts can be inverted without
refitting. Internally produced affine parameters are trusted on application and
inversion, without repeated portable-schema validation.

Rolling windows are created only inside the complete pre-test history. Split
membership is assigned to the original series before windows are generated, so
all windows from one series stay in one S1 partition. Complete non-overlapping
blocks use configured input length plus future horizon; no partial block is
padded. Daily uses input length 64 and horizon 14.

High-volume rolling modelling inputs, transformed windows and their labels
belong in `results/windows.duckdb`. Membership, reusable features,
representations, raw series and experiment state remain in the parent. The
child records the same experiment identity; it is first created by Process 03.

## Directional labels

The stored binary definition is `directional_strict_v1`:

```text
1 when future_value > reference_value; otherwise 0
```

Equality is 0 and no numerical tolerance is applied. Missing future values are
unavailable, not class 0. The reference is the final prepared input value on its
original scale. Internal training targets follow the prepared-history sequence
above; official targets remain original observations. Future targets do not enter
window standardisation fit. The historical cleaning boundary is defined above.

## Trust boundary

Process 01 validates the external source and canonical conversion. Processes 02
and 03 validate their native worker results before committing them. Once a
canonical row is committed, downstream code treats it as trusted and does not
repeatedly reconcile it with another source or rehash it during ordinary status.
