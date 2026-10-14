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
training   [0, N-28)
validation [N-28, N-14)
test       [N-14, N)
```

## Process 01 — Import

Process 01 reads the selected GIFT-Eval records, converts each to the canonical
series object and commits it atomically to `results/experiment.duckdb`. It does
not clean, transform or infer statistical seasonality. Restart reads committed
series identities and imports only missing work.

## Process 02 — Preprocess

Preprocessing uses historical observations only and never changes the canonical
target. The supported modes are:

- `standard`: `forecast::na.interp()` fills missing observations;
- `robust`: `forecast::tsclean()` performs the approved missing/outlier handling.

The R period is resolved once from configured frequency unless an explicit
experiment override is approved. It is an R modelling convention, not a
seasonality test and not GIFT-Eval's scoring seasonality.

## Process 03 — Transform and windows

`standardise_sample_v1` fits on the finite prepared historical input. A
nonconstant input uses sample standard deviation (`n-1`); constant/singleton
inputs use scale 1. Store the fitted state so forecasts can be inverted without
refitting.

Rolling windows are created only inside the official training boundary. Split
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
unavailable, not class 0. The reference is the final cleaned input value on its
original scale; future observations never enter cleaning or transformation fit.

## Trust boundary

Process 01 validates the external source and canonical conversion. Processes 02
and 03 validate their native worker results before committing them. Once a
canonical row is committed, downstream code treats it as trusted and does not
repeatedly reconcile it with another source or rehash it during ordinary status.
