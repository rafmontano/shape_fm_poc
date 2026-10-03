# POC2 optional reusable features decision

Status: ID 014 approved by the researcher on 3 October 2026. Implementation is
an optional extension of the accepted rolling-window preparation architecture.
Technical review, independent researcher QA and the bounded-memory correction
were accepted on 3 October 2026. The capability remains opt-in; acceptance does
not enable the eight directional features or start a later migration item.

## Scientific contract

`fforma_base_v1` is one explicitly ordered 42-feature R schema implemented by
`src/r/util/features.R`. It composes the FFORMA-style `tsfeatures` pool by name:
ACF/PACF, ARCH and heterogeneity, crossing/flat-spot, entropy, Holt, Hurst,
lumpiness, nonlinearity, STL, stability, Holt-Winters and unit-root features,
plus `series_length`. Python does not recalculate these values; it submits
prepared inputs and retrieves named rows from the native R provider.

Each input is calculated independently with `tsfeatures(scale = TRUE)`. This is
important: a single multi-series `tsfeatures` call disables scaling for every
row when any member is constant. Bounded requests therefore contain multiple
jobs but invoke the scientific kernel separately for each series, so batch
composition cannot change an individual result.

The schema is identified by its feature-set ID, integer version and all 42
names in fixed order—not by column count or positional insertions. For a
nonseasonal series, the absent `seas_acf1`, `seas_pacf`, `seasonal_strength`,
`peak` and `trough` fields are inserted by name as zero. Final `NA` feature
values become zero, matching the useful FFORMA adaptation. Heterogeneity errors
fall back to four zeros; constant/short/error entropy falls back to zero; and a
failed Holt-Winters AAA fit yields unavailable parameters that the final policy
turns into zero. Unexpected, duplicate, incomplete or non-finite output fails
the job. Other failures are recorded, never represented as successful all-zero
rows.

The eight legacy ARIMA/ETS directional features are retained as named future
capability only:

```text
da_mda_arima, da_mdv_arima, da_mdpv_arima, da_pt_pvalue_arima,
da_mda_ets, da_mdv_ets, da_mdpv_ets, da_pt_pvalue_ets
```

They are disabled because legacy training used each window's future while real
prediction used a historical holdout. Activating either definition would embed
different information at training and prediction time. ID 014 does not choose a
replacement strategy and keeps futures and labels outside the base provider.

## Connected optional workflow

The sole researcher entry point exposes `prepare-features`. It is not invoked by
`prepare-windows`, normal forecasting, Mantis or every model. On an explicit
request, the Mac coordinator:

1. validates the completed parent/child preparation identity;
2. resolves periods through the existing central GIFT-Eval adapter;
3. counts the complete request and enforces the local limit before reading any
   prepared input array;
4. finds windows without an accepted exact provenance match through a database
   anti-join and reads only those inputs in deterministic keyset pages;
5. schedules native R calculations through Prefect and, for heavy work, the
   approved Dask CPU profile;
6. validates names, order, values, worker dependencies and source identity; and
7. writes compact rows through the Mac's sole writer.

Focused local runs require an explicit bound of at most 200 windows. Heavy runs
require the approved 8-Mac/15-Ubuntu CPU profile, existing admission/memory
safeguards and no nested R parallelism. The implementation does not copy the
legacy PSOCK/foreach orchestration, hard-coded worker count, chunk RDS cache or
full feature-enriched datasets.

Each row is linked to the existing `window_id` and stores only its 42 doubles,
feature/source/dependency provenance, transformed-input hash, transformation
recipe/state hash, resolved period, worker evidence, status and attempt count.
It does not duplicate prepared arrays, futures, labels or S1 membership. Cache
reuse requires the feature definition, R dependency identity, reviewed source
manifest and all input/transformation/period provenance to match. Different
valid provenance variants remain distinct. A failed variant remains explicit
and is retried rather than silently dropped; a later accepted row is reusable by
all consumers with the same contract.

The page size is the configured feature batch size multiplied by the existing
maximum in-flight batch authority. Each page is committed before the next page
is read. The immutable `(series_key, window_ordinal, window_id)` keyset prevents
commits from shifting or skipping pending work. Completion and reuse are counted
in DuckDB, so accepted-row identities are not copied into an unbounded Python
set and a fully reused restart reads no transformed arrays.

Read-only `results` retrieval extends the existing parent/child window selector
with `--feature-set-id fforma_base_v1`. It returns the selected prepared window
and one accepted feature row with names, values and its complete provenance.

## Legacy callers and boundaries

The unchanged legacy `07a_ts_features.R` mixed base extraction, optional
directional forecasts, PSOCK scheduling, chunk caching and assembly of full RDS
datasets containing `x`, `xx`, labels and features. ID 014 replaces only its
base feature-provider responsibility. Direct scientific consumers are the
XGBoost training/evaluation path, Rotation Forest and other feature-based
classification selected through the sktime experiment path, and feature/PCA
analysis scripts.

Legacy Mantis and other series-based model runners also read the feature-enriched
RDS merely to recover `x`, labels or shared metadata. That file dependency
unnecessarily coupled those models to feature extraction; it is documented but
not migrated here. Mantis training, prediction, resizing, embeddings and feature
generation remain outside this approval. ID 014 also does not independently
close ID 015 or change any classifier, probability threshold, directional
metric, model failure or forecast-padding behavior.

## Compatibility scope

Focused parity tests execute the current provider and the unchanged legacy
`m4_tsc_fmts_2026/src/r/features.R` over identical period-1 and seasonal inputs.
The explicit provider preserves the observed base values, order, scaling,
missing treatment and established fallbacks. This is scoped base-feature parity,
not a claim that historical end-to-end datasets, cleaning, directional fields
or models are numerically equivalent.
