# POC2 rolling windows and train test split decision

Status: Combined IDs 011 and 016 approved by the researcher on 1 October 2026.
The previously confirmed window sizes and stride rule remain unchanged.
AMP-Code is authorised to implement this scope and publish its reviewed changes
to GitHub, with safe Ubuntu synchronisation. The version-5 implementation and
focused Mac validation are complete. The required 100-series distributed
acceptance and Ubuntu synchronization remain outstanding because that host was
unavailable; see the [acceptance record](poc2-rolling-windows-acceptance.md).

## Scope

Combine rolling-window preparation (ID 011) and split management (ID 016) into
one outer ShapeFM preparation workflow, following the
[research vision](research-vision.md). This is not compulsory processing for
every forecast model and is not a redefinition of Gate 3.

Features, labels, Mantis resizing/embeddings, model training, prediction and
accuracy optimisation remain outside this approval.

## Window settings

One default per frequency. Stride = input window + future horizon.

| Frequency | Input window | Future horizon | Stride |
| --- | ---: | ---: | ---: |
| 10S | 512 | 60 | 572 |
| 5T | 512 | 48 | 560 |
| 10T | 512 | 48 | 560 |
| 15T | 512 | 48 | 560 |
| Hourly | 256 | 48 | 304 |
| Daily | 64 | 14 | 78 |
| Weekly | 64 | 13 | 77 |
| Monthly | 64 | 18 | 82 |
| Quarterly | 32 | 8 | 40 |
| Annual | 16 | 6 | 22 |

Use M4 future horizons for Daily, Weekly and Monthly across datasets.
These are window-preparation settings, not overrides of official GIFT-Eval
evaluation horizons. Non-overlapping complete input/future blocks preserve the
previous S1 rolling mechanism; these defaults are not claimed accuracy optima.

## One split strategy

Use S1, as selected in the previous project's saved configuration:

- Approximately 80% training and 20% test, with split seed 123.
- Split eligible original series within each frequency pool, not window rows.
  Every window from a series inherits the same partition.
- No label stratification, class balancing, training caps or alternative
  strategies. No additional validation partition is created.
- Store actual series membership once and reuse it across consumers and
  horizons. The window counts need not be 80/20 because series lengths differ.
- For exact historical reproduction, reuse compatible saved membership where
  available, after verifying its source-series mapping. Otherwise create and
  record a new S1 assignment. Seed 123 in Python does not guarantee the same
  selected series as seed 123 in R.

This historical train/test split is separate from official future evaluation
holdouts. Protect those holdouts before determining window eligibility.
A frequency pool with fewer than two eligible series cannot supply both
partitions and must be reported, not silently assigned to both.

## Central settings and storage

Define window lengths, horizons, stride rule, split policy, proportion, seed,
preparation and boundary policies before experiment creation. Follow the
[global setting standard](code-standards.md): validate and store original and
resolved configuration in the parent DuckDB; resume from the stored definition.
Stride values are derived and persisted, not independently editable defaults.
Scientific changes require a new experiment, preserving previous results.

Use the existing parent DuckDB and one additional windows DuckDB for all
selected frequencies. The parent owns canonical identities and stable numeric
lookup keys. The child reuses those keys and stores descriptive strings once.
ShapeFM validates consistency between files.

The child stores the immutable series-to-partition mapping once, alongside its
experiment/split identity. Windows inherit membership through the series key.
Do not create separate copies of train and test window arrays or duplicate
partition descriptions in every window.

Store transformed input values, source positions, preparation identity and
fitted state. Keep raw observations and future values in the parent, accessible
through their identities and positions. No raw window copies.

## End to end process

1. Read selected series and protect reserved observations.
2. Determine which series can supply complete input/future blocks.
3. Assign and persist the single S1 train/test split.
4. Create windows, fit the approved
   [standardise_sample_v1](poc2-standardisation.md) on each input only, and store
   transformed inputs with their identity and preparation state.
5. Report series/window counts by frequency and partition; verify retrieval
   and restart without duplicate windows or changed membership.

For L permitted observations in a contiguous segment, complete blocks number
floor(L / stride). No padding or automatic resizing; report zero-window series
and unused trailing observations. The earlier prediction-padding helper is
preserved but is not used here.

## Libraries and acceptance

Use tsai windowing and splitting utilities within one ShapeFM workflow.
Split the eligible series-ID list, never the generated windows. ShapeFM owns
settings, membership and storage; library defaults cannot choose them.
Reuse the bounded scheduler and sole Python database writer.

Validate with small fixtures for all frequencies and the existing first
100 official M4 Daily series. Check boundaries, transformations, disjoint
series membership, persisted settings, retrieval and restart. Heavy tests follow
the [execution policy](execution-policy.md), including Mac/Ubuntu source
synchronisation. No Mantis training or full-dataset acceptance is included.

The implementation completed these focused checks on Mac. It did not substitute
a heavy local run for unavailable Ubuntu; the two-host acceptance remains open.

## Approval and evidence

The researcher approved this combined scope on 1 October 2026 and requested
AMP-Code instructions including GitHub synchronisation. The
[approved implementation instructions](amp-poc2-rolling-windows-instructions.md)
authorise scoped implementation, testing, publication and machine
synchronisation while preserving unrelated work. Training and prediction are
not authorised by this decision.

Historical sources in
`/Users/monta/Documents/Projects/m4_tsc_fmts_2026/src/r/`:
`utils.R`, `03_rolling_windows.R`, `08_split_index.R` and
`0_common_new.R`. The saved configuration selects S1, TRAIN_FRAC=0.80 and
SPLIT_SEED=123; this does not prove which settings every historical run used.
Library references: [tsai splitting](https://timeseriesai.github.io/tsai/data.validation.html)
and [windowing](https://timeseriesai.github.io/tsai/data.preparation.html#slidingwindow).
