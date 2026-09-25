# Foundation Stage 1 data contract

## Identity

`datasets.dataset_id` identifies an exact imported version and is derived from:

- logical dataset `gift_eval/m4_daily`;
- pinned Hugging Face revision and the three source-file hashes;
- canonical import/data-contract version and material canonicalisation settings.

Execution controls such as `max_series` and `workers` are excluded. Smoke and
full imports therefore share one dataset ID and one logical restartable run.

Within that version, `(dataset_id, series_id)` is the primary series identity.
M4 Daily keeps the official source item ID as `series_id`, so the first series
is retrieved as dataset `m4_daily`, series `0`.

## Series

Each `series` row contains one complete `FLOAT[]` target, source identity and
row, frequency, start timestamp, observation count, deterministic target hash,
source metadata, and creation time. This layout is optimized for retrieving a
complete time series as one object.

## Evaluation windows

All array boundaries are **zero-based and end-exclusive**. M4 Daily uses one
generic `validation_and_test` window with horizon 14. For target length `N`:

```text
train_start       0
train_end         N - 28
validation_start N - 28
validation_end   N - 14
test_start       N - 14
test_end         N
```

For first series `0`, `N = 1020`: training is `[0, 992)`, validation is
`[992, 1006)`, and test is `[1006, 1020)`. These reproduce the official
GIFT-Eval short-term M4 semantics.
