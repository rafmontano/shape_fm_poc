# POC 1: official end-to-end GIFT-Eval demonstration

## Authority and data flow

Official GIFT-Eval owns configuration, splits, forecast instances, horizon,
seasonality, missing-label handling, metrics, aggregation, result names, and
submission shape. ShapeFM supplies forecasts through `ShapeFMPredictor`; the
pinned framework evaluates them with `gluonts.model.evaluate_forecasts`, the
API used by its pinned Chronos-2 notebook, with `axis=None`,
`mask_invalid_label=True`, `allow_nan_forecast=False`, and official
`get_seasonality(freq)`. ShapeFM contains no metric implementation.

```text
official Dataset.test_data → ShapeFM stages 2–5 → ShapeFMPredictor
                           → official evaluate_forecasts → official metrics
```

All scientific arrays and evaluation results live in `data/shapefm.duckdb`.
Immutable source data, model weights, and temporary worker data remain external;
their revisions, hashes, and locations are recorded. One coordinator owns the
writable database. Workers receive ordinary objects and return ordinary results.
Each result and task completion is one transaction.

Planning, running, and export are write operations and must finish before an R
read connection is opened. Forecast, official-result, and status retrieval use
that existing read-only DBI connection directly; they never invoke Python or
request a writable DuckDB connection.

## Grid and gates

The development grid is:

- cleaning: `identity`, `tsclean` (`forecast::tsclean`, context only);
- transformation: `identity`, `minmax_then_standardize` (context-fitted and
  reversed in the opposite order; constant series are explicit);
- model: AutoARIMA and pinned Chronos-2;
- adjustment: `identity`;
- candidates: AutoARIMA, Chronos-2, and corresponding-quantile 50/50 average.
  If that average contains quantile crossing, deterministic monotone
  rearrangement sorts the nine quantiles at that horizon position; already
  ordered averages are unchanged, and the adjustment is recorded with the
  forecast.

For ten forecast instances, deterministic task counts are Stage 2: 20,
Stage 3: 40, Stage 4: 80, Stage 5: 120, Stage 6: 12. Gates are strict and run in
order. Completed IDs skip on restart; failures retain attempts and retry alone.
Expanding that experiment from smoke to full M4 Daily preserves completed
Stages 2–5, but transactionally resets Stage 6, removes smoke-only official
evaluations and registered subset exports, and returns the experiment to
`planned`. Full Stage 6 refuses to run unless each variant/candidate has exactly
4,227 unique forecasts at unique contiguous official positions. Every official
evaluation records that input count and a deterministic forecast-input
fingerprint; recomputation replaces an invalidated metric rather than silently
retaining it.
`sequential_safe` is the default execution profile. Profiles and manual
overrides are invocation controls: they are recorded but never enter experiment
or task identity. Larger profile values use bounded local workers for pure
computations, while R and model adapters keep internal CPU threading at one.
Stage 2 and Stage 4 split external R/model work into bounded `batch_size`
payloads. Up to `workers` payloads may run concurrently, but only the coordinator
touches DuckDB; it commits each returned result with its task completion. Thus a
later batch failure retains completed earlier batches and restart retries only
unfinished tasks.

The official framework reports M4 Daily seasonality through
`get_seasonality("D")` (currently `1` in the pinned environment). ShapeFM passes
that value to both `forecast::tsclean` and AutoARIMA and constructs their R
`ts` inputs with it; no frequency is hard-coded in the R worker.

The provisional demonstration candidate is identity cleaning,
min-max-then-standardize, identity adjustment, and equal-weight AutoARIMA plus
Chronos-2. It is not selected using test results and is not called “best.”
The provisional label is not part of scientific experiment identity, so any
completed variant can be frozen later without recomputing upstream results.

## Commands

```sh
# Plan first ten official instances
.tools/uv/uv run --locked shapefm-poc1 plan --scope smoke

# Run/resume all gates (sequential default)
.tools/uv/uv run --locked shapefm-poc1 run --profile sequential_safe

# Run/resume on the validated Mac profile
.tools/uv/uv run --locked shapefm-poc1 run --profile mac_m1pro_10core_16gb

# Inspect status and official results
.tools/uv/uv run --locked shapefm-poc1 status
.tools/uv/uv run --locked shapefm-poc1 results

# Validate/export the provisional development subset
.tools/uv/uv run --locked shapefm-poc1 export

# Materialise the full plan, or inspect it without writes
.tools/uv/uv run --locked shapefm-poc1 plan --scope m4_daily
.tools/uv/uv run --locked shapefm-poc1 plan --scope m4_daily --dry-run
.tools/uv/uv run --locked shapefm-poc1 plan --scope manifest
```

See [local execution](local-execution.md) for committed profile values,
accelerator validation, persistent Chronos batching, calibration, and the
Ubuntu CUDA validation procedure.

The subset exporter writes the official 15-column shape but marks its config
`NON_SUBMITTABLE_DEVELOPMENT_SUBSET` and records all missing manifest rows. A
complete export is rejected until every configuration in the pinned official
manifest is present exactly once with every official metric.
Submission metadata is configured in `config/experiments/poc1.json` and checked
against the pinned GIFT-Eval required fields, enumerations, and public-link
shape before export. It is currently marked `draft` and
`submission_approved: false`; because this repository is private,
`replication_code_available` is `No` and no public code link is claimed. A
future submission-ready export requires explicit user-approved organisation,
model name and type, model and public code links, and leakage declaration.

The complete manifest is read from the pinned framework's result shape and
qualified as `dataset/frequency/term`. ShapeFM scans every pinned
`results/*/all_results.csv`: all 117 files having the exact 15-column schema and
97 unique qualified configurations agree on one configuration set (among 132
files scanned). Manifest metadata records those sources, the consensus flag,
and a deterministic set hash. The selected reference rows are additionally
validated for finite metrics, domains, and variate counts.
POC 1 execution remains intentionally narrower: only the one-window
`m4_daily/D/short` configuration is accepted. Any multi-window configuration
is rejected rather than being silently treated as one window.

Future work may freeze any completed variant without rerunning unaffected
upstream tasks. Mantis, MOMENT, training, learned selection, NAS, and
multi-machine execution are deliberately POC 2 or later.
