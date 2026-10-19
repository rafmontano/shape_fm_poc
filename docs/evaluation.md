# Evaluation, sensitivity and research outputs

Process 06 evaluates stored results. It never refits forecast models and never
changes stored forecasts.

## Direction convention

The research definition is exact:

```text
sign(y) = 1  when y > 0
sign(y) = -1 when y <= 0
```

There is no near-zero class, numerical tolerance or special validation. Stored
binary classifier labels may encode the same classes as `1` and `0`; reporting
maps them consistently.

Preserve the previous project's two evaluation bases rather than merging them:

- Direct DTW/Mantis classifier evaluation, including their horizon-accuracy and
  rank inputs, compares predictions with labels defined against the final
  prepared historical input value. Reuse the official Process 03 labels, derived
  from raw official actuals and the prepared endpoint. The authorities are
  `m4_tsc_fmts_2026/src/r/10a_build_real_eval_parallel.R`,
  `src/r/13_baseline_dtw_real.R` and `src/python/run_mantis_experiment.py`.
- Paper Tables 1/2 and sensitivity retain the raw historical endpoint for
  actual directions, point-forecast directions and adjustment decisions. This
  includes standalone Mantis terminal DA in Table 2. The authorities are
  `src/r/sensitivity/05_run_lambda_sensitivity.R` and
  `src/r/sensitivity/05a_build_tables1_2_from_05.R` in the previous project.

Identify the evaluation basis in stored scores and report provenance; do not
overwrite or reuse one basis's counts for the other. Both consume the same stored
predictions: no second forecast, representation or classifier calculation is
needed. Prepared-origin horizon accuracy need not equal raw-origin Table 2 DA.
This restores the historical distinction, not a new choice of metric.

## Evaluation profiles

GIFT-Eval is the official benchmark evaluator for compatible forecast outputs.
Its configured invalid-label masking and benchmark seasonality remain separate
from R model seasonality.

The M4 paper-table profile calculates full-horizon sMAPE, MASE, OWA relative to
Naive2 and terminal-horizon directional accuracy. Daily MASE uses lag 1. The
profile is distinct from ordinary GIFT-Eval metrics and is identified explicitly
in stored results.

### Naive2 reference for paper OWA

The paper benchmark uses Naive2 calculated from the original canonical historical
observations up to the forecast origin, without cleaning or standardisation.
Process 04 calculates and stores this forecast; Process 06 only evaluates it.
Use the previous paper project's Naive2 method and R seasonality convention
(M4 Daily: period 1, horizon 14). GIFT-Eval remains the observation authority;
this does not authorize importing M4comp2018 histories or comparing sources.
FFORMA and SMYL remain unchanged archived M4comp2018 forecasts from Process 01.

OWA is one half of the sum of cohort-mean sMAPE divided by Naive2 cohort-mean
sMAPE and cohort-mean MASE divided by Naive2 cohort-mean MASE. Do not average
per-series OWA, change this formula, or substitute a cleaned-history benchmark.
The previous authority is `m4_tsc_fmts_2026`: `02_add_smyl_forecasts.R`,
`01_fct_methods.R`, `05_run_lambda_sensitivity.R`, and
`results/paper/tables/table1_daily_full_precision.csv`.

## Sensitivity and selection

Chronos retains the corrected raw-history forecast route. The fresh run approved
in [POC2](poc2.md) replaces previous generated results; old selective-correction
instructions do not authorize reusing previous fitted models or predictions in
that run. During the new run, ordinary durable restart still reuses completed work.

The approved Mantis adjustment uses the final-horizon Mantis direction to
select one scalar for the complete point-forecast path:

```text
base_direction = 1 when forecast[H] > last_observation, otherwise 0
scale = 1           when base_direction agrees with Mantis
scale = lambda_up   when they disagree and Mantis predicts up
scale = lambda_down when they disagree and Mantis predicts down
adjusted = scale * base_forecast
```

The ex-post grid is:

```text
lambda_up:   1.000, 1.005, ..., 1.120  (25)
lambda_down: 1.000, 0.995, ..., 0.900  (21)
```

SMYL and Chronos-2 each evaluate all 525 pairs: 1,050 sensitivity candidates. Selection is
from the stored surface, ordered by lowest OWA, distance from `(1,1)`, upward
departure, downward departure and stable lambda values. Only selected adjusted
forecast vectors are retained. This is sensitivity analysis using realized actuals, not
a deployable forecast-time method.

SMYL Oracle is also an ex-post sensitivity analysis: it evaluates the approved fixed
multiplier grid and selects the first minimum sMAPE. It is never presented as an
available forecast.

## Tables

One selected-results object supplies both tables:

- Table 1: complete approved M4 Daily methods and metrics.
- POC2 Table 2: Daily column only for terminal DA, full-horizon OWA and OWA
  improvement relative to SMYL.

POC2 never constructs `All` from Daily. POC3 adds all approved M4 frequencies,
weights frequency metrics by series count and recalculates OWA from aggregated
sMAPE/MASE; it does not average frequency OWA values.

## Figures and files

All researcher-facing CSV, PDF and PNG files are derived from stored DuckDB
results and live only in:

```text
results/tables/
results/figures/
```

No root-level `Rplots.pdf` or experiment/evidence subfolders are permitted.
Export is read-only with respect to DuckDB and does not schedule scientific work.

Final Process 06 delivery copies the published tables and figures from the
coordinator to the same repository-relative locations on each participating
worker host, once per host rather than per worker process. Use existing SSH/rsync
within the publication/completion sequence; do not add a scientific subflow or
transfer framework. Single-host execution requires no copy. The coordinator is
authoritative and copies never flow back automatically. An unavailable host is
reported as delivery pending without invalidating accepted science. Delivery
retry copies existing outputs only, without recalculation or regeneration, even
when scientific milestones are already complete. Copy only declared publication
files; do not broadly delete destination content. These are output copies, not
full experiment backups: databases, models and runtime logs are excluded.

POC2 Figure 2 has separate files, not a combined panel:

- directional accuracy by horizon; and
- the configured descriptive critical-difference/rank view.

The research and paper CD versions may initially contain the same data but remain
separate output files. Daily horizons share series, so ranks are descriptive and
are not claimed as independent-sample significance evidence.

Presentation is part of reproduction: filenames, labels, fonts, colours,
dimensions and layout match the approved reference. A discrepancy is recorded
and discussed; it is not silently redesigned.

## Parallel execution and restart

Prefect owns Process 06 dependencies. Dask distributes bounded independent
candidate/metric batches across eligible CPU workers; workers do not start nested
parallel frameworks. The coordinator writes aggregate results. Each atomic item
is calculated once on its assigned worker; ordinary valid floating-point
differences are accepted, without parity workloads or tolerance gates.

Sensitivity follows the placement principle in [architecture](architecture.md#placement-and-degree-of-parallelism):
the same bounded candidate batches can run on one or several machines through
the shared Prefect/Dask executor. Selected-coordinator CPU workers are the
operational placement for 06.03, retaining local parallelism and the existing R
kernel. There is no fallback to peer workers. Other provider placements and the
approved scientific batch definitions are unchanged.
Successful completion of the declared outputs and no-new-work restart resolves
the sensitivity execution issue for POC2. The underlying Ubuntu crash cause was
not established; this is a limitation, not an open investigation, backlog item or
acceptance blocker. The fresh Ubuntu-coordinated full-Daily exercise is approved
to execute sensitivity on Ubuntu; assess any new failure using its own evidence.

Each candidate identity is committed independently. Restart submits only missing
candidates and continues to selection, tables and exports once prerequisites are
complete.

The durable identifiers remain `06.03` (Materialise sensitivity surface) and
`06.04` (Select and retain sensitivity results). The complete surface contains
1,057 sensitivity candidates, including the seven baseline/oracle candidates.
Candidates and reports retain scientific identities and lineage, not internal
input/result/report fingerprints. Horizon accuracy is derived from stored
correct and evaluation counts; reports do not store and police a duplicate
accuracy value. This does not change sensitivity execution or selection.

## Final review

For the current methodology correction, the researcher explicitly requests
comparison with saved results from `m4_tsc_fmts_2026`. Preserve that project's
source and results during ShapeFM cleanup. Compare every corresponding Daily
Table 1 and Table 2 result and direct-classifier horizon result on the same
evaluation basis. Report prior/current values, absolute differences, meaningful
relative differences and percentage-point differences for directional accuracy.
Use full precision when available; distinguish paper rounding from substantive
differences. Match method, dataset/frequency, horizon, cohort and evaluation basis;
do not compare rank values from different model pools as if they were equivalent.

Use existing saved per-series results to localise differences where available.
For unexplained differences inspect the old and new source paths and report
evidence, not speculative causes. Retained tsai membership can change training
examples even after restoring historical cap rules; historical unrecorded model
revision/precision remains an uncertainty. A matching aggregate alone does not
prove every underlying prediction matches. Mark unavailable or incomparable
reference results explicitly rather than silently passing them.

Do not rerun the previous experiment, build a permanent comparison framework,
invent acceptance tolerances, or tune outputs to match. Small development
fixtures may check the restored DTW kernel against the previous R calculation;
no full-workload parity run is required. Present the comparison with produced
outputs for researcher acceptance. Do not claim scientific equivalence solely
because execution completed. Any further scientific recalculation or methodology
change identified during review requires direction.
The current fresh-run authorization is defined in [POC2](poc2.md); it is not a
standing permission to reset experiments or repeat scientific workloads.
