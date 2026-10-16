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

## Evaluation profiles

GIFT-Eval is the official benchmark evaluator for compatible forecast outputs.
Its configured invalid-label masking and benchmark seasonality remain separate
from R model seasonality.

The M4 paper-table profile calculates full-horizon sMAPE, MASE, OWA relative to
Naive2 and terminal-horizon directional accuracy. Daily MASE uses lag 1. The
profile is distinct from ordinary GIFT-Eval metrics and is identified explicitly
in stored results.

## Sensitivity and selection

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
the shared Prefect/Dask executor. For the current experiment's recovery, restrict
06.03 to Mac CPU workers, retaining local parallelism and the existing R kernel.
Ubuntu sensitivity placement remains disabled pending resolution of its worker
crashes; other provider placements and the approved worker profile are unchanged.

Each candidate identity is committed independently. Restart submits only missing
candidates and continues to selection, tables and exports once prerequisites are
complete.

The durable identifiers remain `06.03` (Materialise sensitivity surface) and
`06.04` (Select and retain sensitivity results). The complete surface contains
1,057 sensitivity candidates, including the seven baseline/oracle candidates.

## Final review

Present the final produced tables and figures to the researcher. Do not run
automatic historical comparison or a second scientific workload for review.
Stop for a material final-result issue only when the researcher identifies it.
E/F acceptance continues accepted Process 04 evidence through Combine/Evaluate
only; it does not rerun Forecast or restart the experiment from zero.
