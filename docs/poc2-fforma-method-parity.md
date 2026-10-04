# POC2 FFORMA method parity decision

Status: Approved by the researcher on 5 October 2026.

## Purpose

ID 018 preserves the scientific behaviour of the supplied FFORMA forecast
functions while adapting them to ShapeFM's standard request, result,
provenance, probabilistic-output and distributed-execution contracts.

For identical numeric input, `stats::ts` frequency, horizon, package version
and random seed where applicable, the ShapeFM point mean must reproduce the
corresponding FFORMA function within the approved numerical tolerance.

ShapeFM may add validation, serialization, provenance, quantiles and
orchestration. These additions must not change model selection, model
arguments, method-specific fallback or the point mean.

## Parity boundary

Parity requires:

- the same input values and `stats::ts` frequency;
- the same forecast horizon;
- the same forecasting function;
- the same scientific arguments and defaults;
- the same random seed and package version for stochastic methods;
- the same method-specific fallback; and
- the same point-forecast mean.

ShapeFM's median, quantiles and provenance are extensions. They must come from
the same fitted model or its approved method-specific fallback and must not
replace or alter the reference point mean.

SES, Holt, Damped and M4 Comb remain governed by the separate
[M4 benchmark-method decision](poc2-m4-benchmark-methods.md). They are not
redefined by this FFORMA parity decision.

## Approved method definitions

| Method | Approved scientific definition |
| --- | --- |
| Seasonal naive | Repeat the last seasonal cycle using `frequency(x)` and `tail()` |
| Naive | `naive(x, h=length(x))`, then `forecast(..., h=h)$mean` |
| AutoARIMA | `auto.arima(x, stepwise=FALSE, approximation=FALSE)` |
| ETS | `ets(x, opt.crit="mae")` |
| NNETAR | `nnetar(x)`, followed by the ordinary fitted-model forecast mean |
| TBATS | `tbats(x, use.parallel=FALSE)` |
| STL-AR | `stlm(x, modelfunction=stats::ar)`; on fitting error use `auto.arima(x, d=0, D=0)` |
| Random walk with drift | `rwf(x, drift=TRUE, h=length(x))`, then forecast to the requested horizon |
| Theta | `thetaf(x, h=h)$mean` |
| Naive2 | Supplied M4 seasonality test, optional multiplicative decomposition, naive forecast and reseasonalization |

Explicit arguments that equal package defaults are permitted only when they do
not change the calculation. The FFORMA baseline configuration must reject
scientific overrides that change these definitions. Alternative settings need
a different model-variant identity.

NNETAR uses the experiment seed so that the stochastic fit is reproducible.
The seed and package version are provenance, not a different forecasting model.
Predictive simulation may extend the output with median and quantiles, but the
stored mean remains the ordinary forecast from the same fitted NNETAR model.

## STL-AR fallback

The AutoARIMA fallback is part of the supplied STL-AR method:

```r
model <- tryCatch(
  forecast::stlm(x, modelfunction = stats::ar),
  error = function(e) forecast::auto.arima(x, d = 0, D = 0)
)
```

If STL succeeds, the executed calculation is STL-AR. If STL fails, the
executed calculation is AutoARIMA with fixed `d=0` and `D=0`. It is not
seasonal naive and it is not the separately configured AutoARIMA candidate.

ShapeFM records the selected branch, original STL error, settings and package
version. This makes the original hidden branch traceable without changing its
forecast values.

## Failure policy

The FFORMA method pool has no generic seasonal-naive substitution. A method:

1. runs its approved calculation;
2. uses only a fallback explicitly defined by that reference method; or
3. fails visibly under the workflow's task and retry policy.

Infrastructure retries and scientific model fallback are separate. A retry
must not change model identity or scientific settings. Output-validation errors
must not be converted into a different model forecast.

## Frequency is an input decision

Algorithm parity and experiment-input parity are separate.

- With period 1, both STL-AR implementations should select the approved
  `auto.arima(d=0,D=0)` fallback and return matching means.
- With period 7 and the same values, both should attempt STL-AR and return
  matching means or the same method-specific fallback if fitting fails.

The previous project represented M4 Daily with frequency 7. An experiment
intended to reproduce those results must provide the same frequency-7 input.
ShapeFM must not hide an input-frequency difference by changing a model.

## Required evidence

For every supplied method, compare the reference function and ShapeFM adapter
with identical values, frequency, horizon, package version and seed. Require
matching point means within `1e-10`, unless a different tolerance is explicitly
justified by numeric precision.

The tests must cover:

- period-1 and period-7 input;
- short and longer histories;
- the successful and fallback STL-AR branches;
- deterministic seasonal and nonseasonal Naive2 fixtures;
- stochastic NNETAR reproduction under the same seed;
- absence of pool-wide scientific substitution;
- visible failure for methods without a supplied fallback; and
- quantiles derived from the same fitted model without changing the mean.

The final ID 018 test uses a fresh database and the agreed 100-series,
all-model, M4-Comb experiment on Mac and Ubuntu. It reports every requested and
executed calculation, branch and fallback reason; revalidates M4 Comb lineage
and arithmetic; and proves restart without changing scientific fingerprints.

ID 018 closes only after this evidence passes and the researcher accepts it.
The number of STL fallbacks is not itself the criterion: every result must
follow the supplied STL-AR branch for the exact same input and record that
branch truthfully.
