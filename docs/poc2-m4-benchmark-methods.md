# POC2 M4 benchmark-method pool decision

Status: ID 018 approved by the researcher on 4 October 2026. Its first increment
authorised a bounded extension of the existing R forecast-method library. A
same-day activation addendum now authorises Naive2, SES, Holt and Damped in one
new experiment configuration and the official M4 Comb in Process 05. Existing
experiment definitions and accepted evidence remain unchanged.

## Purpose

Preserve the missing M4 statistical benchmark methods under ShapeFM's current
coding and scientific standards, then activate them through the normal ShapeFM
workflow in a new experiment. The initial library increment did not select,
schedule, persist or evaluate them; the activation addendum below does.

ShapeFM keeps one forecast-method pool per language while that remains practical.
The R implementations and their small private utilities therefore belong in the
existing `src/r/util/forecast_methods.R`; ID 018 does not create another R
forecast-method source file. A future split requires a demonstrated readability
or maintenance need and a separate reviewed change.

## Approved scientific scope

Add these point-forecast functions to the existing R pool:

1. `naive2_forec`
2. `ses_forec`
3. `holt_forec`
4. `damped_forec`

Add the private M4 preparation functions needed by those methods in the same
file, including:

- `.m4_seasonality_test`; and
- one small shared preparation/restoration implementation rather than repeated
  seasonal logic in each method.

The M4 seasonality test uses the official 90% autocorrelation rule with critical
value 1.645. Period one is nonseasonal without running the test. Fewer than three
complete cycles and an indeterminate result are nonseasonal. A seasonal series
uses classical multiplicative decomposition; forecasts are calculated from the
adjusted series and then reseasonalised.

The repository's `seasonal_period.R` has a different responsibility: it estimates
and diagnoses a candidate model period. It does not replace the M4 test, which
asks whether a series is seasonal given an already resolved period. The
researcher's historical R module and the official M4 R benchmark definition are
the sole scientific authorities for ID 018.

Theta, ETS and AutoARIMA already exist in the R pool. Chronos-2 already exists in
the native Python provider. ID 018 neither duplicates nor changes them and does
not restore the historical Reticulate bridge.

## Method contract and proportional design

The four added methods remain small mean-only scientific functions inside the
shared R library. Each receives one finite `stats::ts` and one positive forecast
horizon and returns one
finite numeric point-forecast vector of exactly that horizon. They do not mutate
the input or access configuration, files, DuckDB, Python, Prefect, Dask, future
actuals or evaluation state.

Use small stateless R functions, not a new custom class. This is the code
standard's practical exception for a bounded scientific kernel: `stats::ts`
already supplies the required native object, and another class would add objects
and indirection without useful state or behaviour. Inputs are not modified so R's
copy-on-modify semantics avoid avoidable copies. Decomposition is performed only
when the M4 test selects it, and shared preparation avoids repeated work within a
compatible bounded call.

Do not fabricate prediction intervals or quantiles. The approved activation
adapter wraps their validated means in the common `mean_only` result envelope
without changing the scientific functions or their outputs.

## Available versus active

`src/r/util/forecast_methods.R` is the single R forecast pool, but library presence
does not imply experiment activation:

- **Active and registered** methods appear in the applicable capability-aware
  allowlist, can be selected by validated configuration and can be scheduled by
  Process 04. The established nine-method function and order remain unchanged.
- **Available but unregistered** methods are implemented, documented and tested,
  but cannot be selected or scheduled.

The initial ID 018 implementation adds the four methods only to the second
category. The approved activation must preserve the existing nine-method order
while adding an explicit capability-aware allowlist for the four point methods.
It uses a new experiment JSON and new database; it must not reinterpret older
configurations, task cardinalities, accepted results or evidence.

## M4 Comb and component dependencies

M4 Comb is the equal one-third arithmetic combination of SES, Holt and Damped.
It is a combination recipe, not a model that may fit or recalculate its component
methods. Do not import the historical `comb_forec()` implementation that calls
the three model functions internally, and do not add another combination engine
in R. ShapeFM's Process 05 remains the sole calculation boundary.

```text
m4_comb:
  ses:     1/3
  holt:    1/3
  damped:  1/3
```

The approved activation uses this dependency flow:

```text
02 Preprocess -> 03 Transform -> 04 Forecast SES/Holt/Damped
                                    |       |       |
                                    v       v       v
                              stored component forecasts
                                            |
                                            v
                          05 Combine: M4 Comb (1/3 each)
                                            |
                                            v
                         stored combination and component links
```

The new experiment configuration declares base forecasts and combinations
separately. Planning expands combination prerequisites, creates one Process 04
task per unique base method and deduplicates shared components. Process 05 runs
only after all required successful component forecasts are stored. It reads
those forecasts, calculates the combination once, stores a separate forecast row
and records every component forecast ID, name and weight in
`forecast_components`. It never invokes a forecasting model.

Consequently, a two-component combination retains three forecasts, while M4 Comb
retains SES, Holt, Damped and the combined forecast. Components remain separately
retrievable, evaluable and reusable. Resume skips accepted components and an
accepted combination; missing prerequisites are completed before combination.
Changed methods or weights require a new experiment definition. The exact M4
benchmark components do not use fallback; a component failure remains explicit
and prevents its dependent combination from running.

M4 Comb and its three components use the approved `mean_only` forecast contract:
mean is required, while median, quantile levels and quantiles are null. Process
06 uses the separately labelled mean-based metric profile and never fabricates
uncertainty. The package `forecastHybrid` is explicitly excluded because it
supports a different component set, refits models from the series and does not
implement the official M4 benchmark.

## Approved execution validation

Repeat the bounded first-100-series M4 Daily execution on Mac and Ubuntu through
the normal entry point and approved execution profile. The new configuration
contains the nine existing R methods, Chronos-2, Naive2, SES, Holt, Damped and
M4 Comb. The prior ten-way equal-weight experiment remains historical evidence
and is not substituted for M4 Comb in this run.

Acceptance concerns execution, coordination and collaboration rather than model
accuracy: planned tasks complete on eligible workers; both machines contribute;
the GPU route handles Chronos; the Mac remains the only database writer; Process
05 waits for three stored components; combination lineage and one-third weights
are exact; failures, duplicates and orphan rows are absent; capability-appropriate
evaluations complete; and a repeated run skips accepted work without changing
forecasts, links, evaluations or fingerprints. No ranking or accuracy threshold
is an acceptance criterion.

## Implementation validation

The bounded implementation must demonstrate that:

- the official and historical R calculations agree on deterministic period-one,
  short, seasonal, nonseasonal and approved boundary fixtures within an explicit
  numerical tolerance;
- SES, Holt and Damped reproduce their official M4 point definitions on matched
  inputs and package versions;
- every output is finite, numeric and exactly horizon length;
- input `stats::ts` objects and their frequency are not modified;
- shared seasonal preparation is used rather than copied into each public method;
- the existing nine registered method identifiers, order and callables are
  unchanged;
- all four point methods use the same language-neutral result field set as the
  Python and probabilistic R providers, with null probabilistic values;
- the new configuration, planner, worker, task, storage and point-evaluation
  paths select the point methods without changing older experiments;
- M4 Comb consumes only the three stored component means and no R function or
  Process 05 code refits those components;
- `forecastHybrid` is absent from source, environments and locks; and
- focused contract, R, orchestration, storage and evaluation tests pass before
  the authorised two-machine execution.

Update `forecast-methods.md`, architecture, configuration and execution evidence
to describe the final implemented state. Keep code documentation concise and
follow the source header and function-contract standards. Report any conflict
between the official M4 definition and historical project behaviour instead of
choosing one silently.

The approved activation authorises one local checkpoint commit and exact-revision
Ubuntu synchronisation for the bounded execution test. It does not authorise a
GitHub push, tag, dependency installation or accuracy study. Technical review
and independent researcher QA remain required before publication.

## Sources

- [Official M4 benchmark implementation](https://github.com/Mcompetitions/M4-methods/blob/master/Benchmarks%20and%20Evaluation.R)
- [M4 Competition paper](https://doi.org/10.1016/j.ijforecast.2019.04.014)
- [`forecastHybrid::hybridModel()` documentation](https://search.r-project.org/CRAN/refmans/forecastHybrid/html/hybridModel.html)
- [`forecast.hybridModel()` interval documentation](https://rdrr.io/cran/forecastHybrid/man/forecast.hybridModel.html)
