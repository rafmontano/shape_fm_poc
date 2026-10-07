# POC2 ID 033 directional forecast metrics

Status: ID 033 closed on 7 October 2026 after the Researcher and Chief Architect
accepted the architecture, implementation and local validation. The published
capability remains dormant; acceptance does not claim scientific accuracy.

## Decision

Import the useful directional calculations from the historical
`m4_tsc_fmts_2026/src/r/forecast_methods3.R` as one dormant, model-neutral R
utility. Do not add the capability to the active six-process pipeline or the
FFORMA feature provider.

The import preserves an inconclusive research option without duplicating
forecast models or presenting it as established meta-learner evidence. It must
consume comparable forecast means already calculated by ShapeFM. It must not
fit AutoARIMA, ETS or any other model.

## Historical responsibility assessment

| Historical function | Decision | ShapeFM responsibility |
| --- | --- | --- |
| `vec_to_ts2` | Do not import | Use the existing central R time-series input and period conversion. |
| `MDA2` | Import and rename | Preserve the historical mean directional accuracy calculation. |
| `MDV2` | Import and rename | Preserve the custom signed directional-value calculation. |
| `MDPV2` | Import and rename | Preserve the custom signed percentage directional-value calculation with explicit zero-denominator handling. |
| `PT2` | Import and rename as legacy | Preserve the historical simplified statistic without claiming canonical Pesaran-Timmermann equivalence. |
| `PT_pvalue2` | Import and rename as legacy | Preserve its two-sided normal p-value calculation. |
| `auto_arima_forec` | Do not import | Use the existing ShapeFM AutoARIMA forecast result. |
| `ets_forec` | Do not import | Use the existing ShapeFM ETS forecast result. |

The historical AutoARIMA wrapper also omitted the approved FFORMA configuration
now used by ShapeFM. Importing it would create a second scientific definition.
The existing forecast pool remains authoritative.

## Scientific meaning and limits

Every metric compares `diff(actual)` with `diff(forecast)`. For a realized
future path of length `h`, this produces `h - 1` comparisons. It does not compare
the last observed context value with horizon one, and a one-step horizon has no
comparison under this historical definition.

- Mean directional accuracy records matching signs. Two zero changes count as
  a match, preserving the historical tie behavior. Optional reward and penalty
  values retain the historical remapping.
- Mean directional value signs the absolute realized change positively for a
  directional match and negatively for a mismatch. Its units are the units of
  the series.
- Mean directional percentage value applies the same sign to
  `abs(actual change / destination actual) * 100`. A zero destination actual
  makes this metric unavailable rather than infinite or silently zero.
- The historical PT calculation removes zero directions and calculates a
  simplified z-style statistic and two-sided normal p-value. Degenerate inputs
  make both values unavailable. It is versioned and named as a legacy metric;
  a future canonical Pesaran-Timmermann implementation would be a separate
  research decision and metric ID. The canonical reference is
  [Pesaran and Timmermann (1992)](https://doi.org/10.1080/07350015.1992.10509922).

These are retrospective metrics because they require the realized future path.
They are not live prediction features. ID 033 does not resolve the historical
semantic mismatch: training used each window's actual future, while real
prediction constructed a historical pseudo-holdout. The eight fields recorded
in [the ID 014 feature decision](poc2-features.md) therefore remain disabled.

The existing DTW/Mantis directional labels and accuracy evaluate binary classes
independently by horizon. They are not duplicates of these within-path forecast
change metrics and remain unchanged.

## Utility and interface

Create one unnumbered shared utility:

```text
src/r/util/directional_forecast_metrics.R
```

The file belongs under `util` because the calculations are independent of one
process and may later support different research workflows. It has no gate
number because it is not an executable gate or substep. Do not add another
folder or place it in `features.R`, whose accepted responsibility is the
context-only 42-field FFORMA feature set.

Use small documented stateless functions:

```text
legacy_mean_directional_accuracy
legacy_mean_directional_value
legacy_mean_directional_percentage_value
legacy_pt_statistic
legacy_pt_p_value
calculate_directional_forecast_metrics
```

This is the code standard's bounded functional exception: the module is a small
collection of pure scientific kernels with no durable state or side effects. A
class would add ceremony without useful ownership or lifecycle.

The public aggregator contract is:

```r
calculate_directional_forecast_metrics(
  actual,
  forecasts,
  reward = 1,
  penalty = 0
)
```

Inputs:

- `actual`: a finite numeric realized future path on its original scale;
- `forecasts`: a non-empty named list of finite numeric point-forecast mean
  vectors on the same original scale and with the same length as `actual`;
- `reward` and `penalty`: finite scalars used only by the historical MDA
  remapping.

The utility validates names, finite values, equal lengths and length of at least
two. It accepts one or many model results and never assumes AutoARIMA and ETS
are the only candidates.

Return one ordinary JSON-compatible R list with this stable meaning:

```text
metric_set_id: legacy_directional_forecast_v1
comparison_definition: within_future_path_changes_v1
actual_length: h
comparison_count: h - 1
models:
  <candidate name>:
    values: mda, mdv, mdpv, legacy_pt_statistic, legacy_pt_p_value
    unavailable: named reasons for values represented internally as NA_real_
```

The R utility may use `NA_real_` for an unavailable value only when the same
result includes its explicit reason. A future language adapter must serialize
that value as JSON `null`, never a non-standard `NaN` or `Infinity` token.

The metric-set and comparison-definition identifiers are code constants. They
version the imported mathematics; they are not new experiment globals.

## ShapeFM boundary

The approved dormant relationship is:

```text
stored original-scale forecast means (one or many candidates)
                         +
              realized future path
                         |
                         v
       directional_forecast_metrics.R
                         |
                         v
        versioned in-memory metric result
```

There is no database write, JSON worker, Prefect flow, Dask task, configuration
field, CLI command or model registration in ID 033. No production module may
source the utility during normal import, preprocessing, transformation,
forecasting, combination, evaluation, feature preparation or directional-model
execution.

If activated later, the approved starting concept is to retrieve stored
original-scale forecasts, join them to a consistently defined realized
historical holdout, calculate a separately versioned feature set and persist it
through the normal coordinator-only storage boundary. Activation first requires
an explicit leakage-free rule that supplies information with the same meaning
during meta-training and real prediction.

## Validation and acceptance

Add one focused test file:

```text
src/r/tests/test_directional_forecast_metrics.R
```

Tests must prove:

1. exact parity with the historical formulas on deterministic finite fixtures;
2. one and multiple named forecast candidates produce isolated results;
3. ties preserve the historical sign-comparison behavior;
4. unequal lengths, unnamed candidates, duplicate names, non-finite inputs and
   paths shorter than two fail clearly;
5. zero MDPV denominators are explicitly unavailable, never infinite or zero;
6. empty or degenerate PT comparisons are explicitly unavailable;
7. metric-set IDs, comparison count, output names and unavailable reasons are
   stable; and
8. the utility has no `forecast` dependency, model fitting, database access,
   workflow integration or active feature-provider reference.

Repository tests must contain fixed fixtures and expected values; they must not
depend on the previous repository at runtime. A one-time read-only side-by-side
comparison with the historical source may supplement, but not replace, the
committed parity tests.

Acceptance is functional preservation and architectural isolation. It does not
claim that these metrics improve XGBoost, validate forecasting accuracy, or
approve their use as meta-learner inputs.

## Accepted implementation evidence

The accepted implementation adds only:

```text
src/r/util/directional_forecast_metrics.R
src/r/tests/test_directional_forecast_metrics.R
```

The five kernels preserve the historical finite-input arithmetic, and the
model-neutral aggregator implements the approved validation, version IDs,
multi-candidate result envelope and explicit unavailable reasons. No active
production source refers to the utility.

Accepted local evidence on 7 October 2026:

- all twelve focused directional-metric test groups passed under the project R
  environment and dependency-free `Rscript --vanilla`;
- the deterministic fixtures matched the historical calculations, including
  reward/penalty remapping;
- existing R feature and forecast-method suites passed;
- eight relevant locked/no-sync Python regression tests passed;
- both new R files parsed and whitespace checks passed;
- dependency locks remained unchanged; and
- inspection confirmed no model fitting, added dependency, database access,
  workflow activation or feature-schema change.

The Researcher completed human QA and approved ID 033 on 7 October 2026. A
100-series, distributed or accuracy experiment is not required because this is
a dormant dependency-free utility, not an activated scientific workflow.

## Explicit exclusions

ID 033 does not:

- activate or store the eight disabled directional features;
- change the 42-field FFORMA feature schema;
- change forecast methods, settings, fallbacks or stored forecasts;
- introduce model-specific fitting or a second AutoARIMA/ETS implementation;
- change experiment JSON, DuckDB schema, Prefect, Dask or the six processes;
- define a canonical Pesaran-Timmermann implementation; or
- close the research question about whether directional metrics improve model
  selection.
