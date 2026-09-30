# POC2 object adaptation and seasonal period

Approved on 30 September 2026. This decision applies the
[research vision](research-vision.md) through small adapters using the existing
POC2 infrastructure. It does not introduce a universal translation framework.

## Decision

DuckDB remains the authority for observations, dataset frequency and stored
experiment configuration. Reuse those records and existing result contracts.
Do not add duplicate frequency or seasonality storage to every result.

The shared R adapter constructs the `ts` input needed by the nine forecast-package
methods. The existing Chronos-2 adapter constructs its own required input.
Translation belongs at these boundaries, outside the forecasting methods.
Future libraries such as fable may have their own small adapter when brought
into scope; POC2 does not implement them in advance.

Resolve the period for R preprocessing and R forecasting in one place:

1. Use an explicit experiment override when supplied.
2. Otherwise call the pinned GIFT-Eval environment's GluonTS
   `get_seasonality()` using the stored dataset frequency.

Reuse the existing configuration and stored resolved configuration for this
decision. Worker requests may carry the resolved value; carrying a value to a
worker does not require adding another persistent data model. Models that do
not need this value need not receive it.

The resolver is a reproducible convention, not a statistical test that discovers
seasonality. FFORMA's method-specific seasonality test remains unchanged.
Benchmark evaluation keeps its own benchmark policy; a model or preprocessing
override must not silently change official scoring.

## Compatibility and scope

In the currently pinned environment, daily frequency resolves to 1. An explicit
override of 7 reproduces the former weekly-period cleaning setup when the same
observations, package versions and settings are used. These are distinct
experiments; their outputs are not expected to be identical.

This rule replaces the earlier proposal to obtain production R frequency from
M4comp2018. That package remains useful for official references and QA.
Agreement with original FFORMA must be tested using matched inputs and frequency.

The resolver and adapter pattern can be reused for other dataset frequencies.
This decision does not claim that all GIFT-Eval datasets are supported by the
current importer or every model. Unsupported shapes, frequencies or model
requirements must be identified explicitly, not silently coerced.

Existing experiment databases retain their scientific configuration and results.
Use a new experiment for a changed period policy. Retain existing schema fields
for compatibility; do not remove them merely to eliminate historical redundancy.

## QA objects

Preserve `src/r/qa/get_m4_daily_series.R` and the user's interactive workflow.
Sourcing it leaves `selected_m4_series` with the ten M4comp2018 components.
Historical and future observations come from DuckDB; package metadata, time
bases and submission matrices come from M4comp2018 after identity and value
checks. Its provenance must make these sources clear.

This comparison object deliberately preserves the source time base. It is not
the production adapter and must not be changed to use an experiment override.
Future actuals available for QA must not enter forecast-model inputs.

## Acceptance

- One shared resolver uses stored frequency and an optional experiment override.
- Existing R and Chronos adapters are reused where possible.
- New default daily resolution and explicit period-7 reproduction are tested
  separately, with equivalent inputs and versions for each comparison.
- Standard and robust preprocessing, forecast result contracts, fallback and
  official-reference retrieval remain intact.
- No new per-result duplicate storage, general adapter framework, or unnecessary
  cross-language object conversion is introduced.
- Existing experiments and user QA scripts remain usable.
- A separate vision-alignment review lists potential issues without fixing them.
