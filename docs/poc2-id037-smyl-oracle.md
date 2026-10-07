# POC2 ID 037: SMYL Oracle diagnostic

## Status

The architecture was approved and the bounded dormant implementation was
accepted by the researcher on 7 October 2026 after Chief Architect review. The
accepted candidate is authorised for normal publication and Ubuntu source
synchronisation. This does not authorise pipeline activation, schema changes or
experiment execution.

## Approved decision

Import the unique SMYL Oracle calculation from the historical
`src/r/sensitivity/04a_add_smyl_oracle.R` as a small dormant R utility. Preserve
its calculation exactly on valid inputs, but do not import the legacy sensitivity
runner, RDS storage, mutable globals or execution settings.

The Oracle is an ex-post evaluation diagnostic. It is not a deployable forecast,
forecast provider, training feature, meta-learner input or forecast combination.

## Research purpose

The historical Oracle measures how well the stored SMYL forecast could perform
if its multiplicative scale were selected with knowledge of the realised future.
It supports reproduction of the relevant Table 1 diagnostic and provides an
upper-bound comparison. It does not represent information available at forecast
time.

## Scientific calculation

For one series, let `a[1:H]` be the realised future and `s[1:H]` the stored SMYL
mean forecast. The fixed multiplier grid is:

```text
G = {0.500, 0.501, ..., 1.499, 1.500}
```

It contains exactly 1,001 values. For every `g` in `G`, calculate the historical
full-horizon objective:

```text
smape(g) = mean(200 * abs(a - g*s) / (abs(a) + abs(g*s)), na.rm = TRUE)
```

Select the first exact minimum, matching R's historical `which.min()` behaviour.
The adjusted vector is `g*s`. Record the selected multiplier, objective sMAPE and
the number of grid values within `sqrt(.Machine$double.eps)` of the minimum.

The fixed method identity is:

```text
smyl_scalar_smape_oracle_v1
```

The grid is part of this versioned scientific definition. It is not a researcher
execution setting. A different range or step defines another method and requires
separate approval and identity.

## Input and output contract

The utility receives two finite, numeric, non-empty vectors of equal length:

- `actual`: the realised future from the canonical forecast instance;
- `smyl_forecast`: the stored `m4_smyl` mean for the same series and horizon.

It returns one result containing:

- `method_id`;
- `multiplier`;
- `objective_smape`;
- `near_tie_count`; and
- `adjusted_mean` with the same length as the inputs.

Invalid types, unequal lengths, empty vectors and non-finite values fail clearly.
Historical zero-over-zero terms remain omitted by the Oracle-specific objective.
If no finite comparison remains, the result must be explicitly unavailable or
fail; it must not select an ambiguous multiplier.

## Six-process ownership

```text
Processes 01-04
    canonical actual future + stored m4_smyl mean
                         |
                         v
                 Process 06 Evaluate
                  /                 \
       ordinary SMYL metrics    SMYL Oracle diagnostic
                                1,001 fixed multipliers
                  \                 /
                   Table 1 evidence
```

Process 06 owns future activation because the calculation requires realised
future values. It must not run in Process 04 Forecast or enter Process 05 as a
deployable combination or adjustment. The ordinary SMYL forecast remains stored
and independently retrievable.

## Source organisation

The proposed source location is:

```text
src/r/util/forecast_adjustments.R
```

Use one cohesive shared adjustment utility rather than one source file per
historical spreadsheet ID. Later approved adjustment calculations, including the
ID 038 Mantis adjustment, may share this file when that remains readable.

The Oracle kernel is a bounded practical functional exception under the code
standard: it is a small stateless calculation for which a class would add no
useful responsibility. It must still have concise Purpose, Inputs and Outputs
documentation.

## Configuration and legacy exclusions

Do not import `src/r/sensitivity/00_sensitivity_common.R`. ID 037 does not need
its scientific settings. ShapeFM already owns its relevant responsibilities:

- experiment dataset, frequency and series scope come from central configuration;
- horizon and actual values come from canonical forecast instances;
- `m4_smyl` identity and mean come from stored reference forecasts;
- Prefect owns process orchestration;
- Dask owns eligible task scheduling; and
- DuckDB owns experiment identities and durable research evidence.

Do not import:

- `PERIODS_TO_RUN`, test-subset or worker globals;
- M4 rank constants or lambda grids;
- frequency-to-path helpers;
- RDS readers, writers or directory creation;
- the per-frequency `lapply()` runner;
- mutable `s$fct` object updates;
- console progress messages or manual garbage collection; or
- the unused package dependencies loaded by the common sensitivity script.

## Persistence boundary

The first increment is dormant and does not change the DuckDB schema or active
pipeline. When Table 1 activation is separately approved, store the result as an
Oracle/evaluation diagnostic rather than an ordinary forecast. The durable record
must identify at least:

- experiment and forecast-instance identity;
- source `m4_smyl` reference-forecast identity;
- Oracle method identity and grid definition or fingerprint;
- selected multiplier, objective sMAPE and near-tie count; and
- adjusted-vector content or an unambiguous deterministic content hash.

Activation must preserve the original SMYL forecast and prevent Oracle results
from being consumed as deployable model output, training data or combination
input. The exact diagnostic table change belongs to the later joint Table 1
activation decision with IDs 038 and 041.

## Validation required for the dormant increment

1. Compare the new utility with the historical function on frozen asymmetric
   fixtures and confirm the complete result, not only the multiplier.
2. Cover first-minimum tie selection and near-tie counting.
3. Cover a known multiplier at each grid boundary and one interior multiplier.
4. Cover zero-denominator behaviour and the all-unavailable case explicitly.
5. Reject invalid, unequal-length and non-finite inputs clearly.
6. Confirm the source has no database, filesystem, model-fitting, package or
   orchestration dependency.
7. Confirm no production entry point sources the utility and no experiment
   configuration activates it.
8. Run the existing relevant R regression tests without changing scientific
   results or dependency locks.

## Definition of done

The dormant ID 037 increment is complete when the pure calculation and focused
tests reproduce the historical valid-input behaviour, documentation identifies
it as an ex-post diagnostic, and isolation checks prove that it is not active in
the six-process pipeline. Table 1 activation, persistence and comparative-result
acceptance remain a later decision with IDs 038 and 041.

## Accepted implementation evidence

Chief Architect review inspected the source and tests and independently reran
the validation on 7 October 2026. The researcher then approved the candidate.

- The focused ID 037 suite passed all 9 assertion groups.
- The ID 033 dormant-utility suite passed all 12 assertion groups.
- The existing forecast-method regression suite passed for all 9 registered
  methods.
- Frozen fixtures covered both grid endpoints, an interior optimum, exact and
  near ties, signed and zero paths, zero-over-zero omission and invalid inputs.
- The result contract contains exactly the approved five fields.
- The utility loads no package and has no filesystem, database, model-fitting or
  orchestration dependency.
- No production source or experiment configuration activates the utility.
- Configuration, schema and dependency locks remain unchanged.

This evidence accepts the dormant calculation and its isolation. It does not
claim Table 1 reproduction or scientific usefulness in an active experiment.
