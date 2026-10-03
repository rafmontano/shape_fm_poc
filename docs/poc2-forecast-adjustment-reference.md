# Historical forecast adjustments for Table 1

Recorded on 3 October 2026 for later POC2 migration. This reference preserves
the legacy SMYL–Mantis, Chronos-2–Mantis and SMYL–Oracle calculations. It does
not implement them, approve a new tuning experiment, or reopen accepted work.

The researcher confirmed that the Mac's saved legacy results are not the latest.
Their small difference from the paper is therefore not a migration blocker or
evidence of a new implementation defect. Use the appropriate latest reference
when validating migrated results; no new numerical tolerance is defined here.

## Mantis directional adjustment

Let `b[1:H]` be the original-scale baseline forecast, `r` the final observed
history value supplied by the legacy sensitivity dataset, and `m` Mantis's
binary predicted direction at the final horizon. The legacy comparison is
strict: `d = 1 if b[H] > r, otherwise 0`.

```text
gamma = 1            when d == m
gamma = lambda_up    when d != m and m == 1
gamma = lambda_down  when d != m and m == 0
adjusted[h] = gamma * b[h], for every h = 1,...,H
```

One final-horizon decision scales the entire forecast vector. This is not
independent adjustment at every horizon and does not add a percentage to `r`.
The same function serves SMYL and Chronos-2. The recorded Daily pair for both
was `lambda_up = 1.015`, `lambda_down = 1.000`, with `H = 14`. Thus downward
disagreement leaves values unchanged for that particular pair.

Historically, each method/frequency pair selected its lowest full-horizon OWA
over 525 pairs: upward 1.000–1.120 by 0.005 and downward 1.000–0.900 by 0.005.
Exact OWA ties prefer the pair closest to (1,1), then the smallest upward and
downward departures. Selection used evaluation outcomes and is exploratory,
not evidence of held-out tuning. Whether to reuse the recorded pair or repeat
selection remains part of the later ID 038 decision; the whole sensitivity
reporting pipeline need not be imported.

## SMYL Oracle

For each series, the legacy function tests 1,001 multipliers from 0.500 to 1.500
in increments of 0.001. It selects the multiplier giving minimum mean sMAPE
across the complete actual future and returns that multiplier times the entire
SMYL forecast vector. `which.min()` selects the first exact minimum. It also
records the multiplier, error and near-tie count.

The Oracle uses realised future values. Keep it explicitly separate from
deployable forecasts and training features. It is an ex-post scaling benchmark,
not simply a forecast supplied with the correct direction.

## Migration references

Sources below are in the unchanged `m4_tsc_fmts_2026` legacy repository:

- ID 036: `src/r/sensitivity/04_add_mantis_direction.R` joins Mantis predictions.
- ID 037: `src/r/sensitivity/04a_add_smyl_oracle.R`, `add_smyl_oracle_one()`.
- ID 038: `src/r/sensitivity/05_run_lambda_sensitivity.R`, `adjust_forecast()`.
- ID 041: `src/r/sensitivity/05a_build_tables1_2_from_05.R`, lambda selection
  and Table 1 assembly. Its separate sensitivity-summary script is not required.

The paper `step_in_the_right_direction.pdf` reports the Daily results in
Table 1 and the selected multipliers in Table 3. Table 1 reports directional
accuracy at horizon 14; sMAPE, MASE and OWA use all 14 horizons. Naive2 supplies
the OWA denominator even though it is not displayed as a Table 1 row.

POC2 scope is Table 1 and Figure 2's left panel only, as clarified by the
researcher. This note preserves required scientific logic, not legacy RDS
duplication or launchers. Implementation must use the existing stored forecasts,
identities, central experiment settings and Prefect/Dask workflow standard.
