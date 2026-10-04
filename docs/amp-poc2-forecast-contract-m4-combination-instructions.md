# AMP instructions for the forecast contract and M4 combination

## Objective

Implement the approved language-neutral forecast black-box contract, activate
the four M4 point methods already present in the R pool, implement the official
M4 Comb from stored component forecasts, and repeat the bounded first-100-series
M4 Daily execution on Mac and Ubuntu.

The acceptance objective is reliable execution, coordination, worker
collaboration, persistence and restart. It is not a comparison of forecast
accuracy.

## Governing decisions

Read these documents completely before editing:

1. `AGENTS.md`
2. `docs/code-standards.md`
3. `docs/forecast-black-box-contract.md`
4. `docs/poc2-m4-benchmark-methods.md`
5. `docs/poc2-workflow-orchestration-decision.md`
6. `docs/execution-policy.md`

Preserve the current uncommitted candidate and unrelated accepted work. Audit
the complete starting diff before editing. Do not reset, discard, reinstall or
rewrite accepted databases and evidence.

## Scientific boundary

Do not install, import or add `forecastHybrid`. It does not implement the
official M4 Comb.

The official calculation is fixed:

```text
m4_comb = (ses + holt + damped) / 3
```

Naive2, SES, Holt and Damped use the already implemented official M4 seasonal
test, adjustment and restoration logic. Do not change their validated means.
They and M4 Comb are `mean_only`: mean is present; median, quantile levels and
quantiles are null. They receive no fabricated intervals.

## Common request and result contract

Implement one versioned serialized request and one versioned serialized result
shape for R and Python providers as defined in
`docs/forecast-black-box-contract.md`.

- Every per-task request carries the same identity, context, horizon, frequency,
  resolved seasonal period, scale, capability, seed and settings fields.
- Every per-task success returns the same top-level fields in both languages.
- Provider-specific values remain inside `provenance` or the outer batch
  envelope; they do not create different scientific contracts.
- `probabilistic` results contain mean, median, configured quantile levels and a
  levels-by-horizon quantile matrix.
- `mean_only` results use the same fields but set median, quantile levels and
  quantiles to null.
- The coordinator validates identities, capability, scale, horizon, finite
  values, shapes, q0.5 equality and noncrossing quantiles before storage.
- No worker reads or writes DuckDB. The Mac coordinator remains the sole writer.

Use the simplest shared object/validator that satisfies the approved OOP and
pragmatic standards. Do not create parallel contract implementations or
unnecessary forwarding modules.

## Probabilistic output profile

Update the central probabilistic profile so providers directly return all
values required by the approved metrics:

```text
0.025, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.975
```

The nine R probabilistic methods and Chronos-2 must produce these levels through
their fitted distribution, intervals or simulations. The evaluator may reshape
them but must not interpolate or extrapolate missing forecast values. Weighted
quantile loss continues to use only q0.1 through q0.9; MSIS uses the supplied
q0.025 and q0.975. Preserve each method's approved mean and model settings.

## Process 04 activation

Add Naive2, SES, Holt and Damped to a capability-aware configured allowlist
without changing the established order or identity of the existing nine R
methods. Route them through the normal R worker, Dask scheduling, coordinator
validation, inverse transformation and single-writer commit path.

For each of the first 100 series, Process 04 must store fourteen independent base
forecasts:

- nine existing R probabilistic candidates;
- Chronos-2;
- Naive2;
- SES;
- Holt; and
- Damped.

The four M4 point methods must not use the existing seasonal-naive fallback. A
failure remains an explicit failed task rather than a different benchmark hidden
under the requested identity.

## Process 05 M4 combination

Add one configured `m4_comb` recipe with SES, Holt and Damped weights of exactly
one third. Do not call R and do not refit a component in Process 05.

For every forecast instance:

1. require the three successful stored component rows;
2. read their original-scale mean vectors;
3. require equal horizons and finite values;
4. calculate the corresponding arithmetic mean once;
5. store a separate `mean_only` `m4_comb` forecast; and
6. store exactly three `forecast_components` links with the component IDs, names
   and weights.

The combination identity and content hash must include the rule, ordered
components, weights, capability, scale and output. Restart must not recalculate
an accepted combination.

Do not include the previous ten-way `equal_weight` candidate in the new
acceptance configuration. Preserve its code, configuration and historical
evidence unchanged; it was a different experiment.

## Process 06 capability-aware evaluation

Keep the full probabilistic evaluation for the ten probabilistic base models.
Use q0.1 through q0.9 for weighted quantile loss and the directly supplied
q0.025/q0.975 for MSIS.

Add a separately named mean-based evaluation profile for Naive2, SES, Holt,
Damped and M4 Comb. Configure MSE, MAE, MASE, MAPE, sMAPE, RMSE, NRMSE and ND to
use the stored mean. Do not calculate MSE at q0.5, MSIS or weighted quantile loss
for these records. Store the evaluation profile and capability in provenance,
and keep the bounded run non-submittable.

## New experiment configuration

Create a new configuration version and a fresh isolated DuckDB. Do not change or
reinterpret version 8 or its evidence. Reuse the same first-official 100 M4 Daily
series, preparation variant, seed and approved execution profile so the new run
tests the expanded forecast graph rather than a different dataset or workflow.

Expected final cardinalities are:

- 1,400 Process 04 base forecasts;
- 100 Process 05 M4 Comb forecasts;
- 1,500 total forecast rows;
- 1,000 probabilistic rows;
- 500 mean-only rows;
- 300 M4 component-link rows; and
- 15 capability-appropriate evaluation rows.

Treat these as graph and storage assertions, not accuracy targets.

## Tests before distributed execution

Add focused tests for:

- identical R and Python request/result field sets;
- success and error envelopes;
- probabilistic and mean-only validation;
- direct q0.025/q0.975 production and metric consumption;
- no evaluator interpolation or extrapolation;
- activation and storage of all four M4 point methods;
- dependency planning and deduplication;
- Process 05 refusing missing, failed, wrong-horizon or non-finite components;
- exact M4 arithmetic and three-link lineage;
- capability-aware Process 06 selection;
- idempotent restart and stable hashes; and
- preservation of older configuration behaviour.

Run the relevant locked fast and integration suites without synchronising or
installing environments. Validate Python compilation, R parsing, JSON contracts
and whitespace. Use a small local fixture before the distributed run.

## Repeated two-machine execution test

After local validation, create one reviewable checkpoint commit containing only
the approved candidate and documentation. Synchronise that exact revision to
Ubuntu without pushing to GitHub. Do not begin remote work until Ubuntu is
available, its OS maintenance is complete, both worktrees are clean at the same
revision, environments and data are present, and the execution-policy preflight
passes.

Run the new first-100-series experiment through `src/python/00_main.py` using the
approved two-machine execution profile. Do not introduce a second entry point or
manual worker-only scientific path.

The evidence must demonstrate:

1. the configured Mac CPU, Ubuntu CPU and Ubuntu GPU worker pools registered;
2. both Mac and Ubuntu CPU workers completed eligible R tasks;
3. Ubuntu GPU workers completed Chronos tasks;
4. the Mac coordinator was the only DuckDB writer;
5. Processes 01 through 06 followed the normal dependencies;
6. all expected tasks and rows completed with no failures, duplicates or orphans;
7. every M4 Comb started only after its three stored components existed;
8. all 100 M4 Comb means equal the stored one-third arithmetic result within an
   explicit floating-point tolerance;
9. all 300 component links use the configured one-third representation and each
   three-weight sum equals one within an explicit floating-point tolerance;
10. all capability-appropriate evaluations completed with finite values;
11. memory, spill, worker-removal and GPU safety checks passed; and
12. an immediate repeated invocation selected no completed scientific work and
    left row counts, hashes, timestamps and evaluation fingerprints unchanged.

Do not rank models, compare which model is more accurate or impose an accuracy
threshold. Metric values are execution outputs only in this acceptance.

## Stop and report

Stop without GitHub push after the distributed test. Report:

- exact commit and source manifest;
- configuration and scientific fingerprints;
- worker topology and per-host/per-resource task contribution;
- process/task/forecast/evaluation cardinalities;
- capability and shape validation;
- M4 dependency, formula and lineage evidence;
- failures, retries, duplicates and resource evidence;
- restart evidence; and
- any limitation or deviation.

Do not push, tag, install packages, change accepted evidence or begin accuracy
analysis without a new Chief Developer approval.
