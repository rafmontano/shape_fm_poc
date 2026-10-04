# POC2 forecast-contract and M4-combination execution evidence

Executed on 4 October 2026. This record reports implementation and execution
evidence for configuration version 9. It is not ChatGPT technical acceptance or
researcher acceptance, and it does not compare or rank forecast accuracy.

## Scope and identity

The normal researcher entry point ran the first 100 official M4 Daily series
through one `robust` plus `standardise_sample_v1` variant, fourteen independent
Process 04 forecasts, the Process 05 `m4_comb`, and capability-appropriate
Process 06 evaluation.

- Runtime Git revision: `6b7c63e8ef3765c5995ffdd42875ef076a5cc376`.
- Runtime Git tree: `242bf78083664cf1aafc4047df30b46ae5621a16`;
  its 182-entry `git ls-tree -r HEAD` manifest has SHA-256
  `27bee5701bb099713f7bc5e6c7646ac0d096a91d9c211e0311e52534f7b3ded5`.
- Both Mac and Ubuntu were clean at that exact revision before execution.
- Configuration file SHA-256:
  `8c9366fbdb7f8f54bc60e56ef0452b1654d18eb09a06767cf65684c8603a85cb`.
- Scientific hash:
  `6e7edfd3d7705ea7172b3dc32bde04a1b843050eb455338e7ccd4ea3624a04ec`.
- Configuration-integrity hash:
  `7b3f0b572a1496fbdc8e6810b4a9381e25f5839054c3989a44fdb9edbd1172cd`.
- Evidence database:
  `.amp/in/artifacts/poc2_m4_daily_100_forecast_contract_m4_comb_v2.duckdb`.
- Post-restart database SHA-256:
  `debe7caceb8b516294ac10ff05cd40ddda6a81714231fa428805fabee4e8d904`.

No dependency was installed or synchronized. `forecastHybrid` was not used.
The GIFT-Eval environment was used only by the approved evaluator, not as a
Naive2 scientific reference.

## Validation before execution

The complete non-integration Python suite passed 253 tests. Focused contract,
forecast-flow, configuration, M4-reference and experiment-execution coverage
passed 63 tests after the final Chronos provider correction. The pinned
GIFT-Eval integration suite passed three tests. The R forecast-method suite
passed 44 checks. R parsing, Python compilation and `git diff --check` passed.

An earlier isolated diagnostic database stopped after Chronos-2 returned
crossing independently estimated quantiles. The coordinator correctly rejected
that result. The provider was corrected to apply per-horizon monotone
rearrangement, preserve the model mean, derive the median from rearranged q0.5,
and record `identity` or `sorted_per_horizon`. The fresh evidence run described
below used a new database; it had no failed or retried scientific task. Of its
100 Chronos results, 90 recorded `identity` and 10 recorded
`sorted_per_horizon`.

## Two-machine execution

Processes 01 through 06 completed in order. Process 04 completed 1,400 base
forecasts in 956.566 seconds. Process 05 started only after Process 04 completed
and validated, and Process 06 started only after Process 05 completed and
validated.

The approved profile registered 38 workers: 8 Mac CPU, 15 Ubuntu CPU and 15
logical Ubuntu GPU workers. All 38 names appear in stored execution metadata.
The Mac workers produced 44 ETS forecasts. Ubuntu CPU workers produced 1,256 R
forecasts, and Ubuntu GPU workers produced all 100 Chronos-2 forecasts on the
RTX 5090. The scheduler recorded no removal, failure, error, spill or memory
pause between worker registration and the normal shutdown boundary.

Stored safety telemetry reported:

| Host | Forecasts | Minimum host memory available | Minimum accelerator memory available | Maximum owned RSS | Swap growth | Safety responses |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Mac | 44 | 4.462 GiB | n/a | 0.233 GiB | 0 | 0 |
| Ubuntu | 1,356 | 115.223 GiB | 29.604 GiB | 1.568 GiB | 0 | 0 |

Every experiment invocation and DuckDB mutation was coordinated on the Mac.
The resolved profile recorded exactly one database writer. Dask workers returned
serialized results and did not open DuckDB.

## Stored result audit

The database contains exactly 1,500 forecasts: 100 for each of the fourteen
configured base candidates and 100 `m4_comb` forecasts. The capability split is
1,000 `probabilistic` and 500 `mean_only`. Every mean is finite and horizon 14;
every probabilistic result has the eleven configured finite, noncrossing
quantiles and matching median; all mean-only median and quantile fields are
null. There are no duplicate forecast keys or IDs.

All 3,115 task attempts completed on their first attempt. There is no failed,
interrupted or retried attempt, task error, duplicate task key, component orphan
or process error. The existing visible STL-AR period-one fallback remains
unchanged: all 100 requests executed seasonal naïve with reason
`y is not a seasonal ts object`. This run therefore verifies the configured
STL-AR candidate contract, not a period-seven STL fit.

Process 05 stored exactly 100 `m4_comb` forecasts and 300 component links. Every
combination links, in order, to the independently stored SES, Holt and Damped
forecast for the same instance. Each weight is the configured floating-point
representation of one third; every three-weight sum differs from one by zero.
Every component creation timestamp precedes its combination timestamp. Comparing
all 1,400 horizon points with `(ses + holt + damped) / 3` produced maximum
absolute error `1.8189894035458565e-12`. Process 05 did not invoke or refit a
forecast model.

Process 06 stored 15 finite evaluation rows over 100 inputs each: ten use
`gift_eval_probabilistic_v1` and five use `mean_based_v1`. The latter are
Naive2, SES, Holt, Damped and M4 Comb. Their records contain only approved
mean-based metrics; no quantile, interval or fabricated-median metric was added.

## Restart evidence

The normal resume command omitted `--configuration`, as required for an existing
database, and requested Processes 01 through 06 with the approved profile. All
six processes returned `skipped_completed`; no scientific task was selected.
The following pre/post fingerprints were identical:

- forecasts, hashes and timestamps:
  `d95f3de83ddd7709c38a3fa2852310550068c34a1c27b013498775e57f9f661c`;
- task states, attempt counts and timestamps:
  `1cf0b1e6d9c218daa305b0e6ab025dcd7ffeddcfbdc88614a4da2c9e23e8333e`;
- evaluation inputs and timestamps:
  `5ddd71e677def70a00e5eba639d5da1522cbb40b9693ea2f1038ac210d0584a4`;
- M4 component lineage:
  `11e22d8528226d97c535d756eb055412c64bee538b0c80cab7776ce701c6597c`.

The restart adds only its completed orchestration event. The 1,500 forecasts,
300 links, 15 evaluations and 3,115 scientific attempts remain unchanged.

## Disposition

This evidence satisfies the implementation instructions' execution assertions:
normal orchestration and dependency order, both-machine contribution, CPU/GPU
routing, single-writer persistence, exact M4 lineage and arithmetic, capability
shapes, finite evaluations, resource safety and immutable scientific restart.
Technical review and researcher QA remain separate required acceptance steps.
