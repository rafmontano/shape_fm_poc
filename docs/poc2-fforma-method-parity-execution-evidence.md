# POC2 FFORMA method-parity execution evidence

Executed on 5 October 2026. This record reports implementation and execution
evidence for the approved ID 018 FFORMA method-parity correction. The Chief
Architect accepted the technical evidence and the researcher approved final
closure on 5 October 2026. This is not an accuracy comparison.

## Scope and tested identity

The normal researcher entry point ran Processes 01–06 for the first 100 M4
Daily series, fourteen independent base candidates and official point-only M4
Comb. The run used a new database and the approved version-9 configuration and
`poc2_seasonal_recovery` profile v3.

- Local unpublished checkpoint:
  `94bb17fd667c03cd675508725d29b24478431156`.
- Git tree: `5069ea58a3424d102ef66d1c45cc27851d15377f`.
- The 184-entry `git ls-tree -r HEAD` manifest SHA-256 was
  `5f0915a2d1a0e0c233c2effc9feaff27bddbb0638be17df49923a56d3e40d5be`.
- Mac and Ubuntu were clean on `main` at that exact unpublished revision before
  execution. The pinned GIFT-Eval submodule was
  `4d5ab3fa0fe7451bbf59bb1ff6dd76e6e414d64a` on both machines.
- `uv.lock` SHA-256 was
  `ebe4777668fddebcd73dec46c01d794f4e9d1f81389ffc03a90e7c18c528d2e0`;
  `renv.lock` SHA-256 was
  `775c4d6a07e969c75074ec7a74b70e2eb5ae6de7d4d8346267dac4063eb23022`.
- Configuration SHA-256 was
  `8c9366fbdb7f8f54bc60e56ef0452b1654d18eb09a06767cf65684c8603a85cb`.
  Stored scientific and integrity hashes remained
  `6e7edfd3d7705ea7172b3dc32bde04a1b843050eb455338e7ccd4ea3624a04ec`
  and
  `7b3f0b572a1496fbdc8e6810b4a9381e25f5839054c3989a44fdb9edbd1172cd`.
- Evidence database:
  `.amp/in/artifacts/id018_fforma_method_parity_v2.duckdb`.
  Its post-restart SHA-256 was
  `364fbf7798c4ade0571dc6590c7503191ee8ebc7ca7a2f81d1bdd88b33e4c845`.

No dependency was installed or synchronized. Mac and Ubuntu used R 4.6.1,
`forecast` 8.24.0, `jsonlite` 2.0.0 and `tsfeatures` 1.1.1. Both used Python
3.12.14, DuckDB 1.5.5, Dask/Distributed 2026.8.0 and Prefect 3.8.7. Ubuntu used
Chronos 2.2.2, PyTorch 2.14.0+cu130 and the RTX 5090 through CUDA. The pinned
source-data hashes all matched configuration. `forecastHybrid` was not used.

## Local validation

- The complete fast Python suite passed 256 tests.
- The six standard integration tests passed, and the separate locked GIFT-Eval
  environment passed its three tests: nine integration tests total.
- The four R suites passed 65 named checks: 46 forecast-method, nine feature,
  nine transformation and one M4 import check.
- The forecast-method checks used independent reference calculations for all
  nine registered FFORMA methods and Naive2. They passed period-1 and period-7,
  short and longer history, seasonal and nonseasonal Naive2, successful STL-AR,
  fixed-AutoARIMA STL fallback, deterministic NNETAR, manual seasonal-naive and
  original naïve/drift two-step parity at tolerance `1e-10`.
- Failure checks proved that method and invalid-output failures did not execute
  seasonal naïve, retry preserved model identity/settings, baseline overrides
  were rejected, and STL fallback identity/reason survived JSON and storage.
- All 20 R files parsed, Python compiled, `git diff --check` passed, production
  contained no pool-wide seasonal-naive fallback, and no generated artifact
  entered the checkpoint.

## Two-machine execution

Processes 01–06 completed in order. Process 04 completed its 1,400 base
forecasts in 947.761 seconds; Process 05 and Process 06 then completed and
validated before the managed cluster shut down.

The selected profile registered 38 unique workers and every worker contributed:
eight Mac CPU workers produced 50 forecasts, fifteen Ubuntu CPU workers produced
1,250 forecasts, and fifteen logical Ubuntu GPU workers produced all 100
Chronos-2 forecasts. The Mac remained the sole DuckDB writer.

The requested and executed calculation audit was:

| Requested candidate | Requested | Executed as requested | Executed fixed STL fallback |
| --- | ---: | ---: | ---: |
| AutoARIMA | 100 | 100 | 0 |
| ETS | 100 | 100 | 0 |
| NNETAR | 100 | 100 | 0 |
| TBATS | 100 | 100 | 0 |
| STL-AR | 100 | 0 | 100 as fixed AutoARIMA |
| Random walk with drift | 100 | 100 | 0 |
| Theta | 100 | 100 | 0 |
| Naïve | 100 | 100 | 0 |
| Seasonal naïve | 100 | 100 | 0 |
| Naive2 | 100 | 100 | 0 |
| SES | 100 | 100 | 0 |
| Holt | 100 | 100 | 0 |
| Damped | 100 | 100 | 0 |
| Chronos-2 | 100 | 100 | 0 |

All input periods were 1, so all 100 STL fitting attempts selected the approved
`auto.arima(d=0,D=0)` branch. Every row retained requested identity `stlm_ar`,
executed identity `auto_arima`, package version 8.24.0, exact fixed settings,
and original error `y is not a seasonal ts object`. There were zero successful
STL-AR branches for this input and zero seasonal-naive substitutions for another
method. The only requested/executed identity difference was the approved STL
branch.

## Stored result and coordination audit

The database contains 1,500 forecasts: 100 for each of fourteen base candidates
and 100 M4 Comb forecasts. The capability split is 1,000 probabilistic and 500
mean-only forecasts. Every mean is finite and length 14. Every probabilistic
row has eleven finite, noncrossing quantiles and matching q0.5/median; every
mean-only row has null probabilistic fields.

Task cardinalities were 100 Process 01 imports, 100 Process 02 tasks, 100 Process
03 tasks, 1,400 Process 04 tasks, 1,500 Process 05 tasks and 15 Process 06 tasks.
All 3,115 scientific tasks and attempts completed on attempt one. The 15
evaluations consumed exactly 1,500 forecast inputs: ten probabilistic evaluations
over 1,000 inputs and five mean-based evaluations over 500 inputs.

There were no failed or interrupted task attempts, nonzero provider retries,
duplicate forecast IDs or keys, component or combination orphans, malformed
forecast outputs, failed execution events, or stored process errors.

Process 05 stored exactly 300 links for 100 M4 Comb forecasts. Every combination
records components in `SES`, `Holt`, `Damped` order; all links target the same
forecast instance, all source timestamps precede the combination timestamp,
and every weight is the configured floating-point one third with an exact
three-weight sum of one. Recalculation from stored component means produced a
maximum absolute difference of `1.8189894035458565e-12` across 1,400 points.

## Resource safety

Stored per-forecast telemetry reported:

| Pool | Forecasts | Minimum host memory available | Minimum accelerator memory available | Maximum owned RSS | Swap growth | Safety responses |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Mac CPU | 50 | 5.065 GiB | n/a | 0.233 GiB | 0 GiB | 0 |
| Ubuntu CPU | 1,250 | 116.622 GiB | n/a | 0.278 GiB | 0 GiB | 0 |
| Ubuntu GPU | 100 | 115.580 GiB | 29.376 GiB | 1.568 GiB | 0 GiB | 0 |

Mac started with 1.256 GiB swap already in use but recorded no growth. Admission
throttling totalled 142.5 seconds on Mac CPU and 16.5 seconds on Ubuntu CPU; the
maximum GPU startup throttle was 263.541 seconds. No unsafe reason, memory-floor
breach, worker-level safety response, or Ubuntu swap use was recorded.

## Restart evidence

The normal restart requested Processes 01–06 with the same profile and existing
database but omitted `--configuration`. All six returned `skipped_completed`;
no scientific task was selected. Before/after row counts and SHA-256 fingerprints
were identical:

| Scientific state | Rows | SHA-256 |
| --- | ---: | --- |
| forecasts | 1,500 | `fe1ca135ed516288fde97214f40c2259c5f82a1f72b88d29296d5bbb62d6ef24` |
| tasks | 3,115 | `16123627d1cae0d830589415c9e7004e6277ad1bdfc9a08885ee87321ebc2b8e` |
| attempts | 3,115 | `46db9ab832cd2f5122c56d642afe2b0d1d3eb49f22a4d5b1fc1a475627bfcb21` |
| evaluations | 15 | `c3819454c6aeb38251a34f1928c9bd304cdb283f216b8ddb9816295f56082d67` |
| component links | 300 | `06edeaf66cfc74b955162ffbae515b40d6ad104c74bb17125ef54fd2b307f78d` |

Both run and restart execution events completed without errors. Managed
Prefect, Dask and forecast-worker processes were stopped after evidence
collection. At the execution handoff, Ubuntu remained clean at the tested
revision and the Mac differed only by this new evidence document. The closure
status was added after Chief Architect review and researcher acceptance.

## Operational note and remaining limitations

The first launch attempt stopped at preflight because the required Mac-LAN
`PREFECT_API_URL` had not been exported. It created only an initialization
database (`id018_fforma_method_parity_v1.duckdb`) and no scientific work. That
artifact was preserved and excluded; the complete run used the fresh `v2`
database above after Ubuntu connectivity to the owned Prefect API was verified.

This period-1 execution necessarily validates the fixed AutoARIMA STL branch,
not a successful seasonal STL fit; the independent local period-7 parity test
validates the successful branch. The run validates parity and coordination, not
comparative forecast accuracy. Historical v8/v9 records remain unchanged.

## Acceptance and closure

The Chief Architect review found no blocking technical or scientific-contract
gap. The researcher accepted the implementation and evidence on 5 October
2026. ID 018 is closed.

Closure confirms the FFORMA method configurations and point means, the supplied
STL-AR fixed-AutoARIMA branch, the absence of pool-wide model substitution, the
M4 benchmark components and combination, two-machine execution, traceability
and immutable restart. It does not claim forecast-accuracy superiority or a
distributed period-7 result-reproduction experiment.

ID 021 had not started at the closure boundary. Publication of checkpoint
`94bb17fd667c03cd675508725d29b24478431156` and this evidence record was
authorised after acceptance.
