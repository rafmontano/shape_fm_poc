# POC2 bounded all-model execution evidence

Executed on 4 October 2026. This record reports implementation evidence for
configuration version 8; it is not ChatGPT technical acceptance or researcher
acceptance. One scientific limitation remains open before the run can establish
that every requested model fitted.

## Scope and identity

The normal researcher entry point ran the first 100 official M4 Daily series
through one `robust` plus `standardise_sample_v1` variant, the nine registered R
candidates, Chronos-2, and the existing ten-way equal-weight combination.
ID 018's four unregistered point methods and future M4 Comb were not activated.

- Git revision: `df637324946968463105e93365e17430560f1c77`, with the reviewed
  uncommitted candidate present on both hosts.
- Runtime source manifest: 113 files with SHA-256
  `1befe98b62270ec0833a6bb603baeb6d41ed03ea81d7f5c3dcedee5f0c0c6deb`.
- Scientific hash:
  `1b9d4bc599251bb13720d7a73555f822987f0269eea0a36ee6c67e40d596124e`.
- Configuration-integrity hash:
  `dd0d95721aa4259ac0b99ae4b4cadbb737458b472844cc2d312faafcc3261d19`.
- Evidence database:
  `.amp/in/artifacts/poc2_m4_daily_100_all_models.duckdb`.

## Execution and recovery

The approved `poc2_seasonal_recovery` profile exposed 8 Mac CPU workers, 15
Ubuntu CPU workers and 15 logical Ubuntu GPU workers. Process 04 completed all
1,000 base forecasts in 852.855 seconds. Its native contributions were 44 ETS
forecasts across all eight Mac workers, 856 R forecasts on Ubuntu, and 100
Chronos-2 forecasts across all fifteen Ubuntu GPU workers. Mac remained the
sole DuckDB writer. Processes 05 and 06 then completed 1,100 candidate tasks in
4.165 seconds and 11 evaluation tasks in 14.230 seconds.

The first Processes 01–03 invocation was not issue-free. Process 01 completed,
but remote Process 02 tasks attempted to start worker-local ephemeral Prefect
servers. Forty-four preprocessing tasks committed and 56 failed with
`Timed out while attempting to connect to ephemeral Prefect API server`; the
Ubuntu log also reported an unavailable SQLite path. Scientific calculations
did not fail.

The distributed preflight was corrected to require a Mac-LAN
`PREFECT_API_URL` and propagate it, with
`PREFECT_SERVER_EPHEMERAL_ENABLED=false`, to both worker pools. A single
Mac-hosted Prefect server was reachable from Ubuntu. Normal recovery selected
the 56 failed tasks, skipped the 44 durable completions, and completed all 100;
Process 03 then completed all 100. This is successful durable recovery, not an
issue-free first attempt.

## Stored result audit

The final database contains exactly 1,100 forecasts. Every one of the 100
forecast instances has exactly eleven candidates, and every stored mean,
median and 0.1–0.9 quantile array is finite and exactly horizon 14.

| Candidate | Stored rows | Requested method | Executed method | Fallbacks |
| --- | ---: | --- | --- | ---: |
| AutoARIMA | 100 | `auto_arima_forec` | `auto_arima_forec` | 0 |
| Chronos-2 | 100 | Chronos-2 | Chronos-2 | 0 |
| ETS | 100 | `ets_forec` | `ets_forec` | 0 |
| Naïve | 100 | `naive_forec` | `naive_forec` | 0 |
| NNETAR | 100 | `nnetar_forec` | `nnetar_forec` | 0 |
| Random walk with drift | 100 | `rw_drift_forec` | `rw_drift_forec` | 0 |
| Seasonal naïve | 100 | `snaive_forec` | `snaive_forec` | 0 |
| STL-AR | 100 | `stlm_ar_forec` | `snaive_forec` | 100 |
| TBATS | 100 | `tbats_forec` | `tbats_forec` | 0 |
| Theta | 100 | `thetaf_forec` | `thetaf_forec` | 0 |
| Existing equal weight | 100 | ten stored components | Process 05 average | n/a |

The STL-AR provenance is accurate: M4 Daily resolves to period 1 under the
approved default, so `forecast::stlm()` rejects the nonseasonal `stats::ts` and
the existing visible fallback executes `snaive_forec` with reason
`y is not a seasonal ts object`. These are valid stored STL-AR *candidate*
forecasts, but they are not evidence that STL-AR fitted. An explicit period-7
experiment would be a different scientific experiment and requires researcher
approval; this run did not silently make that change.

Each equal-weight row has exactly ten `forecast_components` rows. Every
component ID resolves to the corresponding independently stored base forecast,
each component name occurs 100 times at weight 0.1, and every weight sum is one.
Recalculation of all combination means, medians and quantiles had maximum
absolute difference `1.8189894035458565e-12`. No model was invoked or refitted
by combination code.

All eleven official evaluation rows consumed 100 forecast inputs and contain
finite metrics and a forecast-input fingerprint. `is_complete_manifest` and
`is_submittable` are false by design because 100 series are a bounded subset of
the 4,227-series benchmark, not a complete submission manifest.

## Sequential comparison and restart

One real stored series was forecast sequentially on Ubuntu through the same
nine-method native R worker and pinned Chronos-2 CUDA adapter. Against its
stored distributed arrays, all means, medians and quantiles matched with
`rtol = atol = 1e-5`; the observed maximum difference was zero for every
candidate. This comparison reproduces the visible STL-AR fallback rather than
claiming an STL-AR fit. Chronos-2 reported the RTX 5090 and retained more than
29.6 GiB accelerator and 120.8 GiB host-memory headroom.

A diagnostic Mac arm64 NNETAR repeat differed from the stored Ubuntu x86_64
result by a maximum 0.157 despite the configured deterministic seed. Therefore
same-host Ubuntu output, not cross-architecture bitwise identity, is the accepted
sequential comparison for this evidence. Cross-architecture NNETAR equivalence
remains a documented limitation.

A normal Processes 04–06 resume reported all three processes
`skipped_completed`. A SHA-256 snapshot over all 1,100 forecast hashes and
timestamps, 1,000 component links, eleven evaluation fingerprints/timestamps,
2,311 task states/attempt counts, and 2,367 attempt rows remained exactly
`34b555632c1699fac613c690df110b8bf8c5c2ba5e0b309b179fb019863b236e`
before and after the resume.

## Acceptance disposition

The run verifies the two-host execution route, all expected row counts,
probabilistic shapes, finite values, combination lineage, evaluations, native
adapter reproducibility on the execution host, and restart immutability. It does
not satisfy the stronger statement that all ten underlying model
implementations fitted, because all 100 STL-AR requests used the approved
fallback. The researcher must choose whether visible period-1 fallback is the
desired Daily contract or authorize a separate explicit period-7 experiment.
