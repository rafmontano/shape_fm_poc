# POC2 seasonal-period tuning acceptance and recovery

Recorded 30 September 2026. This closes the approved 100-series Gate 4 test in
the [seasonal-period decision](poc2-seasonal-period-tuning.md) under the
[execution policy](execution-policy.md). It is not full-pipeline acceptance and
does not authorize ID 010, all 4,227 M4 Daily series, more models or GPU work.

Subsequent review on 30 September 2026 accepted these completed results but
found three remaining runtime-policy gaps: optional-profile local execution,
hardcoded Mac routing/concurrency and memory checks limited to admission.
The researcher approved their correction and an increase to eight Mac CPU
workers in the [safeguard follow-up](amp-poc2-execution-safeguards-instructions.md),
which subsequently passed the focused evidence recorded below.
The original distributed-recovery section remains the historical 5+15 run and
is not relabelled as 8+15 evidence. The focused safeguard evidence is recorded
separately at the end; the completed acceptance was not rerun.

## Scientific result

The acceptance contains 100 M4 Daily series, four existing preparation
variants, and AutoARIMA plus ETS: 800 final forecasts and 800 linked selections.
All three historical folds precede the official test boundary. The validation
MAEs below are means across comparable selections; they are selection-window
evidence, not independent official-test performance.

| Model | Baseline selected | Estimated selected | Selected | Inconclusive | Comparable | Mean baseline MAE | Mean estimated MAE |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| AutoARIMA | 382 | 18 | 369 | 31 | 373 | 227.8263 | 225.9057 |
| ETS | 395 | 5 | 356 | 44 | 356 | 196.9513 | 196.9128 |
| Total | 777 | 23 | 725 | 75 | 729 | — | — |

The database contains 1,200 fold preparations, 2,400 candidates and 4,800
policy validations. Of those validations, 4,697 succeeded, 98 recorded a
candidate substitution and five AutoARIMA estimated-policy fits reached the
unchanged 1,800-second timeout. Seventy-one selections were inconclusive because
one or more windows were incomplete, substituted, fell back or failed; two had
an ineligible final-history estimate, and two estimated final fits timed out.
Four AutoARIMA final forecasts therefore used the recorded baseline substitution.
No completed forecast is missing.

## Distributed recovery evidence

`config/execution_profiles.json` owns the named `poc2_seasonal_recovery`
profile. The CLI's explicit `--execution-profile` override is operational and
append-only; it does not rewrite the experiment's scientific identity. Effective
capacity was five Mac CPU workers plus fifteen Ubuntu CPU workers. Memory-aware
admission exposed two Mac and fifteen Ubuntu R-tuning slots, limited in-flight
work to 17, assigned 3 GiB and 6 GiB Dask worker limits respectively, and
enforced 3 GiB Mac and 16 GiB Ubuntu host-available-memory floors. Python on the
Mac remained the only DuckDB writer.

Before recovery, an isolated two-task check ran ETS on the Mac and AutoARIMA on
Ubuntu with overlapping execution. Against sequential references, mean, median,
all quantiles, validation MAEs, selected policy and period were compared with a
maximum absolute tolerance of 1e-10; the observed maximum difference was zero.
The recovery then selected only the 392 incomplete tasks and skipped the prior
408. Of the newly completed forecasts, Ubuntu produced 382 and the Mac 10. The
recovery invocation ran for 4,155.96 seconds (about 69 minutes). Twenty workers
remained connected, neither host crossed its memory floor, Ubuntu used no swap,
the Mac's existing approximately 0.6 GiB swap did not grow, and Dask reported no
spill.

The heavy run used repository HEAD `94a3f09607f55bdf16e987aebc9be1e4b19f706b`
plus a synchronized uncommitted runtime-source manifest with fingerprint
`78bfa388802ee68d0566eba29843ef471eb08c0f3737fe2872c07d5c8572fa91`.
After the parent-record recovery correction, both machines again matched across
61 runtime files at fingerprint
`f4916e540c3c08a0482fc68f21cde688001802de532bc53deddcabe25fa93485`.
Ubuntu loaded R 4.6.1, forecast 8.24.0, jsonlite 2.0.0 and tsfeatures 1.1.1.
The missing pinned tsfeatures dependency was restored locally; its RcppRoll
0.4.0 dependency required a native Ubuntu rebuild rather than copying a Mac
library. No environment, database, result, secret or compiled Mac library was
transferred.

## Preservation and restart proof

The pre-recovery database and WAL remain together in the ignored dated recovery
directory with SHA-256 checksums. Comparing a replayed copy of that backup with
the completed database, excluding creation timestamps only, found every original
row present and value-identical: 408 forecasts, 408 selections, 615 folds, 1,225
candidates and 2,448 validations.

Normal recovery marked orphaned attempts, tasks, invocations and execution
events as interrupted rather than successful. A final repeated Gate 4 command
returned `skipped_completed`, created the expected new execution event, created
no forecast invocation, and left all scientific counts and fingerprints
unchanged:

| Table | Rows | SHA-256 fingerprint |
| --- | ---: | --- |
| `forecasts` | 800 | `43d69990b4f819684a9cec0fb4147dbe045dbd2f286547a1adbcf995c1742f4f` |
| `seasonal_period_selections` | 800 | `222d65c06aae75fbedbd66f78c6e2a68a61637a120edc10c9ee65edef2c23909` |
| `seasonal_tuning_folds` | 1,200 | `199ee0136bc3b9c6febc5ea26e39c1ba3368faeaa6aa6ee94305084e21ad3bbe` |
| `seasonal_period_candidates` | 2,400 | `bea4643accb9d5b2884255de14021644974f0a8cf0cc861130299489683fea60` |
| `seasonal_tuning_validations` | 4,800 | `36bf65a1bcafa7c8444ada73ad21935d692c8aa45824cf981941a9732d83deed` |

The production recovery command was:

```bash
SHAPEFM_UBUNTU_HOST=<verified-ubuntu-ssh-host> \
SHAPEFM_SSH_HOST_KEY_ALIAS=wsubuntu1.local \
SHAPEFM_MAC_BIND_HOST=<mac-lan-ipv4> \
RENV_CONFIG_SYNCHRONIZED_CHECK=false PYTHONPATH=src/python \
.tools/uv/uv run --locked --no-sync python src/python/00_main.py run \
  --database results/poc2_m4_daily_100_period_tuning.duckdb \
  --processes 4 --execution-profile poc2_seasonal_recovery
```

For read-only inspection, source `src/r/qa/inspect_seasonal_period_tuning.R` in
R/RStudio. It leaves the selected series' windows, candidates, validations,
selection and final forecast available in the session. The existing
`src/r/qa/get_m4_daily_series.R` contract is unchanged.

## Limitations

- This accepts only Gate 4 seasonal tuning on the bounded 100-series scope. It
  does not establish complete Gates 1–6, all-series, Chronos or GPU acceptance.
- Validation improvement is used only for policy selection and must not be
  interpreted as official-test improvement.
- One Ubuntu R child reached approximately 12 GiB resident memory. Dask's 6 GiB
  worker limit does not constrain an external R child, although the 16 GiB host
  floor and Ubuntu's observed 103–121 GiB available memory kept this run safe.
  Future expansion should retain host-level admission and re-evaluate the tail.
- Five estimated-policy fold fits and two final estimated fits reached the
  approved 30-minute timeout. Their failures and substitutions remain visible;
  model settings and timeout were not weakened to make the acceptance pass.
- ID 010 remains queued and no control spreadsheet was changed.

## Eight-worker safeguard evidence

The focused follow-up ran separately on 30 September 2026 and did not refit or
write the accepted 800 forecasts. Profile `poc2_seasonal_recovery` version 2
started 8 Mac CPU workers and 15 Ubuntu CPU workers; its 15 logical Ubuntu GPU
slots remained configured but were not launched for this R-only check. All 23
CPU workers passed source, dependency, topology and resource validation. The
effective profile fingerprint was
`0d4f95d099120837609f5854ff09245d23af8bab6a94084c1e384aa095f3cca7`.
Both hosts matched across 61 runtime files at source-manifest fingerprint
`e28154d3a2789808110b20751118dd7b5096fdbbd8040247e59c69a3d2fda365`.

Twenty-four low-memory tasks completed across all eight named Mac workers with
peak overlap eight, proving useful capacity rather than registration alone. A
real isolated check then ran AutoARIMA on Ubuntu concurrently with two ETS groups
on distinct Mac workers. Peak real overlap was three and Mac real-fit overlap was
two, exceeding the removed one-task queue. Each group prepared three fresh
historical folds and six validations. Mean, median, quantiles, validation MAEs,
selected policy and final period matched one-at-a-time references exactly;
maximum absolute difference was 0 at tolerance 1e-10.

The profile admits up to 8 AutoARIMA and 15 ETS tasks, but memory reservations
may safely admit fewer. AutoARIMA has a conservative 12 GiB fit budget and is
placed on Ubuntu. ETS has a measured 0.75 GiB budget: production-shaped probes
peaked at 0.267 GiB, while the larger budget permits two concurrent Mac fits at
the observed headroom. During the concurrent check, minimum available memory was
4.51 GiB on Mac and 121.43 GiB on Ubuntu; Mac's existing 0.522 GiB swap did not
grow, Ubuntu swap remained zero, and no safety response occurred. Monitoring
covers the owned R process tree throughout each fit and treats a sustained floor
or swap breach as retryable infrastructure interruption, not model fallback.

A synthetic resource interruption was retried once successfully. The normal
guarded CLI then ran against an isolated copy of the completed database and
returned `skipped_completed` with no forecast invocation and unchanged counts.
The production database remained at 800 forecasts, 800 selections, 1,200 folds,
2,400 candidates and 4,800 validations, with its prior table fingerprints and
all completed evidence preserved. Its file SHA-256 after the read-only check was
`cbb5a8604dcfd7dbc8a5c4c6d930825d74711c6721061e13bfafcee884c2dbc7`.

The focused evidence does not claim that eight memory-intensive R fits are safe
simultaneously on a 16 GiB Mac. Eight workers are eligible and useful; admission
is intentionally constrained by current host headroom and measured model budget.
It also does not expand acceptance to all 4,227 series, Chronos, GPU execution,
Gates 1–6 or ID 010.
