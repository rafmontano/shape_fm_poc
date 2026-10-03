# Global-setting mapping

This is the concise Objective 1 disposition for active production settings.
The default creation-time authority is
`config/experiments/poc2_m4_daily_100_resolved_period.json`; DuckDB is
authoritative after creation. The original `poc2_m4_daily_100.json` remains the
historical version-1 period-7 experiment. Derived counts and identities are
calculated rather than configured.
The separately approved Gate 3 acceptance uses
`config/experiments/poc2_m4_daily_100_standardised.json` (version 4); it does
not replace the production default or reinterpret older experiments.
The Stage 2 opt-in `poc2_m4_daily_100_r_pool.json` (version 7) connects the
approved nine-method R pool and inherits corrected v6 window/S1 preparation.
See the [versioned contract](experiment-configuration.md#version-7-adds-the-approved-r-pool-without-rewriting-history).

## Experiment globals

| JSON area | Current scope | Consumers |
|---|---|---|
| `configuration_version`, `experiment` | version 2 by default; opt-in v3 tuning; v4 standardisation experiment; name, date, objective | validator, DuckDB metadata, status |
| `reproducibility.seed` | `1234` | scientific identity and future stochastic workers |
| `data` | pinned GIFT-Eval M4 Daily source; first 100 official series; 14-step, one-window benchmark | import, planning, evaluation |
| `pipeline` | Processes 01–06; optional R-period override; standard/robust preprocessing; versioned transformations; v4 explicit context length; combination; opt-in v3/v4 period tuning | task planning and Processes 02–05 |
| `models` | v1/v2 and normal v4 AutoARIMA + Chronos-2; tuning v3/v4 AutoARIMA + ETS; v7 nine registered R methods | Process 04 workers and provenance |
| scientific `evaluation` fields | GIFT-Eval revision, method, options, provisional candidate and submission metadata | Process 06 and export validation |

The scientific fingerprint covers the data, scientific pipeline, models,
evaluation, and seed. Name, date, description, and all execution settings are
excluded.

## Execution globals

The [execution policy](execution-policy.md) is the approved operational decision.
The [safeguard follow-up](amp-poc2-execution-safeguards-instructions.md) makes
profile use mandatory for heavy work, centralises routing and memory controls,
and implements eight Mac CPU workers. The rows below describe ownership, not
permission to reuse a smaller profile implicitly.

| JSON area | Current scope | Consumers |
|---|---|---|
| `execution.default.mode`, workers and batch sizes | creation snapshot; approved resume override is resolved centrally and recorded | coordinator and execution events |
| in-flight, retries, worker timeouts and thread limits | shared local/distributed execution behaviour | coordinator, Dask, R and Chronos worker payloads |
| memory floors and one-writer rule | resource safety and database ownership | run gate and acceptance telemetry |
| `execution.final_acceptance` | acceptance evidence snapshot; v4 is sequential Processes 1–3 on one Mac worker; historical small cases are not heavy-test defaults | acceptance harness and preflight |
| `execution.paths` | prepared environments and worker entry points | subprocess launch and preflight |
| `execution.restart` | skip completed; retry failed/interrupted | process and task selection |

The configuration-integrity fingerprint covers the complete resolved document,
including these execution globals and derived values.

## Machine environment

`SHAPEFM_UBUNTU_HOST`, `SHAPEFM_UBUNTU_ROOT`, `SHAPEFM_MAC_HOST`,
`SHAPEFM_MAC_BIND_HOST`, `SHAPEFM_DASK_ADDRESS`, and
`SHAPEFM_DASK_WORKER_ADDRESS` select the current machines and network endpoints.
`CUDA_VISIBLE_DEVICES`, Hugging Face cache variables, host identity, and actual
device state are machine environment and execution evidence, not experiment
identity. Effective host, dependency, resource, and code state is recorded.

## Code constants

| Constant class | Reason retained in code |
|---|---|
| configuration/schema versions, process IDs, table names and statuses | versioned protocol and persistence contracts |
| JSON stdin/stdout action and field names | Python/R worker wire protocol |
| Dask resource labels and worker-cache mechanics | implementation coordination, not researcher choices |
| source filenames and required external result columns | pinned external format contracts |
| cleaning, transformation, combination and metric implementations | versioned implementations selected by experiment globals |
| scheduler ports, PID/log names, polling and telemetry cadence | internal control and diagnostics |
| pinned package defaults not intentionally exposed by ShapeFM | controlled by code and lockfile versions |

Developer calibration grids, repetitions, tolerances, and historical evidence
remain calibration/test constants. The existing entries in
`config/execution_profiles.json` include historical setup/calibration settings;
they are not evidence that the old 2+4 profile is the approved heavy-test profile.
That file also owns the implemented `poc2_seasonal_recovery` profile. Its current
version 3 identity records 8 Mac CPU workers, 15 Ubuntu CPU workers and 15
logical Ubuntu GPU slots. For R-only tuning, all 23 CPU workers are eligible;
AutoARIMA uses the Ubuntu-only capability, while ETS uses the shared capability
subject to profile-owned memory admission. Runtime configuration does not
introduce a competing scientific experiment authority.
`config/dependencies/gift_eval.json` remains a setup dependency input.
The removed `config/imports/m4_daily.json` must not be reintroduced.

The registered v4 transformation state is persisted in the existing
`transformed_series.parameters` JSON field; no parallel state table or
configuration source was introduced. The explicit context length is currently
a stored and tested capability only. It must not be described as active model
windowing until a selected consumer is integrated.
