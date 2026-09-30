# Global-setting mapping

This is the concise Objective 1 disposition for active production settings.
The default creation-time authority is
`config/experiments/poc2_m4_daily_100_resolved_period.json`; DuckDB is
authoritative after creation. The original `poc2_m4_daily_100.json` remains the
historical version-1 period-7 experiment. Derived counts and identities are
calculated rather than configured.

## Experiment globals

| JSON area | Current scope | Consumers |
|---|---|---|
| `configuration_version`, `experiment` | version 2 by default; name, date, objective | validator, DuckDB metadata, status |
| `reproducibility.seed` | `1234` | scientific identity and future stochastic workers |
| `data` | pinned GIFT-Eval M4 Daily source; first 100 official series; 14-step, one-window benchmark | import, planning, evaluation |
| `pipeline` | Processes 01–06; optional R-period override; standard/robust preprocessing (robust default); identity/min-max-standardize; identity adjustment; equal-weight combination | task planning and Processes 02–05 |
| `models` | AutoARIMA settings; pinned Chronos-2 identity, dtype, quantiles, and prediction policies | Process 04 workers and provenance |
| scientific `evaluation` fields | GIFT-Eval revision, method, options, provisional candidate and submission metadata | Process 06 and export validation |

The scientific fingerprint covers the data, scientific pipeline, models,
evaluation, and seed. Name, date, description, and all execution settings are
excluded.

## Execution globals

| JSON area | Current scope | Consumers |
|---|---|---|
| `execution.default.mode`, workers and batch sizes | sequential default and per-process queue controls | coordinator and execution events |
| in-flight, retries, worker timeouts and thread limits | shared local/distributed execution behaviour | coordinator, Dask, R and Chronos worker payloads |
| memory floors and one-writer rule | resource safety and database ownership | run gate and acceptance telemetry |
| `execution.final_acceptance` | one Mac CPU worker plus one Ubuntu GPU worker | two-machine gate only |
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
remain calibration/test constants. `config/execution_profiles.json` and
`config/dependencies/gift_eval.json` are setup/calibration inputs and are not
production experiment authorities. The removed `config/imports/m4_daily.json`
must not be reintroduced as a competing source.
