# ShapeFM

ShapeFM is one traceable, restartable time-series research system. Its only
researcher-facing experiment entry point is:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py --help
```

`--no-sync` is required when using the already prepared locked environments;
the command does not install or update dependencies. The public actions are
`plan`, `run`, `status`, `results`, and `test`.

## Installation

From the repository root, install the complete R and Python dependency superset,
submodules, datasets, and pinned model assets with:

```sh
scripts/setup.sh
```

This is equivalent to `scripts/setup.sh install all`. It is non-destructive,
reuses valid existing environments and downloads, and validates the complete
installation. Selective installation is intentionally unsupported. Run the
read-only audit at any time with:

```sh
scripts/setup.sh verify
```

For a clean reconstruction, `scripts/setup.sh rebuild all --confirm-delete`
moves every managed folder to an `original-folder-name_DDMMYY` sibling before
installing. It never deletes the old folders and stops before any move if a
dated destination already exists. See the [environment guide](docs/environment.md)
for prerequisites, all six Python environments, R and M4 handling, accelerator
behavior, recovery, download safeguards, and installation reports.

Setup-managed environments, generated data/models/results, local tool and
download caches, temporary downloads, and their dated rebuild archives are
ignored by narrowly scoped repository-relative rules. Environment manifests,
lockfiles, source, configuration, documentation, and tests remain trackable.

## Research workflow

The six ordered processes are:

```text
01 import → 02 preprocess → 03 transform → 04 forecast → 05 combine → 06 evaluate
```

Python coordinates every process and is the sole DuckDB writer. R is a
specialised worker for standard/robust preprocessing and an allowlisted nine-method
forecast pool. The pool's common probabilistic contract and explicit
seasonal-naive fallback are documented in
[`docs/forecast-methods.md`](docs/forecast-methods.md).
Import missingness and the two preprocessing modes are documented in
[`docs/preprocessing.md`](docs/preprocessing.md).
Completed task state and scientific provenance are stored transactionally, so
rerunning the same experiment skips completed work without changing experiment,
task, forecast, or evaluation identity.

Inspect the deterministic first-100-series plan:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py plan \
  --configuration config/experiments/poc2_m4_daily_100.json
```

Create one experiment database and execute Processes 01–03:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py run \
  --database results/poc2_m4_daily_100.duckdb \
  --configuration config/experiments/poc2_m4_daily_100.json \
  --processes 1-3
```

Resume Processes 04–06 from DuckDB alone. Do not pass the original JSON again:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py run \
  --database results/poc2_m4_daily_100.duckdb --processes 4-6
```

An existing database rejects `--configuration`. Repeating the second command
records another execution event but skips completed scientific work.

Run or restart the isolated two-machine acceptance case:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py test \
  --database results/poc2_final_acceptance.duckdb \
  --report results/poc2_final_acceptance_report.json
```

Read status and official evaluation results:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py status \
  --database results/poc2_final_acceptance.duckdb
.tools/uv/uv run --locked --no-sync python src/python/00_main.py results \
  --database results/poc2_final_acceptance.duckdb
```

Read one stored forecast by supplying all three selectors:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py results \
  --database results/poc2_final_acceptance.duckdb \
  --variant-id VARIANT_ID --series-id SERIES_ID --candidate CANDIDATE
```

Generated databases and reports remain ignored; `test` refuses to overwrite
`data/shapefm.duckdb`.

## Final source structure

```text
src/python/00_main.py                    single researcher interface
src/python/01_import.py                  Process 01 wrapper
src/python/02_preprocess.py              Process 02 wrapper
src/python/03_transform.py               Process 03 wrapper
src/python/04_forecast.py                Process 04 wrapper
src/python/04_02_forecast_chronos.py     Chronos forecasting substep
src/python/05_combine.py                 Process 05 wrapper
src/python/06_evaluate.py                Process 06 wrapper
src/python/06_01_evaluate_gift_eval.py   official evaluation substep
src/python/util/                         shared Python implementation
src/python/tests/                        unit, integration, and acceptance tests
src/r/02_01_preprocess_series.R          R preprocessing substep
src/r/04_01_forecast_auto_arima.R        R AutoARIMA substep
src/r/util/forecast_methods.R             shared registered R forecast pool
src/r/util/time_series_input.R           shared R time-series input contract
src/r/tests/                              focused R contract tests
```

## POC2 phases and topology

POC2 has two phases. **Preparation** established the stable structure, single
entry point, distributed acceptance, traceability, and restart proof. **Import**
uses a versioned JSON experiment definition for database creation and then uses
the stored DuckDB configuration exclusively for every resume.

The accepted historical end-to-end baseline used 5 Mac CPU workers, 15 Ubuntu
CPU workers, and 1 Ubuntu GPU worker (21 Dask workers). A subsequent isolated
calibration found that 15 logical Chronos worker processes sharing one physical
Ubuntu RTX 5090 were scientifically equivalent, faster, and resource-safe.
The Preparation topology was 5 Mac CPU + 15 Ubuntu CPU + 15 logical Ubuntu GPU
workers = 35 Dask workers. One physical GPU and 15 logical execution slots are
distinct facts. The complete 100-series pipeline and its restart passed with
this topology; measured evidence is recorded in
[`docs/poc2-preparation-completion.md`](docs/poc2-preparation-completion.md).
The POC2 Import acceptance target is deliberately two workers: one Mac CPU and
one Ubuntu GPU worker; the scheduler is not a worker. This new target has not
yet been accepted.

See the [architecture](docs/architecture.md), [code standards](docs/code-standards.md),
[experiment configuration reference](docs/experiment-configuration.md),
[configuration inventory](docs/configuration-inventory.md), [data contract](docs/data-contract.md),
and [local execution contract](docs/local-execution.md).
