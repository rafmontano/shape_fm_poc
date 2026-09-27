# ShapeFM

ShapeFM is one traceable, restartable time-series research system. Its only
researcher-facing entry point is:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py --help
```

`--no-sync` is required when using the already prepared locked environments;
the command does not install or update dependencies. The public actions are
`plan`, `status`, `results`, and `test`.

## Research workflow

The six ordered processes are:

```text
01 import → 02 preprocess → 03 transform → 04 forecast → 05 combine → 06 evaluate
```

Python coordinates every process and is the sole DuckDB writer. R is a
specialised worker for `tsclean` preprocessing and AutoARIMA forecasting.
Completed task state and scientific provenance are stored transactionally, so
rerunning the same experiment skips completed work without changing experiment,
task, forecast, or evaluation identity.

Inspect the deterministic first-100-series plan:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py plan --series-limit 100
```

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
src/python/00_main.py                 single researcher interface
src/python/04_forecast_chronos.py     Chronos process worker
src/python/06_evaluate_gift_eval.py   official evaluation bridge
src/python/util/                       shared Python implementation
src/python/tests/                      unit, integration, and acceptance tests
src/r/02_preprocess_series.R           R preprocessing worker
src/r/04_forecast_auto_arima.R         R AutoARIMA worker
src/r/util/time_series_input.R         shared R time-series input contract
```

## POC2 phases and topology

POC2 has two phases. **Preparation** establishes the stable structure, single
entry point, distributed acceptance, traceability, and restart proof. **Import**
will later introduce approved scientific components without creating a second
repository or changing this interface.

The accepted historical end-to-end baseline used 5 Mac CPU workers, 15 Ubuntu
CPU workers, and 1 Ubuntu GPU worker (21 Dask workers). A subsequent isolated
calibration found that 15 logical Chronos worker processes sharing one physical
Ubuntu RTX 5090 were scientifically equivalent, faster, and resource-safe.
The final integrated topology is 5 Mac CPU + 15 Ubuntu CPU + 15 logical
Ubuntu GPU workers = 35 Dask workers. One physical GPU and 15 logical execution
slots are distinct facts. The complete 100-series pipeline and its restart
passed with this topology; measured evidence is recorded in
[`docs/poc2-preparation-completion.md`](docs/poc2-preparation-completion.md).

See the [architecture](docs/architecture.md), [code standards](docs/code-standards.md),
[data contract](docs/data-contract.md), and [local execution contract](docs/local-execution.md).
