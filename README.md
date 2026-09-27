# ShapeFM

ShapeFM has one intended researcher entry point:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py --help
```

`00_main.py` never installs or synchronizes environments. Ordinary
`uv run --locked` may synchronize the existing environment; the commands below
use `--no-sync` and therefore require an already prepared environment.
Supplying no action only displays help.

## Deterministic acceptance case

Inspect the dry plan for the first 100 official M4 Daily series:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py plan --series-limit 100
```

After the local checkpoint and explicit approval for two-machine execution,
run the complete import-to-official-evaluation acceptance action from the Mac:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py test
```

Run the same command a second time to prove restart behaviour against the same
isolated database. The acceptance database and generated report are written
under ignored `results/`; `data/shapefm.duckdb` is never used by this action.

Inspect the isolated experiment without writing:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py status
```

Read the official evaluation results from the same isolated acceptance database:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py results
```

Read one stored forecast by supplying all three selectors:

```sh
.tools/uv/uv run --locked --no-sync python src/python/00_main.py results \
  --variant-id VARIANT_ID --series-id SERIES_ID --candidate CANDIDATE
```

The accepted end-to-end baseline used 5 Mac CPU workers, 15 Ubuntu CPU workers,
and 1 Ubuntu GPU worker: 21 Dask workers in total. The subsequently approved
target uses 5 Mac CPU workers, 15 Ubuntu CPU workers, and 15 logical Ubuntu GPU
workers sharing one physical RTX 5090: 35 workers in total. The 15-GPU-worker
calibration passed scientific-equivalence, throughput, and resource-safety
checks. The complete 100-series pipeline has not yet been rerun with the
integrated 35-worker target, so that topology remains pending integrated
acceptance. Python remains the only DuckDB writer and official GIFT-Eval remains
the evaluation authority.

See [architecture](docs/architecture.md), [data contract](docs/data-contract.md),
and [POC2 preparation instructions](docs/amp-poc2-preparation-instructions.md).
