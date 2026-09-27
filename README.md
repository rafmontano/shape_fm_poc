# ShapeFM

ShapeFM has one intended researcher entry point:

```sh
.tools/uv/uv run --locked python src/python/00_main.py --help
```

It never installs or synchronizes environments automatically. Supplying no
action only displays help.

## Deterministic acceptance case

Inspect the dry plan for the first 100 official M4 Daily series:

```sh
.tools/uv/uv run --locked python src/python/00_main.py plan --series-limit 100
```

After the local checkpoint and explicit approval for two-machine execution,
run the complete import-to-official-evaluation acceptance action from the Mac:

```sh
.tools/uv/uv run --locked python src/python/00_main.py test
```

Run the same command a second time to prove restart behaviour against the same
isolated database. The acceptance database and generated report are written
under ignored `results/`; `data/shapefm.duckdb` is never used by this action.

Inspect the isolated experiment without writing:

```sh
.tools/uv/uv run --locked python src/python/00_main.py status
```

The acceptance topology is five Mac CPU workers and sixteen Ubuntu workers:
fifteen CPU workers plus one dedicated CUDA worker. Python remains the only
DuckDB writer and official GIFT-Eval remains the evaluation authority.

See [architecture](docs/architecture.md), [data contract](docs/data-contract.md),
and [POC2 preparation instructions](docs/amp-poc2-preparation-instructions.md).
