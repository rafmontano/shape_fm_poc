# Portable environments

Installed environments must never be copied between macOS and Ubuntu. Git
stores declarations, interpreter selections, activation files, and lockfiles;
each worker recreates environments locally. The NAS stores data, weights, and
results—not executable environments.

## Core Python environment

The importer uses uv 0.12.18, uv-managed CPython 3.12.14, and one `uv.lock`
resolved for macOS ARM64 and Linux x86_64.

```sh
mkdir -p .tools/uv
curl -LsSf https://astral.sh/uv/0.12.18/install.sh | \
  env UV_INSTALL_DIR="$PWD/.tools/uv" UV_NO_MODIFY_PATH=1 sh
export UV_PYTHON_INSTALL_DIR="$PWD/.tools/python"
export UV_CACHE_DIR="$PWD/.tools/cache"
export UV_PYTHON_PREFERENCE=only-managed
.tools/uv/uv python install 3.12.14
.tools/uv/uv sync --locked
```

Run Python tools with `.tools/uv/uv run --locked`.

## Official GIFT-Eval environment

GIFT-Eval is an HTTPS Git submodule pinned by the parent repository. Either
clone ShapeFM with its submodules:

```sh
git clone --recurse-submodules <shape-fm-repository-url>
```

or initialize them after a normal clone:

```sh
git submodule update --init --recursive
```

Its full software stack is isolated from the lightweight ShapeFM importer:

```sh
.tools/uv/uv sync --project environments/gift-eval --locked
environments/gift-eval/.venv/bin/python -c 'import gift_eval'
```

The parent submodule reference and `config/dependencies/gift_eval.json` pin code
commit `4d5ab3fa0fe7451bbf59bb1ff6dd76e6e414d64a`. Do not edit the submodule.

## Gate 00 data acquisition

One researcher-facing operation initializes the submodule when needed, restores
both locked environments, downloads only pinned M4 Daily, validates code and
data, and reports what is ready:

```sh
Rscript workflows/gate_00_setup/01_prepare_gift_eval.R
```

The default source is `data/source/gift_eval`; the canonical database is
`data/shapefm.duckdb`. `SHAPEFM_GIFT_EVAL_ROOT` and `SHAPEFM_DATABASE` are
optional overrides. To verify without acquisition, run
`02_verify_gift_eval.R`.

The locked acquisition command always uses dataset revision
`30841734ac5cfddbd0c3bad6d09d2b6b32becbb0`:

```sh
.tools/uv/uv run --locked shapefm-acquire m4_daily
.tools/uv/uv run --locked shapefm-acquire complete  # later; not Stage 1
```

It resumes Hugging Face downloads, skips a hash-valid snapshot, validates Arrow
and metadata, and writes an ignored local `source-manifest.json` with file
hashes. Dataset licence and citation references are recorded in the dependency
declaration.

## R environment

Foundation Stage 1 requires R 4.6.1 and renv 1.2.4. On Ubuntu, install the same R version,
preferably with rig, before restoring packages.

```sh
RENV_PATHS_CACHE="$PWD/.tools/renv-cache" \
  Rscript -e 'renv::restore(prompt = FALSE)'
```

Foundation Stage 1 uses DBI and DuckDB. POC 1 adds `forecast` for `tsclean` and
AutoARIMA plus `jsonlite` for the isolated R worker protocol. R Arrow remains
unnecessary. Training environments are deliberately deferred to POC 2, which
will address Mantis and MOMENT separately.

## Chronos-2 environment (POC 1)

Chronos-2 is isolated and locked separately:

```sh
.tools/uv/uv sync --project environments/chronos-2 --locked
```

The model is `amazon/chronos-2` at revision
`29ec3766d36d6f73f0696f85560a422f50e8498c`, used through
`chronos-forecasting==2.2.2`. Weights are cached outside Git. `device=auto`
selects CUDA on Ubuntu when available, then supported Apple MPS, otherwise CPU;
the actual device and float32 dtype are recorded per result. Environments are
restored independently on macOS and Ubuntu rather than copied between them.

POC 1 also adds locked R packages `forecast` 8.24.0 and `jsonlite` 2.0.0.
