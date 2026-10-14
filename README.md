# ShapeFM

ShapeFM is a traceable, restartable time-series research system. It uses one
six-process workflow:

```text
01 Import → 02 Preprocess → 03 Transform → 04 Forecast → 05 Combine → 06 Evaluate
```

The researcher entry point is `src/python/experiment.py`. Prefect coordinates the
workflow, Dask schedules eligible bounded computation, Python/R providers own
scientific calculations, and the selected coordinator writes authoritative
state to DuckDB. MLflow projects researcher-facing milestones without owning
scientific acceptance or restart.

## Documentation

Start with the [documentation index](docs/README.md). It links the current
research vision, architecture, standards, scientific contracts, operation guide
and POC2 status. Superseded instructions and evidence remain in Git history; they
are not current guidance.

## Install and verify

```sh
scripts/setup.sh
scripts/setup.sh verify
```

See [environment and execution](docs/environment.md) for machines, environments,
distributed execution and recovery.

## Active experiment

Before creating a new experiment, edit `config/experiment.json`. Normal commands
use fixed paths:

```sh
.venv/bin/python src/python/experiment.py plan --processes 1-3
.venv/bin/python src/python/experiment.py run --processes 1-3
.venv/bin/python src/python/experiment.py status
```

Use only the process range needed. Existing accepted items are skipped on
restart. The completed 100-series experiment is delivered at the canonical root
paths below. Inspect it with `status` or open its existing tables/figures; do not
rerun, restart or regenerate it for X01 review. Increment D is researcher-accepted;
the complete A–F candidate awaits final researcher review, not another workload.

## Repository layout

The six-process implementation uses this structure:

```text
config/                         active experiment, machines and execution profiles
data/gift_eval/                 pinned canonical source data
src/python/experiment.py        researcher entry point
src/python/p01_import/          Process 01, including canonical source acquisition
src/python/p02_preprocess/      Process 02
src/python/p03_transform/       Process 03
src/python/p04_forecast/        Process 04
src/python/p05_combine/         Process 05
src/python/p06_evaluate/        Process 06
src/python/util/                code used by two or more processes
src/r/                          R workers, utilities and tests
results/experiment.duckdb       parent scientific store
results/windows.duckdb          Process 03 high-volume child store
results/tables/                 derived tables
results/figures/                derived figures
models/<experiment-id>/         fifteen fitted directional artifacts
.runtime/mlflow/                one resumable run and published artifacts
docs/                           living documentation only
```

Generated data, models, results, caches and runtime logs are untracked. Git holds
source, configuration, dependency locks and living documentation.
Normal commands resolve these paths against the repository root, independently
of the current shell directory. Focused tests use self-cleaning temporary roots;
no review workspace or symlink is needed by the delivered experiment.

## Current work

POC2 scientific Part B is accepted; POC2 remains open for the exit backlog. The
active item is [POC2-X01](docs/poc2-x01-architecture-transformation.md), which is
simplifying the six processes without changing accepted science. See
[POC2 scope and status](docs/poc2.md).
