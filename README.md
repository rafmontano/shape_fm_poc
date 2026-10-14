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
restart. Increment D (`D94e12b3`) is researcher-accepted through Process 04;
E/F are being implemented, not accepted. Their bounded acceptance continues
accepted 100-series 04 evidence through only Combine/Evaluate, without rerunning
Forecast or starting from zero. Do not request the default full range yet.

## Target repository layout

POC2-X01 is transforming the active source tree toward this structure:

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
src/r/                          R workers, utilities, tests and QA
results/experiment.duckdb       parent scientific store
results/windows.duckdb          Process 03 high-volume child store
results/tables/                 derived tables
results/figures/                derived figures
models/                         fitted directional meta-learners
docs/                           living documentation only
```

Generated data, models, results, caches and runtime logs are untracked. Git holds
source, configuration, dependency locks and living documentation.
Unmigrated later processes retain their legacy scientific modules until bounded
acceptance of their replacements; the retired import/control-plane path is not
a second researcher entry point.

## Current work

POC2 scientific Part B is accepted; POC2 remains open for the exit backlog. The
active item is [POC2-X01](docs/poc2-x01-architecture-transformation.md), which is
simplifying the six processes without changing accepted science. See
[POC2 scope and status](docs/poc2.md).
