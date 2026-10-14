# Environment and execution

## Installation

From the repository root:

```sh
scripts/setup.sh
scripts/setup.sh verify
scripts/setup.sh --help
```

Setup installs/verifies the pinned project, GIFT-Eval, Chronos-2, Mantis and
classifier environments plus R dependencies and local model/data assets. It is
non-destructive and reuses valid installations. Lockfiles and dependency manifests
are tracked; environments, downloads and generated results are not.

These are the only setup operations. Setup synchronizes the root and retained
Python environments from their locks, restores R with renv, and acquires or
reuses pinned data and model snapshots. Verification is read-only and offline:
it checks small imports and asset presence without loading models or calculating
on a GPU. Setup does not archive directories, write installation reports or run
scientific processes. Scientific execution must not update dependencies
automatically.

## Machines

Stable host facts live in `config/machines.json`; execution selection and worker
capacity live in `config/execution_profiles.json`.

Current topology:

- MacBook Pro: coordinator and CPU worker capacity;
- Ubuntu: CPU workers and the CUDA/Chronos/Mantis worker;
- future Mac Studio: intended coordinator, added by configuration rather than a
  source-code branch.

Normal execution derives Prefect/Dask addresses from the selected coordinator
and machine inventory. The researcher does not manually manage
`PREFECT_API_URL` or hard-code an IP for each run. A hostname/IP problem is an
operational error with a clear message.

## Execution ownership

Prefect runs on the selected coordinator and expresses experiment/process
dependencies. Dask schedules bounded work on enabled workers. The same scientific
callable runs sequentially or through Dask. The coordinator is the sole writer
to both DuckDB files and owns the authoritative fitted-model copy.

R and Python providers run in their pinned environments with one internal thread
where Dask owns parallelism. Chronos uses one persistent GPU model process and a
complete pinned local checkpoint; ordinary execution is offline.

## Normal researcher commands

```sh
.venv/bin/python src/python/experiment.py plan --processes 1
.venv/bin/python src/python/experiment.py run --processes 1
.venv/bin/python src/python/experiment.py status
```

The active experiment uses fixed configuration and output paths; normal commands
do not require experiment names or database paths. Run only the process range
needed. A change to Process 03 is validated through Process 03, not through
Process 06.

During Increment A, request Process 01 explicitly: the new flow path for later
processes and `export` is not available until those increments migrate. The
source layout uses `experiment.py`, importable `p01_import` through
`p06_evaluate` process packages, and shared adapters in `util/`. Later legacy
scientific modules remain for migration, not as an alternative researcher CLI.
Setup acquires canonical data through `p01_import.acquisition`; import execution
itself reads the local pinned source and does not acquire a second dataset.
MLflow records one resumable research run using operational state under
`.runtime/mlflow/`; DuckDB alone decides accepted work and restart.

## Failure and restart

On failure, preserve committed results and record process, meaningful substep,
batch/item and native cause. Stop only the affected work when possible. Do not
delete both databases or repeat completed processes merely to troubleshoot a
later failure.

Infrastructure retry may repeat an uncommitted item but cannot change data,
model, seed or settings. Native crashes and environment qualification belong to
focused diagnostics before another expensive full run. Memory/swap thresholds
may warn for tuning when integrity is intact; corrupt output, provider failure or
database conflict remains fatal.

Before publication or machine synchronization, verify branch/revision,
worktree cleanliness, stopped writers and generated-file exclusions. Push and
remote synchronization require explicit researcher authorization.
