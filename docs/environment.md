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
At the end of Python environment synchronization, setup removes generated
`src/*.egg-info` directories with a simple shell cleanup, not an additional
cleanup framework. Generated packaging metadata is not retained source.

## Machines

Stable host facts live in `config/machines.json`; execution selection and worker
capacity live in `config/execution_profiles.json`.

Current topology:

- MacBook Pro: coordinator and CPU worker capacity;
- Ubuntu: CPU workers and the CUDA/Chronos/Mantis worker;
- future Mac Studio: intended coordinator, added by configuration rather than a
  source-code branch.

The definitive `mvp_two_machine` operational profile has six Mac CPU workers,
28 Ubuntu CPU workers and sixteen logical GPU workers sharing the RTX 5090.
One Mac CPU worker retains the restricted `ARCHIVED` resource for coordinator-host
reference retrieval. Worker counts are operational configuration, not scientific
identity. Do not automatically reduce the sixteen GPU workers: GPU startup,
memory or execution failure requires preserving progress and researcher
direction. CPU sensitivity investigation does not start GPU model processes.

The profile's `subflows.06.03.resources` requires `CPU` and `MAC_CPU`; only Mac
CPU allocations advertise `MAC_CPU`. The shared executor applies that resource
constraint to every sensitivity group. When this is the first pending workload,
only eligible Mac allocations start; subsequent CPU selection work reuses them.
An already running full cluster retains its workers, but Ubuntu workers remain
ineligible for 06.03. Mac placement is supported; the unestablished Ubuntu crash
cause is a limitation, not a separate qualification requirement. The POC2 closure
policy is defined in [evaluation](evaluation.md#parallel-execution-and-restart).

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

The two-machine profile bounds Dask's `worker.connections.incoming` to 1 on
both hosts to limit outgoing large-data service requests. Dask permits twice
that limit for same-host peers. This network-transfer bound does not reduce
worker counts, threads or the maximum in-flight scientific work groups.

## Normal researcher commands

```sh
.venv/bin/python src/python/experiment.py plan --processes 1-3
.venv/bin/python src/python/experiment.py run --processes 1-3
.venv/bin/python src/python/experiment.py status
```

The active experiment uses fixed configuration and output paths; normal commands
do not require experiment names or database paths. Run only the process range
needed. A change to Process 03 is validated through Process 03, not through
Process 06.

The accepted experiment is in canonical `results/`, `models/` and
`.runtime/mlflow/`, not a review workspace. Do not recalculate accepted science
merely to inspect or review it. The normal entry point defaults to
these repository-root locations. The source layout uses `experiment.py`,
importable `p01_import` through `p06_evaluate` process packages, and shared
adapters in `util/`; superseded execution paths have been retired.
R uses the same `pNN_process` folders and descriptive `snake_case` filenames;
process-only workers/helpers live in their process folder, shared code in
`util/` is used by at least two processes, and each language keeps central tests.
Setup acquires canonical data through `p01_import.acquisition`; import execution
itself reads the local pinned source and does not acquire a second dataset.
MLflow records one resumable research run using operational state under
`.runtime/mlflow/`; DuckDB alone decides accepted work and restart.

All operational state belongs under repository-root `.runtime/`: Prefect in
`.runtime/prefect/`, Dask in `.runtime/dask/` and MLflow in `.runtime/mlflow/`.
The normal researcher entry point sets `PREFECT_HOME` to `.runtime/prefect/`
early, before importing Prefect. The obsolete root `.prefect/` is removed rather
than retained as a second operational location. These paths do not enter
scientific identity.

## Failure and restart

On failure, preserve committed results and record process, meaningful substep,
batch/item and native cause. Stop only the affected work when possible. Do not
delete both databases or repeat completed processes merely to troubleshoot a
later failure.

Dask workers enable Python's fault handler. Failed native batches identify their
candidate keys, host and worker in exception notes. Integer-limit serialization
failures additionally report suspect field paths, types and bit lengths without
decimal-formatting the integers. This inspection runs only after serialization
fails; it does not add a normal full-cohort validation pass or repair input.

Infrastructure retry may repeat an uncommitted item but cannot change data,
model, seed or settings. Native crashes and environment qualification belong to
focused diagnostics before another expensive full run. Memory/swap thresholds
may warn for tuning when integrity is intact; corrupt output, provider failure or
database conflict remains fatal.

Before publication or machine synchronization, verify branch/revision,
worktree cleanliness, stopped writers and generated-file exclusions. Push and
remote synchronization require explicit researcher authorization.
