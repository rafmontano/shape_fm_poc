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

- Ubuntu: selected coordinator, CPU workers and CUDA/Chronos/Mantis workers;
- MacBook Pro: CPU worker capacity;
- future Mac Studio: intended coordinator, added by configuration rather than a
  source-code branch.

The definitive `mvp_two_machine` operational profile has six Mac CPU workers,
28 Ubuntu CPU workers and sixteen logical GPU workers sharing the RTX 5090.
Process 01 retrieves archived forecasts in one coordinator-host R invocation;
archive loading is not a distributed worker resource. All coordinator
CPU allocations receive `COORDINATOR_CPU`. Worker counts are operational configuration, not scientific
identity. Do not automatically reduce the sixteen GPU workers: GPU startup,
memory or execution failure requires preserving progress and researcher
direction. CPU sensitivity investigation does not start GPU model processes.

The profile's `subflows.06.03.resources` requires `CPU` and `COORDINATOR_CPU`.
The shared executor applies that constraint to every sensitivity group, without
fallback to the peer. Worker allocations start when eligible work first needs
them; GPU services do not start for CPU-only work. Selecting a different
coordinator moves archived retrieval, sensitivity, database writes, tracking and
publication together without changing scientific functions or identities.
Remote SSH worker startup includes the native R executable paths and keeps Mac
workers awake with `caffeinate`. Run the normal entry point in a persistent
terminal and inhibit Ubuntu sleep while the experiment is active.

Ubuntu coordination has executed Processes 01–03 with the approved CPU profile
and started all sixteen logical GPU workers for Process 04. Full Ubuntu
completion is not established: the current experiment is stopped in 04.01 after
the initial archived-provider failure and two authorised recovery attempts.
Rebuilding locked M4comp2018 0.2.0, including xz lazy-data compression, did not
resolve native R loading failures; the cause remains unestablished. The two
canonical databases retain accepted work, including 559 individual forecasts.
Subflow 01.04 now imports missing archives into common forecast storage, with
atomic completion references rather than duplicate vectors. Focused checks cover
partial restart, unchanged accepted timestamps, common retrieval and Forecast's
import prerequisite. An isolated Ubuntu Process 01 run imported 200 forecasts for
100 Daily series; its repeated import added zero items without reopening the
archive. Preserved full-experiment continuation remains to be completed. Do not
reset the experiment or move archived work to Mac. The separate ordinary R
forecast worker SIGSEGV occurred during `renv_scope_lock` in sandbox activation,
before fitting; this design change does not establish a fix for that failure.

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
