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
workers awake automatically with `caffeinate -is` for the worker process's
lifetime: idle-sleep protection plus system-sleep protection on AC power.
No manual caffeinate command or global power-setting change is required.
Keep the Mac on AC power; forced sleep and unsupported closed-lid operation are
not prevented by this assertion. Run the normal entry point in a persistent
terminal and inhibit Ubuntu sleep while the experiment is active.

### Runtime model files

The coordinator owns canonical directional models. Remote fits return temporary
file references; rsync over the existing SSH connection retrieves complete files
for canonical publication before DuckDB accepts training completion. At 04.04
entry, ordinary incremental rsync prepares the required files on participating
CPU hosts. Providers load local files and retain models for their process-owned
session lifetime; requests and numerical results remain JSON.

There are no model-content checksum gates, Base64 model messages, registration
handshakes or automatic retraining of completed items. Copy/load errors surface
with their native path/host context. Missing worker copies can be recopied;
loss of an authoritative completed model requires researcher direction.

XMP remains researcher-reported disabled. Runtime does not change firmware,
qualify the environment or infer the cause of earlier native failures.

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

Downstream reads select modelling keys, window kind and required fields in
DuckDB before decoding numerical arrays. Individual forecasts load only pending
official contexts; representations load the required cohort/official values;
training loads the reference cohort; prediction/evaluation load official inputs.
The shared executor consumes each returned group's input payloads after accepting
its results and releases its supported Dask futures. Atomic keys remain intact;
restart reconstructs missing requests from DuckDB. Bounded continuous refill
replaces wave barriers as described in [architecture](architecture.md#restart-and-progress).
Group sizes, worker counts, resource placement, calculations and model delivery
remain unchanged; this is an operational execution change only.

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

The noninteractive entry point defaults Python plotting to the `Agg` backend
before importing the workflow and tsai. This avoids GUI/Tk state in flow threads;
explicit backend settings are retained. Publication figures remain rendered by R.

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

The technically accepted full Daily run logged Dask nanny cleanup and Prefect
API-flush errors only after scientific completion, during shutdown. Both the run
and its no-new-work restart exited successfully; accepted results remain preserved.

The Chronos correction's one-in-flight-group recovery was invocation-only, not a
stored experiment or profile change. The current `mvp_two_machine` profile still
permits up to 24 in-flight sensitivity work groups on the coordinator's 28 CPU
workers; a completed-state resume schedules none. Native failures at normal
concurrency remain unresolved; successful recovery does not establish reliability
at the unchanged default concurrency.

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
