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

### Preserved Ubuntu experiment

Experiment `f78d1042-bc9f-434a-b162-daacb9c25d71` retains its canonical parent and
child databases and MLflow run `79f69155d77b43a2892844bf560d0267`. Processes 01–03
remain accepted: 4,227 canonical/prepared series, 128,933 modelling inputs and
14,227 reusable-feature items. Their accepted records and timestamps, and all
559 forecasts accepted before archived-ingestion implementation, are unchanged.

Subflow 01.04 imports archives into common forecast storage with atomic
completion references, not duplicate vectors. The full Ubuntu invocation loaded
M4comp2018 once and imported 8,390 missing forecasts, reusing 64; all 8,454
SMYL/FFORMA inputs are now accepted. A common canonical-series/window query
returns AutoARIMA, Chronos, SMYL and FFORMA together. Focused checks passed for
association, partial restart, unchanged timestamps, common retrieval and
Forecast's prerequisite/reuse behaviour. An isolated Process 01 run imported 200
forecasts for 100 real Daily series; repeated import added zero items without
reopening the archive or fitting models. Its temporary experiment was removed.

The full continuation is stopped in 04.01 with **35,348 of 67,632** forecasts
accepted (26,894 calculated and 8,454 imported). After an initial coordinator
`MemoryError` while reading modelling inputs, the first recovery died with a
Python general-protection fault. The final permitted recovery committed another
26,399 calculated forecasts before coordinator Python SIGSEGV during
`cloudpickle.reducer_override` → Dask message serialization at approximately
13:44:02 on 17 October 2026. Its log is
`.runtime/dask/full_daily_ingestion_recovery_2.log`; fault tracing identified no
scientific provider or atomic key for the in-flight serialized message. There
were 5,203 completed work-group log entries, which are not an acceptance count.
No further recovery was started. The native crash cause remains unestablished;
ample available memory and a successful standalone input read do not diagnose it.

Both machines performed calculation work; all 50 approved workers registered.
Ubuntu owned ingestion, commits, tracking and GPU forecasts (1,884 new Chronos
forecasts in this continuation); Mac and Ubuntu CPU workers both executed native
groups. Exact per-host CPU item totals are not recorded by the existing forecast
envelope. No worker counts, scientific settings, dependencies or accepted values
were changed. The separate earlier ordinary-R SIGSEGV in `renv_scope_lock`
during sandbox activation remains distinct and undiagnosed; successful ingestion
does not establish a fix for it.

Owned execution services and writers are stopped, and the same resumable MLflow
run is marked failed. Both databases remain on Ubuntu. Required fitted models,
five CSVs and six figures have not been produced; later subflows and Processes
05–06 were not reached. A full no-new-work restart cannot be claimed while the
experiment is incomplete. Further execution requires researcher direction under
the exhausted bounded recovery rule, not a reset or archive retrieval on Mac.

Recorded flow timings (seconds; accepted earlier science was not repeated):

| Process / subflow | Elapsed |
| --- | ---: |
| 01 canonical import, before archive addition | 101.501 |
| 01.01 / 01.02 / 01.03 | 2.002 / 97.415 / 1.002 |
| 01.04 full archived import, including planning/commit | 92.024 |
| 02 Preprocess | 191.438 |
| 02.01 / 02.02 | 188.388 / 1.002 |
| 03 Transform | 6,481.968 |
| 03.01 / 03.02 / 03.03 / 03.04 | 8.219 / 5,798.722 / 672.850 / 1.003 |
| 04.01 final continuation, start to last pre-crash task log | 5,919.176 (incomplete) |
| 04.02–04.05 / 05 / 06 | Not reached |

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
restart reconstructs missing requests from DuckDB. Fixed group sizes, waves,
resource placement and successful-group draining on failure are unchanged.

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
