# Approved execution and machine availability policy

Approved on 30 September 2026. This is the current operational decision for
ShapeFM heavy testing, including seasonal-period tuning. It supersedes earlier
Mac-only tuning guidance and prevents historical calibration or smoke-test
settings from becoming the heavy-test default. Scientific decisions are unchanged.

The [workflow orchestration standard](poc2-workflow-orchestration-decision.md),
approved on 2 October 2026, adds Prefect above the existing Dask execution layer.
Stage 1 was accepted for progression on 3 October 2026 with remaining checks
carried into Stage 2; full migration acceptance remains open under the current
AMP instructions. The migration does not change this
policy, worker capacities, memory floors or availability rules. Prefect consumes
the same resolved profile and owns the bounded application retry on its paths;
it does not introduce another compute-worker pool or stacked retry authority.

Updated approval on 30 September 2026: increase Mac CPU capacity from five to
eight workers and correct the three execution safeguards identified in review.
The 800-forecast seasonal test is complete and its results remain accepted.
The [approved follow-up](amp-poc2-execution-safeguards-instructions.md) is now
implemented: heavy tuning requires the exact versioned distributed profile or a
recorded researcher-approved local exception, placement and limits come from
that profile, and owned R process trees are monitored throughout execution.

The [acceptance record](poc2-seasonal-period-tuning-results.md) preserves the
completed five-Mac-worker run separately from the focused eight-worker safeguard
evidence. Do not repeat the completed acceptance. The researcher reviewed the
safeguard result and authorised ID 010 implementation on 1 October 2026; its
heavy tests remain subject to this policy.

## Approved worker capacity

| Pool | Capacity | Role |
| --- | ---: | --- |
| Mac CPU | 8 workers | CPU work; Mac also owns coordination and scheduling |
| Ubuntu CPU | 15 workers | CPU work |
| Ubuntu GPU | 15 logical workers | Approved GPU workloads on one physical RTX 5090 |

The approved CPU/GPU capacity is now 38 workers; the scheduler is not a worker.
R-only AutoARIMA/ETS work uses the 8 + 15 CPU pools, or 23 workers. GPU work is
not required: the 15 logical slots are neither 15 physical GPUs nor additional
R workers. Do not load Chronos or launch GPU workers solely to meet a total
unrelated to the selected workload.

The earlier five-Mac-worker topology and actual contributions were accepted in
the [Preparation completion record](poc2-preparation-completion.md). That
historical evidence must not be relabelled as an eight-worker test. The older
2 Mac + 4 Ubuntu calibration, Objective 1's one-CPU/one-GPU test, and the
interrupted tuning run's one-Mac configuration have historical or test-specific
roles. Preserve their evidence and stored experiments, but never select those
settings implicitly for a new heavy test or this recovery.

## One effective execution configuration

Reuse the existing configuration interface and scheduler. Implement one named,
versioned machine-readable profile for the approved capacities and safety
controls; document its exact owner. Do not maintain competing worker counts in
launchers, validators, model-specific loops and test harnesses. They must consume
the same resolved execution object. Small tests explicitly declare their limited
purpose and profile, rather than inheriting a hidden reduced default.

Creation captures execution defaults in the experiment document and DuckDB.
For resume, preserve the original/resolved documents and integrity hashes.
Apply the approved profile as an explicit operational override through the
existing researcher entry point and record resolved values in append-only
execution history. Editing the source experiment JSON alone does not update an
existing database. Do not mutate scientific identity to change worker placement
or silently reinterpret old stored profiles.

Before expensive work, report and validate the selected profile/version,
workload, expected and actual pools per host, thread and in-flight limits,
memory headroom, host availability, tested source identity and dependencies.
An unexplained mismatch blocks heavy execution. The option
`--processes 4` selects Gate 4; it does not request four workers.

The heavy-run guard is mandatory even when an optional profile flag is omitted.
Fail clearly unless the approved distributed profile or a recorded,
researcher-approved local exception is selected. Explicitly bounded lightweight
tests and read-only inspection remain possible; do not exempt every invocation
without a profile as a supposed small test. Apply the guard through the shared
entry/coordinator path, not only one recovery command.

Profile-owned placement, concurrency, host floors and fit budgets must reach
the consumers unchanged. Remove the fixed every-twentieth-ETS allocation and
one-task Mac queue. Eligible work should continue reaching the Mac while its
approved resources permit it; no artificial participation quota. Eight workers
must be available for useful eligible computation, not merely registered as
permanently unused capacity. The current profile owner is
`config/execution_profiles.json`; evolve its reviewed runtime profile and record
the effective version or fingerprint without changing historical snapshots.

### Approved profile reconciliation

Approved on 3 October 2026 for the Stage 1 closure follow-up; implementation and
subsequent Stage 1 acceptance with limitations are recorded in the acceptance
record. The approved change evolves `poc2_seasonal_recovery` from runtime profile
v2 to v3 so GPU workloads receive the existing 4 GiB accelerator headroom rather
than v2's zero value. Extend the existing managed lifecycle to support the
configured GPU pool when required, with launchers and validators consuming the
same effective profile. Reconcile the exact-version tuning guard with v3.

Preserve the approved 8 Mac/15 Ubuntu CPU capacities, 15 logical Ubuntu GPU
workers, host floors, 12 GiB R fit budget, AutoARIMA concurrency cap of 8, other
thread/batch/admission limits and `cpu_gpu_overlap=false`. Worker capacity is
not a requirement to run every fit or load every GPU model concurrently.
CPU-only work must not launch an unused GPU pool. Ordinary forecasting must
enforce model-specific concurrency caps as well as the overall in-flight limit.

Keep prior runtime snapshots and experiment hashes unchanged. Apply v3 as an
explicit operational override with its resolved values/fingerprint recorded in
execution history, not a silent reinterpretation of stored v2. This approval
does not authorise scientific changes. The later
[pragmatic Stage 2 amendment](poc2-workflow-orchestration-decision.md#pragmatic-stage-2-amendment)
authorises Stage 2 and checkpoint publication. Deferred CPU-overlap and
scheduler-loss checks move into Stage 2; the safeguards above are not relaxed.

Runtime profile v4 implements the separately approved machine-environment
decision without changing those v3 capacities or safeguards. It replaces active
Mac/Ubuntu-specific allocation fields with `coordinator` and logical `machines`
entries resolved against `config/machines.json`; historical v3 evidence remains
unchanged.

## Machine inventory and coordinator selection

Approved on 6 October 2026: use the
[machine-environment decision](machine-environment.md) for host discovery and
coordinator selection. `config/machines.json` describes stable hosts, project
roots, capabilities and service ports. The selected execution profile chooses
exactly one coordinator, enabled machines and their worker allocations.

The current active profile continues to select the MacBook Pro as coordinator
and Ubuntu as worker; all approved capacities and safety limits in this policy
remain unchanged. The design does not claim that the future Mac Studio has been
calibrated. After it arrives, add its inventory record, calibrate it, and approve
a profile before it performs research work.

The selected coordinator owns Prefect, the Dask scheduler, the sole writable
DuckDB connection and the authoritative external fitted-model directory. Normal
execution derives `PREFECT_API_URL` and Dask endpoints from the coordinator's
configured hostname and service ports. It must not require a manually exported
IP address. Bind services only to the private LAN for the owned run, validate
reachability from every enabled worker before scientific mutation, and stop all
owned services afterward.

An enabled unavailable machine blocks execution; ShapeFM does not silently
downscale. A disabled machine may be off. Moving an existing experiment to a new
coordinator requires a deliberate verified transfer of both DuckDB and its
external fitted-model directory, after which only the receiving coordinator may
write. Do not introduce automatic state migration in the MVP.

## Memory safety and workload placement

Keep one thread per R job and avoid nested parallelism. Python on the selected
coordinator is the sole DuckDB writer; remote workers receive serializable inputs
and return results, never database connections.

Capacity is not permission to run eight memory-intensive fits simultaneously
on the 16 GiB Mac. The completed recovery reported an Ubuntu R child reaching
approximately 12 GiB, exceeding the 5 GiB AutoARIMA admission estimate and
Dask's 6 GiB worker limit. Account for the R child process tree as well as
Python/Dask memory; a parent-worker limit does not cap its R children.

Use the existing bounded scheduler with explicit memory-aware admission and
placement, retaining the approved pools. Preserve at least 3 GiB Mac and 16 GiB
Ubuntu available host memory; the 4 GiB GPU headroom applies when GPU work is
used. Report configured workers, active fits and throttling reasons separately.
Prefer suitable placement of large fits on Ubuntu; do not silently replace the
profile with fewer workers or bypass safety checks to keep workers busy.
Resolve and report per-worker/in-flight budgets before the heavy run.

Admission must reflect the observed tail, and safety monitoring must continue
during fits, not stop after a pre-launch memory sample. Stop admitting work on
pressure; if active work itself threatens the floor, use a documented bounded
response for owned processes and retain retryable state. Distinguish resource
interruptions from model failures and scientific baseline substitution.
Increasing capacity must not remove the floors or force eight heavy fits.
When fewer jobs can safely run, report measured reasons rather than a hidden
one- or two-worker cap.

Monitor both hosts. Memory pressure, swap growth, worker loss or an unavailable
host requires a visible safety response and durable recovery, not a clean
acceptance claim. Do not change model settings, periods, folds, transformations
or timeouts merely to accelerate a run. Repeated failures require diagnosis
rather than an unbounded retry loop.

## When the laptop is away from Ubuntu

Until the researcher says otherwise, both machines are required for heavy
testing. The Mac is a laptop; Ubuntu may be unavailable during remote work.
The researcher will advise when that applies. Lost connectivity is not automatic
permission for a long Mac-only replacement run.

If Ubuntu is unavailable, leave distributed acceptance outstanding and continue
only safe independent lightweight checks. Ask for direction before heavy local
execution. If a local exception is approved, record its scope, limits and approval
in execution evidence; do not replace the standing profile. The exception does
not prove two-machine acceptance. When Ubuntu returns, repeat synchronisation
and readiness checks before distributed work.

GitHub and host synchronisation are separate actions. Check their state before
use, preserve local work, and do not assume the latest work was published.
Do not commit/push unrelated work or infer new publication permission from an
availability notice. Do not widen network exposure or disable SSH host-key
checking to make a remote connection work.

## Ubuntu OS-upgrade maintenance pause

Recorded on 3 October 2026 for the planned Ubuntu OS upgrade. After the accepted
ID 013/014 source is published and safely synchronized, Ubuntu enters a project
maintenance pause. Do not start ShapeFM jobs, workers, Prefect/Dask services,
database writers, automatic recovery or another migration item until the
researcher confirms that the upgrade is complete. This pause does not authorise
the upgrade, reboot or shutdown.

After that confirmation, perform only lightweight connectivity, source,
environment and relevant GPU/driver readiness checks before further project
work. Reapply the normal source-synchronization, dependency and execution-profile
checks before any later distributed run; do not treat pre-upgrade process or
service state as restart authority.

## Safe synchronisation and evidence

Inspect both checkouts and active processes before starting workers. Synchronise
reviewed source, tests, configuration, documentation and locks without deleting
or overwriting conflicting work. Matching Git HEAD is insufficient when relevant
uncommitted files exist: verify a manifest of the actual tested source and
relevant submodules/locks on both hosts. Restart only test-owned workers.

Keep native environments local. Do not copy Mac virtual environments, compiled
R libraries, secrets, databases, results or machine-specific settings to Ubuntu.
The reviewed tracked `config/machines.json` inventory is shared project
configuration and is synchronised with source; machine-local overrides,
credentials, environments and caches are not.
Restore required missing packages from approved locks using the existing
non-destructive installation approach, without unapproved upgrades or resets.
If conflicts prevent safe synchronisation, stop before testing and report them.

Heavy distributed acceptance requires real completed work on both hosts,
overlapping execution, per-host task counts, source/dependency identity,
resource evidence and a small sequential comparison with declared numerical
tolerances. Connected workers or a parallel pure helper function are not
forecasting acceptance. Test rejection of mismatched configuration, stale
source, missing dependencies and unavailable hosts as well as success.

Preserve successful scientific work on interruption. Use normal recovery for
orphaned attempts; record actual failures, timeouts and substitutions. Never
fabricate success or silently recompute completed results. After recovery,
prove that another resume skips completed work and preserves fingerprints.
