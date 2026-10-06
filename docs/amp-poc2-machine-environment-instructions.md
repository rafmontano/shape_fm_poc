# AMP instructions: machine inventory and coordinator selection

Status: Approved implementation request, 6 October 2026.

## Outcome

Implement the approved [machine environment and coordinator decision](machine-environment.md)
on top of the current unpublished ID 026--028 candidate. Then resume the pending
fresh 100-series two-machine acceptance. This is one operational correction and
acceptance continuation, not another scientific checkpoint or redesign.

Normal execution must no longer require the researcher or AMP to export a Mac
IP or `PREFECT_API_URL`. The same implementation must allow a future Mac Studio
to become coordinator by configuration and calibration rather than a production
source-code branch.

## Starting state and preservation

Expected starting state:

- Mac `main` is at `7a23d342a6788e17942408001d3a4145e322d07b` with
  exactly the approved uncommitted machine-environment documentation changes in
  `AGENTS.md`, `docs/architecture.md`, `docs/code-standards.md`,
  `docs/configuration-inventory.md`, `docs/execution-policy.md`,
  `docs/poc2-workflow-orchestration-decision.md`, this instruction file and
  `docs/machine-environment.md`;
- Ubuntu `main` is clean at the same revision;
- implementation commit `534e955655d51febd3ffbe0fb00f0919e17dfd4d`
  and Mantis checkpoint correction `7a23d342a6788e17942408001d3a4145e322d07b`
  are unpublished descendants of `origin/main` at `52b33a8`;
- Ubuntu's existing five stashes are unchanged;
- the stopped preflight database
  `.amp/in/id026-028-v11-first100-acceptance.duckdb` contains no scientific
  work and is not valid acceptance evidence; and
- no owned Prefect, Dask, DTW, Mantis, R or Chronos process is running.

Verify this state before editing. Preserve the approved documentation, both
unpublished commits and all accepted ID 021 and ID 026--028 scientific
behaviour. Do not reset, amend, rebase, force-push, delete or apply stashes. Do
not reuse the stopped preflight database for the successful acceptance.

## Governing references

Read completely before editing:

1. `AGENTS.md`;
2. `docs/research-vision.md`;
3. `docs/code-standards.md`;
4. `docs/poc2-workflow-orchestration-decision.md`;
5. `docs/execution-policy.md`;
6. `docs/machine-environment.md`;
7. `docs/poc2-id026-fitted-model-storage-requirements.md`;
8. `docs/poc2-id026-fitted-model-storage.md`;
9. `docs/poc2-id027-028-mantis-directional.md`; and
10. `docs/amp-poc2-id026-028-model-storage-instructions.md`.

The new machine-environment decision governs operational host resolution. It
does not change DTW, Mantis, Random Forest, labels, representations, model
artifacts, evaluation or the accepted execution safety limits.

## Implementation

### 1. Add the shared machine inventory

Add tracked `config/machines.json` with configuration version 1 and the approved
current records:

- `macbook_pro`: `RMMacbookPro.local`, current Mac project root, CPU and MPS;
- `ubuntu_primary`: `WSUbuntu1.local`, SSH user `rafmontano`, current Ubuntu
  project root, CPU and CUDA; and
- Prefect 4200, Dask scheduler 8786 and Dask dashboard 8787.

Do not store an IP address, password, key, environment path, dependency version,
worker count or scientific setting in this file. Verify the exact hostnames and
paths read-only before finalising them; report a mismatch instead of guessing.

### 2. Resolve inventory and execution profile centrally

Extend the existing configuration responsibility rather than adding a competing
configuration framework. Use one cohesive small object or existing suitable
object to:

- load and validate the inventory once;
- reject unknown fields, unsafe machine IDs, duplicate host identities, invalid
  capabilities, non-absolute project roots and invalid ports;
- resolve the selected execution profile against the inventory;
- require exactly one enabled coordinator;
- expose coordinator, enabled workers, capabilities, roots and service ports to
  the cluster lifecycle; and
- produce a serialisable non-secret resolved topology for execution evidence.

No worker or model script may reread `config/machines.json`. Consumers receive
only the resolved subset they need.

### 3. Evolve execution profiles without duplicate authority

Update the active distributed profile so it selects logical machine IDs and
associates the existing approved worker counts with them. Preserve every
existing memory floor, concurrency limit, fit budget, thread limit, in-flight
limit and accelerator safeguard.

Normalise the active profile to one effective representation. Do not leave
competing `Mac`/`Ubuntu` worker counts in active consumers. Preserve historical
profile compatibility through the central loader only where current tests or
stored evidence require it; do not rewrite historical experiment documents.

Worker and machine names must not enter scientific identity. Existing
operational profile fingerprinting and execution evidence may evolve in the
normal versioned way.

### 4. Remove hardcoded two-host resolution

Replace production assumptions such as fixed `SHAPEFM_UBUNTU_HOST`,
`SHAPEFM_UBUNTU_ROOT`, `en0` Mac discovery and manually required
`PREFECT_API_URL` with values resolved from the inventory and selected profile.
Audit all production launch, preflight, calibration and acceptance paths; do not
correct only the command that just failed.

Environment variables may remain documented explicit per-run overrides, but the
ordinary researcher path must work without them. Apply overrides centrally,
record effective non-secret values and reject invalid combinations.

### 5. Own Prefect and Dask endpoint construction

For a distributed run:

1. select the profile's coordinator;
2. start the owned Prefect service on the coordinator bound to all private-LAN
   interfaces and the configured port;
3. derive `PREFECT_API_URL` from the coordinator hostname and Prefect port;
4. derive Dask scheduler/worker addresses from the coordinator hostname and
   configured ports;
5. propagate the resolved endpoints to remote processes; and
6. stop every owned service and worker on success or failure.

Never use `localhost`, `127.0.0.1` or `0.0.0.0` as a remote client address.
Never write a discovered IP into the tracked inventory. An explicit temporary
address override is acceptable only as a recorded operational override.

Before database or model mutation, verify from every enabled remote host that
the coordinator hostname and Prefect endpoint are reachable. Also retain the
existing SSH, source, lock, dependency, process/port and resource checks. An
enabled unavailable host blocks the run; do not silently downscale.

Local-only profiles use only the coordinator and do not require a remote
endpoint check.

### 6. Preserve storage authority

The selected coordinator is the sole DuckDB writer and owns the authoritative
external fitted-model directory. Workers continue returning bounded results or
staged artifacts through the existing validated consolidation path.

Do not implement automatic coordinator-state migration. Add validation and
documentation only: resuming an existing experiment on another coordinator
requires a deliberate verified transfer of both its DuckDB file and complete
experiment model directory, with only the receiving copy allowed to execute.

### 7. Make a future coordinator a configuration case

Add focused synthetic coverage using a `mac_studio` inventory entry and profile.
The test must prove that coordinator selection, endpoint derivation and worker
resolution change through configuration without production source changes.
Do not add an unverified real Mac Studio record to the committed current
inventory and do not invent future worker counts.

## Focused validation

Add or update focused tests proving:

- current MacBook/Ubuntu inventory and profile resolution;
- exactly one enabled coordinator;
- unknown machine and incompatible capability rejection;
- disabled machines are not required or launched;
- enabled unavailable machines block before scientific mutation;
- Prefect and Dask client endpoints use the coordinator hostname and configured
  ports without manual `PREFECT_API_URL`;
- `0.0.0.0` is used only for binding, never as a client endpoint;
- explicit overrides are central, validated and recorded;
- a synthetic Mac Studio can replace the MacBook as coordinator without source
  branching;
- operational topology does not change scientific IDs or fingerprints;
- existing historical profile/configuration fixtures remain supported where
  required; and
- service cleanup occurs after controlled success and failure.

Run the established local configuration, execution, workflow, CLI, directional,
DTW, Mantis, model-storage and integration suites in their correct locked
environments. Compile changed Python, validate JSON, run `git diff --check`, and
confirm dependency locks remain byte-identical. No scientific calculation or
expected output may be changed to make these checks pass.

## Two-machine acceptance continuation

After local validation succeeds:

1. create one separate implementation commit on top of `7a23d34`;
2. transfer that exact commit to clean Ubuntu `main` without touching its
   stashes;
3. verify exact source/configuration/lock identity, environments, required data,
   checkpoint checksums, resources, ports and no stale processes;
4. start the owned Prefect service through the new automatic resolution path;
5. prove from Ubuntu that the effective Prefect API is reachable;
6. use a fresh database, fresh report and fresh external-model experiment
   namespace, leaving the stopped preflight artifact untouched;
7. run the approved first 100 M4 Daily series through Processes 01--06 with DTW
   and Mantis/Random Forest on both machines; and
8. run the normal restart against the same successful experiment.

Retain the approved ID 026--028 scientific acceptance:

- one complete DTW fitted artifact;
- fourteen horizon-specific Random Forest artifacts;
- 1,400 DTW predictions;
- 1,400 Mantis/Random Forest predictions;
- 28 directional evaluations;
- real eligible contribution from both machines;
- zero accepted failed or duplicate scientific outputs; and
- restart performs no DTW recalibration or Random Forest fitting and preserves
  scientific counts and fingerprints.

Also record:

- selected profile and coordinator machine ID;
- resolved Prefect/Dask endpoints;
- enabled and actual workers per machine;
- source, dependency and model-checkpoint identities;
- resource safety and host contributions; and
- complete owned-service shutdown.

The test evaluates execution, coordination, persistence and restart, not model
accuracy. Stop and preserve evidence on any real preflight, resource, scientific
or integrity failure. Do not weaken safety or silently substitute a local run.

## Documentation and delivery boundary

Update affected architecture, configuration, execution and researcher-facing
documentation to match the implemented path. Preserve historical evidence as
historical evidence; do not rewrite old Mac-coordinator acceptance records.

Do not push GitHub, create closure documentation, tag, amend or declare IDs
026--028 closed in this assignment. After successful local and two-machine
evidence, stop for Chief Architect review and researcher acceptance. Mac and
Ubuntu should remain clean at the exact tested implementation revision; GitHub
may remain at the published baseline until closure is authorised.

## Final report

Report concisely:

1. implemented machine inventory and profile schema;
2. removed hardcoded host/address authorities;
3. current and synthetic future topology resolution;
4. local test results and unchanged lock hashes;
5. exact Mac/Ubuntu tested revision and cleanliness;
6. resolved endpoints and connectivity evidence;
7. scientific/model counts and host contributions;
8. restart/model-reuse evidence;
9. owned-service shutdown;
10. any limitation or failure; and
11. explicit confirmation that nothing was pushed or closed.
