# Machine environment and coordinator selection

Status: Approved by the researcher on 6 October 2026. The local implementation
is complete; its required two-machine acceptance remains pending.

## Decision

ShapeFM separates two operational questions:

1. `config/machines.json` describes the available machines and how ShapeFM can
   reach them.
2. `config/execution_profiles.json` selects one coordinator, the participating
   machines and their approved worker allocations for an execution.

The experiment configuration continues to select an execution profile. It does
not contain hostnames, project paths, changing IP addresses or per-machine
connection details. Machine placement is operational and does not change the
scientific identity of an experiment.

This is a small configuration boundary, not a fleet-management system. It
supports the current MacBook Pro plus Ubuntu environment and allows a future Mac
Studio to become coordinator without changing production source code.

## Machine inventory

The tracked machine inventory has this initial shape:

```json
{
  "configuration_version": 1,
  "machines": {
    "macbook_pro": {
      "hostname": "RMMacbookPro.local",
      "project_root": "/Users/monta/Documents/Projects/shape_fm_poc",
      "capabilities": ["cpu", "mps"]
    },
    "ubuntu_primary": {
      "hostname": "WSUbuntu1.local",
      "ssh_user": "rafmontano",
      "project_root": "/home/rafmontano/Documents/PhD/2026/projects/shape_fm_poc",
      "capabilities": ["cpu", "cuda"]
    }
  },
  "services": {
    "prefect_port": 4200,
    "dask_scheduler_port": 8786,
    "dask_dashboard_port": 8787
  }
}
```

Machine identifiers such as `macbook_pro` and `ubuntu_primary` are stable
logical names. A machine record contains only stable operational facts:

- LAN hostname;
- optional SSH user for remote execution;
- repository location on that machine; and
- available compute capabilities.

The inventory must not contain passwords, keys, dependency environments,
scientific settings, worker counts or a changing IP address. It is committed and
synchronised because every participant must resolve the same logical machines.
Native environments, credentials and secrets remain local.

## Execution profiles

An execution profile references machine identifiers from the inventory. The
active two-machine profile conceptually contains:

```json
{
  "coordinator": "macbook_pro",
  "machines": {
    "macbook_pro": {
      "enabled": true,
      "cpu_workers": 8
    },
    "ubuntu_primary": {
      "enabled": true,
      "cpu_workers": 15,
      "gpu_capacity": 15
    }
  }
}
```

The profile retains the existing memory floors, admission limits, model-specific
concurrency and other safety controls. Those values are not copied into the
machine inventory. Historical profiles remain interpretable; the active profile
must have one normalised representation and no competing Mac/Ubuntu worker-count
fields after migration.

Exactly one enabled machine is the coordinator. An enabled non-coordinator is a
worker host. A disabled or omitted machine may be switched off and does not
participate. An enabled but unavailable machine stops preflight; ShapeFM must not
silently reduce the approved topology.

## Coordinator responsibilities

The selected coordinator owns:

- the researcher entry point;
- the temporary self-hosted Prefect service;
- the Dask scheduler;
- the sole writable DuckDB connection;
- the authoritative external fitted-model directory;
- result validation and consolidation; and
- execution evidence and controlled service shutdown.

Workers receive bounded validated tasks and return results. They never receive a
writable DuckDB connection and never become an independent research authority.
The coordinator may also contribute eligible CPU or accelerator work when the
profile enables it.

## Network resolution

ShapeFM derives service addresses at execution time. The coordinator's Prefect
service listens on all of its active private-LAN interfaces; clients connect
through the coordinator's configured hostname, for example:

```text
http://RMMacbookPro.local:4200/api
```

`0.0.0.0` is a server bind address and must never be used as the client URL.
`PREFECT_API_URL`, the Dask worker scheduler address and related endpoint values
are derived runtime values, not researcher inputs. Environment variables may
remain explicit temporary overrides, but normal execution must not require the
researcher to export them.

The implemented temporary override is `SHAPEFM_COORDINATOR_ADDRESS`. It is
resolved once with the inventory/profile, recorded in execution evidence and
rejected when it is a loopback or server bind address. Legacy per-host and Dask
address variables are not machine authorities.

This hostname-based approach permits the coordinator to move between Wi-Fi and
a network adapter without storing a changing IP address. Before creating
scientific work, preflight verifies from every enabled remote machine that:

- the coordinator hostname resolves;
- Prefect is reachable on its configured port;
- Dask ports are available and reachable as required;
- SSH trust and the configured project path are valid; and
- the expected source, locks, dependencies and resources are present.

If hostname resolution is unavailable, an explicit temporary address override
may be supplied and recorded. ShapeFM must not edit the tracked inventory merely
to persist an automatically discovered IP address.

## Execution flow

```text
experiment configuration
        |
        v
selected execution profile ---- machine inventory
        |                              |
        +---------- resolve -----------+
                       |
                       v
        coordinator + enabled worker hosts
                       |
              Prefect orchestration
                       |
                Dask scheduling
                       |
          native bounded worker results
                       |
        coordinator validation and storage
```

The central execution path performs these steps:

1. Load and validate the machine inventory.
2. Load the selected execution profile.
3. Resolve exactly one enabled coordinator and all enabled workers.
4. Validate machine capabilities against requested workloads.
5. Start owned Prefect and Dask services on the coordinator.
6. Derive and verify the effective private-LAN endpoints.
7. Start the configured workers and verify the exact topology.
8. Execute, validate and persist work through the existing architecture.
9. Record the effective machines, addresses, resources and contributions as
   execution evidence.
10. Stop all owned services and remote workers.

Local-only execution uses a profile containing only the coordinator. It does not
require a remote Prefect endpoint check.

## Adding the Mac Studio

Adding the future Mac Studio requires operational configuration and calibration,
not a production-code branch:

1. Add `mac_studio` to `config/machines.json` with its stable hostname, project
   root and capabilities.
2. Synchronise the exact reviewed Git revision and prepare the approved locked
   environments and required data.
3. Establish verified private-LAN and SSH connectivity to intended workers.
4. Calibrate safe worker and memory limits.
5. Add a reviewed execution profile selecting `mac_studio` as coordinator.
6. Enable or disable the MacBook Pro in that profile as the researcher chooses.
7. Run a bounded multi-machine acceptance before using the profile for research.

A likely future profile is:

```text
coordinator: mac_studio
workers:     mac_studio + ubuntu_primary
optional:    macbook_pro enabled or disabled by profile
```

CPU and accelerator jobs are routed by declared capabilities rather than by
operating-system names. Worker counts are selected only after calibration.

## Changing coordinator

A new experiment may begin directly on the selected coordinator. To resume an
existing experiment on another coordinator, first transfer and verify the
complete authoritative experiment state:

- its DuckDB file; and
- its external fitted-model experiment directory.

Only the receiving coordinator may execute afterward. The old and new copies
must never both act as writable authorities for the same experiment. The MVP
uses an explicit controlled transfer with checksums; it does not introduce an
automatic state-migration service.

## Identity and evidence

Machine selection, network address, worker count and coordinator identity are
operational. They do not enter the scientific fingerprint. The effective
topology, source revision, dependency state, addresses, host contributions and
resource evidence are recorded in execution history.

Changing machines must not change dataset membership, preparation, seed, model
definition, evaluation inputs or intended scientific outputs. Existing content
fingerprints continue to expose unexpected output differences, including any
hardware-dependent difference.

## Initial implementation acceptance

Implementation is accepted only when:

1. Normal two-machine execution requires no manually exported
   `PREFECT_API_URL` or Mac IP address.
2. The active profile resolves the MacBook Pro coordinator and Ubuntu worker
   from `config/machines.json`.
3. A synthetic profile can select a future `mac_studio` coordinator without a
   source-code change.
4. Missing, duplicate, disabled, unreachable or capability-incompatible
   machines fail clearly before scientific work.
5. Existing profile safety controls, single-writer storage, restart and
   scientific identities remain unchanged.
6. The pending fresh ID 026--028 first-100 M4 Daily two-machine run completes,
   followed by its model-reuse restart, using this resolution path.
7. Effective topology and host contribution are queryable as execution evidence.
8. All owned Prefect, Dask and worker processes stop after testing.

This acceptance reuses the already-approved ID 026--028 campaign; it does not
create another independent scientific experiment or accuracy target.
