# ==============================================================================
# acceptance.py
#
# Purpose: Run the fixed two-machine acceptance workflow and record auditable evidence.
# Inputs: Repository, database, report, and invocation metadata; both hosts and their prepared locked environments.
# Outputs: Acceptance DuckDB state and a JSON report containing scientific, restart, topology, and resource evidence.
# Run from: Imported; not run directly.
# ==============================================================================

"""Run and audit the fixed two-machine ShapeFM acceptance workflow."""

from __future__ import annotations

import collections
import hashlib
import json
import os
import shlex
import socket
import subprocess
import threading
import time
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

import duckdb

from util.configuration import ExperimentConfiguration, canonical_json
from util.database import initialize_experiment_database, load_database_configuration
from util.distributed_execution import (
    CHRONOS_GPU_RESOURCE,
    worker_resource_snapshot,
)
from util.execution_profiles import ExecutionProfile, ExecutionSettings
from util.experiment_execution import POC1Coordinator
from util.import_execution import ImportCoordinator


# Experiment globals: acceptance scope and expected scientific row/task counts are
# derived from authoritative experiment JSON on creation or DuckDB on resume and
# recorded in database/report evidence; zero/empty values are pre-activation only.
SERIES_LIMIT = 0
EXPECTED_TASK_COUNTS: dict[int, int] = {}
EXPECTED_FORECASTS = 0
EXPECTED_EVALUATIONS = 0
EXPECTED_CHRONOS_TASKS = 0
# Execution globals: worker topology, concurrency, and host-memory evidence limits
# are activated from JSON or DuckDB and recorded in the acceptance report; zero
# values are pre-activation only and cannot override authoritative configuration.
MAC_CPU_WORKERS = 0
UBUNTU_CPU_WORKERS = 0
UBUNTU_GPU_WORKERS = 0
PHYSICAL_GPU_COUNT = 0
EXPECTED_WORKERS = 0
MAX_IN_FLIGHT = 0
MAC_SYSTEM_MEMORY_BYTES = 0
UBUNTU_SYSTEM_MEMORY_BYTES = 0
# Execution global: active contract, set once per invocation from JSON or DuckDB authority.
_ACTIVE_CONFIGURATION: ExperimentConfiguration | None = None
# Code constant: exact bytes per GiB used to interpret configured memory values.
GIB = 1024**3
# Execution globals: minimum free-memory thresholds activated from the authoritative
# experiment JSON or DuckDB; zero is only the pre-activation bootstrap value.
MAC_MEMORY_HEADROOM_BYTES = 0
UBUNTU_MEMORY_HEADROOM_BYTES = 0
GPU_MEMORY_HEADROOM_BYTES = 0
# Execution globals: per-worker Dask limits activated from JSON or DuckDB authority;
# zero is only the pre-activation bootstrap value.
MAC_CPU_MEMORY_GIB = 0
UBUNTU_CPU_MEMORY_GIB = 0
UBUNTU_GPU_MEMORY_GIB = 0
# Execution global: unsafe-sample limit activated from JSON or DuckDB authority;
# zero is only the pre-activation bootstrap value.
PERSISTENT_UNSAFE_SAMPLES = 0


def _activate_configuration(configuration: ExperimentConfiguration) -> None:
    """Activate one authoritative acceptance contract.

    Purpose: Resolve all workload, topology, and safety expectations for this invocation.
    Inputs: A validated experiment configuration.
    Outputs: Mutates this module's active configuration and acceptance expectation globals.
    """
    global SERIES_LIMIT, EXPECTED_TASK_COUNTS, EXPECTED_FORECASTS
    global EXPECTED_EVALUATIONS, EXPECTED_CHRONOS_TASKS, MAC_CPU_WORKERS
    global UBUNTU_CPU_WORKERS, UBUNTU_GPU_WORKERS, PHYSICAL_GPU_COUNT
    global EXPECTED_WORKERS, MAX_IN_FLIGHT, MAC_CPU_MEMORY_GIB
    global UBUNTU_CPU_MEMORY_GIB, UBUNTU_GPU_MEMORY_GIB
    global MAC_SYSTEM_MEMORY_BYTES, UBUNTU_SYSTEM_MEMORY_BYTES
    global MAC_MEMORY_HEADROOM_BYTES, UBUNTU_MEMORY_HEADROOM_BYTES
    global GPU_MEMORY_HEADROOM_BYTES, PERSISTENT_UNSAFE_SAMPLES
    global _ACTIVE_CONFIGURATION
    _ACTIVE_CONFIGURATION = configuration
    resolved = configuration.resolved
    derived = resolved["derived"]
    acceptance = resolved["execution"]["final_acceptance"]
    workers = acceptance["workers"]
    memory = acceptance["worker_memory_gib"]
    safety = acceptance["resource_safety"]
    SERIES_LIMIT = configuration.series_count
    EXPECTED_TASK_COUNTS = {
        int(stage): int(count)
        for stage, count in derived["expected_task_counts"].items()
        if int(stage) >= 2
    }
    EXPECTED_FORECASTS = int(derived["expected_forecast_rows"])
    EXPECTED_EVALUATIONS = int(derived["expected_evaluation_rows"])
    EXPECTED_CHRONOS_TASKS = (
        SERIES_LIMIT * int(derived["variant_count"])
    )
    MAC_CPU_WORKERS = int(workers["mac_cpu"])
    UBUNTU_CPU_WORKERS = int(workers["ubuntu_cpu"])
    UBUNTU_GPU_WORKERS = int(workers["ubuntu_gpu"])
    PHYSICAL_GPU_COUNT = int(acceptance["physical_gpu_count"])
    EXPECTED_WORKERS = int(workers["total"])
    MAX_IN_FLIGHT = int(resolved["execution"]["default"]["dask_max_in_flight"])
    MAC_CPU_MEMORY_GIB = int(memory["mac_cpu"])
    UBUNTU_CPU_MEMORY_GIB = 0
    UBUNTU_GPU_MEMORY_GIB = int(memory["ubuntu_gpu"])
    MAC_SYSTEM_MEMORY_BYTES = int(safety["mac_system_memory_bytes"])
    UBUNTU_SYSTEM_MEMORY_BYTES = int(safety["ubuntu_system_memory_bytes"])
    MAC_MEMORY_HEADROOM_BYTES = int(safety["mac_min_available_gib"]) * GIB
    UBUNTU_MEMORY_HEADROOM_BYTES = int(safety["ubuntu_min_available_gib"]) * GIB
    GPU_MEMORY_HEADROOM_BYTES = int(safety["gpu_min_available_gib"]) * GIB
    PERSISTENT_UNSAFE_SAMPLES = int(safety["persistent_unsafe_samples"])


def _utc_now() -> str:
    """Return the current UTC timestamp in ISO 8601 form for report provenance."""
    return datetime.now(UTC).isoformat()


def _mark_process(
    connection: duckdb.DuckDBPyConnection,
    process_id: int,
    status: str,
    summary: dict[str, Any] | None = None,
    error: str | None = None,
) -> None:
    """Persist one acceptance process transition.

    Purpose: Keep Process 01–06 lifecycle state synchronized with orchestration.
    Inputs: Open DuckDB connection, process ID, status, and optional summary/error.
    Outputs: Updates the matching ``experiment_processes`` row in the caller's transaction.
    """
    connection.execute(
        """UPDATE experiment_processes SET status=?,
           started_at=CASE WHEN ?='running' THEN current_timestamp ELSE started_at END,
           completed_at=CASE WHEN ?='completed' THEN current_timestamp ELSE NULL END,
           updated_at=current_timestamp, summary=?, last_error=?
           WHERE process_id=?""",
        [
            status,
            status,
            status,
            canonical_json(summary) if summary is not None else None,
            error,
            process_id,
        ],
    )


def _memory_budget() -> dict[str, Any]:
    """Derive acceptance host-memory evidence.

    Purpose: Compare configured worker ceilings with required host headroom.
    Inputs: Activated topology, system-memory, worker-limit, and threshold globals.
    Outputs: Returns Mac/Ubuntu budget details and rationale; mutates no state.
    """
    mac_total = MAC_SYSTEM_MEMORY_BYTES
    ubuntu_total = UBUNTU_SYSTEM_MEMORY_BYTES
    mac_workers = MAC_CPU_WORKERS * MAC_CPU_MEMORY_GIB * GIB
    ubuntu_workers = (
        UBUNTU_CPU_WORKERS * UBUNTU_CPU_MEMORY_GIB
        + UBUNTU_GPU_WORKERS * UBUNTU_GPU_MEMORY_GIB
    ) * GIB
    return {
        "mac": {
            "system_memory_bytes": mac_total,
            "configured_worker_memory_ceiling_bytes": mac_workers,
            "memory_outside_worker_ceilings_bytes": mac_total - mac_workers,
            "required_headroom_bytes": MAC_MEMORY_HEADROOM_BYTES,
            "worker_layout": f"{MAC_CPU_WORKERS} CPU x {MAC_CPU_MEMORY_GIB} GiB",
        },
        "ubuntu": {
            "system_memory_bytes": ubuntu_total,
            "configured_worker_memory_ceiling_bytes": ubuntu_workers,
            "memory_outside_worker_ceilings_bytes": ubuntu_total - ubuntu_workers,
            "required_headroom_bytes": UBUNTU_MEMORY_HEADROOM_BYTES,
            "worker_layout": (
                f"{UBUNTU_CPU_WORKERS} CPU x {UBUNTU_CPU_MEMORY_GIB} GiB + "
                f"{UBUNTU_GPU_WORKERS} logical GPU x {UBUNTU_GPU_MEMORY_GIB} GiB"
            ),
        },
        "rationale": (
            "Worker ceilings and topology are resolved from the stored experiment "
            "configuration; host headroom remains enforced by resource telemetry."
        ),
    }


def _resolve_acceptance_profile():
    """Resolve the Dask profile used by acceptance.

    Purpose: Combine stored execution settings with the required two-host topology.
    Inputs: The active module-level experiment configuration and resolved globals.
    Outputs: Returns an execution profile and recorded overrides; mutates no state.
    """
    if _ACTIVE_CONFIGURATION is None:
        raise RuntimeError("acceptance configuration has not been activated")
    values = _ACTIVE_CONFIGURATION.resolved["execution"]["default"]
    process_workers = values["process_workers"]
    overrides = {
        "dask_mac_cpu_workers": MAC_CPU_WORKERS,
        "dask_ubuntu_cpu_workers": UBUNTU_CPU_WORKERS,
        "dask_max_in_flight": MAX_IN_FLIGHT,
    }
    return (
        ExecutionProfile(
            name="stored_final_acceptance",
            required_accelerator=None,
            expected_accelerator_name=None,
            cleaning_workers=int(process_workers["2"]),
            transformation_workers=int(process_workers["3"]),
            autoarima_workers=int(process_workers["4"]),
            chronos_processes=1,
            chronos_inference_batch_size=int(values["batch_sizes"]["chronos"]),
            combination_workers=int(process_workers["5"]),
            evaluation_workers=int(process_workers["6"]),
            cpu_gpu_overlap=bool(values["cpu_gpu_overlap"]),
            system_memory_min_available_gib=float(values["system_memory_min_available_gib"]),
            accelerator_memory_min_available_gib=float(values["accelerator_memory_min_available_gib"]),
            database_writers=int(values["database_writers"]),
            **overrides,
        ),
        overrides,
    )


def _run(
    arguments: list[str],
    *,
    root: Path,
    timeout: float = 30,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Execute a bounded repository command.

    Purpose: Standardize environment, working directory, capture, and timeout behavior.
    Inputs: Argument vector, repository root, timeout, and check policy.
    Outputs: Returns captured stdout/stderr; starts a subprocess and may raise on failure.
    """
    return subprocess.run(
        arguments,
        cwd=root,
        env={**os.environ, "RENV_CONFIG_SYNCHRONIZED_CHECK": "false"},
        check=check,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _python_identity_script(packages: tuple[str, ...]) -> str:
    """Build a Python snippet that prints its interpreter and package versions."""
    return (
        "import importlib.metadata as m,platform;"
        f"print('|'.join([platform.python_version(), *[m.version(n) for n in {packages!r}]]))"
    )


def _python_environment_identity(
    executable: Path, packages: tuple[str, ...], root: Path
) -> str:
    """Read a locked Python environment identity.

    Purpose: Compare interpreter and package versions across acceptance hosts.
    Inputs: Python executable, package names, and repository root.
    Outputs: Returns a pipe-delimited identity; runs a captured Python subprocess.
    """
    return _run(
        [str(executable), "-c", _python_identity_script(packages)], root=root
    ).stdout.strip()


def _local_environment_identity(
    root: Path, configuration: ExperimentConfiguration
) -> dict[str, str]:
    """Verify all local locked runtime identities.

    Purpose: Reject drift in project, GiftEval, Chronos, or R environments.
    Inputs: Repository root and authoritative configuration paths.
    Outputs: Returns version identities; runs Python/R subprocesses and raises on mismatch.
    """
    paths = configuration.resolved["execution"]["paths"]
    gift_environment = configuration.resolved["evaluation"]["gift_eval"]["environment"]
    r = _run(
        [
            "Rscript",
            "-e",
            'cat(paste(R.version$major, R.version$minor, packageVersion("forecast"), '
            'packageVersion("jsonlite"), sep="|"))',
        ],
        root=root,
    ).stdout.strip()
    identity = {
        "project": _python_environment_identity(
            root / paths["project_environment"] / "bin/python",
            ("dask", "distributed", "duckdb", "pyarrow"),
            root,
        ),
        "gift_eval": _python_environment_identity(
            root / gift_environment / "bin/python",
            ("gluonts", "datasets", "pyarrow"),
            root,
        ),
        "chronos": _python_environment_identity(
            root / paths["chronos_environment"] / "bin/python",
            ("chronos-forecasting", "torch"),
            root,
        ),
        "r": r,
    }
    expected_prefixes = {
        "project": "3.12.14|2026.8.0|2026.8.0|",
        "gift_eval": "3.12.14|0.15.1|2.17.1|",
        "chronos": "3.12.14|2.2.2|",
        "r": "4|6.1|8.24.0|2.0.0",
    }
    mismatches = [
        name
        for name, prefix in expected_prefixes.items()
        if not identity[name].startswith(prefix)
    ]
    if mismatches:
        raise RuntimeError(
            "local locked environment does not match accepted versions: "
            + ", ".join(mismatches)
        )
    return identity


def _sha256(value: Any) -> str:
    """Hash a value's canonical JSON representation for evidence comparison."""
    encoded = json.dumps(
        value, ensure_ascii=True, separators=(",", ":"), sort_keys=True, default=str
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _require_existing_environment(
    root: Path, configuration: ExperimentConfiguration
) -> dict[str, str]:
    """Validate pre-existing acceptance prerequisites.

    Purpose: Ensure tools, locked interpreters, and source data exist without installation.
    Inputs: Repository root and authoritative configuration.
    Outputs: Returns resolved prerequisite paths; reads filesystem state only.
    """
    paths_config = configuration.resolved["execution"]["paths"]
    evaluation = configuration.resolved["evaluation"]["gift_eval"]
    data = configuration.resolved["data"]
    paths = {
        "uv": root / ".tools/uv/uv",
        "project_python": root / paths_config["project_environment"] / "bin/python",
        "gift_eval_python": root / evaluation["environment"] / "bin/python",
        "chronos_python": root / paths_config["chronos_environment"] / "bin/python",
        "gift_eval_source": root / data["source"]["directory"] / data["dataset_name"],
    }
    missing = [name for name, path in paths.items() if not path.exists()]
    if missing:
        raise RuntimeError(
            "required locked environment is unavailable; no installation was attempted: "
            + ", ".join(missing)
        )
    return {name: str(path.resolve()) for name, path in paths.items()}


def _host_role(hostname: str, topology: dict[str, Any]) -> str | None:
    """Classify a topology hostname as the Mac or Ubuntu host."""
    if hostname in topology["topology"]["mac_hosts"]:
        return "mac"
    if hostname in topology["topology"]["ubuntu_hosts"]:
        return "ubuntu"
    return None


def _summarize_resources(
    snapshots: list[dict[str, Any]],
    errors: list[str],
    topology: dict[str, Any],
) -> dict[str, Any]:
    """Reduce telemetry into acceptance safety evidence.

    Purpose: Detect worker churn and memory, swap, spill, or GPU safety violations.
    Inputs: Resource snapshots, sampler errors, and expected topology.
    Outputs: Returns observations, reasons, thresholds, and pass status; mutates no inputs.
    """
    expected_workers = set(topology["workers"])
    worker_sets = [set(sample["workers"]) for sample in snapshots]
    all_workers = set().union(*worker_sets) if worker_sets else set()
    continuously_present = set.intersection(*worker_sets) if worker_sets else set()
    host_observations: dict[str, dict[str, Any]] = {}
    valid_samples = 0
    for sample in snapshots:
        if set(sample["workers"]) == expected_workers:
            valid_samples += 1
        per_host: dict[str, list[dict[str, Any]]] = {}
        for worker in sample["workers"].values():
            per_host.setdefault(worker["hostname"], []).append(worker)
        for hostname, workers in per_host.items():
            observation = host_observations.setdefault(
                hostname,
                {
                    "role": _host_role(hostname, topology),
                    "sample_count": 0,
                    "minimum_available_memory_bytes": None,
                    "initial_swap_used_bytes": None,
                    "maximum_swap_used_bytes": 0,
                    "maximum_dask_spill_bytes": 0,
                    "minimum_gpu_available_memory_bytes": None,
                },
            )
            available = min(item["system_available_memory_bytes"] for item in workers)
            swap = max(item["swap_used_bytes"] for item in workers)
            spill = sum(
                item["dask_spilled_memory_bytes"] + item["dask_spilled_disk_bytes"]
                for item in workers
            )
            gpu_available = [
                item["gpu"]["available_memory_bytes"]
                for item in workers
                if "gpu" in item
            ]
            observation["sample_count"] += 1
            current_minimum = observation["minimum_available_memory_bytes"]
            observation["minimum_available_memory_bytes"] = (
                available if current_minimum is None else min(current_minimum, available)
            )
            if observation["initial_swap_used_bytes"] is None:
                observation["initial_swap_used_bytes"] = swap
            observation["maximum_swap_used_bytes"] = max(
                observation["maximum_swap_used_bytes"], swap
            )
            observation["maximum_dask_spill_bytes"] = max(
                observation["maximum_dask_spill_bytes"], spill
            )
            if gpu_available:
                available_gpu = min(gpu_available)
                current_gpu = observation["minimum_gpu_available_memory_bytes"]
                observation["minimum_gpu_available_memory_bytes"] = (
                    available_gpu if current_gpu is None else min(current_gpu, available_gpu)
                )

    unsafe_reasons = []
    missing_or_replaced = max(
        len(all_workers - expected_workers),
        len(expected_workers - continuously_present),
    )
    if valid_samples == 0:
        unsafe_reasons.append("no complete resource sample")
    if errors:
        unsafe_reasons.append("resource sampler errors occurred")
    if missing_or_replaced:
        unsafe_reasons.append("a worker was replaced, removed, or unexpectedly added")
    for hostname, observation in host_observations.items():
        role = observation["role"]
        threshold = (
            MAC_MEMORY_HEADROOM_BYTES if role == "mac" else UBUNTU_MEMORY_HEADROOM_BYTES
        )
        if role is None:
            unsafe_reasons.append(f"unknown sampled host {hostname}")
        elif observation["minimum_available_memory_bytes"] < threshold:
            unsafe_reasons.append(f"{role} memory headroom fell below threshold")
        observation["swap_growth_bytes"] = (
            observation["maximum_swap_used_bytes"]
            - observation["initial_swap_used_bytes"]
        )
        if observation["swap_growth_bytes"] > 0:
            unsafe_reasons.append(f"swap grew on {hostname}")
        if observation["maximum_dask_spill_bytes"] > 0:
            unsafe_reasons.append(f"Dask spilled on {hostname}")
        gpu_available = observation["minimum_gpu_available_memory_bytes"]
        if gpu_available is not None and gpu_available < GPU_MEMORY_HEADROOM_BYTES:
            unsafe_reasons.append("GPU memory headroom fell below threshold")
    expected_hosts = set(topology["topology"]["mac_hosts"]) | set(
        topology["topology"]["ubuntu_hosts"]
    )
    for hostname in sorted(expected_hosts - set(host_observations)):
        unsafe_reasons.append(f"no resource observation for {hostname}")
    if not any(
        observation["minimum_gpu_available_memory_bytes"] is not None
        for observation in host_observations.values()
    ):
        unsafe_reasons.append("no GPU memory observation")
    memory_limits = snapshots[0]["scheduler_workers"] if snapshots else {}
    for address, worker in topology["workers"].items():
        expected_limit = (
            UBUNTU_GPU_MEMORY_GIB * GIB
            if worker["resources"].get(CHRONOS_GPU_RESOURCE, 0) == 1
            else MAC_CPU_MEMORY_GIB * GIB
            if worker["hostname"] in topology["topology"]["mac_hosts"]
            else UBUNTU_CPU_MEMORY_GIB * GIB
        )
        actual_limit = memory_limits.get(address, {}).get("memory_limit")
        if actual_limit != expected_limit:
            unsafe_reasons.append(
                f"worker {address} memory limit {actual_limit} != {expected_limit}"
            )
    return {
        "sample_count": len(snapshots),
        "valid_sample_count": valid_samples,
        "sampling_errors": errors,
        "host_observations": host_observations,
        "worker_replacements_or_removals": missing_or_replaced,
        "worker_memory_limits": memory_limits,
        "thresholds": {
            "mac_available_memory_bytes": MAC_MEMORY_HEADROOM_BYTES,
            "ubuntu_available_memory_bytes": UBUNTU_MEMORY_HEADROOM_BYTES,
            "gpu_available_memory_bytes": GPU_MEMORY_HEADROOM_BYTES,
        },
        "unsafe_reasons": sorted(set(unsafe_reasons)),
        "passed": not unsafe_reasons,
    }


class _ResourceSampler:
    """Own acceptance telemetry sampling and safety-triggered cluster shutdown.

    Purpose: Sample scheduler/worker resources in a background thread and stop
    the cluster after persistent unsafe conditions.
    Inputs: Scheduler address, expected topology, and an idempotent stop callback.
    Outputs: Owned snapshots, errors, stop reason, and a summarized evidence mapping;
    the callback may terminate cluster subprocesses.
    """
    def __init__(
        self,
        address: str,
        topology: dict[str, Any],
        stop_cluster: Callable[[], None],
    ):
        """Prepare owned telemetry state and its background thread.

        Purpose: Configure sampling and controlled shutdown without starting either.
        Inputs: Scheduler address, expected topology, and idempotent stop callback.
        Outputs: Initializes snapshot/error/events/thread fields; performs no I/O yet.
        """
        self.address = address
        self.topology = topology
        self.stop_cluster = stop_cluster
        self.snapshots: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self.unsafe_stop_reason: str | None = None
        self._stop = threading.Event()
        self._first_sample = threading.Event()
        self._thread = threading.Thread(target=self._sample, daemon=True)

    def _sample(self) -> None:
        """Run the sampler thread.

        Purpose: Poll resources and trigger shutdown after persistent unsafe samples.
        Inputs: Instance scheduler address, topology, events, and stop callback.
        Outputs: Mutates snapshots/errors/reason and may close client and cluster processes.
        """
        from distributed import Client

        client = None
        consecutive_unsafe = 0
        initial_swap_by_host: dict[str, int] = {}
        try:
            client = Client(self.address, timeout="30s")
            while not self._stop.is_set():
                try:
                    info = client.scheduler_info()
                    self.snapshots.append(
                        {
                            "at": time.time(),
                            "workers": client.run(worker_resource_snapshot),
                            "scheduler_workers": {
                                address: {
                                    "name": worker.get("name"),
                                    "host": worker.get("host"),
                                    "memory_limit": worker.get("memory_limit"),
                                }
                                for address, worker in info["workers"].items()
                            },
                        }
                    )
                    self._first_sample.set()
                    current = _summarize_resources(
                        [self.snapshots[-1]], [], self.topology
                    )
                    swap_growth = False
                    for hostname, observation in current["host_observations"].items():
                        initial_swap = initial_swap_by_host.setdefault(
                            hostname, observation["maximum_swap_used_bytes"]
                        )
                        swap_growth = swap_growth or (
                            observation["maximum_swap_used_bytes"] > initial_swap
                        )
                    consecutive_unsafe = (
                        consecutive_unsafe + 1
                        if not current["passed"] or swap_growth
                        else 0
                    )
                except BaseException as exc:
                    self.errors.append(f"{type(exc).__name__}: {exc}")
                    self._first_sample.set()
                    consecutive_unsafe += 1
                if consecutive_unsafe >= PERSISTENT_UNSAFE_SAMPLES:
                    self.unsafe_stop_reason = (
                        "unsafe resource conditions persisted for "
                        f"{PERSISTENT_UNSAFE_SAMPLES} samples"
                    )
                    self._stop.set()
                    self.stop_cluster()
                    return
                self._stop.wait(5)
        except BaseException as exc:
            self.errors.append(f"{type(exc).__name__}: {exc}")
            self.unsafe_stop_reason = "resource sampler could not connect"
            self._first_sample.set()
            self._stop.set()
            self.stop_cluster()
        finally:
            if client is not None:
                client.close(timeout=5)

    def __enter__(self) -> "_ResourceSampler":
        """Start background sampling.

        Purpose: Establish telemetry before scientific work proceeds.
        Inputs: The prepared sampler thread.
        Outputs: Starts the thread, may append a timeout error, and returns ``self``.
        """
        self._thread.start()
        if not self._first_sample.wait(35):
            self.errors.append("resource sampler produced no result within 35 seconds")
        return self

    def __exit__(self, *_: object) -> None:
        """Stop background sampling.

        Purpose: Bound telemetry lifetime to the managed acceptance phase.
        Inputs: Context-manager exit values, which are ignored.
        Outputs: Sets the stop event and joins the owned thread for up to 60 seconds.
        """
        self._stop.set()
        self._thread.join(timeout=60)

    def summary(self) -> dict[str, Any]:
        """Build final resource evidence.

        Purpose: Combine captured samples/errors with any controlled-stop reason.
        Inputs: Sampler-owned telemetry and expected topology.
        Outputs: Returns a new summary mapping; performs no external side effects.
        """
        summary = _summarize_resources(self.snapshots, self.errors, self.topology)
        summary["controlled_stop_reason"] = self.unsafe_stop_reason
        return summary


class _TwoMachineCluster:
    """Own processes for the fixed Mac/Ubuntu acceptance cluster.

    Purpose: Validate environments and start, inspect, and stop the scheduler and
    workers required by the acceptance topology.
    Inputs: Repository root, authoritative configuration, and documented SHAPEFM_*
    machine-environment overrides.
    Outputs: Cluster topology and process evidence; the instance owns local and
    remote subprocess lifecycle side effects.
    """

    def __init__(self, root: Path, configuration: ExperimentConfiguration):
        """Configure owned cluster state without starting processes.

        Purpose: Resolve commands, hosts, addresses, and runtime paths for acceptance.
        Inputs: Repository root, configuration, and optional SHAPEFM_* environment values.
        Outputs: Initializes topology/process fields and may probe the Mac bind address.
        """
        self.root = root
        self.configuration = configuration
        self.configuration_hash = configuration.scientific_hash
        # Machine environment: SHAPEFM_* values override source fallbacks for hosts,
        # installation, and Dask addresses; effective values are retained in evidence.
        self.ubuntu_host = os.environ.get(
            "SHAPEFM_UBUNTU_HOST", "rafmontano@WSUbuntu1.local"
        )
        self.ubuntu_root = os.environ.get(
            "SHAPEFM_UBUNTU_ROOT",
            "/home/rafmontano/Documents/PhD/2026/projects/shape_fm_poc",
        )
        self.mac_host = os.environ.get("SHAPEFM_MAC_HOST", "RMMacbookPro.local")
        self.bind_host = os.environ.get("SHAPEFM_MAC_BIND_HOST") or _run(
            ["ipconfig", "getifaddr", "en0"], root=root, check=False
        ).stdout.strip()
        if not self.bind_host:
            raise RuntimeError("set SHAPEFM_MAC_BIND_HOST to the Mac LAN IPv4 address")
        self.scheduler_address = os.environ.get(
            "SHAPEFM_DASK_ADDRESS", "tcp://127.0.0.1:8786"
        )
        self.worker_address = os.environ.get(
            "SHAPEFM_DASK_WORKER_ADDRESS", f"tcp://{self.mac_host}:8786"
        )
        self.runtime = root / "data/dask"
        self.processes: list[subprocess.Popen[Any]] = []

    def _ssh(self, command: str, timeout: float = 30) -> str:
        """Execute a bounded remote command.

        Purpose: Standardize noninteractive Ubuntu operations.
        Inputs: Shell command and timeout seconds.
        Outputs: Returns stripped stdout; starts SSH and may change remote state.
        """
        return _run(
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=10",
                self.ubuntu_host,
                command,
            ],
            root=self.root,
            timeout=timeout,
        ).stdout.strip()

    def preflight(self) -> dict[str, Any]:
        """Validate both hosts before process startup.

        Purpose: Require matching clean revisions, free ports, locked runtimes, and GPU.
        Inputs: Configured local/remote paths, hosts, and acceptance contract.
        Outputs: Returns provenance/environment evidence; runs local and SSH probes only.
        """
        commit = _run(["git", "rev-parse", "HEAD"], root=self.root).stdout.strip()
        dirty = _run(
            ["git", "status", "--porcelain", "--untracked-files=all"], root=self.root
        ).stdout.strip()
        if dirty:
            raise RuntimeError("Mac worktree must be clean before two-machine acceptance")
        local_ports = _run(
            ["lsof", "-nP", "-iTCP:8786", "-iTCP:8787", "-sTCP:LISTEN"],
            root=self.root,
            check=False,
        )
        if local_ports.stdout.strip():
            raise RuntimeError(
                "Mac acceptance ports 8786/8787 are already owned:\n"
                + local_ports.stdout.strip()
            )
        stale_local = _run(
            [
                "pgrep",
                "-af",
                "[a]cceptance-mac-cpu|[d]ask scheduler.*--port 8786",
            ],
            root=self.root,
            check=False,
        )
        if stale_local.stdout.strip():
            raise RuntimeError(
                "stale Mac acceptance process found:\n" + stale_local.stdout.strip()
            )
        resolved = self.configuration.resolved
        acceptance = resolved["execution"]["final_acceptance"]
        paths = resolved["execution"]["paths"]
        evaluation = resolved["evaluation"]["gift_eval"]
        data = resolved["data"]
        local_revision = _run(
            ["git", "-C", evaluation["source_directory"], "rev-parse", "HEAD"],
            root=self.root,
        ).stdout.strip()
        _run(
            [
                "env",
                "PYTHONPATH=src/python",
                str(self.root / paths["project_environment"] / "bin/python"),
                "-c",
                "import util.experiment_execution, util.distributed_execution",
            ],
            root=self.root,
        )
        local_environment = _local_environment_identity(self.root, self.configuration)
        project_script = shlex.quote(
            _python_identity_script(("dask", "distributed", "duckdb", "pyarrow"))
        )
        gift_script = shlex.quote(
            _python_identity_script(("gluonts", "datasets", "pyarrow"))
        )
        chronos_script = shlex.quote(
            _python_identity_script(("chronos-forecasting", "torch"))
        )
        r_script = shlex.quote(
            'cat(paste(R.version$major, R.version$minor, packageVersion("forecast"), '
            'packageVersion("jsonlite"), sep="|"))'
        )
        remote = self._ssh(
            "set -eu; "
            f"cd {shlex.quote(self.ubuntu_root)}; "
            "test -z \"$(git status --porcelain --untracked-files=all)\"; "
            "test -x .tools/uv/uv; "
            f"test -x {shlex.quote(paths['project_environment'] + '/bin/python')}; "
            f"test -x {shlex.quote(evaluation['environment'] + '/bin/python')}; "
            f"test -x {shlex.quote(paths['chronos_environment'] + '/bin/python')}; "
            f"test -d {shlex.quote(data['source']['directory'] + '/' + data['dataset_name'])}; "
            "test ! -e data/dask/acceptance-ubuntu-cpu.pid; "
            "test ! -e data/dask/acceptance-ubuntu-gpu.pid; "
            "! pgrep -f '[a]cceptance-ubuntu-cpu' >/dev/null; "
            "! pgrep -f '[a]cceptance-ubuntu-gpu' >/dev/null; "
            "! ss -ltn | grep -Eq ':(8786|8787)[[:space:]]'; "
            "printf '%s\\n%s\\n%s\\n' \"$(git rev-parse HEAD)\" "
            f"\"$(git -C {shlex.quote(evaluation['source_directory'])} rev-parse HEAD)\" \"$(hostname)\"; "
            "nvidia-smi --query-gpu=name --format=csv,noheader; "
            f"PYTHONPATH=src/python {shlex.quote(paths['project_environment'] + '/bin/python')} -c "
            "'import util.experiment_execution, util.distributed_execution'; "
            f"{shlex.quote(paths['project_environment'] + '/bin/python')} -c {project_script}; "
            f"{shlex.quote(evaluation['environment'] + '/bin/python')} -c {gift_script}; "
            f"{shlex.quote(paths['chronos_environment'] + '/bin/python')} -c {chronos_script}; "
            f"RENV_CONFIG_SYNCHRONIZED_CHECK=false Rscript -e {r_script}"
        ).splitlines()
        if (
            remote[:2] != [commit, local_revision]
            or len(remote) != 8
            or remote[3] != acceptance["gpu_name"]
        ):
            raise RuntimeError(
                "Mac/Ubuntu revision or connectivity preflight failed: "
                f"expected {[commit, local_revision]!r}, found {remote!r}"
            )
        remote_environment = dict(
            zip(("project", "gift_eval", "chronos", "r"), remote[4:], strict=True)
        )
        if remote_environment != local_environment:
            raise RuntimeError(
                "Mac/Ubuntu locked environment mismatch: "
                f"Mac={local_environment!r}, Ubuntu={remote_environment!r}"
            )
        return {
            "repository_revision": commit,
            "gift_eval_revision": local_revision,
            "mac_hostname": socket.gethostname(),
            "ubuntu_hostname": remote[2],
            "physical_gpu_count": PHYSICAL_GPU_COUNT,
            "physical_gpu_name": remote[3],
            "ports_available": [8786, 8787],
            "stale_processes_found": False,
            "environments_present": True,
            "environment_identity": local_environment,
            "connectivity_verified": True,
        }

    def _start_local(self, arguments: list[str], log_name: str) -> None:
        """Start and retain one local cluster process.

        Purpose: Give the cluster explicit lifecycle ownership of local daemons.
        Inputs: Process argument vector and runtime log filename.
        Outputs: Appends stdout/stderr to a log and adds the subprocess to ``processes``.
        """
        log = (self.runtime / log_name).open("a", encoding="utf-8")
        environment = {
            **os.environ,
            "PYTHONPATH": str(self.root / "src/python"),
            "RENV_CONFIG_SYNCHRONIZED_CHECK": "false",
        }
        process = subprocess.Popen(
            arguments,
            cwd=self.root,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        log.close()
        self.processes.append(process)

    def start(self, preflight: dict[str, Any]) -> dict[str, Any]:
        """Create the fixed two-machine cluster.

        Purpose: Start scheduler/workers and prove their advertised topology.
        Inputs: Successful preflight evidence and configured runtime settings.
        Outputs: Starts local/remote subprocesses, writes logs/PIDs, and returns topology evidence.
        """
        commit = preflight["repository_revision"]
        self.runtime.mkdir(parents=True, exist_ok=True)
        thread_limit = str(
            self.configuration.execution["thread_limits"]["dask_worker"]
        )
        dask_timeout = float(self.configuration.execution["dask_timeout_seconds"])
        uv = str(self.root / ".tools/uv/uv")
        self._start_local(
            [
                uv,
                "run",
                "--locked",
                "--no-sync",
                "dask",
                "scheduler",
                "--host",
                "0.0.0.0",
                "--port",
                "8786",
                "--dashboard-address",
                "127.0.0.1:8787",
            ],
            "acceptance-scheduler.log",
        )
        time.sleep(2)
        self._start_local(
            [
                uv,
                "run",
                "--locked",
                "--no-sync",
                "dask",
                "worker",
                self.worker_address,
                "--nworkers",
                str(MAC_CPU_WORKERS),
                "--nthreads",
                thread_limit,
                "--name",
                "acceptance-mac-cpu",
                "--host",
                self.bind_host,
                "--resources",
                "CPU=1",
                "--memory-limit",
                f"{MAC_CPU_MEMORY_GIB}GiB",
                "--no-dashboard",
            ],
            "acceptance-mac-cpu.log",
        )
        remote_root = shlex.quote(self.ubuntu_root)
        worker_address = shlex.quote(self.worker_address)
        ubuntu_cpu = ""
        if UBUNTU_CPU_WORKERS:
            ubuntu_cpu = (
                "nohup env PYTHONPATH=src/python RENV_CONFIG_SYNCHRONIZED_CHECK=false "
                ".tools/uv/uv run --locked --no-sync "
                f"dask worker {worker_address} --nworkers {UBUNTU_CPU_WORKERS} --nthreads {thread_limit} "
                "--name acceptance-ubuntu-cpu --resources CPU=1 "
                f"--memory-limit {UBUNTU_CPU_MEMORY_GIB}GiB --no-dashboard "
                ">data/dask/acceptance-ubuntu-cpu.log 2>&1 </dev/null & "
                "echo $! >data/dask/acceptance-ubuntu-cpu.pid; "
            )
        self._ssh(
            "set -eu; "
            f"cd {remote_root}; mkdir -p data/dask; "
            + ubuntu_cpu
            + "nohup env CUDA_VISIBLE_DEVICES=0 PYTHONPATH=src/python "
            "RENV_CONFIG_SYNCHRONIZED_CHECK=false "
            ".tools/uv/uv run --locked --no-sync dask worker "
            f"{worker_address} --nworkers {UBUNTU_GPU_WORKERS} --nthreads {thread_limit} "
            "--name acceptance-ubuntu-gpu "
            f"--resources {CHRONOS_GPU_RESOURCE}=1 "
            f"--memory-limit {UBUNTU_GPU_MEMORY_GIB}GiB --no-dashboard "
            ">data/dask/acceptance-ubuntu-gpu.log 2>&1 </dev/null & "
            "echo $! >data/dask/acceptance-ubuntu-gpu.pid"
        )
        from distributed import Client

        from util.distributed_execution import validate_cluster

        client = Client(self.scheduler_address, timeout=f"{dask_timeout}s")
        try:
            reports = validate_cluster(
                client,
                expected_workers=EXPECTED_WORKERS,
                timeout=dask_timeout,
                expected_commit=commit,
                expected_configuration_hash=self.configuration_hash,
                expected_gift_eval_revision=_ACTIVE_CONFIGURATION.resolved["evaluation"]["gift_eval"]["code_revision"],
                expected_chronos_revision=_ACTIVE_CONFIGURATION.resolved["models"]["chronos_2"]["revision"],
                expected_chronos_version=_ACTIVE_CONFIGURATION.resolved["models"]["chronos_2"]["chronos_forecasting"],
                chronos_repository=_ACTIVE_CONFIGURATION.resolved["models"]["chronos_2"]["repository"],
                chronos_environment=_ACTIVE_CONFIGURATION.resolved["execution"]["paths"]["chronos_environment"],
                gift_eval_source_directory=_ACTIVE_CONFIGURATION.resolved["evaluation"]["gift_eval"]["source_directory"],
                require_gpu=True,
                expected_gpu_name=_ACTIVE_CONFIGURATION.resolved["execution"]["final_acceptance"]["gpu_name"],
                expected_gpu_workers=UBUNTU_GPU_WORKERS,
            )
            local_host = socket.gethostname()
            mac_cpu = sum(
                report["hostname"] == local_host
                and report["resources"].get("CPU", 0) == 1
                for report in reports.values()
            )
            ubuntu_cpu = sum(
                report["hostname"] != local_host
                and report["resources"].get("CPU", 0) == 1
                for report in reports.values()
            )
            ubuntu_gpu = sum(
                report["hostname"] != local_host
                and report["resources"].get(CHRONOS_GPU_RESOURCE, 0) == 1
                for report in reports.values()
            )
            mac_gpu = sum(
                report["hostname"] == local_host
                and report["resources"].get(CHRONOS_GPU_RESOURCE, 0) >= 1
                for report in reports.values()
            )
            topology = {
                "mac_cpu_workers": mac_cpu,
                "ubuntu_cpu_workers": ubuntu_cpu,
                "physical_gpu_count": PHYSICAL_GPU_COUNT,
                "logical_gpu_worker_processes": ubuntu_gpu,
                "ubuntu_gpu_workers": ubuntu_gpu,
                "mac_gpu_workers": mac_gpu,
                "total_workers": len(reports),
                "mac_hosts": sorted(
                    {
                        report["hostname"]
                        for report in reports.values()
                        if report["hostname"] == local_host
                    }
                ),
                "ubuntu_hosts": sorted(
                    {
                        report["hostname"]
                        for report in reports.values()
                        if report["hostname"] != local_host
                    }
                ),
                "gpu_worker_addresses": sorted(
                    address
                    for address, report in reports.items()
                    if report["resources"].get(CHRONOS_GPU_RESOURCE, 0) == 1
                ),
            }
            expected = {
                "mac_cpu_workers": MAC_CPU_WORKERS,
                "ubuntu_cpu_workers": UBUNTU_CPU_WORKERS,
                "physical_gpu_count": PHYSICAL_GPU_COUNT,
                "logical_gpu_worker_processes": UBUNTU_GPU_WORKERS,
                "ubuntu_gpu_workers": UBUNTU_GPU_WORKERS,
                "mac_gpu_workers": 0,
                "total_workers": EXPECTED_WORKERS,
            }
            if {key: topology[key] for key in expected} != expected:
                raise RuntimeError(f"invalid acceptance topology: {topology}; expected {expected}")
            return {"topology": topology, "workers": reports}
        finally:
            client.close(timeout=5)

    def stop(self) -> None:
        """Release every cluster process owned by this instance.

        Purpose: Make acceptance cleanup safe after success or partial startup failure.
        Inputs: Retained local processes and configured scheduler/Ubuntu host.
        Outputs: Closes scheduler workers and terminates local/remote subprocesses best-effort.
        """
        from distributed import Client

        try:
            client = Client(self.scheduler_address, timeout="5s")
            try:
                client.shutdown()
            finally:
                client.close(timeout=5)
        except BaseException:
            pass
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
        try:
            self._ssh(
                f"cd {shlex.quote(self.ubuntu_root)}; "
                "for file in data/dask/acceptance-ubuntu-cpu.pid data/dask/acceptance-ubuntu-gpu.pid; "
                "do if test -s \"$file\"; then kill \"$(cat \"$file\")\" 2>/dev/null || true; "
                "rm -f \"$file\"; fi; done",
                timeout=15,
            )
        except BaseException:
            pass


def _database_evidence(database: Path, experiment_id: str) -> dict[str, Any]:
    """Read complete scientific evidence from DuckDB.

    Purpose: Audit cardinalities, contributions, retries, results, and provenance.
    Inputs: Existing database path and experiment identifier.
    Outputs: Returns an evidence mapping; opens and closes a read-only DuckDB connection.
    """
    connection = duckdb.connect(str(database), read_only=True)
    try:
        series_count, instance_count = connection.execute(
            """SELECT count(DISTINCT i.series_id), count(DISTINCT i.forecast_instance_id)
               FROM forecast_instances i
               JOIN experiment_tasks t USING (forecast_instance_id)
               WHERE t.experiment_id=? AND t.stage=2""",
            [experiment_id],
        ).fetchone()
        tasks = {
            int(stage): {status: int(count) for status, count in rows}
            for stage, rows in (
                (
                    stage,
                    connection.execute(
                        """SELECT status, count(*) FROM experiment_tasks
                           WHERE experiment_id=? AND stage=? GROUP BY status""",
                        [experiment_id, stage],
                    ).fetchall(),
                )
                for stage in range(2, 7)
            )
        }
        forecast_rows = connection.execute(
            """SELECT forecast_id, content_hash FROM forecasts
               WHERE experiment_id=? ORDER BY forecast_id""",
            [experiment_id],
        ).fetchall()
        evaluation_rows = connection.execute(
            """SELECT evaluation_id, evaluation_input_count,
                      forecast_input_fingerprint, CAST(metrics AS VARCHAR)
               FROM official_evaluations WHERE experiment_id=? ORDER BY evaluation_id""",
            [experiment_id],
        ).fetchall()
        attempts = connection.execute(
            """SELECT t.stage, t.candidate, a.status,
                      CAST(a.resource_usage AS VARCHAR), a.error
               FROM experiment_task_attempts a
               JOIN experiment_tasks t USING (task_id)
               WHERE t.experiment_id=?""",
            [experiment_id],
        ).fetchall()
        hosts: collections.Counter[str] = collections.Counter()
        resource_contributions: collections.Counter[str] = collections.Counter()
        host_resource_contributions: collections.Counter[tuple[str, str]] = (
            collections.Counter()
        )
        chronos_contributions: collections.Counter[tuple[str, str, str]] = (
            collections.Counter()
        )
        failed_attempts = []
        retries = 0
        chronos_attempts = []
        for stage, candidate, status, encoded, error in attempts:
            resource = json.loads(encoded or "{}")
            if status == "completed":
                hosts[resource.get("hostname", "Mac coordinator/local")] += 1
                advertised = resource.get("resources", {})
                contribution = (
                    CHRONOS_GPU_RESOURCE
                    if advertised.get(CHRONOS_GPU_RESOURCE, 0) >= 1
                    else "CPU"
                    if advertised.get("CPU", 0) >= 1
                    else "coordinator"
                )
                resource_contributions[contribution] += 1
                host_resource_contributions[
                    (resource.get("hostname", "Mac coordinator/local"), contribution)
                ] += 1
                retries += int(resource.get("retry_count", 0))
            else:
                failed_attempts.append(
                    {"stage": int(stage), "candidate": candidate, "error": error}
                )
            if stage == 4 and candidate == "chronos_2" and status == "completed":
                chronos_attempts.append(resource)
                chronos_contributions[
                    (
                        resource.get("hostname", ""),
                        resource.get("dask_worker", ""),
                        CHRONOS_GPU_RESOURCE
                        if resource.get("resources", {}).get(CHRONOS_GPU_RESOURCE, 0)
                        == 1
                        else "other",
                    )
                ] += 1
        chronos_provenance = {
            _sha256(
                {
                    key: item.get(key)
                    for key in (
                        "hostname",
                        "dask_worker",
                        "dask_worker_name",
                        "resources",
                        "accelerator_backend",
                        "accelerator_device_name",
                        "torch_version",
                        "chronos_forecasting_version",
                        "cuda_version",
                    )
                }
            ): {
                key: item.get(key)
                for key in (
                    "hostname",
                    "dask_worker",
                    "dask_worker_name",
                    "resources",
                    "accelerator_backend",
                    "accelerator_device_name",
                    "torch_version",
                    "chronos_forecasting_version",
                    "cuda_version",
                )
            }
            for item in chronos_attempts
        }
        duplicate_counts = connection.execute(
            """SELECT
                 (SELECT count(*) FROM forecasts WHERE experiment_id=?) -
                 (SELECT count(DISTINCT (variant_id, forecast_instance_id, candidate))
                    FROM forecasts WHERE experiment_id=?),
                 (SELECT count(*) FROM official_evaluations WHERE experiment_id=?) -
                 (SELECT count(DISTINCT (variant_id, candidate, benchmark_configuration_id))
                    FROM official_evaluations WHERE experiment_id=?)""",
            [experiment_id] * 4,
        ).fetchone()
        return {
            "series_count": int(series_count),
            "forecast_instance_count": int(instance_count),
            "tasks": tasks,
            "forecast_count": len(forecast_rows),
            "evaluation_count": len(evaluation_rows),
            "evaluation_input_counts": sorted({int(row[1]) for row in evaluation_rows}),
            "forecast_fingerprint": _sha256(forecast_rows),
            "evaluation_fingerprint": _sha256(evaluation_rows),
            "task_contribution_by_host": dict(sorted(hosts.items())),
            "task_contribution_by_resource": dict(sorted(resource_contributions.items())),
            "task_contribution_by_host_and_resource": [
                {
                    "hostname": hostname,
                    "advertised_resource": resource,
                    "completed_tasks": count,
                }
                for (hostname, resource), count in sorted(
                    host_resource_contributions.items()
                )
            ],
            "failed_attempt_count": len(failed_attempts),
            "failed_attempts": failed_attempts,
            "worker_retry_count": retries,
            "chronos_attempt_count": len(chronos_attempts),
            "chronos_contribution": [
                {
                    "hostname": hostname,
                    "dask_worker": worker,
                    "advertised_resource": resource,
                    "completed_tasks": count,
                }
                for (hostname, worker, resource), count in sorted(
                    chronos_contributions.items()
                )
            ],
            "chronos_worker_provenance": list(chronos_provenance.values()),
            "chronos_all_gpu": bool(chronos_attempts)
            and all(
                item.get("resources", {}).get(CHRONOS_GPU_RESOURCE, 0) >= 1
                for item in chronos_attempts
            ),
            "duplicate_forecast_rows": int(duplicate_counts[0]),
            "duplicate_evaluation_rows": int(duplicate_counts[1]),
        }
    finally:
        connection.close()


def _contribution_checks(
    evidence: dict[str, Any], topology: dict[str, Any]
) -> dict[str, Any]:
    """Evaluate host and GPU task participation.

    Purpose: Prove both hosts contributed and every expected GPU worker ran Chronos work.
    Inputs: Database evidence and the applicable initial/current topology.
    Outputs: Returns counts, worker identities, and pass status; mutates no state.
    """
    mac_hosts = set(topology["topology"]["mac_hosts"])
    ubuntu_hosts = set(topology["topology"]["ubuntu_hosts"])
    gpu_workers = set(topology["topology"]["gpu_worker_addresses"])
    contributions = evidence["task_contribution_by_host_and_resource"]
    mac_cpu_tasks = sum(
        item["completed_tasks"]
        for item in contributions
        if item["hostname"] in mac_hosts and item["advertised_resource"] == "CPU"
    )
    ubuntu_cpu_tasks = sum(
        item["completed_tasks"]
        for item in contributions
        if item["hostname"] in ubuntu_hosts
        and item["advertised_resource"] == "CPU"
    )
    chronos = evidence["chronos_contribution"]
    chronos_tasks = sum(item["completed_tasks"] for item in chronos)
    contributing_gpu_workers = {
        item["dask_worker"] for item in chronos if item["completed_tasks"] > 0
    }
    chronos_only_on_gpu_workers = (
        len(gpu_workers) == UBUNTU_GPU_WORKERS
        and bool(chronos)
        and all(
            item["hostname"] in ubuntu_hosts
            and item["dask_worker"] in gpu_workers
            and item["advertised_resource"] == CHRONOS_GPU_RESOURCE
            for item in chronos
        )
    )
    all_gpu_workers_contributed = contributing_gpu_workers == gpu_workers
    return {
        "mac_cpu_completed_tasks": mac_cpu_tasks,
        "ubuntu_cpu_completed_tasks": ubuntu_cpu_tasks,
        "chronos_completed_tasks": chronos_tasks,
        "chronos_only_on_ubuntu_gpu_workers": chronos_only_on_gpu_workers,
        "contributing_logical_gpu_workers": len(contributing_gpu_workers),
        "all_logical_gpu_workers_contributed": all_gpu_workers_contributed,
        "passed": (
            mac_cpu_tasks >= 1
            and chronos_tasks == EXPECTED_CHRONOS_TASKS
            and chronos_only_on_gpu_workers
            and all_gpu_workers_contributed
        ),
    }


def _matching_report(
    report: dict[str, Any] | None, database: Path, repository_revision: str
) -> bool:
    """Test whether a report belongs to the resolved database and repository revision."""
    return bool(
        report
        and Path(report.get("database", "")).resolve() == database
        and report.get("repository_revision") == repository_revision
    )


def _failure_database_reusable(phase: str) -> bool:
    """Allow reuse only when failure occurred before scientific execution."""
    return phase in {"import", "plan", "cluster_start"}


def _contribution_topology(
    report: dict[str, Any], run_kind: str, current_topology: dict[str, Any]
) -> dict[str, Any]:
    """Use the initial topology when checking restart contribution evidence."""
    initial = report.get("initial_run") or {}
    if run_kind == "restart" and initial.get("topology"):
        return initial["topology"]
    return current_topology


def _report_document(
    previous: dict[str, Any] | None,
    *,
    database: Path,
    entry_invocation: dict[str, Any],
    metadata: dict[str, Any],
) -> dict[str, Any]:
    """Build an acceptance report envelope.

    Purpose: Refresh invocation metadata while retaining initial/restart and failure history.
    Inputs: Optional previous report, database, invocation, and preflight metadata.
    Outputs: Returns a new report mapping; performs no file or database writes.
    """
    report = dict(previous or {})
    report.update(
        {
            "entry_invocation": entry_invocation,
            "repository_revision": entry_invocation["repository_revision"],
            "database": str(database),
            **metadata,
        }
    )
    report.setdefault("initial_run", None)
    report.setdefault("restart_run", None)
    report.setdefault("failure_history", [])
    return report


def _record_success(
    report: dict[str, Any], run_kind: str, run: dict[str, Any]
) -> dict[str, Any]:
    """Derive report state after a successful run phase.

    Purpose: Preserve phase evidence and decide reuse/final acceptance status.
    Inputs: Existing report, ``initial`` or ``restart`` kind, and run evidence.
    Outputs: Returns an updated report mapping; mutates no files or DuckDB state.
    """
    updated = dict(report)
    recorded_run = dict(run)
    recorded_run["database_reusable"] = run_kind == "initial"
    updated[f"{run_kind}_run"] = recorded_run
    initial_complete = bool(
        updated.get("initial_run", {}).get("scientific_run_complete")
        if updated.get("initial_run")
        else False
    )
    restart_complete = bool(
        updated.get("restart_run", {}).get("restart_passed")
        if updated.get("restart_run")
        else False
    )
    updated["scientific_run_complete"] = initial_complete
    updated["acceptance_passed"] = initial_complete and restart_complete
    updated["database_reusable"] = initial_complete and not restart_complete
    updated["database_reuse_reason"] = (
        "required_restart_test"
        if updated["database_reusable"]
        else "acceptance_complete"
        if updated["acceptance_passed"]
        else "initial_scientific_run_incomplete"
    )
    updated["decision"] = (
        "pass"
        if updated["acceptance_passed"]
        else "restart_required"
        if initial_complete
        else "fail"
    )
    return updated


def _write_failure_report(
    report_path: Path,
    report: dict[str, Any],
    run_kind: str,
    *,
    phase: str,
    error: BaseException,
    started_at: str,
    runtime_seconds: float,
    diagnostics: dict[str, Any],
) -> dict[str, Any]:
    """Persist an auditable acceptance failure.

    Purpose: Preserve phase diagnostics and determine whether DuckDB may resume.
    Inputs: Report path/state, run kind, phase, exception, timing, and diagnostics.
    Outputs: Rewrites the JSON report and returns its mapping; does not mutate DuckDB.
    """
    database_reusable = _failure_database_reusable(phase)
    failure = {
        "status": "failed",
        "started_at": started_at,
        "ended_at": _utc_now(),
        "phase": phase,
        "runtime_seconds": runtime_seconds,
        "database_reusable": database_reusable,
        "error": {"type": type(error).__name__, "message": str(error)},
        **diagnostics,
    }
    updated = dict(report)
    updated[f"{run_kind}_run"] = failure
    history = list(updated.get("failure_history", []))
    history.append(
        {
            "at": failure["ended_at"],
            "run_kind": run_kind,
            "phase": phase,
            "database_reusable": database_reusable,
            "error": failure["error"],
            "diagnostics": diagnostics,
        }
    )
    updated["failure_history"] = history
    updated["acceptance_passed"] = False
    updated["database_reusable"] = database_reusable
    updated["database_reuse_reason"] = (
        "failure_before_scientific_execution"
        if database_reusable
        else "scientific_or_evidence_failure_requires_fresh_database"
    )
    updated["decision"] = "fail"
    report_path.write_text(
        json.dumps(updated, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    return updated


def run_acceptance(
    root: Path,
    database: Path,
    report_path: Path,
    entry_invocation: dict[str, Any],
) -> dict[str, Any]:
    """Run or restart the fixed acceptance case and persist auditable evidence.

    Purpose: Execute both acceptance passes, verify restart and resource evidence,
    and reject use of the authoritative production database.
    Inputs: Repository root, isolated database/report paths, and CLI invocation
    metadata; configuration comes from JSON on creation and DuckDB on resume.
    Outputs: Return the report mapping and write the isolated DuckDB database and
    JSON report; start and stop local/remote cluster subprocesses as required.
    """
    database = database.resolve()
    report_path = report_path.resolve()
    authoritative = (root / "data/shapefm.duckdb").resolve()
    if database == authoritative:
        raise ValueError("acceptance test refuses to write to data/shapefm.duckdb")
    database_was_fresh = not database.exists()
    previous = None
    if report_path.exists():
        previous = json.loads(report_path.read_text(encoding="utf-8"))
    previous_matches_database = _matching_report(
        previous, database, entry_invocation["repository_revision"]
    )
    if previous is not None and not previous_matches_database:
        raise ValueError(
            "acceptance report belongs to another database or repository revision; "
            "use a fresh database and a fresh report path"
        )
    if not database_was_fresh and not previous_matches_database:
        raise ValueError(
            "acceptance database already exists without a matching report for this revision; "
            "use a fresh database and report"
        )
    if previous_matches_database and previous.get("database_reusable") is False:
        raise ValueError(
            "matching acceptance report marks this database as non-reusable; "
            "use a fresh database and report"
        )
    if database_was_fresh:
        configuration = initialize_experiment_database(
            database, root / "config/experiments/poc2_m4_daily_100.json"
        )
    else:
        configuration = load_database_configuration(database)
    _activate_configuration(configuration)
    environments = _require_existing_environment(root, configuration)
    cluster = _TwoMachineCluster(root, configuration)
    preflight = cluster.preflight()
    report = _report_document(
        previous if previous_matches_database else None,
        database=database,
        entry_invocation=entry_invocation,
        metadata={
            "series_limit_requested": SERIES_LIMIT,
            "source_revision": configuration.resolved["data"]["source"]["revision"],
            "gift_eval_revision": configuration.resolved["evaluation"]["gift_eval"]["code_revision"],
            "model_revision": configuration.resolved["models"]["chronos_2"]["revision"],
            "environments": environments,
            "preflight": preflight,
            "memory_budget": _memory_budget(),
        },
    )
    initial_run = report.get("initial_run") or {}
    run_kind = "restart" if initial_run.get("scientific_run_complete") else "initial"
    database.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    started_at = _utc_now()
    started = time.monotonic()
    phase = "import"
    database_work_started = False
    cluster_control_started = False
    topology: dict[str, Any] | None = None
    resources: dict[str, Any] | None = None
    imported: dict[str, Any] | None = None
    plan = None
    execution: list[dict[str, Any]] | None = None
    evidence: dict[str, Any] | None = None
    checks: dict[str, Any] | None = None
    sampler: _ResourceSampler | None = None
    try:
        database_work_started = True
        with ImportCoordinator(database) as coordinator:
            _mark_process(coordinator.connection, 1, "running")
            try:
                imported = coordinator.import_configured()
            except BaseException as exc:
                _mark_process(coordinator.connection, 1, "failed", error=str(exc))
                raise
            _mark_process(coordinator.connection, 1, "completed", imported)
        if (
            imported["selected_series"] != SERIES_LIMIT
            or imported["series_count"] != SERIES_LIMIT
        ):
            raise RuntimeError(f"import selected inconsistent series counts: {imported}")

        phase = "plan"
        with POC1Coordinator(database) as coordinator:
            plan = coordinator.plan()
            if (
                plan.instance_count != SERIES_LIMIT
                or plan.task_counts != EXPECTED_TASK_COUNTS
            ):
                raise RuntimeError(f"unexpected 100-series plan: {asdict(plan)}")
            imported_ids = {
                row[0]
                for row in coordinator.connection.execute(
                    "SELECT series_id FROM series WHERE dataset_id=?",
                    [imported["dataset_id"]],
                ).fetchall()
            }
            planned_ids = {
                row[0]
                for row in coordinator.connection.execute(
                    """SELECT DISTINCT i.series_id FROM forecast_instances i
                       JOIN experiment_tasks t USING (forecast_instance_id)
                       WHERE t.experiment_id=? AND t.stage=2""",
                    [plan.experiment_id],
                ).fetchall()
            }
            if imported_ids != planned_ids or len(planned_ids) != SERIES_LIMIT:
                raise RuntimeError("imported and officially planned series identifiers differ")

        phase = "cluster_start"
        cluster_control_started = True
        topology = cluster.start(preflight)
        profile, profile_overrides = _resolve_acceptance_profile()
        settings = ExecutionSettings(
            mode="dask",
            dask_scheduler_address=cluster.scheduler_address,
            dask_timeout_seconds=configuration.resolved["execution"]["default"]["dask_timeout_seconds"],
            dask_expected_workers=EXPECTED_WORKERS,
            dask_expected_gpu_workers=UBUNTU_GPU_WORKERS,
            dask_max_in_flight=profile.dask_max_in_flight or MAX_IN_FLIGHT,
            dask_retries=configuration.resolved["execution"]["default"]["dask_retries"],
        )
        phase = "scientific_execution"
        sampler = _ResourceSampler(cluster.scheduler_address, topology, cluster.stop)
        try:
            with sampler:
                with POC1Coordinator(database) as coordinator:
                    execution = []
                    for process_id in range(2, 7):
                        _mark_process(coordinator.connection, process_id, "running")
                        try:
                            result = coordinator.run_gate(
                                plan.experiment_id,
                                process_id,
                                execution=(profile, profile_overrides),
                                execution_settings=settings,
                            )
                        except BaseException as exc:
                            _mark_process(
                                coordinator.connection,
                                process_id,
                                "failed",
                                error=str(exc),
                            )
                            raise
                        _mark_process(
                            coordinator.connection,
                            process_id,
                            "completed",
                            result,
                        )
                        execution.append(result)
                    coordinator.connection.execute(
                        "UPDATE experiments SET status='completed', updated_at=current_timestamp WHERE experiment_id=?",
                        [plan.experiment_id],
                    )
        finally:
            resources = sampler.summary()
        phase = "resource_validation"
        if not resources["passed"]:
            raise RuntimeError(
                "unsafe or incomplete resource evidence: "
                + "; ".join(resources["unsafe_reasons"])
            )

        phase = "evidence_validation"
        evidence = _database_evidence(database, plan.experiment_id)
        initial = report.get("initial_run") or {}
        contribution_topology = _contribution_topology(report, run_kind, topology)
        contribution = _contribution_checks(evidence, contribution_topology)
        tasks_complete = all(
            evidence["tasks"].get(stage)
            == {"completed": EXPECTED_TASK_COUNTS[stage]}
            for stage in EXPECTED_TASK_COUNTS
        )
        scientific_counts_pass = (
            evidence["series_count"] == SERIES_LIMIT
            and evidence["forecast_instance_count"] == SERIES_LIMIT
            and evidence["forecast_count"] == EXPECTED_FORECASTS
            and evidence["evaluation_count"] == EXPECTED_EVALUATIONS
            and evidence["evaluation_input_counts"] == [SERIES_LIMIT]
        )
        restart_evidence = {
            "prior_successful_run_found": bool(
                run_kind == "restart" and initial.get("scientific_run_complete")
            ),
            "import_skipped_completed": imported["skipped_completed"],
            "selected_tasks": {
                str(item["stage"]): item["selected"] for item in execution
            },
            "fingerprints_preserved": bool(initial)
            and initial.get("evidence", {}).get("forecast_fingerprint")
            == evidence["forecast_fingerprint"]
            and initial.get("evidence", {}).get("evaluation_fingerprint")
            == evidence["evaluation_fingerprint"],
        }
        restart_pass = run_kind == "restart" and (
            restart_evidence["prior_successful_run_found"]
            and imported["skipped_completed"] == SERIES_LIMIT
            and set(restart_evidence["selected_tasks"].values()) == {0}
            and restart_evidence["fingerprints_preserved"]
        )
        scientific_run_complete = all(
            (
                tasks_complete,
                scientific_counts_pass,
                contribution["passed"],
                evidence["duplicate_forecast_rows"] == 0,
                evidence["duplicate_evaluation_rows"] == 0,
                evidence["failed_attempt_count"] == 0,
                evidence["worker_retry_count"] == 0,
                resources["passed"],
            )
        )
        checks = {
            "tasks_complete": tasks_complete,
            "scientific_counts_pass": scientific_counts_pass,
            "contribution": contribution,
            "resources_pass": resources["passed"],
            "no_duplicate_forecasts": evidence["duplicate_forecast_rows"] == 0,
            "no_duplicate_evaluations": evidence["duplicate_evaluation_rows"] == 0,
            "no_failed_attempts": evidence["failed_attempt_count"] == 0,
            "no_retries": evidence["worker_retry_count"] == 0,
        }
        if not scientific_run_complete:
            raise RuntimeError("one or more scientific acceptance gates failed")
        run_record = {
            "status": "completed",
            "started_at": started_at,
            "ended_at": _utc_now(),
            "database_was_fresh": database_was_fresh,
            "entry_invocation": entry_invocation,
            "import": imported,
            "plan": asdict(plan),
            "execution": execution,
            "execution_profile_overrides": profile_overrides,
            "topology": topology,
            "resources": resources,
            "runtime_seconds": time.monotonic() - started,
            "evidence": evidence,
            "checks": checks,
            "restart": restart_evidence,
            "scientific_run_complete": scientific_run_complete,
            "restart_passed": restart_pass,
        }
        report = _record_success(report, run_kind, run_record)
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        return report
    except BaseException as exc:
        if database_work_started:
            if sampler is not None and resources is None:
                resources = sampler.summary()
            evidence_collection_error = None
            if plan is not None and evidence is None:
                try:
                    evidence = _database_evidence(database, plan.experiment_id)
                except BaseException as evidence_error:
                    evidence_collection_error = {
                        "type": type(evidence_error).__name__,
                        "message": str(evidence_error),
                    }
            _write_failure_report(
                report_path,
                report,
                run_kind,
                phase=phase,
                error=exc,
                started_at=started_at,
                runtime_seconds=time.monotonic() - started,
                diagnostics={
                    "database_was_fresh": database_was_fresh,
                    "import": imported,
                    "plan": asdict(plan) if plan is not None else None,
                    "execution": execution,
                    "topology": topology,
                    "resources": resources,
                    "evidence": evidence,
                    "evidence_collection_error": evidence_collection_error,
                    "checks": checks,
                },
            )
        raise
    finally:
        if cluster_control_started:
            cluster.stop()
