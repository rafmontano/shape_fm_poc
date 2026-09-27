# ==============================================================================
# acceptance.py
#
# Purpose: Run the fixed two-machine acceptance workflow and record auditable evidence.
# Inputs: Repository, database, report, and invocation metadata; both hosts and their prepared locked environments.
# Outputs: Acceptance DuckDB state and a JSON report containing scientific, restart, topology, and resource evidence.
# Run from: Imported by `src/python/00_main.py test`; not run directly.
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

from util.configuration import load_config
from util.distributed_execution import (
    CHRONOS_GPU_RESOURCE,
    worker_resource_snapshot,
)
from util.execution_profiles import ExecutionSettings, resolve_execution_profile
from util.experiment_execution import POC1Coordinator, expected_task_counts
from util.import_execution import ImportCoordinator


# M4 Daily series included in the acceptance workload.
SERIES_LIMIT = 100
# Expected task totals by stage for that workload and its configured variants.
EXPECTED_TASK_COUNTS = expected_task_counts(SERIES_LIMIT)
# Forecast and evaluation row counts required for a successful run.
EXPECTED_FORECASTS = 1_200
EXPECTED_EVALUATIONS = 12
# Chronos tasks expected across 100 series and four preprocessing variants.
EXPECTED_CHRONOS_TASKS = 400
# Required logical workers by host and resource type.
MAC_CPU_WORKERS = 5
UBUNTU_CPU_WORKERS = 15
UBUNTU_GPU_WORKERS = 15
# All logical GPU workers share one physical RTX 5090.
PHYSICAL_GPU_COUNT = 1
# Total logical Dask workers required before execution starts.
EXPECTED_WORKERS = MAC_CPU_WORKERS + UBUNTU_CPU_WORKERS + UBUNTU_GPU_WORKERS
# Maximum submitted but unfinished Dask batches.
MAX_IN_FLIGHT = 32
# Bytes per gibibyte for worker limits and safety thresholds.
GIB = 1024**3
# Minimum free host and GPU memory accepted by telemetry checks.
MAC_MEMORY_HEADROOM_BYTES = 3 * GIB
UBUNTU_MEMORY_HEADROOM_BYTES = 16 * GIB
GPU_MEMORY_HEADROOM_BYTES = 4 * GIB
# Per-worker Dask memory limits in GiB.
MAC_CPU_MEMORY_GIB = 2
UBUNTU_CPU_MEMORY_GIB = 2
UBUNTU_GPU_MEMORY_GIB = 4
# Consecutive unsafe samples that trigger controlled cluster shutdown.
PERSISTENT_UNSAFE_SAMPLES = 3


def _utc_now() -> str:
    """Return the current UTC timestamp in ISO 8601 form for report provenance."""
    return datetime.now(UTC).isoformat()


def _memory_budget() -> dict[str, Any]:
    """Describe worker ceilings and required host-memory headroom."""
    mac_total = 16 * GIB
    ubuntu_total = 128_000_000_000
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
            "Configured Mac worker ceilings total 10 GiB, leaving 6 GiB outside "
            "worker ceilings on the 16 GiB Mac. Configured Ubuntu worker ceilings "
            "total 90 GiB (96.6 GB), leaving about 31.4 GB outside worker ceilings "
            "on the stated 128 GB system, above the 16 GiB threshold."
        ),
    }


def _resolve_acceptance_profile():
    """Resolve the two-machine profile with the acceptance worker and queue limits."""
    overrides = {
        "dask_mac_cpu_workers": MAC_CPU_WORKERS,
        "dask_ubuntu_cpu_workers": UBUNTU_CPU_WORKERS,
        "dask_max_in_flight": MAX_IN_FLIGHT,
    }
    return resolve_execution_profile("two_machine_dask", overrides)


def _run(
    arguments: list[str],
    *,
    root: Path,
    timeout: float = 30,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run a command from the repository root with captured text output."""
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
    """Read an interpreter's Python and requested package versions as a pipe-delimited identity."""
    return _run(
        [str(executable), "-c", _python_identity_script(packages)], root=root
    ).stdout.strip()


def _local_environment_identity(root: Path) -> dict[str, str]:
    """Verify locked local Python and R versions and return their identities."""
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
            root / ".venv/bin/python",
            ("dask", "distributed", "duckdb", "pyarrow"),
            root,
        ),
        "gift_eval": _python_environment_identity(
            root / "environments/gift-eval/.venv/bin/python",
            ("gluonts", "datasets", "pyarrow"),
            root,
        ),
        "chronos": _python_environment_identity(
            root / "environments/chronos-2/.venv/bin/python",
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


def _require_existing_environment(root: Path) -> dict[str, str]:
    """Require provisioned tools, environments, and source data without installing them."""
    paths = {
        "uv": root / ".tools/uv/uv",
        "project_python": root / ".venv/bin/python",
        "gift_eval_python": root / "environments/gift-eval/.venv/bin/python",
        "chronos_python": root / "environments/chronos-2/.venv/bin/python",
        "gift_eval_source": root / "data/source/gift_eval/m4_daily",
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
    """Aggregate telemetry and report worker, memory, swap, spill, and GPU violations."""
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
    """Sample Dask resources and stop the cluster after persistent unsafe conditions."""
    def __init__(
        self,
        address: str,
        topology: dict[str, Any],
        stop_cluster: Callable[[], None],
    ):
        """Prepare a sampler for `address` and its expected `topology`.

        Samples, errors, and a controlled-stop reason are accumulated on the
        instance. `stop_cluster` is invoked if connection or safety checks fail.
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
        """Poll scheduler and worker telemetry until stopped or safety shutdown."""
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
        """Start sampling and wait up to 35 seconds for the first result."""
        self._thread.start()
        if not self._first_sample.wait(35):
            self.errors.append("resource sampler produced no result within 35 seconds")
        return self

    def __exit__(self, *_: object) -> None:
        """Request sampler shutdown and wait for its background thread."""
        self._stop.set()
        self._thread.join(timeout=60)

    def summary(self) -> dict[str, Any]:
        """Summarize collected telemetry and include any controlled-stop reason."""
        summary = _summarize_resources(self.snapshots, self.errors, self.topology)
        summary["controlled_stop_reason"] = self.unsafe_stop_reason
        return summary


class _TwoMachineCluster:
    """Manage the acceptance scheduler and fixed Mac/Ubuntu worker topology."""

    def __init__(self, root: Path):
        """Configure cluster commands and runtime paths beneath repository `root`.

        Host names, roots, bind addresses, and scheduler addresses may come from
        SHAPEFM_* environment variables. Started local processes are retained for
        cleanup; no cluster process is started by construction.
        """
        self.root = root
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
        """Run a noninteractive command on the configured Ubuntu host."""
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
        """Verify clean matching hosts, free ports, locked environments, and the GPU."""
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
        local_revision = _run(
            ["git", "-C", "external/gift-eval", "rev-parse", "HEAD"], root=self.root
        ).stdout.strip()
        _run(
            [
                "env",
                "PYTHONPATH=src/python",
                str(self.root / ".venv/bin/python"),
                "-c",
                "import util.experiment_execution, util.distributed_execution",
            ],
            root=self.root,
        )
        local_environment = _local_environment_identity(self.root)
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
            "test -x .tools/uv/uv; test -x .venv/bin/python; "
            "test -x environments/gift-eval/.venv/bin/python; "
            "test -x environments/chronos-2/.venv/bin/python; "
            "test -d data/source/gift_eval/m4_daily; "
            "test ! -e data/dask/acceptance-ubuntu-cpu.pid; "
            "test ! -e data/dask/acceptance-ubuntu-gpu.pid; "
            "! pgrep -f '[a]cceptance-ubuntu-cpu' >/dev/null; "
            "! pgrep -f '[a]cceptance-ubuntu-gpu' >/dev/null; "
            "! ss -ltn | grep -Eq ':(8786|8787)[[:space:]]'; "
            "printf '%s\\n%s\\n%s\\n' \"$(git rev-parse HEAD)\" "
            "\"$(git -C external/gift-eval rev-parse HEAD)\" \"$(hostname)\"; "
            "nvidia-smi --query-gpu=name --format=csv,noheader; "
            "PYTHONPATH=src/python .venv/bin/python -c "
            "'import util.experiment_execution, util.distributed_execution'; "
            f".venv/bin/python -c {project_script}; "
            f"environments/gift-eval/.venv/bin/python -c {gift_script}; "
            f"environments/chronos-2/.venv/bin/python -c {chronos_script}; "
            f"RENV_CONFIG_SYNCHRONIZED_CHECK=false Rscript -e {r_script}"
        ).splitlines()
        if (
            remote[:2] != [commit, local_revision]
            or len(remote) != 8
            or remote[3] != "NVIDIA GeForce RTX 5090"
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
        """Start a detached local process, append its output to runtime logs, and retain it for cleanup."""
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
        """Start all scheduler and worker processes, validate topology, and return worker reports."""
        commit = preflight["repository_revision"]
        self.runtime.mkdir(parents=True, exist_ok=True)
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
                "1",
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
        self._ssh(
            "set -eu; "
            f"cd {remote_root}; mkdir -p data/dask; "
            "nohup env PYTHONPATH=src/python RENV_CONFIG_SYNCHRONIZED_CHECK=false "
            ".tools/uv/uv run --locked --no-sync "
            f"dask worker {worker_address} "
            f"--nworkers {UBUNTU_CPU_WORKERS} --nthreads 1 "
            "--name acceptance-ubuntu-cpu --resources CPU=1 "
            f"--memory-limit {UBUNTU_CPU_MEMORY_GIB}GiB --no-dashboard "
            ">data/dask/acceptance-ubuntu-cpu.log 2>&1 </dev/null & "
            "echo $! >data/dask/acceptance-ubuntu-cpu.pid; "
            "nohup env CUDA_VISIBLE_DEVICES=0 PYTHONPATH=src/python "
            "RENV_CONFIG_SYNCHRONIZED_CHECK=false "
            ".tools/uv/uv run --locked --no-sync dask worker "
            f"{worker_address} --nworkers {UBUNTU_GPU_WORKERS} --nthreads 1 "
            "--name acceptance-ubuntu-gpu "
            f"--resources {CHRONOS_GPU_RESOURCE}=1 "
            f"--memory-limit {UBUNTU_GPU_MEMORY_GIB}GiB --no-dashboard "
            ">data/dask/acceptance-ubuntu-gpu.log 2>&1 </dev/null & "
            "echo $! >data/dask/acceptance-ubuntu-gpu.pid"
        )
        from distributed import Client

        from util.configuration import json_fingerprint
        from util.distributed_execution import validate_cluster
        from util.experiment_execution import scientific_configuration

        client = Client(self.scheduler_address, timeout="180s")
        try:
            config = json.loads(
                (self.root / "config/experiments/m4_daily_reference.json").read_text(
                    encoding="utf-8"
                )
            )
            reports = validate_cluster(
                client,
                expected_workers=EXPECTED_WORKERS,
                timeout=180,
                expected_commit=commit,
                expected_configuration_hash=json_fingerprint(
                    scientific_configuration(config)
                ),
                require_gpu=True,
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
        """Best-effort shutdown of the scheduler and all owned local and remote workers."""
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
    """Collect task, result, contribution, retry, and provenance evidence for an experiment."""
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
    """Check CPU participation on both hosts and Chronos work on every GPU worker."""
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
            and ubuntu_cpu_tasks >= 1
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
    """Create or refresh the report envelope while preserving run history."""
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
    """Record a successful initial or restart run and derive the acceptance decision."""
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
    """Persist phase-specific failure diagnostics and database reuse status."""
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
    """Run or restart the 100-series acceptance case and persist its evidence report."""
    database = database.resolve()
    report_path = report_path.resolve()
    authoritative = (root / "data/shapefm.duckdb").resolve()
    if database == authoritative:
        raise ValueError("acceptance test refuses to write to data/shapefm.duckdb")
    environments = _require_existing_environment(root)
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
    cluster = _TwoMachineCluster(root)
    preflight = cluster.preflight()
    dependency = json.loads(
        (root / "config/dependencies/gift_eval.json").read_text(encoding="utf-8")
    )
    experiment_config = json.loads(
        (root / "config/experiments/m4_daily_reference.json").read_text(
            encoding="utf-8"
        )
    )
    report = _report_document(
        previous if previous_matches_database else None,
        database=database,
        entry_invocation=entry_invocation,
        metadata={
            "series_limit_requested": SERIES_LIMIT,
            "source_revision": dependency["dataset"]["revision"],
            "gift_eval_revision": experiment_config["benchmark"]["gift_eval_revision"],
            "model_revision": experiment_config["models"]["chronos_2"]["revision"],
            "environments": environments,
            "preflight": preflight,
            "memory_budget": _memory_budget(),
        },
    )
    initial_run = report.get("initial_run") or {}
    run_kind = "restart" if initial_run.get("scientific_run_complete") else "initial"
    database.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    import_config = load_config(root / "config/imports/m4_daily.json", SERIES_LIMIT)
    source = root / dependency["dataset"]["default_local_source_directory"] / "m4_daily"
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
            imported = coordinator.import_m4_daily(
                source,
                import_config,
                dependency["dataset"]["revision"],
                workers=1,
            )
        if (
            imported["selected_series"] != SERIES_LIMIT
            or imported["series_count"] != SERIES_LIMIT
        ):
            raise RuntimeError(f"import selected inconsistent series counts: {imported}")

        phase = "plan"
        with POC1Coordinator(database) as coordinator:
            plan = coordinator.plan("m4_daily", series_limit=SERIES_LIMIT)
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
            dask_timeout_seconds=180,
            dask_expected_workers=EXPECTED_WORKERS,
            dask_expected_gpu_workers=UBUNTU_GPU_WORKERS,
            dask_max_in_flight=profile.dask_max_in_flight or MAX_IN_FLIGHT,
            dask_retries=2,
        )
        phase = "scientific_execution"
        sampler = _ResourceSampler(cluster.scheduler_address, topology, cluster.stop)
        try:
            with sampler:
                with POC1Coordinator(database) as coordinator:
                    execution = coordinator.run_all(
                        plan,
                        execution=(profile, profile_overrides),
                        execution_settings=settings,
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
