# ==============================================================================
# distributed_cluster.py
#
# Purpose: Inspect and manage the approved ShapeFM two-machine Dask CPU cluster.
# Inputs: An execution profile, verified machine environment, and connected Dask client.
# Outputs: Managed scheduler/worker lifecycle plus cluster diagnostics; never DuckDB writes.
# Run from: Imported; not run directly.
# ==============================================================================

"""Inspect and manage the ShapeFM Dask cluster without opening DuckDB."""

from __future__ import annotations

import os
import shlex
import signal
import subprocess
import time
from pathlib import Path
from typing import Any

from distributed import Client

from .database import load_database_configuration
from .distributed_execution import (
    AUTOARIMA_R_RESOURCE,
    MAC_TUNING_R_RESOURCE,
    ROOT,
    TUNING_R_RESOURCE,
    UBUNTU_TUNING_R_RESOURCE,
    validate_cluster,
)
from .execution_profiles import GIB, ExecutionProfile, system_memory


def _configuration_hash(database: Path) -> str:
    """Purpose: Read the authoritative scientific identity from an experiment database.

    Inputs: Path to an existing, valid ShapeFM DuckDB database.
    Outputs: Validated scientific configuration SHA-256 digest.
    """
    return load_database_configuration(database).scientific_hash


def _commit() -> str:
    """Purpose: Capture the repository revision used by cluster workers.

    Inputs: The repository rooted at ``ROOT`` and an available Git subprocess.
    Outputs: Current ``HEAD`` commit text; subprocess failures propagate.
    """
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _worker_summary(client: Client) -> dict:
    """Purpose: Build deterministic scheduler and worker diagnostics.

    Inputs: A connected Dask ``Client`` whose scheduler exposes worker metadata.
    Outputs: Scheduler/dashboard addresses, worker count, and workers sorted by address
    with thread, resource, host, and memory-limit state.
    """
    info = client.scheduler_info()
    workers = []
    for address, worker in sorted(info["workers"].items()):
        workers.append(
            {
                "address": address,
                "name": worker.get("name"),
                "host": worker.get("host"),
                "nthreads": worker.get("nthreads"),
                "resources": worker.get("resources", {}),
                "memory_limit": worker.get("memory_limit"),
            }
        )
    return {
        "scheduler": info["address"],
        "dashboard": "http://127.0.0.1:8787/status",
        "worker_count": len(workers),
        "workers": workers,
    }


class ManagedTuningCluster:
    """Own approved CPU pools and the optional workload-required Ubuntu GPU pool.

    Machine addresses remain environment configuration. The profile owns worker
    counts, fit admission slots, and memory limits; this class owns only processes
    it starts and never transfers source, environments, databases, or results.
    """

    def __init__(self, profile: ExecutionProfile, requires_gpu: bool = False):
        """Resolve machine settings and workload topology before process startup."""
        required = (
            profile.dask_mac_cpu_workers,
            profile.dask_ubuntu_cpu_workers,
            profile.dask_mac_tuning_workers,
            profile.dask_ubuntu_tuning_workers,
            profile.dask_mac_worker_memory_gib,
            profile.dask_ubuntu_worker_memory_gib,
            profile.dask_mac_memory_min_available_gib,
            profile.dask_ubuntu_memory_min_available_gib,
            profile.dask_autoarima_max_in_flight,
            profile.dask_ets_max_in_flight,
            profile.dask_autoarima_fit_budget_gib,
            profile.dask_ets_fit_budget_gib,
            profile.dask_memory_admission_timeout_seconds,
            profile.dask_memory_poll_interval_seconds,
            profile.dask_memory_breach_grace_seconds,
            profile.dask_swap_growth_limit_gib,
        )
        if any(value is None for value in required):
            raise ValueError(f"profile {profile.name} does not define a managed tuning cluster")
        if (
            profile.dask_mac_tuning_workers != profile.dask_mac_cpu_workers
            or profile.dask_ubuntu_tuning_workers != profile.dask_ubuntu_cpu_workers
        ):
            raise ValueError("every managed CPU worker must be eligible for tuning work")
        self.profile = profile
        self.topology = profile.distributed_topology(requires_gpu)
        if requires_gpu and self.topology["ubuntu_gpu_workers"] < 1:
            raise ValueError(f"profile {profile.name} does not define GPU workers")
        if requires_gpu and profile.accelerator_memory_min_available_gib <= 0:
            raise ValueError("GPU workloads require a positive accelerator memory floor")
        self.ubuntu_host = os.environ.get(
            "SHAPEFM_UBUNTU_HOST", "rafmontano@WSUbuntu1.local"
        )
        self.ubuntu_root = os.environ.get(
            "SHAPEFM_UBUNTU_ROOT",
            "/home/rafmontano/Documents/PhD/2026/projects/shape_fm_poc",
        )
        self.host_key_alias = os.environ.get("SHAPEFM_SSH_HOST_KEY_ALIAS")
        self.mac_bind_host = os.environ.get("SHAPEFM_MAC_BIND_HOST") or subprocess.run(
            ["ipconfig", "getifaddr", "en0"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout.strip()
        if not self.mac_bind_host:
            raise RuntimeError("set SHAPEFM_MAC_BIND_HOST to the Mac LAN IPv4 address")
        self.scheduler_address = "tcp://127.0.0.1:8786"
        self.worker_scheduler_address = f"tcp://{self.mac_bind_host}:8786"
        self.runtime = ROOT / ".amp/in/artifacts/seasonal-recovery-cluster"
        self.processes: list[subprocess.Popen[str]] = []
        self.remote_started = False

    def _ssh_arguments(self, command: str) -> list[str]:
        """Build strict noninteractive SSH arguments for the verified Ubuntu host."""
        arguments = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "StrictHostKeyChecking=yes",
        ]
        if self.host_key_alias:
            arguments.extend(["-o", f"HostKeyAlias={self.host_key_alias}"])
        return [*arguments, self.ubuntu_host, command]

    def _ssh(self, command: str, timeout: float = 30) -> str:
        """Run one bounded command on the configured Ubuntu machine."""
        return subprocess.run(
            self._ssh_arguments(command),
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        ).stdout.strip()

    def preflight(self) -> dict[str, Any]:
        """Reject unavailable hosts, occupied ports, stale workers, or low memory."""
        occupied = subprocess.run(
            ["lsof", "-nP", "-iTCP:8786", "-iTCP:8787", "-sTCP:LISTEN"],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if occupied:
            raise RuntimeError(f"Dask recovery ports are already occupied:\n{occupied}")
        remote = self._ssh(
            "set -eu; "
            f"cd {shlex.quote(self.ubuntu_root)}; "
            "test -x .tools/uv/uv; test -f src/python/util/distributed_execution.py; "
            "test ! -e .amp/in/seasonal-recovery-ubuntu.pid; "
            "printf '%s|%s|%s' \"$(hostname)\" \"$(nproc)\" "
            "\"$(awk '/MemAvailable/ {printf \"%.3f\", $2/1024/1024}' /proc/meminfo)\""
        )
        hostname, logical_cpus, available_gib = remote.split("|")
        if int(logical_cpus) < int(self.profile.dask_ubuntu_cpu_workers or 0):
            raise RuntimeError("Ubuntu has fewer logical CPUs than the approved worker pool")
        ubuntu_floor = float(self.profile.dask_ubuntu_memory_min_available_gib or 0)
        if float(available_gib) < ubuntu_floor:
            raise RuntimeError(
                f"Ubuntu memory floor is not met: {available_gib} GiB available"
            )
        local_logical_cpus = os.cpu_count() or 0
        if local_logical_cpus < int(self.profile.dask_mac_cpu_workers or 0):
            raise RuntimeError("Mac has fewer logical CPUs than the approved worker pool")
        local_available_gib = system_memory().get("available_bytes", 0) / GIB
        mac_floor = float(self.profile.dask_mac_memory_min_available_gib or 0)
        if local_available_gib < mac_floor:
            raise RuntimeError(
                f"Mac memory floor is not met: {local_available_gib:.3f} GiB available"
            )
        return {
            "mac_bind_host": self.mac_bind_host,
            "ubuntu_hostname": hostname,
            "ubuntu_logical_cpus": int(logical_cpus),
            "ubuntu_available_memory_gib": float(available_gib),
            "mac_available_memory_gib": local_available_gib,
            "profile": self.profile.to_dict(),
            "profile_fingerprint": self.profile.fingerprint,
        }

    def _start_local(self, arguments: list[str], log_name: str) -> None:
        """Start one owned local daemon in a separate process group."""
        self.runtime.mkdir(parents=True, exist_ok=True)
        log = (self.runtime / log_name).open("a", encoding="utf-8")
        process = subprocess.Popen(
            arguments,
            cwd=ROOT,
            env={
                **os.environ,
                "PYTHONPATH": str(ROOT / "src/python"),
                "RENV_CONFIG_SYNCHRONIZED_CHECK": "false",
            },
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        log.close()
        self.processes.append(process)

    def start(self) -> dict[str, Any]:
        """Start scheduler and exact profile-owned CPU worker pools."""
        evidence = self.preflight()
        uv = str(ROOT / ".tools/uv/uv")
        base = [uv, "run", "--locked", "--no-sync", "dask"]
        self._start_local(
            [
                *base,
                "scheduler",
                "--host",
                "0.0.0.0",
                "--port",
                "8786",
                "--dashboard-address",
                "127.0.0.1:8787",
            ],
            "scheduler.log",
        )
        time.sleep(2)
        mac_tuning = int(self.profile.dask_mac_tuning_workers or 0)
        if mac_tuning:
            self._start_local(
                [
                    *base,
                    "worker",
                    self.worker_scheduler_address,
                    "--nworkers",
                    str(mac_tuning),
                    "--nthreads",
                    "1",
                    "--name",
                    "seasonal-mac-tuning",
                    "--resources",
                    f"CPU=1 {TUNING_R_RESOURCE}=1 {MAC_TUNING_R_RESOURCE}=1",
                    "--memory-limit",
                    f"{self.profile.dask_mac_worker_memory_gib}GiB",
                    "--no-dashboard",
                ],
                "mac-tuning.log",
            )
        remote_root = shlex.quote(self.ubuntu_root)
        remote_workers = (
            ".tools/uv/uv run --locked --no-sync dask worker "
            f"{shlex.quote(self.worker_scheduler_address)} "
            f"--nworkers {self.profile.dask_ubuntu_tuning_workers} --nthreads 1 "
            "--name seasonal-ubuntu-tuning "
            f"--resources 'CPU=1 {TUNING_R_RESOURCE}=1 {AUTOARIMA_R_RESOURCE}=1 "
            f"{UBUNTU_TUNING_R_RESOURCE}=1' "
            f"--memory-limit {self.profile.dask_ubuntu_worker_memory_gib}GiB "
            "--no-dashboard >>data/dask/seasonal-recovery.log 2>&1 & "
        )
        if self.topology["requires_gpu"]:
            from .distributed_execution import CHRONOS_GPU_RESOURCE
            remote_workers += (
                ".tools/uv/uv run --locked --no-sync dask worker "
                f"{shlex.quote(self.worker_scheduler_address)} "
                f"--nworkers {self.topology['ubuntu_gpu_workers']} --nthreads 1 "
                "--name seasonal-ubuntu-gpu "
                f"--resources 'GPU=1 {CHRONOS_GPU_RESOURCE}=1' "
                f"--memory-limit {self.profile.dask_ubuntu_worker_memory_gib}GiB --no-dashboard "
                ">>data/dask/seasonal-recovery-gpu.log 2>&1 & "
            )
        remote_command = (
            "set -eu; "
            f"cd {remote_root}; mkdir -p .amp/in data/dask; "
            "setsid nohup env PYTHONPATH=src/python RENV_CONFIG_SYNCHRONIZED_CHECK=false "
            f"sh -c {shlex.quote(remote_workers + 'wait')} "
            ">>data/dask/seasonal-recovery-launch.log 2>&1 </dev/null & "
            "echo $! >.amp/in/seasonal-recovery-ubuntu.pid"
        )
        self.remote_started = True
        self._ssh(remote_command)
        evidence["scheduler_address"] = self.scheduler_address
        evidence["configured_cpu_workers"] = {
            "mac": self.profile.dask_mac_cpu_workers,
            "ubuntu": self.profile.dask_ubuntu_cpu_workers,
        }
        evidence["configured_logical_gpu_workers"] = {
            "ubuntu": self.profile.dask_ubuntu_gpu_workers,
            "launched": self.topology["ubuntu_gpu_workers"],
        }
        evidence["resolved_topology"] = self.topology
        evidence["active_tuning_slots"] = {
            "mac": self.profile.dask_mac_tuning_workers,
            "ubuntu": self.profile.dask_ubuntu_tuning_workers,
        }
        return evidence

    def stop(self) -> None:
        """Stop only remote and local process groups created by this instance."""
        if self.remote_started:
            try:
                self._ssh(
                    "set +e; "
                    f"cd {shlex.quote(self.ubuntu_root)}; "
                    "p=.amp/in/seasonal-recovery-ubuntu.pid; "
                    "if test -f \"$p\"; then pid=$(cat \"$p\"); "
                    "kill -TERM -- -\"$pid\" 2>/dev/null; sleep 2; "
                    "kill -KILL -- -\"$pid\" 2>/dev/null; rm -f \"$p\"; fi"
                )
            except (OSError, subprocess.SubprocessError):
                # Best-effort remote cleanup must never prevent owned local
                # process groups from being released after partial startup.
                pass
            finally:
                self.remote_started = False
        for process in reversed(self.processes):
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)
        self.processes.clear()

    def __enter__(self) -> "ManagedTuningCluster":
        """Start the cluster for a bounded coordinator context."""
        self.start()
        return self

    def __exit__(self, *_: object) -> None:
        """Release all owned processes on normal or exceptional exit."""
        self.stop()
