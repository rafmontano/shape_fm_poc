# ==============================================================================
# shared_distributed_cluster.py
#
# Purpose: Preflight and own the configured Prefect/Dask machine environment.
# Inputs: A centrally resolved execution profile and workload GPU requirement.
# Outputs: Managed service/worker lifecycle and operational topology evidence.
# Run from: Imported by the researcher action; never opens or writes DuckDB.
# ==============================================================================

"""Managed Prefect and Dask lifecycle for a resolved machine environment."""

from __future__ import annotations

import hashlib
import os
import platform
import shlex
import signal
import subprocess
import time
from pathlib import Path
from typing import Any
from urllib.request import ProxyHandler, build_opener

from .shared_distributed_execution import (
    AUTOARIMA_R_RESOURCE,
    MAC_TUNING_R_RESOURCE,
    ROOT,
    TUNING_R_RESOURCE,
    UBUNTU_TUNING_R_RESOURCE,
)
from .shared_execution_profiles import GIB, ExecutionProfile, system_memory
from .shared_machine_environment import ResolvedMachine


class ManagedTuningCluster:
    """Own coordinator services and every worker started for one resolved profile."""

    def __init__(
        self,
        profile: ExecutionProfile,
        requires_gpu: bool = False,
        gpu_workers: int | None = None,
        configuration: Any | None = None,
    ):
        """Resolve workload topology without starting a service or mutating research state."""
        environment = profile.machine_environment
        if environment is None:
            raise ValueError(
                f"profile {profile.name} does not define a managed machine environment"
            )
        if any(
            value is None
            for value in (
                profile.dask_autoarima_max_in_flight,
                profile.dask_ets_max_in_flight,
                profile.dask_autoarima_fit_budget_gib,
                profile.dask_ets_fit_budget_gib,
                profile.dask_memory_admission_timeout_seconds,
                profile.dask_memory_poll_interval_seconds,
                profile.dask_memory_breach_grace_seconds,
                profile.dask_swap_growth_limit_gib,
            )
        ):
            raise ValueError(f"profile {profile.name} lacks managed-cluster safeguards")
        if any(
            machine.allocation.tuning_workers != machine.allocation.cpu_workers
            for machine in environment.machines
        ):
            raise ValueError("every managed CPU worker must be eligible for tuning work")
        if gpu_workers is not None and (
            not requires_gpu
            or isinstance(gpu_workers, bool)
            or not isinstance(gpu_workers, int)
            or gpu_workers < 1
        ):
            raise ValueError("GPU worker override must be a positive workload capacity")
        self.profile = profile
        self.configuration = configuration
        self.environment = environment
        self.topology = environment.topology(requires_gpu, gpu_workers)
        if requires_gpu and self.topology["gpu_workers"] < 1:
            raise ValueError(f"profile {profile.name} does not define GPU workers")
        if requires_gpu and profile.accelerator_memory_min_available_gib <= 0:
            raise ValueError("GPU workloads require a positive accelerator memory floor")
        self.prefect_api_url = environment.prefect_api_url
        self.scheduler_address = environment.scheduler_address
        self.dashboard_url = environment.dashboard_url
        self.runtime = ROOT / ".amp/in/artifacts/managed-machine-cluster"
        self.processes: list[subprocess.Popen[str]] = []
        self.remote_started: set[str] = set()
        self.host_key_alias = os.environ.get("SHAPEFM_SSH_HOST_KEY_ALIAS")
        self._previous_prefect_environment: dict[str, str | None] | None = None

    def _ssh_arguments(self, machine: ResolvedMachine, command: str) -> list[str]:
        """Build strict noninteractive SSH arguments for one resolved remote."""
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
        return [*arguments, machine.machine.ssh_target, command]

    def _ssh(
        self, machine: ResolvedMachine, command: str, timeout: float = 30
    ) -> str:
        """Run one bounded command on a resolved remote machine."""
        return subprocess.run(
            self._ssh_arguments(machine, command),
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        ).stdout.strip()

    def _assert_coordinator(self) -> None:
        """Require the entry point to run on the selected coordinator checkout."""
        configured = self.environment.coordinator.machine
        observed = platform.node().rstrip(".").lower()
        expected = configured.hostname.rstrip(".").lower()
        if observed != expected:
            raise RuntimeError(
                f"selected coordinator {configured.machine_id} requires host "
                f"{configured.hostname}, found {platform.node()}"
            )
        if ROOT.resolve() != Path(configured.project_root).resolve():
            raise RuntimeError(
                f"selected coordinator project root is {configured.project_root}, "
                f"found {ROOT.resolve()}"
            )

    def preflight(self) -> dict[str, Any]:
        """Reject unavailable hosts, stale workers, occupied ports, or low memory."""
        self._assert_coordinator()
        revision = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True,
            capture_output=True, text=True, timeout=30,
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=all"],
            cwd=ROOT, check=True, capture_output=True, text=True, timeout=30,
        ).stdout.strip()
        if dirty:
            raise RuntimeError("coordinator worktree must be clean before distributed execution")
        required_files: dict[str, str] = {}
        if self.configuration is not None:
            paths = self.configuration.execution_paths
            for key in ("classifiers_lock", "mantis_lock"):
                relative = paths.get(key)
                if relative:
                    path = ROOT / relative
                    if not path.is_file():
                        raise RuntimeError(f"required lock is missing: {relative}")
                    required_files[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
            source = self.configuration.resolved["data"]["source"]
            dataset = self.configuration.resolved["data"]["dataset_name"]
            for name, digest in source["files"].items():
                relative = str(Path(source["directory"]) / dataset / name)
                path = ROOT / relative
                if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                    raise RuntimeError(f"required source file differs: {relative}")
                required_files[relative] = digest
            if getattr(self.configuration, "version", 0) in {11, 12}:
                subprocess.run(
                    [
                        str(ROOT / paths["classifiers_environment"] / "bin/python"),
                        "-c",
                        "import aeon, sklearn",
                    ],
                    cwd=ROOT, check=True, capture_output=True, text=True, timeout=60,
                )
                subprocess.run(
                    [
                        str(ROOT / paths["mantis_environment"] / "bin/python"),
                        str(ROOT / paths["mantis_worker"]),
                        "describe",
                        "--device",
                        "mps",
                    ],
                    cwd=ROOT, check=True, capture_output=True, text=True, timeout=300,
                )
        ports = self.environment.services
        occupied = subprocess.run(
            [
                "lsof",
                "-nP",
                f"-iTCP:{ports.prefect_port}",
                f"-iTCP:{ports.dask_scheduler_port}",
                f"-iTCP:{ports.dask_dashboard_port}",
                "-sTCP:LISTEN",
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if occupied:
            raise RuntimeError(f"managed service ports are already occupied:\n{occupied}")
        reports: dict[str, Any] = {}
        for remote in self.environment.remotes:
            root = shlex.quote(remote.machine.project_root)
            pid = shlex.quote(f".amp/in/managed-cluster-{remote.machine.machine_id}.pid")
            file_checks = "".join(
                f"test \"$(sha256sum {shlex.quote(path)} | cut -d' ' -f1)\" = "
                f"{shlex.quote(digest)}; "
                for path, digest in required_files.items()
            )
            runtime_checks = ""
            if (
                self.configuration is not None
                and getattr(self.configuration, "version", 0) in {11, 12}
            ):
                runtime_checks = (
                    "test -x environments/classifiers/.venv/bin/python; "
                    "environments/classifiers/.venv/bin/python -c "
                    + shlex.quote("import aeon, sklearn")
                    + "; test -x environments/mantis/.venv/bin/python; "
                    "environments/mantis/.venv/bin/python src/python/04_05_encode_mantis.py "
                    "describe --device cuda >/dev/null; "
                )
            report = self._ssh(
                remote,
                "set -eu; "
                f"cd {root}; "
                f"test \"$(git rev-parse HEAD)\" = {shlex.quote(revision)}; "
                "test -z \"$(git status --porcelain --untracked-files=all)\"; "
                "test -x .tools/uv/uv; test -f src/python/util/shared_distributed_execution.py; "
                + file_checks
                + runtime_checks
                + f"test ! -e {pid}; "
                "! pgrep -f '[m]anaged-shapefm-' >/dev/null; "
                f"getent hosts {shlex.quote(self.environment.client_host)} >/dev/null; "
                "printf '%s|%s|%s' \"$(hostname)\" \"$(nproc)\" "
                "\"$(awk '/MemAvailable/ {printf \"%.3f\", $2/1024/1024}' /proc/meminfo)\"",
            )
            hostname, logical_cpus, available_gib = report.split("|")
            allocation = remote.allocation
            if int(logical_cpus) < allocation.cpu_workers:
                raise RuntimeError(
                    f"{remote.machine.machine_id} has fewer logical CPUs than its worker pool"
                )
            floor = float(allocation.memory_min_available_gib or 0)
            if float(available_gib) < floor:
                raise RuntimeError(
                    f"{remote.machine.machine_id} memory floor is not met: "
                    f"{available_gib} GiB available"
                )
            reports[remote.machine.machine_id] = {
                "hostname": hostname,
                "logical_cpus": int(logical_cpus),
                "available_memory_gib": float(available_gib),
                "coordinator_hostname_resolved": True,
            }
        coordinator = self.environment.coordinator.allocation
        if (os.cpu_count() or 0) < coordinator.cpu_workers:
            raise RuntimeError("coordinator has fewer logical CPUs than its worker pool")
        available = system_memory().get("available_bytes", 0) / GIB
        if available < float(coordinator.memory_min_available_gib or 0):
            raise RuntimeError(
                f"coordinator memory floor is not met: {available:.3f} GiB available"
            )
        return {
            "machine_environment": self.environment.to_dict(),
            "repository_revision": revision,
            "required_file_fingerprints": required_files,
            "profile": self.profile.to_dict(),
            "profile_fingerprint": self.profile.fingerprint,
            "coordinator_available_memory_gib": available,
            "remote_preflight": reports,
            "ports_available": [
                ports.prefect_port,
                ports.dask_scheduler_port,
                ports.dask_dashboard_port,
            ],
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
                "PREFECT_API_URL": self.prefect_api_url,
                "PREFECT_SERVER_EPHEMERAL_ENABLED": "false",
            },
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        log.close()
        self.processes.append(process)

    def _wait_for_prefect(self, timeout: float = 60) -> None:
        """Wait until the owned coordinator Prefect API is healthy."""
        health = self.prefect_api_url.removesuffix("/api") + "/api/health"
        opener = build_opener(ProxyHandler({}))
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.processes and self.processes[0].poll() is not None:
                raise RuntimeError("owned Prefect service exited during startup")
            try:
                with opener.open(health, timeout=2) as response:
                    if response.status == 200:
                        return
            except OSError:
                time.sleep(0.5)
        raise RuntimeError(f"owned Prefect service did not become healthy at {health}")

    def _verify_remote_endpoint(self, remote: ResolvedMachine, service: str) -> None:
        """Verify coordinator Prefect or Dask reachability from one enabled remote."""
        if service == "prefect":
            script = (
                "import urllib.request; "
                f"u={self.prefect_api_url.removesuffix('/api') + '/api/health'!r}; "
                "o=urllib.request.build_opener(urllib.request.ProxyHandler({})); "
                "assert o.open(u, timeout=10).status == 200"
            )
        elif service == "dask":
            script = (
                "import socket; "
                f"s=socket.create_connection(({self.environment.client_host!r}, "
                f"{self.environment.services.dask_scheduler_port}), timeout=10); s.close()"
            )
        else:
            raise ValueError(f"unknown service {service}")
        self._ssh(
            remote,
            f"cd {shlex.quote(remote.machine.project_root)}; "
            f".venv/bin/python -c {shlex.quote(script)}",
            timeout=20,
        )

    def _remote_worker_command(self, remote: ResolvedMachine) -> str:
        """Build one remote machine's bounded CPU/GPU worker process group."""
        machine_id = remote.machine.machine_id
        allocation = remote.allocation
        worker_memory = allocation.worker_memory_gib
        topology = self.topology["workers_by_machine"][machine_id]
        commands = []
        if topology["cpu_workers"]:
            commands.append(
                ".tools/uv/uv run --locked --no-sync dask worker "
                f"{shlex.quote(self.scheduler_address)} "
                f"--nworkers {topology['cpu_workers']} --nthreads 1 "
                f"--name managed-shapefm-{machine_id}-cpu "
                f"--resources 'CPU=1 {TUNING_R_RESOURCE}=1 {AUTOARIMA_R_RESOURCE}=1 "
                f"{UBUNTU_TUNING_R_RESOURCE}=1' "
                f"--memory-limit {worker_memory}GiB --no-dashboard "
                f">>data/dask/managed-{machine_id}-cpu.log 2>&1 &"
            )
        if topology["gpu_workers"]:
            from .shared_distributed_execution import CHRONOS_GPU_RESOURCE

            commands.append(
                "env CUDA_VISIBLE_DEVICES=0 .tools/uv/uv run --locked --no-sync "
                f"dask worker {shlex.quote(self.scheduler_address)} "
                f"--nworkers {topology['gpu_workers']} --nthreads 1 "
                f"--name managed-shapefm-{machine_id}-gpu "
                f"--resources 'GPU=1 {CHRONOS_GPU_RESOURCE}=1' "
                f"--memory-limit {worker_memory}GiB --no-dashboard "
                f">>data/dask/managed-{machine_id}-gpu.log 2>&1 &"
            )
        return " ".join(commands) + " wait"

    def start(self) -> dict[str, Any]:
        """Preflight, start owned services/workers, and verify remote endpoints."""
        evidence = self.preflight()
        uv = str(ROOT / ".tools/uv/uv")
        ports = self.environment.services
        try:
            self._previous_prefect_environment = {
                name: os.environ.get(name)
                for name in ("PREFECT_API_URL", "PREFECT_SERVER_EPHEMERAL_ENABLED")
            }
            os.environ["PREFECT_API_URL"] = self.prefect_api_url
            os.environ["PREFECT_SERVER_EPHEMERAL_ENABLED"] = "false"
            self._start_local(
                [
                    uv,
                    "run",
                    "--locked",
                    "--no-sync",
                    "prefect",
                    "server",
                    "start",
                    "--host",
                    "0.0.0.0",
                    "--port",
                    str(ports.prefect_port),
                ],
                "prefect.log",
            )
            self._wait_for_prefect()
            for remote in self.environment.remotes:
                self._verify_remote_endpoint(remote, "prefect")
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
                    str(ports.dask_scheduler_port),
                    "--dashboard-address",
                    f"0.0.0.0:{ports.dask_dashboard_port}",
                ],
                "scheduler.log",
            )
            time.sleep(2)
            for remote in self.environment.remotes:
                self._verify_remote_endpoint(remote, "dask")
            coordinator = self.environment.coordinator
            coordinator_workers = self.topology["workers_by_machine"][
                coordinator.machine.machine_id
            ]
            if coordinator_workers["cpu_workers"]:
                self._start_local(
                    [
                        uv,
                        "run",
                        "--locked",
                        "--no-sync",
                        "dask",
                        "worker",
                        self.scheduler_address,
                        "--nworkers",
                        str(coordinator_workers["cpu_workers"]),
                        "--nthreads",
                        "1",
                        "--name",
                        f"managed-shapefm-{coordinator.machine.machine_id}-cpu",
                        "--resources",
                        f"CPU=1 {TUNING_R_RESOURCE}=1 {MAC_TUNING_R_RESOURCE}=1",
                        "--memory-limit",
                        f"{coordinator.allocation.worker_memory_gib}GiB",
                        "--no-dashboard",
                    ],
                    f"{coordinator.machine.machine_id}-cpu.log",
                )
            for remote in self.environment.remotes:
                root = shlex.quote(remote.machine.project_root)
                machine_id = remote.machine.machine_id
                pid = shlex.quote(f".amp/in/managed-cluster-{machine_id}.pid")
                command = self._remote_worker_command(remote)
                remote_command = (
                    "set -eu; "
                    f"cd {root}; mkdir -p .amp/in data/dask; "
                    "setsid nohup env PYTHONPATH=src/python "
                    "RENV_CONFIG_SYNCHRONIZED_CHECK=false "
                    f"PREFECT_API_URL={shlex.quote(self.prefect_api_url)} "
                    "PREFECT_SERVER_EPHEMERAL_ENABLED=false "
                    f"sh -c {shlex.quote(command)} "
                    f">>data/dask/managed-{machine_id}-launch.log 2>&1 </dev/null & "
                    f"echo $! >{pid}"
                )
                self._ssh(remote, remote_command)
                self.remote_started.add(machine_id)
        except BaseException:
            self.stop()
            raise
        evidence.update(
            {
                "prefect_api_url": self.prefect_api_url,
                "scheduler_address": self.scheduler_address,
                "dashboard_url": self.dashboard_url,
                "resolved_topology": self.topology,
                "remote_prefect_reachable": [
                    value.machine.machine_id for value in self.environment.remotes
                ],
                "remote_scheduler_reachable": [
                    value.machine.machine_id for value in self.environment.remotes
                ],
            }
        )
        return evidence

    def stop(self) -> None:
        """Stop only remote and local process groups created by this instance."""
        for remote in self.environment.remotes:
            machine_id = remote.machine.machine_id
            if machine_id not in self.remote_started:
                continue
            try:
                pid = shlex.quote(f".amp/in/managed-cluster-{machine_id}.pid")
                self._ssh(
                    remote,
                    "set +e; "
                    f"cd {shlex.quote(remote.machine.project_root)}; "
                    f"p={pid}; if test -f \"$p\"; then pid=$(cat \"$p\"); "
                    "kill -TERM -- -\"$pid\" 2>/dev/null; sleep 2; "
                    "kill -KILL -- -\"$pid\" 2>/dev/null; rm -f \"$p\"; fi",
                )
            except (OSError, subprocess.SubprocessError):
                pass
        self.remote_started.clear()
        for process in reversed(self.processes):
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)
        self.processes.clear()
        if self._previous_prefect_environment is not None:
            for name, value in self._previous_prefect_environment.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
            self._previous_prefect_environment = None

    def __enter__(self) -> "ManagedTuningCluster":
        """Start the cluster for a bounded coordinator context."""
        self.start()
        return self

    def __exit__(self, *_: object) -> None:
        """Release all owned processes on normal or exceptional exit."""
        self.stop()
