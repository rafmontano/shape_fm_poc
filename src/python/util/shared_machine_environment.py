# ==============================================================================
# shared_machine_environment.py
#
# Purpose: Validate the machine inventory and resolve one execution topology.
# Inputs: Tracked machine/profile JSON and optional explicit operational overrides.
# Outputs: Immutable coordinator, worker, endpoint, and non-secret evidence objects.
# Run from: Imported by the central execution-profile loader and cluster lifecycle.
# ==============================================================================

"""Central machine inventory and coordinator-selection boundary."""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

from .p01_02_import_execution import repository_root


_MACHINE_ID = re.compile(r"^[a-z][a-z0-9_]*$")
_CAPABILITIES = frozenset({"cpu", "cuda", "mps"})
_INVENTORY_FIELDS = frozenset({"configuration_version", "machines", "services"})
_MACHINE_FIELDS = frozenset({"hostname", "ssh_user", "project_root", "capabilities"})
_SERVICE_FIELDS = frozenset(
    {"prefect_port", "dask_scheduler_port", "dask_dashboard_port"}
)
_ALLOCATION_FIELDS = frozenset(
    {
        "enabled",
        "cpu_workers",
        "gpu_capacity",
        "tuning_workers",
        "worker_memory_gib",
        "memory_min_available_gib",
    }
)
_UNSAFE_CLIENT_HOSTS = frozenset({"localhost", "127.0.0.1", "0.0.0.0", "::1"})


def _reject_unknown(values: Mapping[str, Any], allowed: frozenset[str], context: str) -> None:
    unknown = sorted(set(values) - allowed)
    if unknown:
        raise ValueError(f"unknown {context} fields: {', '.join(unknown)}")


@dataclass(frozen=True)
class MachineRecord:
    """One validated stable machine identity from the tracked inventory."""

    machine_id: str
    hostname: str
    project_root: str
    capabilities: tuple[str, ...]
    ssh_user: str | None = None

    @property
    def ssh_target(self) -> str:
        """Return the strict SSH destination for a remote machine."""
        return f"{self.ssh_user}@{self.hostname}" if self.ssh_user else self.hostname


@dataclass(frozen=True)
class MachineAllocation:
    """Profile-owned worker and memory limits for one enabled machine."""

    enabled: bool
    cpu_workers: int = 0
    gpu_capacity: int = 0
    tuning_workers: int = 0
    worker_memory_gib: int | None = None
    memory_min_available_gib: float | None = None


@dataclass(frozen=True)
class ServicePorts:
    """Tracked ports used by coordinator-owned temporary services."""

    prefect_port: int
    dask_scheduler_port: int
    dask_dashboard_port: int


@dataclass(frozen=True)
class ResolvedMachine:
    """One enabled inventory record paired with its profile allocation."""

    machine: MachineRecord
    allocation: MachineAllocation
    coordinator: bool


@dataclass(frozen=True)
class MachineEnvironment:
    """Complete immutable operational topology for one execution profile."""

    configuration_version: int
    coordinator_id: str
    machines: tuple[ResolvedMachine, ...]
    services: ServicePorts
    overrides: tuple[tuple[str, str], ...] = ()

    @property
    def coordinator(self) -> ResolvedMachine:
        """Return the sole enabled coordinator."""
        return next(machine for machine in self.machines if machine.coordinator)

    @property
    def remotes(self) -> tuple[ResolvedMachine, ...]:
        """Return enabled non-coordinator machines in stable ID order."""
        return tuple(machine for machine in self.machines if not machine.coordinator)

    @property
    def client_host(self) -> str:
        """Return the effective non-loopback coordinator address used by clients."""
        return (
            dict(self.overrides).get("coordinator_address")
            or self.coordinator.machine.hostname
        )

    @property
    def prefect_api_url(self) -> str:
        """Return the coordinator-hostname Prefect client endpoint."""
        return f"http://{self.client_host}:{self.services.prefect_port}/api"

    @property
    def scheduler_address(self) -> str:
        """Return the coordinator-hostname Dask client/worker endpoint."""
        return f"tcp://{self.client_host}:{self.services.dask_scheduler_port}"

    @property
    def dashboard_url(self) -> str:
        """Return the coordinator-hostname Dask dashboard URL."""
        return f"http://{self.client_host}:{self.services.dask_dashboard_port}/status"

    def machine(self, machine_id: str) -> ResolvedMachine:
        """Return one enabled machine by logical ID."""
        try:
            return next(value for value in self.machines if value.machine.machine_id == machine_id)
        except StopIteration as exc:
            raise ValueError(f"machine {machine_id!r} is not enabled") from exc

    def topology(self, requires_gpu: bool, gpu_workers: int | None = None) -> dict[str, Any]:
        """Resolve generic per-machine CPU/GPU worker counts for one workload."""
        workers: dict[str, dict[str, int]] = {}
        remaining_gpu = gpu_workers
        for resolved in self.machines:
            allocation = resolved.allocation
            gpu = allocation.gpu_capacity if requires_gpu else 0
            if remaining_gpu is not None and gpu:
                gpu = min(gpu, remaining_gpu)
                remaining_gpu -= gpu
            workers[resolved.machine.machine_id] = {
                "cpu_workers": allocation.cpu_workers,
                "gpu_workers": gpu,
                "tuning_workers": allocation.tuning_workers,
            }
        if remaining_gpu not in {None, 0}:
            raise ValueError("GPU worker override exceeds enabled profile capacity")
        cpu = sum(value["cpu_workers"] for value in workers.values())
        gpu = sum(value["gpu_workers"] for value in workers.values())
        return {
            "coordinator_machine_id": self.coordinator_id,
            "hostnames_by_machine": {
                item.machine.machine_id: item.machine.hostname for item in self.machines
            },
            "workers_by_machine": workers,
            "cpu_workers": cpu,
            "gpu_workers": gpu,
            "total_workers": cpu + gpu,
            "requires_gpu": requires_gpu,
        }

    def to_dict(self) -> dict[str, Any]:
        """Return serialisable non-secret effective topology evidence."""
        return {
            "configuration_version": self.configuration_version,
            "coordinator_machine_id": self.coordinator_id,
            "machines": {
                item.machine.machine_id: {
                    **asdict(item.machine),
                    "allocation": asdict(item.allocation),
                    "coordinator": item.coordinator,
                }
                for item in self.machines
            },
            "services": asdict(self.services),
            "endpoints": {
                "prefect_api_url": self.prefect_api_url,
                "dask_scheduler_address": self.scheduler_address,
                "dask_dashboard_url": self.dashboard_url,
            },
            "overrides": dict(self.overrides),
        }


def _load_inventory(path: Path) -> tuple[int, dict[str, MachineRecord], ServicePorts]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("machine inventory must be a JSON object")
    _reject_unknown(raw, _INVENTORY_FIELDS, "machine inventory")
    if raw.get("configuration_version") != 1:
        raise ValueError("machine inventory configuration_version must be 1")
    machines_raw = raw.get("machines")
    services_raw = raw.get("services")
    if not isinstance(machines_raw, dict) or not machines_raw:
        raise ValueError("machine inventory requires at least one machine")
    if not isinstance(services_raw, dict):
        raise ValueError("machine inventory services must be an object")
    _reject_unknown(services_raw, _SERVICE_FIELDS, "service")
    if set(services_raw) != _SERVICE_FIELDS:
        raise ValueError("machine inventory must define all service ports")
    ports = []
    for name in sorted(_SERVICE_FIELDS):
        value = services_raw[name]
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65535:
            raise ValueError(f"{name} must be a valid TCP port")
        ports.append(value)
    if len(set(ports)) != len(ports):
        raise ValueError("machine service ports must be distinct")
    records: dict[str, MachineRecord] = {}
    identities: set[str] = set()
    for machine_id, values in machines_raw.items():
        if not isinstance(machine_id, str) or not _MACHINE_ID.fullmatch(machine_id):
            raise ValueError(f"unsafe machine ID {machine_id!r}")
        if not isinstance(values, dict):
            raise ValueError(f"machine {machine_id} must be an object")
        _reject_unknown(values, _MACHINE_FIELDS, f"machine {machine_id}")
        if set(values) - {"ssh_user"} != _MACHINE_FIELDS - {"ssh_user"}:
            raise ValueError(f"machine {machine_id} has incomplete fields")
        hostname = values["hostname"]
        root = values["project_root"]
        capabilities = values["capabilities"]
        if not isinstance(hostname, str) or not hostname.strip() or hostname in _UNSAFE_CLIENT_HOSTS:
            raise ValueError(f"machine {machine_id} has an invalid hostname")
        identity = hostname.rstrip(".").lower()
        if identity in identities:
            raise ValueError(f"duplicate machine hostname {hostname!r}")
        identities.add(identity)
        if not isinstance(root, str) or not Path(root).is_absolute():
            raise ValueError(f"machine {machine_id} project_root must be absolute")
        if (
            not isinstance(capabilities, list)
            or not capabilities
            or any(value not in _CAPABILITIES for value in capabilities)
            or len(set(capabilities)) != len(capabilities)
        ):
            raise ValueError(f"machine {machine_id} has invalid capabilities")
        ssh_user = values.get("ssh_user")
        if ssh_user is not None and (
            not isinstance(ssh_user, str) or not re.fullmatch(r"[A-Za-z0-9._-]+", ssh_user)
        ):
            raise ValueError(f"machine {machine_id} has an invalid ssh_user")
        records[machine_id] = MachineRecord(
            machine_id, hostname, root, tuple(sorted(capabilities)), ssh_user
        )
    return 1, records, ServicePorts(**services_raw)


def resolve_machine_environment(
    coordinator_id: str,
    allocations: Mapping[str, Any],
    inventory_path: Path | None = None,
    environment: Mapping[str, str] | None = None,
) -> MachineEnvironment:
    """Validate inventory/profile data and return one normalized topology."""
    path = inventory_path or repository_root() / "config/machines.json"
    version, records, services = _load_inventory(path)
    if coordinator_id not in records:
        raise ValueError(f"unknown coordinator machine {coordinator_id!r}")
    if not isinstance(allocations, Mapping) or not allocations:
        raise ValueError("distributed profile requires machine allocations")
    enabled: list[ResolvedMachine] = []
    for machine_id, values in allocations.items():
        if machine_id not in records:
            raise ValueError(f"unknown profile machine {machine_id!r}")
        if not isinstance(values, Mapping):
            raise ValueError(f"machine allocation {machine_id} must be an object")
        _reject_unknown(values, _ALLOCATION_FIELDS, f"machine allocation {machine_id}")
        allocation = MachineAllocation(**values)
        for field in ("cpu_workers", "gpu_capacity", "tuning_workers"):
            value = getattr(allocation, field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{machine_id}.{field} must be a nonnegative integer")
        if allocation.tuning_workers > allocation.cpu_workers:
            raise ValueError(f"{machine_id} tuning workers exceed its CPU workers")
        for field in ("worker_memory_gib", "memory_min_available_gib"):
            value = getattr(allocation, field)
            if value is not None and (isinstance(value, bool) or value <= 0):
                raise ValueError(f"{machine_id}.{field} must be positive")
        capabilities = records[machine_id].capabilities
        if allocation.cpu_workers and "cpu" not in capabilities:
            raise ValueError(f"machine {machine_id} lacks CPU capability")
        if allocation.gpu_capacity and "cuda" not in capabilities:
            raise ValueError(f"machine {machine_id} lacks CUDA capability")
        if allocation.enabled:
            enabled.append(
                ResolvedMachine(records[machine_id], allocation, machine_id == coordinator_id)
            )
    if sum(machine.coordinator for machine in enabled) != 1:
        raise ValueError("exactly one enabled machine must be the coordinator")
    env = environment if environment is not None else os.environ
    overrides: list[tuple[str, str]] = []
    address = env.get("SHAPEFM_COORDINATOR_ADDRESS")
    if address:
        normalized = address.lower()
        parsed = urlparse(f"//{address}")
        if (
            parsed.hostname != normalized
            or normalized in _UNSAFE_CLIENT_HOSTS
            or not re.fullmatch(r"[A-Za-z0-9_.-]+", address)
        ):
            raise ValueError("SHAPEFM_COORDINATOR_ADDRESS must be a safe non-loopback host")
        overrides.append(("coordinator_address", address))
    return MachineEnvironment(
        version,
        coordinator_id,
        tuple(sorted(enabled, key=lambda item: item.machine.machine_id)),
        services,
        tuple(overrides),
    )
