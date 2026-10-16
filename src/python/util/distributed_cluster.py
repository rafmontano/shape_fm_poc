"""Shared coordinator Prefect API and local/SSH Dask services for Processes 01–06.

Configured inventory and worker allocations start supported Dask services.
Dask owns scheduling; this coordinator-owned lifetime only starts/stops its own
services and returns the scheduler endpoint. No probes or scientific storage.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
from urllib.error import URLError
from urllib.request import urlopen

from distributed import Client, LocalCluster
from distributed.utils import get_ip
from prefect.settings import PREFECT_API_URL, temporary_settings

from util.runtime import CODE_ROOT


def _coordinator_host(settings: dict[str, Any], machines: dict[str, Any]) -> str:
    peer = next(allocation["machine"] for allocation in settings["workers"]
                if allocation["machine"] != settings["coordinator"])
    # Bind the route to the peer, not every loopback/link-local mDNS answer.
    return get_ip(machines[peer]["hostname"])


@contextmanager
def coordinator_api(profile: str) -> Iterator[None]:
    """Supply the supported reachable Prefect API before distributed flows start.

    Prefect's ephemeral localhost server cannot serve other machines. Inventory
    defines this coordinator service; task contexts carry its URL, not a second
    worker-local server. The owner stops only the subprocess it starts.
    """
    profiles = json.loads((CODE_ROOT / "config/execution_profiles.json").read_text())
    settings = profiles.get(profile, {})
    if "workers" not in settings:
        yield
        return
    inventory = json.loads((CODE_ROOT / "config/machines.json").read_text())
    host = _coordinator_host(settings, inventory["machines"])
    port = inventory["services"]["prefect_port"]
    url = f"http://{host}:{port}/api"
    process = subprocess.Popen([sys.executable, "-m", "prefect", "server", "start",
        "--host", host, "--port", str(port), "--no-ui"], cwd=CODE_ROOT)
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f"coordinator Prefect API exited with {process.returncode}")
            try:
                with urlopen(f"{url}/health", timeout=1) as response:
                    if response.status == 200:
                        break
            except URLError:
                time.sleep(0.1)
        else:
            raise TimeoutError("coordinator Prefect API did not become ready")
        with temporary_settings({PREFECT_API_URL: url}):
            yield
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=30)


class DistributedCluster:
    """Own the selected coordinator's Dask services for one experiment invocation."""

    def __init__(self, settings: dict[str, Any]):
        """Resolve configured machine/service inventory without starting services."""
        self.settings = settings
        self.processes = []
        self.client = None
        self.scheduler = None
        inventory = json.loads((CODE_ROOT / "config/machines.json").read_text())
        self.machines = inventory["machines"]
        self.port = inventory["services"]["dask_scheduler_port"]
        self.started_allocations: set[int] = set()
        self.allocations = []
        archived_assigned = False
        for allocation in settings["workers"]:
            allocation = dict(allocation)
            if allocation["machine"] == settings["coordinator"] and "CPU=" in allocation["resources"]:
                allocation["resources"] += ",COORDINATOR_CPU=1"
                if not archived_assigned:
                    allocation["resources"] += ",ARCHIVED=1"
                    archived_assigned = True
            self.allocations.append(allocation)

    def start(self, resources: dict[str, float] | None = None) -> str:
        """Start configured workers eligible for the opening workload's resources."""
        host = _coordinator_host(self.settings, self.machines)
        self.scheduler = LocalCluster(n_workers=0, host=host,
            scheduler_port=self.port, dashboard_address=None)
        self.client = Client(self.scheduler)
        self.ensure_workers(resources or {"CPU": 1})
        return self.scheduler.scheduler_address

    def ensure_workers(self, resources: dict[str, float]) -> None:
        """Start eligible allocations once, deferring GPU services until needed."""
        for index, allocation in enumerate(self.allocations):
            if index in self.started_allocations:
                continue
            available = dict(part.split("=") for part in allocation["resources"].split(","))
            if any(float(available.get(key, 0)) < value for key, value in resources.items()):
                continue
            machine = self.machines[allocation["machine"]]
            root = Path(machine["project_root"])
            command = [str(root / ".venv/bin/python"), "-X", "faulthandler", "-m", "distributed.cli.dask_worker", self.scheduler.scheduler_address,
                "--nworkers", str(allocation["count"]), "--nthreads", "1", "--no-dashboard",
                "--memory-limit", allocation["memory_limit"],
                "--resources", allocation["resources"], "--local-directory", str(root / ".runtime/dask")]
            environment = {"PYTHONPATH": str(root / "src/python"),
                           "PREFECT_HOME": str(root / ".runtime/prefect")}
            if "worker_connections_incoming" in self.settings:
                environment["DASK_DISTRIBUTED__WORKER__CONNECTIONS__INCOMING"] = str(
                    self.settings["worker_connections_incoming"])
            if allocation["machine"] != self.settings["coordinator"]:
                target = f"{machine['ssh_user']}@{machine['hostname']}"
                if "mps" in machine["capabilities"]:
                    command = ["caffeinate", "-i", *command]
                remote = ('export PATH=/opt/homebrew/bin:/usr/local/bin:"$PATH"; '
                    f"cd {shlex.quote(str(root))} && exec " + shlex.join(
                    ["env", *(f"{key}={value}" for key, value in environment.items()), *command]))
                command = ["ssh", "-o", "BatchMode=yes", target, remote]
            self.processes.append(subprocess.Popen(command, cwd=CODE_ROOT, env={**os.environ, **environment}))
            self.started_allocations.add(index)
        total = sum(self.allocations[index]["count"] for index in self.started_allocations)
        self.client.wait_for_workers(total, timeout=120)

    def close(self) -> None:
        """Stop only this invocation's Dask services; never touch another cluster."""
        if self.client is not None:
            self.client.close()
        if self.scheduler is not None:
            self.scheduler.close()
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=30)
