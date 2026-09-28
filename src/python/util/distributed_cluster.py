# ==============================================================================
# distributed_cluster.py
#
# Purpose: Inspect and validate the ShapeFM Dask cluster without opening DuckDB.
# Inputs: Reference experiment JSON, repository HEAD, and a connected Dask client.
# Outputs: Expected identity values and scheduler/worker metadata for cluster diagnostics.
# Run from: Imported; not run directly.
# ==============================================================================

"""Inspect and validate the ShapeFM Dask cluster without opening DuckDB."""

from __future__ import annotations

import subprocess
from pathlib import Path

from distributed import Client

from .database import load_database_configuration
from .distributed_execution import ROOT, validate_cluster


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
