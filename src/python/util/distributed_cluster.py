"""Inspect and validate the ShapeFM Dask cluster without opening DuckDB."""

from __future__ import annotations

import json
import subprocess

from distributed import Client

from .configuration import json_fingerprint
from .distributed_execution import ROOT, validate_cluster


def _configuration_hash() -> str:
    config = json.loads(
        (ROOT / "config/experiments/m4_daily_reference.json").read_text()
    )
    return json_fingerprint(
        {
            key: value
            for key, value in config.items()
            if key not in {"provisional_candidate", "submission_metadata"}
        }
    )


def _commit() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _worker_summary(client: Client) -> dict:
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
