"""Inspect and validate the ShapeFM Dask cluster without opening DuckDB."""

from __future__ import annotations

import argparse
import json
import subprocess

from distributed import Client

from .config import json_fingerprint
from .dask_execution import ROOT, validate_cluster


def _configuration_hash() -> str:
    config = json.loads((ROOT / "config/experiments/poc1.json").read_text())
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["status", "preflight", "shutdown"])
    parser.add_argument("--address", default="tcp://127.0.0.1:8786")
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--expected-workers", type=int, default=7)
    parser.add_argument("--require-gpu", action="store_true")
    args = parser.parse_args()
    client = Client(args.address, timeout=f"{args.timeout}s")
    try:
        if args.command == "status":
            result = _worker_summary(client)
        elif args.command == "preflight":
            reports = validate_cluster(
                client,
                expected_workers=args.expected_workers,
                timeout=args.timeout,
                expected_commit=_commit(),
                expected_configuration_hash=_configuration_hash(),
                require_gpu=args.require_gpu,
            )
            result = {**_worker_summary(client), "preflight": reports}
        else:
            result = {"scheduler": args.address, "shutdown": True}
            print(json.dumps(result, sort_keys=True))
            client.shutdown()
            return
        print(json.dumps(result, sort_keys=True))
    finally:
        client.close(timeout=5)


if __name__ == "__main__":
    main()
