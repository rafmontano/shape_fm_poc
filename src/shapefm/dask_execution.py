"""Serializable ShapeFM Dask workers and bounded coordinator submission.

This module never opens DuckDB. Workers receive ordinary batches and return
ordinary result objects; the Mac coordinator remains the sole database owner.
"""

from __future__ import annotations

import atexit
import json
import os
import platform
import socket
import subprocess
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import Any

import dask
import distributed
from distributed import Client, Future, as_completed, get_worker

from .config import json_fingerprint
from .transformations import transform


EXPECTED_DASK_VERSION = "2026.8.0"
EXPECTED_GIFT_EVAL_REVISION = "4d5ab3fa0fe7451bbf59bb1ff6dd76e6e414d64a"
EXPECTED_CHRONOS_REVISION = "29ec3766d36d6f73f0696f85560a422f50e8498c"
ROOT = Path(__file__).resolve().parents[2]


def _worker_provenance(
    retry_count: int = 0, worker: Any = None
) -> dict[str, Any]:
    worker = worker or get_worker()
    state = getattr(worker, "state", None)
    resources = dict(getattr(state, "total_resources", {}) or {})
    try:
        task = state.tasks[worker.get_current_task()]
        retry_count = max(retry_count, task.run_id - 1)
    except (AttributeError, KeyError, TypeError):
        pass
    return {
        "execution_backend": "Dask Distributed",
        "hostname": socket.gethostname(),
        "dask_worker": worker.address,
        "dask_worker_name": str(worker.name),
        "resources": resources,
        "retry_count": retry_count,
    }


def _run_r(payload: dict[str, Any], timeout: float = 1800.0) -> dict[str, Any]:
    completed = subprocess.run(
        ["Rscript", str(ROOT / "R/poc1_worker.R")],
        cwd=ROOT,
        input=json.dumps(payload),
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
        env={**os.environ, "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1"},
    )
    return json.loads(completed.stdout)


def clean_batch(batch: list[dict[str, Any]], retry_count: int = 0) -> dict[str, Any]:
    started = time.monotonic()
    response = _run_r(
        {
            "action": "clean",
            "jobs": [
                {key: value for key, value in job.items() if key != "instance_id"}
                for job in batch
            ],
        }
    )
    runtime = time.monotonic() - started
    return {
        "results": response["results"],
        "packages": response["packages"],
        "runtime_seconds": runtime,
        "worker": _worker_provenance(retry_count),
    }


def transform_batch(batch: list[dict[str, Any]], retry_count: int = 0) -> dict[str, Any]:
    started = time.monotonic()
    results = []
    for job in batch:
        result = transform(job["values"], job["method"])
        results.append(
            {
                "id": job["id"],
                "values": list(result.values),
                "parameters": result.parameters,
            }
        )
    return {
        "results": results,
        "runtime_seconds": time.monotonic() - started,
        "worker": _worker_provenance(retry_count),
    }


def autoarima_batch(
    batch: list[dict[str, Any]], settings: dict[str, Any], retry_count: int = 0
) -> dict[str, Any]:
    started = time.monotonic()
    response = _run_r(
        {
            "action": "forecast",
            "jobs": [
                {
                    key: value
                    for key, value in job.items()
                    if key not in {"model", "instance_id", "variant_id"}
                }
                for job in batch
            ],
        }
    )
    runtime = time.monotonic() - started
    return {
        "results": response["results"],
        "runtime_seconds": runtime,
        "worker": {
            **_worker_provenance(retry_count),
            "packages": response["packages"],
            "settings": settings,
            "batch_task_count": len(batch),
        },
    }


def combine_batch(batch: list[dict[str, Any]], retry_count: int = 0) -> dict[str, Any]:
    started = time.monotonic()
    results = []
    for job in batch:
        left, right = job["left"], job["right"]

        def average(a: list[float], b: list[float]) -> list[float]:
            return [(x + y) / 2.0 for x, y in zip(a, b, strict=True)]

        quantiles = [
            average(left_values, right_values)
            for left_values, right_values in zip(
                left["quantiles"], right["quantiles"], strict=True
            )
        ]
        for point in zip(*quantiles, strict=True):
            if tuple(point) != tuple(sorted(point)):
                raise ValueError("combined quantiles are not ordered")
        results.append(
            {
                "id": job["id"],
                "mean": average(left["mean"], right["mean"]),
                "median": average(left["median"], right["median"]),
                "quantiles": quantiles,
            }
        )
    return {
        "results": results,
        "runtime_seconds": time.monotonic() - started,
        "worker": _worker_provenance(retry_count),
    }


_chronos_lock = threading.Lock()
_chronos_worker: Any = None
_chronos_key: tuple[str, str, str] | None = None
_chronos_generation = 0


def _close_chronos() -> None:
    global _chronos_worker, _chronos_key
    with _chronos_lock:
        if _chronos_worker is not None:
            _chronos_worker.close(force=True)
        _chronos_worker = None
        _chronos_key = None


atexit.register(_close_chronos)


def _get_chronos(model: str, revision: str, device: str) -> tuple[Any, int]:
    global _chronos_worker, _chronos_key, _chronos_generation
    from .execution import PersistentChronosWorker

    key = (model, revision, device)
    with _chronos_lock:
        if _chronos_worker is None or _chronos_key != key:
            if _chronos_worker is not None:
                _chronos_worker.close(force=True)
            command = [
                str(ROOT / "environments/chronos-2/.venv/bin/python"),
                str(ROOT / "src/shapefm/chronos_worker.py"),
                "serve",
                "--model",
                model,
                "--revision",
                revision,
                "--device",
                device,
            ]
            _chronos_worker = PersistentChronosWorker(command)
            _chronos_worker.start()
            _chronos_key = key
            _chronos_generation += 1
        return _chronos_worker, _chronos_generation


def chronos_batch(
    batch: list[dict[str, Any]],
    model: str,
    revision: str,
    quantile_levels: list[float],
    device: str,
    retry_count: int = 0,
) -> dict[str, Any]:
    worker, generation = _get_chronos(model, revision, device)
    started = time.monotonic()
    response = worker.request(
        {
            "command": "predict",
            "batch_id": f"chronos-batch/{uuid.uuid4().hex}",
            "jobs": [
                {
                    key: value
                    for key, value in job.items()
                    if key
                    not in {"model", "instance_id", "variant_id", "seasonality"}
                }
                for job in batch
            ],
            "horizon": batch[0]["horizon"],
            "quantile_levels": quantile_levels,
            "inference_batch_size": len(batch),
        }
    )
    if response.get("type") != "result":
        error = response.get("error", f"invalid Chronos response: {response}")
        if response.get("error_kind") == "out_of_memory":
            _close_chronos()
        raise RuntimeError(error)
    runtime = time.monotonic() - started
    return {
        "results": response["results"],
        "runtime_seconds": runtime,
        "worker": {
            **_worker_provenance(retry_count),
            **{key: value for key, value in worker.ready.items() if key != "type"},
            "worker_generation": generation,
            "effective_batch_size": response["effective_batch_size"],
            "inference_seconds": response["inference_seconds"],
            "peak_process_memory_bytes": response["peak_process_memory_bytes"],
            "accelerator_memory_after": response["accelerator_memory"],
        },
    }


def _command(*arguments: str, timeout: float = 30.0) -> str:
    return subprocess.run(
        list(arguments),
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    ).stdout.strip()


def worker_preflight(dask_worker: Any = None) -> dict[str, Any]:
    """Return exact environment identity from inside one Dask worker."""
    config = json.loads((ROOT / "config/experiments/poc1.json").read_text())
    scientific = {
        key: value
        for key, value in config.items()
        if key not in {"provisional_candidate", "submission_metadata"}
    }
    chronos_script = """
import importlib.metadata as metadata, json, pathlib, torch
root = pathlib.Path.home() / '.cache/huggingface/hub/models--amazon--chronos-2'
revision = (root / 'refs/main').read_text().strip()
print(json.dumps({
    'chronos_forecasting': metadata.version('chronos-forecasting'),
    'torch': torch.__version__,
    'torch_cuda': torch.version.cuda,
    'cuda_available': torch.cuda.is_available(),
    'cuda_name': torch.cuda.get_device_name(0) if torch.cuda.is_available() else None,
    'checkpoint_revision': revision,
    'checkpoint_present': (root / 'snapshots' / revision / 'model.safetensors').exists(),
}))
"""
    chronos = json.loads(
        _command(
            str(ROOT / "environments/chronos-2/.venv/bin/python"),
            "-c",
            chronos_script,
            timeout=60,
        )
    )
    r_packages = json.loads(
        _command(
            "Rscript",
            "-e",
            'cat(jsonlite::toJSON(list(R=as.character(getRversion()), '
            'renv=as.character(packageVersion("renv")), '
            'DBI=as.character(packageVersion("DBI")), '
            'duckdb=as.character(packageVersion("duckdb")), '
            'forecast=as.character(packageVersion("forecast")), '
            'jsonlite=as.character(packageVersion("jsonlite"))), auto_unbox=TRUE))',
        )
    )
    return {
        **_worker_provenance(worker=dask_worker),
        "git_commit": _command("git", "rev-parse", "HEAD"),
        "git_dirty": bool(_command("git", "status", "--porcelain", "--untracked-files=all")),
        "python_version": platform.python_version(),
        "dask_version": dask.__version__,
        "distributed_version": distributed.__version__,
        "configuration_hash": json_fingerprint(scientific),
        "gift_eval_revision": _command(
            "git", "-C", str(ROOT / "external/gift-eval"), "rev-parse", "HEAD"
        ),
        "r_packages": r_packages,
        "chronos": chronos,
    }


def validate_cluster(
    client: Client,
    *,
    expected_workers: int,
    timeout: float,
    expected_commit: str,
    expected_configuration_hash: str,
    require_gpu: bool,
    expected_gpu_name: str | None = "NVIDIA GeForce RTX 5090",
) -> dict[str, dict[str, Any]]:
    client.wait_for_workers(expected_workers, timeout=timeout)
    reports = client.run(worker_preflight)
    failures = []
    gpu_workers = 0
    for address, report in reports.items():
        expected = {
            "git_commit": expected_commit,
            "git_dirty": False,
            "python_version": "3.12.14",
            "dask_version": EXPECTED_DASK_VERSION,
            "distributed_version": EXPECTED_DASK_VERSION,
            "configuration_hash": expected_configuration_hash,
            "gift_eval_revision": EXPECTED_GIFT_EVAL_REVISION,
        }
        for field, value in expected.items():
            if report.get(field) != value:
                failures.append(
                    f"{address}: {field}={report.get(field)!r}, expected {value!r}"
                )
        r_expected = {
            "R": "4.6.1",
            "renv": "1.2.4",
            "forecast": "8.24.0",
            "jsonlite": "2.0.0",
        }
        for field, value in r_expected.items():
            if report["r_packages"].get(field) != value:
                failures.append(
                    f"{address}: R {field}={report['r_packages'].get(field)!r}, expected {value!r}"
                )
        chronos = report["chronos"]
        if chronos["chronos_forecasting"] != "2.2.2":
            failures.append(f"{address}: unexpected Chronos version")
        if chronos["checkpoint_revision"] != EXPECTED_CHRONOS_REVISION or not chronos[
            "checkpoint_present"
        ]:
            failures.append(f"{address}: pinned Chronos checkpoint is unavailable")
        if report["resources"].get("GPU", 0) >= 1:
            gpu_workers += 1
            if expected_gpu_name and (
                not chronos["cuda_available"]
                or chronos["cuda_name"] != expected_gpu_name
            ):
                failures.append(f"{address}: GPU worker is not the required RTX 5090 CUDA host")
    if len(reports) != expected_workers:
        failures.append(f"registered {len(reports)} workers, expected {expected_workers}")
    if require_gpu and gpu_workers != 1:
        failures.append(f"registered {gpu_workers} GPU workers, expected exactly 1")
    if failures:
        raise RuntimeError("Dask worker preflight failed:\n" + "\n".join(failures))
    return reports


def _batch_key(batch: list[dict[str, Any]]) -> str:
    identifiers = [job["id"] for job in batch]
    digest = json_fingerprint(identifiers)[:12]
    return f"{identifiers[0]}/batch-{len(identifiers)}-{digest}"


def run_batches(
    client: Client,
    function: Callable[..., dict[str, Any]],
    batches: Iterable[list[dict[str, Any]]],
    *,
    resources: dict[str, float],
    max_in_flight: int,
    retries: int,
    extra_arguments: tuple[Any, ...] = (),
) -> Iterator[tuple[list[dict[str, Any]], dict[str, Any]]]:
    """Submit a bounded window and release each future after its result is consumed."""
    pending_batches = iter(batches)
    future_batches: dict[Future, list[dict[str, Any]]] = {}
    completed = as_completed(with_results=True, raise_errors=False)

    def submit_one() -> bool:
        try:
            batch = next(pending_batches)
        except StopIteration:
            return False
        future = client.submit(
            function,
            batch,
            *extra_arguments,
            key=_batch_key(batch),
            resources=resources,
            retries=retries,
            pure=False,
        )
        future_batches[future] = batch
        completed.add(future)
        return True

    for _ in range(max_in_flight):
        if not submit_one():
            break
    try:
        while future_batches:
            future, result = next(completed)
            batch = future_batches.pop(future)
            if isinstance(result, BaseException):
                raise result
            yield batch, result
            future.release()
            submit_one()
    finally:
        for future in future_batches:
            future.cancel()
            future.release()


def run_batch_groups(
    client: Client,
    groups: dict[
        str,
        tuple[
            Callable[..., dict[str, Any]],
            Iterable[list[dict[str, Any]]],
            dict[str, float],
            tuple[Any, ...],
            int,
        ],
    ],
    *,
    retries: int,
) -> Iterator[tuple[str, list[dict[str, Any]], dict[str, Any] | BaseException]]:
    """Run bounded heterogeneous CPU/GPU queues and report completions incrementally."""
    iterators = {name: iter(specification[1]) for name, specification in groups.items()}
    future_batches: dict[Future, tuple[str, list[dict[str, Any]]]] = {}
    completed = as_completed(with_results=True, raise_errors=False)

    def submit_one(name: str) -> bool:
        function, _, resources, arguments, _ = groups[name]
        try:
            batch = next(iterators[name])
        except StopIteration:
            return False
        future = client.submit(
            function,
            batch,
            *arguments,
            key=_batch_key(batch),
            resources=resources,
            retries=retries,
            pure=False,
        )
        future_batches[future] = (name, batch)
        completed.add(future)
        return True

    for name, specification in groups.items():
        for _ in range(specification[4]):
            if not submit_one(name):
                break
    try:
        while future_batches:
            future, result = next(completed)
            name, batch = future_batches.pop(future)
            yield name, batch, result
            future.release()
            submit_one(name)
    finally:
        for future in future_batches:
            future.cancel()
            future.release()
