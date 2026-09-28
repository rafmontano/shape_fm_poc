# ==============================================================================
# distributed_execution.py
#
# Purpose: Serializable ShapeFM Dask workers and bounded coordinator submission.
# Inputs: Serializable job batches, Dask clients/workers, execution settings, and model identities.
# Outputs: Serializable worker results/provenance and bounded completion iterators; never DuckDB writes.
# Run from: Imported; not run directly.
# ==============================================================================

"""Serializable ShapeFM Dask workers and bounded coordinator submission."""

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

from .configuration import json_fingerprint
from .forecast_combination import combine_equal_weight
from .transformations import transform


# Code constant: pinned Dask protocol release required on coordinator and workers.
EXPECTED_DASK_VERSION = "2026.8.0"
# Code constant: repository root derived from this source path; it has no override.
ROOT = Path(__file__).resolve().parents[3]
# Code constant: Dask protocol label for one logical Chronos GPU slot.
CHRONOS_GPU_RESOURCE = "CHRONOS_GPU_SLOT"


def _worker_provenance(
    retry_count: int = 0, worker: Any = None
) -> dict[str, Any]:
    """Purpose: Describe one Dask execution attempt. Inputs: ``retry_count`` is the zero-based coordinator retry count; ``worker`` is a Dask Worker-like object, defaulting to the current worker. Outputs: A serializable mapping of backend, host, worker identity, total resource capacities, and retry count; samples current worker state without mutating it."""
    worker = worker or get_worker()
    state = getattr(worker, "state", None)
    resources = dict(getattr(state, "total_resources", {}) or {})
    return {
        "execution_backend": "Dask Distributed",
        "hostname": socket.gethostname(),
        "dask_worker": worker.address,
        "dask_worker_name": str(worker.name),
        "resources": resources,
        "retry_count": retry_count,
    }


def worker_resource_snapshot(dask_worker: Any = None) -> dict[str, Any]:
    """Purpose: Sample resource telemetry on a Dask worker. Inputs: ``dask_worker`` is a Dask Worker-like object or ``None`` for the current worker. Outputs: A mapping with CPU percent and memory/spill counters in bytes, plus NVIDIA utilization percent, byte capacities, and name for GPU-slot workers; invokes ``nvidia-smi`` and samples process-global psutil state."""
    import psutil

    worker = dask_worker or get_worker()
    state = getattr(worker, "state", None)
    resources = dict(getattr(state, "total_resources", {}) or {})
    memory = psutil.virtual_memory()
    swap = psutil.swap_memory()
    spilled = getattr(getattr(worker, "data", None), "spilled_total", None)
    snapshot: dict[str, Any] = {
        "hostname": socket.gethostname(),
        "worker": worker.address,
        "resources": resources,
        "cpu_percent": psutil.cpu_percent(interval=None),
        "system_available_memory_bytes": int(memory.available),
        "swap_used_bytes": int(swap.used),
        "dask_spilled_memory_bytes": int(getattr(spilled, "memory", 0)),
        "dask_spilled_disk_bytes": int(getattr(spilled, "disk", 0)),
    }
    if resources.get(CHRONOS_GPU_RESOURCE, 0) >= 1:
        query = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=utilization.gpu,memory.free,memory.total,name",
                "--format=csv,noheader,nounits",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout.strip().split(", ")
        snapshot["gpu"] = {
            "utilization_percent": float(query[0]),
            "available_memory_bytes": int(float(query[1]) * 1024 * 1024),
            "total_memory_bytes": int(float(query[2]) * 1024 * 1024),
            "name": query[3],
        }
    return snapshot


def _run_r(
    payload: dict[str, Any], script: str, timeout: float, threads: int
) -> dict[str, Any]:
    """Purpose: Execute a repository R JSON bridge. Inputs: ``payload`` is JSON-serializable request data, ``script`` is a repository-relative R script path, ``timeout`` is seconds, and ``threads`` is the positive BLAS/OpenMP thread limit. Outputs: The decoded JSON object from stdout; starts and waits for an ``Rscript`` subprocess with repository cwd and thread-limit environment variables."""
    completed = subprocess.run(
        ["Rscript", str(ROOT / script)],
        cwd=ROOT,
        input=json.dumps(payload),
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
        env={
            **os.environ,
            "OMP_NUM_THREADS": str(threads),
            "OPENBLAS_NUM_THREADS": str(threads),
            "RENV_CONFIG_SYNCHRONIZED_CHECK": "false",
        },
    )
    return json.loads(completed.stdout)


def clean_batch(
    batch: list[dict[str, Any]],
    script: str,
    timeout: float,
    threads: int,
    retry_count: int = 0,
) -> dict[str, Any]:
    """Purpose: Clean a serializable batch through the R worker. Inputs: ``batch`` contains job mappings with IDs and series values from the coordinator, ``script`` is repository-relative, ``timeout`` is seconds, ``threads`` is the R thread limit, and ``retry_count`` is the zero-based Dask attempt. Outputs: Cleaned results, R package versions, elapsed seconds, and worker provenance; launches one R subprocess and omits coordinator-only instance IDs."""
    started = time.monotonic()
    response = _run_r(
        {
            "action": "clean",
            "jobs": [
                {key: value for key, value in job.items() if key != "instance_id"}
                for job in batch
            ],
        },
        script,
        timeout,
        threads,
    )
    runtime = time.monotonic() - started
    return {
        "results": response["results"],
        "packages": response["packages"],
        "runtime_seconds": runtime,
        "worker": _worker_provenance(retry_count),
    }


def transform_batch(batch: list[dict[str, Any]], retry_count: int = 0) -> dict[str, Any]:
    """Purpose: Apply configured transformations to one worker batch. Inputs: ``batch`` is coordinator-originated job mappings containing ID, numeric series values, and an allowed transformation method; ``retry_count`` is the zero-based Dask attempt. Outputs: Serializable transformed one-dimensional values and parameters per ID, elapsed seconds, and sampled worker provenance."""
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
    batch: list[dict[str, Any]],
    settings: dict[str, Any],
    script: str,
    timeout: float,
    threads: int,
    retry_count: int = 0,
) -> dict[str, Any]:
    """Purpose: Forecast one batch with the R AutoARIMA bridge. Inputs: ``batch`` contains coordinator jobs with IDs, numeric contexts, horizons, and seasonality; ``settings`` is resolved AutoARIMA configuration; ``script`` is repository-relative; timeout is seconds, threads is the R limit, and retry count is zero-based. Outputs: Forecast result mappings, elapsed seconds, and worker/package/settings provenance; launches one R subprocess and strips routing-only fields."""
    started = time.monotonic()
    response = _run_r(
        {
            "action": "forecast",
            "settings": settings,
            "jobs": [
                {
                    key: value
                    for key, value in job.items()
                    if key not in {"model", "instance_id", "variant_id"}
                }
                for job in batch
            ],
        },
        script,
        timeout,
        threads,
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
    """Purpose: Combine paired forecasts for a worker batch. Inputs: ``batch`` contains IDs, left/right forecast mappings, and two combination weights from the coordinator; ``retry_count`` is the zero-based Dask attempt. Outputs: Combined forecasts per ID, elapsed seconds, and sampled worker provenance; performs no external writes."""
    started = time.monotonic()
    results = []
    for job in batch:
        combination = combine_equal_weight(
            job["left"], job["right"], job["weights"]
        )
        results.append(
            {
                "id": job["id"],
                **combination,
            }
        )
    return {
        "results": results,
        "runtime_seconds": time.monotonic() - started,
        "worker": _worker_provenance(retry_count),
    }


# Process-global lock serializes access to one cached Chronos subprocess per Dask worker.
_chronos_lock = threading.Lock()
# Cached worker client and its model/revision/device key; both are replaced together.
_chronos_worker: Any = None
_chronos_key: tuple[str, str, str, str, int, str, str, float, float] | None = None
# Monotonic process-local restart counter reported with forecast provenance.
_chronos_generation = 0


def _close_chronos() -> None:
    """Purpose: Dispose of the process-global Chronos bridge. Inputs: None; uses the module-owned worker, key, and lock. Outputs: None; force-terminates any child process and clears cached process identity state."""
    global _chronos_worker, _chronos_key
    with _chronos_lock:
        if _chronos_worker is not None:
            _chronos_worker.close(force=True)
        _chronos_worker = None
        _chronos_key = None


atexit.register(_close_chronos)


def _get_chronos(
    model: str,
    revision: str,
    device: str,
    dtype: str,
    internal_cpu_threads: int,
    environment: str,
    worker_script: str,
    startup_timeout: float,
    request_timeout: float,
) -> tuple[Any, int]:
    """Purpose: Acquire the Dask process's keyed persistent Chronos bridge. Inputs: Model/revision/device/dtype strings, positive internal CPU threads, repository-relative environment and worker paths, and startup/request timeouts in seconds. Outputs: The owned ``PersistentChronosWorker`` and monotonic generation number; under a global lock, starts a subprocess or force-replaces one whose full configuration key differs."""
    global _chronos_worker, _chronos_key, _chronos_generation
    from .execution_profiles import PersistentChronosWorker

    key = (
        model,
        revision,
        device,
        dtype,
        internal_cpu_threads,
        environment,
        worker_script,
        startup_timeout,
        request_timeout,
    )
    with _chronos_lock:
        if _chronos_worker is None or _chronos_key != key:
            if _chronos_worker is not None:
                _chronos_worker.close(force=True)
            command = [
                str(ROOT / environment / "bin/python"),
                str(ROOT / worker_script),
                "serve",
                "--model",
                model,
                "--revision",
                revision,
                "--device",
                device,
                "--dtype",
                dtype,
                "--internal-cpu-threads",
                str(internal_cpu_threads),
            ]
            _chronos_worker = PersistentChronosWorker(
                command, startup_timeout=startup_timeout
            )
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
    dtype: str,
    cross_learning: bool,
    predict_batches_jointly: bool,
    internal_cpu_threads: int,
    environment: str,
    worker_script: str,
    startup_timeout: float,
    request_timeout: float,
    retry_count: int = 0,
) -> dict[str, Any]:
    """Purpose: Forecast one homogeneous batch through the cached Chronos subprocess. Inputs: ``batch`` contains coordinator jobs with IDs, numeric contexts, and a common positive horizon; model identity, quantiles in [0,1], device/dtype and batching flags configure inference; thread count and timeouts are bounded execution controls; retry count is zero-based. Outputs: Forecasts, elapsed seconds, worker/process generation, effective batch size, inference seconds, and memory telemetry; may start/restart or close the cached subprocess on out-of-memory errors."""
    worker, generation = _get_chronos(
        model,
        revision,
        device,
        dtype,
        internal_cpu_threads,
        environment,
        worker_script,
        startup_timeout,
        request_timeout,
    )
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
            "cross_learning": cross_learning,
            "predict_batches_jointly": predict_batches_jointly,
        },
        timeout=request_timeout,
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
    """Purpose: Execute a preflight command from the repository root. Inputs: ``arguments`` are executable and argv strings; ``timeout`` is seconds. Outputs: Stripped stdout text; starts and waits for a subprocess and raises on nonzero status or timeout."""
    return subprocess.run(
        list(arguments),
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    ).stdout.strip()


def worker_preflight(
    configuration_hash: str,
    chronos_repository: str,
    chronos_revision: str,
    chronos_environment: str,
    gift_eval_source_directory: str,
    dask_worker: Any = None,
) -> dict[str, Any]:
    """Purpose: Probe one Dask worker for cluster compatibility. Inputs: Coordinator configuration hash, pinned Chronos repository/revision, repository-relative Python environment and GIFT-Eval checkout paths, plus an optional Dask Worker object. Outputs: Serializable host/resources, Git state, Python/Dask/R/Chronos versions, checkpoint and CUDA evidence; launches Python, R, and Git subprocesses and reads the local model cache."""
    chronos_script = """
import importlib.metadata as metadata, json, os, pathlib, sys, torch
cache = pathlib.Path(os.environ.get('HF_HOME', pathlib.Path.home() / '.cache/huggingface')) / 'hub'
root = cache / ('models--' + sys.argv[1].replace('/', '--'))
revision = sys.argv[2]
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
            str(ROOT / chronos_environment / "bin/python"),
            "-c",
            chronos_script,
            chronos_repository,
            chronos_revision,
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
        "configuration_hash": configuration_hash,
        "gift_eval_revision": _command(
            "git",
            "-C",
            str(ROOT / gift_eval_source_directory),
            "rev-parse",
            "HEAD",
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
    expected_gift_eval_revision: str,
    expected_chronos_revision: str,
    expected_chronos_version: str,
    chronos_repository: str,
    chronos_environment: str,
    gift_eval_source_directory: str,
    require_gpu: bool,
    expected_gpu_name: str | None,
    expected_gpu_workers: int = 1,
) -> dict[str, dict[str, Any]]:
    """Purpose: Enforce the coordinator's Dask cluster identity contract. Inputs: A distributed ``Client``; positive expected worker/GPU counts; timeout in seconds; pinned commit, configuration, source, checkpoint and package identities; dependency paths; and GPU requirements/name. Outputs: Reports keyed by worker address when all checks pass; waits for workers, remotely runs hardware/software probes, and raises one aggregated error on mismatch."""
    if expected_gpu_workers < 1:
        raise ValueError("expected_gpu_workers must be positive")
    client.wait_for_workers(expected_workers, timeout=timeout)
    reports = client.run(
        worker_preflight,
        expected_configuration_hash,
        chronos_repository,
        expected_chronos_revision,
        chronos_environment,
        gift_eval_source_directory,
    )
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
            "gift_eval_revision": expected_gift_eval_revision,
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
        if chronos["chronos_forecasting"] != expected_chronos_version:
            failures.append(f"{address}: unexpected Chronos version")
        if chronos["checkpoint_revision"] != expected_chronos_revision or not chronos[
            "checkpoint_present"
        ]:
            failures.append(f"{address}: pinned Chronos checkpoint is unavailable")
        if report["resources"].get(CHRONOS_GPU_RESOURCE, 0) >= 1:
            gpu_workers += 1
            if expected_gpu_name and (
                not chronos["cuda_available"]
                or chronos["cuda_name"] != expected_gpu_name
            ):
                failures.append(f"{address}: GPU worker is not the required RTX 5090 CUDA host")
    if len(reports) != expected_workers:
        failures.append(f"registered {len(reports)} workers, expected {expected_workers}")
    if require_gpu and gpu_workers != expected_gpu_workers:
        failures.append(
            f"registered {gpu_workers} GPU workers, expected exactly "
            f"{expected_gpu_workers}"
        )
    if failures:
        raise RuntimeError("Dask worker preflight failed:\n" + "\n".join(failures))
    return reports


def _batch_key(batch: list[dict[str, Any]]) -> str:
    """Purpose: Build a deterministic Dask task-key prefix. Inputs: A nonempty ordered batch of job mappings with string ``id`` values. Outputs: A string containing the first ID, batch length, and a 12-hex SHA-256-derived fingerprint; has no side effects."""
    identifiers = [job["id"] for job in batch]
    digest = json_fingerprint(identifiers)[:12]
    return f"{identifiers[0]}/batch-{len(identifiers)}-{digest}"


def _future_error(result: Any) -> BaseException | None:
    """Purpose: Normalize Dask completion failures. Inputs: A completion result of any type, including an exception or three-item ``exc_info`` tuple. Outputs: The represented ``BaseException`` or ``None`` for a successful/nonstandard value; has no side effects."""
    if isinstance(result, BaseException):
        return result
    if (
        isinstance(result, tuple)
        and len(result) == 3
        and isinstance(result[0], type)
        and issubclass(result[0], BaseException)
        and isinstance(result[1], BaseException)
    ):
        return result[1]
    return None


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
    """Purpose: Execute homogeneous Dask batches with bounded concurrency and explicit retries. Inputs: A distributed ``Client``, serializable worker callable, iterable of job batches, Dask resource quantities, positive in-flight limit, nonnegative retry count, and positional worker arguments. Outputs: An iterator of original batch/result pairs in completion order; submits uniquely keyed impure tasks, retries failed batches up to the limit, releases consumed futures, and cancels/releases pending work on exit."""
    pending_batches = iter(batches)
    future_batches: dict[Future, tuple[list[dict[str, Any]], int]] = {}
    completed = as_completed(with_results=True, raise_errors=False)

    def submit_batch(batch: list[dict[str, Any]], retry_count: int) -> None:
        """Purpose: Register one batch attempt with Dask. Inputs: A job-mapping batch and zero-based retry count. Outputs: None; submits one impure future with requested resources and records it in closure-owned completion state."""
        future = client.submit(
            function,
            batch,
            *extra_arguments,
            retry_count,
            key=f"{_batch_key(batch)}/dask-attempt-{retry_count}",
            resources=resources,
            retries=0,
            pure=False,
        )
        future_batches[future] = (batch, retry_count)
        completed.add(future)

    def submit_one() -> bool:
        """Purpose: Advance the pending batch iterator once. Inputs: None; consumes closure-owned input. Outputs: ``True`` after submitting attempt zero or ``False`` when exhausted; mutates pending-future state."""
        try:
            batch = next(pending_batches)
        except StopIteration:
            return False
        submit_batch(batch, 0)
        return True

    for _ in range(max_in_flight):
        if not submit_one():
            break
    try:
        while future_batches:
            future, result = next(completed)
            batch, retry_count = future_batches.pop(future)
            error = _future_error(result)
            if error is not None:
                future.release()
                if retry_count < retries:
                    submit_batch(batch, retry_count + 1)
                    continue
                raise error
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
    """Purpose: Execute named heterogeneous Dask queues concurrently with per-group bounds. Inputs: A distributed ``Client``; group mappings of worker callable, batch iterable, resource quantities, positional arguments, and positive in-flight limit; plus a nonnegative retry limit. Outputs: Completion-order tuples of group name, original batch, and result or terminal exception; retries each failed attempt, releases futures, and cancels/releases outstanding work on exit."""
    iterators = {name: iter(specification[1]) for name, specification in groups.items()}
    future_batches: dict[Future, tuple[str, list[dict[str, Any]], int]] = {}
    completed = as_completed(with_results=True, raise_errors=False)

    def submit_batch(
        name: str, batch: list[dict[str, Any]], retry_count: int
    ) -> None:
        """Purpose: Register one named-group batch attempt with Dask. Inputs: Existing group name, job-mapping batch, and zero-based retry count. Outputs: None; submits an impure resource-constrained future and updates closure-owned completion state."""
        function, _, resources, arguments, _ = groups[name]
        future = client.submit(
            function,
            batch,
            *arguments,
            retry_count,
            key=f"{_batch_key(batch)}/dask-attempt-{retry_count}",
            resources=resources,
            retries=0,
            pure=False,
        )
        future_batches[future] = (name, batch, retry_count)
        completed.add(future)

    def submit_one(name: str) -> bool:
        """Purpose: Advance one named group's batch iterator. Inputs: An existing group name. Outputs: ``True`` after submitting attempt zero or ``False`` at exhaustion; mutates pending-future state."""
        try:
            batch = next(iterators[name])
        except StopIteration:
            return False
        submit_batch(name, batch, 0)
        return True

    for name, specification in groups.items():
        for _ in range(specification[4]):
            if not submit_one(name):
                break
    try:
        while future_batches:
            future, result = next(completed)
            name, batch, retry_count = future_batches.pop(future)
            error = _future_error(result)
            if error is not None and retry_count < retries:
                future.release()
                submit_batch(name, batch, retry_count + 1)
                continue
            yield name, batch, error or result
            future.release()
            submit_one(name)
    finally:
        for future in future_batches:
            future.cancel()
            future.release()
