# ==============================================================================
# forecast_flow.py
#
# Purpose: Readable ordinary Gate 4 Prefect composition.
# Inputs: Coordinator-local storage, bounded jobs, provider objects, and execution limits.
# Outputs: Validated committed forecasts; compute tasks never receive storage handles.
# Execution: Mac flow owns storage; Prefect tasks compute locally or on existing Dask.
# Run from: Imported; not run directly.
# ==============================================================================

"""Prefect flow for ordinary AutoARIMA and Chronos forecasting."""

from __future__ import annotations

from collections import deque
from contextlib import nullcontext
from pathlib import Path
from typing import Any

import dask
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from prefect.futures import as_completed
from prefect_dask import DaskTaskRunner

from .distributed_execution import AUTOARIMA_R_RESOURCE
from .forecast_storage import ForecastStorage


@task(name="compute AutoARIMA forecast batch", cache_policy=NO_CACHE, persist_result=False)
def compute_autoarima_batch(provider: Any, batch: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute one bounded AutoARIMA batch without coordinator storage."""
    return provider.forecast("auto_arima", batch)


@task(name="compute Chronos forecast batch", cache_policy=NO_CACHE, persist_result=False)
def compute_chronos_batch(provider: Any,
                          batch: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute one bounded Chronos batch without coordinator storage."""
    return provider.forecast("chronos_2", batch)


def _batches(values: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    """Partition jobs into stable bounded batches (small flow-local utility)."""
    return [values[index:index + size] for index in range(0, len(values), size)]


def _run_submission_phase(*, cpu_batches: list[list[dict[str, Any]]],
                          gpu_batches: list[list[dict[str, Any]]], auto_task: Any,
                          chronos_task: Any, auto_provider: Any, chronos_provider: Any,
                          storage: ForecastStorage, max_in_flight: int,
                          autoarima_max_in_flight: int, distributed: bool) -> None:
    """Submit one bounded phase, reserving a slot while CPU work can progress.

    Submitted Prefect futures, including futures undergoing Prefect retries, own
    capacity until their terminal result is collected.  After the first failure
    no new work is admitted, while every already-submitted future is drained.
    """
    pending_cpu = deque(cpu_batches)
    pending_gpu = deque(gpu_batches)
    submitted: dict[Any, tuple[str, list[dict[str, Any]]]] = {}
    active_cpu = 0
    failures: list[Exception] = []

    while submitted or pending_cpu or pending_gpu:
        while not failures and len(submitted) < max_in_flight:
            model = None
            if pending_cpu and active_cpu < autoarima_max_in_flight:
                model = "auto_arima"
                batch = pending_cpu.popleft()
            else:
                cpu_work_remains = bool(pending_cpu) or active_cpu > 0
                # A blocked GPU future must not consume the last global slot
                # while eligible/current CPU work may need that slot to refill.
                gpu_allowed = pending_gpu and (
                    not cpu_work_remains or len(submitted) - active_cpu < max_in_flight - 1
                )
                # Local providers retain the existing single Chronos process;
                # distributed concurrency belongs to the GPU worker resources.
                if not distributed and any(model == "chronos_2" for model, _ in submitted.values()):
                    gpu_allowed = False
                if gpu_allowed:
                    model = "chronos_2"
                    batch = pending_gpu.popleft()
                else:
                    break
            # Ordinary protected fits use the same approved large-fit capability
            # as tuning; generic Mac CPU capacity does not imply a12GiB fit fits.
            resource = ({"CPU": 1, AUTOARIMA_R_RESOURCE: 1}
                        if model == "auto_arima" else {"CHRONOS_GPU_SLOT": 1})
            annotation = dask.annotate(resources=resource, retries=0) if distributed else nullcontext()
            try:
                with annotation:
                    future = (auto_task.submit(auto_provider, batch)
                              if model == "auto_arima"
                              else chronos_task.submit(chronos_provider, batch))
            except Exception as error:
                failures.append(error)
                break
            submitted[future] = (model, batch)
            active_cpu += model == "auto_arima"

        if not submitted:
            if failures:
                break
            if pending_cpu or pending_gpu:
                raise RuntimeError("forecast admission limits prevent pending work from progressing")
            break

        future = next(as_completed(list(submitted)))
        model, batch = submitted.pop(future)
        active_cpu -= model == "auto_arima"
        try:
            storage.commit_response(batch, future.result())
        except Exception as error:
            failures.append(error)

    if failures:
        raise failures[0]


@flow(name="gate-4-ordinary-forecast", persist_result=False, validate_parameters=False)
def ordinary_forecast_flow(database: Path, experiment_id: str, attempts: dict[str, int],
                           rows: list[tuple], *, storage_type: type[ForecastStorage],
                           auto_provider: Any, chronos_provider: Any,
                           auto_batch_size: int, chronos_batch_size: int,
                           max_in_flight: int, autoarima_max_in_flight: int,
                           cpu_gpu_overlap: bool,
                           distributed: bool, retries: int) -> None:
    """Prepare jobs, submit bounded named compute tasks, commit locally, and verify."""
    from .experiment_execution import ExperimentCoordinator, _length_aware_batches

    if max_in_flight < 1 or autoarima_max_in_flight < 1:
        raise ValueError("forecast in-flight limits must be positive")

    # Live storage is created inside the coordinator flow, never in its serialized
    # parameters: prefect-dask propagates parent parameters in task context.
    with ExperimentCoordinator(database) as coordinator:
        storage = storage_type(coordinator, experiment_id, attempts)
        jobs = storage.prepare_pending_jobs(rows)
        auto_batches = _batches(
            [job for job in jobs if job["model"] == "auto_arima"], auto_batch_size
        )
        chronos_batches = _length_aware_batches(
            [job for job in jobs if job["model"] == "chronos_2"], chronos_batch_size
        )
        auto_task = compute_autoarima_batch.with_options(retries=retries)
        chronos_task = compute_chronos_batch.with_options(retries=retries)
        phases = ([(auto_batches, chronos_batches)] if cpu_gpu_overlap
                  else [(auto_batches, []), ([], chronos_batches)])
        for cpu_phase, gpu_phase in phases:
            _run_submission_phase(
                cpu_batches=cpu_phase, gpu_batches=gpu_phase, auto_task=auto_task,
                chronos_task=chronos_task, auto_provider=auto_provider,
                chronos_provider=chronos_provider, storage=storage,
                max_in_flight=max_in_flight,
                autoarima_max_in_flight=autoarima_max_in_flight,
                distributed=distributed,
            )
        storage.verify_completion(jobs)


def run_ordinary_forecast_flow(*, scheduler_address: str | None, **kwargs: Any) -> None:
    """Run locally or bind explicitly to an existing scheduler; never create a cluster."""
    storage = kwargs.pop("storage")
    kwargs.update(database=storage.coordinator.database_path,
                  experiment_id=storage.experiment_id, attempts=storage.attempts,
                  storage_type=type(storage))
    selected = ordinary_forecast_flow
    if kwargs["distributed"]:
        if not scheduler_address:
            raise RuntimeError("ordinary Gate 4 Dask execution requires an existing scheduler address")
        selected = ordinary_forecast_flow.with_options(task_runner=DaskTaskRunner(address=scheduler_address))
    selected(**kwargs)
