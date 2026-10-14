"""Shared work-group execution for all six ShapeFM processes.

Inputs are pure atomic WorkItems and calculation/acceptance callbacks. Dask and
Prefect's supported task runner place bounded groups; only the coordinator
accepts returned scientific results into DuckDB. No scientific setting changes.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable

from prefect import task
from prefect.cache_policies import NO_CACHE
from prefect.futures import as_completed
from prefect_dask import DaskTaskRunner

from util.runtime import ProgressSnapshot, WorkItem


@dataclass
class GroupResult:
    """Completed atomic outputs and the first original failure in a work group."""

    results: list[tuple[str, dict[str, Any]]]
    error: Exception | None = None
    failed_key: str | None = None


@task(name="Calculate scientific work group", cache_policy=NO_CACHE, persist_result=False)
def calculate_group(items: list[WorkItem], calculate: Callable[[WorkItem], dict[str, Any]],
                    calculate_batch: Callable | None = None) -> GroupResult:
    """Work-group task: calculate atomic items without scientific persistence.

    The coordinator commits each returned completed item before surfacing a later
    failure. DuckDB atomic keys, not this task's operational state, own restart.
    """
    if calculate_batch is not None:
        try:
            return GroupResult(calculate_batch(items))
        except Exception as error:
            return GroupResult([], error)
    results = []
    for item in items:
        try:
            results.append((item.key, calculate(item)))
        except Exception as error:
            return GroupResult(results, error, item.key)
    return GroupResult(results)


class WorkGroupExecutor:
    """One bounded executor for all meaningful subflows; no second scheduler.

    Sequential and distributed modes invoke the same calculation. Dask owns
    scheduling and resources, Prefect owns tasks, and DuckDB owns acceptance.
    """

    def __init__(self, context: Any):
        self.context = context

    def run(self, *, subflow_id: str, items: list[WorkItem], calculate: Callable,
            accept: Callable, on_progress: Callable, calculate_batch: Callable | None = None) -> None:
        """Execute only pending atomic keys and preserve commits across failure.

        Completion progress is derived from the owning database after commits.
        Periodic tracking is separately gated by the stored experiment setting.
        """
        started = time.monotonic()
        repository = self.context.windows() if subflow_id == "03.02" else self.context.repository
        execution = self.context.configuration["execution"]["default"]
        group_size = int(execution.get("work_group_size", 8))
        groups = []
        for offset in range(0, len(items), group_size):
            chunk = items[offset:offset + group_size]
            # Resource-homogeneous groups keep CPU/GPU placement explicit.
            for resource in {tuple(sorted(i.resources.items())) for i in chunk}:
                groups.append([i for i in chunk if tuple(sorted(i.resources.items())) == resource])

        def progress() -> None:
            done, total = repository.counts(subflow_id)
            minutes = (time.monotonic() - started) / 60
            rate = done / minutes if minutes > 0 and done else None
            on_progress(ProgressSnapshot(subflow_id, done, total, minutes, rate,
                                         (total - done) / rate if rate else None))

        def receive(result: GroupResult) -> None:
            if result.results:
                accept(result.results)
                progress()
            if result.error is not None:
                result.error.add_note(f"ShapeFM {subflow_id}, atomic item {result.failed_key}")
                raise result.error

        profile = self.context.request.execution_profile
        if profile == "sequential_safe":
            for group in groups:
                receive(calculate_group.fn(group, calculate, calculate_batch))
        elif groups:
            profiles = self.context.configuration.get("execution_profiles", {})
            settings = profiles[profile]
            runner = DaskTaskRunner(address=settings.get("scheduler_address"),
                                    cluster_kwargs=settings.get("cluster_kwargs", {
                                        "n_workers": 2, "threads_per_worker": 1,
                                        "processes": False, "dashboard_address": None}))
            with runner:
                limit = int(settings.get("max_in_flight", 4))
                for offset in range(0, len(groups), limit):
                    futures = []
                    for group in groups[offset:offset + limit]:
                        import dask
                        with dask.annotate(resources=group[0].resources):
                            futures.append(runner.submit(calculate_group,
                                parameters={"items": group, "calculate": calculate,
                                            "calculate_batch": calculate_batch}))
                    for future in as_completed(futures):
                        receive(future.result())
        repository.require_complete(subflow_id)
        progress()
