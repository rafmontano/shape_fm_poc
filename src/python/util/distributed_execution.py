"""Shared work-group execution for all six ShapeFM processes.

Inputs are pure atomic WorkItems and calculation/acceptance callbacks. Dask and
Prefect's supported task runner place bounded groups; only the coordinator
accepts returned scientific results into DuckDB. No scientific setting changes.
"""

from __future__ import annotations

import json
import socket
import time
import traceback
from dataclasses import dataclass
from typing import Any, Callable

from prefect import task
from prefect.cache_policies import NO_CACHE
from prefect.futures import as_completed
from prefect_dask import DaskTaskRunner

from util.runtime import CODE_ROOT, ProgressSnapshot, WorkItem


@dataclass
class GroupResult:
    """Completed atomic outputs and the first original failure in a work group."""

    results: list[tuple[str, dict[str, Any]]]
    error: Exception | None = None
    failed_key: str | None = None


@task(name="Calculate scientific work group", cache_policy=NO_CACHE, persist_result=False)
def calculate_group(items: list[WorkItem], calculate: Callable[..., dict[str, Any]],
                    calculate_batch: Callable | None = None,
                    shared_inputs: dict[str, Any] | None = None) -> GroupResult:
    """Work-group task: calculate atomic items without scientific persistence.

    The coordinator commits each returned completed item before surfacing a later
    failure. DuckDB atomic keys, not this task's operational state, own restart.
    """
    if calculate_batch is not None:
        results = []
        try:
            for result in calculate_batch(items, **(shared_inputs or {})):
                results.append(result)
            return GroupResult(results)
        except Exception as error:
            from distributed import get_worker
            try:
                worker = get_worker().address
            except ValueError:
                worker = "sequential"
            error.add_note(f"ShapeFM work group {[item.key for item in items]}, "
                           f"host={socket.gethostname()}, worker={worker}")
            return GroupResult(results, error)
    results = []
    for item in items:
        try:
            results.append((item.key, calculate(item, **(shared_inputs or {}))))
        except Exception as error:
            return GroupResult(results, error, item.key)
    return GroupResult(results)


class WorkGroupExecutor:
    """One bounded executor for all meaningful subflows; no second scheduler.

    Sequential and distributed modes invoke the same calculation. Dask owns
    scheduling and resources, Prefect owns tasks, and DuckDB owns acceptance.
    """

    def __init__(self, context: Any):
        """Bind coordinator callbacks and placement profile; defer cluster startup."""
        self.context = context
        profiles = json.loads((CODE_ROOT / "config/execution_profiles.json").read_text())
        profiles.update(context.configuration.get("execution_profiles", {}))
        self.settings = profiles[context.request.execution_profile]
        self.cluster = None

    def resources(self, provider: str) -> dict[str, float]:
        """Read registry placement labels; they never enter scientific identity."""
        return self.settings.get("providers", {}).get(provider, {}).get("resources", {})

    def close(self) -> None:
        """Release this invocation's cluster, retaining accepted scientific state."""
        if self.cluster is not None:
            self.cluster.close()

    def run(self, *, subflow_id: str, items: list[WorkItem], calculate: Callable,
            accept: Callable, on_progress: Callable, calculate_batch: Callable | None = None,
            shared_inputs: dict[str, Any] | None = None) -> None:
        """Execute only pending atomic keys and preserve commits across failure.

        Completion progress is derived from the owning database after commits.
        Periodic tracking is separately gated by the stored experiment setting.
        Immutable shared inputs are scattered once, not embedded in each task.
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
            """Report accepted database counts, never uncommitted worker progress."""
            done, total = repository.counts(subflow_id)
            minutes = (time.monotonic() - started) / 60
            rate = done / minutes if minutes > 0 and done else None
            on_progress(ProgressSnapshot(subflow_id, done, total, minutes, rate,
                                         (total - done) / rate if rate else None))

        def receive(result: GroupResult) -> None:
            """Accept completed items before surfacing a later work-group failure."""
            if result.results:
                accept(result.results)
                progress()
            if result.error is not None:
                result.error.add_note(f"ShapeFM {subflow_id}, atomic item {result.failed_key}")
                raise result.error

        profile = self.context.request.execution_profile
        if profile == "sequential_safe":
            for group in groups:
                receive(calculate_group.fn(group, calculate, calculate_batch, shared_inputs))
        elif groups:
            settings = self.settings
            placement = settings.get("subflows", {}).get(subflow_id, {}).get("resources", {})
            address = settings.get("scheduler_address")
            if "workers" in settings:
                if self.cluster is None:
                    from util.distributed_cluster import DistributedCluster
                    self.cluster = DistributedCluster(settings)
                    self.cluster.address = self.cluster.start(resources=placement)
                address = self.cluster.address
                for group_resources in {tuple(sorted(group[0].resources.items())) for group in groups}:
                    self.cluster.ensure_workers({**(dict(group_resources) or {"CPU": 1}), **placement})
            runner = (DaskTaskRunner(address=address) if address else
                      DaskTaskRunner(cluster_kwargs=settings.get("cluster_kwargs", {
                          "n_workers": 2, "threads_per_worker": 1,
                          "processes": False, "dashboard_address": None,
                          "local_directory": str(CODE_ROOT / ".runtime/dask")})))
            with runner:
                shared = shared_inputs
                if shared_inputs:
                    [shared] = runner.client.scatter([shared_inputs], hash=False, broadcast=False)
                limit = int(settings.get("max_in_flight", 4))
                for offset in range(0, len(groups), limit):
                    futures = []
                    for group in groups[offset:offset + limit]:
                        import dask
                        resources = group[0].resources or ({"CPU": 1} if "workers" in settings else {})
                        resources = {**resources, **placement}
                        with dask.annotate(resources=resources):
                            futures.append(runner.submit(calculate_group,
                                parameters={"items": group, "calculate": calculate,
                                            "calculate_batch": calculate_batch, "shared_inputs": shared}))
                    first_error = None
                    for future in as_completed(futures):
                        try:
                            receive(future.result())
                        except Exception as error:
                            # Retain every failed group's notes while draining successful commits.
                            traceback.print_exception(error)
                            if first_error is None:
                                first_error = error
                    if first_error is not None:
                        raise first_error
        repository.require_complete(subflow_id)
        progress()
