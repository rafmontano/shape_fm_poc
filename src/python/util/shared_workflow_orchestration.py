# ==============================================================================
# shared_workflow_orchestration.py
#
# Purpose: Define ShapeFM's Prefect experiment, gate, and preparation workflows.
# Inputs: Existing DuckDB paths, process IDs, resolved execution controls, and thin callables around established coordinators.
# Outputs: Prefect run identities and existing scientific summaries; research results remain in DuckDB.
# Run from: Imported; not run directly.
# ==============================================================================

"""Thin Prefect workflows over the existing ShapeFM coordinator boundaries."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import platform
import subprocess
import tempfile
import time
from contextlib import contextmanager, nullcontext
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, Iterator


# Machine environment: keep persistent Prefect operational SQLite state on the
# Mac coordinator in one ignored repository-local directory unless explicitly set.
ROOT = Path(__file__).resolve().parents[3]
os.environ.setdefault("PREFECT_HOME", str(ROOT / ".prefect"))
os.environ.setdefault("PREFECT_SERVER_ANALYTICS_ENABLED", "false")
os.environ.setdefault("PREFECT_CLOUD_ENABLE_ORCHESTRATION_TELEMETRY", "false")
os.environ.setdefault("PREFECT_TELEMETRY_ENABLE_RESOURCE_METRICS", "false")

import dask
from prefect import flow, task
from prefect.context import get_run_context
from prefect.futures import as_completed
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.tasks import NO_CACHE
from prefect_dask import DaskTaskRunner


GateRunner = Callable[[int], dict[str, Any]]
GateInspector = Callable[[int], dict[str, Any]]
GateValidator = Callable[[int, dict[str, Any]], dict[str, Any]]
PreparationRunner = Callable[[], dict[str, Any]]
PreparationInspector = Callable[[], dict[str, Any]]
PreparationValidator = Callable[[dict[str, Any]], dict[str, Any]]
IdentityRecorder = Callable[[str], None]


def _local_worker() -> dict[str, Any]:
    """Describe coordinator-local compute without exposing storage state."""
    return {
        "execution_backend": "Prefect local compute task",
        "hostname": platform.node(),
        "retry_count": get_run_context().task_run.run_count - 1,
    }


@task(name="Gate 02 clean batch", persist_result=False, cache_policy=NO_CACHE)
def compute_clean_batch(
    batch: list[dict[str, Any]], options: dict[str, Any], distributed: bool
) -> dict[str, Any]:
    """Clean one serializable batch locally or on the selected Dask worker."""
    from .shared_distributed_execution import clean_batch

    return clean_batch(
        batch, options["script"], options["timeout"], options["threads"],
        get_run_context().task_run.run_count - 1,
    )


@task(name="Gate 03 transform batch", persist_result=False, cache_policy=NO_CACHE)
def compute_transform_batch(
    batch: list[dict[str, Any]], options: dict[str, Any], distributed: bool
) -> dict[str, Any]:
    """Transform one serializable batch in a named Prefect compute task."""
    from .shared_distributed_execution import transform_batch, window_preparation_batch

    if options.get("bounded_preparation"):
        return window_preparation_batch(
            batch,
            options["script"],
            options["timeout"],
            options["threads"],
            options.get("memory_safety"),
            get_run_context().task_run.run_count - 1,
        )

    return transform_batch(batch, get_run_context().task_run.run_count - 1)


@task(name="Gate 04 directional DTW batch", persist_result=False, cache_policy=NO_CACHE)
def compute_directional_dtw_batch(
    batch: list[dict[str, Any]], options: dict[str, Any], distributed: bool
) -> dict[str, Any]:
    """Run one directional DTW block in the isolated classifier environment."""
    from .shared_distributed_execution import (
        directional_dtw_batch,
        mantis_representation_batch,
        random_forest_classification_batch,
    )

    if options.get("component") == "mantis_representation":
        return mantis_representation_batch(
            batch,
            options["mantis_environment"],
            options["worker_script"],
            options["device"],
            options["timeout"],
            get_run_context().task_run.run_count - 1,
        )
    if options.get("component") == "random_forest_classifier":
        return random_forest_classification_batch(
            batch,
            options["classifier_environment"],
            options["worker_script"],
            options["dataset_fingerprint"],
            options["operation"],
            options["model_storage"],
            options["frequency"],
            options["timeout"],
            get_run_context().task_run.run_count - 1,
        )

    return directional_dtw_batch(
        batch,
        options["operation"],
        options["classifier_environment"],
        options["worker_script"],
        options["reference_fingerprint"],
        options.get("model_storage"),
        options.get("frequency"),
        options["timeout"],
        options["memory_min_available_gib"],
        options["swap_growth_limit_gib"],
        get_run_context().task_run.run_count - 1,
    )


@task(name="Gate 05 combine batch", persist_result=False, cache_policy=NO_CACHE)
def compute_combine_batch(
    batch: list[dict[str, Any]], _options: dict[str, Any], distributed: bool
) -> dict[str, Any]:
    """Combine one forecast batch in a named Prefect compute task."""
    from .shared_distributed_execution import combine_batch

    return combine_batch(batch, get_run_context().task_run.run_count - 1)


@task(name="Gate 06 official evaluator", persist_result=False, cache_policy=NO_CACHE)
def compute_evaluation(
    batch: list[dict[str, Any]], options: dict[str, Any], _distributed: bool
) -> dict[str, Any]:
    """Run one official evaluator payload on the Mac without DuckDB access."""
    item = batch[0]
    started = time.monotonic()
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as stream:
        json.dump(item["payload"], stream)
        payload_path = Path(stream.name)
    try:
        completed = subprocess.run(
            [options["python"], options["bridge"], "evaluate", "--source-root",
             options["source_root"], "--payload", str(payload_path)],
            cwd=ROOT, check=True, capture_output=True, text=True,
            timeout=options["timeout"],
        )
    finally:
        payload_path.unlink(missing_ok=True)
    return {"results": [{"id": item["task_id"],
                          "official": json.loads(completed.stdout)}],
            "runtime_seconds": time.monotonic() - started,
            "worker": _local_worker()}


COMPUTE_TASKS = {
    2: compute_clean_batch,
    3: compute_transform_batch,
    4: compute_directional_dtw_batch,
    5: compute_combine_batch,
    6: compute_evaluation,
}


@flow(name="ShapeFM bounded gate compute", persist_result=False)
def gate_compute_flow(
    process_id: int, payload_path: str, options: dict[str, Any],
    distributed: bool, retries: int, max_in_flight: int,
) -> Iterator[dict[str, Any]]:
    """Yield completion-order outcomes for immediate Mac commits; drain on failure."""
    if process_id not in COMPUTE_TASKS or max_in_flight < 1 or retries < 0:
        raise ValueError("invalid gate compute controls")
    selected = COMPUTE_TASKS[process_id].with_options(retries=retries)
    pending = {}
    # Only this Mac flow reads the temporary payload. Dask tasks receive their
    # bounded batch, never this coordinator-local path or a database connection.
    with Path(payload_path).open() as source:
        remaining = iter(json.load(source))
    exhausted = False
    failed = False
    while pending or not exhausted:
        while not failed and len(pending) < max_in_flight and not exhausted:
            try:
                batch = next(remaining)
            except StopIteration:
                exhausted = True
                break
            resources = (
                {"CHRONOS_GPU_SLOT": 1}
                if options.get("component") == "mantis_representation"
                else {"CPU": 1}
            )
            annotation = dask.annotate(resources=resources, retries=0) if distributed else nullcontext()
            try:
                with annotation:
                    pending[selected.submit(batch, options, distributed)] = batch
            except Exception as exc:
                failed = True
                yield {"batch": batch, "error": f"{type(exc).__name__}: {exc}"}
        if pending:
            future = next(as_completed(list(pending)))
            batch = pending.pop(future)
            try:
                response = future.result()
            except Exception as exc:
                failed = True
                yield {"batch": batch, "error": f"{type(exc).__name__}: {exc}"}
            else:
                yield {"batch": batch, "response": response}
        elif failed:
            break


def run_gate_compute_flow(
    *, process_id: int, batches: list[list[dict[str, Any]]],
    options: dict[str, Any], scheduler_address: str | None, retries: int,
    max_in_flight: int, local_workers: int,
) -> Iterator[dict[str, Any]]:
    """Bind eligible gates to existing Dask or explicit bounded local task workers."""
    distributed = scheduler_address is not None and process_id in {2, 3, 4, 5}
    if process_id in {2, 3, 4, 5} and scheduler_address is not None:
        selected = gate_compute_flow.with_options(
            task_runner=DaskTaskRunner(address=scheduler_address)
        )
    else:
        selected = gate_compute_flow.with_options(
            task_runner=ThreadPoolTaskRunner(max_workers=local_workers)
        )
    # Prefect persists flow parameters even with result persistence disabled.
    # Keep scientific arrays out of its API and parameter-size limit. This file
    # lives only for this generator; DuckDB remains the durable source on resume.
    with tempfile.TemporaryDirectory(prefix="shapefm-gate-") as directory:
        payload = Path(directory) / "batches.json"
        payload.write_text(json.dumps(batches), encoding="utf-8")
        yield from selected(process_id, str(payload), options, distributed, retries, max_in_flight)


@contextmanager
def research_writer_locks(database_paths: Sequence[Path]) -> Iterator[None]:
    """Refuse overlapping Mac writer workflows for any selected DuckDB path."""
    lock_directory = ROOT / ".prefect/writer-locks"
    lock_directory.mkdir(parents=True, exist_ok=True)
    handles = []
    resolved_paths = sorted({path.resolve() for path in database_paths}, key=str)
    try:
        for database_path in resolved_paths:
            identity = hashlib.sha256(str(database_path).encode("utf-8")).hexdigest()
            handle = (lock_directory / f"{identity}.lock").open("a+", encoding="utf-8")
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                handle.close()
                raise RuntimeError(
                    f"another writer workflow is active for {database_path}"
                ) from exc
            handle.seek(0)
            handle.truncate()
            handle.write(f"pid={os.getpid()} database={database_path}\n")
            handle.flush()
            handles.append(handle)
        yield
    finally:
        for handle in reversed(handles):
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()


def _flow_run_id() -> str:
    """Return the current Prefect flow-run identity as a stable string."""
    return str(get_run_context().flow_run.id)


def _task_run_id() -> str:
    """Return the current Prefect task-run identity as a stable string."""
    return str(get_run_context().task_run.id)


@task(
    name="record research workflow identity",
    persist_result=False,
    cache_policy=NO_CACHE,
)
def record_workflow_identity(
    prefect_flow_run_id: str, recorder: IdentityRecorder | None
) -> None:
    """Link a Prefect flow run to its DuckDB execution when a recorder is supplied."""
    if recorder is not None:
        recorder(prefect_flow_run_id)


@task(
    name="inspect gate inputs and prerequisite",
    persist_result=False,
    cache_policy=NO_CACHE,
)
def inspect_gate(process_id: int, inspector: GateInspector) -> dict[str, Any]:
    """Read authoritative DuckDB state before a gate and attach task identity."""
    return {**inspector(process_id), "prefect_task_run_id": _task_run_id()}


@task(
    name="execute ShapeFM gate",
    persist_result=False,
    cache_policy=NO_CACHE,
)
def execute_gate(
    process_id: int,
    inspected: dict[str, Any],
    runner: GateRunner,
) -> dict[str, Any]:
    """Run one numbered gate after its authoritative input inspection."""
    if inspected.get("process_id") != process_id:
        raise RuntimeError("gate inspection returned a mismatched process identity")
    result = runner(process_id)
    return {**result, "prefect_task_run_id": _task_run_id()}


@task(
    name="validate committed gate output",
    persist_result=False,
    cache_policy=NO_CACHE,
)
def validate_gate(
    process_id: int,
    result: dict[str, Any],
    validator: GateValidator,
) -> dict[str, Any]:
    """Validate a gate's committed DuckDB state and attach task identity."""
    return {
        **validator(process_id, result),
        "prefect_task_run_id": _task_run_id(),
    }


@flow(name="ShapeFM gate", persist_result=False)
def gate_flow(
    process_id: int,
    inspector: GateInspector,
    runner: GateRunner,
    validator: GateValidator,
    retries: int,
) -> dict[str, Any]:
    """Inspect, execute, and validate one restartable gate as a named child flow."""
    if process_id not in range(1, 7):
        raise ValueError("ShapeFM process_id must be within 1-6")
    if retries < 0:
        raise ValueError("Prefect gate retries cannot be negative")
    inspected = inspect_gate.with_options(name=f"Inspect Process {process_id:02d}")(
        process_id, inspector
    )
    result = execute_gate.with_options(
        # Gate compute tasks own retries; repeating a writer gate would duplicate
        # invocation/attempt bookkeeping after partially committed results.
        name=f"Process {process_id:02d}", retries=0 if process_id in {2, 3, 4, 5, 6} else retries
    )(process_id, inspected, runner)
    validated = validate_gate.with_options(name=f"Validate Process {process_id:02d}")(
        process_id, result, validator
    )
    return {
        **result,
        "prefect_gate_flow_run_id": _flow_run_id(),
        "prefect_substeps": {
            "inspect_task_run_id": inspected["prefect_task_run_id"],
            "execute_task_run_id": result["prefect_task_run_id"],
            "validate_task_run_id": validated["prefect_task_run_id"],
        },
        "validation": {
            key: value
            for key, value in validated.items()
            if key != "prefect_task_run_id"
        },
    }


@flow(name="ShapeFM experiment", persist_result=False)
def experiment_flow(
    processes: Sequence[int],
    completed_processes: Sequence[int],
    inspector: GateInspector,
    runner: GateRunner,
    validator: GateValidator,
    retries: int,
    recorder: IdentityRecorder | None = None,
) -> dict[str, Any]:
    """Run selected gates in dependency order and collect every child result."""
    selected = tuple(processes)
    if not selected or tuple(sorted(set(selected))) != selected:
        raise ValueError("ShapeFM processes must be unique and in ascending order")
    if any(process_id not in range(1, 7) for process_id in selected):
        raise ValueError("ShapeFM processes must be within 1-6")
    prefect_flow_run_id = _flow_run_id()
    record_workflow_identity(prefect_flow_run_id, recorder)
    completed = set(completed_processes)
    summaries: list[dict[str, Any]] = []
    for process_id in selected:
        if process_id in completed:
            inspected = inspect_gate.with_options(
                name=f"Inspect completed Process {process_id:02d}"
            )(process_id, inspector)
            summaries.append(
                {
                    "process_id": process_id,
                    "status": "skipped_completed",
                    "prefect_substeps": {
                        "inspect_task_run_id": inspected["prefect_task_run_id"]
                    },
                    "validation": {
                        key: value
                        for key, value in inspected.items()
                        if key not in {"prefect_task_run_id", "status_before"}
                    },
                }
            )
            continue
        summary = gate_flow.with_options(name=f"ShapeFM Process {process_id:02d}")(
            process_id, inspector, runner, validator, retries
        )
        summaries.append(summary)
    return {
        "prefect_flow_run_id": prefect_flow_run_id,
        "processes": summaries,
    }


@task(
    name="prepare and persist rolling windows",
    persist_result=False,
    cache_policy=NO_CACHE,
)
def execute_window_preparation(runner: PreparationRunner) -> dict[str, Any]:
    """Run existing bounded preparation and attach its Prefect task identity."""
    return {**runner(), "prefect_task_run_id": _task_run_id()}


@task(
    name="inspect rolling-window inputs",
    persist_result=False,
    cache_policy=NO_CACHE,
)
def inspect_window_preparation(inspector: PreparationInspector) -> dict[str, Any]:
    """Validate parent/child preparation inputs and attach task identity."""
    return {**inspector(), "prefect_task_run_id": _task_run_id()}


@task(
    name="validate persisted rolling windows",
    persist_result=False,
    cache_policy=NO_CACHE,
)
def validate_window_preparation(
    result: dict[str, Any], validator: PreparationValidator
) -> dict[str, Any]:
    """Validate persisted parent/child preparation output and attach task identity."""
    return {**validator(result), "prefect_task_run_id": _task_run_id()}


@flow(name="ShapeFM rolling-window preparation", persist_result=False)
def window_preparation_flow(
    inspector: PreparationInspector,
    runner: PreparationRunner,
    validator: PreparationValidator,
    retries: int,
) -> dict[str, Any]:
    """Inspect, execute, and validate the optional rolling-window/S1 workflow."""
    if retries < 0:
        raise ValueError("Prefect preparation retries cannot be negative")
    inspected = inspect_window_preparation(inspector)
    # Native compute tasks own retries; never repeat the enclosing writer task.
    result = execute_window_preparation.with_options(retries=0)(runner)
    validated = validate_window_preparation(result, validator)
    return {
        **result,
        "prefect_flow_run_id": _flow_run_id(),
        "prefect_substeps": {
            "inspect_task_run_id": inspected["prefect_task_run_id"],
            "execute_task_run_id": result["prefect_task_run_id"],
            "validate_task_run_id": validated["prefect_task_run_id"],
        },
        "validation": {
            key: value
            for key, value in validated.items()
            if key != "prefect_task_run_id"
        },
    }
