# ==============================================================================
# workflow_orchestration.py
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
import os
from contextlib import contextmanager
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

from prefect import flow, task
from prefect.context import get_run_context
from prefect.tasks import NO_CACHE


GateRunner = Callable[[int], dict[str, Any]]
GateInspector = Callable[[int], dict[str, Any]]
GateValidator = Callable[[int, dict[str, Any]], dict[str, Any]]
PreparationRunner = Callable[[], dict[str, Any]]
PreparationInspector = Callable[[], dict[str, Any]]
PreparationValidator = Callable[[dict[str, Any]], dict[str, Any]]
IdentityRecorder = Callable[[str], None]


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
        name=f"Process {process_id:02d}", retries=0 if process_id == 4 else retries
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
    result = execute_window_preparation.with_options(retries=retries)(runner)
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
