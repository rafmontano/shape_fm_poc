# ==============================================================================
# researcher_actions.py
# Purpose: Dispatch validated researcher requests through their owning workflow.
# Inputs: ResearcherRequest, stored configuration and explicit operational overrides.
# Outputs: JSON-ready results or visible failures; append-only execution evidence.
# Execution: Mac coordinator; writer locks surround mutation, read-only actions stay local.
# Run from: Imported; not run directly.
# Authority: Central configuration/profile and existing Prefect/Dask infrastructure.
# ==============================================================================

"""Cohesive researcher action handlers behind the single CLI entry point."""

from __future__ import annotations

import importlib.util
import platform
import subprocess
import tempfile
import uuid
from dataclasses import asdict
from functools import partial
from pathlib import Path
from typing import Any

import duckdb

from tests.acceptance import run_acceptance
from .configuration import PROCESS_NAMES
from .database import initialize_experiment_database, load_database_configuration
from .execution_event_storage import ExecutionEventStorage
from .execution_profiles import (APPROVED_HEAVY_TUNING_PROFILE, ExecutionSettings,
                                 resolve_execution_profile, validate_heavy_tuning_execution)
from .experiment_execution import (ExperimentCoordinator, configuration_status,
                                   experiment_status, get_forecast,
                                   latest_experiment_id, official_results)
from .process_storage import ProcessStorage
from .researcher_cli import ROOT
from .researcher_request import ResearcherRequest
from .window_preparation import WindowPreparationCoordinator, get_prepared_window
from .workflow_orchestration import experiment_flow, research_writer_locks, window_preparation_flow

# Interface constant: one numbered adapter per scientific gate, resolved from ROOT.
PROCESS_WRAPPER_PATHS = {
    number: ROOT / f"src/python/{name}"
    for number, name in {
        1: "01_import.py", 2: "02_preprocess.py", 3: "03_transform.py",
        4: "04_forecast.py", 5: "05_combine.py", 6: "06_evaluate.py",
    }.items()
}


class ProcessWrapperRegistry:
    """Load and validate explicit numbered process adapters."""

    def load(self, process_id: int) -> Any:
        """Load a numbered adapter and reject a missing or mismatched gate contract."""
        try:
            path = PROCESS_WRAPPER_PATHS[process_id]
        except KeyError as exc:
            raise ValueError(f"no wrapper is configured for Process {process_id:02d}") from exc
        if not path.is_file():
            raise FileNotFoundError(f"Process {process_id:02d} wrapper does not exist: {path}")
        specification = importlib.util.spec_from_file_location(f"_shapefm_process_{process_id:02d}", path)
        if specification is None or specification.loader is None:
            raise RuntimeError(f"cannot load Process {process_id:02d} wrapper: {path}")
        module = importlib.util.module_from_spec(specification)
        specification.loader.exec_module(module)
        if getattr(module, "PROCESS_NUMBER", None) != process_id:
            raise RuntimeError(f"Process {process_id:02d} wrapper declares a mismatched process: {path}")
        if not callable(getattr(module, "run", None)):
            raise RuntimeError(f"Process {process_id:02d} wrapper has no callable run: {path}")
        return module


class ProcessAction:
    """Create/resume an experiment and coordinate durable gate execution."""

    def __init__(self, registry: ProcessWrapperRegistry | None = None):
        self.registry = registry or ProcessWrapperRegistry()

    def run(self, database: Path, configuration_path: Path | None, processes: tuple[int, ...],
            execution_profile: str | None = None,
            local_heavy_exception: str | None = None) -> dict[str, Any]:
        with research_writer_locks((database,)):
            return self._run(database, configuration_path, processes, execution_profile,
                             local_heavy_exception)

    def _run(self, database: Path, configuration_path: Path | None, processes: tuple[int, ...],
             execution_profile: str | None, local_heavy_exception: str | None) -> dict[str, Any]:
        """Execute selected gates and always release an acquired managed cluster."""
        if database.exists():
            if configuration_path is not None:
                raise ValueError("--configuration is only valid when creating a new database")
        elif configuration_path is None:
            raise ValueError("--configuration is required when creating a new database")
        else:
            initialize_experiment_database(database, configuration_path)
        configuration = load_database_configuration(database)
        storage = ProcessStorage(database)
        execution, settings, cluster, cluster_evidence = self._execution(
            configuration, processes, execution_profile, local_heavy_exception,
            requires_gpu=storage.forecast_requires_gpu("chronos_2" in configuration.resolved["models"])
            if execution_profile and processes == (4,) else None,
        )
        try:
            events = ExecutionEventStorage(database)
            states = {number: status for number, status, _ in storage.process_rows()}
            for process_id in processes:
                missing = [
                    prior
                    for prior in range(1, process_id)
                    if states[prior] != "completed" and prior not in processes
                ]
                if missing:
                    raise RuntimeError(
                        f"Process {process_id:02d} requires completed Processes "
                        + ", ".join(f"{value:02d}" for value in missing)
                    )
            execution_id = f"execution/{uuid.uuid4().hex}"
            operational_execution = (
                execution[0].to_dict()
                if execution is not None
                else configuration.resolved["execution"]["default"]
            )
            events.start(
                execution_id,
                processes,
                {
                    "requested_processes": list(processes),
                    "execution": operational_execution,
                    "execution_settings": settings.to_dict() if settings is not None else configuration.execution,
                    "execution_profile_override": execution_profile,
                    "managed_cluster": cluster_evidence,
                },
                self._repository_revision(),
                {
                    "node": platform.node(),
                    "system": platform.system(),
                    "machine": platform.machine(),
                },
            )
            workflow = experiment_flow(
                processes, tuple(key for key, value in states.items() if value == "completed"),
                partial(self._inspect, storage),
                partial(self._execute, storage, execution, settings),
                partial(self._validate, storage),
                retries=(
                    0
                    if execution is not None or processes == (4,)
                    else int(configuration.execution["dask_retries"])
                ),
                recorder=partial(events.record_prefect_identity, execution_id),
            )
            events.finish(
                execution_id,
                "completed",
                {"processes": workflow["processes"]},
            )
            return {
                "execution_id": execution_id,
                "configuration": {
                    "version": configuration.version, "name": configuration.name,
                    "date": configuration.date, "description": configuration.description,
                    "seed": configuration.seed, "scientific_hash": configuration.scientific_hash,
                    "configuration_integrity_hash": configuration.configuration_integrity_hash,
                    "source": "DuckDB",
                },
                "prefect_flow_run_id": workflow["prefect_flow_run_id"],
                "processes": workflow["processes"],
            }
        except BaseException as exc:
            if "events" in locals() and "execution_id" in locals():
                events.finish(
                    execution_id,
                    "failed",
                    {"processes": []},
                    f"{type(exc).__name__}: {exc}",
                )
            raise
        finally:
            if cluster is not None:
                cluster.stop()

    @staticmethod
    def _repository_revision() -> str:
        """Return the checked repository revision recorded with an execution."""
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        ).stdout.strip()

    def _execution(self, configuration: Any, processes: tuple[int, ...], profile_name: str | None,
                   exception: str | None, requires_gpu: bool | None = None) -> tuple[Any, Any, Any, Any]:
        """Resolve operational overrides; reject unsafe GPU policy before service startup."""
        heavy = 4 in processes and configuration.seasonal_period_tuning and configuration.seasonal_period_tuning["enabled"]
        if profile_name and exception:
            raise ValueError("choose an execution profile or a local-heavy exception, not both")
        if heavy and not profile_name and not exception:
            raise RuntimeError(f"heavy seasonal tuning requires --execution-profile {APPROVED_HEAVY_TUNING_PROFILE}; local execution requires a recorded approval")
        if (profile_name or exception) and processes != (4,):
            raise ValueError("an execution override currently requires --processes 4")
        if not profile_name and not exception:
            return None, None, None, None
        selected = profile_name or "sequential_safe"
        profile, overrides = resolve_execution_profile(selected)
        if exception:
            execution = (profile, {"local_heavy_exception": {"approval_reference": exception,
                         "scope": "Gate 4 seasonal-period tuning"},
                         "execution_profile_fingerprint": profile.fingerprint})
            return execution, ExecutionSettings(mode="sequential", dask_max_in_flight=1,
                dask_retries=0, dask_timeout_seconds=float(configuration.execution["dask_timeout_seconds"])), None, None
        from .distributed_cluster import ManagedTuningCluster
        if requires_gpu is None:
            requires_gpu = "chronos_2" in configuration.resolved["models"]
        topology = profile.distributed_topology(requires_gpu)
        guard_settings = ExecutionSettings(mode="dask", dask_scheduler_address="managed",
            dask_expected_workers=topology["total_workers"],
            dask_expected_gpu_workers=topology["ubuntu_gpu_workers"],
            dask_max_in_flight=int(profile.dask_max_in_flight or 1), dask_retries=0)
        validate_heavy_tuning_execution(profile, guard_settings, overrides)
        cluster = ManagedTuningCluster(profile, requires_gpu=requires_gpu)
        try:
            evidence = cluster.start()
        except BaseException:
            cluster.stop()
            raise
        execution = (profile, {**overrides, "operational_profile_override": profile_name,
                               "execution_profile_fingerprint": profile.fingerprint,
                               "managed_cluster": evidence})
        settings = ExecutionSettings(mode="dask", dask_scheduler_address=cluster.scheduler_address,
            dask_timeout_seconds=float(configuration.execution["dask_timeout_seconds"]),
            dask_expected_workers=topology["total_workers"],
            dask_expected_gpu_workers=topology["ubuntu_gpu_workers"],
            dask_max_in_flight=int(profile.dask_max_in_flight or 1), dask_retries=0)
        return execution, settings, cluster, evidence

    def _inspect(self, storage: ProcessStorage, process_id: int) -> dict[str, Any]:
        """Revalidate every required predecessor and invalidate corrupt gate state."""
        rows = storage.process_rows()
        current = rows[process_id - 1]
        validated = {}
        for number, status, summary in rows[:process_id]:
            if number < process_id and status != "completed":
                raise RuntimeError(f"Process {number} must be completed before Process {process_id}")
            if status == "completed":
                try:
                    checked = storage.validate(number, summary)
                except BaseException as exc:
                    storage.transition(number, "failed", error=f"{type(exc).__name__}: {exc}")
                    raise
                if number == process_id:
                    validated = checked
        return {"process_id": process_id, "status_before": current[1], **validated}

    def _execute(self, storage: ProcessStorage, execution: Any, settings: Any,
                 process_id: int) -> dict[str, Any]:
        storage.transition(process_id, "running")
        try:
            wrapper = self.registry.load(process_id)
            summary = wrapper.run(storage.database, execution=execution, execution_settings=settings) if execution is not None else wrapper.run(storage.database)
            storage.validate(process_id, summary)
            storage.transition(process_id, "completed", summary)
            return {"process_id": process_id, "status": "completed", "summary": summary}
        except BaseException as exc:
            storage.transition(process_id, "failed", error=f"{type(exc).__name__}: {exc}")
            raise

    @staticmethod
    def _validate(storage: ProcessStorage, process_id: int, result: dict[str, Any]) -> dict[str, Any]:
        if result.get("process_id") != process_id or result.get("status") != "completed":
            raise RuntimeError("gate returned a mismatched or incomplete result")
        rows = storage.process_rows()
        try:
            validation = storage.validate(process_id, rows[process_id - 1][2])
        except BaseException as exc:
            storage.transition(process_id, "failed", error=f"{type(exc).__name__}: {exc}")
            raise
        return {"process_id": process_id, "committed_status": rows[process_id - 1][1], **validation}

class WindowPreparationAction:
    """Compose guarded rolling-window inspection, execution, and validation."""

    def run(self, database: Path, windows_database: Path, execution_profile: str | None = None,
            local_max_series: int | None = None, local_max_windows: int | None = None) -> dict[str, Any]:
        """Run optional preparation through its Prefect child-validation path."""
        bounds = (local_max_series, local_max_windows)
        if execution_profile is None and any(value is None for value in bounds):
            raise RuntimeError("local window preparation requires both --local-max-series and --local-max-windows; heavy preparation requires --execution-profile " + APPROVED_HEAVY_TUNING_PROFILE)
        if execution_profile is not None:
            if any(value is not None for value in bounds):
                raise ValueError("local preparation bounds cannot be combined with an execution profile")
            if execution_profile != APPROVED_HEAVY_TUNING_PROFILE:
                raise ValueError("distributed window preparation requires execution profile " + APPROVED_HEAVY_TUNING_PROFILE)
        from .distributed_execution import repository_source_manifest, source_manifest_fingerprint
        manifest_rows = repository_source_manifest()
        manifest = source_manifest_fingerprint(manifest_rows)
        with research_writer_locks((database, windows_database)):
            configuration = load_database_configuration(database)
            return window_preparation_flow(
                partial(self._inspect_preparation, database, windows_database),
                partial(
                    self._execute_preparation,
                    database,
                    windows_database,
                    execution_profile,
                    local_max_series,
                    local_max_windows,
                    manifest_rows,
                    manifest,
                ),
                partial(self._validate_preparation, database, windows_database),
                retries=int(configuration.execution["dask_retries"]),
            )

    @staticmethod
    def _inspect_preparation(database: Path, windows_database: Path) -> dict[str, Any]:
        """Validate parent identity and the completed, valid Gate 1 prerequisite."""
        if database.resolve() == windows_database.resolve():
            raise ValueError("windows database must differ from the parent database")
        process = configuration_status(database)["processes"][0]
        if process["status"] != "completed":
            raise RuntimeError("window preparation requires completed Process 01")
        ProcessStorage(database).validate(1, process["summary"])
        return {
            "parent_database": str(database.resolve()),
            "windows_database": str(windows_database.resolve()),
            "process_01_status": process["status"],
        }

    def _execute_preparation(
        self,
        database: Path,
        windows_database: Path,
        execution_profile: str | None,
        local_max_series: int | None,
        local_max_windows: int | None,
        manifest_rows: dict[str, str],
        manifest: str,
    ) -> dict[str, Any]:
        """Execute bounded local work or the retained approved distributed route."""
        if execution_profile is not None:
            return self._distributed(
                database,
                windows_database,
                execution_profile,
                manifest_rows,
                manifest,
            )
        with WindowPreparationCoordinator(database, windows_database) as coordinator:
            result = coordinator.run(
                source_manifest_hash=manifest,
                local_limits={
                    "max_series": int(local_max_series),
                    "max_windows": int(local_max_windows),
                },
                dask_retries=0,
            )
        return {
            **result,
            "execution_mode": "bounded_local_focused",
            "source_manifest": manifest,
        }

    @staticmethod
    def _validate_preparation(
        database: Path,
        windows_database: Path,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        """Validate child identity and count against both persisted databases."""
        parent = duckdb.connect(str(database.resolve()), read_only=True)
        child = duckdb.connect(str(windows_database.resolve()), read_only=True)
        try:
            parent_row = parent.execute(
                "SELECT status FROM window_preparation_runs WHERE preparation_id=?",
                [result["preparation_id"]],
            ).fetchone()
            metadata = child.execute(
                "SELECT preparation_id FROM preparation_metadata"
            ).fetchall()
            windows = child.execute("SELECT count(*) FROM prepared_windows").fetchone()[0]
        finally:
            child.close()
            parent.close()
        if parent_row != ("completed",) or metadata != [(result["preparation_id"],)]:
            raise RuntimeError("persisted window preparation identity is incomplete")
        if int(windows) != int(result["total_windows"]):
            raise RuntimeError("persisted window count does not match preparation result")
        return {
            "preparation_id": result["preparation_id"],
            "persisted_windows": int(windows),
            "output_validated": True,
        }

    @staticmethod
    def _distributed(database: Path, windows_database: Path, profile_name: str,
                     manifest_rows: dict[str, str], manifest: str) -> dict[str, Any]:
        """Run the retained approved-profile distributed preparation boundary."""
        from distributed import Client
        from .distributed_cluster import ManagedTuningCluster
        from .distributed_execution import package_version_probe, validate_tuning_cluster

        profile, _ = resolve_execution_profile(profile_name)
        cluster = ManagedTuningCluster(profile)
        try:
            evidence = cluster.start()
            client = Client(cluster.scheduler_address, timeout="180s")
            try:
                workers = validate_tuning_cluster(
                    client,
                    expected_workers=int(profile.dask_mac_cpu_workers or 0) + int(profile.dask_ubuntu_cpu_workers or 0),
                    expected_mac_workers=int(profile.dask_mac_cpu_workers or 0),
                    expected_ubuntu_workers=int(profile.dask_ubuntu_cpu_workers or 0),
                    expected_tuning_workers=int(profile.dask_mac_tuning_workers or 0) + int(profile.dask_ubuntu_tuning_workers or 0),
                    timeout=180, expected_manifest=manifest_rows,
                )
                versions = client.run(package_version_probe, "tsai")
                if set(versions.values()) != {"1.0.1"}:
                    raise RuntimeError(f"window workers do not share pinned tsai 1.0.1: {versions}")
                safety = {
                    "mac_hostname": platform.node(),
                    "mac_minimum_available_gib": profile.dask_mac_memory_min_available_gib,
                    "ubuntu_minimum_available_gib": profile.dask_ubuntu_memory_min_available_gib,
                    "fit_budget_gib": profile.dask_ets_fit_budget_gib,
                    "admission_timeout_seconds": profile.dask_memory_admission_timeout_seconds,
                    "poll_interval_seconds": profile.dask_memory_poll_interval_seconds,
                    "breach_grace_seconds": profile.dask_memory_breach_grace_seconds,
                    "swap_growth_limit_gib": profile.dask_swap_growth_limit_gib,
                }
                if any(value is None for value in safety.values()):
                    raise RuntimeError("approved profile lacks window memory-safety controls")
                with WindowPreparationCoordinator(database, windows_database) as coordinator:
                    result = coordinator.run(dask_client=client, source_manifest_hash=manifest,
                        memory_safety=safety, execution_profile=profile_name, dask_retries=0)
            finally:
                client.close()
        finally:
            cluster.stop()
        return {**result, "execution_mode": "distributed", "execution_profile": profile.to_dict(),
                "cluster": evidence, "worker_preflight": workers,
                "dependency_versions": versions, "source_manifest": manifest}


class ResearcherActions:
    """Dispatch typed requests to command-specific action methods."""

    def __init__(self, process_action: ProcessAction | None = None,
                 window_action: WindowPreparationAction | None = None):
        self.process_action = process_action or ProcessAction()
        self.window_action = window_action or WindowPreparationAction()

    def dispatch(self, request: ResearcherRequest, invocation: dict[str, Any]) -> dict[str, Any]:
        method = getattr(self, f"_{request.action.replace('-', '_')}")
        return method(request, invocation)

    def _plan(self, request: ResearcherRequest, invocation: dict[str, Any]) -> dict[str, Any]:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "plan.duckdb"
            initialize_experiment_database(database, request.value("configuration"))
            with ExperimentCoordinator(database) as coordinator:
                result = coordinator.plan(dry_run=True)
        return {"invocation": invocation, "plan": result}

    def _run(self, request: ResearcherRequest, invocation: dict[str, Any]) -> dict[str, Any]:
        configuration = request.value("configuration")
        return {"invocation": invocation, "execution": self.process_action.run(
            request.database, configuration.resolve() if configuration else None,
            request.value("processes"), request.value("execution_profile"),
            request.value("local_heavy_exception"))}

    def _prepare_windows(self, request: ResearcherRequest, invocation: dict[str, Any]) -> dict[str, Any]:
        if not request.database.is_file():
            raise FileNotFoundError(f"database does not exist: {request.database}")
        return {"invocation": invocation, "window_preparation": self.window_action.run(
            request.database, request.value("windows_database").resolve(),
            request.value("execution_profile"), request.value("local_max_series"),
            request.value("local_max_windows"))}

    def _status(self, request: ResearcherRequest, invocation: dict[str, Any]) -> dict[str, Any]:
        status = configuration_status(request.database)
        try:
            identity = request.value("experiment_id") or latest_experiment_id(request.database)
        except RuntimeError:
            identity = None
        if identity is not None:
            status["experiment"] = experiment_status(request.database, identity)
        return {"invocation": invocation, "status": status}

    def _results(self, request: ResearcherRequest, invocation: dict[str, Any]) -> dict[str, Any]:
        windows = (request.value("windows_database"), request.value("dataset_id"),
                   request.value("series_id"), request.value("window_ordinal"))
        if any(value is not None for value in (windows[0], windows[1], windows[3])):
            if not all(value is not None for value in windows):
                raise ValueError("--windows-database, --dataset-id, --series-id, and --window-ordinal must be supplied together")
            return {"invocation": invocation, "prepared_window": asdict(get_prepared_window(
                request.database, windows[0], dataset_id=windows[1], series_id=windows[2], window_ordinal=windows[3]))}
        selectors = (request.value("variant_id"), request.value("series_id"), request.value("candidate"))
        if any(selectors) and not all(selectors):
            raise ValueError("--variant-id, --series-id, and --candidate must be supplied together")
        if not request.database.is_file():
            raise FileNotFoundError(f"database does not exist: {request.database}")
        identity = request.value("experiment_id") or latest_experiment_id(request.database)
        if all(selectors):
            return {"invocation": invocation, "experiment_id": identity,
                    "forecast": asdict(get_forecast(request.database, identity, *selectors))}
        results = official_results(request.database, identity)
        if not results:
            raise RuntimeError(f"experiment not found or has no official evaluations: {identity}")
        return {"invocation": invocation, "experiment_id": identity, "results": results}

    @staticmethod
    def _test(request: ResearcherRequest, invocation: dict[str, Any]) -> dict[str, Any]:
        return run_acceptance(ROOT, request.database, request.value("report"), invocation)


def run_configured_processes(database: Path, configuration_path: Path | None,
                             processes: tuple[int, ...], execution_profile: str | None = None,
                             local_heavy_exception: str | None = None) -> dict[str, Any]:
    """Signature-compatible action boundary for migration callers."""
    return ProcessAction().run(database, configuration_path, processes, execution_profile,
                               local_heavy_exception)


def run_window_preparation(database: Path, windows_database: Path,
                           execution_profile: str | None = None,
                           local_max_series: int | None = None,
                           local_max_windows: int | None = None) -> dict[str, Any]:
    """Signature-compatible optional preparation action boundary."""
    return WindowPreparationAction().run(database, windows_database, execution_profile,
                                         local_max_series, local_max_windows)
