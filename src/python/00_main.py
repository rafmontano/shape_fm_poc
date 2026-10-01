#!/usr/bin/env python3
# ==============================================================================
# 00_main.py
#
# Purpose: Single researcher entry point for ShapeFM.
# Inputs: A plan, run, prepare-windows, status, results, or test subcommand and selectors.
# Outputs: JSON planning, execution, status, result, or acceptance records on stdout.
# Run from: .tools/uv/uv run --locked --no-sync python src/python/00_main.py <action> [options]
# ==============================================================================

"""Single researcher entry point for ShapeFM."""

from __future__ import annotations

import argparse
import importlib.util
import json
import platform
import subprocess
import sys
import tempfile
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any, Sequence

import duckdb


# Code constant: repository root derived from this source path; it has no override.
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/python"))

from util.configuration import PROCESS_NAMES, canonical_json
from util.database import initialize_experiment_database, load_database_configuration
from util.experiment_execution import (
    ExperimentCoordinator,
    configuration_status,
    experiment_status,
    get_forecast,
    latest_experiment_id,
    official_results,
)
from util.window_preparation import WindowPreparationCoordinator, get_prepared_window
from util.execution_profiles import (
    APPROVED_HEAVY_TUNING_PROFILE,
    ExecutionSettings,
    resolve_execution_profile,
)
from tests.acceptance import run_acceptance


# Code constant: explicit repository-controlled Process 01–06 wrapper paths. These
# modules are loaded in this Python process and are not alternative researcher CLIs.
PROCESS_WRAPPER_PATHS = {
    1: ROOT / "src/python/01_import.py",
    2: ROOT / "src/python/02_preprocess.py",
    3: ROOT / "src/python/03_transform.py",
    4: ROOT / "src/python/04_forecast.py",
    5: ROOT / "src/python/05_combine.py",
    6: ROOT / "src/python/06_evaluate.py",
}


# Bootstrap/interface default: acceptance database used only while locating DuckDB;
# ``--database`` overrides it, and the path is not part of scientific identity.
DEFAULT_DATABASE = ROOT / "results/poc2_acceptance.duckdb"
# Bootstrap/interface default: invocation label used before the plan creates its
# temporary DuckDB; it has no CLI override and is not part of scientific identity.
DEFAULT_PLAN_DATABASE = ROOT / "results/poc2_local_plan.duckdb"
# Bootstrap/interface default: acceptance-report destination before DuckDB is located;
# ``test --report`` overrides it, and it is not part of scientific identity.
DEFAULT_REPORT = ROOT / "results/poc2_acceptance_report.json"
# Bootstrap/interface default: creation JSON used before DuckDB becomes authoritative;
# ``plan|run --configuration`` overrides it, and the path itself is not scientific identity.
DEFAULT_CONFIGURATION = (
    ROOT / "config/experiments/poc2_m4_daily_100_resolved_period.json"
)


def positive_integer(value: str) -> int:
    """Parse an argparse value and reject zero, negative, and non-integer series limits."""
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def process_selection(value: str) -> tuple[int, ...]:
    """Purpose: Parse the CLI process selector and enforce the Processes 01–06 contract.

    Inputs: One argparse string containing either an integer or an inclusive ``start-end`` range.
    Outputs: An ordered tuple of process IDs, or ``ArgumentTypeError`` for malformed/out-of-range input.
    """
    try:
        if "-" in value:
            start_text, end_text = value.split("-", 1)
            start, end = int(start_text), int(end_text)
            selected = tuple(range(start, end + 1))
        else:
            selected = (int(value),)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be one process or an inclusive range, such as 1-3") from exc
    if not selected or any(process_id not in PROCESS_NAMES for process_id in selected):
        raise argparse.ArgumentTypeError("process selection must be within 1-6")
    return selected


def load_process_wrapper(process_id: int) -> Any:
    """Purpose: Load and validate one numbered process wrapper from its explicit path.

    Inputs: Process number 1–6 present in ``PROCESS_WRAPPER_PATHS``.
    Outputs: An in-process module whose ``PROCESS_NUMBER`` matches and whose ``run``
    attribute is callable; raises a clear error for missing or malformed wrappers.
    Notes: Uses only repository-controlled paths and does not start a subprocess or
    add aliases for numbered filenames.
    """
    try:
        path = PROCESS_WRAPPER_PATHS[process_id]
    except KeyError as exc:
        raise ValueError(f"no wrapper is configured for Process {process_id:02d}") from exc
    if not path.is_file():
        raise FileNotFoundError(f"Process {process_id:02d} wrapper does not exist: {path}")
    specification = importlib.util.spec_from_file_location(
        f"_shapefm_process_{process_id:02d}", path
    )
    if specification is None or specification.loader is None:
        raise RuntimeError(f"cannot load Process {process_id:02d} wrapper: {path}")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    declared_process = getattr(module, "PROCESS_NUMBER", None)
    if declared_process != process_id:
        raise RuntimeError(
            f"Process {process_id:02d} wrapper declares PROCESS_NUMBER={declared_process!r}: {path}"
        )
    if not callable(getattr(module, "run", None)):
        raise RuntimeError(f"Process {process_id:02d} wrapper has no callable run: {path}")
    return module


def _git(*arguments: str) -> str:
    """Return stripped stdout from a checked, repository-root Git command."""
    return subprocess.run(
        ["git", *arguments],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    ).stdout.strip()


def invocation_record(action: str, args: argparse.Namespace, database: Path) -> dict[str, Any]:
    """Purpose: Build the provenance record attached to researcher-facing CLI results.

    Inputs: The selected action, parsed argparse namespace, and resolved database path.
    Outputs: A JSON-ready mapping of arguments, command, Git state, host/Python environment, and database.
    Side effects: Runs read-only Git subprocesses against the repository working tree.
    """
    return {
        "action": action,
        "arguments": {
            key: str(value) if isinstance(value, Path) else value
            for key, value in vars(args).items()
            if key != "action"
        },
        "command": [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:]],
        "repository_root": str(ROOT),
        "repository_revision": _git("rev-parse", "HEAD"),
        "working_tree": _git("status", "--short", "--untracked-files=all").splitlines(),
        "environment": {
            "python": platform.python_version(),
            "python_executable": sys.executable,
            "system": platform.system(),
            "machine": platform.machine(),
            "node": platform.node(),
        },
        "database": str(database.resolve()),
    }


def build_parser() -> argparse.ArgumentParser:
    """Purpose: Define parsing and dispatch inputs for the researcher-facing CLI.

    Inputs: Defaults derived from repository paths plus later command-line arguments.
    Outputs: Parser for the six-process actions plus explicit window preparation.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action")

    plan = subparsers.add_parser("plan", help="dry-plan a deterministic M4 Daily subset")
    plan.add_argument("--configuration", type=Path, default=DEFAULT_CONFIGURATION)

    run = subparsers.add_parser("run", help="create or resume configured Processes 01-06")
    run.add_argument("--database", type=Path, required=True)
    run.add_argument("--configuration", type=Path)
    run.add_argument("--processes", type=process_selection, default=tuple(PROCESS_NAMES))
    run.add_argument(
        "--execution-profile",
        help="audited operational profile override for an existing experiment",
    )
    run.add_argument(
        "--local-heavy-exception",
        metavar="APPROVAL_REFERENCE",
        help="recorded researcher approval for a bounded local heavy run",
    )

    prepare_windows = subparsers.add_parser(
        "prepare-windows",
        help="create or resume the configured rolling-window child database",
    )
    prepare_windows.add_argument("--database", type=Path, required=True)
    prepare_windows.add_argument("--windows-database", type=Path, required=True)
    prepare_windows.add_argument(
        "--execution-profile",
        help="approved operational profile for distributed preparation",
    )
    prepare_windows.add_argument(
        "--local-max-series",
        type=positive_integer,
        help="explicit series bound required for a local focused preparation",
    )
    prepare_windows.add_argument(
        "--local-max-windows",
        type=positive_integer,
        help="explicit window bound required for a local focused preparation",
    )

    status = subparsers.add_parser("status", help="read acceptance experiment status")
    status.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    status.add_argument("--experiment-id")

    results = subparsers.add_parser(
        "results", help="read official evaluations or one stored forecast"
    )
    results.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    results.add_argument("--experiment-id")
    results.add_argument("--variant-id")
    results.add_argument("--series-id")
    results.add_argument("--candidate")
    results.add_argument("--windows-database", type=Path)
    results.add_argument("--dataset-id")
    results.add_argument("--window-ordinal", type=int)

    test = subparsers.add_parser("test", help="run or restart the 100-series acceptance case")
    test.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    test.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    return parser


def _set_process_state(
    database: Path,
    process_id: int,
    status: str,
    summary: dict[str, Any] | None = None,
    error: str | None = None,
) -> None:
    """Purpose: Persist a coordinator-owned process transition atomically.

    Inputs: DuckDB path, process ID, status, and optional summary/error payloads.
    Outputs: None; raises on transaction failure.
    Side effects: Opens DuckDB, updates timestamps/state, and commits or rolls back one transaction.
    """
    connection = duckdb.connect(str(database))
    try:
        connection.execute("BEGIN TRANSACTION")
        if status == "running":
            connection.execute(
                """UPDATE experiment_processes SET status='running',
                   started_at=current_timestamp, completed_at=NULL,
                   updated_at=current_timestamp, summary=NULL, last_error=NULL
                   WHERE process_id=?""",
                [process_id],
            )
        else:
            connection.execute(
                """UPDATE experiment_processes SET status=?, completed_at=current_timestamp,
                   updated_at=current_timestamp, summary=?, last_error=?
                   WHERE process_id=?""",
                [status, canonical_json(summary) if summary is not None else None, error, process_id],
            )
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()


def _recover_interrupted_run_records(
    connection: duckdb.DuckDBPyConnection, processes: tuple[int, ...]
) -> None:
    """Close stale parent records before recording a later sole-coordinator run.

    Task recovery is owned by ``ExperimentCoordinator._begin_invocation``. These
    parent rows cannot remain genuinely active because ShapeFM permits one Mac
    coordinator and callers check that no coordinator is alive before recovery.
    """
    interruption = "interrupted before completion; recovered by a later run"
    connection.execute(
        """UPDATE execution_events SET status='failed', completed_at=current_timestamp,
           error=? WHERE status='running'""",
        [interruption],
    )
    requested_gates = [PROCESS_NAMES[process_id] for process_id in processes]
    placeholders = ", ".join("?" for _ in requested_gates)
    connection.execute(
        f"""UPDATE experiment_invocations SET status='failed', ended_at=current_timestamp,
            error=? WHERE status='running' AND requested_gate IN ({placeholders})""",
        [interruption, *requested_gates],
    )


def run_configured_processes(
    database: Path,
    configuration_path: Path | None,
    processes: tuple[int, ...],
    execution_profile: str | None = None,
    local_heavy_exception: str | None = None,
) -> dict[str, Any]:
    """Purpose: Create or resume an experiment and dispatch selected Processes 01–06 in order.

    Inputs: A DuckDB path, an optional creation configuration path, and ordered process IDs.
    Outputs: Execution/configuration metadata and per-process completion or skip summaries.
    Side effects: Initializes or mutates DuckDB, invokes numbered wrappers in-process, and records execution state.
    """
    if database.exists():
        if configuration_path is not None:
            raise ValueError("--configuration is only valid when creating a new database")
    else:
        if configuration_path is None:
            raise ValueError("--configuration is required when creating a new database")
        initialize_experiment_database(database, configuration_path)
    configuration = load_database_configuration(database)
    heavy_tuning = (
        4 in processes
        and configuration.seasonal_period_tuning is not None
        and configuration.seasonal_period_tuning["enabled"]
    )
    if execution_profile is not None and local_heavy_exception is not None:
        raise ValueError("choose an execution profile or a local-heavy exception, not both")
    if heavy_tuning and execution_profile is None and local_heavy_exception is None:
        raise RuntimeError(
            "heavy seasonal tuning requires --execution-profile "
            f"{APPROVED_HEAVY_TUNING_PROFILE}; local execution requires a recorded approval"
        )
    execution = None
    execution_settings = None
    managed_cluster = None
    cluster_evidence = None
    if execution_profile is not None:
        if processes != (4,):
            raise ValueError(
                "an operational execution-profile override currently requires --processes 4"
            )
        profile, overrides = resolve_execution_profile(execution_profile)
        from util.distributed_cluster import ManagedTuningCluster

        managed_cluster = ManagedTuningCluster(profile)
        try:
            cluster_evidence = managed_cluster.start()
        except BaseException:
            managed_cluster.stop()
            raise
        execution = (
            profile,
            {
                **overrides,
                "operational_profile_override": execution_profile,
                "execution_profile_fingerprint": profile.fingerprint,
                "managed_cluster": cluster_evidence,
            },
        )
        execution_settings = ExecutionSettings(
            mode="dask",
            dask_scheduler_address=managed_cluster.scheduler_address,
            dask_timeout_seconds=float(configuration.execution["dask_timeout_seconds"]),
            dask_expected_workers=int(profile.dask_mac_cpu_workers or 0)
            + int(profile.dask_ubuntu_cpu_workers or 0),
            dask_expected_gpu_workers=1,
            dask_max_in_flight=int(profile.dask_max_in_flight or 1),
            dask_retries=int(configuration.execution["dask_retries"]),
        )
    elif local_heavy_exception is not None:
        if processes != (4,):
            raise ValueError("a local-heavy exception currently requires --processes 4")
        profile, _ = resolve_execution_profile("sequential_safe")
        execution = (
            profile,
            {
                "local_heavy_exception": {
                    "approval_reference": local_heavy_exception,
                    "scope": "Gate 4 seasonal-period tuning",
                },
                "execution_profile_fingerprint": profile.fingerprint,
            },
        )
        execution_settings = ExecutionSettings(
            mode="sequential",
            dask_timeout_seconds=float(configuration.execution["dask_timeout_seconds"]),
            dask_max_in_flight=1,
            dask_retries=int(configuration.execution["dask_retries"]),
        )
    connection = duckdb.connect(str(database))
    execution_id = f"execution/{uuid.uuid4().hex}"
    try:
        states = dict(
            connection.execute(
                "SELECT process_id, status FROM experiment_processes"
            ).fetchall()
        )
        for process_id in processes:
            incomplete_prerequisites = [
                prior
                for prior in range(1, process_id)
                if states[prior] != "completed" and prior not in processes
            ]
            if incomplete_prerequisites:
                raise RuntimeError(
                    f"Process {process_id:02d} requires completed Processes "
                    + ", ".join(f"{value:02d}" for value in incomplete_prerequisites)
                )
        _recover_interrupted_run_records(connection, processes)
        operational = {
            "requested_processes": list(processes),
            "execution": (
                execution[0].to_dict()
                if execution is not None
                else configuration.resolved["execution"]["default"]
            ),
            "execution_profile_override": execution_profile,
            "managed_cluster": cluster_evidence,
        }
        connection.execute(
            """INSERT INTO execution_events
            (execution_id, requested_processes, operational_configuration,
             repository_revision, machine, status)
            VALUES (?, ?, ?, ?, ?, 'running')""",
            [
                execution_id,
                canonical_json(list(processes)),
                canonical_json(operational),
                _git("rev-parse", "HEAD"),
                canonical_json({"node": platform.node(), "system": platform.system(), "machine": platform.machine()}),
            ],
        )
    finally:
        connection.close()

    summaries: list[dict[str, Any]] = []
    active_process: int | None = None
    try:
        for process_id in processes:
            state = configuration_status(database)["processes"][process_id - 1]["status"]
            if state == "completed":
                summaries.append({"process_id": process_id, "status": "skipped_completed"})
                continue
            active_process = process_id
            _set_process_state(database, process_id, "running")
            wrapper = load_process_wrapper(process_id)
            result = (
                wrapper.run(
                    database,
                    execution=execution,
                    execution_settings=execution_settings,
                )
                if execution is not None
                else wrapper.run(database)
            )
            _set_process_state(database, process_id, "completed", result)
            summaries.append({"process_id": process_id, "status": "completed", "summary": result})
        connection = duckdb.connect(str(database))
        try:
            connection.execute(
                """UPDATE execution_events SET status='completed', completed_at=current_timestamp,
                   summary=? WHERE execution_id=?""",
                [canonical_json({"processes": summaries}), execution_id],
            )
        finally:
            connection.close()
        return {
            "execution_id": execution_id,
            "configuration": {
                "version": configuration.version,
                "name": configuration.name,
                "date": configuration.date,
                "description": configuration.description,
                "seed": configuration.seed,
                "scientific_hash": configuration.scientific_hash,
                "configuration_integrity_hash": configuration.configuration_integrity_hash,
                "source": "DuckDB",
            },
            "processes": summaries,
        }
    except BaseException as exc:
        error = f"{type(exc).__name__}: {exc}"
        if active_process is not None:
            _set_process_state(database, active_process, "failed", error=error)
        connection = duckdb.connect(str(database))
        try:
            connection.execute(
                """UPDATE execution_events SET status='failed', completed_at=current_timestamp,
                   summary=?, error=? WHERE execution_id=?""",
                [canonical_json({"processes": summaries}), error, execution_id],
            )
        finally:
            connection.close()
        raise
    finally:
        if managed_cluster is not None:
            managed_cluster.stop()


def run_window_preparation(
    database: Path,
    windows_database: Path,
    execution_profile: str | None = None,
    local_max_series: int | None = None,
    local_max_windows: int | None = None,
) -> dict[str, Any]:
    """Purpose: Run the opt-in v5 preparation locally or on the approved CPU cluster.

    Inputs: Existing parent DuckDB, child destination, and optional named profile.
    Outputs: Persisted split/window summary and execution/source evidence.
    Side effects: Starts and stops only its managed Dask processes when requested;
    the Mac coordinator remains the sole writer to both DuckDB files.
    """
    from util.distributed_execution import (
        package_version_probe,
        repository_source_manifest,
        source_manifest_fingerprint,
        validate_tuning_cluster,
    )

    local_bounds = (local_max_series, local_max_windows)
    if execution_profile is None and any(value is None for value in local_bounds):
        raise RuntimeError(
            "local window preparation requires both --local-max-series and "
            "--local-max-windows; heavy preparation requires --execution-profile "
            f"{APPROVED_HEAVY_TUNING_PROFILE}"
        )
    if execution_profile is not None and any(value is not None for value in local_bounds):
        raise ValueError("local preparation bounds cannot be combined with an execution profile")
    manifest = repository_source_manifest()
    manifest_hash = source_manifest_fingerprint(manifest)
    if execution_profile is None:
        with WindowPreparationCoordinator(database, windows_database) as coordinator:
            result = coordinator.run(
                source_manifest_hash=manifest_hash,
                local_limits={
                    "max_series": int(local_max_series),
                    "max_windows": int(local_max_windows),
                },
            )
        return {
            **result,
            "execution_mode": "bounded_local_focused",
            "source_manifest": manifest_hash,
        }
    if execution_profile != APPROVED_HEAVY_TUNING_PROFILE:
        raise ValueError(
            "distributed window preparation requires execution profile "
            f"{APPROVED_HEAVY_TUNING_PROFILE}"
        )
    profile, _ = resolve_execution_profile(execution_profile)
    from distributed import Client
    from util.distributed_cluster import ManagedTuningCluster

    cluster = ManagedTuningCluster(profile)
    try:
        cluster_evidence = cluster.start()
        client = Client(cluster.scheduler_address, timeout="180s")
        try:
            worker_evidence = validate_tuning_cluster(
                client,
                expected_workers=int(profile.dask_mac_cpu_workers or 0)
                + int(profile.dask_ubuntu_cpu_workers or 0),
                expected_mac_workers=int(profile.dask_mac_cpu_workers or 0),
                expected_ubuntu_workers=int(profile.dask_ubuntu_cpu_workers or 0),
                expected_tuning_workers=int(profile.dask_mac_tuning_workers or 0)
                + int(profile.dask_ubuntu_tuning_workers or 0),
                timeout=180,
                expected_manifest=manifest,
            )
            dependency_versions = client.run(package_version_probe, "tsai")
            if set(dependency_versions.values()) != {"1.0.1"}:
                raise RuntimeError(
                    f"window workers do not share pinned tsai 1.0.1: {dependency_versions}"
                )
            memory_safety = {
                "mac_hostname": platform.node(),
                "mac_minimum_available_gib": profile.dask_mac_memory_min_available_gib,
                "ubuntu_minimum_available_gib": profile.dask_ubuntu_memory_min_available_gib,
                # Window cleaning is lighter than an ETS fit; retaining the
                # approved ETS budget is a conservative profile-owned bound.
                "fit_budget_gib": profile.dask_ets_fit_budget_gib,
                "admission_timeout_seconds": profile.dask_memory_admission_timeout_seconds,
                "poll_interval_seconds": profile.dask_memory_poll_interval_seconds,
                "breach_grace_seconds": profile.dask_memory_breach_grace_seconds,
                "swap_growth_limit_gib": profile.dask_swap_growth_limit_gib,
            }
            if any(value is None for value in memory_safety.values()):
                raise RuntimeError("approved profile lacks window memory-safety controls")
            with WindowPreparationCoordinator(database, windows_database) as coordinator:
                result = coordinator.run(
                    dask_client=client,
                    source_manifest_hash=manifest_hash,
                    memory_safety=memory_safety,
                )
        finally:
            client.close()
    finally:
        cluster.stop()
    return {
        **result,
        "execution_mode": "distributed",
        "execution_profile": profile.to_dict(),
        "cluster": cluster_evidence,
        "worker_preflight": worker_evidence,
        "dependency_versions": dependency_versions,
        "source_manifest": manifest_hash,
    }


def main(argv: Sequence[str] | None = None) -> int:
    """Purpose: Parse and dispatch one researcher CLI action.

    Inputs: Optional argument sequence; when absent argparse reads the process command line.
    Outputs: Pretty JSON on stdout and status 0 for help/success; an error on stderr and status 1 for expected failures.
    Side effects: Depending on the action, reads/writes DuckDB, creates a temporary plan database, or writes a test report.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.action is None:
        parser.print_help()
        return 0
    database = getattr(args, "database", DEFAULT_PLAN_DATABASE).resolve()
    invocation = invocation_record(args.action, args, database)
    try:
        if args.action == "results":
            window_selectors = (
                args.windows_database,
                args.dataset_id,
                args.series_id,
                args.window_ordinal,
            )
            window_route_requested = any(
                value is not None
                for value in (
                    args.windows_database,
                    args.dataset_id,
                    args.window_ordinal,
                )
            )
            if window_route_requested:
                if not all(value is not None for value in window_selectors):
                    raise ValueError(
                        "--windows-database, --dataset-id, --series-id, and "
                        "--window-ordinal must be supplied together"
                    )
                output = {
                    "invocation": invocation,
                    "prepared_window": asdict(
                        get_prepared_window(
                            database,
                            args.windows_database,
                            dataset_id=args.dataset_id,
                            series_id=args.series_id,
                            window_ordinal=args.window_ordinal,
                        )
                    ),
                }
                print(json.dumps(output, indent=2, sort_keys=True, default=str))
                return 0
            forecast_selectors = (args.variant_id, args.series_id, args.candidate)
            if any(forecast_selectors) and not all(forecast_selectors):
                raise ValueError(
                    "--variant-id, --series-id, and --candidate must be supplied together"
                )
            if not database.is_file():
                raise FileNotFoundError(f"database does not exist: {database}")
            experiment_id = args.experiment_id or latest_experiment_id(database)
            if all(forecast_selectors):
                output = {
                    "invocation": invocation,
                    "experiment_id": experiment_id,
                    "forecast": asdict(
                        get_forecast(
                            database,
                            experiment_id,
                            args.variant_id,
                            args.series_id,
                            args.candidate,
                        )
                    ),
                }
            else:
                evaluations = official_results(database, experiment_id)
                if not evaluations:
                    raise RuntimeError(
                        "experiment not found or has no official evaluations: "
                        f"{experiment_id}"
                    )
                output = {
                    "invocation": invocation,
                    "experiment_id": experiment_id,
                    "results": evaluations,
                }
        elif args.action == "plan":
            with tempfile.TemporaryDirectory() as directory:
                temporary = Path(directory) / "plan.duckdb"
                initialize_experiment_database(temporary, args.configuration)
                with ExperimentCoordinator(temporary) as coordinator:
                    result = coordinator.plan(dry_run=True)
            output = {"invocation": invocation, "plan": result}
        elif args.action == "run":
            output = {
                "invocation": invocation,
                "execution": run_configured_processes(
                    database,
                    args.configuration.resolve() if args.configuration else None,
                    args.processes,
                    args.execution_profile,
                    args.local_heavy_exception,
                ),
            }
        elif args.action == "prepare-windows":
            if not database.is_file():
                raise FileNotFoundError(f"database does not exist: {database}")
            output = {
                "invocation": invocation,
                "window_preparation": run_window_preparation(
                    database,
                    args.windows_database.resolve(),
                    args.execution_profile,
                    args.local_max_series,
                    args.local_max_windows,
                ),
            }
        elif args.action == "status":
            status = configuration_status(database)
            try:
                experiment_id = args.experiment_id or latest_experiment_id(database)
            except RuntimeError:
                experiment_id = None
            if experiment_id is not None:
                status["experiment"] = experiment_status(database, experiment_id)
            output = {"invocation": invocation, "status": status}
        else:
            output = run_acceptance(ROOT, database, args.report, invocation)
        print(json.dumps(output, indent=2, sort_keys=True, default=str))
        return 0
    except (OSError, RuntimeError, ValueError, KeyError, subprocess.SubprocessError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
