#!/usr/bin/env python3
# ==============================================================================
# 00_main.py
#
# Purpose: Single researcher entry point for ShapeFM.
# Inputs: A plan, run, status, results, or test subcommand plus configuration/database selectors.
# Outputs: JSON planning, execution, status, result, or acceptance records on stdout.
# Run from: .tools/uv/uv run --locked --no-sync python src/python/00_main.py <plan|run|status|results|test> [options]
# ==============================================================================

"""Single researcher entry point for ShapeFM."""

from __future__ import annotations

import argparse
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


# ROOT: repository root resolved from this source file.
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/python"))

from util.configuration import PROCESS_NAMES, canonical_json
from util.database import initialize_experiment_database, load_database_configuration
from util.experiment_execution import (
    POC1Coordinator,
    configuration_status,
    experiment_status,
    get_forecast,
    latest_experiment_id,
    official_results,
)
from util.import_execution import ImportCoordinator
from tests.acceptance import run_acceptance


# DEFAULT_DATABASE: repository-relative default path used when the caller supplies no override.
DEFAULT_DATABASE = ROOT / "results/poc2_acceptance.duckdb"
# DEFAULT_PLAN_DATABASE: repository-relative default path used when the caller supplies no override.
DEFAULT_PLAN_DATABASE = ROOT / "results/poc2_local_plan.duckdb"
# DEFAULT_REPORT: repository-relative default path used when the caller supplies no override.
DEFAULT_REPORT = ROOT / "results/poc2_acceptance_report.json"
# DEFAULT_CONFIGURATION: complete researcher-authored definition used only before database creation.
DEFAULT_CONFIGURATION = ROOT / "config/experiments/poc2_m4_daily_100.json"


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
    """Parse one process number or inclusive range and require ordered Processes 01–06."""
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
    """Capture CLI arguments, Git state, host identity, and the resolved database for result provenance."""
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
    """Define the researcher-facing plan, status, results, and acceptance-test CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action")

    plan = subparsers.add_parser("plan", help="dry-plan a deterministic M4 Daily subset")
    plan.add_argument("--configuration", type=Path, default=DEFAULT_CONFIGURATION)

    run = subparsers.add_parser("run", help="create or resume configured Processes 01-06")
    run.add_argument("--database", type=Path, required=True)
    run.add_argument("--configuration", type=Path)
    run.add_argument("--processes", type=process_selection, default=tuple(PROCESS_NAMES))

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
    """Transactionally update one coordinator-owned process state row."""
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


def run_configured_processes(
    database: Path, configuration_path: Path | None, processes: tuple[int, ...]
) -> dict[str, Any]:
    """Create or resume one experiment database and execute only selected incomplete processes."""
    if database.exists():
        if configuration_path is not None:
            raise ValueError("--configuration is only valid when creating a new database")
    else:
        if configuration_path is None:
            raise ValueError("--configuration is required when creating a new database")
        initialize_experiment_database(database, configuration_path)
    configuration = load_database_configuration(database)
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
        operational = {
            "requested_processes": list(processes),
            "execution": configuration.resolved["execution"]["default"],
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
            if process_id == 1:
                with ImportCoordinator(database) as coordinator:
                    result = coordinator.import_configured()
            elif process_id == 2:
                with POC1Coordinator(database) as coordinator:
                    plan = coordinator.plan()
                    result = coordinator.run_gate(plan.experiment_id, 2)
            else:
                experiment_id = latest_experiment_id(database)
                with POC1Coordinator(database) as coordinator:
                    result = coordinator.run_gate(experiment_id, process_id)
                    if process_id == 6:
                        coordinator.connection.execute(
                            "UPDATE experiments SET status='completed', updated_at=current_timestamp WHERE experiment_id=?",
                            [experiment_id],
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


def main(argv: Sequence[str] | None = None) -> int:
    """Dispatch one CLI action, print its JSON record, and return zero on success or one on an expected failure."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.action is None:
        parser.print_help()
        return 0
    database = getattr(args, "database", DEFAULT_PLAN_DATABASE).resolve()
    invocation = invocation_record(args.action, args, database)
    try:
        if args.action == "results":
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
                with POC1Coordinator(temporary) as coordinator:
                    result = coordinator.plan(dry_run=True)
            output = {"invocation": invocation, "plan": result}
        elif args.action == "run":
            output = {
                "invocation": invocation,
                "execution": run_configured_processes(
                    database,
                    args.configuration.resolve() if args.configuration else None,
                    args.processes,
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
