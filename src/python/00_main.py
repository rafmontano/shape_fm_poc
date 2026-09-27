#!/usr/bin/env python3
# ==============================================================================
# 00_main.py
#
# Purpose: Single researcher entry point for ShapeFM.
# Inputs: A plan, status, results, or test subcommand and its database, report, experiment, forecast, or series selectors.
# Outputs: JSON plan/status/result/acceptance records on stdout; test also persists its DuckDB database and JSON report.
# Run from: .tools/uv/uv run --locked --no-sync python src/python/00_main.py <plan|status|results|test> [options]
# ==============================================================================

"""Single researcher entry point for ShapeFM."""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Sequence


# ROOT: repository root resolved from this source file.
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/python"))

from util.experiment_execution import (
    POC1Coordinator,
    experiment_status,
    get_forecast,
    latest_experiment_id,
    official_results,
)
from tests.acceptance import SERIES_LIMIT, run_acceptance


# DEFAULT_DATABASE: repository-relative default path used when the caller supplies no override.
DEFAULT_DATABASE = ROOT / "results/poc2_acceptance.duckdb"
# DEFAULT_PLAN_DATABASE: repository-relative default path used when the caller supplies no override.
DEFAULT_PLAN_DATABASE = ROOT / "results/poc2_local_plan.duckdb"
# DEFAULT_REPORT: repository-relative default path used when the caller supplies no override.
DEFAULT_REPORT = ROOT / "results/poc2_acceptance_report.json"


def positive_integer(value: str) -> int:
    """Parse an argparse value and reject zero, negative, and non-integer series limits."""
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


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
    plan.add_argument("--series-limit", type=positive_integer, default=SERIES_LIMIT)
    plan.add_argument("--database", type=Path, default=DEFAULT_PLAN_DATABASE)

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


def main(argv: Sequence[str] | None = None) -> int:
    """Dispatch one CLI action, print its JSON record, and return zero on success or one on an expected failure."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.action is None:
        parser.print_help()
        return 0
    database = args.database.resolve()
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
            if database == (ROOT / "data/shapefm.duckdb").resolve():
                raise ValueError("dry plan refuses to open the authoritative database for writing")
            with POC1Coordinator(database) as coordinator:
                result = coordinator.plan(
                    "m4_daily", dry_run=True, series_limit=args.series_limit
                )
            output = {"invocation": invocation, "plan": result}
        elif args.action == "status":
            experiment_id = args.experiment_id or latest_experiment_id(database)
            output = {
                "invocation": invocation,
                "status": experiment_status(database, experiment_id),
            }
        else:
            output = run_acceptance(ROOT, database, args.report, invocation)
        print(json.dumps(output, indent=2, sort_keys=True, default=str))
        return 0
    except (OSError, RuntimeError, ValueError, KeyError, subprocess.SubprocessError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
