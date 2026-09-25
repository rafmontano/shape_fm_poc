"""Researcher-facing POC 1 command line interface."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from .database import DEFAULT_DATABASE
from .poc1 import (
    ExperimentPlan,
    POC1Coordinator,
    experiment_status,
    get_forecast,
    latest_experiment_id,
    official_results,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan_parser = subparsers.add_parser("plan")
    plan_parser.add_argument("--scope", choices=["smoke", "m4_daily", "manifest"], default="smoke")
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--experiment-id")
    run_parser.add_argument("--stage", type=int, choices=range(2, 7))
    run_parser.add_argument("--workers", type=int, default=1)
    run_parser.add_argument("--device", default="auto")
    run_parser.add_argument("--batch-size", type=int, default=8)
    status_parser = subparsers.add_parser("status")
    status_parser.add_argument("--experiment-id")
    results_parser = subparsers.add_parser("results")
    results_parser.add_argument("--experiment-id")
    export_parser = subparsers.add_parser("export")
    export_parser.add_argument("--experiment-id")
    export_parser.add_argument("--model-name", default="ShapeFM-POC1-provisional")
    get_parser = subparsers.add_parser("get-forecast")
    get_parser.add_argument("--experiment-id", required=True)
    get_parser.add_argument("--variant-id", required=True)
    get_parser.add_argument("--series-id", required=True)
    get_parser.add_argument("--candidate", required=True)
    args = parser.parse_args()
    if args.command == "get-forecast":
        print(json.dumps(asdict(get_forecast(args.database, args.experiment_id, args.variant_id, args.series_id, args.candidate))))
        return
    if args.command in {"status", "results"}:
        experiment_id = args.experiment_id or latest_experiment_id(args.database)
        result = (
            experiment_status(args.database, experiment_id)
            if args.command == "status"
            else official_results(args.database, experiment_id)
        )
        print(json.dumps(result, default=str))
        return
    selected_experiment = getattr(args, "experiment_id", None)
    if args.command in {"run", "export"} and selected_experiment is None:
        selected_experiment = latest_experiment_id(args.database)
    with POC1Coordinator(args.database) as coordinator:
        if args.command == "plan":
            value = coordinator.plan(args.scope)
            result = asdict(value) if isinstance(value, ExperimentPlan) else value
        elif args.command == "run":
            experiment_id = selected_experiment
            if args.stage:
                result = coordinator.run_gate(
                    experiment_id, args.stage, args.workers, args.device, args.batch_size
                )
            else:
                count = coordinator.connection.execute(
                    "SELECT count(*) FROM forecast_instances i JOIN experiments e USING (dataset_id) WHERE e.experiment_id=?",
                    [experiment_id],
                ).fetchone()[0]
                plan = ExperimentPlan(experiment_id, "stored", count, 4, {}, "m4_daily/D/short")
                result = coordinator.run_all(plan, args.workers, args.device, args.batch_size)
        else:
            result = coordinator.export_candidate(
                selected_experiment, args.model_name
            )
    print(json.dumps(result, default=str))


if __name__ == "__main__":
    main()
