"""Researcher-facing POC 1 command line interface."""

from __future__ import annotations

import argparse
import json
import tempfile
from dataclasses import asdict
from pathlib import Path

from .calibration import calibrate, calibrate_dask_profile
from .database import DEFAULT_DATABASE
from .execution import ExecutionSettings, resolve_execution_profile
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
    plan_parser.add_argument("--dry-run", action="store_true")
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--experiment-id")
    run_parser.add_argument("--stage", type=int, choices=range(2, 7))
    run_parser.add_argument("--profile", default="sequential_safe")
    run_parser.add_argument("--workers", type=int)
    run_parser.add_argument("--device")
    run_parser.add_argument("--batch-size", type=int)
    run_parser.add_argument("--cleaning-workers", type=int)
    run_parser.add_argument("--transformation-workers", type=int)
    run_parser.add_argument("--autoarima-workers", type=int)
    run_parser.add_argument("--chronos-processes", type=int)
    run_parser.add_argument("--chronos-batch-size", type=int)
    run_parser.add_argument("--combination-workers", type=int)
    run_parser.add_argument("--evaluation-workers", type=int)
    run_parser.add_argument(
        "--execution", choices=["sequential", "local", "dask"], default="local"
    )
    run_parser.add_argument("--dask-address")
    run_parser.add_argument("--dask-timeout", type=float, default=60.0)
    run_parser.add_argument("--dask-expected-workers", type=int, default=1)
    run_parser.add_argument("--dask-max-in-flight", type=int, default=8)
    run_parser.add_argument("--dask-retries", type=int, default=2)
    run_parser.add_argument(
        "--cpu-gpu-overlap", action=argparse.BooleanOptionalAction, default=None
    )
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
    hardware_parser = subparsers.add_parser("validate-hardware")
    hardware_parser.add_argument("--profile", required=True)
    hardware_parser.add_argument("--chronos-smoke", action="store_true")
    calibration_parser = subparsers.add_parser("calibrate")
    calibration_parser.add_argument("--profile", required=True)
    calibration_parser.add_argument("--output", type=Path, required=True)
    dask_calibration_parser = subparsers.add_parser("calibrate-dask")
    dask_calibration_parser.add_argument("--address", required=True)
    dask_calibration_parser.add_argument("--expected-workers", type=int, required=True)
    dask_calibration_parser.add_argument("--profile-name", required=True)
    dask_calibration_parser.add_argument("--mac-cpu-workers", type=int, required=True)
    dask_calibration_parser.add_argument("--ubuntu-cpu-workers", type=int, required=True)
    dask_calibration_parser.add_argument("--chronos-batch-size", type=int, required=True)
    dask_calibration_parser.add_argument("--max-in-flight", type=int, required=True)
    dask_calibration_parser.add_argument("--baseline", type=Path)
    dask_calibration_parser.add_argument("--output", type=Path, required=True)
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
    if args.command == "validate-hardware":
        profile, _ = resolve_execution_profile(args.profile)
        with POC1Coordinator(args.database) as coordinator:
            result = coordinator.validate_hardware(profile, args.chronos_smoke)
        print(json.dumps(result, default=str))
        return
    if args.command == "calibrate":
        profile, _ = resolve_execution_profile(args.profile)
        with tempfile.TemporaryDirectory(prefix="shapefm-calibration-cli-") as directory:
            with POC1Coordinator(Path(directory) / "hardware.duckdb") as coordinator:
                hardware = coordinator.validate_hardware(
                    profile, run_chronos_smoke=False
                )["hardware"]
        result = calibrate(profile, hardware, args.database, args.output)
        print(json.dumps(result, default=str))
        return
    if args.command == "calibrate-dask":
        result = calibrate_dask_profile(
            database_path=args.database,
            scheduler_address=args.address,
            expected_workers=args.expected_workers,
            profile_name=args.profile_name,
            mac_cpu_workers=args.mac_cpu_workers,
            ubuntu_cpu_workers=args.ubuntu_cpu_workers,
            chronos_batch_size=args.chronos_batch_size,
            max_in_flight=args.max_in_flight,
            output=args.output,
            baseline=args.baseline,
        )
        print(
            json.dumps(
                {key: value for key, value in result.items() if key != "_scientific_outputs"},
                default=str,
            )
        )
        return
    selected_experiment = getattr(args, "experiment_id", None)
    if args.command in {"run", "export"} and selected_experiment is None:
        selected_experiment = latest_experiment_id(args.database)
    with POC1Coordinator(args.database) as coordinator:
        if args.command == "plan":
            value = coordinator.plan(args.scope, dry_run=args.dry_run)
            result = asdict(value) if isinstance(value, ExperimentPlan) else value
        elif args.command == "run":
            experiment_id = selected_experiment
            overrides = {
                "cleaning_workers": args.cleaning_workers,
                "transformation_workers": args.transformation_workers,
                "autoarima_workers": args.autoarima_workers,
                "chronos_processes": args.chronos_processes,
                "chronos_inference_batch_size": args.chronos_batch_size
                if args.chronos_batch_size is not None
                else args.batch_size,
                "combination_workers": args.combination_workers,
                "evaluation_workers": args.evaluation_workers,
                "cpu_gpu_overlap": args.cpu_gpu_overlap,
            }
            if args.workers is not None:
                for field in (
                    "cleaning_workers",
                    "transformation_workers",
                    "autoarima_workers",
                    "combination_workers",
                    "evaluation_workers",
                ):
                    if overrides[field] is None:
                        overrides[field] = args.workers
            if args.device and args.device != "auto":
                overrides["required_accelerator"] = args.device
            execution = resolve_execution_profile(args.profile, overrides)
            execution_settings = ExecutionSettings(
                mode=args.execution,
                dask_scheduler_address=args.dask_address,
                dask_timeout_seconds=args.dask_timeout,
                dask_expected_workers=args.dask_expected_workers,
                dask_max_in_flight=args.dask_max_in_flight,
                dask_retries=args.dask_retries,
            )
            if args.stage:
                result = coordinator.run_gate(
                    experiment_id,
                    args.stage,
                    execution=execution,
                    execution_settings=execution_settings,
                )
            else:
                count = coordinator.connection.execute(
                    "SELECT count(*) FROM forecast_instances i JOIN experiments e USING (dataset_id) WHERE e.experiment_id=?",
                    [experiment_id],
                ).fetchone()[0]
                plan = ExperimentPlan(experiment_id, "stored", count, 4, {}, "m4_daily/D/short")
                result = coordinator.run_all(
                    plan,
                    execution=execution,
                    execution_settings=execution_settings,
                )
        else:
            result = coordinator.export_candidate(
                selected_experiment, args.model_name
            )
    print(json.dumps(result, default=str))


if __name__ == "__main__":
    main()
