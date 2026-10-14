"""Researcher entry point and experiment-level orchestration for Processes01–06.

Inputs are the process range and creation-time JSON or stored experiment. Outputs
are scientific data under results/ and coordinator research milestones. The
approved process/subflow hierarchy, not model-specific runners, owns execution.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict
from importlib import import_module
from pathlib import Path
from typing import Sequence

import duckdb

# Prefect reads its home during import, before any flow or server is started.
os.environ["PREFECT_HOME"] = str(Path(__file__).resolve().parents[2] / ".runtime/prefect")
from prefect import flow

from util.runtime import CODE_ROOT, ExperimentRequest, open_experiment
from util.research_tracking import MLflowResearchTracking


def run_experiment(request: ExperimentRequest) -> str:
    """Prepare coordinator services and invoke the single scientific experiment flow.

    Distributed requests use inventory-derived orchestration addresses; the
    researcher selects a profile, not manually managed Prefect/Dask endpoints.
    """
    from util.distributed_cluster import coordinator_api
    with coordinator_api(request.execution_profile):
        return experiment_flow(request)


@flow(name="ShapeFM experiment")
def experiment_flow(request: ExperimentRequest) -> str:
    """Experiment-level flow; DuckDB process/subflow/atomic keys govern resume.

    Execute the approved six processes in order, stopping at the requested
    boundary. Tracking is a coordinator projection, not scientific authority.
    """
    context = open_experiment(request)
    tracking = MLflowResearchTracking(context)
    context.with_tracking(tracking)
    try:
        with tracking.experiment(context.experiment_ref):
            for number, name in enumerate(("import", "preprocess", "transform", "forecast", "combine", "evaluate"), 1):
                if request.includes(number):
                    module = import_module(f"p{number:02d}_{name}.flow")
                    getattr(module, f"run_{name}")(context)
        return context.experiment_ref.experiment_id
    finally:
        context.close()


def status(root: Path = CODE_ROOT) -> dict:
    """Read scientific completion and work counts without scheduling or mutation."""
    with duckdb.connect(str(root / "results/experiment.duckdb"), read_only=True) as connection:
        experiment_id, run_id = connection.execute("SELECT experiment_id,mlflow_run_id FROM experiment").fetchone()
        milestones = [row[0] for row in connection.execute("SELECT milestone_id FROM milestones ORDER BY milestone_id").fetchall()]
        counts = connection.execute("SELECT subflow_id,count(*) FROM atomic_results GROUP BY subflow_id ORDER BY subflow_id").fetchall()
    return {"experiment_id": experiment_id, "mlflow_run_id": run_id, "accepted_milestones": milestones,
            "accepted_items": dict(counts)}


def _processes(value: str) -> tuple[int, ...]:
    bounds = value.split("-")
    numbers = tuple(range(int(bounds[0]), int(bounds[-1]) + 1))
    if not numbers or any(n not in range(1, 7) for n in numbers):
        raise argparse.ArgumentTypeError("process range must lie within 1–6")
    return numbers


def main(argv: Sequence[str] | None = None) -> int:
    """Dispatch one researcher request; report failures without implicit reset."""
    parser = argparse.ArgumentParser(description="ShapeFM six-process experiment")
    parser.add_argument("action", choices=("run", "status", "plan", "export"))
    parser.add_argument("--processes", type=_processes, default=(1, 2, 3, 4, 5, 6))
    parser.add_argument("--execution-profile", default="sequential_safe")
    parser.add_argument("--root", type=Path, default=CODE_ROOT, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.action == "run":
        result = run_experiment(ExperimentRequest(args.processes, args.root, execution_profile=args.execution_profile))
    elif args.action == "status":
        result = status(args.root)
    elif args.action == "plan":
        result = {"processes": args.processes, "configuration": str(args.root / "config/experiment.json")}
    else:
        from p06_evaluate.publication import publish_stored_outputs
        result = asdict(publish_stored_outputs(args.root))
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
