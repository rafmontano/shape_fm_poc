#!/usr/bin/env python3
# ==============================================================================
# 04_06_train_random_forest.py
#
# Purpose: Fit and persist independent model-neutral Random Forest jobs.
# Inputs: One immutable feature dataset and lightweight horizon jobs on standard input.
# Outputs: Fitted-artifact evidence and stable classifier-run identities; never predicts.
# Run from: environments/classifiers/.venv; JSON request on stdin and response on stdout.
# ==============================================================================

"""Isolated one-thread Random Forest training worker."""

from __future__ import annotations

import argparse
import importlib.util
import json
import platform
import socket
import sys
import time
from typing import Any

import numpy as np

from util.shared_classification import (
    ClassificationDataset,
    ClassificationJob,
    RandomForestClassifierProvider,
    canonical_json,
)
from util.shared_model_storage import ModelStorage


def runtime_description() -> dict[str, Any]:
    """Report the authoritative classifier environment and forbidden-module state."""
    provider = RandomForestClassifierProvider()
    forbidden = ("mantis", "torch", "duckdb", "prefect", "dask", "distributed")
    return {
        "environment": "environments/classifiers",
        "python": platform.python_version(),
        "numpy": np.__version__,
        "scikit-learn": provider.describe().implementation_version,
        "mantis_available": importlib.util.find_spec("mantis") is not None,
        "torch_available": importlib.util.find_spec("torch") is not None,
        "forbidden_modules_loaded": sorted(
            name for name in forbidden if name in sys.modules
        ),
        "classifier": provider.describe().to_dict(),
    }


def train(
    dataset_value: dict[str, Any], jobs: list[dict[str, Any]], storage: ModelStorage,
    frequency: str, overwrite: bool,
) -> tuple[ClassificationDataset, tuple[dict[str, Any], ...]]:
    """Fit only missing horizon jobs and atomically publish their native providers."""
    if not jobs:
        raise ValueError("classifier worker requires a nonempty jobs list")
    dataset = ClassificationDataset.from_dict(dataset_value)
    results: list[dict[str, Any]] = []
    job_ids = []
    for value in jobs:
        job = ClassificationJob.from_dict(value)
        job_ids.append(job.job_id)
        if storage.exists("directional_mantis_rf", frequency, job.horizon) and not overwrite:
            artifact = storage.save(
                None, "directional_mantis_rf", frequency, job.horizon, overwrite=False
            )
        else:
            provider = RandomForestClassifierProvider(
                feature_dimension=job.specification.feature_dimension,
                feature_dtype=job.specification.feature_dtype,
            ).fit_job(dataset, job)
            artifact = storage.save(
                provider, "directional_mantis_rf", frequency, job.horizon,
                overwrite=overwrite,
            )
        results.append({
            "job_id": job.job_id,
            "horizon": job.horizon,
            "classifier_run_id": RandomForestClassifierProvider.classifier_run_id(job),
            "classifier_id": job.specification.classifier_id,
            "training_fingerprint": job.training_fingerprint,
            "evaluation_fingerprint": job.evaluation_fingerprint,
            "resolved_parameters": dict(job.specification.parameters),
            "artifact": artifact,
        })
    if len(job_ids) != len(set(job_ids)):
        raise ValueError("classifier job identities must be unique")
    return dataset, tuple(results)


def main() -> None:
    """Describe the worker or execute portable classifier jobs from standard input."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("describe", "train"))
    parser.add_argument("--model-root")
    parser.add_argument("--experiment")
    parser.add_argument("--frequency")
    parser.add_argument("--overwrite", action="store_true")
    arguments = parser.parse_args()
    started = time.monotonic()
    if arguments.operation == "describe":
        response = {"runtime": runtime_description()}
        response["runtime_seconds"] = time.monotonic() - started
    else:
        request = json.load(sys.stdin)
        jobs = request.get("jobs") if isinstance(request, dict) else None
        dataset_value = request.get("dataset") if isinstance(request, dict) else None
        if not isinstance(dataset_value, dict) or not isinstance(jobs, list):
            raise ValueError("classifier request must contain one dataset and a jobs list")
        if not all((arguments.model_root, arguments.experiment, arguments.frequency)):
            parser.error("training requires model root, experiment, and frequency")
        dataset, results = train(
            dataset_value, jobs,
            ModelStorage(arguments.model_root, arguments.experiment),
            arguments.frequency, arguments.overwrite,
        )
        worker_provenance = {
            "runtime": runtime_description(),
            "worker": {
                "hostname": socket.gethostname(),
                "platform": platform.platform(),
            },
            "runtime_seconds": time.monotonic() - started,
        }
        response = {
            "operation": "train",
            "dataset_id": dataset.dataset_id,
            "results": list(results),
            "worker_provenance": worker_provenance,
        }
    print(canonical_json(response))


if __name__ == "__main__":
    main()
