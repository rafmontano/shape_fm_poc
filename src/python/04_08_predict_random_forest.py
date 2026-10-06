#!/usr/bin/env python3
# ==============================================================================
# 04_08_predict_random_forest.py
#
# Purpose: Load fitted Random Forest horizons and calculate model-neutral predictions.
# Inputs: One immutable feature dataset, lightweight jobs, and configured model scope.
# Outputs: Validated classification response with operational worker provenance.
# Run from: environments/classifiers/.venv; JSON request on stdin and response on stdout.
# ==============================================================================

"""Isolated Random Forest prediction worker with no fitting path."""

from __future__ import annotations

import argparse
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
    ClassificationResponse,
    RandomForestClassifierProvider,
    canonical_json,
)
from util.shared_model_storage import ModelStorage


def runtime_description() -> dict[str, Any]:
    """Report the authoritative prediction runtime."""
    provider = RandomForestClassifierProvider()
    return {"environment": "environments/classifiers",
            "python": platform.python_version(), "numpy": np.__version__,
            "scikit-learn": provider.describe().implementation_version,
            "classifier": provider.describe().to_dict()}


def main() -> None:
    """Describe or predict from fitted files; this executable cannot fit models."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("describe", "predict"))
    parser.add_argument("--model-root")
    parser.add_argument("--experiment")
    parser.add_argument("--frequency")
    arguments = parser.parse_args()
    started = time.monotonic()
    if arguments.operation == "describe":
        print(canonical_json({"runtime": runtime_description()}))
        return
    if not all((arguments.model_root, arguments.experiment, arguments.frequency)):
        parser.error("prediction requires model root, experiment, and frequency")
    request = json.load(sys.stdin)
    dataset_value = request.get("dataset") if isinstance(request, dict) else None
    job_values = request.get("jobs") if isinstance(request, dict) else None
    if not isinstance(dataset_value, dict) or not isinstance(job_values, list) or not job_values:
        raise ValueError("prediction requires one dataset and a nonempty jobs list")
    dataset = ClassificationDataset.from_dict(dataset_value)
    storage = ModelStorage(arguments.model_root, arguments.experiment)
    results = []
    for value in job_values:
        job = ClassificationJob.from_dict(value)
        provider = storage.load("directional_mantis_rf", arguments.frequency, job.horizon)
        if not isinstance(provider, RandomForestClassifierProvider):
            raise RuntimeError("loaded Random Forest artifact has the wrong object type")
        results.append(provider.predict_job(dataset, job))
    provenance = {"runtime": runtime_description(),
                  "worker": {"hostname": socket.gethostname(),
                             "platform": platform.platform()},
                  "runtime_seconds": time.monotonic() - started}
    envelope = ClassificationResponse.create(
        dataset_id=dataset.dataset_id, results=results, worker_provenance=provenance
    )
    print(canonical_json({"operation": "predict", "response": envelope.to_dict()}))


if __name__ == "__main__":
    main()
