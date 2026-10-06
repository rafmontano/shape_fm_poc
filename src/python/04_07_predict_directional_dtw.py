#!/usr/bin/env python3
# ==============================================================================
# 04_07_predict_directional_dtw.py
#
# Purpose: Load one fitted DTW object and calculate bounded official predictions.
# Inputs: Model-store scope and prediction jobs containing only accepted official inputs.
# Outputs: Compact nearest-neighbour predictions and runtime provenance; never trains.
# Run from: environments/classifiers/.venv; JSON request on stdin and response on stdout.
# ==============================================================================

"""Isolated prediction-only worker for the accepted directional DTW baseline."""

from __future__ import annotations

import argparse
import json
import platform
import sys
import time
from pathlib import Path
from typing import Any

import numba

from util.p04_04_directional_dtw import (
    DTYPE,
    DirectionalDTWModel,
    DirectionalDTWRunner,
    canonical_json,
    dependency_versions,
)
from util.shared_model_storage import ModelStorage


def runtime_description() -> dict[str, Any]:
    """Return the locked prediction runtime used for operational provenance."""
    return {**dependency_versions(), "python": platform.python_version(),
            "platform": platform.platform(), "numeric_dtype": DTYPE,
            "numba_threads": numba.get_num_threads()}


def main() -> None:
    """Load fitted state and predict; this executable has no calibration path."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("describe", "prediction"))
    parser.add_argument("--model-root", type=Path)
    parser.add_argument("--experiment")
    parser.add_argument("--frequency")
    parser.add_argument("--reference-fingerprint")
    arguments = parser.parse_args()
    numba.set_num_threads(1)
    if arguments.operation == "describe":
        print(canonical_json({"runtime": runtime_description()}))
        return
    if not all((arguments.model_root, arguments.experiment, arguments.frequency,
                arguments.reference_fingerprint)):
        parser.error(
            "prediction requires model root, experiment, frequency, and reference fingerprint"
        )
    request = json.load(sys.stdin)
    jobs = request.get("jobs") if isinstance(request, dict) else None
    if not isinstance(jobs, list) or not jobs:
        raise ValueError("prediction requires a nonempty jobs list")
    started = time.monotonic()
    fitted = ModelStorage(arguments.model_root, arguments.experiment).load(
        "directional_dtw", arguments.frequency, "all_horizons"
    )
    if not isinstance(fitted, DirectionalDTWModel):
        raise RuntimeError("loaded directional DTW artifact has the wrong object type")
    if fitted.reference_fingerprint != arguments.reference_fingerprint:
        raise RuntimeError("loaded directional DTW artifact has a stale reference fingerprint")
    for job in jobs:
        horizons = job.get("horizons") if isinstance(job, dict) else None
        width = job.get("effective_width") if isinstance(job, dict) else None
        if not isinstance(horizons, list) or any(
            fitted.selected_widths[int(horizon) - 1] != width
            for horizon in horizons
        ):
            raise RuntimeError("directional DTW prediction job differs from fitted widths")
    results = DirectionalDTWRunner.from_model(fitted).predict(jobs)
    print(canonical_json({"operation": "prediction", "results": results,
                          "runtime_seconds": time.monotonic() - started,
                          "runtime": runtime_description()}))


if __name__ == "__main__":
    main()
