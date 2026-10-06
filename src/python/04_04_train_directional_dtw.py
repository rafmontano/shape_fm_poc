#!/usr/bin/env python3
# ==============================================================================
# 04_04_train_directional_dtw.py
#
# Purpose: Calibrate direct-aeon DTW and publish one multi-horizon fitted object.
# Inputs: A verified S1 cache, calibration blocks, selected widths, and model-store scope.
# Outputs: Calibration counts or one atomically saved fitted-artifact record.
# Run from: environments/classifiers/.venv; JSON request on stdin and response on stdout.
# ==============================================================================

"""Isolated training-only worker for the accepted directional DTW baseline."""

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
    INPUT_LENGTH,
    DirectionalDTWRunner,
    canonical_json,
    dependency_versions,
)
from util.shared_model_storage import ModelStorage


def runtime_description() -> dict[str, Any]:
    """Return the locked calculation runtime used for scientific provenance."""
    return {
        **dependency_versions(),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "numeric_dtype": DTYPE,
        "numba_threads": numba.get_num_threads(),
    }


def main() -> None:
    """Describe, calibrate, or publish fitted state without performing prediction."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("describe", "calibration", "publish"))
    parser.add_argument("--reference-cache", type=Path)
    parser.add_argument("--reference-fingerprint")
    parser.add_argument("--model-root", type=Path)
    parser.add_argument("--experiment")
    parser.add_argument("--frequency")
    parser.add_argument("--overwrite", action="store_true")
    arguments = parser.parse_args()
    numba.set_num_threads(1)
    if arguments.operation == "describe":
        print(canonical_json({"runtime": runtime_description(),
                              "engine": "aeon.distances.dtw_distance",
                              "input_length": INPUT_LENGTH}))
        return
    if arguments.reference_cache is None or arguments.reference_fingerprint is None:
        parser.error("training requires the reference cache and fingerprint")
    request = json.load(sys.stdin)
    started = time.monotonic()
    runner = DirectionalDTWRunner(
        arguments.reference_cache, arguments.reference_fingerprint
    )
    if arguments.operation == "calibration":
        jobs = request.get("jobs") if isinstance(request, dict) else None
        if not isinstance(jobs, list) or not jobs:
            raise ValueError("calibration requires a nonempty jobs list")
        results = runner.calibrate(jobs)
        response = {"operation": "calibration", "results": results}
    else:
        if not all((arguments.model_root, arguments.experiment, arguments.frequency)):
            parser.error("publish requires model root, experiment, and frequency")
        widths = request.get("selected_widths") if isinstance(request, dict) else None
        if not isinstance(widths, list):
            raise ValueError("publish requires selected_widths")
        storage = ModelStorage(arguments.model_root, arguments.experiment)
        evidence = storage.save(
            runner.fitted_model(tuple(widths), arguments.reference_fingerprint),
            "directional_dtw",
            arguments.frequency,
            "all_horizons",
            overwrite=arguments.overwrite,
        )
        response = {"operation": "publish", "artifact": evidence}
    response.update(runtime_seconds=time.monotonic() - started,
                    runtime=runtime_description())
    print(canonical_json(response))


if __name__ == "__main__":
    main()
