#!/usr/bin/env python3
# ==============================================================================
# 04_04_directional_dtw.py
#
# Purpose: Preserve the closed version-10 DTW worker protocol for existing databases.
# Inputs: Immutable S1 cache and version-10 calibration or prediction blocks.
# Outputs: Compact accepted version-10 results and runtime provenance.
# Run from: environments/classifiers/.venv; historical compatibility only.
# ==============================================================================

"""Compatibility executable for closed configuration-version-10 DTW runs."""

from __future__ import annotations

import argparse
import json
import platform
import sys
import time
from pathlib import Path

import numba

from util.p04_04_directional_dtw import (
    DTYPE,
    INPUT_LENGTH,
    DirectionalDTWRunner,
    canonical_json,
    dependency_versions,
)


def main() -> None:
    """Execute the unchanged version-10 describe/calibration/prediction protocol."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("describe", "calibration", "prediction"))
    parser.add_argument("--reference-cache", type=Path)
    parser.add_argument("--reference-fingerprint")
    arguments = parser.parse_args()
    numba.set_num_threads(1)
    runtime = {**dependency_versions(), "python": platform.python_version(),
               "platform": platform.platform(), "numeric_dtype": DTYPE,
               "numba_threads": numba.get_num_threads()}
    if arguments.operation == "describe":
        print(canonical_json({"runtime": runtime,
                              "engine": "aeon.distances.dtw_distance",
                              "input_length": INPUT_LENGTH}))
        return
    if arguments.reference_cache is None or arguments.reference_fingerprint is None:
        parser.error("calibration and prediction require the reference cache and fingerprint")
    request = json.load(sys.stdin)
    jobs = request.get("jobs") if isinstance(request, dict) else None
    if not isinstance(jobs, list) or not jobs:
        raise ValueError("worker request must contain a nonempty jobs list")
    started = time.monotonic()
    runner = DirectionalDTWRunner(
        arguments.reference_cache, arguments.reference_fingerprint
    )
    results = (
        runner.calibrate(jobs)
        if arguments.operation == "calibration"
        else runner.predict(jobs)
    )
    print(canonical_json({"operation": arguments.operation, "results": results,
                          "runtime_seconds": time.monotonic() - started,
                          "runtime": runtime}))


if __name__ == "__main__":
    main()
