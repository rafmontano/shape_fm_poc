# ==============================================================================
# 04_04_directional_dtw.py
#
# Purpose: Calculate directional 1NN-DTW calibration and prediction blocks.
# Inputs: Immutable cached S1 references plus bounded query blocks from Process 04.
# Outputs: Compact nearest-neighbour counts or predictions with runtime provenance.
# Run from: Classifier environment; JSON request on stdin and JSON response on stdout.
# ==============================================================================

"""Direct aeon 1NN-DTW calculation for the ID 021 directional baseline."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Operational controls are set before importing numerical libraries. They do not
# alter the scientific definition and prevent model-owned nested scheduling.
for _variable in (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "NUMEXPR_NUM_THREADS",
):
    os.environ[_variable] = "1"

import numba
import numpy as np
from aeon.distances import dtw_distance


INPUT_LENGTH = 64
HORIZONS = tuple(range(1, 15))
DTYPE = "float64"
EXPECTED_VERSIONS = {
    "aeon": "1.6.0",
    "numpy": "2.0.2",
    "numba": "0.61.2",
    "scikit-learn": "1.7.2",
}


def canonical_json(value: Any) -> str:
    """Serialize cache content identically to the coordinator fingerprint rule."""
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def content_fingerprint(value: Any) -> str:
    """Return the SHA-256 fingerprint of canonical JSON-compatible content."""
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def dependency_versions() -> dict[str, str]:
    """Report and validate the actual locked numerical runtime."""
    versions = {
        name: importlib.metadata.version(name) for name in EXPECTED_VERSIONS
    }
    if versions != EXPECTED_VERSIONS:
        raise RuntimeError(
            f"directional DTW runtime differs from the locked contract: {versions!r}"
        )
    return versions


def effective_width(proportion: float) -> int:
    """Map one configured Sakoe-Chiba proportion to aeon's equal-length width."""
    if (
        isinstance(proportion, bool)
        or not isinstance(proportion, (int, float))
        or not math.isfinite(float(proportion))
        or not 0.0 <= float(proportion) < 1.0
    ):
        raise ValueError("window proportion must be finite and within [0, 1)")
    return int(float(proportion) * INPUT_LENGTH)


def _vector(values: Any, field: str) -> np.ndarray:
    """Validate one exact-length finite univariate float64 input."""
    try:
        vector = np.asarray(values, dtype=np.float64)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be numeric") from exc
    if vector.shape != (INPUT_LENGTH,) or not np.isfinite(vector).all():
        raise ValueError(f"{field} must contain exactly 64 finite float64 values")
    return vector


def _labels(values: Any, field: str) -> tuple[int, ...]:
    """Validate one complete fourteen-horizon binary label vector."""
    if (
        not isinstance(values, list)
        or len(values) != len(HORIZONS)
        or any(isinstance(value, bool) or value not in {0, 1} for value in values)
    ):
        raise ValueError(f"{field} must contain fourteen binary integer labels")
    return tuple(int(value) for value in values)


@dataclass(frozen=True)
class Reference:
    """One immutable accepted S1 training window used by the lazy learner."""

    identity: str
    source_series_identity: str
    values: np.ndarray
    labels: tuple[int, ...]


class DirectionalDTWRunner:
    """Hold one verified reference library and execute calibration or prediction."""

    def __init__(self, cache_path: Path, expected_fingerprint: str):
        """Load and verify complete cached values, source identities, and labels once."""
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        observed = content_fingerprint(payload)
        if observed != expected_fingerprint:
            raise RuntimeError(
                "cached directional reference content does not match its fingerprint"
            )
        rows = payload.get("references") if isinstance(payload, dict) else None
        if not isinstance(rows, list) or not rows:
            raise ValueError("directional reference cache must contain references")
        references = []
        identities = []
        for index, row in enumerate(rows):
            if not isinstance(row, dict) or set(row) != {
                "identity",
                "source_series_identity",
                "values",
                "labels",
            }:
                raise ValueError("directional reference cache has an invalid record")
            identity = row["identity"]
            source = row["source_series_identity"]
            if not isinstance(identity, str) or not identity or not isinstance(source, str) or not source:
                raise ValueError("reference identities must be nonempty strings")
            identities.append(identity)
            references.append(
                Reference(
                    identity,
                    source,
                    _vector(row["values"], f"references[{index}].values"),
                    _labels(row["labels"], f"references[{index}].labels"),
                )
            )
        if len(identities) != len(set(identities)):
            raise ValueError("directional reference identities must be unique")
        self.references = tuple(sorted(references, key=lambda item: item.identity))

    def _nearest(
        self,
        query: np.ndarray,
        width: int,
        excluded_source: str | None,
    ) -> tuple[Reference, float]:
        """Find one deterministic nearest reference using direct aeon DTW."""
        if isinstance(width, bool) or not isinstance(width, int) or not 0 <= width < INPUT_LENGTH:
            raise ValueError("effective width must be an integer within 0..63")
        proportion = width / INPUT_LENGTH
        nearest: Reference | None = None
        nearest_distance = math.inf
        for reference in self.references:
            if reference.source_series_identity == excluded_source:
                continue
            distance = float(
                dtw_distance(query, reference.values, window=proportion)
            )
            if not math.isfinite(distance):
                raise RuntimeError("aeon returned a non-finite DTW distance")
            if (
                distance < nearest_distance
                or distance == nearest_distance
                and nearest is not None
                and reference.identity < nearest.identity
            ):
                nearest = reference
                nearest_distance = distance
        if nearest is None:
            raise RuntimeError("no eligible directional reference remains for query")
        return nearest, nearest_distance

    def calibrate(self, jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Score one or more distinct-width × validation-query blocks."""
        results = []
        references = {reference.identity: reference for reference in self.references}
        for job in jobs:
            width = job.get("effective_width")
            query_ids = job.get("query_identities")
            if not isinstance(query_ids, list) or not query_ids:
                raise ValueError("calibration block requires query identities")
            correct = [0] * len(HORIZONS)
            evaluated = [0] * len(HORIZONS)
            for identity in query_ids:
                query = references.get(identity)
                if query is None:
                    raise ValueError(f"unknown calibration query identity: {identity!r}")
                nearest, _ = self._nearest(
                    query.values, width, query.source_series_identity
                )
                for index, (predicted, actual) in enumerate(
                    zip(nearest.labels, query.labels, strict=True)
                ):
                    evaluated[index] += 1
                    correct[index] += int(predicted == actual)
            results.append(
                {
                    "id": job.get("id"),
                    "effective_width": width,
                    "correct": correct,
                    "evaluated": evaluated,
                }
            )
        return results

    def predict(self, jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Predict configured horizon groups for bounded official-input blocks."""
        results = []
        for job in jobs:
            width = job.get("effective_width")
            horizons = job.get("horizons")
            queries = job.get("queries")
            if (
                not isinstance(horizons, list)
                or not horizons
                or any(horizon not in HORIZONS for horizon in horizons)
                or len(horizons) != len(set(horizons))
                or not isinstance(queries, list)
                or not queries
            ):
                raise ValueError("prediction block requires unique horizons and queries")
            predictions = []
            for query in queries:
                if not isinstance(query, dict) or set(query) != {"identity", "values"}:
                    raise ValueError("prediction query has an invalid contract")
                vector = _vector(query["values"], "prediction query values")
                nearest, distance = self._nearest(vector, width, None)
                predictions.extend(
                    {
                        "evaluation_input_identity": query["identity"],
                        "horizon": horizon,
                        "prediction": nearest.labels[horizon - 1],
                        "nearest_reference_identity": nearest.identity,
                        "nearest_distance": distance,
                        "effective_width": width,
                    }
                    for horizon in horizons
                )
            results.append({"id": job.get("id"), "predictions": predictions})
        return results


def execute(
    operation: str,
    cache_path: Path,
    reference_fingerprint: str,
    jobs: list[dict[str, Any]],
) -> dict[str, Any]:
    """Execute one validated worker request and return compact provenance."""
    versions = dependency_versions()
    numba.set_num_threads(1)
    started = time.monotonic()
    runner = DirectionalDTWRunner(cache_path, reference_fingerprint)
    if operation == "calibration":
        results = runner.calibrate(jobs)
    elif operation == "prediction":
        results = runner.predict(jobs)
    else:
        raise ValueError("operation must be calibration or prediction")
    return {
        "operation": operation,
        "results": results,
        "runtime_seconds": time.monotonic() - started,
        "runtime": {
            **versions,
            "python": platform.python_version(),
            "platform": platform.platform(),
            "numeric_dtype": DTYPE,
            "numba_threads": numba.get_num_threads(),
        },
    }


def main() -> None:
    """Read one JSON request and emit one JSON response for the coordinator."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "operation", choices=("describe", "calibration", "prediction")
    )
    parser.add_argument("--reference-cache", type=Path)
    parser.add_argument("--reference-fingerprint")
    arguments = parser.parse_args()
    if arguments.operation == "describe":
        print(
            canonical_json(
                {
                    "runtime": {
                        **dependency_versions(),
                        "python": platform.python_version(),
                        "platform": platform.platform(),
                        "numeric_dtype": DTYPE,
                    },
                    "engine": "aeon.distances.dtw_distance",
                    "input_length": INPUT_LENGTH,
                }
            )
        )
        return
    if arguments.reference_cache is None or arguments.reference_fingerprint is None:
        parser.error("calibration and prediction require the reference cache and fingerprint")
    request = json.load(sys.stdin)
    jobs = request.get("jobs") if isinstance(request, dict) else None
    if not isinstance(jobs, list) or not jobs:
        raise ValueError("worker request must contain a nonempty jobs list")
    print(
        canonical_json(
            execute(
                arguments.operation,
                arguments.reference_cache,
                arguments.reference_fingerprint,
                jobs,
            )
        )
    )


if __name__ == "__main__":
    main()
