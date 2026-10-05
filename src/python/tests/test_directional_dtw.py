"""Numerical, tie, exclusion, and cache-integrity tests for ID 021 DTW."""

from __future__ import annotations

import importlib.util
import json
import math
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WORKER_PATH = ROOT / "src/python/04_04_directional_dtw.py"
if importlib.util.find_spec("aeon") is None:
    raise unittest.SkipTest("directional numerical tests require the classifiers environment")
SPEC = importlib.util.spec_from_file_location("directional_dtw_worker", WORKER_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load directional DTW worker")
DTW = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = DTW
SPEC.loader.exec_module(DTW)


def reference(
    identity: str,
    source: str,
    value: float,
    label: int,
) -> dict[str, object]:
    """Build one exact-length constant reference for focused numerical tests."""
    return {
        "identity": identity,
        "source_series_identity": source,
        "values": [value] * 64,
        "labels": [label] * 14,
    }


class DirectionalDTWTests(unittest.TestCase):
    """Exercise the direct aeon worker without project-environment substitution."""

    def runner(self, references: list[dict[str, object]]):
        """Create one verified temporary immutable library and runner."""
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        payload = {"references": references}
        path = Path(directory.name) / "references.json"
        path.write_text(DTW.canonical_json(payload), encoding="utf-8")
        return DTW.DirectionalDTWRunner(path, DTW.content_fingerprint(payload)), path

    def test_locked_dependency_versions(self):
        self.assertEqual(DTW.dependency_versions(), DTW.EXPECTED_VERSIONS)

    def test_candidate_proportions_cover_each_effective_width_once(self):
        mapping = [DTW.effective_width(index / 100) for index in range(100)]
        self.assertEqual(set(mapping), set(range(64)))
        self.assertEqual(mapping[:3], [0, 0, 1])

    def test_aeon_zero_constrained_and_unconstrained_distances(self):
        left = DTW.np.arange(64, dtype=DTW.np.float64)
        right = left.copy()
        right[-1] += 1
        self.assertEqual(float(DTW.dtw_distance(left, right, window=0.0)), 1.0)
        self.assertEqual(float(DTW.dtw_distance(left, right, window=0.25)), 1.0)
        self.assertEqual(float(DTW.dtw_distance(left, right)), 1.0)

    def test_aeon_uses_squared_euclidean_local_cost(self):
        left = DTW.np.zeros(64, dtype=DTW.np.float64)
        right = left.copy()
        right[7] = 2
        self.assertEqual(float(DTW.dtw_distance(left, right, window=0.0)), 4.0)

    def test_equal_distance_uses_lowest_stable_reference_identity(self):
        runner, _ = self.runner(
            [reference("reference/b", "series/b", 0.0, 0),
             reference("reference/a", "series/a", 0.0, 1)]
        )
        nearest, distance = runner._nearest(DTW.np.zeros(64), 0, None)
        self.assertEqual(nearest.identity, "reference/a")
        self.assertEqual(distance, 0.0)

    def test_calibration_excludes_every_reference_from_query_source(self):
        runner, _ = self.runner(
            [reference("reference/query", "series/shared", 0.0, 0),
             reference("reference/same-source", "series/shared", 0.0, 0),
             reference("reference/eligible", "series/other", 1.0, 1)]
        )
        result = runner.calibrate(
            [{"id": "block", "effective_width": 0,
              "query_identities": ["reference/query"]}]
        )[0]
        self.assertEqual(result["evaluated"], [1] * 14)
        self.assertEqual(result["correct"], [0] * 14)

    def test_calibration_scores_all_fourteen_horizons(self):
        runner, _ = self.runner(
            [reference("reference/a", "series/a", 0.0, 1),
             reference("reference/b", "series/b", 0.0, 1)]
        )
        result = runner.calibrate(
            [{"id": "block", "effective_width": 7,
              "query_identities": ["reference/a", "reference/b"]}]
        )[0]
        self.assertEqual(result["correct"], [2] * 14)
        self.assertEqual(result["evaluated"], [2] * 14)

    def test_prediction_groups_horizons_around_one_neighbour(self):
        runner, _ = self.runner([reference("reference/a", "series/a", 2.0, 1)])
        result = runner.predict(
            [{"id": "block", "effective_width": 3, "horizons": [1, 4, 14],
              "queries": [{"identity": "evaluation/a", "values": [2.0] * 64}]}]
        )[0]
        self.assertEqual([item["horizon"] for item in result["predictions"]], [1, 4, 14])
        self.assertTrue(all(item["prediction"] == 1 for item in result["predictions"]))
        self.assertTrue(all(item["nearest_distance"] == 0.0 for item in result["predictions"]))

    def test_invalid_values_are_rejected(self):
        invalid = reference("reference/a", "series/a", 0.0, 0)
        invalid["values"][3] = math.nan
        with self.assertRaisesRegex(ValueError, "finite float64"):
            self.runner([invalid])

    def test_cache_tampering_is_rejected_even_with_same_identity(self):
        references = [reference("reference/a", "series/a", 0.0, 0)]
        runner, path = self.runner(references)
        self.assertEqual(runner.references[0].labels[0], 0)
        altered = {"references": [reference("reference/a", "series/a", 0.0, 1)]}
        path.write_text(json.dumps(altered), encoding="utf-8")
        original = {"references": references}
        with self.assertRaisesRegex(RuntimeError, "does not match its fingerprint"):
            DTW.DirectionalDTWRunner(path, DTW.content_fingerprint(original))


if __name__ == "__main__":
    unittest.main()
