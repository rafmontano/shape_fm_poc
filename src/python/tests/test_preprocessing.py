"""Focused Gate 2 tests for standard and robust R preprocessing."""

from __future__ import annotations

import json
import math
import os
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WORKER = ROOT / "src/r/02_01_preprocess_series.R"


def run_worker(jobs: list[dict]) -> dict:
    """Run the bounded preprocessing worker and decode its JSON response."""
    completed = subprocess.run(
        ["Rscript", str(WORKER)],
        cwd=ROOT,
        input=json.dumps({"action": "preprocess", "jobs": jobs}),
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
        env={**os.environ, "RENV_CONFIG_SYNCHRONIZED_CHECK": "false"},
    )
    return json.loads(completed.stdout)


class PreprocessingTests(unittest.TestCase):
    """Verify mode semantics, dimensions, finite outputs, and visible provenance."""

    def test_standard_preserves_finite_values_and_only_fills_missing_positions(self):
        jobs = [
            {
                "id": "complete",
                "context": [1.0, 2.0, 3.0, 4.0],
                "mode": "standard",
                "seasonality": 7,
            },
            {
                "id": "missing",
                "context": [1.0, None, 3.0, 4.0],
                "mode": "standard",
                "seasonality": 7,
            },
        ]
        complete, missing = run_worker(jobs)["results"]
        self.assertEqual(complete["values"], jobs[0]["context"])
        self.assertFalse(complete["values_changed"])
        self.assertEqual(len(missing["values"]), len(jobs[1]["context"]))
        self.assertEqual(
            [missing["values"][index] for index in (0, 2, 3)], [1.0, 3.0, 4.0]
        )
        self.assertTrue(all(math.isfinite(value) for value in missing["values"]))
        self.assertEqual(missing["missing_count_before"], 1)
        self.assertEqual(missing["missing_count_after"], 0)
        self.assertTrue(missing["values_changed"])

    def test_robust_returns_finite_same_length_values_and_provenance(self):
        context = [1.0, 2.0, 100.0, 4.0, 5.0, None, 7.0, 8.0]
        result = run_worker([
            {
                "id": "robust",
                "context": context,
                "mode": "robust",
                "seasonality": 7,
            }
        ])["results"][0]
        self.assertEqual(result["preprocessing_mode"], "robust")
        self.assertEqual(result["status"], "success")
        self.assertEqual(len(result["values"]), len(context))
        self.assertTrue(all(math.isfinite(value) for value in result["values"]))
        self.assertEqual(result["missing_count_before"], 1)
        self.assertEqual(result["missing_count_after"], 0)

    def test_infinity_and_unrecoverable_series_fail_clearly(self):
        for context in ([1.0, float("inf"), 3.0], [None, None, None]):
            with self.subTest(context=context):
                completed = subprocess.run(
                    ["Rscript", str(WORKER)],
                    cwd=ROOT,
                    input=json.dumps({
                        "action": "preprocess",
                        "jobs": [{
                            "id": "invalid",
                            "context": context,
                            "mode": "standard",
                            "seasonality": 7,
                        }],
                    }),
                    capture_output=True,
                    text=True,
                    timeout=60,
                    env={**os.environ, "RENV_CONFIG_SYNCHRONIZED_CHECK": "false"},
                )
                self.assertNotEqual(completed.returncode, 0)
                self.assertRegex(
                    completed.stderr,
                    "infinity|finite vector|missing values|non-missing|invalid char",
                )


if __name__ == "__main__":
    unittest.main()
