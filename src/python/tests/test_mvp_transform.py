"""Process 03 public-flow scientific and restart contracts.

Synthetic asymmetric windows protect training boundaries, future independence,
sample-SD state, shared cohort caps and parent/child storage ownership.
"""

import json
import math
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from experiment import run_experiment, status
from tests.test_mvp_import import configuration, write_source
from util.runtime import ExperimentRequest


def read_results(path: Path) -> list[tuple]:
    """Read accepted scientific records including their durable acceptance times."""
    with duckdb.connect(str(path), read_only=True) as connection:
        return connection.execute("SELECT * FROM atomic_results ORDER BY subflow_id,atomic_key").fetchall()


class MVPTransformTests(unittest.TestCase):
    """Modelling inputs preserve local cleaning/labels and one shared capped cohort."""

    def test_windows_cohort_features_and_restart(self):
        """Different futures leave identical inputs unchanged; the cap excludes neither evaluation nor storage."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            targets = []
            for series in range(8):
                values = [float(index % 7 + index / 8) for index in range(197)]
                values[64:78] = [500.0 if series % 2 else -500.0] * 14
                values[142:156] = [-600.0 if series % 2 else 600.0] * 14
                values[183:] = [200000.0 + series] * 14
                targets.append(values)
            write_source(root, targets)
            configuration(root, 8)
            path = root / "config/experiment.json"
            document = json.loads(path.read_text())
            document["pipeline"]["window_preparation"]["training_cohort"]["cap"] = 3
            path.write_text(json.dumps(document))
            request = ExperimentRequest((1, 2, 3), root)
            run_experiment(request)
            parent = read_results(root / "results/experiment.duckdb")
            child = read_results(root / "results/windows.duckdb")
            inputs = {key: json.loads(value) for subflow, key, value, _ in child if subflow == "03.02"}
            rolling = {key: row for key, row in inputs.items() if row["kind"] == "rolling"}
            official = {key: row for key, row in inputs.items() if row["kind"] == "official"}
            self.assertEqual(len(rolling), 16)
            self.assertEqual(len(official), 8)
            for row in rolling.values():
                self.assertIn(row["input_start"], (0, 78))
                self.assertLessEqual(row["future_end"], 169)
                self.assertEqual(row["labels"], [int(row["actual"][0] > 0)] * 14)
                cleaned = row["cleaned_input"]
                centre = math.fsum(cleaned) / 64
                scale = math.sqrt(math.fsum((value - centre) ** 2 for value in cleaned) / 63)
                self.assertEqual(row["state"]["centre"], centre)
                self.assertEqual(row["state"]["scale"], scale)
                self.assertEqual(row["values"], [(value - centre) / scale for value in cleaned])
            first_inputs = [row["values"] for row in rolling.values() if row["input_start"] == 0]
            self.assertTrue(all(values == first_inputs[0] for values in first_inputs))
            cohort = next(json.loads(value) for subflow, key, value, _ in parent if subflow == "reference_cohort")
            self.assertEqual(cohort["configured_cap"], 3)
            self.assertEqual(len(cohort["input_keys"]), 3)
            self.assertTrue(all(inputs[key]["partition"] == "train" for key in cohort["input_keys"]))
            self.assertEqual({inputs[key]["labels"][0] for key in cohort["input_keys"]}, {0, 1})
            features = {key: json.loads(value) for subflow, key, value, _ in parent if subflow == "03.03"}
            self.assertEqual(set(features), set(cohort["input_keys"]) | set(official))
            self.assertTrue(all(len(row["values"]) == 42 and len(row["names"]) == 42 for row in features.values()))
            self.assertFalse(any(subflow == "03.02" for subflow, *_ in parent))
            with duckdb.connect(str(root / "results/windows.duckdb"), read_only=True) as connection:
                self.assertEqual(connection.execute("SELECT experiment_id FROM ownership").fetchone()[0], status(root)["experiment_id"])
            path.unlink()
            with patch("p03_transform.application.run_r", side_effect=AssertionError("accepted inputs/features repeated")), \
                 patch("p03_transform.application.s1_partition", side_effect=AssertionError("accepted membership repeated")):
                run_experiment(request)
            self.assertEqual(parent, read_results(root / "results/experiment.duckdb"))
            self.assertEqual(child, read_results(root / "results/windows.duckdb"))
