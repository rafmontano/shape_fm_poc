"""Process 03 public-flow scientific and restart contracts.

Synthetic asymmetric windows protect full pre-test boundaries, prepared targets,
sample-SD state, model-specific cohorts and parent/child storage ownership.
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
    """Modelling inputs slice once-prepared histories and model-specific cohorts."""

    def test_windows_cohort_features_and_restart(self):
        """Full historical blocks survive; only DTW references are capped."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            targets = []
            for series in range(8):
                values = [float(index % 7 + index / 8) for index in range(170)]
                values[64:78] = [500.0 if series % 2 else -500.0] * 14
                values[142:156] = [-600.0 if series % 2 else 600.0] * 14
                values[156:] = [200000.0 + series] * 14
                targets.append(values)
            write_source(root, targets)
            configuration(root, 8)
            path = root / "config/experiment.json"
            document = json.loads(path.read_text())
            document["pipeline"]["window_preparation"]["dtw_training_cohort"]["cap"] = 3
            path.write_text(json.dumps(document))
            request = ExperimentRequest((1, 2, 3), root)
            run_experiment(request)
            parent = read_results(root / "results/experiment.duckdb")
            child = read_results(root / "results/windows.duckdb")
            inputs = {key: json.loads(value) for subflow, key, value, _ in child if subflow == "03.02"}
            rolling = {key: row for key, row in inputs.items() if row["kind"] == "rolling"}
            official = {key: row for key, row in inputs.items() if row["kind"] == "official"}
            prepared = {key: json.loads(value) for subflow, key, value, _ in parent if subflow == "02.01"}
            canonical = {key: json.loads(value) for subflow, key, value, _ in parent if subflow == "01.02"}
            self.assertTrue(all(row["train_end"] == row["test_start"] == 156 for row in canonical.values()))
            self.assertEqual(len(rolling), 16)
            self.assertEqual(len(official), 8)
            for row in rolling.values():
                self.assertIn(row["input_start"], (0, 78))
                self.assertLessEqual(row["future_end"], 156)
                history = prepared[row["series_key"]]["values"]
                self.assertEqual(row["cleaned_input"], history[row["input_start"]:row["input_end"]])
                self.assertEqual(row["actual"], history[row["future_start"]:row["future_end"]])
                self.assertEqual(row["labels"], [int(v > row["cleaned_input"][-1]) for v in row["actual"]])
                cleaned = row["cleaned_input"]
                centre = math.fsum(cleaned) / 64
                scale = math.sqrt(math.fsum((value - centre) ** 2 for value in cleaned) / 63)
                self.assertEqual(row["state"]["centre"], centre)
                self.assertEqual(row["state"]["scale"], scale)
                self.assertEqual(row["values"], [(value - centre) / scale for value in cleaned])
            for row in official.values():
                self.assertEqual(row["actual"], canonical[row["series_key"]]["target"][156:])
                self.assertEqual(row["cleaned_input"], prepared[row["series_key"]]["values"][-64:])
            cohorts = {key: json.loads(value) for subflow, key, value, _ in parent if subflow == "reference_cohort"}
            cohort = cohorts["directional_dtw"]
            self.assertEqual(cohort["configured_cap"], 3)
            self.assertEqual(len(cohort["input_keys"]), 3)
            self.assertTrue(all(inputs[key]["partition"] == "train" for key in cohort["input_keys"]))
            eligible = sorted(key for key, row in rolling.items() if row["partition"] == "train")
            self.assertEqual(cohorts["directional_mantis_rf"]["input_keys"], eligible)
            self.assertGreater(len(eligible), 3)
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
