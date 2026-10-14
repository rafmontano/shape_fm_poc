"""Process 05 public-flow contract: stored components, lineage and atomic restart."""

import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from experiment import run_experiment
from util.runtime import CODE_ROOT, ExperimentRequest, open_experiment


class MVPCombineTests(unittest.TestCase):
    """Combining stored means must not fit models or create ex-post candidates."""

    def test_combination_lineage_and_no_work_restart(self):
        """Asymmetric stored components expose missing weights or reordered lineage."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "config").mkdir()
            (root / "config/experiment.json").write_text((CODE_ROOT / "config/experiment.json").read_text())
            request = ExperimentRequest((5,), root)
            context = open_experiment(request)
            context.state.accept_process("04")
            context.repository.accept("04.01", [(f"input/{model}", {
                "input_key": "input", "model": model, "mean": values,
                "forecast_capability": "mean_only"})
                for model, values in (("ses", [1, 2]), ("holt", [4, 8]), ("damped", [13, 17]))])
            context.close()
            run_experiment(request)
            with duckdb.connect(str(root / "results/experiment.duckdb"), read_only=True) as connection:
                before = connection.execute("SELECT * FROM atomic_results ORDER BY subflow_id,atomic_key").fetchall()
                row = json.loads(connection.execute("SELECT result FROM atomic_results WHERE subflow_id='05.01'").fetchone()[0])
                self.assertEqual(row["mean"], [6, 9])
                self.assertEqual(row["component_keys"], {m: f"input/{m}" for m in ("ses", "holt", "damped")})
                self.assertIsNone(row["quantiles"])
                self.assertEqual(connection.execute("SELECT count(*) FROM atomic_plan WHERE subflow_id='05.02'").fetchone()[0], 0)
            run_experiment(request)
            with duckdb.connect(str(root / "results/experiment.duckdb"), read_only=True) as connection:
                self.assertEqual(before, connection.execute("SELECT * FROM atomic_results ORDER BY subflow_id,atomic_key").fetchall())
