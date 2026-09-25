"""Dependency-heavy integration with the pinned official GIFT-Eval interfaces."""

import json
import subprocess
import tempfile
import unittest
from pathlib import Path


class OfficialAdapterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = Path(__file__).resolve().parents[2]
        cls.python = cls.root / "environments/gift-eval/.venv/bin/python"
        cls.bridge = cls.root / "scripts/gift_eval_bridge.py"
        cls.source = cls.root / "data/source/gift_eval"

    def bridge_call(self, *arguments):
        completed = subprocess.run(
            [str(self.python), str(self.bridge), *arguments],
            cwd=self.root,
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
        return json.loads(completed.stdout)

    def test_official_instances_are_context_only_and_evaluate_officially(self):
        description = self.bridge_call(
            "describe", "--source-root", str(self.source), "--limit", "2"
        )
        self.assertEqual(description["configuration_name"], "m4_daily/D/short")
        self.assertEqual(description["prediction_length"], 14)
        self.assertEqual(description["window_count"], 1)
        self.assertEqual(description["seasonality"], 1)
        forecasts = []
        for instance in description["instances"]:
            value = instance["context"][-1]
            forecasts.append(
                {"mean": [value] * 14, "quantiles": [[value] * 14 for _ in range(9)]}
            )
            self.assertEqual(len(instance["actual"]), 14)
        with tempfile.NamedTemporaryFile("w", suffix=".json") as stream:
            json.dump({"forecasts": forecasts}, stream)
            stream.flush()
            metrics = self.bridge_call(
                "evaluate",
                "--source-root",
                str(self.source),
                "--payload",
                stream.name,
            )
        self.assertEqual(len(metrics), 11)
        self.assertIn("MSE[mean]", metrics)
        self.assertIn("mean_weighted_sum_quantile_loss", metrics)

    def test_manifest_is_complete_qualified_and_validated(self):
        manifest = self.bridge_call("manifest", "--root", str(self.root))
        self.assertTrue(manifest["validated"])
        self.assertEqual(manifest["manifest_role"], "pinned_consensus_manifest")
        self.assertEqual(manifest["configuration_count"], 97)
        self.assertEqual(len(set(manifest["configurations"])), 97)
        consensus = manifest["consensus_validation"]
        self.assertEqual(consensus["all_results_file_count"], 132)
        self.assertEqual(consensus["complete_file_count"], 117)
        self.assertTrue(consensus["all_complete_files_agree"])
        self.assertEqual(consensus["disagreements"], [])
        self.assertEqual(len(consensus["configuration_set_sha256"]), 64)
        entry = next(
            item
            for item in manifest["entries"]
            if item["configuration_name"] == "m4_daily/D/short"
        )
        self.assertEqual(
            entry,
            {
                "configuration_name": "m4_daily/D/short",
                "dataset_name": "m4_daily",
                "frequency": "D",
                "term": "short",
                "domain": "Econ/Fin",
                "num_variates": 1,
            },
        )


if __name__ == "__main__":
    unittest.main()
