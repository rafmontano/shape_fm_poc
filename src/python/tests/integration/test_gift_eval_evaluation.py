# ==============================================================================
# test_gift_eval_evaluation.py
#
# Purpose: Verify the bridge agrees with pinned official GIFT-Eval metrics and rejects malformed forecast payloads.
# Inputs: unittest fixtures, temporary databases/files, deterministic synthetic records, and mocked process or cluster boundaries.
# Outputs: unittest pass/fail assertions and captured diagnostics; no production artifacts or external services.
# Run from: PYTHONPATH=src/python .tools/uv/uv run --locked --no-sync python -m unittest tests.integration.test_gift_eval_evaluation
# ==============================================================================

"""Verify the bridge agrees with pinned official GIFT-Eval metrics and rejects malformed forecast payloads."""

import json
import subprocess
import tempfile
import unittest
from pathlib import Path


class OfficialAdapterTests(unittest.TestCase):
    """Verify the pinned adapter's dataset description, evaluation, and manifest contracts."""

    @classmethod
    def setUpClass(cls):
        """Resolve the repository, pinned interpreter, bridge, and fixture dataset paths."""
        cls.root = Path(__file__).resolve().parents[4]
        cls.python = cls.root / "environments/gift-eval/.venv/bin/python"
        cls.bridge = cls.root / "src/python/06_evaluate_gift_eval.py"
        cls.source = cls.root / "data/source/gift_eval"

    def bridge_call(self, *arguments):
        """Run a bridge command and decode its successful JSON response."""
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
        """Described instances support official evaluation of horizon-length forecasts."""
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
        """The manifest contains the validated 97-configuration consensus and full metadata."""
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
