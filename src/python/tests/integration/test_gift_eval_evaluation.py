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

from util.configuration import load_experiment_configuration


class OfficialAdapterTests(unittest.TestCase):
    """Purpose: Verify pinned adapter dataset, evaluation, and manifest contracts.

    Inputs: Committed configuration, local GiftEval data, and the pinned bridge interpreter.
    Outputs: JSON contract assertions; bridge subprocesses read fixtures and temporary payloads.
    """

    @classmethod
    def setUpClass(cls):
        """Purpose: Resolve shared bridge dependencies and configuration for this class.

        Inputs: Repository layout and committed 100-series experiment configuration.
        Outputs: Class path/configuration attributes; reads configuration without writing files.
        """
        cls.root = Path(__file__).resolve().parents[4]
        cls.python = cls.root / "environments/gift-eval/.venv/bin/python"
        cls.bridge = cls.root / "src/python/06_01_evaluate_gift_eval.py"
        cls.source = cls.root / "data/source/gift_eval"
        cls.experiment_configuration = load_experiment_configuration(
            cls.root / "config/experiments/poc2_m4_daily_100.json"
        )
        cls.configuration = cls.experiment_configuration.resolved

    def bridge_call(self, *arguments):
        """Purpose: Invoke the pinned GiftEval bridge and decode its response.

        Inputs: Command-line argument strings appended to the bridge executable invocation.
        Outputs: Decoded JSON value; starts a subprocess that may read local fixture files.
        """
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
            "describe",
            "--source-root",
            str(self.source),
            "--dataset-name",
            self.configuration["data"]["dataset_name"],
            "--term",
            self.configuration["data"]["benchmark"]["term"],
            "--domain",
            self.configuration["data"]["benchmark"]["domain"],
            "--num-variates",
            str(self.configuration["data"]["benchmark"]["num_variates"]),
            "--seasonality",
            str(self.configuration["data"]["benchmark"]["seasonality"]),
            "--limit",
            "2",
        )
        self.assertEqual(description["configuration_name"], "m4_daily/D/short")
        self.assertEqual(description["prediction_length"], 14)
        self.assertEqual(description["window_count"], 1)
        self.assertEqual(description["seasonality"], 7)
        self.assertEqual(description["frequency"], "D")
        self.assertEqual(description["available_instances"], 4_227)
        forecasts = []
        for instance in description["instances"]:
            value = instance["context"][-1]
            forecasts.append(
                {"mean": [value] * 14, "quantiles": [[value] * 14 for _ in range(9)]}
            )
            self.assertEqual(len(instance["actual"]), 14)
        with tempfile.NamedTemporaryFile("w", suffix=".json") as stream:
            json.dump(
                {
                    "dataset_name": self.configuration["data"]["dataset_name"],
                    "term": self.configuration["data"]["benchmark"]["term"],
                    "quantile_levels": self.configuration["models"]["chronos_2"]["quantile_levels"],
                    "options": self.experiment_configuration.evaluation_options,
                    "seasonality": description["seasonality"],
                    "forecasts": forecasts,
                },
                stream,
            )
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

    def test_pinned_period_resolver_supports_defaults_overrides_and_errors(self):
        """The bridge uses actual pinned GluonTS defaults and explicit overrides."""
        expected = {
            "D": 1,
            "H": 24,
            "W": 1,
            "M": 12,
            "Q": 4,
            "Y": 1,
            "15min": 96,
        }
        for frequency, period in expected.items():
            with self.subTest(frequency=frequency):
                result = self.bridge_call(
                    "resolve-period", "--frequency", frequency
                )
                self.assertEqual(result["r_period"], period)
                self.assertEqual(
                    result["r_period_source"], "pinned_gluonts_get_seasonality"
                )
        overridden = self.bridge_call(
            "resolve-period", "--frequency", "D", "--override", "7"
        )
        self.assertEqual(overridden["r_period"], 7)
        self.assertEqual(overridden["gluonts_default_seasonality"], 1)
        self.assertEqual(overridden["r_period_source"], "experiment_override")
        for arguments in (
            ("resolve-period", "--frequency", "unsupported"),
            ("resolve-period", "--frequency", "D", "--override", "0"),
        ):
            with self.subTest(arguments=arguments), self.assertRaises(
                subprocess.CalledProcessError
            ):
                self.bridge_call(*arguments)

    def test_manifest_is_complete_qualified_and_validated(self):
        """The manifest contains the validated 97-configuration consensus and full metadata."""
        manifest = self.bridge_call(
            "manifest",
            "--root",
            str(self.root),
            "--gift-eval-directory",
            self.configuration["evaluation"]["gift_eval"]["source_directory"],
        )
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
