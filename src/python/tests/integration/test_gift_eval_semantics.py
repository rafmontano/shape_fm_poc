# ==============================================================================
# test_gift_eval_semantics.py
#
# Purpose: Verify pinned GIFT-Eval dataset metadata, split, metric, and quantile semantics used by experiment planning.
# Inputs: unittest fixtures, temporary databases/files, deterministic synthetic records, and mocked process or cluster boundaries.
# Outputs: unittest pass/fail assertions and captured diagnostics; no production artifacts or external services.
# Run from: PYTHONPATH=src/python .tools/uv/uv run --project environments/gift-eval --locked --no-sync python -m unittest tests.integration.test_gift_eval_semantics
# ==============================================================================

"""Verify pinned GIFT-Eval dataset metadata, split, metric, and quantile semantics used by experiment planning."""

import os
import unittest
from pathlib import Path

from gift_eval.data import Dataset
from util.configuration import load_experiment_configuration


class GiftEvalSemanticsTests(unittest.TestCase):
    """Purpose: Verify pinned GIFT-Eval metadata and temporal split semantics.

    Inputs: The committed experiment configuration and local pinned M4 Daily dataset.
    Outputs: Metadata and split assertions; sets ``GIFT_EVAL`` and reads dataset files only.
    """
    def test_m4_daily_contract_matches_official_package(self):
        """M4 Daily configuration and train, validation, and test boundaries match GIFT-Eval."""
        root = Path(__file__).resolve().parents[4]
        # Test/calibration value: SHAPEFM_GIFT_EVAL_ROOT is a machine-environment
        # override for this fixture; it does not override production data authority.
        source_root = Path(
            os.environ.get("SHAPEFM_GIFT_EVAL_ROOT", root / "data/source/gift_eval")
        )
        os.environ["GIFT_EVAL"] = str(source_root)
        config = load_experiment_configuration(
            root / "config/experiments/poc2_m4_daily_100.json"
        ).resolved["data"]

        dataset = Dataset("m4_daily", term="short")
        self.assertEqual(dataset.freq, config["benchmark"]["frequency"])
        self.assertEqual(
            dataset.prediction_length, config["benchmark"]["prediction_length"]
        )
        self.assertEqual(dataset.windows, config["benchmark"]["evaluation_windows"])

        source_length = len(dataset.hf_dataset[0]["target"])
        training = next(iter(dataset.training_dataset))
        validation = next(iter(dataset.validation_dataset))
        test_input = next(iter(dataset.test_data.input))
        test_label = next(iter(dataset.test_data.label))
        horizon = dataset.prediction_length
        self.assertEqual(len(training["target"]), source_length - 2 * horizon)
        self.assertEqual(len(validation["target"]), source_length - horizon)
        self.assertEqual(len(test_input["target"]), source_length - horizon)
        self.assertEqual(len(test_label["target"]), horizon)


if __name__ == "__main__":
    unittest.main()
