"""Dependency-heavy checks against the pinned official GIFT-Eval package."""

import json
import os
import unittest
from pathlib import Path

from gift_eval.data import Dataset


class GiftEvalSemanticsTests(unittest.TestCase):
    def test_m4_daily_contract_matches_official_package(self):
        root = Path(__file__).resolve().parents[2]
        source_root = Path(
            os.environ.get("SHAPEFM_GIFT_EVAL_ROOT", root / "data/source/gift_eval")
        )
        os.environ["GIFT_EVAL"] = str(source_root)
        with (root / "config/imports/m4_daily.json").open(encoding="utf-8") as stream:
            config = json.load(stream)

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
