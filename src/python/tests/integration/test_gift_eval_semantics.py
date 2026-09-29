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

import numpy as np
import pandas as pd
from gift_eval.data import Dataset
from gluonts.dataset.common import ListDataset
from gluonts.dataset.split import split
from gluonts.ev.metrics import MAE
from gluonts.model import evaluate_forecasts
from gluonts.model.forecast import QuantileForecast
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

    def test_missing_actual_is_masked_instead_of_becoming_zero_or_failure(self):
        """Official masking excludes NaN labels while finite forecasts remain valid."""
        dataset = ListDataset(
            [{
                "item_id": "missing-label",
                "start": pd.Period("2020-01-01", freq="D"),
                "target": [1.0, 2.0, 3.0, np.nan, 5.0],
            }],
            freq="D",
        )
        _, template = split(dataset, offset=-2)
        test_data = template.generate_instances(prediction_length=2, windows=1)
        context = next(iter(test_data.input))
        forecast = QuantileForecast(
            forecast_arrays=np.asarray([[4.0, 4.0], [4.0, 4.0]]),
            forecast_keys=["mean", "0.5"],
            start_date=context["start"] + len(context["target"]),
            item_id="missing-label",
        )
        result = evaluate_forecasts(
            iter([forecast]),
            test_data=test_data,
            metrics=[MAE()],
            axis=None,
            mask_invalid_label=True,
            allow_nan_forecast=False,
            seasonality=1,
        )
        # Only the finite label 5 is scored: abs(5 - 4) = 1. Treating the
        # missing label as zero would instead produce an aggregate MAE of 2.5.
        self.assertEqual(float(result["MAE[0.5]"].iloc[0]), 1.0)


if __name__ == "__main__":
    unittest.main()
