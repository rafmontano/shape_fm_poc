"""Process 04 native-provider regressions protecting the fixed directional science.

Asymmetric queries and tied references check horizon reuse, stable identity ties
and independent RF horizon labels through the real pinned provider environment.
"""

import json
import tempfile
import unittest
from pathlib import Path

from p04_forecast.application import calculate_forecast_batch, calculate_individual_forecast
from p04_forecast.providers import native_request
from util.runtime import CODE_ROOT, ExperimentRequest, WorkItem, open_experiment


class MVPForecastTests(unittest.TestCase):
    """Native fit/predict responses preserve approved directional contracts."""

    def test_archived_forecasts_honor_placement_and_skip_accepted_items(self):
        """Archived providers use configured Dask resources without changing identity."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "config").mkdir()
            document = json.loads((CODE_ROOT / "config/experiment.json").read_text())
            (root / "config/experiment.json").write_text(json.dumps(document))
            context = open_experiment(ExperimentRequest((4,), root, execution_profile="mvp_two_machine"))
            try:
                context.execution.settings["providers"]["archived"] = {"resources": {"ARCHIVE_TEST": 1}}
                context.repository.accept("01.02", [("series", {
                    "dataset_id": "fixture", "horizon": 14, "source_row": 0})])
                context.windows().accept("03.02", [("official", {
                    "kind": "official", "series_key": "series", "series_id": "D1",
                    "frequency": "D", "r_period": 1, "point_context": [1.0] * 64,
                    "point_state": {}})])
                reference = context.ref("03.04")
                pending = context.forecast.pending_individual_forecasts(reference)
                archived = [item for item in pending if item.payload["provider"] == "archived"]
                self.assertEqual({item.key for item in archived}, {"official/m4_smyl", "official/m4_fforma"})
                self.assertTrue(all(item.resources == {"ARCHIVE_TEST": 1} for item in archived))
                self.assertTrue(all("ARCHIVE_TEST" not in item.resources for item in pending
                                    if item.payload["provider"] != "archived"))
                context.repository.accept("04.01", [("official/m4_smyl", {"accepted_fixture": True})])
                resumed = context.forecast.pending_individual_forecasts(reference)
                self.assertNotIn("official/m4_smyl", {item.key for item in resumed})
                self.assertIn("official/m4_fforma", {item.key for item in resumed})
            finally:
                context.close()

    def test_partial_native_batch_failure_preserves_complete_forecast(self):
        """A later provider error preserves the earlier horizon and its restart key."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "config").mkdir()
            document = json.loads((CODE_ROOT / "config/experiment.json").read_text())
            (root / "config/experiment.json").write_text(json.dumps(document))
            context = open_experiment(ExperimentRequest((4,), root))
            try:
                items = []
                for key, model in (("a", "naive2"), ("b", "unregistered")):
                    request = {"contract_version": "forecast-v1", "experiment_id": context.experiment_ref.experiment_id,
                        "task_id": key, "forecast_instance_id": key, "variant_id": model,
                        "dataset_id": "fixture", "series_id": key, "model_id": model,
                        "required_capability": "mean_only", "context": [i / 2 for i in range(64)],
                        "horizon": 14, "frequency": "D", "seasonal_period": 1,
                        "input_scale": "transformed", "quantile_levels": None, "seed": 1234, "model_settings": {}}
                    items.append(WorkItem(key, {"input_key": key, "model": model, "provider": "r", "request": request,
                        "execution": {"timeout": 60}, "state": {"recipe": "standardise_sample_v1", "version": 1,
                            "centre": 10.0, "scale": 2.0, "count": 64, "constant": False}}))
                pending = context.repository.plan("04.01", items)
                with self.assertRaisesRegex(RuntimeError, "Unsupported R model"):
                    context.execution.run(subflow_id="04.01", items=pending, calculate=calculate_individual_forecast,
                        calculate_batch=calculate_forecast_batch, accept=context.forecast.accept_individual_forecasts,
                        on_progress=lambda snapshot: None)
                accepted = context.repository.rows("04.01")
                self.assertEqual(set(accepted), {"a"})
                self.assertEqual(accepted["a"]["mean"], [73.0] * 14)
                self.assertIsNone(accepted["a"]["quantiles"])
                self.assertEqual([item.key for item in context.repository.plan("04.01", items)], ["b"])
            finally:
                context.close()

    def test_fixed_dtw_one_neighbour_all_horizons_and_identity_tie(self):
        """Identical-distance neighbours with opposite labels resolve by identity."""
        query = [float(i * i % 13 - i / 3) for i in range(64)]
        other = [float(i % 3 + 40) for i in range(64)]
        labels = [1, 0, 1, 1, 0, 0, 1, 0, 0, 0, 1, 1, 0, 1]
        settings = {"environment": "environments/classifiers/.venv", "timeout": 120}
        fitted = native_request("classifiers", settings, {"command": "fit", "model": "directional_dtw",
            "window": 0.10, "references": [
                {"identity": "z-tied", "values": query, "labels": [1 - value for value in labels]},
                {"identity": "b-other", "values": other, "labels": [0] * 14},
                {"identity": "a-tied", "values": query, "labels": labels}]})
        result = native_request("classifiers", settings, {"command": "predict", "model": "directional_dtw",
            "artifacts": [fitted["artifact"]], "values": query})
        self.assertEqual(result["labels"], labels)
        self.assertEqual(result["nearest_reference"], "a-tied")
        self.assertEqual(result["effective_width"], 6)
        self.assertEqual(result["distance"], 0.0)
        result = native_request("classifiers", settings, {"command": "predict", "model": "directional_dtw",
            "artifacts": [fitted["artifact"]], "values": other})
        self.assertEqual(result["nearest_reference"], "b-other")
        self.assertEqual(result["labels"], [0] * 14)

    def test_random_forest_uses_independent_horizon_labels(self):
        """Opposite horizon labels cannot collapse into one reused classifier."""
        document = json.loads((CODE_ROOT / "config/experiment.json").read_text())
        settings = {"environment": "environments/classifiers/.venv", "timeout": 120}
        values = [[float(sign * (i % 7 + 2)) for i in range(256)]
                  for sign in [-1] * 12 + [1] * 12]
        artifacts = []
        for horizon in range(14):
            labels = [horizon % 2] * 12 + [1 - horizon % 2] * 12
            result = native_request("classifiers", settings, {"command": "fit", "model": "directional_mantis_rf",
                "values": values, "labels": labels,
                "parameters": document["classifiers"]["random_forest"]["parameters"]})
            artifacts.append(result["artifact"])
        for index, expected in ((0, [h % 2 for h in range(14)]),
                                (23, [1 - h % 2 for h in range(14)])):
            result = native_request("classifiers", settings, {"command": "predict", "model": "directional_mantis_rf",
                "values": values[index], "artifacts": artifacts})
            self.assertEqual(result["labels"], expected)
