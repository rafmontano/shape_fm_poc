"""Process 04 native-provider regressions protecting the fixed directional science.

Asymmetric queries and tied references check horizon reuse, stable identity ties
and independent RF horizon labels through the real pinned provider environment.
"""

import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from p04_forecast.application import calculate_forecast_batch, calculate_individual_forecast
from p04_forecast.providers import native_request
from util.runtime import CODE_ROOT, ExperimentRequest, WorkItem, open_experiment


class MVPForecastTests(unittest.TestCase):
    """Native fit/predict responses preserve approved directional contracts."""

    def test_paper_baselines_use_raw_pre_origin_history_without_inversion(self):
        """Cleaning/scaling affect peers, never raw paper points or quantiles."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "config").mkdir()
            document = json.loads((CODE_ROOT / "config/experiment.json").read_text())
            document["models"] = {m: document["models"][m] for m in ("naive2", "chronos_2", "ses")}
            (root / "config/experiment.json").write_text(json.dumps(document))
            context = open_experiment(ExperimentRequest((4,), root))
            try:
                key = "series/official/000000003"
                context.repository.accept("01.02", [("series", {"dataset_id": "fixture",
                    "horizon": 14, "test_start": 3, "target": [2., 5., 9.] + [999.] * 14})])
                archives = [(f"{key}/{model}", {"model": model, "mean": [17.] * 14})
                            for model in document["archived_forecasts"]["enabled"]]
                context.repository.accept("04.01", archives)
                context.windows().accept("03.02", [(key, {"kind": "official", "series_key": "series",
                    "series_id": "fixture", "frequency": "D", "r_period": 1,
                    "point_context": [-1., 0., 1.], "point_state": {"centre": 5., "scale": 2.}})])
                items = context.forecast.pending_individual_forecasts(context.ref("03.04"))
                by_model = {item.payload["model"]: item for item in items}
                request = by_model["naive2"].payload["request"]
                self.assertEqual(request["context"], [2., 5., 9.])
                self.assertEqual((request["seasonal_period"], request["horizon"], request["input_scale"]),
                                 (1, 14, "original"))
                chronos = by_model["chronos_2"]
                self.assertEqual(chronos.payload["request"]["context"], [2., 5., 9.])
                self.assertEqual(chronos.payload["request"]["input_scale"], "original")
                peer = by_model["ses"].payload
                self.assertEqual(peer["request"]["context"], [-1., 0., 1.])
                self.assertEqual(peer["request"]["input_scale"], "transformed")
                self.assertEqual(peer["state"], {"centre": 5., "scale": 2.})
                result = calculate_individual_forecast(by_model["naive2"])
                self.assertEqual(result["mean"], [9.] * 14)
                context.forecast.accept_individual_forecasts([(by_model["naive2"].key, result)])
                levels = chronos.payload["request"]["quantile_levels"]
                quantiles = [[10. + index] * 14 for index in range(len(levels))]
                median = quantiles[levels.index(.5)]
                native = {**result, "task_id": chronos.key, "forecast_instance_id": chronos.key,
                    "variant_id": "chronos_2", "requested_model_id": "chronos_2", "executed_model_id": "chronos_2",
                    "forecast_capability": "probabilistic", "mean": median, "median": median,
                    "quantile_levels": levels, "quantiles": quantiles}
                with patch("p04_forecast.application.native_request", return_value={"results": [native]}):
                    calculated = calculate_individual_forecast(chronos)
                self.assertEqual(calculated["mean"], median)
                self.assertEqual(calculated["median"], median)
                self.assertEqual(calculated["quantiles"], quantiles)
                context.forecast.accept_individual_forecasts([(chronos.key, calculated)])
                self.assertEqual(context.repository.rows("04.01", keys=[k for k, _ in archives]), dict(archives))
                self.assertEqual([item.payload["model"] for item in
                    context.forecast.pending_individual_forecasts(context.ref("03.04"))], ["ses"])
            finally:
                context.close()

    def test_chronos_paper_point_is_native_median_not_quantile_average(self):
        """The provider's point remains native even when quantiles are rearranged."""
        subprocess.run([str(CODE_ROOT / "environments/chronos-2/.venv/bin/python"), "-c", '''
from types import SimpleNamespace
import torch
from p04_forecast.chronos import predict
pipeline = SimpleNamespace(predict_quantiles=lambda **kwargs: (
    [torch.tensor([[[9., 3., 7.], [1., 8., 4.]]])], [torch.tensor([[3., 8.]])]))
job = {"contract_version": "forecast-v1", "experiment_id": "fixture", "task_id": "x",
    "forecast_instance_id": "x", "variant_id": "chronos_2", "model_id": "chronos_2",
    "context": [2., 5., 9.], "input_scale": "original", "horizon": 2,
    "model_settings": {"revision": "fixture"}, "seed": 1234}
row = predict(pipeline, {"jobs": [job], "batch_id": "fixture", "horizon": 2,
    "quantile_levels": [.1, .5, .9], "inference_batch_size": 1,
    "cross_learning": False, "predict_batches_jointly": False})["results"][0]
assert row["mean"] == [3., 8.]
assert row["median"] == [7., 4.]
assert row["quantiles"] == [[3., 1.], [7., 4.], [9., 8.]]
'''], cwd=CODE_ROOT, check=True, timeout=60)

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
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "model.pickle"
            shutil.move(fitted["temporary_path"], path)
            result = native_request("classifiers", settings, {"command": "predict", "model": "directional_dtw",
                "model_paths": [str(path)], "values": query})
            self.assertEqual(result["labels"], labels)
            self.assertEqual(result["nearest_reference"], "a-tied")
            self.assertEqual(result["effective_width"], 6)
            self.assertEqual(result["distance"], 0.0)
            path.unlink()  # Later queries reuse the loaded object, not another file read.
            result = native_request("classifiers", settings, {"command": "predict", "model": "directional_dtw",
                "model_paths": [str(path)], "values": other})
            self.assertEqual(result["nearest_reference"], "b-other")
            self.assertEqual(result["labels"], [0] * 14)

    def test_random_forest_uses_independent_horizon_labels(self):
        """Opposite horizon labels cannot collapse into one reused classifier."""
        document = json.loads((CODE_ROOT / "config/experiment.json").read_text())
        settings = {"environment": "environments/classifiers/.venv", "timeout": 120}
        values = [[float(sign * (i % 7 + 2)) for i in range(256)]
                  for sign in [-1] * 12 + [1] * 12]
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for horizon in range(14):
                labels = [horizon % 2] * 12 + [1 - horizon % 2] * 12
                result = native_request("classifiers", settings, {"command": "fit", "model": "directional_mantis_rf",
                    "values": values, "labels": labels,
                    "parameters": document["classifiers"]["random_forest"]["parameters"]})
                path = Path(directory) / f"horizon_{horizon + 1:02d}.pickle"
                shutil.move(result["temporary_path"], path)
                paths.append(str(path))
            for index, expected in ((0, [h % 2 for h in range(14)]),
                                    (23, [1 - h % 2 for h in range(14)])):
                result = native_request("classifiers", settings, {"command": "predict", "model": "directional_mantis_rf",
                    "values": values[index], "model_paths": paths})
                self.assertEqual(result["labels"], expected)
