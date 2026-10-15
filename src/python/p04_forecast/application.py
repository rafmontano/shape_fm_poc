"""Process 04 forecast pool, reusable representations and directional artifacts.

Accepted Process 03 inputs/settings produce parent forecasts, embeddings, model
references and predictions. Workers calculate; this application alone accepts
returned artifacts/results on the coordinator. No calibration or combination.
Scientific definitions: docs/forecast-black-box-contract.md (native forecasts
and affine inversion) and docs/directional-models.md (representations/classifiers).
"""

from __future__ import annotations

import base64
import hashlib
import math
from typing import Any, Iterator

from p04_forecast.providers import native_request
from p04_forecast.contract import ForecastContract
from util.providers import run_r
from util.runtime import DataRef, WorkItem
from util.transformations import inverse_transformation


def calculate_individual_forecast(item: WorkItem) -> dict[str, Any]:
    """Calculate one complete native horizon without access to future actuals."""
    return next(calculate_forecast_batch([item]))[1]


def calculate_forecast_batch(items: list[WorkItem]) -> Iterator[tuple[str, dict[str, Any]]]:
    """Call the accepted forecast black boxes and invert stored affine state.

    docs/forecast-black-box-contract.md owns settings, capabilities and permitted
    STL fallback. Archived vectors are original-scale mean-only references.
    """
    for provider in sorted({item.payload["provider"] for item in items}):
        group = [item for item in items if item.payload["provider"] == provider]
        settings = group[0].payload["execution"]
        if provider == "archived":
            response = run_r("src/r/p04_forecast/archived_forecast_worker.R", {
                "action": "reference_forecast", "jobs": [item.payload["request"] for item in group]},
                timeout=settings["timeout"], threads=1)
            results = response["records"]
            by_key = {(row["series_id"], row["forecast_id"]): row for row in results}
            for item in group:
                row = by_key[(item.payload["request"]["series_id"], item.payload["model"])]
                yield item.key, {**row, "input_key": item.payload["input_key"],
                    "model": item.payload["model"], "output_scale": "original"}
            continue
        requests = [item.payload["request"] for item in group]
        if provider == "r":
            response = run_r("src/r/p04_forecast/forecast_worker.R", {"action": "forecast", "jobs": requests},
                timeout=settings["timeout"], threads=1)
        else:
            response = native_request("chronos", settings, {"command": "predict", "batch_id": group[0].key,
                "jobs": requests, "horizon": requests[0]["horizon"],
                "quantile_levels": requests[0]["quantile_levels"], "inference_batch_size": len(group),
                "predict_batches_jointly": False, "cross_learning": False})
        by_key = {row["task_id"]: row for row in response["results"]}
        for item in group:
            result = ForecastContract().validate_result(item.payload["request"], by_key[item.key])
            if result["status"] != "success":
                raise RuntimeError(f"forecast {item.key}: {result['error_message']}")
            result = dict(result)
            state = item.payload["state"]
            for field in ("mean", "median"):
                if result[field] is not None:
                    result[field] = list(inverse_transformation(result[field], state))
            if result["quantiles"] is not None:
                result["quantiles"] = [list(inverse_transformation(row, state)) for row in result["quantiles"]]
            result.update(input_key=item.payload["input_key"], model=item.payload["model"], output_scale="original")
            yield item.key, result


def calculate_representation(item: WorkItem) -> dict[str, Any]:
    """Calculate one frozen final-layer CLS embedding from a label-free input."""
    return calculate_representation_batch([item])[0][1]


def calculate_representation_batch(items: list[WorkItem]) -> list[tuple[str, dict[str, Any]]]:
    """Reuse a worker's pinned Mantis model for a bounded group of inputs."""
    response = native_request("mantis", items[0].payload["execution"],
        {"command": "encode", "values": [item.payload["values"] for item in items]})
    if len(response["vectors"]) != len(items):
        raise ValueError("Mantis returned an unexpected number of embeddings")
    outputs = []
    for item, vector in zip(items, response["vectors"], strict=True):
        if len(vector) != 256 or not all(math.isfinite(value) for value in vector):
            raise ValueError(f"invalid Mantis embedding: {item.key}")
        outputs.append((item.key, {"input_key": item.payload["input_key"], "values": vector,
            "method": "mantis_8m_legacy_cls", "provider": response["provider"]}))
    return outputs


def calculate_decision_model(item: WorkItem) -> dict[str, Any]:
    """Fit one configured training unit; return trusted native bytes and lineage."""
    response = native_request("classifiers", item.payload["execution"],
        {"command": "fit", **item.payload["request"]})
    return {**response, "model": item.payload["model"], "horizon": item.payload["horizon"],
            "reference_keys": item.payload["reference_keys"]}


def calculate_directional_prediction(item: WorkItem, *, artifacts: dict[str, bytes]) -> dict[str, Any]:
    """Return complete binary labels from accepted artifacts, never fit on queries."""
    response = native_request("classifiers", item.payload["execution"],
        {"command": "predict", **item.payload["request"]}, artifacts=artifacts)
    labels = response["labels"]
    if len(labels) != 14 or any(type(value) is not int or value not in (0, 1) for value in labels):
        raise ValueError(f"invalid directional labels: {item.key}")
    return {**response, "input_key": item.payload["input_key"], "model": item.payload["model"],
            "model_keys": item.payload["model_keys"]}


class Application:
    """Own forecast plans and coordinator-only atomic acceptance for Process 04.

    Individual horizons, reusable embeddings, frequency/horizon model units and
    complete directional vectors are durable atomic boundaries in the parent.
    """

    calculate_individual_forecast = staticmethod(calculate_individual_forecast)
    calculate_forecast_batch = staticmethod(calculate_forecast_batch)
    calculate_representation = staticmethod(calculate_representation)
    calculate_representation_batch = staticmethod(calculate_representation_batch)
    calculate_decision_model = staticmethod(calculate_decision_model)
    calculate_directional_prediction = staticmethod(calculate_directional_prediction)

    def __init__(self, context: Any):
        """Bind accepted configuration, coordinator repository and artifact root."""
        self.context, self.store = context, context.repository
        self.configuration = context.configuration
        self.model_root = context.request.root / "models"

    def modelling_dataset(self) -> DataRef:
        """Return the accepted Process 03 modelling reference."""
        return self.context.ref("03.04")

    def forecast_pool(self) -> DataRef:
        """Return the stored individual/directional pool consumed by Process 05."""
        return self.context.ref("04.05")

    def reconcile_fitted_models(self) -> None:
        """Make missing fitted files eligible again without repeating stored predictions.

        Per docs/directional-models.md, an artifact is part of a fitted unit's
        durable boundary. Existing forecasts/embeddings/predictions stay accepted;
        only missing model units and their enclosing completion are reopened.
        """
        missing = [key for key, row in self.store.rows("04.03").items()
                   if not (self.context.request.root / row["path"]).is_file()]
        if not missing:
            return
        connection = self.store.connection
        connection.begin()
        try:
            connection.executemany("DELETE FROM atomic_results WHERE subflow_id='04.03' AND atomic_key=?",
                                   [(key,) for key in missing])
            connection.execute("DELETE FROM milestones WHERE milestone_id IN ('04','04.03','04.05')")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise

    def _execution(self, provider: str) -> dict[str, Any]:
        execution = self.configuration["execution"]
        settings = {"timeout": execution["default"]["worker_timeouts_seconds"].get(provider, 1800)}
        if provider != "r":
            settings["environment"] = execution["paths"][f"{provider}_environment"]
            settings["device"] = self.context.execution.settings.get("providers", {}).get(provider, {}).get("device", "cpu")
        if provider == "chronos":
            settings["environment"] = execution["paths"]["chronos_environment"]
            settings["model"] = self.configuration["models"]["chronos_2"]
        return settings

    def pending_individual_forecasts(self, modelling: DataRef) -> list[WorkItem]:
        """Plan official-origin native and archived forecasts with past-only requests."""
        canonical = self.store.rows("01.02")
        items = []
        for input_key, row in self.context.windows().rows("03.02").items():
            if row["kind"] != "official":
                continue
            source = canonical[row["series_key"]]
            for model, configuration in self.configuration["models"].items():
                if model.startswith("directional_"):
                    continue
                key = f"{input_key}/{model}"
                capability = "mean_only" if model in ("ses", "holt", "damped", "naive2") else "probabilistic"
                provider = "chronos" if model == "chronos_2" else "r"
                levels = self.configuration["models"]["chronos_2"]["quantile_levels"] if capability == "probabilistic" else None
                request = {"contract_version": "forecast-v1", "experiment_id": self.context.experiment_ref.experiment_id,
                    "task_id": key, "forecast_instance_id": key, "variant_id": model,
                    "dataset_id": source["dataset_id"], "series_id": row["series_id"], "model_id": model,
                    "required_capability": capability, "context": row["point_context"], "horizon": source["horizon"],
                    "frequency": row["frequency"], "seasonal_period": row["r_period"],
                    "input_scale": "transformed", "quantile_levels": levels,
                    "seed": self.configuration["reproducibility"]["seed"],
                    "model_settings": configuration.get("settings", configuration)}
                items.append(WorkItem(key, {"input_key": input_key, "model": model, "provider": provider,
                    "request": request, "state": row["point_state"], "execution": self._execution(provider)},
                    resources=self.context.execution.resources(provider)))
            for model in self.configuration["archived_forecasts"]["enabled"]:
                key = f"{input_key}/{model}"
                request = {"dataset_id": source["dataset_id"], "series_id": row["series_id"],
                    "official_position": source["source_row"], "horizon": source["horizon"], "forecast_ids": [model]}
                items.append(WorkItem(key, {"input_key": input_key, "model": model, "provider": "archived",
                    "request": request, "execution": self._execution("r")},
                    resources=self.context.execution.resources("archived")))
        return self.store.plan("04.01", items)

    def accept_individual_forecasts(self, results: list[tuple[str, dict[str, Any]]]) -> None:
        """Store complete original-scale individual results before any combination."""
        self.store.accept("04.01", results)

    def pending_representations(self, modelling: DataRef) -> list[WorkItem]:
        """Plan shared training and uncapped official Mantis inputs, without labels."""
        inputs = self.context.windows().rows("03.02")
        required = set(self.store.rows("reference_cohort")["S1"]["input_keys"])
        required.update(key for key, row in inputs.items() if row["kind"] == "official")
        return self.store.plan("04.02", [WorkItem(key, {"input_key": key, "values": inputs[key]["values"],
            "execution": self._execution("mantis")}, resources=self.context.execution.resources("mantis"))
            for key in sorted(required)])

    def accept_representations(self, results: list[tuple[str, dict[str, Any]]]) -> None:
        """Store reusable embeddings in the parent, independently of classifiers."""
        self.store.accept("04.02", results)

    def pending_decision_models(self, modelling: DataRef) -> list[WorkItem]:
        """Plan one fixed DTW library and fourteen RF units for the common cohort."""
        inputs, representations = self.context.windows().rows("03.02"), self.store.rows("04.02")
        references = self.store.rows("reference_cohort")["S1"]["input_keys"]
        items = []
        for frequency in self.configuration["pipeline"]["window_preparation"]["selected_frequencies"]:
            keys = [key for key in references if inputs[key]["frequency"] == frequency]
            if not keys:
                raise ValueError(f"no shared training references for {frequency}")
            for model in ("directional_dtw", "directional_mantis_rf"):
                horizons = [None] if model == "directional_dtw" else list(range(1, 15))
                for horizon in horizons:
                    key = f"{frequency}/{model}/{horizon or 'all'}"
                    if key in self.store.keys("04.03"):
                        continue
                    request = {"model": model}
                    if model == "directional_dtw":
                        request.update(window=self.configuration["models"][model]["constraint"]["window_proportion"],
                            references=[{"identity": k, "values": inputs[k]["values"], "labels": inputs[k]["labels"]} for k in keys])
                    else:
                        request.update(values=[representations[k]["values"] for k in keys],
                            labels=[inputs[k]["labels"][horizon - 1] for k in keys],
                            parameters=self.configuration["classifiers"]["random_forest"]["parameters"])
                    items.append(WorkItem(key, {"model": model, "horizon": horizon, "reference_keys": keys,
                        "request": request, "execution": self._execution("classifiers")}))
        return self.store.plan("04.03", items)

    def accept_decision_models(self, results: list[tuple[str, dict[str, Any]]]) -> None:
        """Publish trusted native artifacts atomically, then accept their lineage.

        The sole coordinator owns authoritative model files. A failed database
        acceptance may leave a disposable file, never an accepted partial model.
        """
        accepted = []
        for key, result in results:
            content = base64.b64decode(result["artifact"], validate=True)
            frequency, model, _ = key.split("/")
            frequency_name = {"D": "daily", "W": "weekly", "M": "monthly",
                              "Q": "quarterly", "Y": "yearly", "H": "hourly"}[frequency]
            filename = "model.pickle" if result["horizon"] is None else f"horizon_{result['horizon']:02d}.pickle"
            path = self.model_root / model / frequency_name / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_suffix(".partial")
            temporary.write_bytes(content)
            temporary.replace(path)
            accepted.append((key, {k: value for k, value in result.items() if k != "artifact"} | {
                "path": str(path.relative_to(self.context.request.root)), "size": len(content),
                "sha256": hashlib.sha256(content).hexdigest()}))
        self.store.accept("04.03", accepted)

    def _artifact(self, row: dict[str, Any]) -> bytes:
        content = (self.context.request.root / row["path"]).read_bytes()
        if len(content) != row["size"] or hashlib.sha256(content).hexdigest() != row["sha256"]:
            raise ValueError(f"fitted artifact integrity mismatch: {row['path']}")
        return content

    def directional_artifacts(self, items: list[WorkItem]) -> dict[str, bytes]:
        """Read verified accepted fitted bytes once for the pending prediction plan."""
        required = {key for item in items for key in item.payload["request"]["artifact_ids"]}
        return {row["sha256"]: self._artifact(row) for row in self.store.rows("04.03").values()
                if row["sha256"] in required}

    def pending_directional_predictions(self, modelling: DataRef) -> list[WorkItem]:
        """Plan complete official vectors using accepted model artifacts and no actuals."""
        inputs = self.context.windows().rows("03.02")
        models, representations = self.store.rows("04.03"), self.store.rows("04.02")
        items = []
        for input_key, row in inputs.items():
            if row["kind"] != "official":
                continue
            for model in ("directional_dtw", "directional_mantis_rf"):
                key = f"{input_key}/{model}"
                model_keys = [f"{row['frequency']}/{model}/{h}" for h in
                              (["all"] if model == "directional_dtw" else range(1, 15))]
                request = {"model": model, "artifact_ids": [models[k]["sha256"] for k in model_keys],
                    "values": row["values"] if model == "directional_dtw" else representations[input_key]["values"]}
                items.append(WorkItem(key, {"input_key": input_key, "model": model, "model_keys": model_keys,
                    "request": request, "execution": self._execution("classifiers")}))
        return self.store.plan("04.04", items)

    def accept_directional_predictions(self, results: list[tuple[str, dict[str, Any]]]) -> None:
        """Accept binary complete horizons and their fitted-model lineage in the parent."""
        self.store.accept("04.04", results)

    def accept_forecast_pool(self, modelling: DataRef) -> DataRef:
        """Require every planned forecast, representation, model and prediction key."""
        for subflow in ("04.01", "04.02", "04.03", "04.04"):
            self.store.require_complete(subflow)
        return self.forecast_pool()
