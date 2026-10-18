"""Process 04 forecast pool, reusable representations and directional artifacts.

Accepted Process 03 inputs/settings produce parent forecasts, embeddings, model
references and predictions. Workers calculate; this application alone accepts
returned artifacts/results on the coordinator. No calibration or combination.
Scientific definitions: docs/forecast-black-box-contract.md (native forecasts
and affine inversion) and docs/directional-models.md (representations/classifiers).
"""

from __future__ import annotations

import json
import shutil
import socket
import subprocess
from typing import Any, Iterator

from p04_forecast.providers import native_request
from p04_forecast.contract import ForecastContract
from util.providers import run_r
from util.runtime import CODE_ROOT, DataRef, WorkItem
from util.transformations import inverse_transformation


def calculate_individual_forecast(item: WorkItem) -> dict[str, Any]:
    """Calculate one complete native horizon without access to future actuals."""
    return next(calculate_forecast_batch([item]))[1]


def calculate_forecast_batch(items: list[WorkItem]) -> Iterator[tuple[str, dict[str, Any]]]:
    """Call the accepted forecast black boxes and invert stored affine state.

    docs/forecast-black-box-contract.md owns settings, capabilities and permitted
    STL fallback. Archived vectors are already accepted by Process01.
    """
    for provider in sorted({item.payload["provider"] for item in items}):
        group = [item for item in items if item.payload["provider"] == provider]
        settings = group[0].payload["execution"]
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
            if state:
                for field in ("mean", "median"):
                    if result[field] is not None:
                        result[field] = list(inverse_transformation(result[field], state))
                if result["quantiles"] is not None:
                    result["quantiles"] = [list(inverse_transformation(row, state)) for row in result["quantiles"]]
            result.update(input_key=item.payload["input_key"], model=item.payload["model"],
                          output_scale="original", origin="calculated")
            yield item.key, result


def calculate_representation(item: WorkItem) -> dict[str, Any]:
    """Calculate one frozen final-layer CLS embedding from a label-free input."""
    return calculate_representation_batch([item])[0][1]


def calculate_representation_batch(items: list[WorkItem]) -> list[tuple[str, dict[str, Any]]]:
    """Reuse a worker's pinned Mantis model for a bounded group of inputs."""
    response = native_request("mantis", items[0].payload["execution"],
        {"command": "encode", "values": [item.payload["values"] for item in items]})
    outputs = []
    for item, vector in zip(items, response["vectors"], strict=True):
        outputs.append((item.key, {"input_key": item.payload["input_key"], "values": vector,
            "method": "mantis_8m_legacy_cls", "provider": response["provider"]}))
    return outputs


def calculate_decision_model(item: WorkItem) -> dict[str, Any]:
    """Fit one configured training unit; return a temporary file and lineage."""
    response = native_request("classifiers", item.payload["execution"],
        {"command": "fit", **item.payload["request"]})
    return {**response, "model": item.payload["model"], "horizon": item.payload["horizon"],
            "reference_keys": item.payload["reference_keys"]}


def calculate_directional_prediction(item: WorkItem) -> dict[str, Any]:
    """Return complete binary labels from accepted artifacts, never fit on queries."""
    response = native_request("classifiers", item.payload["execution"],
        {"command": "predict", **item.payload["request"]})
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
        """Require imported archives, then plan only missing calculated forecasts."""
        canonical = self.store.rows("01.02", fields=("dataset_id", "horizon", "test_start"))
        accepted = self.store.keys("04.01")
        archived = [f"{key}/official/{source['test_start']:09d}/{model}"
                    for key, source in canonical.items()
                    for model in self.configuration["archived_forecasts"]["enabled"]]
        missing = set(archived) - accepted
        if missing:
            raise RuntimeError(f"04.01: incomplete Process01 archived import prerequisite (01.04): "
                               f"{len(missing)} missing; first={min(missing)}")
        required = {f"{key}/official/{source['test_start']:09d}" for key, source in canonical.items()
                    if any(f"{key}/official/{source['test_start']:09d}/{model}" not in accepted
                           for model in self.configuration["models"] if not model.startswith("directional_"))}
        raw_keys = [key for key, source in canonical.items()
                    if any(model in self.configuration["models"] and
                           f"{key}/official/{source['test_start']:09d}/{model}" not in accepted
                           for model in ("naive2", "chronos_2"))]
        raw = self.store.rows("01.02", keys=raw_keys, fields=("target",))
        items = []
        for input_key, row in self.context.windows().rows("03.02", keys=required, kind="official",
                fields=("series_key", "series_id", "point_context", "frequency", "r_period", "point_state")).items():
            source = canonical[row["series_key"]]
            for model, configuration in self.configuration["models"].items():
                if model.startswith("directional_"):
                    continue
                key = f"{input_key}/{model}"
                if key in accepted:
                    continue
                capability = "mean_only" if model in ("ses", "holt", "damped", "naive2") else "probabilistic"
                provider = "chronos" if model == "chronos_2" else "r"
                levels = self.configuration["models"]["chronos_2"]["quantile_levels"] if capability == "probabilistic" else None
                # Paper baselines own normalization; research preparation applies to peers.
                original_scale = model in ("naive2", "chronos_2")
                request = {"contract_version": "forecast-v1", "experiment_id": self.context.experiment_ref.experiment_id,
                    "task_id": key, "forecast_instance_id": key, "variant_id": model,
                    "dataset_id": source["dataset_id"], "series_id": row["series_id"], "model_id": model,
                    "required_capability": capability,
                    "context": raw[row["series_key"]]["target"][:source["test_start"]] if original_scale else row["point_context"],
                    "horizon": source["horizon"],
                    "frequency": row["frequency"], "seasonal_period": row["r_period"],
                    "input_scale": "original" if original_scale else "transformed", "quantile_levels": levels,
                    "seed": self.configuration["reproducibility"]["seed"],
                    "model_settings": configuration.get("settings", configuration)}
                items.append(WorkItem(key, {"input_key": input_key, "model": model, "provider": provider,
                    "request": request, "state": {} if original_scale else row["point_state"], "execution": self._execution(provider)},
                    resources=self.context.execution.resources(provider)))
        return self.store.plan("04.01", items)

    def accept_individual_forecasts(self, results: list[tuple[str, dict[str, Any]]]) -> None:
        """Store complete original-scale individual results before any combination."""
        self.store.accept("04.01", results)

    def pending_representations(self, modelling: DataRef) -> list[WorkItem]:
        """Plan shared training and uncapped official Mantis inputs, without labels."""
        required = set(self.store.rows("reference_cohort")["S1"]["input_keys"])
        required.update(self.context.windows().rows("03.02", kind="official", fields=()))
        required.difference_update(self.store.keys("04.02"))
        inputs = self.context.windows().rows("03.02", keys=required, fields=("values",))
        return self.store.plan("04.02", [WorkItem(key, {"input_key": key, "values": inputs[key]["values"],
            "execution": self._execution("mantis")}, resources=self.context.execution.resources("mantis"))
            for key in sorted(required)])

    def accept_representations(self, results: list[tuple[str, dict[str, Any]]]) -> None:
        """Store reusable embeddings in the parent, independently of classifiers."""
        self.store.accept("04.02", results)

    def pending_decision_models(self, modelling: DataRef) -> list[WorkItem]:
        """Plan one fixed DTW library and fourteen RF units for the common cohort."""
        references = self.store.rows("reference_cohort")["S1"]["input_keys"]
        inputs = self.context.windows().rows("03.02", keys=references, fields=("frequency", "values", "labels"))
        representations = self.store.rows("04.02", keys=references, fields=("values",))
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
            frequency, model, _ = key.split("/")
            frequency_name = {"D": "daily", "W": "weekly", "M": "monthly",
                              "Q": "quarterly", "Y": "yearly", "H": "hourly"}[frequency]
            filename = "model.pickle" if result["horizon"] is None else f"horizon_{result['horizon']:02d}.pickle"
            path = self.model_root / model / frequency_name / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_suffix(".partial")
            source = result["temporary_path"]
            remote = result["host"].split(".")[0].lower() != socket.gethostname().split(".")[0].lower()
            if remote:
                machines = json.loads((CODE_ROOT / "config/machines.json").read_text())["machines"]
                machine = next(m for m in machines.values()
                    if m["hostname"].split(".")[0].lower() == result["host"].split(".")[0].lower())
                target = f"{machine['ssh_user']}@{machine['hostname']}"
                subprocess.run(["rsync", "-t", "-e", "ssh -o BatchMode=yes",
                    f"{target}:{source}", str(temporary)], check=True)
            else:
                shutil.move(source, temporary)
            temporary.replace(path)
            if remote:
                subprocess.run(["ssh", "-o", "BatchMode=yes", target, "rm", source], check=True)
            accepted.append((key, {k: value for k, value in result.items() if k != "temporary_path"} | {
                "path": str(path.relative_to(self.context.request.root))}))
        self.store.accept("04.03", accepted)

    def prepare_directional_models(self, items: list[WorkItem]) -> None:
        """Copy required canonical files once to eligible prediction hosts via rsync.

        Default rsync temporary-file publication keeps partial files out of final
        paths. Completed training is never reopened when a copy/load fails.
        """
        paths = sorted({path for item in items for path in item.payload["request"]["model_paths"]})
        if not paths:
            return
        settings = self.context.execution.settings
        machines = json.loads((CODE_ROOT / "config/machines.json").read_text())["machines"]
        hosts = {allocation["machine"] for allocation in settings.get("workers", [])
                 if "CPU=" in allocation["resources"] and allocation["machine"] != settings["coordinator"]}
        for host in sorted(hosts):
            machine = machines[host]
            target = f"{machine['ssh_user']}@{machine['hostname']}:{machine['project_root']}/"
            subprocess.run(["rsync", "-rtR", "-e", "ssh -o BatchMode=yes", *paths, target],
                           cwd=self.context.request.root, check=True)

    def pending_directional_predictions(self, modelling: DataRef) -> list[WorkItem]:
        """Plan complete official vectors using accepted model artifacts and no actuals."""
        inputs = self.context.windows().rows("03.02", kind="official", fields=("frequency", "values"))
        models = self.store.rows("04.03")
        representations = self.store.rows("04.02", keys=inputs, fields=("values",))
        accepted = self.store.keys("04.04")
        items = []
        for input_key, row in inputs.items():
            for model in ("directional_dtw", "directional_mantis_rf"):
                key = f"{input_key}/{model}"
                if key in accepted:
                    continue
                model_keys = [f"{row['frequency']}/{model}/{h}" for h in
                              (["all"] if model == "directional_dtw" else range(1, 15))]
                request = {"model": model, "model_paths": [models[k]["path"] for k in model_keys],
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
