# ==============================================================================
# p04_00_forecast_contract.py
#
# Purpose: Define and validate the language-neutral ShapeFM forecast boundary.
# Inputs: JSON-compatible per-model requests, successful results, and errors.
# Outputs: Validated mappings with one versioned field set; no external effects.
# Run from: Imported by Process 04 providers and coordinator storage.
# ==============================================================================

"""One serialized request/result contract shared by R and Python providers."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ForecastContract:
    """Build and validate one versioned, capability-aware forecast protocol."""

    version: str = "forecast-v1"

    REQUEST_FIELDS = (
        "contract_version", "experiment_id", "task_id", "forecast_instance_id",
        "variant_id", "dataset_id", "series_id", "model_id",
        "required_capability", "context", "horizon", "frequency",
        "seasonal_period", "input_scale", "quantile_levels", "seed",
        "model_settings",
    )
    SUCCESS_FIELDS = (
        "contract_version", "status", "experiment_id", "task_id",
        "forecast_instance_id", "variant_id", "requested_model_id",
        "executed_model_id", "forecast_capability", "output_scale", "horizon",
        "mean", "median", "quantile_levels", "quantiles", "fallback_used",
        "fallback_reason", "runtime_seconds", "provenance",
    )
    ERROR_FIELDS = (
        "contract_version", "status", "experiment_id", "task_id",
        "forecast_instance_id", "variant_id", "requested_model_id",
        "error_type", "error_message", "provenance",
    )

    @staticmethod
    def noncrossing_quantiles(
        quantiles: list[list[float]],
    ) -> tuple[list[list[float]], bool]:
        """Sort quantiles per horizon step and report whether rearrangement occurred."""
        points = list(zip(*quantiles, strict=True))
        rearranged = any(tuple(point) != tuple(sorted(point)) for point in points)
        if not rearranged:
            return quantiles, False
        return [
            list(values)
            for values in zip(*(sorted(point) for point in points), strict=True)
        ], True

    def validate_request(self, request: dict[str, Any]) -> dict[str, Any]:
        """Require the exact common request fields and valid scientific inputs."""
        self._exact_fields(request, self.REQUEST_FIELDS, "forecast request")
        if request["contract_version"] != self.version:
            raise ValueError("unsupported forecast contract version")
        self._identities(request, self.REQUEST_FIELDS[1:8])
        if request["required_capability"] not in {"probabilistic", "mean_only"}:
            raise ValueError("unsupported required forecast capability")
        context = request["context"]
        if not isinstance(context, list) or not context or not self._finite(context):
            raise ValueError("forecast context must be a non-empty finite numeric list")
        for field in ("horizon", "seasonal_period"):
            value = request[field]
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{field} must be a positive integer")
        if not isinstance(request["frequency"], str) or not request["frequency"]:
            raise ValueError("frequency must be a non-empty identifier")
        if not isinstance(request["input_scale"], str) or not request["input_scale"]:
            raise ValueError("input_scale must be a non-empty identifier")
        if isinstance(request["seed"], bool) or not isinstance(request["seed"], int):
            raise ValueError("seed must be an integer")
        if not isinstance(request["model_settings"], dict):
            raise ValueError("model_settings must be an object")
        self._validate_levels(request["quantile_levels"], request["required_capability"])
        return request

    def validate_result(
        self, request: dict[str, Any], result: dict[str, Any]
    ) -> dict[str, Any]:
        """Validate one success or error envelope against its authoritative request."""
        self.validate_request(request)
        if result.get("status") == "error":
            self._exact_fields(result, self.ERROR_FIELDS, "forecast error")
            self._matching_identity(request, result)
            for field in ("error_type", "error_message"):
                if not isinstance(result[field], str) or not result[field]:
                    raise ValueError(f"forecast error {field} must be non-empty")
            if not isinstance(result["provenance"], dict):
                raise ValueError("forecast error provenance must be an object")
            return result

        self._exact_fields(result, self.SUCCESS_FIELDS, "forecast result")
        if result["status"] != "success":
            raise ValueError("forecast result status must be success or error")
        self._matching_identity(request, result)
        if result["requested_model_id"] != request["model_id"]:
            raise ValueError("forecast result requested model does not match request")
        if result["forecast_capability"] != request["required_capability"]:
            raise ValueError("forecast result capability does not match request")
        if result["output_scale"] != request["input_scale"]:
            raise ValueError("forecast result scale does not match request")
        if result["horizon"] != request["horizon"]:
            raise ValueError("forecast result horizon does not match request")
        mean = result["mean"]
        if not isinstance(mean, list) or len(mean) != request["horizon"] or not self._finite(mean):
            raise ValueError("forecast result mean must be one finite horizon vector")
        capability = result["forecast_capability"]
        self._validate_levels(result["quantile_levels"], capability)
        if capability == "mean_only":
            if result["median"] is not None or result["quantiles"] is not None:
                raise ValueError("mean-only result must use null probabilistic fields")
        else:
            levels = result["quantile_levels"]
            if levels != request["quantile_levels"]:
                raise ValueError("forecast result quantile levels do not match request")
            median, quantiles = result["median"], result["quantiles"]
            if not isinstance(median, list) or len(median) != request["horizon"] or not self._finite(median):
                raise ValueError("forecast result median must be one finite horizon vector")
            if (
                not isinstance(quantiles, list) or len(quantiles) != len(levels)
                or any(not isinstance(row, list) or len(row) != request["horizon"]
                       or not self._finite(row) for row in quantiles)
            ):
                raise ValueError("forecast result quantiles must be levels by horizon")
            if any(any(left > right for left, right in zip(column, column[1:]))
                   for column in zip(*quantiles, strict=True)):
                raise ValueError("forecast result quantiles cross")
            median_index = levels.index(0.5)
            if any(abs(left - right) > 1e-10
                   for left, right in zip(median, quantiles[median_index], strict=True)):
                raise ValueError("forecast result median does not equal q0.5")
        if type(result["fallback_used"]) is not bool:
            raise ValueError("fallback_used must be boolean")
        if result["fallback_used"]:
            if result["executed_model_id"] == result["requested_model_id"] or not result["fallback_reason"]:
                raise ValueError("fallback result requires a different model and reason")
        elif (result["executed_model_id"] != result["requested_model_id"]
              or result["fallback_reason"] is not None):
            raise ValueError("non-fallback result has inconsistent provenance")
        runtime = result["runtime_seconds"]
        if isinstance(runtime, bool) or not isinstance(runtime, (int, float)) or not math.isfinite(runtime) or runtime < 0:
            raise ValueError("runtime_seconds must be finite and non-negative")
        if not isinstance(result["provenance"], dict):
            raise ValueError("forecast result provenance must be an object")
        return result

    @staticmethod
    def _exact_fields(value: Any, fields: tuple[str, ...], label: str) -> None:
        if not isinstance(value, dict) or set(value) != set(fields):
            raise ValueError(f"{label} must contain exactly the common contract fields")

    @staticmethod
    def _identities(value: dict[str, Any], fields: tuple[str, ...]) -> None:
        for field in fields:
            if not isinstance(value[field], str) or not value[field]:
                raise ValueError(f"{field} must be a non-empty identifier")

    def _matching_identity(self, request: dict[str, Any], result: dict[str, Any]) -> None:
        if result["contract_version"] != request["contract_version"]:
            raise ValueError("forecast result contract version does not match request")
        for field in ("experiment_id", "task_id", "forecast_instance_id", "variant_id"):
            if result[field] != request[field]:
                raise ValueError(f"forecast result {field} does not match request")
        if result["requested_model_id"] != request["model_id"]:
            raise ValueError("forecast result requested model does not match request")

    @staticmethod
    def _finite(values: list[Any]) -> bool:
        return all(type(value) in {int, float} and math.isfinite(value) for value in values)

    @staticmethod
    def _validate_levels(levels: Any, capability: str) -> None:
        if capability == "mean_only":
            if levels is not None:
                raise ValueError("mean-only quantile levels must be null")
            return
        if (
            not isinstance(levels, list) or not levels or 0.5 not in levels
            or not ForecastContract._finite(levels)
            or any(level <= 0 or level >= 1 for level in levels)
            or any(left >= right for left, right in zip(levels, levels[1:]))
        ):
            raise ValueError("probabilistic quantile levels must be ordered and include q0.5")
