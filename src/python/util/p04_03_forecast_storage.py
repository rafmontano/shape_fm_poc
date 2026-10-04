# ==============================================================================
# p04_03_forecast_storage.py
#
# Purpose: Coordinator-local preparation, validation, commit, and verification for Gate 4.
# Inputs: Authoritative DuckDB state, pending task identities, and worker responses.
# Outputs: Original-scale forecasts and completed task attempts in the single writer.
# Run from: Imported by the Gate 4 Prefect flow; not run directly.
# ==============================================================================

"""Single-writer storage boundary for ordinary Gate 4 forecasting."""

from __future__ import annotations

import json
from typing import Any

from .p04_00_forecast_contract import ForecastContract
from .shared_configuration import (
    R_MODEL_METHODS, R_POINT_METHODS, canonical_json, json_fingerprint,
)
from .shared_transformations import inverse


class ForecastStorage:
    """Own ordinary forecast preparation and validated coordinator-local commits."""

    def __init__(self, coordinator: Any, experiment_id: str, attempts: dict[str, int]):
        self.coordinator = coordinator
        self.connection = coordinator.connection
        self.experiment_id = experiment_id
        self.attempts = attempts
        self.contract = ForecastContract()

    def prepare_pending_jobs(self, rows: list[tuple]) -> list[dict[str, Any]]:
        """Build bounded transformed-context jobs and reject missing source lineage."""
        jobs = []
        for task_id, instance_id, variant_id, model in rows:
            row = self.connection.execute(
                """SELECT t.transformed_target, i.horizon, b.metadata,
                          i.dataset_id, i.series_id, b.frequency
                FROM transformed_series t
                JOIN forecast_instances i USING (forecast_instance_id)
                JOIN benchmark_configurations b USING (benchmark_configuration_id)
                WHERE t.experiment_id=? AND t.variant_id=? AND t.forecast_instance_id=?""",
                [self.experiment_id, variant_id, instance_id],
            ).fetchone()
            if row is None:
                raise RuntimeError(f"missing transformed forecast lineage for task {task_id}")
            values, horizon, benchmark_metadata, dataset_id, series_id, frequency = row
            metadata = json.loads(benchmark_metadata)
            period = metadata.get("r_period", metadata.get("official_seasonality"))
            if period is None:
                raise RuntimeError("benchmark metadata is missing the resolved R period")
            if not isinstance(horizon, int) or horizon < 1:
                raise RuntimeError(f"invalid forecast horizon for task {task_id}")
            capability = "mean_only" if model in R_POINT_METHODS else "probabilistic"
            model_settings = self.coordinator.configuration.r_model_settings(model) \
                if model in {*R_MODEL_METHODS, *R_POINT_METHODS} \
                else self.coordinator.config["models"][model]
            request = {
                "contract_version": self.contract.version,
                "experiment_id": self.experiment_id,
                "task_id": task_id,
                "forecast_instance_id": instance_id,
                "variant_id": variant_id,
                "dataset_id": dataset_id,
                "series_id": series_id,
                "model_id": model,
                "required_capability": capability,
                "context": values,
                "horizon": horizon,
                "frequency": frequency,
                "seasonal_period": int(period),
                "input_scale": "transformed",
                "quantile_levels": None if capability == "mean_only"
                else list(self.coordinator.quantiles),
                "seed": self.coordinator.configuration.seed,
                "model_settings": model_settings,
            }
            jobs.append(self.contract.validate_request(request))
        return jobs

    def commit_response(self, batch: list[dict[str, Any]], response: dict[str, Any]) -> None:
        """Validate identities/shapes/finite values/lineage, inverse, then commit locally."""
        results = response["results"]
        ids = [item.get("task_id") for item in results]
        expected = {job["task_id"] for job in batch}
        if len(ids) != len(set(ids)) or set(ids) != expected:
            raise RuntimeError("Process 04 worker returned missing, duplicate, or unexpected task IDs")
        by_id = {item["task_id"]: item for item in results}
        metadata = response["metadata"]
        runtime = float(response["runtime_seconds"])
        for job in batch:
            result = self.contract.validate_result(job, by_id[job["task_id"]])
            if result["status"] == "error":
                raise RuntimeError(
                    f"forecast provider failed for {job['task_id']}: "
                    f"{result['error_type']}: {result['error_message']}"
                )
            if self.coordinator.configuration.version >= 7 and job["model_id"] in R_MODEL_METHODS:
                requested = job["model_id"]
                fallback = result.get("fallback_used")
                executed = result.get("executed_model_id")
                expected_executed = (
                    "auto_arima" if requested == "stlm_ar" and fallback else requested
                )
                if (type(fallback) is not bool or executed != expected_executed
                        or (fallback and requested != "stlm_ar")
                        or (fallback and not result.get("fallback_reason"))):
                    raise RuntimeError(f"invalid R method/fallback provenance for {job['task_id']}")
            if job["model_id"] in R_POINT_METHODS and result["fallback_used"]:
                raise RuntimeError("M4 point methods must not use fallback")
            horizon = job["horizon"]
            source = self.connection.execute(
                """SELECT transformation_method, parameters, transformation_id
                FROM transformed_series WHERE experiment_id=? AND variant_id=?
                AND forecast_instance_id=?""",
                [self.experiment_id, job["variant_id"], job["forecast_instance_id"]],
            ).fetchone()
            if source is None:
                raise RuntimeError(f"missing transformation lineage for task {job['task_id']}")
            method, parameters, transformation_id = source
            params = json.loads(parameters)
            mean = inverse(result["mean"], method, params)
            median = None if result["median"] is None else inverse(result["median"], method, params)
            restored_quantiles = None if result["quantiles"] is None else [
                inverse(values, method, params) for values in result["quantiles"]
            ]
            from .shared_experiment_execution import validate_forecast_capability

            validate_forecast_capability(
                mean, median, result["quantile_levels"], restored_quantiles,
                result["forecast_capability"], horizon,
            )
            result_metadata = {**metadata, "forecast_contract": {
                key: result[key] for key in (
                    "contract_version", "requested_model_id", "executed_model_id",
                    "forecast_capability", "fallback_used", "fallback_reason", "provenance",
                )
            }}
            self._commit(
                job, transformation_id, mean, median, restored_quantiles,
                result["quantile_levels"], result["forecast_capability"],
                result_metadata, float(result["runtime_seconds"]), runtime / len(batch),
            )

    def _commit(self, job: dict[str, Any], transformation_id: str, mean: tuple,
                median: tuple | None, quantiles: list[tuple] | None,
                quantile_levels: list[float] | None, capability: str,
                metadata: dict[str, Any], provider_runtime: float,
                task_runtime: float) -> None:
        """Insert one idempotent forecast and complete its task in one local transaction."""
        model = job["model_id"]
        forecast_id = f"forecast/{json_fingerprint({'experiment': self.experiment_id, 'variant': job['variant_id'], 'instance': job['forecast_instance_id'], 'candidate': model})[:32]}"
        revision = (self.coordinator.config["models"][model].get("revision")
                    or metadata.get("packages", {}).get("forecast"))
        def insert() -> None:
            self.connection.execute(
                """INSERT INTO forecasts
                (forecast_id, experiment_id, variant_id, forecast_instance_id,
                 candidate, model_revision, parent_result_id, scale, mean, median,
                 quantile_levels, quantiles, runtime_seconds, execution_metadata,
                 content_hash, created_at, forecast_capability)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'original', ?, ?, ?, ?, ?, ?, ?,
                        current_timestamp, ?)
                ON CONFLICT (forecast_id) DO NOTHING""",
                [forecast_id, self.experiment_id, job["variant_id"],
                 job["forecast_instance_id"],
                 model, revision, transformation_id, list(mean),
                 None if median is None else list(median), quantile_levels,
                 None if quantiles is None else [list(values) for values in quantiles],
                 provider_runtime, canonical_json(metadata),
                 json_fingerprint({"capability": capability, "scale": "original",
                                   "mean": mean, "median": median,
                                   "quantile_levels": quantile_levels,
                                   "quantiles": quantiles}), capability],
            )
        self.coordinator._commit_task(job["task_id"], self.attempts[job["task_id"]],
                                      task_runtime, insert, metadata)

    def verify_completion(self, expected_jobs: list[dict[str, Any]]) -> None:
        """Require every authoritative pending identity to have task and forecast rows."""
        for job in expected_jobs:
            row = self.connection.execute(
                """SELECT t.status, f.parent_result_id, array_length(f.mean),
                          array_length(f.quantile_levels), array_length(f.quantiles)
                FROM experiment_tasks t LEFT JOIN forecasts f
                  ON f.experiment_id=t.experiment_id AND f.variant_id=t.variant_id
                 AND f.forecast_instance_id=t.forecast_instance_id AND f.candidate=t.candidate
                WHERE t.task_id=?""", [job["task_id"]]).fetchone()
            if row is None or row[0] != "completed" or row[1] is None:
                raise RuntimeError(f"forecast completion contract failed for task {job['task_id']}")
            expected_shapes = (
                (job["horizon"], None, None) if job["required_capability"] == "mean_only"
                else (job["horizon"], len(self.coordinator.quantiles), len(self.coordinator.quantiles))
            )
            if row[2:] != expected_shapes:
                raise RuntimeError(f"stored forecast shape contract failed for task {job['task_id']}")
