# ==============================================================================
# forecast_storage.py
#
# Purpose: Coordinator-local preparation, validation, commit, and verification for Gate 4.
# Inputs: Authoritative DuckDB state, pending task identities, and worker responses.
# Outputs: Original-scale forecasts and completed task attempts in the single writer.
# ==============================================================================

"""Single-writer storage boundary for ordinary Gate 4 forecasting."""

from __future__ import annotations

import json
import math
from typing import Any

from .configuration import R_MODEL_METHODS, canonical_json, json_fingerprint
from .transformations import inverse


class ForecastStorage:
    """Own ordinary forecast preparation and validated coordinator-local commits."""

    def __init__(self, coordinator: Any, experiment_id: str, attempts: dict[str, int]):
        self.coordinator = coordinator
        self.connection = coordinator.connection
        self.experiment_id = experiment_id
        self.attempts = attempts

    def prepare_pending_jobs(self, rows: list[tuple]) -> list[dict[str, Any]]:
        """Build bounded transformed-context jobs and reject missing source lineage."""
        jobs = []
        for task_id, instance_id, variant_id, model in rows:
            row = self.connection.execute(
                """SELECT t.transformed_target, i.horizon, b.metadata
                FROM transformed_series t
                JOIN forecast_instances i USING (forecast_instance_id)
                JOIN benchmark_configurations b USING (benchmark_configuration_id)
                WHERE t.experiment_id=? AND t.variant_id=? AND t.forecast_instance_id=?""",
                [self.experiment_id, variant_id, instance_id],
            ).fetchone()
            if row is None:
                raise RuntimeError(f"missing transformed forecast lineage for task {task_id}")
            values, horizon, benchmark_metadata = row
            metadata = json.loads(benchmark_metadata)
            period = metadata.get("r_period", metadata.get("official_seasonality"))
            if period is None:
                raise RuntimeError("benchmark metadata is missing the resolved R period")
            if not isinstance(horizon, int) or horizon < 1:
                raise RuntimeError(f"invalid forecast horizon for task {task_id}")
            jobs.append({"id": task_id, "context": values, "horizon": horizon,
                         "seasonality": period, "model": model,
                         "instance_id": instance_id, "variant_id": variant_id})
        return jobs

    def commit_response(self, batch: list[dict[str, Any]], response: dict[str, Any]) -> None:
        """Validate identities/shapes/finite values/lineage, inverse, then commit locally."""
        results = response["results"]
        ids = [item.get("id") for item in results]
        expected = {job["id"] for job in batch}
        if len(ids) != len(set(ids)) or set(ids) != expected:
            raise RuntimeError("Process 04 worker returned missing, duplicate, or unexpected task IDs")
        by_id = {item["id"]: item for item in results}
        metadata = response["metadata"]
        runtime = float(response["runtime_seconds"])
        for job in batch:
            result = by_id[job["id"]]
            if self.coordinator.configuration.version >= 7 and job["model"] in R_MODEL_METHODS:
                requested = R_MODEL_METHODS[job["model"]]
                fallback = result.get("fallback_used")
                executed = result.get("executed_method_id")
                if (result.get("requested_method_id") != requested
                        or type(fallback) is not bool
                        or executed != ("snaive_forec" if fallback else requested)
                        or (fallback and not result.get("fallback_reason"))):
                    raise RuntimeError(f"invalid R method/fallback provenance for {job['id']}")
            horizon = job["horizon"]
            raw_arrays = [result.get("mean"), result.get("median")]
            quantiles = result.get("quantiles")
            if len(raw_arrays[0] or []) != horizon or len(raw_arrays[1] or []) != horizon:
                raise RuntimeError(f"forecast horizon mismatch for task {job['id']}")
            if not isinstance(quantiles, list) or len(quantiles) != len(self.coordinator.quantiles):
                raise RuntimeError(f"forecast quantile shape mismatch for task {job['id']}")
            if any(len(values) != horizon for values in quantiles):
                raise RuntimeError(f"forecast quantile horizon mismatch for task {job['id']}")
            if any(not math.isfinite(float(value)) for values in [*raw_arrays, *quantiles]
                   for value in values):
                raise RuntimeError(f"forecast contains non-finite output for task {job['id']}")
            source = self.connection.execute(
                """SELECT transformation_method, parameters, transformation_id
                FROM transformed_series WHERE experiment_id=? AND variant_id=?
                AND forecast_instance_id=?""",
                [self.experiment_id, job["variant_id"], job["instance_id"]],
            ).fetchone()
            if source is None:
                raise RuntimeError(f"missing transformation lineage for task {job['id']}")
            method, parameters, transformation_id = source
            params = json.loads(parameters)
            mean = inverse(result["mean"], method, params)
            median = inverse(result["median"], method, params)
            restored_quantiles = [inverse(values, method, params) for values in quantiles]
            from .experiment_execution import validate_forecast_capability

            validate_forecast_capability(
                mean, median, self.coordinator.quantiles, restored_quantiles, "probabilistic"
            )
            result_metadata = metadata
            if "requested_method_id" in result:
                result_metadata = {**metadata, "forecast_method": {
                    key: result[key] for key in ("requested_method_id", "executed_method_id",
                                                  "fallback_used", "fallback_reason", "provenance")
                }}
            self._commit(job, transformation_id, mean, median, restored_quantiles,
                         result_metadata, runtime / len(batch))

    def _commit(self, job: dict[str, Any], transformation_id: str, mean: tuple,
                median: tuple, quantiles: list[tuple], metadata: dict[str, Any],
                task_runtime: float) -> None:
        """Insert one idempotent forecast and complete its task in one local transaction."""
        model = job["model"]
        forecast_id = f"forecast/{json_fingerprint({'experiment': self.experiment_id, 'variant': job['variant_id'], 'instance': job['instance_id'], 'candidate': model})[:32]}"
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
                        current_timestamp, 'probabilistic')
                ON CONFLICT (forecast_id) DO NOTHING""",
                [forecast_id, self.experiment_id, job["variant_id"], job["instance_id"],
                 model, revision, transformation_id, list(mean), list(median),
                 list(self.coordinator.quantiles), [list(values) for values in quantiles],
                 metadata.get("runtime_seconds", 0.0), canonical_json(metadata),
                 json_fingerprint({"mean": mean, "quantiles": quantiles})],
            )
        self.coordinator._commit_task(job["id"], self.attempts[job["id"]],
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
                WHERE t.task_id=?""", [job["id"]]).fetchone()
            if row is None or row[0] != "completed" or row[1] is None:
                raise RuntimeError(f"forecast completion contract failed for task {job['id']}")
            if row[2:] != (job["horizon"], len(self.coordinator.quantiles), len(self.coordinator.quantiles)):
                raise RuntimeError(f"stored forecast shape contract failed for task {job['id']}")
