"""Retrieve validated official M4 point forecasts without fitting a model."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import duckdb


APPROVED_M4_PROVIDERS = {"m4_smyl": 118, "m4_fforma": 245}


def retrieve_m4_submission(
    database_path: Path, request: dict[str, Any]
) -> dict[str, Any]:
    """Return one archived mean under the Gate 4 identity and provenance contract.

    The provider accepts no model settings, performs no fitting or substitution, and is
    valid only for the official final M4 window and internal ``official_reference``
    designation. That designation is provenance, not a preprocessing mode.
    """
    required = {
        "experiment_id", "variant_id", "forecast_instance_id", "dataset_id",
        "series_id", "window_id", "forecast_id", "horizon",
    }
    missing = sorted(required - request.keys())
    if missing:
        raise ValueError(f"M4 submission request is missing: {', '.join(missing)}")
    forecast_id = request["forecast_id"]
    if forecast_id not in APPROVED_M4_PROVIDERS:
        raise ValueError(f"unapproved M4 forecast provider: {forecast_id}")
    horizon = request["horizon"]
    if isinstance(horizon, bool) or not isinstance(horizon, int) or horizon < 1:
        raise ValueError("M4 submission horizon must be a positive integer")
    if request["window_id"] != "short/000":
        raise ValueError("archived M4 forecasts require the official final window short/000")
    if request["variant_id"] != "official_reference":
        raise ValueError(
            "archived M4 forecasts require the internal official_reference designation"
        )
    connection = duckdb.connect(str(Path(database_path).resolve()), read_only=True)
    try:
        instance = connection.execute(
            """SELECT dataset_id, series_id, window_id, horizon,
                      context_start, context_end, actual_start, actual_end
               FROM forecast_instances
               WHERE forecast_instance_id=? AND benchmark_configuration_id=(
                   SELECT benchmark_configuration_id FROM experiments WHERE experiment_id=?
               )""",
            [request["forecast_instance_id"], request["experiment_id"]],
        ).fetchone()
        expected_instance = (
            request["dataset_id"], str(request["series_id"]), request["window_id"],
            horizon,
        )
        if instance is None or tuple(instance[:4]) != expected_instance:
            raise ValueError("forecast instance does not match requested dataset, series, window, or horizon")
        window = connection.execute(
            """SELECT test_start, test_end FROM evaluation_windows
               WHERE dataset_id=? AND series_id=? AND window_id=?""",
            [request["dataset_id"], str(request["series_id"]), request["window_id"]],
        ).fetchone()
        if (
            window is None
            or instance[4] != 0
            or instance[5] != instance[6]
            or instance[6] != window[0]
            or instance[7] != window[1]
        ):
            raise ValueError("forecast instance is not the official M4 final evaluation window")
        row = connection.execute(
            """SELECT reference_forecast_id, official_m4_series_id, submission_id,
                      submission_rank, submission_author, horizon, mean,
                      point_semantics, forecast_capability, source_metadata,
                      content_hash
               FROM reference_forecasts
               WHERE dataset_id=? AND series_id=? AND forecast_id=?""",
            [request["dataset_id"], str(request["series_id"]), forecast_id],
        ).fetchone()
        if row is None:
            raise KeyError("requested archived M4 forecast was not imported")
        if row[2] != APPROVED_M4_PROVIDERS[forecast_id]:
            raise ValueError("stored M4 submission does not match the approved provider mapping")
        if row[5] != horizon or len(row[6]) != row[5]:
            raise ValueError("stored M4 forecast horizon is incompatible with the request")
        if row[8] != "mean_only" or any(not math.isfinite(value) for value in row[6]):
            raise ValueError("stored M4 forecast has invalid capability or point values")
        metadata = json.loads(row[9])
        if metadata.get("scale") != "original":
            raise ValueError("stored M4 forecast is not explicitly on the original scale")
        return {
            "status": "success",
            "experiment_id": request["experiment_id"],
            "variant_id": request["variant_id"],
            "forecast_instance_id": request["forecast_instance_id"],
            "dataset_id": request["dataset_id"],
            "series_id": str(request["series_id"]),
            "window_id": request["window_id"],
            "requested_provider": forecast_id,
            "executed_provider": forecast_id,
            "horizon": row[5],
            "mean": list(row[6]),
            "scale": "original",
            "forecast_capability": row[8],
            "provenance": {
                "source": "archived_m4_submission",
                "reference_forecast_id": row[0],
                "official_m4_series_id": row[1],
                "submission_id": row[2],
                "submission_rank": row[3],
                "submission_author": row[4],
                "point_semantics": row[7],
                "content_hash": row[10],
                **metadata,
            },
        }
    finally:
        connection.close()
