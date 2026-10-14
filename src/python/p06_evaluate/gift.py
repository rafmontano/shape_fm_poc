"""Process 06 pinned GIFT-Eval calculation over stored, capability-aware forecasts.

JSON jobs supply canonical raw histories/actuals and accepted forecasts. The
official GluonTS evaluator returns per-series metrics; no model is fitted.
"""

from __future__ import annotations

import json
import sys

import numpy as np
import pandas as pd
from gluonts.dataset.split import split
from gluonts.ev.metrics import (MAE, MAPE, MASE, MSE, MSIS, ND, NRMSE, RMSE,
                               SMAPE, MeanWeightedSumQuantileLoss)
from gluonts.model import evaluate_forecasts
from gluonts.model.forecast import QuantileForecast
from gluonts.time_feature import get_seasonality


def metrics(levels: list[float] | None) -> list:
    """Use the accepted GIFT-Eval metric set without inventing missing quantiles."""
    if levels is None:
        return [MSE(forecast_type="mean"), MAE(forecast_type="mean"),
                MASE(forecast_type="mean"), MAPE(forecast_type="mean"),
                SMAPE(forecast_type="mean"), RMSE(forecast_type="mean"),
                NRMSE(forecast_type="mean"), ND(forecast_type="mean")]
    return [MSE(forecast_type="mean"), MSE(forecast_type=0.5), MAE(), MASE(),
            MAPE(), SMAPE(), MSIS(), RMSE(), NRMSE(), ND(),
            MeanWeightedSumQuantileLoss(quantile_levels=[x for x in levels if 0.1 <= x <= 0.9])]


def evaluate(job: dict) -> dict:
    """Score one series/candidate using canonical raw context and benchmark seasonality.

    The single official test window is reconstructed from stored canonical
    observations; prepared modelling histories do not replace scoring context.
    """
    horizon = len(job["forecast"]["mean"])
    record = {"item_id": job["series_id"], "start": pd.Period(job["start"], freq=job["frequency"]),
              "target": np.asarray(job["context"] + job["actual"], dtype=np.float32)}
    _, template = split([record], offset=-horizon)
    test = template.generate_instances(prediction_length=horizon, windows=1, distance=horizon)
    context = next(iter(test.input))
    levels = job["forecast"].get("quantile_levels")
    forecast = job["forecast"]
    arrays = [forecast["mean"]] if levels is None else [forecast["mean"], *forecast["quantiles"]]
    native = QuantileForecast(np.asarray(arrays, dtype=np.float64),
        start_date=context["start"] + len(context["target"]),
        forecast_keys=["mean"] if levels is None else ["mean", *[str(x) for x in levels]],
        item_id=job["series_id"])
    options = job["options"]
    result = evaluate_forecasts(iter([native]), test_data=test, metrics=metrics(levels),
        batch_size=1, axis=options["axis"], mask_invalid_label=options["mask_invalid_label"],
        allow_nan_forecast=options["allow_nan_forecast"], seasonality=get_seasonality(job["frequency"]))
    return {"key": job["key"], "metrics": {k: float(v) for k, v in result.iloc[0].to_dict().items()}}


def main() -> None:
    """Return one bounded native batch of per-series evaluation results on stdout."""
    payload = json.load(sys.stdin)
    print(json.dumps({"results": [evaluate(job) for job in payload["jobs"]]}, allow_nan=False))


if __name__ == "__main__":
    main()
