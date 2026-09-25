"""Bridge to the isolated, pinned official GIFT-Eval environment."""

from __future__ import annotations

import argparse
import csv
import hashlib
import itertools
import json
import math
import os
from pathlib import Path

import numpy as np
from gift_eval.data import Dataset
from gluonts.dataset.split import split
from gluonts.ev.metrics import (
    MAE,
    MAPE,
    MASE,
    MSE,
    MSIS,
    ND,
    NRMSE,
    RMSE,
    SMAPE,
    MeanWeightedSumQuantileLoss,
)
from gluonts.model import evaluate_forecasts
from gluonts.model.forecast import QuantileForecast
from gluonts.time_feature import get_seasonality


QUANTILES = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
REQUIRED_RESULT_COLUMNS = [
    "dataset",
    "model",
    "eval_metrics/MSE[mean]",
    "eval_metrics/MSE[0.5]",
    "eval_metrics/MAE[0.5]",
    "eval_metrics/MASE[0.5]",
    "eval_metrics/MAPE[0.5]",
    "eval_metrics/sMAPE[0.5]",
    "eval_metrics/MSIS",
    "eval_metrics/RMSE[mean]",
    "eval_metrics/NRMSE[mean]",
    "eval_metrics/ND[0.5]",
    "eval_metrics/mean_weighted_sum_quantile_loss",
    "domain",
    "num_variates",
]


class ShapeFMPredictor:
    """GluonTS-compatible predictor over forecasts computed by ShapeFM."""

    def __init__(self, records: list[dict]):
        self.records = records

    def predict(self, test_data_input):
        for item, context in zip(self.records, test_data_input, strict=True):
            arrays = np.asarray([item["mean"], *item["quantiles"]], dtype=np.float64)
            yield QuantileForecast(
                forecast_arrays=arrays,
                forecast_keys=["mean", *[str(value) for value in QUANTILES]],
                start_date=context["start"] + len(context["target"]),
                item_id=str(context["item_id"]),
            )


def metrics():
    """Return the exact metric objects used by the pinned official notebook."""
    return [
        MSE(forecast_type="mean"),
        MSE(forecast_type=0.5),
        MAE(),
        MASE(),
        MAPE(),
        SMAPE(),
        MSIS(),
        RMSE(),
        NRMSE(),
        ND(),
        MeanWeightedSumQuantileLoss(quantile_levels=QUANTILES),
    ]


def official_dataset(source_root: str) -> Dataset:
    os.environ["GIFT_EVAL"] = source_root
    return Dataset("m4_daily", term="short", to_univariate=False)


def require_single_window(dataset: Dataset) -> None:
    """Reject configurations outside the deliberately narrow POC 1 contract."""
    if dataset.windows != 1:
        raise ValueError(
            f"POC 1 supports exactly one official window; "
            f"{dataset.name}/{dataset.term.value} has {dataset.windows}"
        )


def describe(source_root: str, limit: int) -> dict:
    dataset = official_dataset(source_root)
    require_single_window(dataset)
    entries = []
    pairs = itertools.islice(zip(dataset.test_data.input, dataset.test_data.label), limit)
    for position, (context, label) in enumerate(pairs):
        entries.append(
            {
                "official_position": position,
                "item_id": str(context["item_id"]),
                "variate_id": "0",
                "window_id": "short/000",
                "start": str(context["start"]),
                "forecast_start": str(context["start"] + len(context["target"])),
                "context": np.asarray(context["target"], dtype=np.float32).tolist(),
                "actual": np.asarray(label["target"], dtype=np.float32).tolist(),
            }
        )
    return {
        "configuration_name": "m4_daily/D/short",
        "dataset_name": dataset.name,
        "frequency": dataset.freq,
        "term": dataset.term.value,
        "prediction_length": dataset.prediction_length,
        "window_count": dataset.windows,
        "seasonality": get_seasonality(dataset.freq),
        "domain": "Econ/Fin",
        "num_variates": dataset.target_dim,
        "available_instances": len(dataset.test_data),
        "instances": entries,
    }


def evaluate(source_root: str, payload_path: Path) -> dict:
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    dataset = official_dataset(source_root)
    require_single_window(dataset)
    count = len(payload["forecasts"])
    original = list(itertools.islice(dataset.gluonts_dataset, count))
    _, template = split(original, offset=-dataset.prediction_length * dataset.windows)
    test_data = template.generate_instances(
        prediction_length=dataset.prediction_length,
        windows=dataset.windows,
        distance=dataset.prediction_length,
    )
    predictor = ShapeFMPredictor(payload["forecasts"])
    forecasts = predictor.predict(test_data.input)
    result = evaluate_forecasts(
        forecasts,
        test_data=test_data,
        metrics=metrics(),
        batch_size=1024,
        axis=None,
        mask_invalid_label=True,
        allow_nan_forecast=False,
        seasonality=get_seasonality(dataset.freq),
    ).reset_index(drop=True)
    return {key: float(value) for key, value in result.iloc[0].to_dict().items()}


def manifest(root: Path) -> dict:
    """Read and validate the pinned framework's complete qualified manifest."""
    csv_path = root / "external/gift-eval/results/chronos-2/all_results.csv"
    results_root = root / "external/gift-eval/results"
    properties_path = root / "external/gift-eval/notebooks/dataset_properties.json"
    properties = json.loads(properties_path.read_text(encoding="utf-8"))
    with csv_path.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != REQUIRED_RESULT_COLUMNS:
            raise ValueError("official manifest source does not have the required 15-column schema")
        rows = list(reader)
    entries = []
    seen = set()
    for row in rows:
        configuration = row["dataset"]
        try:
            dataset_name, frequency, term = configuration.rsplit("/", 2)
        except ValueError as exc:
            raise ValueError(f"unqualified GIFT-Eval configuration: {configuration}") from exc
        if not dataset_name or not frequency or term not in {"short", "medium", "long"}:
            raise ValueError(f"invalid GIFT-Eval configuration: {configuration}")
        if dataset_name not in properties:
            raise ValueError(f"configuration has no official dataset properties: {configuration}")
        if configuration in seen:
            raise ValueError(f"duplicate GIFT-Eval configuration: {configuration}")
        seen.add(configuration)
        for column in REQUIRED_RESULT_COLUMNS[2:13]:
            if not math.isfinite(float(row[column])):
                raise ValueError(f"manifest source has invalid metric {column}: {configuration}")
        if row["domain"] != properties[dataset_name]["domain"]:
            raise ValueError(f"manifest domain mismatch: {configuration}")
        if int(row["num_variates"]) != properties[dataset_name]["num_variates"]:
            raise ValueError(f"manifest variate-count mismatch: {configuration}")
        entries.append(
            {
                "configuration_name": configuration,
                "dataset_name": dataset_name,
                "frequency": frequency,
                "term": term,
                "domain": row["domain"],
                "num_variates": int(row["num_variates"]),
            }
        )
    if len(entries) != 97:
        raise ValueError(f"pinned GIFT-Eval manifest has {len(entries)} configurations, expected 97")
    reference_configurations = {entry["configuration_name"] for entry in entries}
    complete_sources = []
    disagreements = []
    all_result_files = sorted(results_root.glob("*/all_results.csv"))
    for candidate in all_result_files:
        with candidate.open(newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            candidate_rows = list(reader)
        configurations = [row.get("dataset") for row in candidate_rows]
        if (
            reader.fieldnames != REQUIRED_RESULT_COLUMNS
            or len(candidate_rows) != 97
            or len(set(configurations)) != 97
            or None in configurations
        ):
            continue
        qualified = True
        for configuration in configurations:
            try:
                dataset_name, frequency, term = configuration.rsplit("/", 2)
            except ValueError:
                qualified = False
                break
            if (
                not dataset_name
                or not frequency
                or term not in {"short", "medium", "long"}
                or dataset_name not in properties
            ):
                qualified = False
                break
        if not qualified:
            continue
        relative = str(candidate.relative_to(root))
        complete_sources.append(relative)
        candidate_set = set(configurations)
        if candidate_set != reference_configurations:
            disagreements.append(
                {
                    "source": relative,
                    "missing": sorted(reference_configurations - candidate_set),
                    "extra": sorted(candidate_set - reference_configurations),
                }
            )
    consensus = bool(complete_sources) and not disagreements
    configuration_set_hash = hashlib.sha256(
        json.dumps(sorted(reference_configurations), separators=(",", ":")).encode()
    ).hexdigest()
    return {
        "source": str(csv_path.relative_to(root)),
        "properties_source": str(properties_path.relative_to(root)),
        "configuration_count": len(entries),
        "configurations": [entry["configuration_name"] for entry in entries],
        "entries": entries,
        "validated": True,
        "manifest_role": "pinned_consensus_manifest" if consensus else "pinned_reference_manifest",
        "consensus_validation": {
            "all_results_file_count": len(all_result_files),
            "complete_file_count": len(complete_sources),
            "complete_sources": complete_sources,
            "all_complete_files_agree": consensus,
            "configuration_set_sha256": configuration_set_hash,
            "disagreements": disagreements,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    describe_parser = subparsers.add_parser("describe")
    describe_parser.add_argument("--source-root", required=True)
    describe_parser.add_argument("--limit", type=int, required=True)
    evaluate_parser = subparsers.add_parser("evaluate")
    evaluate_parser.add_argument("--source-root", required=True)
    evaluate_parser.add_argument("--payload", type=Path, required=True)
    manifest_parser = subparsers.add_parser("manifest")
    manifest_parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "describe":
        result = describe(args.source_root, args.limit)
    elif args.command == "evaluate":
        result = evaluate(args.source_root, args.payload)
    else:
        result = manifest(args.root)
    print(json.dumps(result, separators=(",", ":"), allow_nan=False))


if __name__ == "__main__":
    main()
