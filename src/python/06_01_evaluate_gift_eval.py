# ==============================================================================
# 06_01_evaluate_gift_eval.py
#
# Purpose: Bridge to the isolated, pinned official GIFT-Eval environment.
# Inputs: An internal describe/evaluate/manifest command, pinned source paths, and a forecast-payload path for metric scoring.
# Outputs: One compact JSON dataset description, official metric record, or validated manifest on stdout.
# Run from: Internal bridge command: `.tools/uv/uv run --project environments/gift-eval --locked --no-sync python src/python/06_01_evaluate_gift_eval.py <describe|evaluate|manifest> [options]`.
# ==============================================================================

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


# Code constant: official GIFT-Eval output schema/order, governed by this bridge implementation.
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
    """Purpose: Adapt ordered ShapeFM forecast arrays to the GluonTS predictor protocol.

    Inputs: Forecast records and quantile levels supplied by the evaluation payload.
    Outputs: Stateful, single-pass production of identity-aligned ``QuantileForecast`` objects.
    """

    def __init__(self, records: list[dict], quantile_levels: list[float]):
        """Purpose: Initialize ordered forecast and quantile state for later prediction.

        Inputs: Records with mean/quantile horizon arrays and their configured quantile levels.
        Outputs: None; stores references on this predictor instance.
        """
        self.records = records
        self.quantile_levels = quantile_levels

    def predict(self, test_data_input):
        """Purpose: Bridge stored ShapeFM arrays into evaluator-ready GluonTS forecasts.

        Inputs: Ordered test contexts whose identities and starts correspond one-to-one with stored records.
        Outputs: A generator of mean/quantile arrays with forecast horizons, item IDs, and computed start dates.
        """
        for item, context in zip(self.records, test_data_input, strict=True):
            arrays = np.asarray([item["mean"], *item["quantiles"]], dtype=np.float64)
            yield QuantileForecast(
                forecast_arrays=arrays,
                forecast_keys=["mean", *[str(value) for value in self.quantile_levels]],
                start_date=context["start"] + len(context["target"]),
                item_id=str(context["item_id"]),
            )


def metrics(quantile_levels: list[float]):
    """Build the official metric set using task-supplied quantile levels."""
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
        MeanWeightedSumQuantileLoss(quantile_levels=quantile_levels),
    ]


def official_dataset(source_root: str, dataset_name: str, term: str) -> Dataset:
    """Purpose: Open an official dataset from the pinned GIFT-Eval source tree.

    Inputs: Source root plus dataset and term selected by configuration/payload.
    Outputs: A GIFT-Eval ``Dataset`` preserving multivariate structure.
    Side effects: Sets the process ``GIFT_EVAL`` environment variable used by the library.
    """
    os.environ["GIFT_EVAL"] = source_root
    return Dataset(dataset_name, term=term, to_univariate=False)


def require_single_window(dataset: Dataset) -> None:
    """Reject configurations outside the deliberately narrow POC 1 contract."""
    if dataset.windows != 1:
        raise ValueError(
            f"POC 1 supports exactly one official window; "
            f"{dataset.name}/{dataset.term.value} has {dataset.windows}"
        )


def json_observations(values) -> list[float | None]:
    """Convert official observations to JSON while preserving missing positions.

    NaN is represented as JSON null for the subprocess protocol; infinities remain
    malformed source values and are rejected. Gate 1 independently preserves each
    missing position in the canonical raw series.
    """
    result = []
    for value in np.asarray(values, dtype=np.float32).tolist():
        if value is None or math.isnan(value):
            result.append(None)
        elif math.isinf(value):
            raise ValueError("official observations cannot contain infinity")
        else:
            result.append(float(value))
    return result


def resolve_period(frequency: str, override: int | None = None) -> dict:
    """Resolve the R period from an explicit override or pinned GluonTS.

    Purpose: Keep the reproducible frequency convention in the environment that
    pins GluonTS instead of copying a mapping into the lean coordinator or R.
    Inputs: Stored dataset frequency and an optional positive integer override.
    Outputs: JSON-ready period, source, frequency, and pinned default metadata.
    """
    if not isinstance(frequency, str) or not frequency:
        raise ValueError("stored frequency must be a non-empty string")
    if override is not None and (
        isinstance(override, bool) or not isinstance(override, int) or override < 1
    ):
        raise ValueError("R-period override must be a positive integer")
    pinned_default = int(get_seasonality(frequency))
    return {
        "frequency": frequency,
        "r_period": override if override is not None else pinned_default,
        "r_period_source": (
            "experiment_override" if override is not None else "pinned_gluonts_get_seasonality"
        ),
        "gluonts_default_seasonality": pinned_default,
    }


def describe(
    source_root: str,
    dataset_name: str,
    term: str,
    domain: str,
    num_variates: int,
    limit: int,
    r_period_override: int | None = None,
) -> dict:
    """Purpose: Describe and materialize the configured prefix of an official evaluation task.

    Inputs: Pinned source identity, dataset/term/domain metadata, variate count, and instance limit.
    Outputs: JSON-ready metadata plus float32 context/actual arrays and the official prediction horizon.
    Side effects: Sets ``GIFT_EVAL`` while opening source data.
    """
    dataset = official_dataset(source_root, dataset_name, term)
    require_single_window(dataset)
    period = resolve_period(dataset.freq, r_period_override)
    if dataset.target_dim != num_variates:
        raise ValueError(
            f"configured num_variates={num_variates} does not match source {dataset.target_dim}"
        )
    entries = []
    pairs = itertools.islice(zip(dataset.test_data.input, dataset.test_data.label), limit)
    for position, (context, label) in enumerate(pairs):
        entries.append(
            {
                "official_position": position,
                "item_id": str(context["item_id"]),
                "variate_id": "0",
                "window_id": f"{term}/000",
                "start": str(context["start"]),
                "forecast_start": str(context["start"] + len(context["target"])),
                "context": json_observations(context["target"]),
                "actual": json_observations(label["target"]),
            }
        )
    return {
        "configuration_name": f"{dataset.name}/{dataset.freq}/{dataset.term.value}",
        "dataset_name": dataset.name,
        "frequency": dataset.freq,
        "term": dataset.term.value,
        "prediction_length": dataset.prediction_length,
        "window_count": dataset.windows,
        # ``seasonality`` remains a compatibility alias for historical callers.
        # New code uses the explicit R/evaluation fields and never conflates them.
        "seasonality": period["r_period"],
        "r_period": period["r_period"],
        "r_period_source": period["r_period_source"],
        "gluonts_default_seasonality": period["gluonts_default_seasonality"],
        "evaluation_seasonality": period["gluonts_default_seasonality"],
        "domain": domain,
        "num_variates": num_variates,
        "available_instances": len(dataset.test_data),
        "instances": entries,
    }


def evaluate(source_root: str, payload_path: Path) -> dict:
    """Purpose: Bridge ShapeFM forecast arrays into the pinned official GIFT-Eval scorer.

    Inputs: Pinned source root and a JSON payload path containing dataset, horizon-aligned forecasts,
        quantile levels, and validated evaluator options.
    Outputs: One JSON-ready mapping of official aggregate metric names to finite numeric values.
    Side effects: Reads the payload/source files, sets ``GIFT_EVAL``, and executes evaluator batches.
    """
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    dataset = official_dataset(
        source_root, payload["dataset_name"], payload["term"]
    )
    require_single_window(dataset)
    quantile_levels = payload["quantile_levels"]
    options = payload["options"]
    expected_options = {
        "axis": None,
        "mask_invalid_label": True,
        "allow_nan_forecast": False,
    }
    scientific_options = {key: value for key, value in options.items() if key != "batch_size"}
    seasonality_policy = scientific_options.pop("seasonality", None)
    if (
        scientific_options != expected_options
        or seasonality_policy not in {
            "configured official benchmark seasonality",
            "pinned GluonTS benchmark seasonality",
        }
        or isinstance(options.get("batch_size"), bool)
        or not isinstance(options.get("batch_size"), int)
        or options["batch_size"] < 1
    ):
        raise ValueError("unsupported task-supplied GIFT-Eval options")
    count = len(payload["forecasts"])
    original = list(itertools.islice(dataset.gluonts_dataset, count))
    _, template = split(original, offset=-dataset.prediction_length * dataset.windows)
    test_data = template.generate_instances(
        prediction_length=dataset.prediction_length,
        windows=dataset.windows,
        distance=dataset.prediction_length,
    )
    predictor = ShapeFMPredictor(payload["forecasts"], quantile_levels)
    forecasts = predictor.predict(test_data.input)
    seasonality = payload.get("seasonality")
    if isinstance(seasonality, bool) or not isinstance(seasonality, int) or seasonality < 1:
        raise ValueError("evaluation payload requires a positive official seasonality")
    result = evaluate_forecasts(
        forecasts,
        test_data=test_data,
        metrics=metrics(quantile_levels),
        batch_size=options["batch_size"],
        axis=options["axis"],
        mask_invalid_label=options["mask_invalid_label"],
        allow_nan_forecast=options["allow_nan_forecast"],
        seasonality=seasonality,
    ).reset_index(drop=True)
    return {key: float(value) for key, value in result.iloc[0].to_dict().items()}


def manifest(root: Path, gift_eval_directory: str) -> dict:
    """Purpose: Build a validated configuration manifest from pinned official result files.

    Inputs: Repository root and configured GIFT-Eval checkout directory.
    Outputs: The 97-entry qualified manifest, source paths, consensus details, and configuration-set hash.
    Side effects: Reads CSV/JSON files and all candidate ``results/*/all_results.csv`` files.
    """
    gift_eval_root = root / gift_eval_directory
    csv_path = gift_eval_root / "results/chronos-2/all_results.csv"
    results_root = gift_eval_root / "results"
    properties_path = gift_eval_root / "notebooks/dataset_properties.json"
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
    """Purpose: Parse and dispatch the isolated GIFT-Eval bridge CLI.

    Inputs: ``describe``, ``resolve-period``, ``evaluate``, or ``manifest`` arguments.
    Outputs: One compact finite JSON record on stdout; argparse/exceptions determine failure status.
    Side effects: Reads pinned sources/payloads and may set the process ``GIFT_EVAL`` environment variable.
    """
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    describe_parser = subparsers.add_parser("describe")
    describe_parser.add_argument("--source-root", required=True)
    describe_parser.add_argument("--dataset-name", required=True)
    describe_parser.add_argument("--term", required=True)
    describe_parser.add_argument("--domain", required=True)
    describe_parser.add_argument("--num-variates", type=int, required=True)
    describe_parser.add_argument("--r-period-override", type=int)
    # Compatibility for historical direct invocations; coordinators use the
    # accurately named option above. Supplying both is rejected below.
    describe_parser.add_argument("--seasonality", type=int)
    describe_parser.add_argument("--limit", type=int, required=True)
    period_parser = subparsers.add_parser("resolve-period")
    period_parser.add_argument("--frequency", required=True)
    period_parser.add_argument("--override", type=int)
    evaluate_parser = subparsers.add_parser("evaluate")
    evaluate_parser.add_argument("--source-root", required=True)
    evaluate_parser.add_argument("--payload", type=Path, required=True)
    manifest_parser = subparsers.add_parser("manifest")
    manifest_parser.add_argument("--root", type=Path, required=True)
    manifest_parser.add_argument("--gift-eval-directory", required=True)
    args = parser.parse_args()
    if args.command == "describe":
        if args.r_period_override is not None and args.seasonality is not None:
            parser.error("describe accepts only one R-period override option")
        result = describe(
            args.source_root,
            args.dataset_name,
            args.term,
            args.domain,
            args.num_variates,
            args.limit,
            args.r_period_override if args.r_period_override is not None else args.seasonality,
        )
    elif args.command == "resolve-period":
        result = resolve_period(args.frequency, args.override)
    elif args.command == "evaluate":
        result = evaluate(args.source_root, args.payload)
    else:
        result = manifest(args.root, args.gift_eval_directory)
    print(json.dumps(result, separators=(",", ":"), allow_nan=False))


if __name__ == "__main__":
    main()
