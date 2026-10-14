# ==============================================================================
# configuration_fixtures.py
# Purpose: Build small shared configuration variations from the canonical JSON.
# Inputs: Canonical experiment document and a test-owned temporary directory.
# Outputs: Independent dictionaries or temporary JSON files.
# Run from: Imported; not run directly.
# ==============================================================================
"""Pure test fixtures; functions avoid unnecessary state or fixture classes."""

import json
import tempfile
from pathlib import Path


def table_document(*, full=False, capped=False, version=12):
    """Read canonical table settings and optionally vary population/cohort policy."""
    document = json.loads(
        (Path(__file__).resolve().parents[3] / "config/experiment.json").read_text()
    )
    if version != 12:
        _schema_variation(document, version)
    if full or capped:
        document["data"]["selection"].update(count=4227, expected_source_total=4227)
        document["experiment"]["name"] = "test_full_daily"
        document["archived_forecasts"]["source_reconciliation"] = {
            "version": "cross_source_v1", "series_warning_pct": 0.01,
            "population_review_pct": 1.0, "single_series_material_pct": 1.0,
            "directional_impact_review_pct": 0.1,
        }
    if capped:
        document["configuration_version"] = 13
        document["experiment"].update(name="test_capped_fixed_daily", date="2026-10-12")
        document["pipeline"]["window_preparation"]["training_cohort"] = {
            "cap": 10000, "seed": 1234,
            "method": "historical_first_horizon_stratified_v1",
        }
        for model in ("directional_dtw", "directional_mantis_rf"):
            document["models"][model]["reference"].update(
                cap=10000, sampling="historical_first_horizon_stratified_v1"
            )
        dtw = document["models"]["directional_dtw"]
        dtw["constraint"] = {"name": "sakoe_chiba", "proportion": .10, "effective_width": 6}
        dtw["width_selection"] = "fixed_configuration"
        del dtw["calibration_tie_rule"]
        del document["execution"]["default"]["batch_sizes"]["directional_calibration"]
    if "model_storage" in document["execution"]:
        document["execution"]["model_storage"]["experiment"] = document["experiment"]["name"]
    return document


def _schema_variation(document, version):
    """Exercise supported older schemas without depending on archived documents.

    Keep canonical provider settings; change only schema-specific preparation,
    model membership, evaluation and operational topology required by tests.
    """
    from util.shared_configuration import R_MODEL_METHODS, R_POINT_METHODS

    if version not in range(1, 12):
        raise ValueError("Test schema version must be between 1 and 12")
    document["configuration_version"] = version
    document["experiment"].update(name=f"test_schema_{version}", date="2026-09-28")
    pipeline = document["pipeline"]
    execution = document["execution"]
    evaluation = document["evaluation"]
    if version < 10:
        document.pop("representations")
        document.pop("classifiers")
        execution.pop("model_storage")
        document["archived_forecasts"]["enabled"] = ["m4_smyl"]
        evaluation["method"] = "gift_eval"
        evaluation["options"] = {
            "axis": None, "mask_invalid_label": True, "allow_nan_forecast": False,
            "seasonality": "configured official benchmark seasonality" if version == 1
            else "pinned GluonTS benchmark seasonality",
        }
    else:
        document["archived_forecasts"]["enabled"] = []
        evaluation["method"] = "directional_accuracy"
    evaluation.pop("table_reproduction")
    evaluation.pop("gift_eval_options")
    if version <= 6:
        models = ("auto_arima", "ets") if version == 3 else ("auto_arima", "chronos_2")
        document["models"]["auto_arima"]["settings"].update(stepwise=True)
    elif version == 7:
        models = tuple(R_MODEL_METHODS)
    elif version == 8:
        models = (*R_MODEL_METHODS, "chronos_2")
    elif version == 9:
        models = (*R_MODEL_METHODS, *R_POINT_METHODS, "chronos_2")
    else:
        models = ("directional_dtw",) if version == 10 else ("directional_dtw", "directional_mantis_rf")
    document["models"] = {k: document["models"][k] for k in models}
    if version < 9:
        document["models"]["auto_arima"]["settings"]["interval_levels"] = [20, 40, 60, 80]
        if "chronos_2" in models:
            document["models"]["chronos_2"]["quantile_levels"] = [i / 10 for i in range(1, 10)]
    if version <= 7:
        pipeline["preprocessing"]["modes"] = ["standard", "robust"]
        pipeline["transformations"]["methods"] = [
            "identity", "minmax_then_standardize" if version < 4 else "standardise_sample_v1",
        ]
    if version < 4:
        pipeline.pop("window_preparation")
    elif version == 4:
        pipeline["window_preparation"] = {"context_length": 64}
    elif version == 5:
        pipeline["window_preparation"]["split"].pop("test_count_rule")
    if version == 1:
        pipeline.pop("r_period_override")
        document["data"]["benchmark"].update(
            seasonality=7, seasonality_source="approved M4 Daily weekly cycle from official frequency",
        )
    if version == 3:
        pipeline["seasonal_period_tuning"] = {
            "enabled": True, "validation_windows": 3, "horizon": "dataset_horizon",
            "origin_spacing": "dataset_horizon", "training": "expanding",
            "minimum_cycles": 3, "selection_metric": "mae", "tie_policy": "retain_baseline",
            "inconclusive_policy": "retain_baseline", "candidate_estimator": "forecast::findfrequency",
            "diagnostic": "tsfeatures::seasonal_strength",
        }
    if version < 9:
        pipeline["combination"] = {"method": "equal_weight", "weights": dict.fromkeys(models, 1 / len(models))}
    elif version >= 10:
        pipeline["combination"] = {"method": "none", "weights": {}}
    candidate = pipeline["combination"]["method"] if version < 10 else models[-1]
    evaluation["provisional_candidate"].update(
        candidate=candidate, transformation=pipeline["transformations"]["methods"][-1],
    )
    if version in (3, 10):
        evaluation["submission_metadata"]["model_dtype"] = "float64"
    final = execution["final_acceptance"]
    final.pop("mantis_gpu_processes", None)
    if version <= 6:
        execution["default"]["dask_max_in_flight"] = 2 if version <= 4 else 23
    if version <= 2:
        final["workers"] = {"mac_cpu": 1, "ubuntu_cpu": 0, "ubuntu_gpu": 1, "total": 2}
        final["worker_memory_gib"] = {"mac_cpu": 2, "ubuntu_gpu": 4}
        final["resource_safety"] = {
            "mac_system_memory_bytes": 17179869184, "ubuntu_system_memory_bytes": 128000000000,
            "mac_min_available_gib": 3, "ubuntu_min_available_gib": 16,
            "gpu_min_available_gib": 4, "persistent_unsafe_samples": 3,
        }
    elif version <= 4:
        final.update(mode="sequential", workers={"mac_cpu": 1, "total": 1})
        if version == 3:
            final["worker_memory_gib"] = {"mac_cpu": 2}
        else:
            final.update(processes=[1, 2, 3], system_memory_min_available_gib=2.0)
    elif version in (5, 6, 7, 10):
        final["workers"] = {"mac_cpu": 8, "ubuntu_cpu": 15, "total": 23}
    elif version in (8, 9):
        final["workers"] = {"mac_cpu": 8, "ubuntu_cpu": 15, "ubuntu_gpu": 15, "total": 38}
    else:
        final["mantis_gpu_processes"] = 1
    if version <= 4:
        for key in ("workflow", "execution_profile", "profile_version"):
            final.pop(key, None)
    else:
        final.update(profile_version=2 if version <= 6 else 3,
                     workflow="window_preparation" if version <= 6 else
                     "forecast_pool" if version <= 9 else
                     "directional_dtw" if version == 10 else "directional_comparison")
    if version == 10:
        document.pop("representations")
        document.pop("classifiers")
        execution.pop("model_storage")
        execution["paths"]["directional_dtw_worker"] = "src/python/04_04_directional_dtw.py"
        for key in ("directional_dtw_training_worker", "directional_dtw_prediction_worker"):
            execution["paths"].pop(key)
    if version == 5:
        execution["default"]["batch_sizes"]["window_preparation"] = 16
        execution["default"].pop("window_preparation_windows_per_job")
    _execution_schema(execution, version)


def _execution_schema(execution, version):
    """Remove settings belonging to providers absent from the selected schema."""
    default = execution["default"]
    batches = {"import", "plan", "preprocess", "transform"}
    batches |= ({"directional_calibration", "directional_prediction"} if version >= 10
                else {"auto_arima", "chronos", "combine", "gift_eval"})
    if version >= 5:
        batches.add("window_preparation")
    if version == 3 or version in (7, 8, 9):
        batches.add("r_forecast")
    if version == 11:
        batches |= {"mantis_representation", "random_forest_classifier"}
    timeouts = ({"r", "directional_dtw", "mantis", "classifier"} if version == 11
                else {"r", "directional_dtw"} if version == 10
                else {"r", "chronos_startup", "chronos_request", "gift_eval"})
    threads = {"r", "dask_worker", "classifier" if version >= 10 else "chronos"}
    for field, keys in (("batch_sizes", batches), ("worker_timeouts_seconds", timeouts),
                        ("thread_limits", threads)):
        default[field] = {key: value for key, value in default[field].items() if key in keys}
    if version < 6:
        default.pop("window_preparation_windows_per_job", None)
    paths = {"project_environment", "r_preprocess_worker", "r_m4comp2018_worker"}
    if version < 10:
        paths |= {"chronos_environment", "chronos_worker", "r_auto_arima_worker"}
        if version == 3 or version in (7, 8, 9):
            paths.add("r_forecast_worker")
    else:
        paths |= {"classifiers_environment", "classifiers_lock"}
        if version == 10:
            paths.add("directional_dtw_worker")
        else:
            paths |= {"mantis_environment", "mantis_lock", "mantis_worker",
                      "directional_dtw_training_worker", "directional_dtw_prediction_worker",
                      "random_forest_training_worker", "random_forest_prediction_worker"}
    execution["paths"] = {key: value for key, value in execution["paths"].items() if key in paths}
    final = execution["final_acceptance"]
    if version == 3:
        keys = {"mode", "workers", "worker_memory_gib"}
    elif version == 4:
        keys = {"mode", "workers", "processes", "system_memory_min_available_gib"}
    elif version <= 2:
        keys = {"mode", "workers", "physical_gpu_count", "gpu_name", "worker_memory_gib", "resource_safety"}
    else:
        keys = {"mode", "workflow", "execution_profile", "profile_version", "workers",
                "system_memory_min_available_gib"}
        if version in (8, 9, 11):
            keys |= {"physical_gpu_count", "gpu_name", "accelerator_memory_min_available_gib"}
        if version == 11:
            keys.add("mantis_gpu_processes")
    execution["final_acceptance"] = {key: value for key, value in final.items() if key in keys}


# Module-level consumers own these temporary directories until interpreter exit.
# This bounded functional fixture avoids a stateful class for pure test variations.
_temporary_directories = []


def configuration_path(*, version=12, full=False, capped=False, directory=None):
    """Materialize a schema variation in test-owned temporary storage only."""
    if directory is None:
        temporary = tempfile.TemporaryDirectory(prefix="shapefm-test-config-")
        _temporary_directories.append(temporary)
        directory = temporary.name
    name = f"schema-{version}{'-full' if full else ''}{'-capped' if capped else ''}.json"
    return write_configuration(directory, table_document(version=version, full=full, capped=capped), name)


def write_configuration(directory, document, name="configuration.json"):
    """Write a supplied in-memory variant only inside the caller's temporary directory."""
    path = Path(directory) / name
    path.write_text(json.dumps(document), encoding="utf-8")
    return path
