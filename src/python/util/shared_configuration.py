# ==============================================================================
# shared_configuration.py
#
# Purpose: Validate the complete experiment contract and derive stable identities and windows.
# Inputs: Experiment JSON, stored resolved configuration, source metadata, and observation counts.
# Outputs: Typed experiment settings, stable SHA-256 identities, and evaluation boundaries.
# Run from: Imported; not run directly.
# ==============================================================================

"""Stage 1 configuration, deterministic identities, and window boundaries."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class ImportValidationError(ValueError):
    """Purpose: Identify source-data or import-state contract violations.

    Inputs: Human-readable validation details supplied when raised.
    Outputs: A Stage 1-specific ``ValueError`` for callers to handle.
    """


class ExperimentConfigurationError(ValueError):
    """Purpose: Identify violations of the versioned experiment configuration contract.

    Inputs: Human-readable field or document validation details.
    Outputs: A configuration-specific ``ValueError`` for callers to handle.
    """


# Code constant: v1 preserves coupled period-7, v2 resolves the R period,
# v3 opts into period tuning, v4 selects portable sample standardisation, and
# v5 defines the original rolling-window/S1 contract; v6 corrects split
# arithmetic; v7 adds the approved R pool; v8 adds the bounded one-variant
# all-model acceptance; v9 activates the forecast contract and M4 benchmarks;
# v10 activates the approved directional DTW baseline without forecast rows;
# v11 adds the same-input Mantis/Random-Forest comparison without changing v10.
# v12 combines those providers with stored point means and ex-post paper tables.
SUPPORTED_CONFIGURATION_VERSIONS = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
# Stable production IDs map to the native, allowlisted FFORMA method registry.
R_MODEL_METHODS = {
    name: f"{name}_forec" for name in
    ("auto_arima", "ets", "nnetar", "tbats", "stlm_ar", "rw_drift", "thetaf", "naive", "snaive")
}
# Stable point-only IDs map to the M4 functions without changing the nine-method registry.
R_POINT_METHODS = {
    name: f"{name}_forec" for name in ("naive2", "ses", "holt", "damped")
}
R_FORECAST_METHODS = {**R_MODEL_METHODS, **R_POINT_METHODS}
PROBABILISTIC_QUANTILES = [0.025, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.975]
# Code constant: repository protocol mapping shared by JSON, DuckDB, CLI, and status output.
PROCESS_NAMES = {
    1: "import",
    2: "preprocess",
    3: "transform",
    4: "forecast",
    5: "combine",
    6: "evaluate",
}
# Code constant: runtime-only relocation of worker paths stored by earlier databases.
# Integrity validation uses the original stored document; only subprocess resolution
# applies this mapping, so scientific identity and historical provenance are unchanged.
LEGACY_WORKER_PATHS = {
    "src/python/04_forecast_chronos.py": "src/python/04_02_forecast_chronos.py",
    "src/r/02_preprocess_series.R": "src/r/02_01_preprocess_series.R",
    "src/r/04_forecast_auto_arima.R": "src/r/04_01_forecast_auto_arima.R",
}


def resolve_worker_path(path: str) -> str:
    """Return the current repository path for a stored worker path.

    Purpose: Preserve execution of authoritative databases containing pre-rename paths.
    Inputs: Repository-relative path from validated configuration or current code.
    Outputs: Renamed path for a known legacy worker, otherwise the input unchanged;
    does not mutate or re-hash stored configuration.
    """
    return LEGACY_WORKER_PATHS.get(path, path)


@dataclass(frozen=True)
class ExperimentConfiguration:
    """Purpose: Own a validated experiment's source and resolved configuration state.

    Inputs: ``original`` is the exact decoded researcher document; ``resolved`` adds
    normalized paths and derived cardinalities under configuration version rules.
    Outputs: Immutable access to scientific identity, workflow, and worker contracts.
    Notes: Coordinators reload this state from DuckDB; workers receive selected fields.
    """

    original: dict[str, Any]
    resolved: dict[str, Any]

    @property
    def version(self) -> int:
        """Return the stored configuration-contract version."""
        return int(self.resolved["configuration_version"])

    @property
    def name(self) -> str:
        """Return the researcher-defined experiment name used in persisted metadata."""
        return str(self.resolved["experiment"]["name"])

    @property
    def date(self) -> str:
        """Return the ISO experiment date preserved in DuckDB."""
        return str(self.resolved["experiment"]["date"])

    @property
    def description(self) -> str:
        """Return the researcher-defined experiment description preserved in DuckDB."""
        return str(self.resolved["experiment"]["description"])

    @property
    def seed(self) -> int:
        """Return the immutable experiment seed available to current and future workers."""
        return int(self.resolved["reproducibility"]["seed"])

    @property
    def scientific_hash(self) -> str:
        """Return the digest of settings capable of changing scientific inputs or outputs."""
        return json_fingerprint(self.scientific_configuration)

    @property
    def scientific_configuration(self) -> dict[str, Any]:
        """Purpose: Select resolved fields that define scientific identity.

        Inputs: Validated resolved experiment state.
        Outputs: Deep-copied configuration excluding source paths and descriptive metadata.
        """
        data = deepcopy(self.resolved["data"])
        data["source"].pop("directory", None)
        evaluation = self.resolved["evaluation"]
        scientific = {
            "configuration_version": self.version,
            "reproducibility": self.resolved["reproducibility"],
            "data": data,
            "pipeline": self.resolved["pipeline"],
            "models": self.resolved["models"],
            "archived_forecasts": self.resolved["archived_forecasts"],
            "evaluation": {
                "method": evaluation["method"],
                "gift_eval": {
                    "code_revision": evaluation["gift_eval"]["code_revision"],
                },
                "options": evaluation["options"],
            },
        }
        if self.version >= 11:
            scientific["representations"] = self.resolved["representations"]
            scientific["classifiers"] = self.resolved["classifiers"]
        if self.version == 12:
            scientific["evaluation"]["table_reproduction"] = self.table_reproduction
        return scientific

    @property
    def table_reproduction(self) -> dict[str, Any] | None:
        """Return immutable paper-table science, or None for historical versions."""
        return deepcopy(self.resolved["evaluation"].get("table_reproduction"))

    @property
    def configuration_integrity_hash(self) -> str:
        """Return the digest of the complete resolved configuration document."""
        return json_fingerprint(self.resolved)

    @property
    def execution(self) -> dict[str, Any]:
        """Return the creation-time execution globals used by coordinators and workers."""
        return deepcopy(self.resolved["execution"]["default"])

    @property
    def model_storage(self) -> dict[str, Any]:
        """Return the centrally validated fitted-model root, namespace, and overwrite policy."""
        if self.version < 11:
            raise ExperimentConfigurationError("fitted-model storage requires configuration version 11")
        return deepcopy(self.resolved["execution"]["model_storage"])

    @property
    def execution_paths(self) -> dict[str, str]:
        """Purpose: Resolve configured environments and worker scripts for execution.

        Inputs: Integrity-validated ``execution.paths`` from JSON or DuckDB.
        Outputs: Independent path mapping with legacy worker filenames translated
        centrally to current substeps; stored configuration remains unchanged.
        """
        return {
            name: resolve_worker_path(path)
            for name, path in self.resolved["execution"]["paths"].items()
        }

    @property
    def auto_arima_settings(self) -> dict[str, Any]:
        """Purpose: Assemble the complete AutoARIMA worker contract.

        Inputs: Scientific model settings and centrally controlled R thread limit.
        Outputs: Independent settings mapping with package parallelism disabled.
        """
        settings = deepcopy(self.resolved["models"]["auto_arima"]["settings"])
        settings.update({
            "parallel": False,
            "num_cores": int(self.execution["thread_limits"]["r"]),
        })
        return settings

    def r_model_settings(self, model: str) -> dict[str, Any]:
        """Return the registered R method settings for one configured model.

        Native defaults remain in R; configured settings and the existing R
        thread limit cross the provider boundary without scientific substitution.
        """
        if model == "auto_arima":
            return self.auto_arima_settings
        if model in R_FORECAST_METHODS and model in self.resolved["models"]:
            return deepcopy(self.resolved["models"][model]["settings"])
        raise ExperimentConfigurationError(f"unsupported R forecast model: {model}")

    @property
    def seasonal_period_tuning(self) -> dict[str, Any] | None:
        """Return the opt-in Gate 4 tuning policy when the stored pipeline selects it."""
        if "seasonal_period_tuning" not in self.resolved["pipeline"]:
            return None
        return deepcopy(self.resolved["pipeline"]["seasonal_period_tuning"])

    @property
    def evaluation_options(self) -> dict[str, Any]:
        """Purpose: Assemble evaluator science and execution options.

        Inputs: Stored GIFT-Eval options and centrally controlled evaluation batch size.
        Outputs: Independent evaluator-options mapping.
        """
        options = deepcopy(self.resolved["evaluation"][
            "gift_eval_options" if self.version == 12 else "options"])
        if self.version < 10 or self.version == 12:
            options["batch_size"] = int(self.execution["batch_sizes"]["gift_eval"])
        return options

    @property
    def series_count(self) -> int:
        """Return the deterministic number of selected benchmark series."""
        return int(self.resolved["data"]["selection"]["count"])

    @property
    def source_directory(self) -> Path:
        """Return the repository-relative pinned GIFT-Eval source directory."""
        return Path(self.resolved["data"]["source"]["directory"])

    @property
    def r_period_override(self) -> int | None:
        """Return the explicit R-period override encoded by this contract version.

        Version 1 used ``data.benchmark.seasonality`` for preprocessing, R
        forecasting, and evaluation. Treating that value as an override preserves
        existing databases without rewriting their scientific interpretation.
        Version 2 stores the optional override at its intended pipeline boundary.
        """
        if self.version == 1:
            return int(self.resolved["data"]["benchmark"]["seasonality"])
        value = self.resolved["pipeline"]["r_period_override"]
        return None if value is None else int(value)

    def evaluation_seasonality(self, pinned_default: int) -> int:
        """Return the scorer period without coupling it to a v2 R-period override.

        Version 1 retains its historical configured value. New version-2
        experiments use the pinned GluonTS/GIFT-Eval convention supplied by the
        bridge, even when the researcher overrides preprocessing and R models.
        """
        if self.version == 1:
            return int(self.resolved["data"]["benchmark"]["seasonality"])
        return int(pinned_default)

    @property
    def import_settings(self) -> dict[str, Any]:
        """Purpose: Derive the reduced Stage 1 worker contract.

        Inputs: Authoritative data, benchmark, version, and selection settings.
        Outputs: Import configuration containing scientific fields and selected row limit.
        """
        data = self.resolved["data"]
        benchmark = data["benchmark"]
        result = {
            "schema_version": str(self.version),
            "dataset_name": data["dataset_name"],
            "source_system": data["source"]["system"],
            "benchmark": {
                "frequency": benchmark["frequency"],
                "term": benchmark["term"],
                "prediction_length": benchmark["prediction_length"],
                "evaluation_windows": benchmark["evaluation_windows"],
                "boundary_convention": benchmark["boundary_convention"],
            },
            "max_series": self.series_count,
            "archived_forecasts": deepcopy(self.resolved["archived_forecasts"]),
        }
        if self.version == 1:
            # Preserve the historical import identity of existing v1 experiments.
            result["benchmark"].update(
                seasonality=benchmark["seasonality"],
                seasonality_source=benchmark["seasonality_source"],
            )
        return result

    @property
    def workflow(self) -> dict[str, Any]:
        """Purpose: Derive the process-facing workflow from authoritative state.

        Inputs: Stored benchmark, pipeline, model, and evaluation configuration.
        Outputs: Independent mapping consumed by downstream process coordinators.
        """
        data = self.resolved["data"]
        pipeline = self.resolved["pipeline"]
        evaluation = self.resolved["evaluation"]
        workflow = {
            "contract_version": f"configuration-v{self.version}",
            "benchmark": {
                "configuration": data["benchmark"]["configuration"],
                "dataset_name": data["dataset_name"],
                "term": data["benchmark"]["term"],
                "gift_eval_revision": evaluation["gift_eval"]["code_revision"],
            },
            # ``cleaning`` remains the internal workflow key and database column
            # name for schema compatibility; its values are preprocessing modes.
            "cleaning": list(pipeline["preprocessing"]["modes"]),
            "default_preprocessing": pipeline["preprocessing"]["default"],
            "transformations": list(pipeline["transformations"]["methods"]),
            "models": deepcopy(self.resolved["models"]),
            "archived_forecasts": deepcopy(self.resolved["archived_forecasts"]),
            "adjustment": pipeline["adjustment"],
            "combination": deepcopy(pipeline["combination"]),
            "provisional_candidate": deepcopy(evaluation["provisional_candidate"]),
            "submission_metadata": deepcopy(evaluation["submission_metadata"]),
        }
        if self.version >= 11:
            workflow["representations"] = deepcopy(self.resolved["representations"])
            workflow["classifiers"] = deepcopy(self.resolved["classifiers"])
        if self.version == 12:
            workflow["table_reproduction"] = self.table_reproduction
        if self.version >= 4:
            # Version 4 retains its legacy single-context capability. Version 5
            # carries the separate, opt-in rolling-window preparation definition.
            workflow["window_preparation"] = deepcopy(pipeline["window_preparation"])
        return workflow


def _require_mapping(value: Any, field: str) -> dict[str, Any]:
    """Return a mapping value or raise a field-specific configuration error."""
    if not isinstance(value, dict):
        raise ExperimentConfigurationError(f"{field} must be an object")
    return value


def _require_keys(value: dict[str, Any], keys: set[str], field: str) -> None:
    """Require every named key in an experiment section."""
    missing = sorted(keys - value.keys())
    if missing:
        raise ExperimentConfigurationError(f"{field} is missing: {', '.join(missing)}")


def validate_experiment_configuration(value: dict[str, Any]) -> None:
    """Purpose: Validate the complete POC2 scientific and execution contract.

    Inputs: Decoded configuration mapping claiming a supported contract version.
    Outputs: ``None`` when every nested field and fixed protocol value is valid;
    otherwise raises ``ExperimentConfigurationError`` with field-level context.
    """
    if value.get("configuration_version") == 12:
        _validate_table_configuration(value)
        return
    _require_keys(
        value,
        {"configuration_version", "experiment", "reproducibility", "data", "pipeline", "models", "archived_forecasts", "evaluation", "execution"},
        "configuration",
    )
    version = value["configuration_version"]
    if isinstance(version, bool) or version not in SUPPORTED_CONFIGURATION_VERSIONS:
        raise ExperimentConfigurationError(
            "configuration_version must be one of "
            f"{sorted(SUPPORTED_CONFIGURATION_VERSIONS)}"
        )
    experiment = _require_mapping(value["experiment"], "experiment")
    _require_keys(experiment, {"name", "date", "description"}, "experiment")
    for field in ("name", "date", "description"):
        if not isinstance(experiment[field], str) or not experiment[field].strip():
            raise ExperimentConfigurationError(f"experiment.{field} must be non-empty")
    try:
        from datetime import date

        date.fromisoformat(experiment["date"])
    except ValueError as exc:
        raise ExperimentConfigurationError("experiment.date must use YYYY-MM-DD") from exc

    reproducibility = _require_mapping(value["reproducibility"], "reproducibility")
    _require_keys(reproducibility, {"seed"}, "reproducibility")
    seed = reproducibility["seed"]
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise ExperimentConfigurationError("reproducibility.seed must be a non-negative integer")

    data = _require_mapping(value["data"], "data")
    _require_keys(data, {"dataset_name", "source", "benchmark", "selection"}, "data")
    source = _require_mapping(data["source"], "data.source")
    _require_keys(source, {"system", "revision", "directory", "files"}, "data.source")
    for field in ("system", "revision", "directory"):
        if not isinstance(source[field], str) or not source[field].strip():
            raise ExperimentConfigurationError(f"data.source.{field} must be non-empty")
    source_files = _require_mapping(source["files"], "data.source.files")
    required_source_files = {
        "data-00000-of-00001.arrow",
        "dataset_info.json",
        "state.json",
    }
    if set(source_files) != required_source_files or any(
        not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
        for digest in source_files.values()
    ):
        raise ExperimentConfigurationError(
            "data.source.files must contain the three pinned lowercase SHA-256 digests"
        )
    benchmark = _require_mapping(data["benchmark"], "data.benchmark")
    expected_benchmark = {
        "configuration": "m4_daily/D/short",
        "frequency": "D",
        "term": "short",
        "domain": "Econ/Fin",
        "num_variates": 1,
        "prediction_length": 14,
        "evaluation_windows": 1,
        "boundary_convention": "zero-based, end-exclusive",
    }
    if version == 1:
        expected_benchmark.update(
            seasonality=7,
            seasonality_source="approved M4 Daily weekly cycle from official frequency",
        )
    if data["dataset_name"] != "m4_daily" or benchmark != expected_benchmark:
        raise ExperimentConfigurationError(
            f"data must define the exact M4 Daily benchmark {expected_benchmark!r}"
        )
    selection = _require_mapping(data["selection"], "data.selection")
    if selection != {"method": "first_official", "count": 100}:
        raise ExperimentConfigurationError(
            "data.selection must deterministically select the first 100 official series"
        )

    pipeline = _require_mapping(value["pipeline"], "pipeline")
    _require_keys(
        pipeline,
        {"processes", "preprocessing", "transformations", "adjustment", "combination"},
        "pipeline",
    )
    if version == 1 and "r_period_override" in pipeline:
        raise ExperimentConfigurationError(
            "pipeline.r_period_override belongs to configuration version 2"
        )
    if version >= 2:
        _require_keys(pipeline, {"r_period_override"}, "pipeline")
        override = pipeline["r_period_override"]
        if override is not None and (
            isinstance(override, bool) or not isinstance(override, int) or override < 1
        ):
            raise ExperimentConfigurationError(
                "pipeline.r_period_override must be null or a positive integer"
            )
    expected_processes = [
        {"id": process_id, "name": name} for process_id, name in PROCESS_NAMES.items()
    ]
    if pipeline["processes"] != expected_processes:
        raise ExperimentConfigurationError("pipeline.processes must define ordered Processes 01-06")
    expected_preprocessing = {
        "default": "robust",
        "modes": ["robust"] if version in {8, 9, 10, 11} else ["standard", "robust"],
    }
    if pipeline["preprocessing"] != expected_preprocessing:
        raise ExperimentConfigurationError(
            f"v{version} preprocessing must select only robust"
            if version in {8, 9, 10, 11}
            else "preprocessing must expose standard and robust with robust as default"
        )
    expected_transformations = {
        "methods": [
            "standardise_sample_v1"
        ] if version in {8, 9, 10, 11} else [
            "identity",
            "standardise_sample_v1" if version >= 4 else "minmax_then_standardize",
        ]
    }
    if pipeline["transformations"] != expected_transformations:
        raise ExperimentConfigurationError("unsupported transformation configuration")
    if version == 4:
        _require_keys(pipeline, {"window_preparation"}, "pipeline")
        window_preparation = _require_mapping(
            pipeline["window_preparation"], "pipeline.window_preparation"
        )
        context_length = window_preparation.get("context_length")
        if (
            set(window_preparation) != {"context_length"}
            or isinstance(context_length, bool)
            or not isinstance(context_length, int)
            or context_length < 1
        ):
            raise ExperimentConfigurationError(
                "pipeline.window_preparation.context_length must be a positive integer"
            )
    elif version in {5, 6, 7, 8, 9, 10, 11}:
        _require_keys(pipeline, {"window_preparation"}, "pipeline")
        window_preparation = _require_mapping(
            pipeline["window_preparation"], "pipeline.window_preparation"
        )
        if version >= 6:
            supported_frequencies = {"10S", "5T", "10T", "15T", "H", "D", "W", "M", "Q", "Y"}
            required_fields = {
                "selected_frequencies", "frequencies", "stride_rule", "block_policy",
                "boundary_policy", "preprocessing_mode", "transformation", "split",
            }
            if set(window_preparation) != required_fields:
                raise ExperimentConfigurationError(
                    "pipeline.window_preparation must define the complete v6 contract"
                )
            frequencies = _require_mapping(
                window_preparation["frequencies"], "pipeline.window_preparation.frequencies"
            )
            if not frequencies or not set(frequencies).issubset(supported_frequencies):
                raise ExperimentConfigurationError(
                    "window frequencies must be nonempty supported frequency keys"
                )
            for frequency, settings_value in frequencies.items():
                settings = _require_mapping(
                    settings_value, f"pipeline.window_preparation.frequencies.{frequency}"
                )
                if set(settings) != {"input_length", "future_horizon"} or any(
                    isinstance(settings.get(field), bool)
                    or not isinstance(settings.get(field), int)
                    or settings[field] < 1
                    for field in ("input_length", "future_horizon")
                ):
                    raise ExperimentConfigurationError(
                        f"window frequency {frequency} requires positive integer input_length and future_horizon"
                    )
            selected = window_preparation["selected_frequencies"]
            if (
                not isinstance(selected, list)
                or not selected
                or len(selected) != len(set(selected))
                or not set(selected).issubset(frequencies)
            ):
                raise ExperimentConfigurationError(
                    "selected_frequencies must be a nonempty unique subset of configured frequencies"
                )
            if (
                window_preparation["stride_rule"] != "input_plus_future"
                or window_preparation["block_policy"] != "complete_non_overlapping"
                or window_preparation["boundary_policy"] != "official_training_only"
                or window_preparation["preprocessing_mode"] != "robust"
                or window_preparation["transformation"] != "standardise_sample_v1"
            ):
                raise ExperimentConfigurationError("unsupported v6 window-preparation policy")
            split = _require_mapping(
                window_preparation["split"], "pipeline.window_preparation.split"
            )
            if (
                set(split) != {
                    "split_id", "unit", "training_fraction", "seed", "generator",
                    "membership_source", "test_count_rule",
                }
                or split["split_id"] != "S1"
                or split["unit"] != "series"
                or isinstance(split["training_fraction"], bool)
                or not isinstance(split["training_fraction"], (int, float))
                or not 0 < split["training_fraction"] < 1
                or isinstance(split["seed"], bool)
                or not isinstance(split["seed"], int)
                or split["seed"] < 0
                or split["generator"] != "tsai.TrainValidTestSplitter"
                or split["membership_source"] != "generate"
                or split["test_count_rule"] != "r_double_floor_v1"
            ):
                raise ExperimentConfigurationError("unsupported v6 S1 split contract")
        else:
            # Keep the exact v5 document and its original Decimal split semantics
            # valid for historical databases and resumes.
            expected_windows = {
                "10S": {"input_length": 512, "future_horizon": 60},
                "5T": {"input_length": 512, "future_horizon": 48},
                "10T": {"input_length": 512, "future_horizon": 48},
                "15T": {"input_length": 512, "future_horizon": 48},
                "H": {"input_length": 256, "future_horizon": 48},
                "D": {"input_length": 64, "future_horizon": 14},
                "W": {"input_length": 64, "future_horizon": 13},
                "M": {"input_length": 64, "future_horizon": 18},
                "Q": {"input_length": 32, "future_horizon": 8},
                "Y": {"input_length": 16, "future_horizon": 6},
            }
            expected_split = {
                "split_id": "S1",
                "unit": "series",
                "training_fraction": 0.8,
                "seed": 123,
                "generator": "tsai.TrainValidTestSplitter",
                "membership_source": "generate",
            }
            if window_preparation != {
                "selected_frequencies": ["D"],
                "frequencies": expected_windows,
                "stride_rule": "input_plus_future",
                "block_policy": "complete_non_overlapping",
                "boundary_policy": "official_training_only",
                "preprocessing_mode": "robust",
                "transformation": "standardise_sample_v1",
                "split": expected_split,
            }:
                raise ExperimentConfigurationError(
                    "pipeline.window_preparation must equal the approved v5 rolling-window and S1 contract"
                )
    elif "window_preparation" in pipeline:
        raise ExperimentConfigurationError(
            "pipeline.window_preparation belongs to configuration version 4 or later"
        )
    tuning_enabled = "seasonal_period_tuning" in pipeline
    if pipeline["adjustment"] != "identity":
        raise ExperimentConfigurationError("unsupported adjustment method")
    expected_combination = (
        {"method": "none", "weights": {}}
        if version in {10, 11} else
        {"method": "m4_comb", "weights": {
            "ses": 1.0 / 3.0, "holt": 1.0 / 3.0, "damped": 1.0 / 3.0,
        }}
        if version == 9 else
        {
            "method": "equal_weight",
            "weights": (
                {name: 1.0 / len(value["models"]) for name in value["models"]}
                if version in {7, 8} and value["models"] else
                {"auto_arima": 0.5, "ets": 0.5}
                if tuning_enabled
                else {"auto_arima": 0.5, "chronos_2": 0.5}
            ),
        }
    )
    if pipeline["combination"] != expected_combination:
        raise ExperimentConfigurationError("unsupported forecast combination")

    if version == 3 and not tuning_enabled:
        _require_keys(pipeline, {"seasonal_period_tuning"}, "pipeline")
    if tuning_enabled:
        if version not in {3, 4}:
            raise ExperimentConfigurationError(
                "pipeline.seasonal_period_tuning belongs to configuration version 3 or 4"
            )
        tuning = _require_mapping(
            pipeline["seasonal_period_tuning"], "pipeline.seasonal_period_tuning"
        )
        expected_tuning = {
            "enabled": True,
            "validation_windows": 3,
            "horizon": "dataset_horizon",
            "origin_spacing": "dataset_horizon",
            "training": "expanding",
            "minimum_cycles": 3,
            "selection_metric": "mae",
            "tie_policy": "retain_baseline",
            "inconclusive_policy": "retain_baseline",
            "candidate_estimator": "forecast::findfrequency",
            "diagnostic": "tsfeatures::seasonal_strength",
        }
        if tuning != expected_tuning:
            raise ExperimentConfigurationError(
                "seasonal_period_tuning must equal the approved three-window MAE policy"
            )
    models = _require_mapping(value["models"], "models")
    expected_models = (
        {"auto_arima", "ets"}
        if tuning_enabled
        else {"auto_arima", "chronos_2"}
    )
    if version in {10, 11}:
        expected_directional = {
            "method": "one_nearest_neighbour",
            "engine": {
                "package": "aeon",
                "version": "1.6.0",
                "function": "aeon.distances.dtw_distance",
            },
            "input": {
                "definition": "standardise_sample_v1",
                "length": 64,
                "dtype": "float64",
            },
            "labels": {
                "definition": "directional_strict_v1",
                "horizons": list(range(1, 15)),
            },
            "reference": {
                "membership": "S1",
                "partition": "train",
                "cap": None,
                "sampling": "none",
            },
            "constraint": {
                "name": "sakoe_chiba",
                "candidate_proportions": [index / 100 for index in range(100)],
                "effective_width": "int(window * 64)",
            },
            "local_cost": "squared_euclidean",
            "width_selection": "one_per_horizon",
            "calibration_tie_rule": "smallest_effective_width",
            "neighbour_tie_rule": "lowest_stable_reference_identity",
            "probabilities": False,
        }
        expected_models = {"directional_dtw": expected_directional}
        if version == 11:
            representations = _require_mapping(
                value.get("representations"), "representations"
            )
            classifiers = _require_mapping(value.get("classifiers"), "classifiers")
            expected_representation = {
                "provider": "mantis.Mantis8M",
                "package_version": "1.1.0",
                "checkpoint_repository": "paris-noah/Mantis-8M",
                "checkpoint_revision": "bc7d5ab40c02133386a28e2c127f35c17c86901d",
                "checkpoint_files": {
                    "config.json": "c9e8b1e5d9b95510c7d446d1e1ef46d5d36c3eae0c7a9b409c8d3c6c7ce7837c",
                    "model.safetensors": "d5077b60438c477a627feeef0e7365588bdb351eaa5b5bcfb0b3d5fc5804f8c8",
                },
                "checkpoint_fingerprint": "5ee9a5cf70755561d168b23302d1d35329da05eef50e28c382fc6bb2ace81a8c",
                "input_length": 64,
                "resize": {"length": 512, "mode": "linear", "align_corners": False},
                "representation": "legacy_final_transformer_layer_cls",
                "dtype": "float32",
                "dimension": 256,
                "frozen": True,
            }
            expected_parameters = {
                "bootstrap": True, "ccp_alpha": 0.0, "class_weight": None,
                "criterion": "gini", "max_depth": None, "max_features": "sqrt",
                "max_leaf_nodes": None, "max_samples": None,
                "min_impurity_decrease": 0.0, "min_samples_leaf": 1,
                "min_samples_split": 2, "min_weight_fraction_leaf": 0.0,
                "monotonic_cst": None, "n_estimators": 200, "n_jobs": 1,
                "oob_score": False, "random_state": 42, "verbose": 0,
                "warm_start": False,
            }
            expected_classifier = {
                "classifier_id": "classifier/random-forest/c5bcf252d0e53262dd22271bbe316911",
                "implementation": "sklearn.ensemble.RandomForestClassifier",
                "implementation_version": "1.7.2",
                "parameters": expected_parameters,
                "seed": 42,
                "feature_dimension": 256,
                "feature_dtype": "float32",
                "output_contract": "binary_integer_v1",
            }
            if representations != {"mantis_8m_legacy_cls": expected_representation}:
                raise ExperimentConfigurationError(
                    "v11 representations must define the approved frozen Mantis contract"
                )
            if classifiers != {"random_forest": expected_classifier}:
                raise ExperimentConfigurationError(
                    "v11 classifiers must define the approved Random Forest contract"
                )
            expected_models["directional_mantis_rf"] = {
                "method": "composite_directional_classifier",
                "representation": "mantis_8m_legacy_cls",
                "classifier": "random_forest",
                "input": {
                    "definition": "standardise_sample_v1",
                    "length": 64,
                    "dtype": "float64",
                },
                "labels": {
                    "definition": "directional_strict_v1",
                    "horizons": list(range(1, 15)),
                },
                "reference": {
                    "membership": "S1", "partition": "train",
                    "cap": None, "sampling": "none",
                },
            }
        elif "representations" in value or "classifiers" in value:
            raise ExperimentConfigurationError(
                "representations and classifiers belong to configuration version 11"
            )
        if models != expected_models:
            raise ExperimentConfigurationError(
                f"v{version} models must define the approved directional baseline set"
            )
    elif version in {7, 8, 9}:
        expected_pool = (
            set(R_FORECAST_METHODS) | {"chronos_2"}
            if version == 9 else
            set(R_MODEL_METHODS) | ({"chronos_2"} if version == 8 else set())
        )
        if set(models) != expected_pool:
            raise ExperimentConfigurationError(
                f"v{version} models require the approved nine-method R pool"
                + (" plus Chronos-2" if version in {8, 9} else "")
            )
        for model in set(models) & R_FORECAST_METHODS.keys():
            settings = {"opt_crit": "mae"} if model == "ets" else {}
            if model == "auto_arima":
                settings = {"stepwise": False, "approximation": False,
                            "allowdrift": True, "allowmean": True,
                            "interval_levels": [20, 40, 60, 80, 95] if version == 9
                            else [20, 40, 60, 80]}
            if models[model] != {"package": "forecast", "settings": settings}:
                raise ExperimentConfigurationError(f"unsupported approved R pool settings for {model}")
    elif set(models) != expected_models:
        raise ExperimentConfigurationError(
            "models must contain AutoARIMA and ETS for v3 tuning"
            if tuning_enabled
            else "models must contain AutoARIMA and Chronos-2"
        )
    auto = _require_mapping(models.get("auto_arima", {"package": "forecast", "settings": {}}), "models.auto_arima")
    _require_keys(auto, {"package", "settings"}, "models.auto_arima")
    required_auto = {
        "stepwise": True,
        "approximation": False,
        "allowdrift": True,
        "allowmean": True,
        "interval_levels": [20, 40, 60, 80],
    }
    if version < 7 and (auto["package"] != "forecast" or auto["settings"] != required_auto):
        raise ExperimentConfigurationError("unsupported AutoARIMA settings")
    if tuning_enabled:
        ets = _require_mapping(models["ets"], "models.ets")
        if ets != {"package": "forecast", "settings": {"opt_crit": "mae"}}:
            raise ExperimentConfigurationError("unsupported ETS settings")
    elif "chronos_2" in models:
        chronos = _require_mapping(models["chronos_2"], "models.chronos_2")
        _require_keys(
            chronos,
            {"repository", "revision", "chronos_forecasting", "dtype", "quantile_levels", "predict_batches_jointly", "cross_learning"},
            "models.chronos_2",
        )
        quantiles = chronos["quantile_levels"]
        expected_quantiles = PROBABILISTIC_QUANTILES if version == 9 else [
            0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9
        ]
        if quantiles != expected_quantiles:
            raise ExperimentConfigurationError("Chronos quantile levels do not match the versioned profile")
        if (
            chronos["repository"] != "amazon/chronos-2"
            or not isinstance(chronos["revision"], str)
            or len(chronos["revision"]) != 40
            or chronos["chronos_forecasting"] != "2.2.2"
            or chronos["dtype"] != "float32"
            or chronos["predict_batches_jointly"] is not False
            or chronos["cross_learning"] is not False
        ):
            raise ExperimentConfigurationError("unsupported Chronos-2 settings")

    archived = _require_mapping(value["archived_forecasts"], "archived_forecasts")
    approved_archived = {
        "m4_smyl": {"submission_id": 118},
        "m4_fforma": {"submission_id": 245},
    }
    enabled_archived = archived.get("enabled")
    if (
        archived.get("providers") != approved_archived
        or not isinstance(enabled_archived, list)
        or (not enabled_archived and version not in {10, 11})
        or len(enabled_archived) != len(set(enabled_archived))
        or any(provider not in approved_archived for provider in enabled_archived)
        or archived.get("forecast_capability") != "mean_only"
        or archived.get("reference_designation") != "official_reference"
        or set(archived)
        != {"providers", "enabled", "forecast_capability", "reference_designation"}
    ):
        raise ExperimentConfigurationError(
            "archived_forecasts must select only the approved Smyl/FFORMA mappings"
        )

    evaluation = _require_mapping(value["evaluation"], "evaluation")
    _require_keys(
        evaluation,
        {"method", "gift_eval", "options", "provisional_candidate", "submission_metadata"},
        "evaluation",
    )
    expected_evaluation_method = (
        "directional_accuracy" if version in {10, 11} else "gift_eval"
    )
    if evaluation["method"] != expected_evaluation_method:
        raise ExperimentConfigurationError(
            f"evaluation.method must be {expected_evaluation_method}"
        )
    gift_eval = _require_mapping(evaluation["gift_eval"], "evaluation.gift_eval")
    _require_keys(
        gift_eval,
        {"code_revision", "environment", "source_directory"},
        "evaluation.gift_eval",
    )
    if (
        not isinstance(gift_eval["code_revision"], str)
        or len(gift_eval["code_revision"]) != 40
        or not isinstance(gift_eval["environment"], str)
        or not gift_eval["environment"]
        or not isinstance(gift_eval["source_directory"], str)
        or not gift_eval["source_directory"]
    ):
        raise ExperimentConfigurationError("invalid GIFT-Eval dependency settings")
    expected_evaluation_options = {
        "label_definition": "directional_strict_v1",
        "horizons": list(range(1, 15)),
        "missing_labels": "error",
    } if version in {10, 11} else {
        "axis": None,
        "mask_invalid_label": True,
        "allow_nan_forecast": False,
        "seasonality": (
            "configured official benchmark seasonality"
            if version == 1
            else "pinned GluonTS benchmark seasonality"
        ),
    }
    if evaluation["options"] != expected_evaluation_options:
        raise ExperimentConfigurationError("unsupported GIFT-Eval options")

    execution = _require_mapping(value["execution"], "execution")
    _require_keys(
        execution,
        {"default", "final_acceptance", "paths", "restart"},
        "execution",
    )
    default = _require_mapping(execution["default"], "execution.default")
    _require_keys(
        default,
        {
            "mode",
            "import_workers",
            "process_workers",
            "batch_sizes",
            "dask_max_in_flight",
            "dask_retries",
            "dask_timeout_seconds",
            "worker_timeouts_seconds",
            "thread_limits",
            "cpu_gpu_overlap",
            "system_memory_min_available_gib",
            "accelerator_memory_min_available_gib",
            "database_writers",
        },
        "execution.default",
    )
    if default.get("database_writers") != 1:
        raise ExperimentConfigurationError("execution.default.database_writers must be 1")
    process_workers = _require_mapping(
        default["process_workers"], "execution.default.process_workers"
    )
    if (
        default["mode"] not in {"sequential", "dask"}
        or set(process_workers) != {str(process_id) for process_id in range(2, 7)}
        or any(
            isinstance(item, bool) or not isinstance(item, int) or item < 1
            for item in process_workers.values()
        )
    ):
        raise ExperimentConfigurationError(
            "execution.default must define a supported mode and positive workers for Processes 02-06"
        )
    for field in (
        "import_workers",
        "dask_max_in_flight",
        "dask_timeout_seconds",
    ):
        if not isinstance(default[field], (int, float)) or default[field] <= 0:
            raise ExperimentConfigurationError(f"execution.default.{field} must be positive")
    if not isinstance(default["dask_retries"], int) or default["dask_retries"] < 0:
        raise ExperimentConfigurationError("execution.default.dask_retries cannot be negative")
    if version >= 6 and (
        isinstance(default.get("window_preparation_windows_per_job"), bool)
        or not isinstance(default.get("window_preparation_windows_per_job"), int)
        or default["window_preparation_windows_per_job"] < 1
    ):
        raise ExperimentConfigurationError(
            "execution.default.window_preparation_windows_per_job must be a positive integer"
        )
    expected_batch_sizes = {
        "import", "plan", "preprocess", "transform", "auto_arima", "chronos",
        "combine", "gift_eval",
    }
    if version in {5, 6, 7, 8, 9, 10, 11}:
        expected_batch_sizes.add("window_preparation")
    if version in {10, 11}:
        expected_batch_sizes -= {"auto_arima", "chronos", "combine", "gift_eval"}
        expected_batch_sizes |= {"directional_calibration", "directional_prediction"}
    if version == 11:
        expected_batch_sizes |= {"mantis_representation", "random_forest_classifier"}
    if tuning_enabled or version in {7, 8, 9}:
        expected_batch_sizes.add("r_forecast")
    batch_sizes = _require_mapping(default["batch_sizes"], "execution.default.batch_sizes")
    if set(batch_sizes) != expected_batch_sizes or any(
        isinstance(item, bool) or not isinstance(item, int) or item < 1
        for item in batch_sizes.values()
    ):
        raise ExperimentConfigurationError(
            "execution.default.batch_sizes must define positive integer process batches"
        )
    expected_timeouts = (
        {"r", "directional_dtw", "mantis", "classifier"}
        if version == 11
        else {"r", "directional_dtw"}
        if version == 10
        else {"r", "chronos_startup", "chronos_request", "gift_eval"}
    )
    timeouts = _require_mapping(
        default["worker_timeouts_seconds"], "execution.default.worker_timeouts_seconds"
    )
    if set(timeouts) != expected_timeouts or any(
        isinstance(item, bool) or not isinstance(item, (int, float)) or item <= 0
        for item in timeouts.values()
    ):
        raise ExperimentConfigurationError(
            "execution.default.worker_timeouts_seconds must define positive worker timeouts"
        )
    expected_threads = (
        {"r", "classifier", "dask_worker"}
        if version in {10, 11}
        else {"r", "chronos", "dask_worker"}
    )
    threads = _require_mapping(default["thread_limits"], "execution.default.thread_limits")
    if set(threads) != expected_threads or any(
        isinstance(item, bool) or not isinstance(item, int) or item < 1
        for item in threads.values()
    ):
        raise ExperimentConfigurationError(
            "execution.default.thread_limits must define positive integer thread limits"
        )
    acceptance = _require_mapping(execution["final_acceptance"], "execution.final_acceptance")
    topology = _require_mapping(acceptance.get("workers"), "execution.final_acceptance.workers")
    if tuning_enabled:
        if (
            topology != {"mac_cpu": 1, "total": 1}
            or acceptance.get("mode") != "sequential"
            or acceptance.get("worker_memory_gib") != {"mac_cpu": 2}
            or set(acceptance) != {"mode", "workers", "worker_memory_gib"}
        ):
            raise ExperimentConfigurationError("v3 acceptance must use one bounded Mac CPU worker")
    elif version == 4:
        if (
            topology != {"mac_cpu": 1, "total": 1}
            or acceptance.get("mode") != "sequential"
            or acceptance.get("processes") != [1, 2, 3]
            or acceptance.get("system_memory_min_available_gib") != 2.0
            or set(acceptance)
            != {
                "mode",
                "processes",
                "workers",
                "system_memory_min_available_gib",
            }
        ):
            raise ExperimentConfigurationError(
                "v4 acceptance must describe the sequential Gate 1-3 run"
            )
    elif version in {5, 6, 7, 8, 9, 10, 11}:
        expected_acceptance = {
            "mode": "dask",
            "workflow": (
                "directional_comparison"
                if version == 11
                else "directional_dtw"
                if version == 10
                else "forecast_pool"
                if version in {7, 8, 9}
                else "window_preparation"
            ),
            "execution_profile": "poc2_seasonal_recovery",
            "profile_version": 3 if version in {7, 8, 9, 10, 11} else 2,
            "workers": {"mac_cpu": 8, "ubuntu_cpu": 15, "total": 23},
            "system_memory_min_available_gib": {"mac": 3, "ubuntu": 16},
        }
        if version in {8, 9}:
            expected_acceptance.update({
                "workers": {
                    "mac_cpu": 8,
                    "ubuntu_cpu": 15,
                    "ubuntu_gpu": 15,
                    "total": 38,
                },
                "physical_gpu_count": 1,
                "gpu_name": "NVIDIA GeForce RTX 5090",
                "accelerator_memory_min_available_gib": 4,
            })
        if version == 11:
            expected_acceptance.update({
                "workers": {
                    "mac_cpu": 8,
                    "ubuntu_cpu": 15,
                    "ubuntu_gpu": 1,
                    "total": 24,
                },
                "mantis_gpu_processes": 1,
                "physical_gpu_count": 1,
                "gpu_name": "NVIDIA GeForce RTX 5090",
                "accelerator_memory_min_available_gib": 4,
            })
        if acceptance != expected_acceptance:
            raise ExperimentConfigurationError(
                f"v{version} acceptance must describe its approved two-host workflow profile"
            )
    else:
        if topology != {"mac_cpu": 1, "ubuntu_cpu": 0, "ubuntu_gpu": 1, "total": 2}:
            raise ExperimentConfigurationError("final acceptance must define exactly two Dask workers")
        if (
            acceptance.get("mode") != "dask"
            or acceptance.get("physical_gpu_count") != 1
            or acceptance.get("worker_memory_gib") != {"mac_cpu": 2, "ubuntu_gpu": 4}
            or acceptance.get("resource_safety")
            != {
                "mac_system_memory_bytes": 17179869184,
                "ubuntu_system_memory_bytes": 128000000000,
                "mac_min_available_gib": 3,
                "ubuntu_min_available_gib": 16,
                "gpu_min_available_gib": 4,
                "persistent_unsafe_samples": 3,
            }
        ):
            raise ExperimentConfigurationError("invalid final-acceptance resource settings")
    if version == 11:
        storage = _require_mapping(execution.get("model_storage"), "execution.model_storage")
        if storage != {
            "root": "models",
            "experiment": value["experiment"]["name"],
            "overwrite": False,
        }:
            raise ExperimentConfigurationError(
                "v11 execution.model_storage must define the approved root, namespace, and default"
            )
    elif "model_storage" in execution:
        raise ExperimentConfigurationError("execution.model_storage belongs to version 11")
    paths = _require_mapping(execution["paths"], "execution.paths")
    required_paths = (
        {
            "project_environment",
            "classifiers_environment",
            "classifiers_lock",
            "directional_dtw_worker",
            "r_preprocess_worker",
            "r_m4comp2018_worker",
        }
        if version in {10, 11}
        else {
            "project_environment",
            "chronos_environment",
            "chronos_worker",
            "r_preprocess_worker",
            "r_auto_arima_worker",
            "r_m4comp2018_worker",
        }
    )
    if version == 11:
        required_paths.remove("directional_dtw_worker")
        required_paths |= {
            "mantis_environment", "mantis_lock", "mantis_worker",
            "directional_dtw_training_worker", "directional_dtw_prediction_worker",
            "random_forest_training_worker", "random_forest_prediction_worker",
        }
    if tuning_enabled or version in {7, 8, 9}:
        required_paths.add("r_forecast_worker")
    if set(paths) != required_paths or any(
        not isinstance(path, str) or not path for path in paths.values()
    ):
        raise ExperimentConfigurationError("execution.paths must define all worker paths")
    if execution["restart"] != {
        "completed_tasks": "skip",
        "failed_tasks": "retry",
        "interrupted_tasks": "retry",
    }:
        raise ExperimentConfigurationError("unsupported restart policy")


def _validate_table_configuration(value: dict[str, Any]) -> None:
    """Validate v12 mixed science while reusing the unchanged v11 provider contract.

    Inputs: Complete fresh paper-table document. Outputs: None or a field-specific
    error; the v11 projection is validation-only and never stored or executed.
    """
    _require_keys(value, {"configuration_version", "experiment", "reproducibility",
        "data", "pipeline", "models", "archived_forecasts", "evaluation",
        "execution", "representations", "classifiers"}, "configuration")
    expected_tables = {
        "frequencies": ["D"],
        "adjustment": "mantis_terminal_scalar_v1",
        "grid": {
            "version": "historical_mantis_grid_v1",
            "lambda_up": {"start": 1.0, "stop": 1.12, "step": 0.005},
            "lambda_down": {"start": 1.0, "stop": 0.9, "step": -0.005},
            "inclusive": True,
        },
        "adjusted_base_models": ["m4_smyl", "chronos_2"],
        "direction_model": "directional_mantis_rf",
        "reference_model": "naive2",
        "metric_profile": "m4_paper_tables_v1",
        "outputs": ["Table1", "Table2_Daily", "Figure2"],
        "directional_report_models": [
            "naive2", "m4_fforma", "m4_smyl", "chronos_2",
            "directional_dtw", "directional_mantis_rf",
            "m4_smyl_mantis", "chronos_2_mantis", "m4_smyl_oracle",
        ],
    }
    evaluation = _require_mapping(value.get("evaluation"), "evaluation")
    tables = _require_mapping(evaluation.get("table_reproduction"), "evaluation.table_reproduction")
    grid = _require_mapping(tables.get("grid"), "evaluation.table_reproduction.grid")
    for axis in ("lambda_up", "lambda_down"):
        definition = _require_mapping(grid.get(axis), f"evaluation.table_reproduction.grid.{axis}")
        if any(isinstance(definition.get(field), bool)
               or not isinstance(definition.get(field), (int, float))
               for field in ("start", "stop", "step")):
            raise ExperimentConfigurationError("table grid endpoints and steps must be numbers")
    if grid.get("inclusive") is not True:
        raise ExperimentConfigurationError("table grid must be explicitly inclusive")
    report_models = tables.get("directional_report_models")
    if (not isinstance(report_models, list) or not report_models
        or any(not isinstance(model, str) for model in report_models)
        or len(report_models) != len(set(report_models))
        or not set(report_models).issubset(expected_tables["directional_report_models"])):
        raise ExperimentConfigurationError("v12 directional report models must be unique applicable model IDs")
    expected_tables["directional_report_models"] = report_models
    figure = _require_mapping(tables.get("figure_2"), "evaluation.table_reproduction.figure_2")
    if set(figure) != {"horizon_models", "cd_models", "cd_settings"}:
        raise ExperimentConfigurationError("Figure 2 requires horizon_models, cd_models and cd_settings")
    applicable = {"directional_mantis_rf", "directional_dtw", "m4_fforma", "chronos_2", "m4_smyl"}
    for subset in [figure.get("horizon_models"), figure.get("cd_models")]:
        if (not isinstance(subset, list) or len(subset) < 2
                or any(not isinstance(m, str) for m in subset)
                or len(set(subset)) != len(subset)
                or not set(subset).issubset(applicable & set(report_models))):
            raise ExperimentConfigurationError("Figure 2 requires explicit unique approved stored models")
    cd_settings = _require_mapping(figure.get("cd_settings"), "figure_2.cd_settings")
    if (cd_settings != {"alpha": 0.05, "reverse": True, "cex": 0.75, "useDingbats": False}
            or cd_settings["reverse"] is not True or cd_settings["useDingbats"] is not False):
        raise ExperimentConfigurationError("Figure 2 must preserve historical scmamp settings")
    expected_tables["figure_2"] = figure
    if evaluation.get("method") != "paper_tables" or tables != expected_tables:
        raise ExperimentConfigurationError("v12 evaluation must define the approved Daily paper-table science")
    models = _require_mapping(value.get("models"), "models")
    if set(models) != set(R_FORECAST_METHODS) | {"chronos_2", "directional_dtw", "directional_mantis_rf"}:
        raise ExperimentConfigurationError("v12 requires the complete v9 pool and both v11 directional models")
    if models["naive2"] != {"package": "forecast", "settings": {}}:
        raise ExperimentConfigurationError("v12 must preserve native Naive2 settings")
    if models["chronos_2"] != {
        "repository": "amazon/chronos-2",
        "revision": "29ec3766d36d6f73f0696f85560a422f50e8498c",
        "chronos_forecasting": "2.2.2", "dtype": "float32",
        "quantile_levels": PROBABILISTIC_QUANTILES,
        "predict_batches_jointly": False, "cross_learning": False,
    }:
        raise ExperimentConfigurationError("v12 must preserve the v9 Chronos-2 contract")
    archived = _require_mapping(value["archived_forecasts"], "archived_forecasts")
    if archived.get("enabled") != ["m4_smyl", "m4_fforma"]:
        raise ExperimentConfigurationError("v12 requires both approved archive means")
    projected = deepcopy(value)
    data = _require_mapping(value["data"], "data")
    selection = _require_mapping(data.get("selection"), "data.selection")
    count = selection.get("count")
    if (selection.get("method") != "first_official" or type(count) is not int or count < 1
            or set(selection) - {"method", "count", "expected_source_total"}
            or (count == 4227 or "expected_source_total" in selection)
            and selection != {"method": "first_official", "count": 4227, "expected_source_total": 4227}):
        raise ExperimentConfigurationError("v12 selection requires a positive count or guarded full Daily 4227")
    projected["data"]["selection"] = {"method": "first_official", "count": 100}
    projected["configuration_version"] = 11
    projected["models"] = {key: models[key] for key in ("directional_dtw", "directional_mantis_rf")}
    projected["pipeline"]["combination"] = {"method": "none", "weights": {}}
    projected["evaluation"].pop("table_reproduction")
    projected["evaluation"].pop("gift_eval_options")
    projected["evaluation"]["method"] = "directional_accuracy"
    execution = _require_mapping(projected["execution"], "execution")
    acceptance = _require_mapping(execution.get("final_acceptance"), "execution.final_acceptance")
    if acceptance.get("workflow") != "paper_tables" or acceptance.get("profile_version") != 4:
        raise ExperimentConfigurationError("v12 acceptance must identify paper_tables and runtime profile v4")
    acceptance["workflow"] = "directional_comparison"
    acceptance["profile_version"] = 3
    defaults = _require_mapping(execution.get("default"), "execution.default")
    batches = _require_mapping(defaults.get("batch_sizes"), "execution.default.batch_sizes")
    timeouts = _require_mapping(defaults.get("worker_timeouts_seconds"), "execution.default.worker_timeouts_seconds")
    threads = _require_mapping(defaults.get("thread_limits"), "execution.default.thread_limits")
    paths = _require_mapping(execution.get("paths"), "execution.paths")
    extra_batches = {"auto_arima", "r_forecast", "chronos", "combine", "gift_eval", "table_sensitivity"}
    for key in extra_batches:
        item = batches.pop(key, None)
        if isinstance(item, bool) or not isinstance(item, int) or item < 1:
            raise ExperimentConfigurationError(f"v12 execution batch {key} must be a positive integer")
    for key in ("chronos_startup", "chronos_request", "gift_eval"):
        item = timeouts.pop(key, None)
        if isinstance(item, bool) or not isinstance(item, (int, float)) or item <= 0:
            raise ExperimentConfigurationError(f"v12 timeout {key} must be positive")
    if threads.pop("chronos", None) != 1:
        raise ExperimentConfigurationError("v12 Chronos threads must be 1")
    for key in ("chronos_environment", "chronos_worker", "r_auto_arima_worker", "r_forecast_worker"):
        item = paths.pop(key, None)
        if not isinstance(item, str) or not item:
            raise ExperimentConfigurationError(f"v12 execution path {key} is required")
    validate_experiment_configuration(projected)

    # Validation-only ordinary projection reuses the immutable v9 contract.
    ordinary = deepcopy(value)
    ordinary["configuration_version"] = 9
    ordinary["data"]["selection"] = {"method": "first_official", "count": 100}
    ordinary["models"] = {key: models[key] for key in (*R_FORECAST_METHODS, "chronos_2")}
    for key in ("representations", "classifiers"):
        ordinary.pop(key)
    ordinary["evaluation"].pop("table_reproduction")
    ordinary["evaluation"]["method"] = "gift_eval"
    ordinary["evaluation"]["options"] = ordinary["evaluation"].pop("gift_eval_options")
    execution = ordinary["execution"]
    execution.pop("model_storage")
    for key in ("directional_calibration", "directional_prediction", "mantis_representation",
                "random_forest_classifier", "table_sensitivity"):
        execution["default"]["batch_sizes"].pop(key)
    for key in ("directional_dtw", "mantis", "classifier"):
        execution["default"]["worker_timeouts_seconds"].pop(key)
    execution["default"]["thread_limits"].pop("classifier")
    execution["paths"] = {key: path for key, path in execution["paths"].items()
        if key in {"project_environment", "chronos_environment", "chronos_worker",
                   "r_preprocess_worker", "r_auto_arima_worker", "r_forecast_worker", "r_m4comp2018_worker"}}
    acceptance = execution["final_acceptance"]
    acceptance.update(workflow="forecast_pool", profile_version=3,
                      workers={"mac_cpu": 8, "ubuntu_cpu": 15, "ubuntu_gpu": 15, "total": 38})
    acceptance.pop("mantis_gpu_processes")
    validate_experiment_configuration(ordinary)


def resolve_experiment_configuration(value: dict[str, Any]) -> ExperimentConfiguration:
    """Purpose: Validate a document and derive deterministic pipeline cardinalities.

    Inputs: Complete decoded researcher configuration.
    Outputs: Immutable configuration retaining the original and a resolved copy with
    expected task, forecast, evaluation, variant, and candidate counts.
    """
    validate_experiment_configuration(value)
    original = deepcopy(value)
    resolved = deepcopy(value)
    if resolved["configuration_version"] in {5, 6, 7, 8, 9, 10, 11, 12}:
        frequencies = resolved["pipeline"]["window_preparation"]["frequencies"]
        for settings in frequencies.values():
            settings["stride"] = settings["input_length"] + settings["future_horizon"]
    series_count = int(resolved["data"]["selection"]["count"])
    cleaning_count = len(resolved["pipeline"]["preprocessing"]["modes"])
    transformation_count = len(resolved["pipeline"]["transformations"]["methods"])
    variant_count = cleaning_count * transformation_count
    model_count = len(resolved["models"])
    candidate_count = model_count + 1
    directional = resolved["configuration_version"] in {10, 11}
    directional_models = 2 if resolved["configuration_version"] == 11 else 1
    expected_counts = (
        {
            "1": series_count,
            "2": series_count,
            "3": series_count,
            "4": (
                1 + series_count * 14 + 1 + 28
                if resolved["configuration_version"] == 11
                else 1 + series_count * 14
            ),
            "5": 1,
            "6": 14 * directional_models,
        }
        if directional
        else {
            "1": series_count,
            "2": series_count * cleaning_count,
            "3": series_count * variant_count,
            "4": series_count * variant_count * model_count,
            "5": series_count * variant_count * candidate_count,
            "6": variant_count * candidate_count,
        }
    )
    resolved["derived"] = {
        "variant_count": variant_count,
        "candidate_count": candidate_count,
        "expected_task_counts": expected_counts,
        "expected_forecast_rows": 0 if directional else series_count * variant_count * candidate_count,
        "expected_directional_prediction_rows": (
            series_count * 14 * directional_models if directional else 0
        ),
        "expected_evaluation_rows": (
            14 * directional_models if directional else variant_count * candidate_count
        ),
    }
    if resolved["configuration_version"] == 12:
        ordinary_model_count = len(set(resolved["models"]) & (set(R_FORECAST_METHODS) | {"chronos_2"}))
        ordinary_candidate_count = ordinary_model_count + 1
        gift_candidate_count = ordinary_candidate_count + len(resolved["archived_forecasts"]["enabled"])
        resolved["derived"].update({
            "variant_count": 1, "candidate_count": ordinary_candidate_count,
            "ordinary_model_count": ordinary_model_count,
            "directional_model_count": 2,
            "expected_directional_training_tasks": 15,
            "expected_directional_prediction_tasks": series_count * 14 + 14,
            "expected_gift_evaluation_tasks": gift_candidate_count,
            "expected_paper_table_tasks": 1,
            "expected_task_counts": {"1": series_count, "2": series_count,
                "3": series_count, "4": 30 + series_count * (14 + ordinary_model_count),
                "5": series_count * ordinary_candidate_count, "6": 29 + gift_candidate_count},
            "expected_forecast_rows": series_count * ordinary_candidate_count,
            "expected_directional_prediction_rows": series_count * 28,
            "expected_evaluation_rows": 28 + gift_candidate_count,
            "expected_table_candidate_rows": 1050,
        })
    return ExperimentConfiguration(original=original, resolved=resolved)


def load_experiment_configuration(path: Path) -> ExperimentConfiguration:
    """Purpose: Load and resolve a researcher-authored experiment document.

    Inputs: Path to a UTF-8 JSON object.
    Outputs: Validated ``ExperimentConfiguration``; wraps I/O and JSON errors as
    ``ExperimentConfigurationError``.
    """
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ExperimentConfigurationError(f"cannot read experiment configuration {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ExperimentConfigurationError("experiment configuration must be an object")
    return resolve_experiment_configuration(value)


def canonical_json(value: Any) -> str:
    """Serialize a JSON-compatible value with sorted keys and no optional whitespace."""
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def json_fingerprint(value: Any) -> str:
    """Return the hexadecimal SHA-256 digest of ``value``'s canonical JSON."""
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def validate_config(config: dict[str, Any]) -> None:
    """Purpose: Validate the reduced Stage 1 worker configuration.

    Inputs: Import mapping with schema, source, benchmark, and optional series limit.
    Outputs: ``None`` for the exact M4 Daily short-horizon contract; otherwise raises
    ``ImportValidationError``.
    """
    required = {"schema_version", "dataset_name", "source_system", "benchmark", "max_series"}
    missing = sorted(required - config.keys())
    if missing:
        raise ImportValidationError(f"configuration is missing: {', '.join(missing)}")
    if config["max_series"] is not None and (
        not isinstance(config["max_series"], int) or config["max_series"] <= 0
    ):
        raise ImportValidationError("max_series must be a positive integer")
    expected = {
        "frequency": "D",
        "term": "short",
        "prediction_length": 14,
        "evaluation_windows": 1,
        "boundary_convention": "zero-based, end-exclusive",
    }
    if config["benchmark"] != expected:
        raise ImportValidationError(f"M4 Daily benchmark must equal {expected!r}")


def canonical_import_configuration(config: dict[str, Any]) -> dict[str, Any]:
    """Return scientific import settings, excluding the non-identity ``max_series`` limit."""
    return {key: value for key, value in config.items() if key != "max_series"}


def dataset_identity(
    config: dict[str, Any], source_revision: str, source_files: dict[str, Any]
) -> tuple[str, str]:
    """Purpose: Derive stable identities from source and scientific import settings.

    Inputs: Stage 1 configuration, pinned source revision, and source-file provenance.
    Outputs: Content-derived dataset ID and canonical configuration SHA-256 digest.
    """
    canonical_config = canonical_import_configuration(config)
    config_hash = json_fingerprint(canonical_config)
    identity = {
        "logical_dataset": f"gift_eval/{config['dataset_name']}",
        "source_revision": source_revision,
        "source_files": source_files,
        "import_configuration_hash": config_hash,
    }
    return f"gift_eval/{config['dataset_name']}/{json_fingerprint(identity)[:24]}", config_hash


def evaluation_window(
    observation_count: int,
    config: dict[str, Any],
    *,
    window_id: str | None = None,
) -> dict[str, Any]:
    """Purpose: Define leakage-safe training, validation, and test boundaries.

    Inputs: Series observation count, benchmark horizon/convention, and optional ID.
    Outputs: Zero-based, end-exclusive slices reserving the final two horizons for
    validation and test; raises ``ImportValidationError`` when history is insufficient.
    Notes: Boundary offsets count observations, not elapsed time units.
    """
    horizon = config["benchmark"]["prediction_length"]
    if observation_count < horizon * 2:
        raise ImportValidationError(
            f"series has {observation_count} observations; at least {horizon * 2} are required"
        )
    validation_start = observation_count - 2 * horizon
    test_start = observation_count - horizon
    return {
        "window_id": window_id or f"{config['benchmark']['term']}/000",
        "split_name": "validation_and_test",
        "train_start": 0,
        "train_end": validation_start,
        "validation_start": validation_start,
        "validation_end": test_start,
        "test_start": test_start,
        "test_end": observation_count,
        "horizon": horizon,
        "boundary_convention": config["benchmark"]["boundary_convention"],
    }
