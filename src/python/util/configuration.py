# ==============================================================================
# configuration.py
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


# Code constant: configuration schema version implemented here; experiment JSON cannot override it.
SUPPORTED_CONFIGURATION_VERSION = 1
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
        return {
            "configuration_version": self.version,
            "reproducibility": self.resolved["reproducibility"],
            "data": data,
            "pipeline": self.resolved["pipeline"],
            "models": self.resolved["models"],
            "evaluation": {
                "method": evaluation["method"],
                "gift_eval": {
                    "code_revision": evaluation["gift_eval"]["code_revision"],
                },
                "options": evaluation["options"],
            },
        }

    @property
    def configuration_integrity_hash(self) -> str:
        """Return the digest of the complete resolved configuration document."""
        return json_fingerprint(self.resolved)

    @property
    def execution(self) -> dict[str, Any]:
        """Return the creation-time execution globals used by coordinators and workers."""
        return deepcopy(self.resolved["execution"]["default"])

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

    @property
    def evaluation_options(self) -> dict[str, Any]:
        """Purpose: Assemble evaluator science and execution options.

        Inputs: Stored GIFT-Eval options and centrally controlled evaluation batch size.
        Outputs: Independent evaluator-options mapping.
        """
        options = deepcopy(self.resolved["evaluation"]["options"])
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
    def import_settings(self) -> dict[str, Any]:
        """Purpose: Derive the reduced Stage 1 worker contract.

        Inputs: Authoritative data, benchmark, version, and selection settings.
        Outputs: Import configuration containing scientific fields and selected row limit.
        """
        data = self.resolved["data"]
        benchmark = data["benchmark"]
        return {
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
        }

    @property
    def workflow(self) -> dict[str, Any]:
        """Purpose: Derive the process-facing workflow from authoritative state.

        Inputs: Stored benchmark, pipeline, model, and evaluation configuration.
        Outputs: Independent mapping consumed by downstream process coordinators.
        """
        data = self.resolved["data"]
        pipeline = self.resolved["pipeline"]
        evaluation = self.resolved["evaluation"]
        return {
            "contract_version": f"configuration-v{self.version}",
            "benchmark": {
                "configuration": data["benchmark"]["configuration"],
                "dataset_name": data["dataset_name"],
                "term": data["benchmark"]["term"],
                "gift_eval_revision": evaluation["gift_eval"]["code_revision"],
            },
            "cleaning": list(pipeline["cleaning"]["methods"]),
            "transformations": list(pipeline["transformations"]["methods"]),
            "models": deepcopy(self.resolved["models"]),
            "adjustment": pipeline["adjustment"],
            "combination": deepcopy(pipeline["combination"]),
            "provisional_candidate": deepcopy(evaluation["provisional_candidate"]),
            "submission_metadata": deepcopy(evaluation["submission_metadata"]),
        }


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

    Inputs: Decoded configuration mapping claiming version 1.
    Outputs: ``None`` when every nested field and fixed protocol value is valid;
    otherwise raises ``ExperimentConfigurationError`` with field-level context.
    """
    _require_keys(
        value,
        {"configuration_version", "experiment", "reproducibility", "data", "pipeline", "models", "evaluation", "execution"},
        "configuration",
    )
    if value["configuration_version"] != SUPPORTED_CONFIGURATION_VERSION:
        raise ExperimentConfigurationError(
            f"configuration_version must be {SUPPORTED_CONFIGURATION_VERSION}"
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
        {"processes", "cleaning", "transformations", "adjustment", "combination"},
        "pipeline",
    )
    expected_processes = [
        {"id": process_id, "name": name} for process_id, name in PROCESS_NAMES.items()
    ]
    if pipeline["processes"] != expected_processes:
        raise ExperimentConfigurationError("pipeline.processes must define ordered Processes 01-06")
    if pipeline["cleaning"] != {"methods": ["identity", "tsclean"]}:
        raise ExperimentConfigurationError("unsupported cleaning configuration")
    if pipeline["transformations"] != {
        "methods": ["identity", "minmax_then_standardize"]
    }:
        raise ExperimentConfigurationError("unsupported transformation configuration")
    if pipeline["adjustment"] != "identity":
        raise ExperimentConfigurationError("unsupported adjustment method")
    if pipeline["combination"] != {
        "method": "equal_weight",
        "weights": {"auto_arima": 0.5, "chronos_2": 0.5},
    }:
        raise ExperimentConfigurationError("unsupported forecast combination")

    models = _require_mapping(value["models"], "models")
    if set(models) != {"auto_arima", "chronos_2"}:
        raise ExperimentConfigurationError("models must contain AutoARIMA and Chronos-2")
    auto = _require_mapping(models["auto_arima"], "models.auto_arima")
    _require_keys(auto, {"package", "settings"}, "models.auto_arima")
    required_auto = {
        "stepwise": True,
        "approximation": False,
        "allowdrift": True,
        "allowmean": True,
        "interval_levels": [20, 40, 60, 80],
    }
    if auto["package"] != "forecast" or auto["settings"] != required_auto:
        raise ExperimentConfigurationError("unsupported AutoARIMA settings")
    chronos = _require_mapping(models["chronos_2"], "models.chronos_2")
    _require_keys(
        chronos,
        {"repository", "revision", "chronos_forecasting", "dtype", "quantile_levels", "predict_batches_jointly", "cross_learning"},
        "models.chronos_2",
    )
    quantiles = chronos["quantile_levels"]
    if quantiles != [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]:
        raise ExperimentConfigurationError("Chronos quantile levels must equal 0.1 through 0.9")
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

    evaluation = _require_mapping(value["evaluation"], "evaluation")
    _require_keys(
        evaluation,
        {"method", "gift_eval", "options", "provisional_candidate", "submission_metadata"},
        "evaluation",
    )
    if evaluation["method"] != "gift_eval":
        raise ExperimentConfigurationError("evaluation.method must be gift_eval")
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
        "axis": None,
        "mask_invalid_label": True,
        "allow_nan_forecast": False,
        "seasonality": "official get_seasonality(freq)",
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
    expected_batch_sizes = {
        "import", "plan", "preprocess", "transform", "auto_arima", "chronos",
        "combine", "gift_eval",
    }
    batch_sizes = _require_mapping(default["batch_sizes"], "execution.default.batch_sizes")
    if set(batch_sizes) != expected_batch_sizes or any(
        isinstance(item, bool) or not isinstance(item, int) or item < 1
        for item in batch_sizes.values()
    ):
        raise ExperimentConfigurationError(
            "execution.default.batch_sizes must define positive integer process batches"
        )
    expected_timeouts = {"r", "chronos_startup", "chronos_request", "gift_eval"}
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
    expected_threads = {"r", "chronos", "dask_worker"}
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
    paths = _require_mapping(execution["paths"], "execution.paths")
    required_paths = {
        "project_environment",
        "chronos_environment",
        "chronos_worker",
        "r_preprocess_worker",
        "r_auto_arima_worker",
    }
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


def resolve_experiment_configuration(value: dict[str, Any]) -> ExperimentConfiguration:
    """Purpose: Validate a document and derive deterministic pipeline cardinalities.

    Inputs: Complete decoded researcher configuration.
    Outputs: Immutable configuration retaining the original and a resolved copy with
    expected task, forecast, evaluation, variant, and candidate counts.
    """
    validate_experiment_configuration(value)
    original = deepcopy(value)
    resolved = deepcopy(value)
    series_count = int(resolved["data"]["selection"]["count"])
    cleaning_count = len(resolved["pipeline"]["cleaning"]["methods"])
    transformation_count = len(resolved["pipeline"]["transformations"]["methods"])
    variant_count = cleaning_count * transformation_count
    model_count = len(resolved["models"])
    candidate_count = model_count + 1
    resolved["derived"] = {
        "variant_count": variant_count,
        "candidate_count": candidate_count,
        "expected_task_counts": {
            "1": series_count,
            "2": series_count * cleaning_count,
            "3": series_count * variant_count,
            "4": series_count * variant_count * model_count,
            "5": series_count * variant_count * candidate_count,
            "6": variant_count * candidate_count,
        },
        "expected_forecast_rows": series_count * variant_count * candidate_count,
        "expected_evaluation_rows": variant_count * candidate_count,
    }
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
