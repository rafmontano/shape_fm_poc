# ==============================================================================
# shared_experiment_execution.py
#
# Purpose: Plan M4 Daily tasks, execute preprocessing through official evaluation, and export a candidate.
# Inputs: Imported M4 series, experiment configuration, execution profile/settings, and process selection.
# Outputs: Restartable task/invocation rows, forecasts, official evaluations, status, and candidate exports.
# Run from: Imported; not run directly.
# ==============================================================================

"""Plan and execute the restartable M4 Daily experiment in a single-writer DuckDB."""

from __future__ import annotations

import json
import hashlib
import math
import os
import platform
import shlex
import subprocess
import time
import uuid
import csv
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable

import duckdb

from .shared_configuration import (
    R_FORECAST_METHODS, R_MODEL_METHODS, canonical_json, json_fingerprint,
)
from .shared_database import DEFAULT_DATABASE, load_database_configuration, migrate_database
from .shared_execution_profiles import (
    GIB,
    ExecutionProfile,
    ExecutionSettings,
    PersistentChronosWorker,
    system_hardware,
    validate_heavy_tuning_execution,
    validate_system_memory,
)
from .p05_01_forecast_combination import combine_equal_weight, combine_m4_point
from .p01_02_import_execution import repository_root
from .shared_transformations import TransformationResult, inverse
from .shared_provenance import utc_now


# Code constant: process-number to legacy persistent invocation-stage protocol mapping;
# existing ``stage`` fields remain unchanged for database and JSON compatibility.
PROCESSES = {2: "preprocess", 3: "transform", 4: "forecast", 5: "combine", 6: "evaluate"}

# Code constant: complete scientific columns compared by the directional
# insert-or-verify boundary. ``created_at`` is deliberately operational only.
DIRECTIONAL_RECORD_COLUMNS = {
    "directional_evaluation_inputs": (
        "evaluation_input_id", "experiment_id", "variant_id", "forecast_instance_id",
        "preparation_id", "preparation_fingerprint", "raw_input_hash",
        "cleaned_input_hash", "transformed_input_hash", "transformed_input",
        "label_reference", "preprocessing_provenance", "package_versions", "content_hash",
    ),
    "directional_actual_labels": (
        "actual_label_id", "experiment_id", "evaluation_input_id", "definition_id",
        "labels", "content_hash",
    ),
    "directional_model_definitions": (
        "model_definition_id", "experiment_id", "variant_id", "scientific_definition",
        "repository_revision", "scientific_source_fingerprint", "worker_file_fingerprint",
        "classifier_lock_fingerprint", "runtime_versions", "numeric_dtype",
        "reference_library_fingerprint", "preparation_fingerprint",
        "implementation_fingerprint", "content_hash",
    ),
    "directional_calibration_scores": (
        "calibration_score_id", "model_definition_id", "effective_width",
        "representative_proportion", "horizon", "correct_count", "evaluation_count",
        "accuracy", "candidate_policy", "content_hash",
    ),
    "directional_selected_widths": (
        "selected_width_id", "model_definition_id", "horizon", "effective_width",
        "representative_proportion", "calibration_accuracy", "tie_rule", "content_hash",
    ),
    "directional_predictions": (
        "prediction_id", "experiment_id", "model_definition_id", "evaluation_input_id",
        "horizon", "prediction", "nearest_reference_identity", "nearest_distance",
        "effective_width", "execution_metadata", "content_hash",
    ),
    "directional_evaluations": (
        "directional_evaluation_id", "experiment_id", "model_definition_id", "horizon",
        "correct_count", "evaluation_count", "accuracy", "prediction_fingerprint",
        "content_hash",
    ),
    "deterministic_no_work": (
        "no_work_id", "experiment_id", "process_id", "reason", "content_hash",
    ),
    "directional_representation_definitions": (
        "representation_definition_id", "experiment_id", "scientific_definition",
        "content_hash",
    ),
    "directional_representations": (
        "representation_id", "representation_definition_id", "input_id",
        "source_series_id", "role", "input_fingerprint",
        "preparation_definition_id", "preparation_fingerprint",
        "membership_fingerprint", "representation_values", "representation_dtype",
        "representation_dimension", "representation_fingerprint", "content_hash",
    ),
    "directional_representation_executions": (
        "representation_execution_id", "representation_definition_id",
        "worker_provenance", "worker_provenance_fingerprint",
    ),
    "directional_representation_execution_members": (
        "representation_id", "representation_execution_id",
    ),
    "directional_classifier_definitions": (
        "classifier_definition_id", "experiment_id", "scientific_definition",
        "content_hash",
    ),
    "directional_classifier_runs": (
        "classifier_run_id", "classifier_definition_id", "classification_dataset_id",
        "horizon", "training_fingerprint", "evaluation_fingerprint",
        "output_fingerprint", "content_hash",
    ),
    "directional_classifier_executions": (
        "classifier_execution_id", "classifier_run_id", "classification_response_id",
        "worker_provenance", "worker_provenance_fingerprint",
    ),
    "directional_composite_model_definitions": (
        "model_definition_id", "experiment_id", "variant_id",
        "representation_definition_id", "classifier_definition_id",
        "scientific_definition", "preparation_fingerprint",
        "membership_fingerprint", "content_hash",
    ),
    "model_directional_predictions": (
        "prediction_id", "experiment_id", "model_definition_id",
        "evaluation_input_id", "horizon", "prediction", "classifier_run_id",
        "training_fingerprint", "evaluation_fingerprint", "output_fingerprint",
        "content_hash",
    ),
    "directional_dtw_prediction_lineage": (
        "prediction_id", "nearest_reference_identity", "nearest_distance",
        "effective_width", "content_hash",
    ),
    "directional_classifier_prediction_lineage": (
        "prediction_id", "representation_id", "classifier_run_id", "content_hash",
    ),
    "model_directional_evaluations": (
        "directional_evaluation_id", "experiment_id", "model_definition_id",
        "horizon", "correct_count", "evaluation_count", "accuracy",
        "prediction_fingerprint", "content_hash",
    ),
}


def expected_task_counts(
    instance_count: int, workflow: dict[str, Any]
) -> dict[int, int]:
    """Purpose: Derive Process 02–06 task totals for an experiment plan.

    Inputs: Selected forecast-instance count and the stored workflow mapping of
    cleaning methods, transformations, and models.
    Outputs: Process-number to task-count mapping; no database or process effects.
    """
    if instance_count < 0:
        raise ValueError("instance count cannot be negative")
    if workflow.get("table_reproduction"):
        ordinary = sum(not model.startswith("directional_") for model in workflow["models"])
        candidates = ordinary + (workflow["combination"]["method"] != "none")
        variants = len(workflow["cleaning"]) * len(workflow["transformations"])
        return {2: instance_count * len(workflow["cleaning"]),
                3: instance_count * variants,
                4: 30 + instance_count * (14 + variants * ordinary),
                5: instance_count * variants * candidates,
                6: 29 + variants * candidates + 2}
    if set(workflow["models"]) == {"directional_dtw"}:
        horizons = workflow["models"]["directional_dtw"]["labels"]["horizons"]
        return {
            2: instance_count,
            3: instance_count,
            4: 1 + instance_count * len(horizons),
            5: 1,
            6: len(horizons),
        }
    if set(workflow["models"]) == {"directional_dtw", "directional_mantis_rf"}:
        horizons = workflow["models"]["directional_dtw"]["labels"]["horizons"]
        if workflow["models"]["directional_mantis_rf"]["labels"]["horizons"] != horizons:
            raise ValueError("directional providers must use identical horizons")
        return {
            2: instance_count,
            3: instance_count,
            4: 1 + instance_count * len(horizons) + 1 + 2 * len(horizons),
            5: 1,
            6: 2 * len(horizons),
        }
    cleaning_count = len(workflow["cleaning"])
    variant_count = cleaning_count * len(workflow["transformations"])
    model_count = len(workflow["models"])
    candidate_count = model_count + 1
    return {
        2: instance_count * cleaning_count,
        3: instance_count * variant_count,
        4: instance_count * variant_count * model_count,
        5: instance_count * variant_count * candidate_count,
        6: variant_count * candidate_count,
    }


def scientific_configuration(config: dict[str, Any]) -> dict[str, Any]:
    """Return identity-defining experiment settings, excluding export-only metadata."""
    return {
        key: value
        for key, value in config.items()
        if key not in {"provisional_candidate", "submission_metadata"}
    }


def validated_submission_metadata(config: dict[str, Any]) -> dict[str, Any]:
    """Purpose: Validate GIFT-Eval submission metadata before candidate export.

    Inputs: Workflow configuration containing creation-time ``submission_metadata``.
    Outputs: Key-sorted metadata mapping, or ``ValueError`` for an invalid draft
    or approval; no database or filesystem effects.
    """
    metadata = config.get("submission_metadata", {})
    required = {
        "status",
        "submission_approved",
        "model_name",
        "model_type",
        "model_dtype",
        "model_link",
        "code_link",
        "org",
        "testdata_leakage",
        "replication_code_available",
    }
    missing = sorted(required - metadata.keys())
    if missing:
        raise ValueError(f"missing submission metadata: {', '.join(missing)}")
    if metadata["status"] not in {"draft", "approved"}:
        raise ValueError("submission status must be draft or approved")
    if not isinstance(metadata["submission_approved"], bool):
        raise ValueError("submission_approved must be boolean")
    if not metadata["submission_approved"]:
        if metadata["status"] != "draft":
            raise ValueError("an unapproved submission must remain draft")
        if metadata["replication_code_available"] != "No":
            raise ValueError("draft private-repository metadata cannot claim replication code")
        return {key: metadata[key] for key in sorted(required)}
    if metadata["status"] != "approved":
        raise ValueError("approved submission metadata must have approved status")
    allowed_model_types = {
        "statistical",
        "deep-learning",
        "agentic",
        "pretrained",
        "fine-tuned",
        "zero-shot",
    }
    if metadata["model_type"] not in allowed_model_types:
        raise ValueError("invalid submission model_type")
    for field in ("testdata_leakage", "replication_code_available"):
        if metadata[field] not in {"Yes", "No"}:
            raise ValueError(f"submission {field} must be Yes or No")
    if metadata["replication_code_available"] != "Yes":
        raise ValueError("an approved submission requires public replication code")
    for field in ("model_link", "code_link"):
        if not isinstance(metadata[field], str) or not metadata[field].startswith("https://"):
            raise ValueError(f"submission {field} must be a public HTTPS URL")
    for field in ("model_name", "model_type", "model_dtype", "org"):
        if not isinstance(metadata[field], str) or not metadata[field].strip():
            raise ValueError(f"submission {field} must be non-empty")
    return {key: metadata[key] for key in sorted(required)}


@dataclass(frozen=True)
class ExperimentPlan:
    """Purpose: Describe a persisted, executable M4 Daily experiment plan.

    Inputs: Constructed from authoritative configuration and the official benchmark
    selection by :meth:`ExperimentCoordinator.plan`.
    Outputs: Immutable experiment ID, scope, instance and variant counts, per-process
    task counts, and benchmark configuration name; owns no external resources.
    """
    experiment_id: str
    scope: str
    instance_count: int
    variant_count: int
    task_counts: dict[int, int]
    benchmark_configuration: str


@dataclass(frozen=True)
class ExperimentForecast:
    """Purpose: Represent one persisted candidate forecast and held-out target.

    Inputs: Constructed by :func:`get_forecast` from forecast and instance rows.
    Outputs: Immutable IDs plus horizon-length mean and actual vectors. Median and
    quantiles are complete for probabilistic forecasts and absent for mean-only ones.
    """
    forecast_id: str
    experiment_id: str
    variant_id: str
    forecast_instance_id: str
    candidate: str
    mean: tuple[float, ...]
    median: tuple[float, ...] | None
    quantile_levels: tuple[float, ...] | None
    quantiles: tuple[tuple[float, ...], ...] | None
    forecast_capability: str
    actual: tuple[float, ...]


def _batches(values: list[Any], batch_size: int) -> list[list[Any]]:
    """Split values in input order into positive, fixed-size chunks."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    return [values[offset : offset + batch_size] for offset in range(0, len(values), batch_size)]


def _length_aware_batches(
    jobs: list[dict[str, Any]], batch_size: int
) -> list[list[dict[str, Any]]]:
    """Batch by power-of-two context ranges, then by the configured bound."""
    grouped: dict[int, list[dict[str, Any]]] = {}
    for job in sorted(jobs, key=lambda value: (len(value["context"]), value["task_id"])):
        length_bucket = max(1, len(job["context"])).bit_length()
        grouped.setdefault(length_bucket, []).append(job)
    return [
        batch
        for bucket in sorted(grouped)
        for batch in _batches(grouped[bucket], batch_size)
    ]


def _combine_job(job: dict[str, Any]) -> dict[str, Any]:
    """Apply one configured combination recipe without storage access."""
    return (
        combine_m4_point(job["components"], job["weights"])
        if job.get("method") == "m4_comb"
        else combine_equal_weight(job["components"], job["weights"])
    )


class ExperimentCoordinator:
    """Purpose: Own planning and restartable M4 Daily process coordination.

    Inputs: A DuckDB path whose stored configuration defines scientific workflow,
    execution controls, benchmark selection, model revisions, and quantile levels.
    Outputs: Plans, process summaries, forecasts, evaluations, status, and exports;
    owns the sole writable connection and all task/invocation transaction effects,
    and launches isolated R, Chronos, GIFT-Eval, and optional Dask work.
    """

    def __init__(self, database_path: Path = DEFAULT_DATABASE):
        """Purpose: Initialize the coordinator's authoritative state and writer.

        Inputs: Existing experiment DuckDB path, defaulting to the project database.
        Outputs: Open coordinator with migrated schema, stored configuration,
        quantile levels, and hardware cache; migrates and opens DuckDB for writes.
        """
        self.root = repository_root()
        if not Path(database_path).resolve().is_file():
            raise FileNotFoundError(
                f"experiment database does not exist: {Path(database_path).resolve()}"
            )
        self.database_path = migrate_database(database_path)
        self.connection = duckdb.connect(str(self.database_path))
        self.configuration = load_database_configuration(
            self.database_path, self.connection
        )
        self.config = self.configuration.workflow
        self.quantiles = tuple(
            self.configuration.resolved["models"].get("chronos_2", {}).get(
                "quantile_levels", [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
            )
        )
        self._hardware_cache: dict[str, dict[str, Any]] = {}
        self._active_machine_environment = None
        self._active_dask_client = None
        self.table_sensitivity_batch_size = self.configuration.execution["batch_sizes"].get("table_sensitivity")

    def close(self) -> None:
        """Close the coordinator's writable DuckDB connection."""
        self.connection.close()

    def __enter__(self) -> "ExperimentCoordinator":
        """Return the open single-writer coordinator."""
        return self

    def __exit__(self, *_: object) -> None:
        """Close the coordinator's DuckDB connection on context exit."""
        self.close()

    def _gift_bridge(self, *arguments: str, timeout: float = 300.0) -> dict[str, Any]:
        """Purpose: Invoke the pinned GIFT-Eval environment through its JSON bridge.

        Inputs: Bridge CLI arguments and subprocess timeout; environment and script
        paths originate in the stored resolved configuration.
        Outputs: Decoded response mapping; launches a checked subprocess and may
        raise timeout, process, or JSON errors without directly writing DuckDB.
        """
        environment = self.configuration.resolved["evaluation"]["gift_eval"]["environment"]
        command = [
            str(self.root / environment / "bin/python"),
            str(self.root / "src/python/06_01_evaluate_gift_eval.py"),
            *arguments,
        ]
        completed = subprocess.run(
            command,
            cwd=self.root,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return json.loads(completed.stdout)

    def _dataset_id(self) -> str:
        """Purpose: Resolve the imported dataset used to plan an experiment.

        Inputs: Dataset name from authoritative stored configuration.
        Outputs: Newest matching M4 Daily dataset ID read from DuckDB; writes nothing
        and raises ``RuntimeError`` when Process 01 has not completed.
        """
        row = self.connection.execute(
            "SELECT dataset_id FROM datasets WHERE dataset_name = ? "
            "ORDER BY created_at DESC LIMIT 1",
            [self.configuration.resolved["data"]["dataset_name"]],
        ).fetchone()
        if row is None:
            raise RuntimeError("Foundation Process 01 M4 Daily import is required")
        return row[0]

    def _validate_official_configuration(self, official: dict[str, Any]) -> None:
        """Purpose: Guard the POC against an incompatible official benchmark shape.

        Inputs: GIFT-Eval description mapping and configured benchmark identity.
        Outputs: None for the expected configuration with one forecast window;
        raises ``RuntimeError`` otherwise and has no side effects.
        """
        expected_configuration = self.config["benchmark"]["configuration"]
        if official["configuration_name"] != expected_configuration:
            raise RuntimeError(
                f"official configuration {official['configuration_name']} does not match "
                f"configured {expected_configuration}"
            )
        if official["window_count"] != 1:
            raise RuntimeError(
                "The experiment supports exactly one official forecast window; "
                f"{official['configuration_name']} has {official['window_count']}"
            )
        benchmark = self.configuration.resolved["data"]["benchmark"]
        if official["frequency"] != benchmark["frequency"]:
            raise RuntimeError("official GIFT-Eval frequency does not match configuration")
        expected_override = self.configuration.r_period_override
        if expected_override is not None and official["r_period"] != expected_override:
            raise RuntimeError("resolved R period does not match the experiment override")
        if (
            official["evaluation_seasonality"]
            != official["gluonts_default_seasonality"]
        ):
            raise RuntimeError("pinned evaluation seasonality is internally inconsistent")

    def _gift_description_arguments(self, limit: int) -> list[str]:
        """Build one version-aware GIFT-Eval description request.

        Purpose: Resolve the R period once per benchmark description in the pinned
        environment while preserving v1's historical period as an explicit override.
        Inputs: Positive number of official instances to materialize.
        Outputs: Bridge CLI arguments; performs no subprocess or database work.
        """
        benchmark = self.configuration.resolved["data"]["benchmark"]
        arguments = [
            "describe",
            "--source-root",
            str(self.root / self.configuration.source_directory),
            "--dataset-name",
            self.configuration.resolved["data"]["dataset_name"],
            "--term",
            benchmark["term"],
            "--domain",
            benchmark["domain"],
            "--num-variates",
            str(benchmark["num_variates"]),
            "--limit",
            str(limit),
        ]
        if self.configuration.r_period_override is not None:
            arguments.extend(
                ["--r-period-override", str(self.configuration.r_period_override)]
            )
        return arguments

    def _configured_execution(self) -> tuple[ExecutionProfile, ExecutionSettings]:
        """Purpose: Materialize execution controls from authoritative stored settings.

        Inputs: Process workers, batch sizes, memory bounds, and Dask controls in
        the database-backed execution configuration.
        Outputs: Execution profile and settings objects; no external side effects.
        """
        values = self.configuration.execution
        workers = values["process_workers"]
        profile = ExecutionProfile(
            name="stored_default",
            required_accelerator=None,
            expected_accelerator_name=None,
            cleaning_workers=int(workers["2"]),
            transformation_workers=int(workers["3"]),
            autoarima_workers=int(workers["4"]),
            chronos_processes=1,
            chronos_inference_batch_size=int(
                values["batch_sizes"].get(
                    "chronos", values["batch_sizes"].get("directional_prediction", 1)
                )
            ),
            combination_workers=int(workers["5"]),
            evaluation_workers=int(workers["6"]),
            cpu_gpu_overlap=bool(values["cpu_gpu_overlap"]),
            system_memory_min_available_gib=float(values["system_memory_min_available_gib"]),
            accelerator_memory_min_available_gib=float(values["accelerator_memory_min_available_gib"]),
            database_writers=int(values["database_writers"]),
            dask_max_in_flight=int(values["dask_max_in_flight"]),
        )
        settings = ExecutionSettings(
            mode=values["mode"],
            dask_timeout_seconds=float(values["dask_timeout_seconds"]),
            dask_max_in_flight=int(values["dask_max_in_flight"]),
            dask_retries=int(values["dask_retries"]),
        )
        return profile, settings

    def plan(self, dry_run: bool = False) -> ExperimentPlan | dict[str, Any]:
        """Purpose: Select official M4 instances and build the deterministic task graph.

        Inputs: ``dry_run`` plus benchmark, series count, workflow dimensions, and
        execution batch size from authoritative stored configuration.
        Outputs: Dry-run selection/count mapping or persisted :class:`ExperimentPlan`;
        calls GIFT-Eval and, unless dry-running, transactionally inserts benchmark,
        experiment, variant, instance, and Process 02–06 task rows and invalidates stale
        evaluation/export state when scope expands.
        """
        availability = self._gift_bridge(*self._gift_description_arguments(1))
        self._validate_official_configuration(availability)
        available_instances = int(availability["available_instances"])
        limit = self.configuration.series_count
        expected_total = self.configuration.resolved["data"]["selection"].get("expected_source_total")
        if expected_total is not None and available_instances != expected_total:
            raise ValueError(f"full Daily scope requires {expected_total} official instances, found {available_instances}")
        if limit > available_instances:
            raise ValueError(
                f"configured series count {limit} exceeds {available_instances} available M4 Daily instances"
            )
        requested_scope = f"first_official:{limit}"
        if not dry_run:
            dataset_id = self._dataset_id()
            benchmark_identity = {
                "revision": self.config["benchmark"]["gift_eval_revision"],
                "configuration": availability["configuration_name"],
            }
            if self.configuration.version >= 2:
                benchmark_identity["period_policy"] = {
                    "r_period": availability["r_period"],
                    "r_period_source": availability["r_period_source"],
                    "evaluation_seasonality": self.configuration.evaluation_seasonality(
                        availability["evaluation_seasonality"]
                    ),
                }
            benchmark_id = f"benchmark/{json_fingerprint(benchmark_identity)[:24]}"
            scientific = self.configuration.scientific_configuration
            configuration_hash = self.configuration.scientific_hash
            experiment_id = f"experiment/{json_fingerprint({'benchmark': benchmark_id, 'dataset': dataset_id, 'configuration': configuration_hash})[:24]}"
            existing_counts = {
                int(process): int(count)
                for process, count in self.connection.execute(
                    """SELECT stage, count(*) FROM experiment_tasks
                    WHERE experiment_id=? GROUP BY stage""",
                    [experiment_id],
                ).fetchall()
            }
            expected_counts = expected_task_counts(limit, self.config)
            existing_instances = existing_counts.get(2, 0) // len(
                self.config["cleaning"]
            )
            existing_scope_row = self.connection.execute(
                "SELECT scope FROM experiments WHERE experiment_id=?", [experiment_id]
            ).fetchone()
            plan_scope = (
                existing_scope_row[0]
                if existing_instances > limit and existing_scope_row is not None
                else requested_scope
            )
            if existing_counts == expected_counts:
                return ExperimentPlan(
                    experiment_id,
                    plan_scope,
                    limit,
                    len(self.config["cleaning"])
                    * len(self.config["transformations"]),
                    existing_counts,
                    availability["configuration_name"],
                )
        official = self._gift_bridge(*self._gift_description_arguments(limit))
        self._validate_official_configuration(official)
        instances = official["instances"]
        series_ids = [str(item["item_id"]) for item in instances]
        positions = [int(item["official_position"]) for item in instances]
        if (
            len(instances) != limit
            or len(set(series_ids)) != limit
            or positions != list(range(limit))
        ):
            raise RuntimeError(
                f"requested {limit} official M4 Daily series but selected "
                f"{len(instances)} instances and {len(set(series_ids))} distinct series"
            )
        if dry_run:
            task_counts = expected_task_counts(len(instances), self.config)
            directional = self.configuration.version in {10, 11, 12}
            directional_models = 2 if self.configuration.version >= 11 else 1
            return {
                "scope": requested_scope,
                "mode": "dry-run",
                "selection": self.configuration.resolved["data"]["selection"],
                "series_count": len(set(series_ids)),
                "forecast_instances": len(instances),
                "candidate_forecast_rows": (task_counts[5] if self.configuration.version == 12
                                            else 0 if directional else task_counts[5]),
                "directional_calibration_identities": 1 if directional else 0,
                "mantis_representation_identities": (
                    1 if self.configuration.version >= 11 else 0
                ),
                "classifier_horizon_identities": (
                    14 if self.configuration.version >= 11 else 0
                ),
                "directional_prediction_identities": (
                    len(instances) * 14 * directional_models if directional else 0
                ),
                "process_05_no_work_identities": 1 if self.configuration.version in {10, 11} else 0,
                "paper_table_identities": 1 if self.configuration.version == 12 else 0,
                "directional_evaluation_identities": (
                    14 * directional_models if directional else 0
                ),
                "official_evaluation_rows": (task_counts[6] - 29 if self.configuration.version == 12
                                             else 0 if directional else task_counts[6]),
                "benchmark_configuration": official["configuration_name"],
                "task_counts": {
                    str(process): count
                    for process, count in task_counts.items()
                },
                "resource_note": "planning only; no experiment rows were materialised",
            }
        expanding_scope = False
        self.connection.execute("BEGIN TRANSACTION")
        try:
            self.connection.execute(
                """INSERT INTO benchmark_configurations VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                ON CONFLICT (benchmark_configuration_id) DO UPDATE SET metadata=excluded.metadata""",
                [
                    benchmark_id,
                    self.config["benchmark"]["gift_eval_revision"],
                    official["configuration_name"],
                    official["dataset_name"],
                    official["frequency"],
                    official["term"],
                    official["prediction_length"],
                    official["window_count"],
                    official["domain"],
                    official["num_variates"],
                    canonical_json(
                        {
                            "official_available_instances": official["available_instances"],
                            # Compatibility alias retained for old readers/schema.
                            "official_seasonality": official["r_period"],
                            "r_period": official["r_period"],
                            "r_period_source": official["r_period_source"],
                            "evaluation_seasonality": self.configuration.evaluation_seasonality(
                                official["evaluation_seasonality"]
                            ),
                            "gluonts_default_seasonality": official[
                                "gluonts_default_seasonality"
                            ],
                        }
                    ),
                ],
            )
            self.connection.execute(
                """INSERT INTO experiments (
                    experiment_id, benchmark_configuration_id, dataset_id, name,
                    scientific_configuration, configuration_hash, scope, status,
                    provisional_candidate, configuration_version, experiment_date,
                    description
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'planned', ?, ?, CAST(? AS DATE), ?)
                ON CONFLICT (experiment_id) DO UPDATE SET
                    scope = excluded.scope,
                    updated_at = now()""",
                [
                    experiment_id,
                    benchmark_id,
                    dataset_id,
                    self.configuration.name,
                    canonical_json(scientific),
                    configuration_hash,
                    plan_scope,
                    canonical_json(self.config["provisional_candidate"]),
                    self.configuration.version,
                    self.configuration.date,
                    self.configuration.description,
                ],
            )
            variants = []
            for cleaning in self.config["cleaning"]:
                for transformation in self.config["transformations"]:
                    identity = {
                        "experiment_id": experiment_id,
                        "cleaning": cleaning,
                        "transformation": transformation,
                        "adjustment": self.config["adjustment"],
                    }
                    variant_id = f"variant/{json_fingerprint(identity)[:24]}"
                    variants.append((variant_id, cleaning, transformation))
                    self.connection.execute(
                        """INSERT INTO experiment_variants VALUES
                        (?, ?, ?, ?, ?, ?, current_timestamp)
                        ON CONFLICT (variant_id) DO NOTHING""",
                        [
                            variant_id,
                            experiment_id,
                            cleaning,
                            transformation,
                            self.config["adjustment"],
                            canonical_json(identity),
                        ],
                    )
            self.connection.execute("COMMIT")
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise
        plan_batch_size = int(self.configuration.execution["batch_sizes"]["plan"])
        for instance_batch in _batches(official["instances"], plan_batch_size):
            self.connection.execute("BEGIN TRANSACTION")
            try:
                task_rows = []
                for item in instance_batch:
                    instance_identity = {
                        "benchmark": benchmark_id,
                        "series": item["item_id"],
                        "variate": item["variate_id"],
                        "window": item["window_id"],
                    }
                    instance_id = f"instance/{json_fingerprint(instance_identity)[:32]}"
                    context_end = len(item["context"])
                    actual_end = context_end + len(item["actual"])
                    self.connection.execute(
                        """INSERT INTO forecast_instances VALUES
                        (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                        ON CONFLICT (forecast_instance_id) DO NOTHING""",
                        [
                            instance_id,
                            benchmark_id,
                            dataset_id,
                            item["item_id"],
                            item["variate_id"],
                            item["window_id"],
                            item["official_position"],
                            context_end,
                            context_end,
                            actual_end,
                            len(item["actual"]),
                            item["context"],
                            item["actual"],
                            canonical_json(
                                {
                                    "start": item["start"],
                                    "forecast_start": item["forecast_start"],
                                }
                            ),
                        ],
                    )
                    for cleaning in self.config["cleaning"]:
                        task_rows.append(
                            self._task_row(
                                experiment_id, 2, instance_id, None, cleaning
                            )
                        )
                    for variant_id, _, _ in variants:
                        task_rows.append(
                            self._task_row(
                                experiment_id, 3, instance_id, variant_id, None
                            )
                        )
                        if self.configuration.version in {10, 11, 12}:
                            for horizon in range(1, 15):
                                task_rows.append(
                                    self._task_row(
                                        experiment_id,
                                        4,
                                        instance_id,
                                        variant_id,
                                        (
                                            f"directional_dtw:predict:h{horizon:02d}"
                                            if self.configuration.version >= 11
                                            else f"directional_dtw:h{horizon:02d}"
                                        ),
                                    )
                                )
                            if self.configuration.version != 12:
                                continue
                        for model in self.config["models"]:
                            if model.startswith("directional_"):
                                continue
                            task_rows.append(
                                self._task_row(
                                    experiment_id,
                                    4,
                                    instance_id,
                                    variant_id,
                                    model,
                                )
                            )
                        for candidate in (
                            *self.config["models"].keys(),
                            self.config["combination"]["method"],
                        ):
                            if candidate.startswith("directional_") or candidate == "none":
                                continue
                            task_rows.append(
                                self._task_row(
                                    experiment_id,
                                    5,
                                    instance_id,
                                    variant_id,
                                    candidate,
                                )
                            )
                self.connection.executemany(
                    """INSERT INTO experiment_tasks
                    (task_id, experiment_id, stage, forecast_instance_id,
                     variant_id, candidate, status)
                    VALUES (?, ?, ?, ?, ?, ?, 'pending')
                    ON CONFLICT (task_id) DO NOTHING""",
                    task_rows,
                )
                self.connection.execute("COMMIT")
            except BaseException:
                self.connection.execute("ROLLBACK")
                raise
        self.connection.execute("BEGIN TRANSACTION")
        try:
            evaluation_tasks = []
            if self.configuration.version in {10, 11, 12}:
                variant_id = variants[0][0]
                evaluation_tasks.extend(
                    [
                        self._task_row(
                            experiment_id,
                            4,
                            None,
                            variant_id,
                            (
                                "directional_dtw:train"
                                if self.configuration.version >= 11
                                else "directional_dtw:calibration"
                            ),
                        ),
                        self._task_row(
                            experiment_id,
                            5,
                            None,
                            variant_id,
                            (
                                "directional:no_work"
                                if self.configuration.version == 11
                                else "directional_dtw:no_work"
                            ),
                        ),
                    ]
                )
                evaluation_tasks.extend(
                    self._task_row(
                        experiment_id,
                        6,
                        None,
                        variant_id,
                        f"directional_dtw:h{horizon:02d}",
                    )
                    for horizon in range(1, 15)
                )
                if self.configuration.version >= 11:
                    evaluation_tasks.append(
                        self._task_row(
                            experiment_id,
                            4,
                            None,
                            variant_id,
                            "directional_mantis_rf:representations",
                        )
                    )
                    evaluation_tasks.extend(
                        self._task_row(
                            experiment_id,
                            4,
                            None,
                            variant_id,
                            f"directional_mantis_rf:train:h{horizon:02d}",
                        )
                        for horizon in range(1, 15)
                    )
                    evaluation_tasks.extend(
                        self._task_row(
                            experiment_id,
                            4,
                            None,
                            variant_id,
                            f"directional_mantis_rf:predict:h{horizon:02d}",
                        )
                        for horizon in range(1, 15)
                    )
                    evaluation_tasks.extend(
                        self._task_row(
                            experiment_id,
                            6,
                            None,
                            variant_id,
                            f"directional_mantis_rf:h{horizon:02d}",
                        )
                        for horizon in range(1, 15)
                    )
            if self.configuration.version == 12:
                evaluation_tasks = [row for row in evaluation_tasks if row[2] != 5]
                evaluation_tasks.append(self._task_row(
                    experiment_id, 6, None, variants[0][0], "paper_tables"
                ))
            for variant_id, _, _ in variants:
                if self.configuration.version in {10, 11}:
                    continue
                for candidate in (
                    *self.config["models"].keys(), self.config["combination"]["method"]
                ):
                    if candidate.startswith("directional_") or candidate == "none":
                        continue
                    evaluation_tasks.append(
                        self._task_row(
                            experiment_id, 6, None, variant_id, candidate
                        )
                    )
            if self.configuration.version == 12:
                evaluation_tasks.extend(
                    self._task_row(experiment_id, 6, None, "official_reference", candidate)
                    for candidate in ("m4_smyl", "m4_fforma")
                )
            self.connection.executemany(
                """INSERT INTO experiment_tasks
                (task_id, experiment_id, stage, forecast_instance_id,
                 variant_id, candidate, status)
                VALUES (?, ?, ?, ?, ?, ?, 'pending')
                ON CONFLICT (task_id) DO NOTHING""",
                evaluation_tasks,
            )
            if expanding_scope:
                self.connection.execute(
                    "DELETE FROM official_evaluations WHERE experiment_id=?",
                    [experiment_id],
                )
                self.connection.execute(
                    "DELETE FROM submission_exports WHERE experiment_id=?",
                    [experiment_id],
                )
                self.connection.execute(
                    """UPDATE experiment_tasks SET status='pending', started_at=NULL,
                       completed_at=NULL, updated_at=current_timestamp, last_error=NULL
                       WHERE experiment_id=? AND stage=6""",
                    [experiment_id],
                )
                self.connection.execute(
                    """UPDATE experiments SET scope=?, status='planned',
                       updated_at=current_timestamp WHERE experiment_id=?""",
                    [plan_scope, experiment_id],
                )
            self.connection.execute("COMMIT")
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise
        counts = dict(
            self.connection.execute(
                "SELECT stage, count(*) FROM experiment_tasks WHERE experiment_id = ? GROUP BY stage",
                [experiment_id],
            ).fetchall()
        )
        return ExperimentPlan(
            experiment_id,
            plan_scope,
            len(official["instances"]),
            len(variants),
            {int(process): int(count) for process, count in counts.items()},
            official["configuration_name"],
        )

    def _register_task(
        self,
        experiment_id: str,
        process: int,
        instance_id: str | None,
        variant_id: str | None,
        candidate: str | None,
    ) -> str:
        """Purpose: Register one idempotent unit in the persisted task graph.

        Inputs: Experiment/process identity and optional instance, variant, and candidate.
        Outputs: Deterministic task ID; inserts a pending DuckDB row when absent.
        """
        row = self._task_row(
            experiment_id, process, instance_id, variant_id, candidate
        )
        self.connection.execute(
            """INSERT INTO experiment_tasks
            (task_id, experiment_id, stage, forecast_instance_id, variant_id, candidate, status)
            VALUES (?, ?, ?, ?, ?, ?, 'pending') ON CONFLICT (task_id) DO NOTHING""",
            row,
        )
        return row[0]

    def _task_row(
        self,
        experiment_id: str,
        process: int,
        instance_id: str | None,
        variant_id: str | None,
        candidate: str | None,
    ) -> tuple[str, str, int, str | None, str | None, str | None]:
        """Build the deterministic task ID and normalized database tuple for one process unit."""
        identity = {
            "experiment": experiment_id,
            "stage": process,
            "instance": instance_id,
            "variant": variant_id,
            "candidate": candidate,
        }
        task_id = f"poc1-task/{json_fingerprint(identity)[:32]}"
        return (
            task_id,
            experiment_id,
            process,
            instance_id,
            variant_id,
            candidate,
        )

    def _begin_invocation(
        self,
        experiment_id: str,
        process: int,
        workers: int,
        device: str,
        batch_size: int,
        profile: ExecutionProfile | None = None,
        overrides: dict[str, Any] | None = None,
        hardware: dict[str, Any] | None = None,
    ) -> str:
        """Purpose: Start durable accounting for one restartable process invocation.

        Inputs: Experiment and process IDs, resolved concurrency/device/batch controls,
        execution profile and overrides, and measured hardware evidence.
        Outputs: New invocation ID; inserts a running invocation and marks orphaned
        running attempts failed while resetting their tasks to pending in DuckDB.
        """
        invocation_id = f"poc1-invocation/{uuid.uuid4().hex}"
        self.connection.execute(
            """UPDATE experiment_invocations SET status='failed', ended_at=current_timestamp,
               error='interrupted before completion; recovered by a later invocation'
               WHERE experiment_id=? AND requested_gate=? AND status='running'""",
            [experiment_id, PROCESSES[process]],
        )
        self.connection.execute(
            """INSERT INTO experiment_invocations
            (invocation_id, experiment_id, requested_gate, worker_count, device,
             batch_size, environment, machine, started_at, status,
             execution_profile, resolved_execution, execution_overrides, hardware)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', ?, ?, ?, ?)""",
            [
                invocation_id,
                experiment_id,
                PROCESSES[process],
                workers,
                device,
                batch_size,
                canonical_json({"python": platform.python_version()}),
                canonical_json(
                    {"system": platform.system(), "machine": platform.machine(), "node": platform.node()}
                ),
                utc_now(),
                profile.name if profile else "legacy_manual",
                canonical_json(profile.to_dict()) if profile else None,
                canonical_json(overrides or {}),
                canonical_json(hardware or {}),
            ],
        )
        self.connection.execute(
            """UPDATE experiment_task_attempts SET status='failed', ended_at=current_timestamp,
               error='interrupted before completion'
               WHERE status='running' AND task_id IN
               (SELECT task_id FROM experiment_tasks WHERE experiment_id=? AND stage=?)""",
            [experiment_id, process],
        )
        self.connection.execute(
            """UPDATE experiment_tasks SET status='pending', last_error='interrupted before completion',
               updated_at=current_timestamp WHERE experiment_id=? AND stage=? AND status='running'""",
            [experiment_id, process],
        )
        return invocation_id

    def _chronos_hardware(self, requested_device: str) -> dict[str, Any]:
        """Purpose: Probe accelerator availability through the isolated Chronos bridge.

        Inputs: Requested device plus worker/environment paths from stored configuration.
        Outputs: Decoded hardware mapping; launches a checked subprocess and raises
        ``RuntimeError`` with bridge diagnostics when the device is unavailable.
        """
        paths = self.configuration.execution_paths
        try:
            completed = subprocess.run(
                [
                    str(self.root / paths["chronos_environment"] / "bin/python"),
                    str(self.root / paths["chronos_worker"]),
                    "hardware",
                    "--device",
                    requested_device,
                ],
                cwd=self.root,
                check=True,
                capture_output=True,
                text=True,
                timeout=60,
            )
        except subprocess.CalledProcessError as error:
            detail = (error.stderr or error.stdout or str(error)).strip()
            raise RuntimeError(
                f"required accelerator {requested_device!r} is unavailable: {detail}"
            ) from error
        return json.loads(completed.stdout)

    def execution_hardware(self, profile: ExecutionProfile) -> dict[str, Any]:
        """Purpose: Validate and cache execution hardware/software provenance.

        Inputs: Execution profile and stored Chronos model and execution settings.
        Outputs: Host, accelerator, R, forecast-package, and model detail mapping;
        probes Chronos and R subprocesses on first use and mutates only the cache.
        """
        requested = profile.required_accelerator or "auto"
        if requested not in self._hardware_cache:
            accelerator: dict[str, Any] = {}
            if "chronos_2" in self.config["models"]:
                accelerator = self._chronos_hardware(requested)
                expected = profile.expected_accelerator_name
                if expected and expected.lower() not in accelerator["accelerator_device_name"].lower():
                    raise RuntimeError(
                        f"profile {profile.name} requires {expected}, detected "
                        f"{accelerator['accelerator_device_name']}"
                    )
            system = system_hardware()
            validate_system_memory(profile, system)
            r_output = subprocess.run(
                [
                    "Rscript",
                    "-e",
                    'cat(R.version.string, "|", as.character(packageVersion("forecast")))',
                ],
                cwd=self.root,
                check=True,
                capture_output=True,
                text=True,
                timeout=30,
            ).stdout.split("|")
            model_metadata = {}
            if "chronos_2" in self.config["models"]:
                model_metadata = {
                    "model_revision": self.config["models"]["chronos_2"]["revision"],
                    "model_dtype": self.config["models"]["chronos_2"]["dtype"],
                }
            self._hardware_cache[requested] = {
                **system,
                **accelerator,
                "R_version": r_output[0].strip(),
                "forecast_package_version": r_output[1].strip(),
                **model_metadata,
            }
        return self._hardware_cache[requested]

    def validate_hardware(
        self, profile: ExecutionProfile, run_chronos_smoke: bool = False
    ) -> dict[str, Any]:
        """Purpose: Validate a profile and optionally perform one Chronos smoke forecast.

        Inputs: Execution profile and smoke flag; smoke data is the first planned
        context vector and horizon, with model controls from stored configuration.
        Outputs: Profile, hardware, and optional inference timing/shape mapping;
        probes subprocesses and model inference but does not mutate scientific rows.
        """
        details = self.execution_hardware(profile)
        result: dict[str, Any] = {
            "profile": profile.to_dict(),
            "hardware": details,
            "chronos_smoke": None,
        }
        if not run_chronos_smoke:
            return result
        row = self.connection.execute(
            "SELECT context_target, horizon FROM forecast_instances ORDER BY official_position LIMIT 1"
        ).fetchone()
        if row is None:
            raise RuntimeError("plan the smoke experiment before running Chronos validation")
        chronos = self.config["models"]["chronos_2"]
        paths = self.configuration.execution_paths
        command = [
            str(self.root / paths["chronos_environment"] / "bin/python"),
            str(self.root / paths["chronos_worker"]),
            "serve",
            "--model",
            chronos["repository"],
            "--revision",
            chronos["revision"],
            "--device",
            profile.required_accelerator or "auto",
            "--dtype",
            chronos["dtype"],
            "--internal-cpu-threads",
            str(self.configuration.execution["thread_limits"]["chronos"]),
        ]
        with PersistentChronosWorker(
            command,
            startup_timeout=float(
                self.configuration.execution["worker_timeouts_seconds"]["chronos_startup"]
            ),
        ) as worker:
            response = worker.request(
                {
                    "command": "predict",
                    "batch_id": "hardware-validation",
                    "jobs": [{"id": "validation", "context": row[0]}],
                    "horizon": row[1],
                    "quantile_levels": list(self.quantiles),
                    "inference_batch_size": 1,
                    "cross_learning": chronos["cross_learning"],
                    "predict_batches_jointly": chronos["predict_batches_jointly"],
                },
                timeout=float(
                    self.configuration.execution["worker_timeouts_seconds"]["chronos_request"]
                ),
            )
            if response.get("type") != "result" or len(response["results"]) != 1:
                raise RuntimeError(f"Chronos hardware validation failed: {response}")
            result["chronos_smoke"] = {
                "model_load_seconds": worker.ready["model_load_seconds"],
                "model_load_count": worker.ready["model_load_count"],
                "inference_seconds": response["inference_seconds"],
                "effective_batch_size": response["effective_batch_size"],
                "forecast_horizon": len(response["results"][0]["mean"]),
                "backend": worker.ready["accelerator_backend"],
                "device_name": worker.ready["accelerator_device_name"],
            }
        return result

    def _start_tasks(self, rows: list[tuple], invocation_id: str) -> dict[str, int]:
        """Purpose: Begin a durable attempt for every selected incomplete task.

        Inputs: Task query rows and owning invocation ID.
        Outputs: Task-ID to attempt-number mapping; transactionally marks each task
        running and inserts its running attempt row in DuckDB.
        """
        attempts = {}
        for row in rows:
            task_id = row[0]
            self.connection.execute("BEGIN TRANSACTION")
            try:
                self.connection.execute(
                    """UPDATE experiment_tasks SET status='running', attempt_count=attempt_count+1,
                    started_at=current_timestamp, completed_at=NULL, updated_at=current_timestamp,
                    last_error=NULL WHERE task_id=?""",
                    [task_id],
                )
                attempt = self.connection.execute(
                    "SELECT attempt_count FROM experiment_tasks WHERE task_id=?", [task_id]
                ).fetchone()[0]
                self.connection.execute(
                    """INSERT INTO experiment_task_attempts
                    (attempt_id, task_id, invocation_id, attempt_number, started_at, status)
                    VALUES (?, ?, ?, ?, current_timestamp, 'running')""",
                    [f"{task_id}/attempt/{attempt}", task_id, invocation_id, attempt],
                )
                self.connection.execute("COMMIT")
                attempts[task_id] = attempt
            except BaseException:
                self.connection.execute("ROLLBACK")
                raise
        return attempts

    def _commit_task(
        self,
        task_id: str,
        attempt: int,
        runtime: float,
        insert: Callable[[], None],
        resources: dict[str, Any] | None = None,
    ) -> None:
        """Purpose: Commit one scientific result and its task completion atomically.

        Inputs: Task/attempt identity, runtime, callback that writes the process result,
        and optional worker resource provenance.
        Outputs: None; executes the callback and completes task and attempt rows in
        one DuckDB transaction, rolling all writes back on failure.
        """
        self.connection.execute("BEGIN TRANSACTION")
        try:
            insert()
            self.connection.execute(
                """UPDATE experiment_tasks SET status='completed', completed_at=current_timestamp,
                updated_at=current_timestamp, last_error=NULL WHERE task_id=?""",
                [task_id],
            )
            self.connection.execute(
                """UPDATE experiment_task_attempts SET status='completed', ended_at=current_timestamp,
                runtime_seconds=?, resource_usage=? WHERE attempt_id=?""",
                [runtime, canonical_json(resources or {}), f"{task_id}/attempt/{attempt}"],
            )
            self.connection.execute("COMMIT")
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise

    def _insert_or_verify(self, table: str, record: dict[str, Any]) -> None:
        """Insert a directional scientific record or reject any stored conflict."""
        columns = DIRECTIONAL_RECORD_COLUMNS.get(table)
        if columns is None or set(record) != set(columns):
            raise ValueError(f"invalid insert-or-verify contract for {table}")
        identity = record[columns[0]]
        stored = self.connection.execute(
            f"SELECT {', '.join(f'r.{column}' for column in columns)} "
            f"FROM {table} AS r WHERE r.{columns[0]}=?",
            [identity],
        ).fetchone()
        expected = tuple(record[column] for column in columns)
        if stored is None:
            placeholders = ", ".join("?" for _ in columns)
            self.connection.execute(
                f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
                expected,
            )
            return
        normalized_stored = tuple(
            json.loads(value)
            if isinstance(value, str)
            and (
                column.endswith("provenance")
                or column in {
                    "scientific_definition", "runtime_versions", "candidate_policy",
                    "execution_metadata", "package_versions",
                }
            )
            else value
            for column, value in zip(columns, stored, strict=True)
        )
        normalized_expected = tuple(
            json.loads(value)
            if isinstance(value, str)
            and (
                column.endswith("provenance")
                or column in {
                    "scientific_definition", "runtime_versions", "candidate_policy",
                    "execution_metadata", "package_versions",
                }
            )
            else value
            for column, value in zip(columns, expected, strict=True)
        )
        if normalized_stored != normalized_expected:
            raise RuntimeError(
                f"conflicting accepted {table} record for identity {identity}"
            )

    def _fail_task(self, task_id: str, attempt: int, error: str) -> None:
        """Purpose: Persist terminal failure state for one task attempt.

        Inputs: Task ID, current attempt number, and diagnostic text.
        Outputs: None; atomically marks DuckDB task and attempt rows failed.
        """
        self.connection.execute("BEGIN TRANSACTION")
        try:
            self.connection.execute(
                "UPDATE experiment_tasks SET status='failed', last_error=?, updated_at=current_timestamp WHERE task_id=?",
                [error, task_id],
            )
            self.connection.execute(
                """UPDATE experiment_task_attempts SET status='failed', ended_at=current_timestamp,
                error=? WHERE attempt_id=?""",
                [error, f"{task_id}/attempt/{attempt}"],
            )
            self.connection.execute("COMMIT")
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise

    def _r_worker(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Purpose: Execute cleaning or AutoARIMA jobs in the configured R bridge.

        Inputs: JSON-serializable action and batched series/forecast payload; script,
        timeout, and thread limits originate in stored execution configuration.
        Outputs: Decoded result/provenance mapping; launches a checked R subprocess
        with bounded threads and does not permit the worker to access DuckDB.
        """
        paths = self.configuration.execution_paths
        execution = self.configuration.execution
        timeout = float(execution["worker_timeouts_seconds"]["r"])
        threads = str(execution["thread_limits"]["r"])
        action = payload.get("action")
        if action in {"forecast", "diagnose_period"} and "r_forecast_worker" in paths:
            script = paths["r_forecast_worker"]
        elif action == "forecast":
            script = paths["r_auto_arima_worker"]
        else:
            script = paths["r_preprocess_worker"]
        completed = subprocess.run(
            ["Rscript", str(self.root / script)],
            cwd=self.root,
            input=json.dumps(payload),
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
            env={
                **os.environ,
                "OMP_NUM_THREADS": threads,
                "OPENBLAS_NUM_THREADS": threads,
                "RENV_CONFIG_SYNCHRONIZED_CHECK": "false",
            },
        )
        return json.loads(completed.stdout)

    def _pending(self, experiment_id: str, process: int) -> list[tuple]:
        """Purpose: Select restartable work for one process invocation.

        Inputs: Persisted experiment ID and Process 02–06 number.
        Outputs: Ordered incomplete task IDs and instance/variant/candidate dimensions
        read from DuckDB without changing task state.
        """
        if process == 4 and self.configuration.version >= 11:
            self._reopen_missing_fitted_model_tasks(experiment_id)
        return self.connection.execute(
            """SELECT task_id, forecast_instance_id, variant_id, candidate
            FROM experiment_tasks WHERE experiment_id=? AND stage=? AND status!='completed'
            ORDER BY task_id""",
            [experiment_id, process],
        ).fetchall()

    def _reopen_missing_fitted_model_tasks(self, experiment_id: str) -> None:
        """Make real file absence override stale completed version-11 training state."""
        from .shared_model_storage import ModelStorage

        settings = self.configuration.model_storage
        storage = ModelStorage(self.root / settings["root"], settings["experiment"])
        missing = []
        if not storage.exists("directional_dtw", "D", "all_horizons"):
            missing.append("directional_dtw:train")
        missing.extend(
            f"directional_mantis_rf:train:h{horizon:02d}"
            for horizon in range(1, 15)
            if not storage.exists("directional_mantis_rf", "D", horizon)
        )
        if missing:
            placeholders = ",".join("?" for _ in missing)
            self.connection.execute(
                f"""UPDATE experiment_tasks SET status='pending', completed_at=NULL,
                           updated_at=current_timestamp,
                           last_error='fitted model file is missing'
                    WHERE experiment_id=? AND stage=4 AND candidate IN ({placeholders})""",
                [experiment_id, *missing],
            )

    def _check_process_prerequisite(self, experiment_id: str, process: int) -> None:
        """Purpose: Enforce persisted predecessor-process completion.

        Inputs: Persisted experiment ID and requested Process 02–06 number.
        Outputs: None; performs a read-only task count and raises ``RuntimeError``
        when the preceding process still has incomplete tasks.
        """
        if process == 2:
            return
        incomplete = self.connection.execute(
            """SELECT count(*) FROM experiment_tasks
            WHERE experiment_id=? AND stage=? AND status!='completed'""",
            [experiment_id, process - 1],
        ).fetchone()[0]
        if incomplete:
            raise RuntimeError(f"Process {process - 1} has {incomplete} incomplete tasks")

    def run_process(
        self,
        experiment_id: str,
        process: int,
        workers: int | None = None,
        device: str = "auto",
        batch_size: int | None = None,
        execution: tuple[ExecutionProfile, dict[str, Any]] | None = None,
        execution_settings: ExecutionSettings | None = None,
    ) -> dict[str, Any]:
        """Purpose: Execute one restartable process from Processes 02–06.

        Inputs: Experiment/process identity and optional worker, device, batch,
        profile, override, and Dask settings from stored configuration or caller.
        Outputs: Invocation ID and task/runtime summary; validates predecessor and
        hardware requirements, optionally opens Dask, records invocation/attempt state,
        dispatches the process, commits results through the single writer, finalizes
        failures for retry, and closes the Dask client.
        """
        if (
            process not in PROCESSES
            or (workers is not None and workers < 1)
            or (batch_size is not None and batch_size < 1)
        ):
            raise ValueError("process must be 2..6; workers and batch_size must be positive")
        configured_batches = self.configuration.execution["batch_sizes"]
        preprocess_batch_size = int(batch_size or configured_batches["preprocess"])
        if execution is None:
            profile, configured_settings = self._configured_execution()
            overrides = {}
        else:
            profile, overrides = execution
            configured_settings = ExecutionSettings()
        self._active_machine_environment = profile.machine_environment
        self.table_sensitivity_batch_size = (
            profile.table_sensitivity_batch_size
            or configured_batches.get("table_sensitivity")
        )
        settings = execution_settings or configured_settings
        if (
            process == 4
            and self.configuration.seasonal_period_tuning is not None
            and self.configuration.seasonal_period_tuning["enabled"]
        ):
            validate_heavy_tuning_execution(profile, settings, overrides)
        if settings.mode == "sequential":
            profile = replace(
                profile,
                cleaning_workers=1,
                transformation_workers=1,
                autoarima_workers=1,
                chronos_processes=1,
                combination_workers=1,
                evaluation_workers=1,
                cpu_gpu_overlap=False,
            )
        coordinator_hardware = self.execution_hardware(profile)
        hardware: dict[str, Any] = coordinator_hardware
        resolved_device = profile.required_accelerator or device
        dask_client = None
        if settings.mode == "dask" and (process != 6 or self.configuration.version == 12):
            from distributed import Client

            from .shared_distributed_execution import (
                repository_source_manifest,
                validate_directional_cluster,
                validate_cluster,
                validate_tuning_cluster,
            )

            if process == 4 and self.configuration.seasonal_period_tuning is None and not settings.dask_scheduler_address:
                raise RuntimeError(
                    "ordinary Gate 4 Dask execution requires an existing scheduler address"
                )
            if settings.dask_scheduler_address:
                dask_client = Client(settings.dask_scheduler_address,
                                     timeout=f"{settings.dask_timeout_seconds}s")
            else:
                dask_client = Client(n_workers=1, threads_per_worker=1, processes=True,
                                     resources={"CPU": 1, "CHRONOS_GPU_SLOT": 1},
                                     timeout=f"{settings.dask_timeout_seconds}s")
            expected_gpu_name = None
            try:
                if self.configuration.version in {10, 11, 12}:
                    topology = profile.distributed_topology(
                        self.configuration.version == 11
                        or self.configuration.version == 12 and settings.dask_expected_gpu_workers > 0
                    )
                    paths = self.configuration.execution_paths
                    lock_path = self.root / paths["classifiers_lock"]
                    gpu_workers = 0
                    if self.configuration.version >= 11:
                        gpu_workers = int(
                            settings.dask_expected_gpu_workers
                            if settings is not None
                            else self.configuration.resolved["execution"]
                            ["final_acceptance"]["workers"].get("ubuntu_gpu", 0)
                        )
                    mantis_lock_path = (
                        self.root / paths["mantis_lock"]
                        if self.configuration.version >= 11
                        else None
                    )
                    cluster = validate_directional_cluster(
                        dask_client,
                        expected_workers=topology["cpu_workers"] + gpu_workers,
                        expected_mac_workers=0,
                        expected_ubuntu_workers=0,
                        expected_machine_workers=(
                            {
                                machine.machine.hostname: {
                                    "cpu_workers": topology["workers_by_machine"]
                                    [machine.machine.machine_id]["cpu_workers"],
                                    "gpu_workers": (
                                        gpu_workers
                                        if "cuda" in machine.machine.capabilities
                                        else 0
                                    ),
                                }
                                for machine in profile.machine_environment.machines
                            }
                            if profile.machine_environment else None
                        ),
                        timeout=settings.dask_timeout_seconds,
                        expected_manifest=repository_source_manifest(),
                        classifiers_environment=paths["classifiers_environment"],
                        worker_script=paths.get(
                            "directional_dtw_training_worker",
                            paths.get("directional_dtw_worker"),
                        ),
                        classifiers_lock=paths["classifiers_lock"],
                        expected_lock_fingerprint=hashlib.sha256(
                            lock_path.read_bytes()
                        ).hexdigest(),
                        expected_gpu_workers=gpu_workers,
                        mantis_environment=paths.get("mantis_environment"),
                        mantis_worker_script=paths.get("mantis_worker"),
                        mantis_lock=paths.get("mantis_lock"),
                        expected_mantis_lock_fingerprint=(
                            hashlib.sha256(mantis_lock_path.read_bytes()).hexdigest()
                            if mantis_lock_path is not None else None
                        ),
                    )
                    resolved_device = (
                        "cuda" if self.configuration.version == 11 or gpu_workers else "cpu"
                    )
                    if self.configuration.version == 12 and process == 4 and gpu_workers:
                        # Mixed work must pass both existing provider boundaries.
                        chronos_cluster = validate_cluster(
                            dask_client, expected_workers=settings.dask_expected_workers,
                            timeout=settings.dask_timeout_seconds,
                            expected_commit=subprocess.run(
                                ["git", "rev-parse", "HEAD"], cwd=self.root,
                                check=True, capture_output=True, text=True,
                            ).stdout.strip(),
                            expected_configuration_hash=self.configuration.scientific_hash,
                            expected_gift_eval_revision=self.configuration.resolved["evaluation"]["gift_eval"]["code_revision"],
                            expected_chronos_revision=self.config["models"]["chronos_2"]["revision"],
                            expected_chronos_version=self.config["models"]["chronos_2"]["chronos_forecasting"],
                            chronos_repository=self.config["models"]["chronos_2"]["repository"],
                            chronos_environment=paths["chronos_environment"],
                            gift_eval_source_directory=self.configuration.resolved["evaluation"]["gift_eval"]["source_directory"],
                            require_gpu=True,
                            expected_gpu_name=profile.expected_accelerator_name,
                            expected_gpu_workers=gpu_workers,
                            expected_manifest=repository_source_manifest(),
                        )
                        cluster = {"directional": cluster, "chronos": chronos_cluster}
                elif ("chronos_2" not in self.config["models"] or
                        process == 4 and self.configuration.seasonal_period_tuning is not None):
                    topology = profile.distributed_topology(False)
                    cluster = validate_tuning_cluster(
                        dask_client,
                        expected_workers=topology["cpu_workers"],
                        expected_mac_workers=0,
                        expected_ubuntu_workers=0,
                        expected_tuning_workers=sum(
                            value["tuning_workers"]
                            for value in topology["workers_by_machine"].values()
                        ),
                        expected_machine_workers=(
                            {
                                machine.machine.hostname: topology["workers_by_machine"]
                                [machine.machine.machine_id]["cpu_workers"]
                                for machine in profile.machine_environment.machines
                            }
                            if profile.machine_environment else None
                        ),
                        timeout=settings.dask_timeout_seconds,
                        expected_manifest=repository_source_manifest(),
                    )
                    resolved_device = "cpu"
                else:
                    expected_gpu_name = profile.expected_accelerator_name or self.configuration.resolved[
                        "execution"]["final_acceptance"].get("gpu_name")
                    resolved_device = "cuda"
                    expected_commit = subprocess.run(
                        ["git", "rev-parse", "HEAD"],
                        cwd=self.root,
                        check=True,
                        capture_output=True,
                        text=True,
                    ).stdout.strip()
                    cluster = validate_cluster(
                        dask_client,
                        expected_workers=settings.dask_expected_workers,
                        timeout=settings.dask_timeout_seconds,
                        expected_commit=expected_commit,
                        expected_configuration_hash=self.configuration.scientific_hash,
                        expected_gift_eval_revision=self.configuration.resolved["evaluation"]["gift_eval"]["code_revision"],
                        expected_chronos_revision=self.config["models"]["chronos_2"]["revision"],
                        expected_chronos_version=self.config["models"]["chronos_2"]["chronos_forecasting"],
                        chronos_repository=self.config["models"]["chronos_2"]["repository"],
                        chronos_environment=self.configuration.execution_paths["chronos_environment"],
                        gift_eval_source_directory=self.configuration.resolved["evaluation"]["gift_eval"]["source_directory"],
                        require_gpu=process == 4 and settings.dask_expected_gpu_workers > 0,
                        expected_gpu_name=expected_gpu_name,
                        expected_gpu_workers=settings.dask_expected_gpu_workers,
                        expected_manifest=repository_source_manifest(),
                        expected_topology=(profile.distributed_topology(settings.dask_expected_gpu_workers > 0)
                                           if profile.machine_environment is not None
                                           or profile.dask_mac_cpu_workers is not None else None),
                    )
            except BaseException:
                dask_client.close()
                raise
            hardware = {"coordinator": coordinator_hardware, "dask_workers": cluster}
        process_workers = {
            2: profile.cleaning_workers,
            3: profile.transformation_workers,
            4: max(profile.autoarima_workers, profile.chronos_processes),
            5: profile.combination_workers,
            6: profile.evaluation_workers,
        }[process]
        self._check_process_prerequisite(experiment_id, process)
        actual_series, actual_instances = self.connection.execute(
            """SELECT count(DISTINCT i.series_id), count(DISTINCT i.forecast_instance_id)
               FROM forecast_instances i
               JOIN experiment_tasks t USING (forecast_instance_id)
               WHERE t.experiment_id=? AND t.stage=2""",
            [experiment_id],
        ).fetchone()
        invocation_overrides = {
            **overrides,
            **({"table_sensitivity_batch_size": self.table_sensitivity_batch_size}
               if self.configuration.version == 12 else {}),
            "selection": {
                **self.configuration.resolved["data"]["selection"],
                "series_count_actual": int(actual_series),
                "forecast_instance_count_actual": int(actual_instances),
            },
        }
        invocation = self._begin_invocation(
            experiment_id,
            process,
            process_workers,
            resolved_device,
            {
                2: preprocess_batch_size,
                3: int(configured_batches["transform"]),
                4: int(
                    configured_batches.get(
                        "chronos", configured_batches.get("directional_prediction", 1)
                    )
                ),
                5: int(configured_batches.get("combine", 1)),
                6: int(self.table_sensitivity_batch_size
                       if self.configuration.version == 12
                       else configured_batches.get("gift_eval", 1)),
            }[process],
            profile,
            invocation_overrides,
            {**hardware, "execution_settings": settings.to_dict()},
        )
        rows = self._pending(experiment_id, process)
        attempts = self._start_tasks(rows, invocation)
        # Borrowed client: Process 06 reuses this connection; run_process owns closure.
        self._active_dask_client = dask_client
        started = time.monotonic()
        failures = []
        execution_error: BaseException | None = None
        try:
            if process == 2:
                self._run_02_preprocess(
                    experiment_id,
                    rows,
                    attempts,
                    profile.cleaning_workers,
                    preprocess_batch_size,
                    dask_client,
                    settings,
                )
            elif process == 3:
                self._run_03_transform(
                    experiment_id,
                    rows,
                    attempts,
                    profile.transformation_workers,
                    dask_client,
                    settings,
                    profile,
                )
            elif process == 4:
                self._run_04_forecast(
                    experiment_id,
                    rows,
                    attempts,
                    profile,
                    resolved_device,
                    dask_client,
                    settings,
                )
            elif process == 5:
                self._run_05_combine(
                    experiment_id,
                    rows,
                    attempts,
                    profile.combination_workers,
                    dask_client,
                    settings,
                )
            else:
                self._run_06_evaluate(
                    experiment_id,
                    rows,
                    attempts,
                    profile.evaluation_workers,
                    settings,
                )
        except BaseException as exc:
            execution_error = exc
            error = f"{type(exc).__name__}: {exc}"
            for row in rows:
                current = self.connection.execute(
                    "SELECT status FROM experiment_tasks WHERE task_id=?", [row[0]]
                ).fetchone()[0]
                if current == "running":
                    self._fail_task(row[0], attempts[row[0]], error)
                    failures.append(row[0])
        counts = dict(
            self.connection.execute(
                "SELECT status, count(*) FROM experiment_tasks WHERE experiment_id=? AND stage=? GROUP BY status",
                [experiment_id, process],
            ).fetchall()
        )
        status = (
            "completed"
            if execution_error is None and set(counts) <= {"completed"}
            else "failed"
        )
        summary = {
            "stage": process,
            "selected": len(rows),
            "skipped": self.connection.execute(
                "SELECT count(*) FROM experiment_tasks WHERE experiment_id=? AND stage=? AND status='completed'",
                [experiment_id, process],
            ).fetchone()[0]
            - (len(rows) - len(failures)),
            "counts": counts,
            "runtime_seconds": time.monotonic() - started,
        }
        self.connection.execute(
            """UPDATE experiment_invocations SET ended_at=current_timestamp, status=?, summary=?, error=?
            WHERE invocation_id=?""",
            [status, canonical_json(summary), "; ".join(failures) or None, invocation],
        )
        if dask_client is not None:
            dask_client.close()
        self._active_dask_client = None
        if execution_error is not None and not failures:
            # A post-commit orchestration/acknowledgement failure must remain
            # visible even when every scientific row was durably accepted.
            raise execution_error
        if status == "failed":
            raise RuntimeError(f"Process {process} failed; rerun retries failed tasks")
        return {"invocation_id": invocation, **summary}

    def _run_02_preprocess(
        self,
        experiment_id: str,
        rows: list[tuple],
        attempts: dict[str, int],
        workers: int,
        batch_size: int,
        dask_client: Any = None,
        settings: ExecutionSettings | None = None,
    ) -> None:
        """Purpose: Execute Process 02 cleaning for selected training contexts.

        Inputs: Experiment task rows/attempts, worker and batch controls, and optional
        Dask client/settings; contexts and official seasonality come from DuckDB.
        Outputs: None; dispatches R cleaning batches and atomically inserts each
        preprocessed vector, fingerprints, package provenance, and task completion.
        """
        jobs = []
        for task_id, instance_id, _, method in rows:
            context, frequency, metadata = self.connection.execute(
                """SELECT i.context_target, b.frequency, b.metadata FROM forecast_instances i
                JOIN benchmark_configurations b USING (benchmark_configuration_id)
                WHERE i.forecast_instance_id=?""",
                [instance_id],
            ).fetchone()
            benchmark_metadata = json.loads(metadata)
            seasonality = benchmark_metadata.get(
                "r_period", benchmark_metadata.get("official_seasonality")
            )
            if seasonality is None:
                raise RuntimeError("benchmark metadata is missing the resolved R period")
            jobs.append(
                {
                    "id": task_id,
                    "context": context,
                    "mode": method,
                    "official_frequency": frequency,
                    "seasonality": seasonality,
                    "instance_id": instance_id,
                }
            )

        from .shared_workflow_orchestration import run_gate_compute_flow

        outcomes = run_gate_compute_flow(
            process_id=2,
            batches=_batches(jobs, batch_size),
            options={
                "script": self.configuration.execution_paths["r_preprocess_worker"],
                "timeout": float(self.configuration.execution["worker_timeouts_seconds"]["r"]),
                "threads": int(self.configuration.execution["thread_limits"]["r"]),
            },
            scheduler_address=(dask_client.scheduler.address if dask_client else None),
            retries=settings.dask_retries if settings else 0,
            max_in_flight=settings.dask_max_in_flight if settings else workers,
            local_workers=workers,
        )
        errors = []
        for outcome in outcomes:
            if "response" not in outcome:
                errors.append(outcome["error"])
                continue
            batch, response = outcome["batch"], outcome["response"]
            runtime = response["runtime_seconds"]
            result_ids = [item["id"] for item in response["results"]]
            by_id = {item["id"]: item for item in response["results"]}
            if len(result_ids) != len(set(result_ids)) or set(by_id) != {
                job["id"] for job in batch
            }:
                raise RuntimeError("Process 02 worker returned missing, duplicate, or unexpected task IDs")
            for job in batch:
                task_id, instance_id, method = job["id"], job["instance_id"], job["mode"]
                result = by_id[task_id]
                original = job["context"]
                values = result.get("values")
                if (
                    result.get("preprocessing_mode") != method
                    or result.get("status") != "success"
                    or not isinstance(values, list)
                    or len(values) != len(original)
                    or any(
                        not isinstance(value, (int, float)) or not math.isfinite(value)
                        for value in values
                    )
                    or result.get("missing_count_after") != 0
                ):
                    raise RuntimeError(
                        "Process 02 worker returned invalid preprocessing values or provenance"
                    )
                if method == "standard" and any(
                    original_value is not None
                    and not (
                        isinstance(original_value, float) and math.isnan(original_value)
                    )
                    and original_value != processed_value
                    for original_value, processed_value in zip(
                        original, values, strict=True
                    )
                ):
                    raise RuntimeError(
                        "standard preprocessing changed a finite observation"
                    )
                preprocessing_id = f"preprocessed/{json_fingerprint({'experiment': experiment_id, 'instance': instance_id, 'method': method})[:32]}"

                def insert(
                    preprocessing_id=preprocessing_id,
                    instance_id=instance_id,
                    method=method,
                    original=original,
                    result=result,
                    seasonality=job["seasonality"],
                    packages=response["packages"],
                    official_frequency=job["official_frequency"],
                ):
                    """Purpose: Write one cleaned context within its task transaction.

                    Inputs: Captured instance/method IDs, original and cleaned vectors,
                    official seasonality, and R package provenance.
                    Outputs: None; inserts an idempotent ``preprocessed_series`` row.
                    """
                    self.connection.execute(
                        """INSERT INTO preprocessed_series
                        (preprocessing_id, experiment_id, forecast_instance_id,
                         cleaning_method, input_hash, output_hash, context_target,
                         method_configuration, package_versions, parent_result_id,
                         official_frequency, official_seasonality,
                         preprocessing_status, missing_count_before,
                         missing_count_after, values_changed, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?,
                                current_timestamp)
                        ON CONFLICT (preprocessing_id) DO NOTHING""",
                        [
                            preprocessing_id,
                            experiment_id,
                            instance_id,
                            method,
                            json_fingerprint(original),
                            json_fingerprint(result["values"]),
                            values,
                            canonical_json(
                                {
                                    "mode": method,
                                    "seasonality": seasonality,
                                    "seasonality_source": "approved benchmark metadata from official GIFT-Eval frequency",
                                }
                            ),
                            canonical_json(packages),
                            official_frequency,
                            seasonality,
                            result["status"],
                            result["missing_count_before"],
                            result["missing_count_after"],
                            result["values_changed"],
                        ],
                    )

                self._commit_task(
                    task_id,
                    attempts[task_id],
                    runtime / len(batch),
                    insert,
                    response.get("worker"),
                )
        if errors:
            raise RuntimeError("; ".join(errors))

    def _run_03_transform(
        self,
        experiment_id: str,
        rows: list[tuple],
        attempts: dict[str, int],
        workers: int,
        dask_client: Any = None,
        settings: ExecutionSettings | None = None,
        profile: ExecutionProfile | None = None,
    ) -> None:
        """Purpose: Execute Process 03 transformations of cleaned training contexts.

        Inputs: Experiment task rows/attempts, worker count, and optional Dask controls;
        methods and input vectors are read from variant and preprocessing rows.
        Outputs: None; computes transformed vectors locally or on Dask and atomically
        persists values, parameters, lineage, fingerprints, and task completions.
        """
        if self.configuration.version in {10, 11, 12}:
            if not rows:
                return
            self._run_03_directional_preparation(
                experiment_id,
                rows,
                attempts,
                workers,
                dask_client,
                settings,
                profile,
            )
            if self.configuration.version != 12:
                return
        prepared = []
        metadata = []
        for task_id, instance_id, variant_id, _ in rows:
            cleaning, method = self.connection.execute(
                "SELECT cleaning_method, transformation_method FROM experiment_variants WHERE variant_id=?",
                [variant_id],
            ).fetchone()
            pre_id, values = self.connection.execute(
                """SELECT preprocessing_id, context_target FROM preprocessed_series
                WHERE experiment_id=? AND forecast_instance_id=? AND cleaning_method=?""",
                [experiment_id, instance_id, cleaning],
            ).fetchone()
            prepared.append((values, method))
            metadata.append((task_id, instance_id, variant_id, pre_id, method, values))

        def commit_result(
            meta: tuple,
            result: TransformationResult,
            runtime: float,
            resources: dict[str, Any] | None = None,
        ) -> None:
            """Purpose: Commit one Process 03 transformation result.

            Inputs: Task metadata, transformed vector/parameters, runtime, and worker evidence.
            Outputs: None; delegates result insertion and attempt completion to the
            coordinator's single transaction writer.
            """
            task_id, instance_id, variant_id, pre_id, method, values = meta
            transformation_id = f"transformed/{json_fingerprint({'experiment': experiment_id, 'variant': variant_id, 'instance': instance_id})[:32]}"

            def insert(result=result, values=values):
                """Purpose: Write one transformed series within its task transaction.

                Inputs: Captured source vector, transformation result, IDs, and method.
                Outputs: None; inserts an idempotent ``transformed_series`` row with
                scientific parameters, lineage, and input/output hashes.
                """
                self.connection.execute(
                    """INSERT INTO transformed_series VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                    ON CONFLICT (transformation_id) DO NOTHING""",
                    [
                        transformation_id,
                        experiment_id,
                        variant_id,
                        instance_id,
                        pre_id,
                        method,
                        json_fingerprint(values),
                        json_fingerprint(result.values),
                        list(result.values),
                        canonical_json(result.parameters),
                        pre_id,
                    ],
                )

            self._commit_task(
                task_id, attempts[task_id], runtime, insert, resources
            )

        jobs = [
            {"id": meta[0], "values": values, "method": method}
            for meta, (values, method) in zip(metadata, prepared, strict=True)
        ]
        by_task = {meta[0]: meta for meta in metadata}
        from .shared_workflow_orchestration import run_gate_compute_flow

        outcomes = run_gate_compute_flow(
            process_id=3,
            batches=_batches(jobs, int(self.configuration.execution["batch_sizes"]["transform"])),
            options={},
            scheduler_address=(dask_client.scheduler.address if dask_client else None),
            retries=settings.dask_retries if settings else 0,
            max_in_flight=settings.dask_max_in_flight if settings else workers,
            local_workers=workers,
        )
        errors = []
        for outcome in outcomes:
            if "response" not in outcome:
                errors.append(outcome["error"])
                continue
            batch, response = outcome["batch"], outcome["response"]
            result_ids = [item["id"] for item in response["results"]]
            expected_ids = {job["id"] for job in batch}
            if len(result_ids) != len(set(result_ids)) or set(result_ids) != expected_ids:
                raise RuntimeError(
                    "Process 03 worker returned missing, duplicate, or unexpected task IDs"
                )
            runtime = response["runtime_seconds"] / len(batch)
            for result in response["results"]:
                commit_result(
                    by_task[result["id"]],
                    TransformationResult(
                        tuple(result["values"]), result["parameters"]
                    ),
                    runtime,
                    response["worker"],
                )
        if errors:
            raise RuntimeError("; ".join(errors))

    def _run_03_directional_preparation(
        self,
        experiment_id: str,
        rows: list[tuple],
        attempts: dict[str, int],
        workers: int,
        dask_client: Any,
        settings: ExecutionSettings | None,
        profile: ExecutionProfile | None,
    ) -> None:
        """Prepare S1 references and bounded official inputs through one worker path."""
        from .shared_distributed_execution import (
            repository_source_manifest,
            source_manifest_fingerprint,
        )
        from .shared_labels import (
            DIRECTIONAL_LABEL_DEFINITION_ID,
            directional_labels,
        )
        from .window_preparation import WindowPreparationCoordinator

        manifest_hash = source_manifest_fingerprint(repository_source_manifest())
        windows_database = self.database_path.with_name(
            f"{self.database_path.stem}.windows.duckdb"
        )
        with WindowPreparationCoordinator(
            self.database_path, windows_database
        ) as preparation:
            preparation_summary = preparation.run(
                dask_client=dask_client,
                source_manifest_hash=manifest_hash,
                local_limits=(
                    {"max_series": 100, "max_windows": 200}
                    if dask_client is None
                    else None
                ),
                execution_profile=(profile.name if dask_client is not None else None),
                dask_retries=(settings.dask_retries if settings else 0),
                prefect_compute=dask_client is not None,
                max_in_flight=(settings.dask_max_in_flight if settings else workers),
            )
        contract = {
            "preprocessing_mode": "robust",
            "transformation": "standardise_sample_v1",
            "input_length": 64,
            "boundary": "final raw observations before official forecast origin",
            "worker": self.configuration.execution_paths["r_preprocess_worker"],
            "reference_preparation_id": preparation_summary["preparation_id"],
            "reference_membership_fingerprint": preparation_summary[
                "membership_fingerprint"
            ],
        }
        preparation_id = (
            "directional-official-preparation/"
            + json_fingerprint(contract)[:32]
        )
        preparation_fingerprint = json_fingerprint(contract)
        jobs = []
        metadata = {}
        for task_id, instance_id, variant_id, _ in rows:
            context, actual, frequency, benchmark_metadata = self.connection.execute(
                """SELECT i.context_target, i.actual_target, b.frequency, b.metadata
                   FROM forecast_instances AS i
                   JOIN benchmark_configurations AS b
                     ON b.benchmark_configuration_id=i.benchmark_configuration_id
                   WHERE i.forecast_instance_id=?""",
                [instance_id],
            ).fetchone()
            raw = list(context[-64:])
            if len(raw) != 64:
                raise RuntimeError("directional official input requires 64 raw observations")
            seasonality_metadata = json.loads(benchmark_metadata)
            seasonality = seasonality_metadata.get(
                "r_period", seasonality_metadata.get("official_seasonality")
            )
            input_id = "directional-input/" + json_fingerprint(
                {
                    "experiment": experiment_id,
                    "variant": variant_id,
                    "instance": instance_id,
                    "preparation": preparation_id,
                }
            )[:32]
            jobs.append(
                {
                    "id": task_id,
                    "series_key": task_id,
                    "preprocessing_mode": "robust",
                    "transformation": "standardise_sample_v1",
                    "seasonality": seasonality,
                    "windows": [
                        {
                            "window_id": input_id,
                            "input_start": len(context) - 64,
                            "input_end": len(context),
                            "future_start": len(context),
                            "future_end": len(context) + 14,
                            "input": raw,
                        }
                    ],
                }
            )
            metadata[task_id] = {
                "instance_id": instance_id,
                "variant_id": variant_id,
                "input_id": input_id,
                "raw": raw,
                "actual": actual,
                "frequency": frequency,
            }

        from .shared_workflow_orchestration import run_gate_compute_flow

        batches = _batches(
            jobs, int(self.configuration.execution["batch_sizes"]["window_preparation"])
        )
        outcomes = run_gate_compute_flow(
            process_id=3,
            batches=batches,
            options={
                "bounded_preparation": True,
                "script": self.configuration.execution_paths["r_preprocess_worker"],
                "timeout": float(
                    self.configuration.execution["worker_timeouts_seconds"]["r"]
                ),
                "threads": int(self.configuration.execution["thread_limits"]["r"]),
            },
            scheduler_address=(dask_client.scheduler.address if dask_client else None),
            retries=settings.dask_retries if settings else 0,
            max_in_flight=settings.dask_max_in_flight if settings else workers,
            local_workers=workers,
        )
        errors = []
        for outcome in outcomes:
            if "response" not in outcome:
                errors.append(outcome["error"])
                continue
            batch = outcome["batch"]
            response = outcome["response"]
            by_id = {result["id"]: result for result in response["results"]}
            if set(by_id) != {job["id"] for job in batch}:
                raise RuntimeError("directional preparation returned mismatched task IDs")
            for job in batch:
                task_id = job["id"]
                details = metadata[task_id]
                windows = by_id[task_id].get("windows")
                if not isinstance(windows, list) or len(windows) != 1:
                    raise RuntimeError("directional preparation returned an invalid window")
                result = windows[0]
                labels_array = directional_labels(
                    details["actual"], result["prepared_reference"]
                )
                if len(labels_array) != 14 or any(math.isnan(value) for value in labels_array):
                    raise RuntimeError("official directional labels must be complete")
                labels = [int(value) for value in labels_array]
                input_content = {
                    "experiment_id": experiment_id,
                    "variant_id": details["variant_id"],
                    "forecast_instance_id": details["instance_id"],
                    "preparation_id": preparation_id,
                    "preparation_fingerprint": preparation_fingerprint,
                    "raw_input_hash": result["input_hash"],
                    "cleaned_input_hash": result["cleaned_hash"],
                    "transformed_input_hash": result["transformed_hash"],
                    "transformed_input": result["transformed_input"],
                    "label_reference": result["prepared_reference"],
                    "preprocessing_provenance": canonical_json(
                        {
                            **result["preprocessing"],
                            "frequency": details["frequency"],
                            "raw_boundary": [result["input_start"], result["input_end"]],
                            "source_manifest_fingerprint": manifest_hash,
                        }
                    ),
                    "package_versions": canonical_json(response["packages"]),
                }
                input_record = {
                    "evaluation_input_id": details["input_id"],
                    **input_content,
                    "content_hash": json_fingerprint(input_content),
                }
                label_id = "directional-actual/" + json_fingerprint(
                    {
                        "input": details["input_id"],
                        "definition": DIRECTIONAL_LABEL_DEFINITION_ID,
                    }
                )[:32]
                label_content = {
                    "experiment_id": experiment_id,
                    "evaluation_input_id": details["input_id"],
                    "definition_id": DIRECTIONAL_LABEL_DEFINITION_ID,
                    "labels": labels,
                }
                label_record = {
                    "actual_label_id": label_id,
                    **label_content,
                    "content_hash": json_fingerprint(label_content),
                }

                def insert(
                    input_record=input_record,
                    label_record=label_record,
                ) -> None:
                    self._insert_or_verify(
                        "directional_evaluation_inputs", input_record
                    )
                    self._insert_or_verify("directional_actual_labels", label_record)

                if self.configuration.version == 12:
                    # The shared task completes only after the full-history point
                    # transformation below has also been persisted successfully.
                    insert()
                else:
                    self._commit_task(
                        task_id,
                        attempts[task_id],
                        response["runtime_seconds"] / len(batch),
                        insert,
                        response.get("worker"),
                    )
        if errors:
            raise RuntimeError("; ".join(errors))

    def _run_04_forecast(
        self,
        experiment_id: str,
        rows: list[tuple],
        attempts: dict[str, int],
        profile: ExecutionProfile,
        device: str,
        dask_client: Any = None,
        settings: ExecutionSettings | None = None,
    ) -> None:
        """Purpose: Execute Process 04 base-model forecasting on transformed contexts.

        Inputs: Experiment task rows/attempts, execution profile/device, and optional
        Dask controls; horizons, seasonality, model revisions, and quantiles come
        from DuckDB and stored configuration.
        Outputs: None; invokes R AutoARIMA and Chronos workers, adaptively retries
        Chronos OOM batches, inverse-transforms outputs, and transactionally persists
        original-scale forecast arrays, provenance, hashes, and task state.
        """
        if self.configuration.version in {10, 11, 12}:
            if not rows:
                return
            dtw_rows = [
                row for row in rows if row[3].startswith("directional_dtw:")
            ]
            if self.configuration.version == 10 and dtw_rows:
                self._run_04_directional_dtw(
                    experiment_id, dtw_rows, attempts, profile, dask_client, settings
                )
            if self.configuration.version >= 11:
                mantis_rows = [
                    row for row in rows
                    if row[3].startswith("directional_mantis_rf:")
                ]
                dtw_training = [row for row in dtw_rows if row[3].endswith(":train")]
                dtw_prediction = [row for row in dtw_rows if ":predict:" in row[3]]
                mantis_training = [
                    row for row in mantis_rows
                    if row[3].endswith(":representations") or ":train:" in row[3]
                ]
                mantis_prediction = [
                    row for row in mantis_rows if ":predict:" in row[3]
                ]
                if dtw_training:
                    self._run_04_directional_dtw(
                        experiment_id, dtw_training, attempts, profile,
                        dask_client, settings,
                    )
                if mantis_training:
                    self._run_04_directional_mantis_rf(
                        experiment_id,
                        mantis_training,
                        attempts,
                        profile,
                        resolved_device=device,
                        dask_client=dask_client,
                        settings=settings,
                    )
                if dask_client is not None and (dtw_prediction or mantis_prediction):
                    self._synchronize_model_directory_to_ubuntu()
                if dtw_prediction:
                    self._run_04_directional_dtw(
                        experiment_id, dtw_prediction, attempts, profile,
                        dask_client, settings,
                    )
                self._store_v11_dtw_predictions(experiment_id)
                if mantis_prediction:
                    self._run_04_directional_mantis_rf(
                        experiment_id,
                        mantis_prediction,
                        attempts,
                        profile,
                        resolved_device=device,
                        dask_client=dask_client,
                        settings=settings,
                    )
            if self.configuration.version != 12:
                return
            rows = [row for row in rows if not row[3].startswith("directional_")]
            if not rows:
                return
        if self.configuration.seasonal_period_tuning is not None:
            from .p04_04_seasonal_period_tuning import (
                run_distributed_tuned_forecasts,
                run_tuned_forecasts,
            )

            if dask_client is None:
                run_tuned_forecasts(self, experiment_id, rows, attempts)
            else:
                run_distributed_tuned_forecasts(
                    self,
                    experiment_id,
                    rows,
                    attempts,
                    dask_client,
                    settings,
                    profile,
                )
            return
        from .p04_01_forecast_flow import run_ordinary_forecast_flow
        from .p04_02_forecast_provider import (
            DistributedForecastProvider,
            ForecastSafetyPolicy,
            LocalAutoArimaProvider,
            LocalChronosProvider,
        )
        from .p04_03_forecast_storage import ForecastStorage

        storage = ForecastStorage(self, experiment_id, attempts)
        execution = self.configuration.execution
        paths = self.configuration.execution_paths
        distributed = dask_client is not None
        r_settings = {model: self.configuration.r_model_settings(model)
                      for model in self.config["models"] if model in R_FORECAST_METHODS}
        if distributed:
            chronos = self.config["models"].get("chronos_2", {})
            safety = ForecastSafetyPolicy.from_profile(profile, platform.node())
            if any(row[3] == "chronos_2" for row in rows) and safety.accelerator["minimum_available_gib"] <= 0:
                raise RuntimeError("distributed Chronos requires an approved positive GPU headroom policy")
            provider = DistributedForecastProvider(
                r_settings.get("auto_arima", {}),
                paths.get("r_forecast_worker", paths["r_auto_arima_worker"]),
                float(execution["worker_timeouts_seconds"]["r"]),
                int(execution["thread_limits"]["r"]),
                chronos,
                tuple(self.quantiles),
                device,
                int(execution["thread_limits"]["chronos"]),
                paths["chronos_environment"],
                paths["chronos_worker"],
                float(execution["worker_timeouts_seconds"]["chronos_startup"]),
                float(execution["worker_timeouts_seconds"]["chronos_request"]),
                safety,
                r_settings,
            )
            auto_provider = chronos_provider = provider
        else:
            auto_provider = LocalAutoArimaProvider.from_configuration(self.configuration)
            chronos_provider = LocalChronosProvider(
                self.root,
                self.config["models"].get("chronos_2", {}),
                tuple(self.quantiles),
                device,
                profile,
                execution,
                paths,
            )
        run_ordinary_forecast_flow(
            scheduler_address=settings.dask_scheduler_address if settings else None,
            storage=storage,
            rows=rows,
            auto_provider=auto_provider,
            chronos_provider=chronos_provider,
            auto_batch_size=int(execution["batch_sizes"].get("r_forecast", execution["batch_sizes"]["auto_arima"])),
            chronos_batch_size=profile.chronos_inference_batch_size,
            max_in_flight=(settings.dask_max_in_flight if distributed else profile.autoarima_workers),
            autoarima_max_in_flight=(profile.dask_autoarima_max_in_flight
                                    if distributed else profile.autoarima_workers),
            ets_max_in_flight=profile.dask_ets_max_in_flight if distributed else profile.autoarima_workers,
            cpu_gpu_overlap=profile.cpu_gpu_overlap,
            distributed=distributed,
            retries=settings.dask_retries if settings is not None else int(execution["dask_retries"]),
        )

    def _directional_reference_library(
        self,
    ) -> tuple[dict[str, Any], str, dict[str, Any]]:
        """Load the complete accepted S1 training library from the linked child."""
        run = self.connection.execute(
            """SELECT r.preparation_id, r.child_database, r.definition_hash,
                      r.membership_fingerprint
               FROM window_preparation_runs AS r
               WHERE r.status='completed'
               ORDER BY r.completed_at DESC LIMIT 1"""
        ).fetchone()
        if run is None:
            raise RuntimeError("directional DTW requires completed window preparation")
        child_path = (self.database_path.parent / run[1]).resolve()
        series_identities = {
            int(row[0]): f"{row[1]}/{row[2]}"
            for row in self.connection.execute(
                """SELECT sl.series_key, dl.dataset_id, sl.series_id
                   FROM series_lookup AS sl
                   JOIN dataset_lookup AS dl ON dl.dataset_key=sl.dataset_key"""
            ).fetchall()
        }
        child = duckdb.connect(str(child_path), read_only=True)
        try:
            rows = child.execute(
                """SELECT w.window_id, w.series_key, w.transformed_input, l.labels,
                          w.transformed_hash
                   FROM prepared_windows AS w
                   JOIN series_membership AS m
                     ON m.preparation_id=w.preparation_id
                    AND m.series_key=w.series_key
                   JOIN window_directional_labels AS l ON l.window_id=w.window_id
                   WHERE w.preparation_id=? AND m.split_id='S1'
                     AND m.partition='train'
                   ORDER BY w.window_id""",
                [run[0]],
            ).fetchall()
            metadata = child.execute(
                """SELECT p.parent_scientific_hash, p.parent_configuration_hash,
                          p.definition_hash, p.source_manifest_hash
                   FROM preparation_metadata AS p WHERE p.preparation_id=?""",
                [run[0]],
            ).fetchone()
        finally:
            child.close()
        references = []
        for window_id, series_key, values, labels, transformed_hash in rows:
            if series_key not in series_identities:
                raise RuntimeError("directional reference has missing stable series identity")
            if (
                len(values) != 64
                or len(labels) != 14
                or any(value is None for value in labels)
                or json_fingerprint(values) != transformed_hash
            ):
                raise RuntimeError("directional reference library failed value validation")
            references.append(
                {
                    "identity": window_id,
                    "source_series_identity": series_identities[series_key],
                    "values": list(values),
                    "labels": [int(value) for value in labels],
                }
            )
        if not references:
            raise RuntimeError("directional S1 training reference library is empty")
        payload = {"references": references}
        fingerprint = json_fingerprint(payload)
        preparation = {
            "preparation_id": run[0],
            "definition_hash": run[2],
            "membership_fingerprint": run[3],
            "parent_scientific_hash": metadata[0],
            "parent_configuration_hash": metadata[1],
            "child_definition_hash": metadata[2],
            "source_manifest_hash": metadata[3],
            "reference_count": len(references),
        }
        return payload, fingerprint, preparation

    def _directional_implementation(
        self,
        reference_fingerprint: str,
        reference_preparation: dict[str, Any],
    ) -> dict[str, Any]:
        """Build the calculation-only implementation and accepted-input fingerprint."""
        paths = self.configuration.execution_paths
        worker_key = (
            "directional_dtw_training_worker"
            if self.configuration.version >= 11
            else "directional_dtw_worker"
        )
        worker_path = self.root / paths[worker_key]
        worker_relative_path = str(worker_path.relative_to(self.root))
        lock_path = self.root / paths["classifiers_lock"]
        source_paths = {
            worker_relative_path: worker_path,
            str(Path(__file__).resolve().relative_to(self.root)): Path(__file__).resolve(),
            "src/python/util/shared_transformations.py": self.root
            / "src/python/util/shared_transformations.py",
            "src/python/util/shared_labels.py": self.root
            / "src/python/util/shared_labels.py",
            "src/python/util/window_preparation.py": self.root
            / "src/python/util/window_preparation.py",
        }
        source_hashes = {
            relative_path: hashlib.sha256(path.read_bytes()).hexdigest()
            for relative_path, path in source_paths.items()
        }
        completed = subprocess.run(
            [
                str(self.root / paths["classifiers_environment"] / "bin/python"),
                str(worker_path),
                "describe",
            ],
            cwd=self.root,
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
        description = json.loads(completed.stdout)
        expected_runtime = {
            "aeon": "1.6.0",
            "numpy": "2.0.2",
            "numba": "0.61.2",
            "scikit-learn": "1.7.2",
        }
        if any(description["runtime"].get(key) != value for key, value in expected_runtime.items()):
            raise RuntimeError("classifier runtime does not match the locked DTW contract")
        runtime_versions = {
            package: description["runtime"][package]
            for package in expected_runtime
        }
        revision = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=self.root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        official_rows = self.connection.execute(
            """SELECT i.evaluation_input_id, i.content_hash
               FROM directional_evaluation_inputs AS i
               ORDER BY i.evaluation_input_id"""
        ).fetchall()
        preparation_fingerprint = json_fingerprint(
            {
                "reference_preparation": reference_preparation,
                "official_inputs": [list(row) for row in official_rows],
            }
        )
        implementation = {
            "repository_revision": revision,
            "scientific_source_fingerprint": json_fingerprint(source_hashes),
            "scientific_source_files": source_hashes,
            "worker_file_fingerprint": source_hashes[worker_relative_path],
            "classifier_lock_fingerprint": hashlib.sha256(lock_path.read_bytes()).hexdigest(),
            "runtime_versions": runtime_versions,
            "numeric_dtype": "float64",
            "reference_library_fingerprint": reference_fingerprint,
            "preparation_fingerprint": preparation_fingerprint,
        }
        implementation["implementation_fingerprint"] = json_fingerprint(implementation)
        return implementation

    def _run_directional_blocks(
        self,
        operation: str,
        batches: list[list[dict[str, Any]]],
        reference_fingerprint: str,
        workers: int,
        dask_client: Any,
        settings: ExecutionSettings | None,
        profile: ExecutionProfile,
    ) -> list[tuple[list[dict[str, Any]], dict[str, Any]]]:
        """Execute validated calibration or prediction blocks under Prefect/Dask."""
        from .shared_workflow_orchestration import run_gate_compute_flow

        execution = self.configuration.execution
        paths = self.configuration.execution_paths
        worker_script = (
            paths["directional_dtw_prediction_worker"]
            if self.configuration.version >= 11 and operation == "prediction"
            else paths.get("directional_dtw_training_worker", paths.get("directional_dtw_worker"))
        )
        outcomes = run_gate_compute_flow(
            process_id=4,
            batches=batches,
            options={
                "operation": operation,
                "classifier_environment": paths["classifiers_environment"],
                "worker_script": worker_script,
                "reference_fingerprint": reference_fingerprint,
                "model_storage": (
                    self.configuration.model_storage
                    if self.configuration.version >= 11
                    else None
                ),
                "frequency": (
                    self.configuration.resolved["data"]["benchmark"]["frequency"]
                    if self.configuration.version >= 11
                    else None
                ),
                "timeout": float(
                    execution["worker_timeouts_seconds"]["directional_dtw"]
                ),
                "memory_min_available_gib": {
                    "mac": profile.coordinator_memory_floor(),
                    "ubuntu": profile.accelerator_host_memory_floor(),
                    "coordinator": profile.coordinator_memory_floor(),
                    "accelerator_host": profile.accelerator_host_memory_floor(),
                    "by_hostname": profile.memory_floors_by_hostname(),
                },
                "swap_growth_limit_gib": float(
                    profile.dask_swap_growth_limit_gib or 0.25
                ),
            },
            scheduler_address=(dask_client.scheduler.address if dask_client else None),
            retries=settings.dask_retries if settings else 0,
            max_in_flight=settings.dask_max_in_flight if settings else workers,
            local_workers=workers,
        )
        completed = []
        for outcome in outcomes:
            if "response" not in outcome:
                raise RuntimeError(outcome["error"])
            response = outcome["response"]
            runtime = response.get("runtime", {})
            for package, version in {
                "aeon": "1.6.0",
                "numpy": "2.0.2",
                "numba": "0.61.2",
                "scikit-learn": "1.7.2",
            }.items():
                if runtime.get(package) != version:
                    raise RuntimeError("DTW block runtime differs from the locked environment")
            if runtime.get("numeric_dtype") != "float64" or runtime.get("numba_threads") != 1:
                raise RuntimeError("DTW block violated numeric dtype or thread controls")
            completed.append((outcome["batch"], response))
        return completed

    def _run_04_directional_dtw(
        self,
        experiment_id: str,
        rows: list[tuple],
        attempts: dict[str, int],
        profile: ExecutionProfile,
        dask_client: Any,
        settings: ExecutionSettings | None,
    ) -> None:
        """Calibrate per-horizon widths and predict every pending official identity."""
        from .shared_distributed_execution import install_immutable_reference_cache

        payload, reference_fingerprint, reference_preparation = (
            self._directional_reference_library()
        )
        if dask_client is None:
            cache_reports = {
                "local": install_immutable_reference_cache(
                    reference_fingerprint, payload
                )
            }
        else:
            cache_reports = dask_client.run(
                install_immutable_reference_cache,
                reference_fingerprint,
                payload,
            )
            cache_reports["coordinator"] = install_immutable_reference_cache(
                reference_fingerprint, payload
            )
        if any(
            report.get("fingerprint") != reference_fingerprint
            for report in cache_reports.values()
        ):
            raise RuntimeError("not every worker accepted the reference library fingerprint")
        implementation = self._directional_implementation(
            reference_fingerprint, reference_preparation
        )
        variant_id = next(row[2] for row in rows if row[2] is not None)
        model_science = self.config["models"]["directional_dtw"]
        model_identity = {
            "experiment": experiment_id,
            "variant": variant_id,
            "definition": model_science,
            "implementation": implementation["implementation_fingerprint"],
        }
        model_id = "directional-model/" + json_fingerprint(model_identity)[:32]
        model_content = {
            "experiment_id": experiment_id,
            "variant_id": variant_id,
            "scientific_definition": canonical_json(model_science),
            "repository_revision": implementation["repository_revision"],
            "scientific_source_fingerprint": implementation[
                "scientific_source_fingerprint"
            ],
            "worker_file_fingerprint": implementation["worker_file_fingerprint"],
            "classifier_lock_fingerprint": implementation[
                "classifier_lock_fingerprint"
            ],
            "runtime_versions": canonical_json(implementation["runtime_versions"]),
            "numeric_dtype": "float64",
            "reference_library_fingerprint": reference_fingerprint,
            "preparation_fingerprint": implementation["preparation_fingerprint"],
            "implementation_fingerprint": implementation[
                "implementation_fingerprint"
            ],
        }
        model_record = {
            "model_definition_id": model_id,
            **model_content,
            "content_hash": json_fingerprint(model_content),
        }
        training_candidate = (
            "directional_dtw:train"
            if self.configuration.version >= 11
            else "directional_dtw:calibration"
        )
        calibration_row = next(
            (row for row in rows if row[3] == training_candidate), None
        )
        proportions = model_science["constraint"]["candidate_proportions"]
        width_proportions = {}
        for proportion in proportions:
            width_proportions.setdefault(int(proportion * 64), proportion)
        if set(width_proportions) != set(range(64)):
            raise RuntimeError("configured candidates do not cover all 64 effective widths")
        if calibration_row is not None and self.configuration.version >= 11:
            stored_widths = tuple(
                (int(row[0]), int(row[1]))
                for row in self.connection.execute(
                    """SELECT s.horizon, s.effective_width
                       FROM directional_selected_widths AS s
                       WHERE s.model_definition_id=? ORDER BY s.horizon""",
                    [model_id],
                ).fetchall()
            )
            stored_score_count = int(
                self.connection.execute(
                    """SELECT count(*) FROM directional_calibration_scores AS s
                       WHERE s.model_definition_id=?""",
                    [model_id],
                ).fetchone()[0]
            )
            if (
                tuple(horizon for horizon, _ in stored_widths)
                == tuple(range(1, 15))
                and stored_score_count == 64 * 14
            ):
                artifact_evidence = self._publish_directional_dtw_model(
                    tuple(width for _, width in stored_widths), reference_fingerprint
                )
                self._insert_or_verify("directional_model_definitions", model_record)
                self._commit_task(
                    calibration_row[0],
                    attempts[calibration_row[0]],
                    0.0,
                    lambda: None,
                    {
                        "calibration_reused": True,
                        "stored_calibration_score_count": stored_score_count,
                        "fitted_artifact": artifact_evidence,
                    },
                )
                calibration_row = None
        if calibration_row is not None:
            query_ids = [reference["identity"] for reference in payload["references"]]
            block_size = int(
                self.configuration.execution["batch_sizes"]["directional_calibration"]
            )
            blocks = [
                {
                    "id": f"calibration/w{width:02d}/q{offset:04d}",
                    "effective_width": width,
                    "query_identities": query_ids[offset : offset + block_size],
                }
                for width in range(64)
                for offset in range(0, len(query_ids), block_size)
            ]
            responses = self._run_directional_blocks(
                "calibration",
                [[block] for block in blocks],
                reference_fingerprint,
                max(profile.autoarima_workers, 1),
                dask_client,
                settings,
                profile,
            )
            totals = {
                width: {horizon: [0, 0] for horizon in range(1, 15)}
                for width in range(64)
            }
            workers_used: dict[str, int] = {}
            resource_evidence: dict[str, dict[str, int]] = {}
            runtime = 0.0
            for _, response in responses:
                runtime += float(response["runtime_seconds"])
                hostname = response["worker"]["hostname"]
                workers_used[hostname] = workers_used.get(hostname, 0) + len(
                    response["results"]
                )
                usage = response["resource_usage"]
                host_evidence = resource_evidence.setdefault(
                    hostname,
                    {
                        "minimum_available_memory_bytes": min(
                            usage["available_memory_bytes_before"],
                            usage["available_memory_bytes_after"],
                        ),
                        "peak_child_rss_bytes": 0,
                        "maximum_swap_growth_bytes": 0,
                        "peak_dask_spilled_memory_bytes": 0,
                        "peak_dask_spilled_disk_bytes": 0,
                    },
                )
                host_evidence["minimum_available_memory_bytes"] = min(
                    host_evidence["minimum_available_memory_bytes"],
                    usage["available_memory_bytes_before"],
                    usage["available_memory_bytes_after"],
                )
                for target, source in (
                    ("peak_child_rss_bytes", "child_peak_rss_bytes"),
                    ("maximum_swap_growth_bytes", "swap_growth_bytes"),
                    ("peak_dask_spilled_memory_bytes", "dask_spilled_memory_bytes"),
                    ("peak_dask_spilled_disk_bytes", "dask_spilled_disk_bytes"),
                ):
                    host_evidence[target] = max(
                        host_evidence[target], int(usage[source])
                    )
                for result in response["results"]:
                    width = int(result["effective_width"])
                    for horizon, (correct, evaluated) in enumerate(
                        zip(result["correct"], result["evaluated"], strict=True), 1
                    ):
                        totals[width][horizon][0] += int(correct)
                        totals[width][horizon][1] += int(evaluated)
            if any(
                totals[width][horizon][1] != len(query_ids)
                for width in range(64)
                for horizon in range(1, 15)
            ):
                raise RuntimeError("calibration did not score every reference and horizon")
            policy = {
                "candidate_proportions": proportions,
                "effective_width_mapping": {
                    str(width): width_proportions[width] for width in range(64)
                },
                "calibration_exclusion": "same_source_series",
                "calibration_tie_rule": "smallest_effective_width",
                "neighbour_tie_rule": "lowest_stable_reference_identity",
                "reference_library_fingerprint": reference_fingerprint,
            }
            score_records = []
            selected_records = []
            for horizon in range(1, 15):
                selected_width = max(
                    range(64),
                    key=lambda width: (
                        totals[width][horizon][0]
                        / totals[width][horizon][1],
                        -width,
                    ),
                )
                for width in range(64):
                    correct, evaluated = totals[width][horizon]
                    content = {
                        "model_definition_id": model_id,
                        "effective_width": width,
                        "representative_proportion": width_proportions[width],
                        "horizon": horizon,
                        "correct_count": correct,
                        "evaluation_count": evaluated,
                        "accuracy": correct / evaluated,
                        "candidate_policy": canonical_json(policy),
                    }
                    score_records.append(
                        {
                            "calibration_score_id": "directional-score/"
                            + json_fingerprint(
                                {"model": model_id, "width": width, "horizon": horizon}
                            )[:32],
                            **content,
                            "content_hash": json_fingerprint(content),
                        }
                    )
                correct, evaluated = totals[selected_width][horizon]
                selected_content = {
                    "model_definition_id": model_id,
                    "horizon": horizon,
                    "effective_width": selected_width,
                    "representative_proportion": width_proportions[selected_width],
                    "calibration_accuracy": correct / evaluated,
                    "tie_rule": "smallest_effective_width",
                }
                selected_records.append(
                    {
                        "selected_width_id": "directional-width/"
                        + json_fingerprint({"model": model_id, "horizon": horizon})[:32],
                        **selected_content,
                        "content_hash": json_fingerprint(selected_content),
                    }
                )

            def insert_calibration() -> None:
                self._insert_or_verify("directional_model_definitions", model_record)
                for record in score_records:
                    self._insert_or_verify("directional_calibration_scores", record)
                for record in selected_records:
                    self._insert_or_verify("directional_selected_widths", record)

            artifact_evidence = None
            if self.configuration.version >= 11:
                artifact_evidence = self._publish_directional_dtw_model(
                    tuple(record["effective_width"] for record in selected_records),
                    reference_fingerprint,
                )

            self._commit_task(
                calibration_row[0],
                attempts[calibration_row[0]],
                runtime,
                insert_calibration,
                {
                    "worker_block_counts": workers_used,
                    "resource_evidence": resource_evidence,
                    "reference_cache": cache_reports,
                    "distance_calculations": sum(
                        len(query_ids)
                        - sum(
                            1
                            for candidate in payload["references"]
                            if candidate["source_series_identity"]
                            == query["source_series_identity"]
                        )
                        for query in payload["references"]
                    )
                    * 64,
                    "fitted_artifact": artifact_evidence,
                },
            )
        else:
            if self.connection.execute(
                """SELECT count(*) FROM directional_model_definitions AS m
                   WHERE m.model_definition_id=?""",
                [model_id],
            ).fetchone()[0] != 1:
                raise RuntimeError(
                    "completed calibration is missing its directional model definition"
                )
            self._insert_or_verify("directional_model_definitions", model_record)

        selected = {
            int(row[0]): int(row[1])
            for row in self.connection.execute(
                """SELECT s.horizon, s.effective_width
                   FROM directional_selected_widths AS s
                   WHERE s.model_definition_id=? ORDER BY s.horizon""",
                [model_id],
            ).fetchall()
        }
        if set(selected) != set(range(1, 15)):
            raise RuntimeError("directional prediction requires fourteen selected widths")
        prediction_rows = [row for row in rows if row[1] is not None]
        if not prediction_rows:
            return
        task_by_identity = {}
        input_rows = {}
        for row in prediction_rows:
            horizon = int(row[3].rsplit("h", 1)[1])
            input_row = self.connection.execute(
                """SELECT i.evaluation_input_id, i.transformed_input
                   FROM directional_evaluation_inputs AS i
                   WHERE i.experiment_id=? AND i.forecast_instance_id=?
                     AND i.variant_id=?""",
                [experiment_id, row[1], row[2]],
            ).fetchone()
            if input_row is None:
                raise RuntimeError("directional prediction has missing official input")
            input_rows[input_row[0]] = list(input_row[1])
            task_by_identity[(input_row[0], horizon)] = row
        grouped: dict[tuple[int, tuple[int, ...]], list[str]] = {}
        by_input_horizons: dict[tuple[int, str], list[int]] = {}
        for input_id, horizon in task_by_identity:
            by_input_horizons.setdefault((selected[horizon], input_id), []).append(horizon)
        for (width, input_id), horizons in by_input_horizons.items():
            grouped.setdefault((width, tuple(sorted(horizons))), []).append(input_id)
        query_block = int(
            self.configuration.execution["batch_sizes"]["directional_prediction"]
        )
        prediction_jobs = []
        for (width, horizons), input_ids in sorted(grouped.items()):
            for offset in range(0, len(input_ids), query_block):
                selected_inputs = sorted(input_ids)[offset : offset + query_block]
                prediction_jobs.append(
                    {
                        "id": f"prediction/w{width:02d}/h{'-'.join(map(str, horizons))}/q{offset:04d}",
                        "effective_width": width,
                        "horizons": list(horizons),
                        "queries": [
                            {"identity": identity, "values": input_rows[identity]}
                            for identity in selected_inputs
                        ],
                    }
                )
        responses = self._run_directional_blocks(
            "prediction",
            [[job] for job in prediction_jobs],
            reference_fingerprint,
            max(profile.autoarima_workers, 1),
            dask_client,
            settings,
            profile,
        )
        for batch, response in responses:
            if len(batch) != 1:
                raise RuntimeError("directional prediction response must own one block")
            block = batch[0]
            block_provenance = {
                "block_id": block["id"],
                "distance_calculations": len(block["queries"])
                * len(payload["references"]),
            }
            predictions = [
                prediction
                for result in response["results"]
                for prediction in result["predictions"]
            ]
            for prediction in predictions:
                key = (
                    prediction["evaluation_input_identity"],
                    int(prediction["horizon"]),
                )
                row = task_by_identity.pop(key, None)
                if row is None:
                    raise RuntimeError("directional worker returned an unexpected prediction")
                if (
                    prediction["prediction"] not in {0, 1}
                    or not math.isfinite(prediction["nearest_distance"])
                    or prediction["effective_width"] != selected[key[1]]
                ):
                    raise RuntimeError("directional worker returned an invalid prediction")
                content = {
                    "experiment_id": experiment_id,
                    "model_definition_id": model_id,
                    "evaluation_input_id": key[0],
                    "horizon": key[1],
                    "prediction": int(prediction["prediction"]),
                    "nearest_reference_identity": prediction[
                        "nearest_reference_identity"
                    ],
                    "nearest_distance": float(prediction["nearest_distance"]),
                    "effective_width": int(prediction["effective_width"]),
                    "execution_metadata": canonical_json(
                        {
                            "worker": response["worker"],
                            "runtime": response["runtime"],
                            "resource_usage": response["resource_usage"],
                            **block_provenance,
                            "reference_library_fingerprint": reference_fingerprint,
                        }
                    ),
                }
                record = {
                    "prediction_id": "directional-prediction/"
                    + json_fingerprint(
                        {"model": model_id, "input": key[0], "horizon": key[1]}
                    )[:32],
                    **content,
                    "content_hash": json_fingerprint(content),
                }

                def insert_prediction(record=record) -> None:
                    self._insert_or_verify("directional_predictions", record)

                self._commit_task(
                    row[0],
                    attempts[row[0]],
                    response["runtime_seconds"] / max(len(predictions), 1),
                    insert_prediction,
                    {
                        "worker": response["worker"],
                        "resource_usage": response["resource_usage"],
                        **block_provenance,
                    },
                )
        if task_by_identity:
            raise RuntimeError("directional worker omitted accepted prediction identities")

    def _publish_directional_dtw_model(
        self, selected_widths: tuple[int, ...], reference_fingerprint: str
    ) -> dict[str, Any]:
        """Publish the complete fitted DTW object on the authoritative Mac model store."""
        from .shared_distributed_execution import install_immutable_reference_cache

        payload, observed_fingerprint, _ = self._directional_reference_library()
        if observed_fingerprint != reference_fingerprint:
            raise RuntimeError("DTW reference library changed before fitted-model publication")
        cache = install_immutable_reference_cache(reference_fingerprint, payload)["path"]
        paths = self.configuration.execution_paths
        storage = self.configuration.model_storage
        arguments = [
            str(self.root / paths["classifiers_environment"] / "bin/python"),
            str(self.root / paths["directional_dtw_training_worker"]),
            "publish",
            "--reference-cache", cache,
            "--reference-fingerprint", reference_fingerprint,
            "--model-root", str(self.root / storage["root"]),
            "--experiment", storage["experiment"],
            "--frequency", self.configuration.resolved["data"]["benchmark"]["frequency"],
        ]
        if storage["overwrite"]:
            arguments.append("--overwrite")
        completed = subprocess.run(
            arguments,
            cwd=self.root,
            input=canonical_json({"selected_widths": list(selected_widths)}),
            check=True,
            capture_output=True,
            text=True,
            timeout=float(
                self.configuration.execution["worker_timeouts_seconds"]["directional_dtw"]
            ),
        )
        response = json.loads(completed.stdout)
        if response.get("operation") != "publish":
            raise RuntimeError("DTW training worker did not publish fitted state")
        return dict(response["artifact"])

    @staticmethod
    def _secure_ssh_arguments(host: str, command: str | None = None) -> list[str]:
        """Build strict noninteractive SSH/SCP options for the configured Ubuntu host."""
        arguments = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
                     "-o", "StrictHostKeyChecking=yes"]
        alias = os.environ.get("SHAPEFM_SSH_HOST_KEY_ALIAS")
        if alias:
            arguments.extend(["-o", f"HostKeyAlias={alias}"])
        if command is None:
            return arguments
        return ["ssh", *arguments, host, command]

    def _consolidate_classifier_artifact(
        self, evidence: dict[str, Any], worker: dict[str, Any], horizon: int
    ) -> dict[str, Any]:
        """Adopt a remote staged RF file into the authoritative Mac model store."""
        from .shared_model_storage import ModelStorage

        settings = self.configuration.model_storage
        storage = ModelStorage(self.root / settings["root"], settings["experiment"])
        local_path = storage.path("directional_mantis_rf", "D", horizon)
        worker_host = str(worker.get("hostname", "")).split(".", 1)[0].lower()
        local_host = platform.node().split(".", 1)[0].lower()
        if worker_host == local_host and local_path.is_file():
            return evidence
        if local_path.is_file() and not settings["overwrite"]:
            return {**storage.inspect("directional_mantis_rf", "D", horizon),
                    "status": "skipped_existing"}
        if self._active_machine_environment is None:
            raise RuntimeError("remote artifact consolidation requires resolved machines")
        matches = [
            item
            for item in self._active_machine_environment.remotes
            if item.machine.hostname.split(".", 1)[0].lower() == worker_host
        ]
        if len(matches) != 1:
            raise RuntimeError(
                f"cannot resolve artifact worker hostname {worker.get('hostname')!r}"
            )
        machine = matches[0].machine
        host = machine.ssh_target
        remote = Path(machine.project_root) / settings["root"] / evidence["relative_path"]
        incoming = self.root / ".amp/in" / f"rf-h{horizon:02d}-{uuid.uuid4().hex}.joblib"
        incoming.parent.mkdir(parents=True, exist_ok=True)
        try:
            subprocess.run(
                ["scp", *self._secure_ssh_arguments(host), f"{host}:{remote}", str(incoming)],
                cwd=self.root, check=True, capture_output=True, text=True, timeout=300,
            )
            if (
                incoming.stat().st_size != evidence.get("size_bytes")
                or hashlib.sha256(incoming.read_bytes()).hexdigest()
                != evidence.get("sha256")
            ):
                raise RuntimeError(
                    f"transferred Random Forest artifact differs for horizon {horizon}"
                )
            return storage.adopt(
                incoming, "directional_mantis_rf", "D", horizon,
                size_bytes=evidence["size_bytes"], sha256=evidence["sha256"],
                overwrite=settings["overwrite"],
            )
        finally:
            incoming.unlink(missing_ok=True)

    def _synchronize_model_directory_to_ubuntu(self) -> None:
        """Copy authoritative fitted models to every enabled remote before prediction."""
        settings = self.configuration.model_storage
        source = self.root / settings["root"] / settings["experiment"]
        if not source.is_dir():
            raise RuntimeError("authoritative fitted-model directory is missing")
        if self._active_machine_environment is None:
            raise RuntimeError("model synchronization requires resolved machines")
        for resolved in self._active_machine_environment.remotes:
            machine = resolved.machine
            host = machine.ssh_target
            remote_root = Path(machine.project_root) / settings["root"]
            subprocess.run(
                self._secure_ssh_arguments(
                    host, f"mkdir -p {shlex.quote(str(remote_root))}"
                ),
                cwd=self.root, check=True, capture_output=True, text=True, timeout=60,
            )
            subprocess.run(
                ["scp", *self._secure_ssh_arguments(host), "-r", str(source),
                 f"{host}:{remote_root}/"],
                cwd=self.root, check=True, capture_output=True, text=True, timeout=600,
            )

    def _store_v11_dtw_predictions(self, experiment_id: str) -> None:
        """Mirror accepted DTW outputs into the additive model-neutral contract."""
        rows = self.connection.execute(
            """SELECT p.prediction_id, p.model_definition_id, p.evaluation_input_id,
                      p.horizon, p.prediction, p.nearest_reference_identity,
                      p.nearest_distance, p.effective_width,
                      m.reference_library_fingerprint, m.preparation_fingerprint,
                      m.implementation_fingerprint
               FROM directional_predictions AS p
               JOIN directional_model_definitions AS m
                 ON m.model_definition_id=p.model_definition_id
               WHERE p.experiment_id=? ORDER BY p.prediction_id""",
            [experiment_id],
        ).fetchall()
        for row in rows:
            output_fingerprint = json_fingerprint(
                {
                    "prediction": int(row[4]),
                    "nearest_reference_identity": row[5],
                    "nearest_distance": float(row[6]),
                    "effective_width": int(row[7]),
                }
            )
            run_id = "dtw-run/" + json_fingerprint(
                {"model": row[1], "horizon": int(row[3])}
            )[:32]
            scientific = {
                "experiment_id": experiment_id,
                "model_definition_id": row[1],
                "evaluation_input_id": row[2],
                "horizon": int(row[3]),
                "prediction": int(row[4]),
                "classifier_run_id": run_id,
                "training_fingerprint": row[8],
                "evaluation_fingerprint": row[9],
                "output_fingerprint": output_fingerprint,
            }
            common = {
                "prediction_id": row[0],
                **scientific,
                "content_hash": json_fingerprint(scientific),
            }
            lineage_content = {
                "nearest_reference_identity": row[5],
                "nearest_distance": float(row[6]),
                "effective_width": int(row[7]),
            }
            lineage = {
                "prediction_id": row[0],
                **lineage_content,
                "content_hash": json_fingerprint(lineage_content),
            }
            self._insert_or_verify("model_directional_predictions", common)
            self._insert_or_verify("directional_dtw_prediction_lineage", lineage)

    def _directional_mantis_inputs(self) -> tuple[list[Any], str, str]:
        """Load common Process-03 S1 and official values without another split."""
        from .p04_05_directional_mantis import PreparedDirectionalInput

        run = self.connection.execute(
            """SELECT preparation_id, child_database, definition_hash,
                      membership_fingerprint
               FROM window_preparation_runs WHERE status='completed'
               ORDER BY completed_at DESC LIMIT 1"""
        ).fetchone()
        if run is None:
            raise RuntimeError("Mantis requires completed common window preparation")
        series_identities = {
            int(row[0]): f"{row[1]}/{row[2]}"
            for row in self.connection.execute(
                """SELECT sl.series_key, dl.dataset_id, sl.series_id
                   FROM series_lookup AS sl
                   JOIN dataset_lookup AS dl ON dl.dataset_key=sl.dataset_key"""
            ).fetchall()
        }
        child = duckdb.connect(
            str((self.database_path.parent / run[1]).resolve()), read_only=True
        )
        try:
            training_rows = child.execute(
                """SELECT w.window_id, w.series_key, w.transformed_input,
                          w.transformed_hash, l.labels, l.reference_value
                   FROM prepared_windows AS w
                   JOIN series_membership AS m
                     ON m.preparation_id=w.preparation_id
                    AND m.series_key=w.series_key
                   JOIN window_directional_labels AS l ON l.window_id=w.window_id
                   WHERE w.preparation_id=? AND m.split_id='S1'
                     AND m.partition='train' ORDER BY w.window_id""",
                [run[0]],
            ).fetchall()
        finally:
            child.close()
        inputs = [
            PreparedDirectionalInput(
                input_id=row[0],
                source_series_id=series_identities[int(row[1])],
                role="training",
                values=tuple(row[2]),
                label_reference=float(row[5]),
                labels=tuple(int(value) for value in row[4]),
                preparation_definition_id=run[2],
                preparation_fingerprint=run[2],
                membership_fingerprint=run[3],
                input_fingerprint=row[3],
            )
            for row in training_rows
        ]
        official_rows = self.connection.execute(
            """SELECT i.evaluation_input_id, f.series_id, i.transformed_input,
                      i.label_reference, i.preparation_id,
                      i.preparation_fingerprint, i.transformed_input_hash
               FROM directional_evaluation_inputs AS i
               JOIN forecast_instances AS f
                 ON f.forecast_instance_id=i.forecast_instance_id
               ORDER BY i.evaluation_input_id"""
        ).fetchall()
        inputs.extend(
            PreparedDirectionalInput(
                input_id=row[0],
                source_series_id=str(row[1]),
                role="official_evaluation",
                values=tuple(row[2]),
                label_reference=float(row[3]),
                labels=None,
                preparation_definition_id=row[4],
                preparation_fingerprint=row[5],
                membership_fingerprint=run[3],
                input_fingerprint=row[6],
            )
            for row in official_rows
        )
        if len(inputs) != len(training_rows) + self.configuration.series_count:
            raise RuntimeError("common Mantis input set is incomplete")
        return inputs, run[2], run[3]

    def _run_04_directional_mantis_rf(
        self,
        experiment_id: str,
        rows: list[tuple],
        attempts: dict[str, int],
        profile: ExecutionProfile,
        resolved_device: str,
        dask_client: Any,
        settings: ExecutionSettings | None,
    ) -> None:
        """Encode common inputs once and run fourteen independent RF horizons."""
        from .p04_05_directional_mantis import (
            DirectionalMantisComposer,
            RepresentationRecord,
        )
        from .shared_classification import (
            ClassificationDataset,
            ClassificationResponse,
            ClassifierSpec,
            RandomForestClassifierProvider,
        )
        from .shared_distributed_execution import (
            install_immutable_classification_dataset,
        )
        from .shared_workflow_orchestration import run_gate_compute_flow

        inputs, preparation_fingerprint, membership_fingerprint = (
            self._directional_mantis_inputs()
        )
        representation_science = self.config["representations"][
            "mantis_8m_legacy_cls"
        ]
        representation_definition_id = (
            "representation-definition/"
            + json_fingerprint(representation_science)[:32]
        )
        representation_definition_content = {
            "experiment_id": experiment_id,
            "scientific_definition": canonical_json(representation_science),
        }
        representation_definition_record = {
            "representation_definition_id": representation_definition_id,
            **representation_definition_content,
            "content_hash": json_fingerprint(representation_definition_content),
        }
        classifier_science = self.config["classifiers"]["random_forest"]
        classifier_definition_id = classifier_science["classifier_id"]
        classifier_definition_content = {
            "experiment_id": experiment_id,
            "scientific_definition": canonical_json(classifier_science),
        }
        classifier_definition_record = {
            "classifier_definition_id": classifier_definition_id,
            **classifier_definition_content,
            "content_hash": json_fingerprint(classifier_definition_content),
        }
        variant_id = next(row[2] for row in rows if row[2] is not None)
        model_science = self.config["models"]["directional_mantis_rf"]
        model_identity = {
            "experiment": experiment_id,
            "variant": variant_id,
            "definition": model_science,
            "representation": representation_definition_id,
            "classifier": classifier_definition_id,
        }
        model_id = "directional-model/" + json_fingerprint(model_identity)[:32]
        model_content = {
            "experiment_id": experiment_id,
            "variant_id": variant_id,
            "representation_definition_id": representation_definition_id,
            "classifier_definition_id": classifier_definition_id,
            "scientific_definition": canonical_json(model_science),
            "preparation_fingerprint": preparation_fingerprint,
            "membership_fingerprint": membership_fingerprint,
        }
        model_record = {
            "model_definition_id": model_id,
            **model_content,
            "content_hash": json_fingerprint(model_content),
        }
        for table, record in (
            ("directional_representation_definitions", representation_definition_record),
            ("directional_classifier_definitions", classifier_definition_record),
            ("directional_composite_model_definitions", model_record),
        ):
            self._insert_or_verify(table, record)

        representation_task = next(
            (row for row in rows if row[3].endswith(":representations")), None
        )
        requests = DirectionalMantisComposer.representation_requests(
            inputs, representation_definition_id
        )
        if representation_task is not None:
            outcomes = list(
                run_gate_compute_flow(
                    process_id=4,
                    batches=[requests],
                    options={
                        "component": "mantis_representation",
                        "mantis_environment": self.configuration.execution_paths[
                            "mantis_environment"
                        ],
                        "worker_script": self.configuration.execution_paths["mantis_worker"],
                        "device": "cuda" if dask_client is not None else resolved_device,
                        "timeout": float(
                            self.configuration.execution["worker_timeouts_seconds"]["mantis"]
                        ),
                    },
                    scheduler_address=(dask_client.scheduler.address if dask_client else None),
                    retries=settings.dask_retries if settings else 0,
                    max_in_flight=1,
                    local_workers=1,
                )
            )
            if len(outcomes) != 1 or "response" not in outcomes[0]:
                raise RuntimeError("Mantis representation phase did not return one response")
            response = outcomes[0]["response"]
            records = tuple(
                RepresentationRecord.from_dict(value) for value in response["records"]
            )
            # This validates exact request identities and all common-input lineage.
            DirectionalMantisComposer.classification_dataset(
                inputs, records, representation_definition_id
            )
            if len(records) != len(requests):
                raise RuntimeError("Mantis omitted requested representations")

            def insert_representations() -> None:
                provenance = dict(records[0].worker_provenance)
                if any(dict(record.worker_provenance) != provenance for record in records):
                    raise RuntimeError("Mantis batch returned inconsistent worker provenance")
                provenance_fingerprint = json_fingerprint(provenance)
                execution_identity = {
                    "definition": representation_definition_id,
                    "provenance": provenance_fingerprint,
                }
                execution_id = "representation-execution/" + json_fingerprint(
                    execution_identity
                )[:32]
                self._insert_or_verify(
                    "directional_representation_executions",
                    {
                        "representation_execution_id": execution_id,
                        "representation_definition_id": representation_definition_id,
                        "worker_provenance": canonical_json(provenance),
                        "worker_provenance_fingerprint": provenance_fingerprint,
                    },
                )
                for record in records:
                    scientific = {
                        "representation_definition_id": record.definition_id,
                        "input_id": record.input_id,
                        "source_series_id": record.source_series_id,
                        "role": record.role,
                        "input_fingerprint": record.input_fingerprint,
                        "preparation_definition_id": record.preparation_definition_id,
                        "preparation_fingerprint": record.preparation_fingerprint,
                        "membership_fingerprint": record.membership_fingerprint,
                        "representation_values": list(record.values),
                        "representation_dtype": record.dtype,
                        "representation_dimension": record.dimension,
                        "representation_fingerprint": record.representation_fingerprint,
                    }
                    self._insert_or_verify(
                        "directional_representations",
                        {
                            "representation_id": record.representation_id,
                            **scientific,
                            "content_hash": json_fingerprint(scientific),
                        },
                    )
                    self._insert_or_verify(
                        "directional_representation_execution_members",
                        {
                            "representation_id": record.representation_id,
                            "representation_execution_id": execution_id,
                        },
                    )

            self._commit_task(
                representation_task[0],
                attempts[representation_task[0]],
                float(response["runtime_seconds"]),
                insert_representations,
                {"worker": response.get("worker"), "representation_count": len(records)},
            )
        stored = self.connection.execute(
            """SELECT representation_id, input_id, source_series_id, role,
                      representation_definition_id, representation_values,
                      representation_dtype, representation_dimension, input_fingerprint,
                      preparation_definition_id, preparation_fingerprint,
                      membership_fingerprint, representation_fingerprint
               FROM directional_representations
               WHERE representation_definition_id=? ORDER BY representation_id""",
            [representation_definition_id],
        ).fetchall()
        representations = tuple(
            RepresentationRecord(
                representation_id=row[0], input_id=row[1], source_series_id=row[2],
                role=row[3], definition_id=row[4], values=tuple(row[5]), dtype=row[6],
                dimension=int(row[7]), input_fingerprint=row[8],
                preparation_definition_id=row[9], preparation_fingerprint=row[10],
                membership_fingerprint=row[11], representation_fingerprint=row[12],
                worker_provenance={},
            )
            for row in stored
        )
        dataset = DirectionalMantisComposer.classification_dataset(
            inputs, representations, representation_definition_id
        )
        specification = ClassifierSpec.from_dict(classifier_science)
        jobs = DirectionalMantisComposer.classification_jobs(
            inputs, representations, dataset, specification
        )
        dataset_payload = dataset.to_dict()
        dataset_cache_fingerprint = json_fingerprint(dataset_payload)
        if dask_client is None:
            install_immutable_classification_dataset(
                dataset_cache_fingerprint, dataset_payload
            )
        else:
            reports = dask_client.run(
                install_immutable_classification_dataset,
                dataset_cache_fingerprint,
                dataset_payload,
            )
            if any(
                report["fingerprint"] != dataset_cache_fingerprint
                for report in reports.values()
            ):
                raise RuntimeError("classification dataset was not verified on every worker")
        training_by_horizon = {
            int(row[3].rsplit("h", 1)[1]): row
            for row in rows if ":train:h" in row[3]
        }
        prediction_by_horizon = {
            int(row[3].rsplit("h", 1)[1]): row
            for row in rows if ":predict:h" in row[3]
        }
        common_options = {
            "component": "random_forest_classifier",
            "classifier_environment": self.configuration.execution_paths[
                "classifiers_environment"
            ],
            "dataset_fingerprint": dataset_cache_fingerprint,
            "model_storage": self.configuration.model_storage,
            "frequency": self.configuration.resolved["data"]["benchmark"]["frequency"],
            "timeout": float(
                self.configuration.execution["worker_timeouts_seconds"]["classifier"]
            ),
        }
        if training_by_horizon:
            training_jobs = [job for job in jobs if job.horizon in training_by_horizon]
            outcomes = run_gate_compute_flow(
                process_id=4,
                batches=[[job.to_dict()] for job in training_jobs],
                options={
                    **common_options,
                    "operation": "train",
                    "worker_script": self.configuration.execution_paths[
                        "random_forest_training_worker"
                    ],
                },
                scheduler_address=(dask_client.scheduler.address if dask_client else None),
                retries=settings.dask_retries if settings else 0,
                max_in_flight=(settings.dask_max_in_flight if settings else max(profile.autoarima_workers, 1)),
                local_workers=max(profile.autoarima_workers, 1),
            )
            jobs_by_id = {job.job_id: job for job in training_jobs}
            for outcome in outcomes:
                if "response" not in outcome:
                    raise RuntimeError(outcome["error"])
                response = outcome["response"]
                if len(response.get("results", ())) != 1:
                    raise RuntimeError("Random Forest training worker must return one result")
                result = response["results"][0]
                job = jobs_by_id.get(result.get("job_id"))
                if (
                    job is None
                    or result.get("horizon") != job.horizon
                    or result.get("classifier_run_id")
                    != RandomForestClassifierProvider.classifier_run_id(job)
                    or result.get("classifier_id") != job.specification.classifier_id
                    or result.get("training_fingerprint") != job.training_fingerprint
                    or result.get("evaluation_fingerprint") != job.evaluation_fingerprint
                    or result.get("resolved_parameters") != dict(job.specification.parameters)
                ):
                    raise RuntimeError("Random Forest training result differs from its job")
                artifact = self._consolidate_classifier_artifact(
                    result["artifact"], response.get("worker", {}), job.horizon
                )
                task = training_by_horizon.pop(job.horizon)
                self._commit_task(
                    task[0], attempts[task[0]],
                    float(response["subprocess_runtime_seconds"]),
                    lambda: None,
                    {"worker": response.get("worker"), "fitted_artifact": artifact,
                     "training_provenance": response.get("worker_provenance")},
                )
            if training_by_horizon:
                raise RuntimeError("classifier workers omitted submitted training jobs")
        if not prediction_by_horizon:
            return
        prediction_jobs = [job for job in jobs if job.horizon in prediction_by_horizon]
        outcomes = run_gate_compute_flow(
            process_id=4,
            batches=[[job.to_dict()] for job in prediction_jobs],
            options={
                **common_options,
                "operation": "predict",
                "worker_script": self.configuration.execution_paths[
                    "random_forest_prediction_worker"
                ],
            },
            scheduler_address=(dask_client.scheduler.address if dask_client else None),
            retries=settings.dask_retries if settings else 0,
            max_in_flight=(settings.dask_max_in_flight if settings else max(profile.autoarima_workers, 1)),
            local_workers=max(profile.autoarima_workers, 1),
        )
        for outcome in outcomes:
            if "response" not in outcome:
                raise RuntimeError(outcome["error"])
            envelope = ClassificationResponse.from_dict(outcome["response"]["response"])
            result = envelope.results[0]
            job = next(job for job in prediction_jobs if job.job_id == result.job_id)
            predictions = DirectionalMantisComposer.directional_predictions(
                dataset, [job], [envelope], representations,
                experiment_id=experiment_id, model_definition_id=model_id,
            )
            task = prediction_by_horizon.pop(result.horizon)

            def insert_classifier_result(
                result=result, envelope=envelope, predictions=predictions
            ) -> None:
                run_content = {
                    "classifier_definition_id": classifier_definition_id,
                    "classification_dataset_id": dataset.dataset_id,
                    "horizon": result.horizon,
                    "training_fingerprint": result.training_fingerprint,
                    "evaluation_fingerprint": result.evaluation_fingerprint,
                    "output_fingerprint": result.output_fingerprint,
                }
                self._insert_or_verify(
                    "directional_classifier_runs",
                    {
                        "classifier_run_id": result.classifier_run_id,
                        **run_content,
                        "content_hash": json_fingerprint(run_content),
                    },
                )
                self._insert_or_verify(
                    "directional_classifier_executions",
                    {
                        "classifier_execution_id": "classifier-execution/"
                        + json_fingerprint(
                            {"run": result.classifier_run_id, "response": envelope.response_id}
                        )[:32],
                        "classifier_run_id": result.classifier_run_id,
                        "classification_response_id": envelope.response_id,
                        "worker_provenance": canonical_json(envelope.worker_provenance),
                        "worker_provenance_fingerprint": envelope.worker_provenance_fingerprint,
                    },
                )
                by_input = {record.input_id: record for record in representations}
                for prediction in predictions:
                    metadata = prediction.scientific_metadata
                    self._insert_or_verify(
                        "model_directional_predictions",
                        {
                            "prediction_id": prediction.prediction_id,
                            "experiment_id": prediction.experiment_id,
                            "model_definition_id": prediction.model_definition_id,
                            "evaluation_input_id": prediction.evaluation_input_id,
                            "horizon": prediction.horizon,
                            "prediction": prediction.prediction,
                            "classifier_run_id": metadata["classifier_run_id"],
                            "training_fingerprint": metadata["training_fingerprint"],
                            "evaluation_fingerprint": metadata["evaluation_fingerprint"],
                            "output_fingerprint": metadata["output_fingerprint"],
                            "content_hash": prediction.content_hash,
                        },
                    )
                    lineage_content = {
                        "representation_id": by_input[
                            prediction.evaluation_input_id
                        ].representation_id,
                        "classifier_run_id": result.classifier_run_id,
                    }
                    self._insert_or_verify(
                        "directional_classifier_prediction_lineage",
                        {
                            "prediction_id": prediction.prediction_id,
                            **lineage_content,
                            "content_hash": json_fingerprint(lineage_content),
                        },
                    )

            self._commit_task(
                task[0], attempts[task[0]],
                float(outcome["response"]["subprocess_runtime_seconds"]),
                insert_classifier_result,
                {
                    "classification_response_id": envelope.response_id,
                    "worker_provenance_fingerprint": envelope.worker_provenance_fingerprint,
                },
            )
        if prediction_by_horizon:
            raise RuntimeError("classifier workers omitted submitted prediction jobs")

    def _run_05_combine(
        self,
        experiment_id: str,
        rows: list[tuple],
        attempts: dict[str, int],
        workers: int,
        dask_client: Any = None,
        settings: ExecutionSettings | None = None,
    ) -> None:
        """Purpose: Execute Process 05 candidate pass-through and forecast combination.

        Inputs: Experiment task rows/attempts, worker count, and optional Dask controls;
        base mean/median/quantile arrays and weights come from DuckDB/configuration.
        Outputs: None; completes base-candidate tasks and computes and transactionally
        persists equal-weight forecasts, component lineage, hashes, and task state.
        """
        if self.configuration.version in {10, 11}:
            if not rows:
                return
            expected_candidate = (
                "directional:no_work"
                if self.configuration.version == 11
                else "directional_dtw:no_work"
            )
            if len(rows) != 1 or rows[0][3] != expected_candidate:
                raise RuntimeError("directional Process 05 requires one no-work identity")
            row = rows[0]
            content = {
                "experiment_id": experiment_id,
                "process_id": 5,
                "reason": (
                    "no forecast combination is configured for directional models"
                    if self.configuration.version == 11
                    else "no forecast combination is configured for directional DTW"
                ),
            }
            record = {
                "no_work_id": "deterministic-no-work/"
                + json_fingerprint({"experiment": experiment_id, "process": 5})[:32],
                **content,
                "content_hash": json_fingerprint(content),
            }

            def insert_no_work() -> None:
                self._insert_or_verify("deterministic_no_work", record)

            self._commit_task(
                row[0], attempts[row[0]], 0.0, insert_no_work,
                {"execution_backend": "Mac coordinator", "hostname": platform.node()},
            )
            return
        combination_name = self.config["combination"]["method"]
        combination_rows = [row for row in rows if row[3] == combination_name]
        jobs = []
        for task_id, instance_id, variant_id, _ in combination_rows:
            model_names = (
                ("ses", "holt", "damped")
                if combination_name == "m4_comb"
                else tuple(self.config["combination"]["weights"])
            )
            placeholders = ", ".join("?" for _ in model_names)
            components = self.connection.execute(
                """SELECT candidate, mean, median, quantiles, forecast_id,
                          forecast_capability FROM forecasts
                WHERE experiment_id=? AND variant_id=? AND forecast_instance_id=?
                AND candidate IN ("""
                + placeholders
                + ") ORDER BY candidate",
                [experiment_id, variant_id, instance_id, *model_names],
            ).fetchall()
            if len(components) != len(model_names):
                raise RuntimeError(
                    f"{combination_name} requires every configured stored component forecast"
                )
            available = {row[0]: row for row in components}
            mapped = {
                name: {
                    "mean": available[name][1], "median": available[name][2],
                    "quantiles": available[name][3], "id": available[name][4],
                    "capability": available[name][5],
                }
                for name in model_names
            }
            jobs.append(
                {
                    "id": task_id,
                    "method": combination_name,
                    "components": mapped,
                    "weights": self.config["combination"]["weights"],
                }
            )

        def commit_combination(
            row: tuple,
            result: dict[str, Any],
            components: dict[str, Any],
            runtime: float = 0.0,
            resources: dict[str, Any] | None = None,
        ) -> None:
            """Purpose: Commit one configured candidate and its Process 05 attempt.

            Inputs: Task row, combined forecast arrays, component mappings, runtime,
            and optional worker provenance.
            Outputs: None; transactionally inserts ensemble/component rows and marks
            the task attempt complete.
            """
            task_id, instance_id, variant_id, candidate = row
            forecast_id = f"forecast/{json_fingerprint({'experiment': experiment_id, 'variant': variant_id, 'instance': instance_id, 'candidate': candidate})[:32]}"
            capability = "mean_only" if candidate == "m4_comb" else "probabilistic"
            levels = None if capability == "mean_only" else list(self.quantiles)
            ordered_lineage = [
                {"name": name, "forecast_id": component["id"],
                 "weight": self.config["combination"]["weights"][name]}
                for name, component in components["components"].items()
            ]

            def insert() -> None:
                """Purpose: Write one ensemble forecast and its component lineage.

                Inputs: Captured IDs, combined horizon arrays, source forecast IDs,
                configured weights, and quantile-rearrangement flag.
                Outputs: None; inserts ``forecasts`` and ``forecast_components`` rows.
                """
                self.connection.execute(
                    """INSERT INTO forecasts
                    (forecast_id, experiment_id, variant_id, forecast_instance_id,
                     candidate, model_revision, parent_result_id, scale, mean,
                     median, quantile_levels, quantiles, runtime_seconds,
                     execution_metadata, content_hash, created_at,
                     forecast_capability)
                    VALUES (?, ?, ?, ?, ?, NULL, NULL, 'original', ?, ?, ?,
                            ?, 0, ?, ?, current_timestamp, ?)
                    ON CONFLICT (forecast_id) DO NOTHING""",
                    [
                        forecast_id,
                        experiment_id,
                        variant_id,
                        instance_id,
                        candidate,
                        result["mean"],
                        result["median"],
                        levels,
                        result["quantiles"],
                        canonical_json(
                            ({
                                "rule": "stored SES, Holt, and Damped means at one-third each",
                                "forecast_capability": capability,
                                "components": ordered_lineage,
                            } if candidate == "m4_comb" else {
                                "adjustment": (
                                    "monotone_rearrangement"
                                    if result["quantiles_rearranged"]
                                    else "identity"
                                ),
                                "rule": "corresponding means, medians, and quantiles averaged",
                            })
                        ),
                        (json_fingerprint({
                            "rule": "m4_comb", "components": ordered_lineage,
                            "capability": capability, "scale": "original",
                            "mean": result["mean"], "median": None,
                            "quantile_levels": None, "quantiles": None,
                        }) if candidate == "m4_comb" else json_fingerprint(
                            {
                                key: result[key]
                                for key in ("mean", "median", "quantiles")
                            }
                        )),
                        capability,
                    ],
                )
                for name, component in components["components"].items():
                    self.connection.execute(
                        "INSERT INTO forecast_components VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                        [
                            forecast_id,
                            component["id"],
                            name,
                            self.config["combination"]["weights"][name],
                        ],
                    )

            self._commit_task(
                task_id, attempts[task_id], runtime, insert, resources
            )

        for task_id, instance_id, variant_id, candidate in rows:
            if candidate == combination_name:
                continue
            existing = self.connection.execute(
                """SELECT forecast_id FROM forecasts WHERE experiment_id=? AND variant_id=?
                AND forecast_instance_id=? AND candidate=?""",
                [experiment_id, variant_id, instance_id, candidate],
            ).fetchone()
            if existing is None:
                raise RuntimeError(f"missing Process 04 forecast for {candidate}")
            self._commit_task(
                task_id,
                attempts[task_id],
                0.0,
                lambda: None,
                {
                    "execution_backend": "Mac coordinator pass-through",
                    "hostname": platform.node(),
                    "retry_count": 0,
                },
            )

        rows_by_id = {row[0]: row for row in combination_rows}
        jobs_by_id = {job["id"]: job for job in jobs}
        from .shared_workflow_orchestration import run_gate_compute_flow

        outcomes = run_gate_compute_flow(
            process_id=5,
            batches=_batches(jobs, int(self.configuration.execution["batch_sizes"]["combine"])),
            options={},
            scheduler_address=(dask_client.scheduler.address if dask_client else None),
            retries=settings.dask_retries if settings else 0,
            max_in_flight=settings.dask_max_in_flight if settings else workers,
            local_workers=workers,
        )
        errors = []
        for outcome in outcomes:
            if "response" not in outcome:
                errors.append(outcome["error"])
                continue
            batch, response = outcome["batch"], outcome["response"]
            result_ids = [item["id"] for item in response["results"]]
            expected_ids = {job["id"] for job in batch}
            if len(result_ids) != len(set(result_ids)) or set(result_ids) != expected_ids:
                raise RuntimeError(
                    "Process 05 worker returned missing, duplicate, or unexpected task IDs"
                )
            runtime = response["runtime_seconds"] / len(batch)
            for result in response["results"]:
                task_id = result["id"]
                commit_combination(
                    rows_by_id[task_id],
                    result,
                    jobs_by_id[task_id],
                    runtime,
                    response["worker"],
                )
        if errors:
            raise RuntimeError("; ".join(errors))

    def _run_06_evaluate(
        self,
        experiment_id: str,
        rows: list[tuple],
        attempts: dict[str, int],
        workers: int,
        settings: ExecutionSettings | None = None,
    ) -> None:
        """Purpose: Execute Process 06 official evaluation of complete forecast matrices.

        Inputs: Experiment evaluation task rows/attempts and worker count; ordered
        forecast arrays, benchmark identity, quantiles, and options come from DuckDB
        and authoritative configuration.
        Outputs: None; validates complete official-position coverage, runs GIFT-Eval
        subprocesses using temporary payloads, and transactionally upserts metrics,
        input fingerprints, provenance, and task completion.
        """
        configuration_version = getattr(self.configuration, "version", 0)
        if configuration_version == 12:
            from .p06_01_table_flow import run_table_evaluation

            run_table_evaluation(coordinator=self, experiment_id=experiment_id,
                                 rows=[row for row in rows if row[3] == "paper_tables"
                                       or row[3].startswith("directional_")],
                                 attempts=attempts, workers=workers,
                                 settings=settings)
            rows = [row for row in rows if row[3] != "paper_tables"
                    and not row[3].startswith("directional_")]
            if not rows:
                return
        if configuration_version == 11:
            from .shared_labels import directional_accuracy

            if not rows:
                return
            dtw_models = self.connection.execute(
                """SELECT model_definition_id FROM directional_model_definitions
                   WHERE experiment_id=?""",
                [experiment_id],
            ).fetchall()
            mantis_models = self.connection.execute(
                """SELECT model_definition_id
                   FROM directional_composite_model_definitions
                   WHERE experiment_id=?""",
                [experiment_id],
            ).fetchall()
            if len(dtw_models) != 1 or len(mantis_models) != 1:
                raise RuntimeError("comparison evaluation requires both model definitions")
            model_ids = {
                "directional_dtw": dtw_models[0][0],
                "directional_mantis_rf": mantis_models[0][0],
            }
            for row in rows:
                candidate, horizon_text = row[3].rsplit(":h", 1)
                horizon = int(horizon_text)
                model_id = model_ids.get(candidate)
                if model_id is None:
                    raise RuntimeError("unexpected directional evaluation candidate")
                values = self.connection.execute(
                    """SELECT p.evaluation_input_id, p.prediction, a.labels
                       FROM model_directional_predictions AS p
                       JOIN directional_actual_labels AS a
                         ON a.evaluation_input_id=p.evaluation_input_id
                       WHERE p.experiment_id=? AND p.model_definition_id=?
                         AND p.horizon=? ORDER BY p.evaluation_input_id""",
                    [experiment_id, model_id, horizon],
                ).fetchall()
                if len(values) != self.configuration.series_count:
                    raise RuntimeError(
                        f"{candidate} horizon {horizon} has incomplete predictions"
                    )
                score = directional_accuracy(
                    [int(prediction) for _, prediction, _ in values],
                    [int(labels[horizon - 1]) for _, _, labels in values],
                )
                prediction_fingerprint = json_fingerprint(
                    [
                        {
                            "evaluation_input_id": input_id,
                            "prediction": int(prediction),
                            "actual": int(labels[horizon - 1]),
                        }
                        for input_id, prediction, labels in values
                    ]
                )
                content = {
                    "experiment_id": experiment_id,
                    "model_definition_id": model_id,
                    "horizon": horizon,
                    **score,
                    "prediction_fingerprint": prediction_fingerprint,
                }
                record = {
                    "directional_evaluation_id": "directional-evaluation/"
                    + json_fingerprint({"model": model_id, "horizon": horizon})[:32],
                    **content,
                    "content_hash": json_fingerprint(content),
                }

                def insert_evaluation(record=record) -> None:
                    self._insert_or_verify("model_directional_evaluations", record)

                self._commit_task(
                    row[0], attempts[row[0]], 0.0, insert_evaluation,
                    {"execution_backend": "Mac coordinator", "hostname": platform.node()},
                )
            return
        if configuration_version == 10:
            from .shared_labels import directional_accuracy

            if not rows:
                return
            model_rows = self.connection.execute(
                """SELECT m.model_definition_id
                   FROM directional_model_definitions AS m
                   WHERE m.experiment_id=? ORDER BY m.created_at DESC""",
                [experiment_id],
            ).fetchall()
            if len(model_rows) != 1:
                raise RuntimeError("directional evaluation requires one model definition")
            model_id = model_rows[0][0]
            for row in rows:
                horizon = int(row[3].rsplit("h", 1)[1])
                values = self.connection.execute(
                    """SELECT p.evaluation_input_id, p.prediction, a.labels
                       FROM directional_predictions AS p
                       JOIN directional_actual_labels AS a
                         ON a.evaluation_input_id=p.evaluation_input_id
                       WHERE p.experiment_id=? AND p.model_definition_id=?
                         AND p.horizon=?
                       ORDER BY p.evaluation_input_id""",
                    [experiment_id, model_id, horizon],
                ).fetchall()
                if len(values) != self.configuration.series_count:
                    raise RuntimeError(
                        f"directional horizon {horizon} has incomplete predictions"
                    )
                score = directional_accuracy(
                    [int(prediction) for _, prediction, _ in values],
                    [int(labels[horizon - 1]) for _, _, labels in values],
                )
                prediction_fingerprint = json_fingerprint(
                    [
                        {
                            "evaluation_input_id": input_id,
                            "prediction": int(prediction),
                            "actual": int(labels[horizon - 1]),
                        }
                        for input_id, prediction, labels in values
                    ]
                )
                content = {
                    "experiment_id": experiment_id,
                    "model_definition_id": model_id,
                    "horizon": horizon,
                    **score,
                    "prediction_fingerprint": prediction_fingerprint,
                }
                record = {
                    "directional_evaluation_id": "directional-evaluation/"
                    + json_fingerprint({"model": model_id, "horizon": horizon})[:32],
                    **content,
                    "content_hash": json_fingerprint(content),
                }

                def insert_evaluation(record=record) -> None:
                    self._insert_or_verify("directional_evaluations", record)

                self._commit_task(
                    row[0], attempts[row[0]], 0.0, insert_evaluation,
                    {
                        "execution_backend": "Mac coordinator",
                        "hostname": platform.node(),
                    },
                )
            return
        source_root = self.root / self.configuration.source_directory
        benchmark_id = self.connection.execute(
            "SELECT benchmark_configuration_id FROM experiments WHERE experiment_id=?",
            [experiment_id],
        ).fetchone()[0]
        benchmark_metadata = json.loads(
            self.connection.execute(
                "SELECT metadata FROM benchmark_configurations "
                "WHERE benchmark_configuration_id=?",
                [benchmark_id],
            ).fetchone()[0]
        )
        evaluation_seasonality = benchmark_metadata.get(
            "evaluation_seasonality", benchmark_metadata.get("official_seasonality")
        )
        expected_count = self.connection.execute(
            """SELECT count(DISTINCT forecast_instance_id)
               FROM experiment_tasks
               WHERE experiment_id=? AND stage=2""",
            [experiment_id],
        ).fetchone()[0]
        prepared = []
        for task_id, _, variant_id, candidate in rows:
            if candidate in {"m4_smyl", "m4_fforma"}:
                records = self.connection.execute(
                    """SELECT i.forecast_instance_id, i.official_position, r.mean,
                              NULL, r.content_hash, r.forecast_capability
                       FROM forecast_instances i
                       JOIN experiments x ON x.benchmark_configuration_id=i.benchmark_configuration_id
                       JOIN reference_forecasts r ON r.dataset_id=i.dataset_id
                         AND r.series_id=i.series_id AND r.forecast_id=?
                       WHERE x.experiment_id=? AND EXISTS (
                           SELECT 1 FROM experiment_tasks t
                           WHERE t.experiment_id=x.experiment_id AND t.stage=2
                             AND t.forecast_instance_id=i.forecast_instance_id
                       ) ORDER BY i.official_position""",
                    [candidate, experiment_id],
                ).fetchall()
            else:
                records = self.connection.execute(
                    """SELECT f.forecast_instance_id, i.official_position, f.mean,
                              f.quantiles, f.content_hash, f.forecast_capability
                       FROM forecasts f JOIN forecast_instances i USING (forecast_instance_id)
                    WHERE f.experiment_id=? AND f.variant_id=? AND f.candidate=?
                    ORDER BY i.official_position""",
                    [experiment_id, variant_id, candidate],
                ).fetchall()
            instance_ids = [record[0] for record in records]
            positions = [int(record[1]) for record in records]
            capabilities = {record[5] for record in records}
            if len(capabilities) != 1:
                raise RuntimeError("Process 06 candidate mixes forecast capabilities")
            capability = next(iter(capabilities), None)
            if capability == "probabilistic" and any(record[3] is None for record in records):
                raise RuntimeError("probabilistic evaluation requires stored quantiles")
            if capability == "mean_only" and any(record[3] is not None for record in records):
                raise RuntimeError("mean-only evaluation must not contain quantiles")
            if capability not in {"probabilistic", "mean_only"}:
                raise RuntimeError("Process 06 received an unsupported forecast capability")
            if (
                len(records) != expected_count
                or len(set(instance_ids)) != expected_count
                or positions != list(range(expected_count))
            ):
                raise RuntimeError(
                    f"Process 06 requires exactly {expected_count} unique forecasts at "
                    f"official positions 0..{expected_count - 1} for "
                    f"variant={variant_id}, candidate={candidate}; found "
                    f"{len(records)} rows, {len(set(instance_ids))} unique instances, "
                    f"and {len(set(positions))} unique positions"
                )
            input_fingerprint = json_fingerprint(
                [
                    {
                        "forecast_instance_id": record[0],
                        "official_position": record[1],
                        "content_hash": record[4],
                    }
                    for record in records
                ]
            )
            prepared.append(
                {
                    "task_id": task_id,
                    "variant_id": variant_id,
                    "candidate": candidate,
                    "forecast_capability": capability,
                    "evaluation_profile": (
                        "mean_based_v1" if capability == "mean_only"
                        else "gift_eval_probabilistic_v1"
                    ),
                    "evaluation_input_count": len(records),
                    "forecast_input_fingerprint": input_fingerprint,
                    "payload": {
                        "dataset_name": self.configuration.resolved["data"]["dataset_name"],
                        "term": self.configuration.resolved["data"]["benchmark"]["term"],
                        "evaluation_profile": (
                            "mean_based_v1" if capability == "mean_only"
                            else "gift_eval_probabilistic_v1"
                        ),
                        "quantile_levels": None if capability == "mean_only"
                        else list(self.quantiles),
                        "options": self.configuration.evaluation_options,
                        "seasonality": evaluation_seasonality,
                        "forecasts": [
                            {"mean": row[2], "quantiles": row[3]} for row in records
                        ]
                    },
                }
            )

        from .shared_workflow_orchestration import run_gate_compute_flow

        gift_environment = self.configuration.resolved["evaluation"]["gift_eval"]["environment"]
        outcomes = run_gate_compute_flow(
            process_id=6,
            batches=[[item] for item in prepared],
            options={
                "python": str(self.root / gift_environment / "bin/python"),
                "bridge": str(self.root / "src/python/06_01_evaluate_gift_eval.py"),
                "source_root": str(source_root),
                "timeout": float(self.configuration.execution["worker_timeouts_seconds"]["gift_eval"]),
            },
            scheduler_address=None,
            retries=(settings.dask_retries if settings is not None
                     else int(self.configuration.execution["dask_retries"])),
            max_in_flight=workers,
            local_workers=workers,
        )
        errors = []
        for outcome in outcomes:
            if "response" not in outcome:
                errors.append(outcome["error"])
                continue
            item = outcome["batch"][0]
            response = outcome["response"]
            result = response["results"][0]
            if result.get("id") != item["task_id"]:
                raise RuntimeError("Process 06 evaluator returned a mismatched task ID")
            official = result["official"]
            if any(not isinstance(value, (int, float)) or not math.isfinite(value)
                   for value in official.values()):
                raise RuntimeError("Process 06 evaluator returned non-finite metrics")
            runtime = response["runtime_seconds"]
            task_id = item["task_id"]
            variant_id = item["variant_id"]
            candidate = item["candidate"]
            evaluation_input_count = item["evaluation_input_count"]
            forecast_input_fingerprint = item["forecast_input_fingerprint"]
            evaluation_profile = item["evaluation_profile"]
            forecast_capability = item["forecast_capability"]
            evaluation_id = f"evaluation/{json_fingerprint({'experiment': experiment_id, 'variant': variant_id, 'candidate': candidate})[:32]}"

            def insert():
                """Purpose: Write official metrics within the task transaction.

                Inputs: Captured experiment/variant/candidate IDs, metric mapping,
                options, evaluated row count, and exact forecast-input fingerprint.
                Outputs: None; upserts one ``official_evaluations`` DuckDB row.
                """
                self.connection.execute(
                    """INSERT INTO official_evaluations
                    (evaluation_id, experiment_id, variant_id, candidate,
                     benchmark_configuration_id, evaluator, evaluator_revision,
                     options, metrics, evaluation_input_count,
                     forecast_input_fingerprint, is_complete_manifest,
                     is_submittable, created_at)
                    VALUES (?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, false, false, current_timestamp)
                    ON CONFLICT (evaluation_id) DO UPDATE SET
                        evaluator=excluded.evaluator,
                        evaluator_revision=excluded.evaluator_revision,
                        options=excluded.options,
                        metrics=excluded.metrics,
                        evaluation_input_count=excluded.evaluation_input_count,
                        forecast_input_fingerprint=excluded.forecast_input_fingerprint,
                        is_complete_manifest=excluded.is_complete_manifest,
                        is_submittable=excluded.is_submittable,
                        created_at=excluded.created_at""",
                    [
                        evaluation_id,
                        experiment_id,
                        variant_id,
                        candidate,
                        benchmark_id,
                        ("ShapeFM mean-based GluonTS metrics"
                         if forecast_capability == "mean_only"
                         else "gluonts.model.evaluate_forecasts"),
                        self.config["benchmark"]["gift_eval_revision"],
                        canonical_json({
                            **self.configuration.evaluation_options,
                            "evaluation_profile": evaluation_profile,
                            "forecast_capability": forecast_capability,
                        }),
                        canonical_json(official),
                        evaluation_input_count,
                        forecast_input_fingerprint,
                    ],
                )

            self._commit_task(
                task_id,
                attempts[task_id],
                runtime,
                insert,
                response["worker"],
            )
        if errors:
            raise RuntimeError("; ".join(errors))

    def run_all(
        self,
        plan: ExperimentPlan,
        workers: int = 1,
        device: str = "auto",
        batch_size: int = 8,
        execution: tuple[ExecutionProfile, dict[str, Any]] | None = None,
        execution_settings: ExecutionSettings | None = None,
    ) -> list[dict[str, Any]]:
        """Purpose: Execute the full Process 02–06 pipeline in process order.

        Inputs: Persisted experiment plan and optional execution controls forwarded
        to each process.
        Outputs: Ordered process-summary mappings; writes all task/invocation and
        scientific result state, then marks the experiment completed in DuckDB.
        """
        results = []
        for process in PROCESSES:
            results.append(
                self.run_process(
                    plan.experiment_id,
                    process,
                    workers,
                    device,
                    batch_size,
                    execution,
                    execution_settings,
                )
            )
        self.connection.execute(
            "UPDATE experiments SET status='completed', updated_at=current_timestamp WHERE experiment_id=?",
            [plan.experiment_id],
        )
        return results

    def complete_experiment(self, experiment_id: str) -> None:
        """Purpose: Mark a fully evaluated experiment completed.

        Inputs: Persisted experiment ID after successful Process 06 execution.
        Outputs: None; updates experiment status and timestamp through the owned
        single-writer DuckDB connection.
        """
        self.connection.execute(
            "UPDATE experiments SET status='completed', updated_at=current_timestamp WHERE experiment_id=?",
            [experiment_id],
        )

    def status(self, experiment_id: str) -> dict[str, Any]:
        """Purpose: Read coordinator task progress for one experiment.

        Inputs: Persisted experiment ID.
        Outputs: Experiment ID and task counts grouped by stage/status; queries the
        coordinator connection without changing database state.
        """
        rows = self.connection.execute(
            """SELECT stage, status, count(*) FROM experiment_tasks WHERE experiment_id=?
            GROUP BY stage, status ORDER BY stage, status""",
            [experiment_id],
        ).fetchall()
        return {
            "experiment_id": experiment_id,
            "tasks": [
                {"stage": row[0], "stage_name": PROCESSES[row[0]], "status": row[1], "count": row[2]}
                for row in rows
            ],
        }

    def official_results(self, experiment_id: str) -> list[dict[str, Any]]:
        """Purpose: Read compact official results for an experiment.

        Inputs: Persisted experiment ID.
        Outputs: Ordered mappings of cleaning/transformation/candidate identity,
        decoded metric objects, and submittability; performs no database writes.
        """
        rows = self.connection.execute(
            """SELECT v.cleaning_method, v.transformation_method, e.candidate,
               e.metrics, e.is_submittable
            FROM official_evaluations e JOIN experiment_variants v USING (variant_id)
            WHERE e.experiment_id=? ORDER BY 1, 2, 3""",
            [experiment_id],
        ).fetchall()
        return [
            {
                "cleaning": row[0],
                "transformation": row[1],
                "candidate": row[2],
                "metrics": json.loads(row[3]),
                "is_submittable": row[4],
            }
            for row in rows
        ]

    def export_candidate(
        self,
        experiment_id: str,
        model_name: str = "ShapeFM-POC1-provisional",
        output_root: Path = Path("results"),
    ) -> dict[str, Any]:
        """Purpose: Export the configured candidate in GIFT-Eval result layout.

        Inputs: Experiment ID, output model name/root, stored provisional-candidate
        selector, official metrics, submission metadata, and GIFT-Eval manifest.
        Outputs: Export ID/path and validation summary; invokes the manifest bridge,
        creates ``all_results.csv`` and ``config.json``, and inserts a non-submittable
        ``submission_exports`` audit row in DuckDB.
        """
        provisional = self.config["provisional_candidate"]
        row = self.connection.execute(
            """SELECT e.metrics, b.configuration_name, b.domain, b.num_variates,
               e.variant_id, e.candidate
            FROM official_evaluations e
            JOIN experiment_variants v USING (variant_id)
            JOIN benchmark_configurations b USING (benchmark_configuration_id)
            WHERE e.experiment_id=? AND v.cleaning_method=?
              AND v.transformation_method=? AND e.candidate=?""",
            [
                experiment_id,
                provisional["cleaning"],
                provisional["transformation"],
                provisional["candidate"],
            ],
        ).fetchone()
        if row is None:
            raise RuntimeError("the provisional candidate has not completed official evaluation")
        metrics, configuration, domain, num_variates, variant_id, candidate = row
        metric_values = json.loads(metrics)
        manifest = self._gift_bridge(
            "manifest",
            "--root",
            str(self.root),
            "--gift-eval-directory",
            self.configuration.resolved["evaluation"]["gift_eval"]["source_directory"],
        )
        missing_configurations = sorted(set(manifest["configurations"]) - {configuration})
        required_metrics = [
            "MSE[mean]", "MSE[0.5]", "MAE[0.5]", "MASE[0.5]", "MAPE[0.5]",
            "sMAPE[0.5]", "MSIS", "RMSE[mean]", "NRMSE[mean]", "ND[0.5]",
            "mean_weighted_sum_quantile_loss",
        ]
        missing_metrics = [name for name in required_metrics if name not in metric_values]
        validation = {
            "manifest_configuration_count": manifest["configuration_count"],
            "exported_configuration_count": 1,
            "missing_configuration_count": len(missing_configurations),
            "missing_metrics": missing_metrics,
            "duplicate_configurations": [],
            "valid_values": all(
                isinstance(metric_values.get(name), (int, float))
                and math.isfinite(metric_values[name])
                for name in required_metrics
            ),
            "status": "non-submittable development subset",
        }
        output = (self.root / output_root / model_name).resolve()
        output.mkdir(parents=True, exist_ok=True)
        columns = [
            "dataset", "model", *[f"eval_metrics/{name}" for name in required_metrics],
            "domain", "num_variates",
        ]
        with (output / "all_results.csv").open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns)
            writer.writeheader()
            writer.writerow(
                {
                    "dataset": configuration,
                    "model": model_name,
                    **{f"eval_metrics/{name}": metric_values.get(name) for name in required_metrics},
                    "domain": domain,
                    "num_variates": num_variates,
                }
            )
        submission_metadata = validated_submission_metadata(self.config)
        if submission_metadata["submission_approved"]:
            if model_name != submission_metadata["model_name"]:
                raise ValueError("export model name does not match user-approved submission metadata")
        export_config = {
            "model": model_name,
            **{key: value for key, value in submission_metadata.items() if key != "model_name"},
            "shapefm_submission_status": "NON_SUBMITTABLE_DEVELOPMENT_SUBSET",
            "variant_id": variant_id,
            "candidate": candidate,
        }
        (output / "config.json").write_text(
            json.dumps(export_config, indent=2) + "\n", encoding="utf-8"
        )
        export_id = f"export/{uuid.uuid4().hex}"
        self.connection.execute(
            "INSERT INTO submission_exports VALUES (?, ?, ?, ?, ?, ?, false, current_timestamp)",
            [
                export_id,
                experiment_id,
                model_name,
                str(output),
                self.config["benchmark"]["gift_eval_revision"],
                canonical_json(validation),
            ],
        )
        return {"export_id": export_id, "output": str(output), "is_submittable": False, **validation}


def get_forecast(
    database_path: Path, experiment_id: str, variant_id: str, series_id: str, candidate: str
) -> ExperimentForecast:
    """Purpose: Read one candidate forecast with its held-out target.

    Inputs: DuckDB path and experiment, variant, series, and candidate identifiers.
    Outputs: :class:`ExperimentForecast` containing horizon vectors and a
    ``(quantile_levels, horizon)`` quantile matrix; opens and closes a read-only
    connection and raises ``KeyError`` when no matching forecast exists.
    """
    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        row = connection.execute(
            """SELECT f.forecast_id, f.forecast_instance_id, f.mean, f.median,
               f.quantile_levels, f.quantiles, f.forecast_capability, i.actual_target
            FROM forecasts f JOIN forecast_instances i USING (forecast_instance_id)
            WHERE f.experiment_id=? AND f.variant_id=? AND i.series_id=? AND f.candidate=?""",
            [experiment_id, variant_id, str(series_id), candidate],
        ).fetchone()
        if row is None:
            raise KeyError("forecast not found")
        return ExperimentForecast(
            row[0], experiment_id, variant_id, row[1], candidate,
            tuple(row[2]),
            None if row[3] is None else tuple(row[3]),
            None if row[4] is None else tuple(row[4]),
            None if row[5] is None else tuple(tuple(values) for values in row[5]),
            row[6], tuple(row[7]),
        )
    finally:
        connection.close()


def validate_forecast_capability(
    mean: Any,
    median: Any,
    quantile_levels: Any,
    quantiles: Any,
    forecast_capability: str,
    horizon: int | None = None,
) -> None:
    """Validate required mean and all-or-none probabilistic forecast fields."""
    if (
        mean is None
        or not isinstance(mean, (list, tuple))
        or not mean
        or any(
            not isinstance(value, (int, float)) or not math.isfinite(value)
            for value in mean
        )
    ):
        raise ValueError("forecast mean is required")
    probabilistic = (median, quantile_levels, quantiles)
    expected_horizon = len(mean) if horizon is None else horizon
    if len(mean) != expected_horizon:
        raise ValueError("forecast mean length must equal horizon")
    if forecast_capability == "probabilistic":
        if any(value is None for value in probabilistic):
            raise ValueError("probabilistic forecasts require median, levels, and quantiles")
        if (
            len(median) != expected_horizon or len(quantiles) != len(quantile_levels)
            or any(len(values) != expected_horizon for values in quantiles)
        ):
            raise ValueError("probabilistic forecast fields have invalid shapes")
        numeric_values = [*median, *quantile_levels]
        numeric_values.extend(
            value for quantile in quantiles for value in quantile
        )
        if any(
            not isinstance(value, (int, float)) or not math.isfinite(value)
            for value in numeric_values
        ):
            raise ValueError("probabilistic forecast fields must contain only finite values")
        if any(any(left > right for left, right in zip(column, column[1:]))
               for column in zip(*quantiles, strict=True)):
            raise ValueError("probabilistic forecast quantiles cross")
        median_rows = [index for index, level in enumerate(quantile_levels)
                       if abs(level - 0.5) < 1e-12]
        if len(median_rows) != 1 or any(
            abs(left - right) > 1e-10
            for left, right in zip(median, quantiles[median_rows[0]], strict=True)
        ):
            raise ValueError("forecast median must equal q0.5")
    elif forecast_capability == "mean_only":
        if any(value is not None for value in probabilistic):
            raise ValueError("mean-only forecasts must omit every probabilistic field")
    else:
        raise ValueError(f"unsupported forecast capability: {forecast_capability}")


def get_forecast_mean(
    database_path: Path,
    dataset_id: str,
    series_id: str,
    forecast_id: str,
    experiment_id: str | None = None,
    preprocessing_mode: str | None = None,
    variant_id: str | None = None,
) -> tuple[float, ...]:
    """Return a generated or archived forecast mean through one public interface.

    Live forecast identifiers use their registered name (for example
    ``auto_arima_forec``); archived IDs are ``m4_smyl`` and ``m4_fforma``.
    Live lookup uses an explicit mode or the configured robust default. A variant ID
    is required when that still identifies multiple transformed forecasts. Archived
    forecasts bypass preprocessing and retain the internal official-reference source.
    """
    connection = duckdb.connect(str(Path(database_path).resolve()), read_only=True)
    try:
        if forecast_id in {"m4_smyl", "m4_fforma"}:
            if preprocessing_mode is not None or variant_id not in {None, "official_reference"}:
                raise ValueError(
                    "archived M4 forecasts use official_reference, not a preprocessing mode"
                )
            row = connection.execute(
                """SELECT mean FROM reference_forecasts
                   WHERE dataset_id=? AND series_id=? AND forecast_id=?""",
                [dataset_id, str(series_id), forecast_id],
            ).fetchone()
        else:
            candidates = {
                **{method: model for model, method in R_MODEL_METHODS.items()},
                "chronos_2": "chronos_2",
            }
            candidate = candidates.get(forecast_id, forecast_id)
            if preprocessing_mode is None:
                configuration_row = connection.execute(
                    """SELECT resolved_configuration FROM experiment_configuration
                       WHERE configuration_key='experiment'"""
                ).fetchone()
                if configuration_row is None:
                    raise RuntimeError("database has no authoritative preprocessing default")
                preprocessing_mode = json.loads(configuration_row[0])["pipeline"][
                    "preprocessing"
                ]["default"]
            if preprocessing_mode not in {"standard", "robust"}:
                raise ValueError("preprocessing_mode must be standard or robust")
            parameters: list[Any] = [
                dataset_id,
                str(series_id),
                candidate,
                preprocessing_mode,
            ]
            experiment_filter = ""
            if experiment_id is not None:
                experiment_filter = " AND f.experiment_id=?"
                parameters.append(experiment_id)
            variant_filter = ""
            if variant_id is not None:
                variant_filter = " AND f.variant_id=?"
                parameters.append(variant_id)
            rows = connection.execute(
                """SELECT f.mean, f.variant_id
                   FROM forecasts f
                   JOIN forecast_instances i USING (forecast_instance_id)
                   JOIN experiment_variants v USING (variant_id)
                   WHERE i.dataset_id=? AND i.series_id=? AND f.candidate=?
                     AND v.cleaning_method=?"""
                + experiment_filter
                + variant_filter,
                parameters,
            ).fetchall()
            if len(rows) > 1:
                raise ValueError(
                    "forecast mean lookup is ambiguous; provide experiment_id and variant_id"
                )
            row = rows[0] if rows else None
        if row is None:
            raise KeyError("forecast mean not found")
        mean = tuple(float(value) for value in row[0])
        if not mean or any(not math.isfinite(value) for value in mean):
            raise ValueError("stored forecast mean must contain only finite values")
        return mean
    finally:
        connection.close()


def latest_experiment_id(database_path: Path = DEFAULT_DATABASE) -> str:
    """Purpose: Resolve the most recently updated persisted experiment.

    Inputs: Experiment DuckDB path, defaulting to the project database.
    Outputs: Experiment ID string; opens and closes a read-only connection without
    migration and raises ``RuntimeError`` when no plan exists.
    """
    connection = duckdb.connect(str(Path(database_path).resolve()), read_only=True)
    try:
        row = connection.execute(
            "SELECT experiment_id FROM experiments ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()
        if row is None:
            raise RuntimeError("no experiment is planned")
        return row[0]
    finally:
        connection.close()


def experiment_status(
    database_path: Path, experiment_id: str
) -> dict[str, Any]:
    """Purpose: Read a complete operational status snapshot for one experiment.

    Inputs: DuckDB path and persisted experiment ID.
    Outputs: Experiment/configuration metadata, Process 01–06 state, and grouped
    task/invocation counts; opens and closes a read-only connection and writes nothing.
    """
    connection = duckdb.connect(str(Path(database_path).resolve()), read_only=True)
    try:
        experiment = connection.execute(
            """SELECT name, scope, status, CAST(created_at AS VARCHAR),
               CAST(updated_at AS VARCHAR) FROM experiments WHERE experiment_id=?""",
            [experiment_id],
        ).fetchone()
        if experiment is None:
            raise KeyError(f"experiment not found: {experiment_id}")
        tasks = connection.execute(
            """SELECT stage, status, count(*) FROM experiment_tasks
            WHERE experiment_id=? GROUP BY stage, status ORDER BY stage, status""",
            [experiment_id],
        ).fetchall()
        invocations = connection.execute(
            """SELECT requested_gate, status, count(*) FROM experiment_invocations
            WHERE experiment_id=? GROUP BY requested_gate, status
            ORDER BY requested_gate, status""",
            [experiment_id],
        ).fetchall()
        configuration = connection.execute(
            """SELECT configuration_version, experiment_name,
                      CAST(experiment_date AS VARCHAR), experiment_description,
                      reproducibility_seed, scientific_hash,
                      configuration_integrity_hash
               FROM experiment_configuration WHERE configuration_key='experiment'"""
        ).fetchone()
        processes = connection.execute(
            """SELECT process_id, process_name, status, CAST(started_at AS VARCHAR),
                      CAST(completed_at AS VARCHAR), summary, last_error
               FROM experiment_processes ORDER BY process_id"""
        ).fetchall()
        table_summary = None
        if configuration[0] == 12:
            from .p06_02_table_storage import TableStorage

            table_summary = TableStorage(connection).summary(experiment_id)
        return {
            "experiment_id": experiment_id,
            **({"paper_tables": table_summary} if configuration[0] == 12 else {}),
            "name": experiment[0],
            "scope": experiment[1],
            "status": experiment[2],
            "created_at": experiment[3],
            "updated_at": experiment[4],
            "configuration": {
                "version": configuration[0],
                "name": configuration[1],
                "date": configuration[2],
                "description": configuration[3],
                "seed": configuration[4],
                "scientific_hash": configuration[5],
                "configuration_integrity_hash": configuration[6],
            },
            "processes": [
                {
                    "process_id": row[0],
                    "name": row[1],
                    "status": row[2],
                    "started_at": row[3],
                    "completed_at": row[4],
                    "summary": json.loads(row[5]) if row[5] else None,
                    "error": row[6],
                }
                for row in processes
            ],
            "tasks": [
                {"stage": row[0], "stage_name": PROCESSES[row[0]], "status": row[1], "count": row[2]}
                for row in tasks
            ],
            "invocations": [
                {"gate": row[0], "status": row[1], "count": row[2]}
                for row in invocations
            ],
        }
    finally:
        connection.close()


def configuration_status(database_path: Path) -> dict[str, Any]:
    """Purpose: Read authoritative configuration identity and pipeline process state.

    Inputs: Experiment DuckDB path.
    Outputs: Configuration metadata/hash/resolved document and Process 01–06 status
    mappings; opens and closes a read-only connection and writes nothing.
    """
    connection = duckdb.connect(str(Path(database_path).resolve()), read_only=True)
    try:
        configuration = connection.execute(
            """SELECT configuration_version, experiment_name,
                      CAST(experiment_date AS VARCHAR), experiment_description,
                      reproducibility_seed, scientific_hash,
                      configuration_integrity_hash, resolved_configuration
               FROM experiment_configuration WHERE configuration_key='experiment'"""
        ).fetchone()
        if configuration is None:
            raise RuntimeError("database has no authoritative experiment configuration")
        processes = connection.execute(
            """SELECT process_id, process_name, status, CAST(started_at AS VARCHAR),
                      CAST(completed_at AS VARCHAR), summary, last_error
               FROM experiment_processes ORDER BY process_id"""
        ).fetchall()
        return {
            "configuration_version": configuration[0],
            "name": configuration[1],
            "date": configuration[2],
            "description": configuration[3],
            "seed": configuration[4],
            "scientific_hash": configuration[5],
            "configuration_integrity_hash": configuration[6],
            "resolved_configuration": json.loads(configuration[7]),
            "processes": [
                {
                    "process_id": row[0],
                    "name": row[1],
                    "status": row[2],
                    "started_at": row[3],
                    "completed_at": row[4],
                    "summary": json.loads(row[5]) if row[5] else None,
                    "error": row[6],
                }
                for row in processes
            ],
        }
    finally:
        connection.close()


def table_results(database_path: Path, experiment_id: str) -> dict[str, Any]:
    """Read paper diagnostics through their storage owner without opening a writer."""
    from .p06_02_table_storage import TableStorage

    with duckdb.connect(str(Path(database_path).resolve()), read_only=True) as connection:
        return TableStorage(connection).summary(experiment_id)


def official_results(
    database_path: Path, experiment_id: str
) -> list[dict[str, Any]]:
    """Purpose: Read full official evaluation records for one experiment.

    Inputs: DuckDB path and persisted experiment ID.
    Outputs: Ordered evaluation mappings containing variant methods, candidate,
    benchmark/evaluator provenance, decoded options and metrics, manifest flags,
    and timestamps; opens and closes a read-only connection without writes.
    """
    connection = duckdb.connect(str(Path(database_path).resolve()), read_only=True)
    try:
        rows = connection.execute(
            """SELECT e.evaluation_id, e.variant_id, v.cleaning_method,
               v.transformation_method, e.candidate, b.configuration_name,
               e.evaluator, e.evaluator_revision, e.options, e.metrics,
               e.is_complete_manifest, e.is_submittable,
               CAST(e.created_at AS VARCHAR)
            FROM official_evaluations e
            JOIN experiment_variants v USING (variant_id)
            JOIN benchmark_configurations b USING (benchmark_configuration_id)
            WHERE e.experiment_id=? ORDER BY 3, 4, 5""",
            [experiment_id],
        ).fetchall()
        return [
            {
                "evaluation_id": row[0],
                "variant_id": row[1],
                "cleaning": row[2],
                "transformation": row[3],
                "candidate": row[4],
                "benchmark_configuration": row[5],
                "evaluator": row[6],
                "evaluator_revision": row[7],
                "options": json.loads(row[8]),
                "metrics": json.loads(row[9]),
                "is_complete_manifest": row[10],
                "is_submittable": row[11],
                "created_at": row[12],
            }
            for row in rows
        ]
    finally:
        connection.close()
