# ==============================================================================
# experiment_execution.py
#
# Purpose: Plan M4 Daily tasks, execute preprocessing through official evaluation, and export a candidate.
# Inputs: Imported M4 series, experiment configuration, execution profile/settings, and process selection.
# Outputs: Restartable task/invocation rows, forecasts, official evaluations, status, and candidate exports.
# Run from: Imported; not run directly.
# ==============================================================================

"""Plan and execute the restartable M4 Daily experiment in a single-writer DuckDB."""

from __future__ import annotations

import json
import math
import os
import platform
import subprocess
import tempfile
import time
import uuid
import csv
from collections import deque
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable, Iterable

import duckdb

from .configuration import canonical_json, json_fingerprint
from .database import DEFAULT_DATABASE, load_database_configuration, migrate_database
from .execution_profiles import (
    GIB,
    ExecutionProfile,
    ExecutionSettings,
    PersistentChronosWorker,
    system_hardware,
    validate_heavy_tuning_execution,
    validate_system_memory,
)
from .forecast_combination import combine_equal_weight
from .import_execution import repository_root
from .transformations import TransformationResult, inverse, transform
from .provenance import utc_now


# Code constant: process-number to legacy persistent invocation-stage protocol mapping;
# existing ``stage`` fields remain unchanged for database and JSON compatibility.
PROCESSES = {2: "preprocess", 3: "transform", 4: "forecast", 5: "combine", 6: "evaluate"}


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


def _run_parallel(
    function: Callable[[Any], Any], values: list[Any], workers: int
) -> list[Any]:
    """Purpose: Execute pure scientific jobs with deterministic result ordering.

    Inputs: Picklable callable, ordered job values, and local process count.
    Outputs: Results aligned to input order; may spawn and close worker processes.
    """
    if workers == 1:
        return [function(value) for value in values]
    import multiprocessing

    with ProcessPoolExecutor(
        max_workers=workers, mp_context=multiprocessing.get_context("spawn")
    ) as executor:
        return list(executor.map(function, values))


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
    for job in sorted(jobs, key=lambda value: (len(value["context"]), value["id"])):
        length_bucket = max(1, len(job["context"])).bit_length()
        grouped.setdefault(length_bucket, []).append(job)
    return [
        batch
        for bucket in sorted(grouped)
        for batch in _batches(grouped[bucket], batch_size)
    ]


def _run_external_batches(
    function: Callable[[Any], Any], batches: list[Any], workers: int
) -> Iterable[Any]:
    """Purpose: Schedule bounded bridge batches while preserving single-writer safety.

    Inputs: Batch callable, ordered payload batches, and thread count.
    Outputs: Ordered batch results; may create a thread pool whose callbacks launch
    external workers, but grants them no DuckDB connection.
    """
    if workers == 1:
        for batch in batches:
            yield function(batch)
        return
    with ThreadPoolExecutor(max_workers=workers) as executor:
        yield from executor.map(function, batches)


def _transform_job(job: tuple[list[float], str]) -> TransformationResult:
    """Apply the named transformation to one value sequence in a process worker."""
    return transform(job[0], job[1])


def _combine_job(job: dict[str, Any]) -> dict[str, Any]:
    """Equal-weight one pair of component forecast mappings in a process worker."""
    return combine_equal_weight(job["left"], job["right"], job["weights"])


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
            chronos_inference_batch_size=int(values["batch_sizes"]["chronos"]),
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
            return {
                "scope": requested_scope,
                "mode": "dry-run",
                "selection": self.configuration.resolved["data"]["selection"],
                "series_count": len(set(series_ids)),
                "forecast_instances": len(instances),
                "candidate_forecast_rows": expected_task_counts(len(instances), self.config)[5],
                "official_evaluation_rows": expected_task_counts(len(instances), self.config)[6],
                "benchmark_configuration": official["configuration_name"],
                "task_counts": {
                    str(process): count
                    for process, count in expected_task_counts(len(instances), self.config).items()
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
                        for model in self.config["models"]:
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
                            "equal_weight",
                        ):
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
            for variant_id, _, _ in variants:
                for candidate in (*self.config["models"].keys(), "equal_weight"):
                    evaluation_tasks.append(
                        self._task_row(
                            experiment_id, 6, None, variant_id, candidate
                        )
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
        return self.connection.execute(
            """SELECT task_id, forecast_instance_id, variant_id, candidate
            FROM experiment_tasks WHERE experiment_id=? AND stage=? AND status!='completed'
            ORDER BY task_id""",
            [experiment_id, process],
        ).fetchall()

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
        if settings.mode == "dask" and process != 6:
            from distributed import Client

            from .distributed_execution import (
                repository_source_manifest,
                validate_cluster,
                validate_tuning_cluster,
            )

            if settings.dask_scheduler_address:
                dask_client = Client(
                    settings.dask_scheduler_address,
                    timeout=f"{settings.dask_timeout_seconds}s",
                )
                expected_gpu_name = None
            else:
                dask_client = Client(
                    n_workers=1,
                    threads_per_worker=1,
                    processes=True,
                    resources={"CPU": 1, "CHRONOS_GPU_SLOT": 1},
                    timeout=f"{settings.dask_timeout_seconds}s",
                )
                expected_gpu_name = None
            try:
                if process == 4 and self.configuration.seasonal_period_tuning is not None:
                    mac_workers = int(profile.dask_mac_cpu_workers or 0)
                    ubuntu_workers = int(profile.dask_ubuntu_cpu_workers or 0)
                    cluster = validate_tuning_cluster(
                        dask_client,
                        expected_workers=mac_workers + ubuntu_workers,
                        expected_mac_workers=mac_workers,
                        expected_ubuntu_workers=ubuntu_workers,
                        expected_tuning_workers=int(profile.dask_mac_tuning_workers or 0)
                        + int(profile.dask_ubuntu_tuning_workers or 0),
                        timeout=settings.dask_timeout_seconds,
                        expected_manifest=repository_source_manifest(),
                    )
                    resolved_device = "cpu"
                else:
                    expected_gpu_name = self.configuration.resolved["execution"][
                        "final_acceptance"
                    ]["gpu_name"]
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
                        require_gpu=process == 4,
                        expected_gpu_name=expected_gpu_name,
                        expected_gpu_workers=settings.dask_expected_gpu_workers,
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
                4: int(configured_batches["chronos"]),
                5: int(configured_batches["combine"]),
                6: int(configured_batches["gift_eval"]),
            }[process],
            profile,
            invocation_overrides,
            {**hardware, "execution_settings": settings.to_dict()},
        )
        rows = self._pending(experiment_id, process)
        attempts = self._start_tasks(rows, invocation)
        started = time.monotonic()
        failures = []
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
                )
        except BaseException as exc:
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
        status = "completed" if set(counts) <= {"completed"} else "failed"
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

        def invoke(batch: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any], float]:
            """Purpose: Run one local Process 02 R cleaning batch.

            Inputs: Job mappings containing task IDs, contexts, methods, and seasonality.
            Outputs: Original batch, decoded worker response, and elapsed seconds;
            launches the R bridge but does not write DuckDB.
            """
            started = time.monotonic()
            response = self._r_worker(
                {
                    "action": "preprocess",
                    "jobs": [
                        {
                            k: v
                            for k, v in job.items()
                            if k not in {"instance_id", "official_frequency"}
                        }
                        for job in batch
                    ],
                }
            )
            return batch, response, time.monotonic() - started

        if dask_client is not None:
            from .distributed_execution import clean_batch, run_batches

            dask_results = run_batches(
                dask_client,
                clean_batch,
                _batches(jobs, batch_size),
                resources={"CPU": 1},
                max_in_flight=settings.dask_max_in_flight,
                retries=settings.dask_retries,
                extra_arguments=(
                    self.configuration.execution_paths["r_preprocess_worker"],
                    float(self.configuration.execution["worker_timeouts_seconds"]["r"]),
                    int(self.configuration.execution["thread_limits"]["r"]),
                ),
            )
            responses = (
                (
                    batch,
                    {
                        "results": response["results"],
                        "packages": response["packages"],
                        "worker": response["worker"],
                    },
                    response["runtime_seconds"],
                )
                for batch, response in dask_results
            )
        else:
            responses = _run_external_batches(
                invoke, _batches(jobs, batch_size), workers
            )
        for batch, response, runtime in responses:
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

    def _run_03_transform(
        self,
        experiment_id: str,
        rows: list[tuple],
        attempts: dict[str, int],
        workers: int,
        dask_client: Any = None,
        settings: ExecutionSettings | None = None,
    ) -> None:
        """Purpose: Execute Process 03 transformations of cleaned training contexts.

        Inputs: Experiment task rows/attempts, worker count, and optional Dask controls;
        methods and input vectors are read from variant and preprocessing rows.
        Outputs: None; computes transformed vectors locally or on Dask and atomically
        persists values, parameters, lineage, fingerprints, and task completions.
        """
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

        if dask_client is None:
            results = _run_parallel(_transform_job, prepared, workers)
            for meta, result in zip(metadata, results, strict=True):
                commit_result(meta, result, 0.0)
            return

        from .distributed_execution import run_batches, transform_batch

        jobs = [
            {"id": meta[0], "values": values, "method": method}
            for meta, (values, method) in zip(metadata, prepared, strict=True)
        ]
        by_task = {meta[0]: meta for meta in metadata}
        for batch, response in run_batches(
            dask_client,
            transform_batch,
            _batches(jobs, int(self.configuration.execution["batch_sizes"]["transform"])),
            resources={"CPU": 1},
            max_in_flight=settings.dask_max_in_flight,
            retries=settings.dask_retries,
        ):
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
        if self.configuration.seasonal_period_tuning is not None:
            from .seasonal_period_tuning import (
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
        prepared = []
        for task_id, instance_id, variant_id, model in rows:
            values, horizon, benchmark_metadata = self.connection.execute(
                """SELECT t.transformed_target, i.horizon, b.metadata
                FROM transformed_series t
                JOIN forecast_instances i USING (forecast_instance_id)
                JOIN benchmark_configurations b USING (benchmark_configuration_id)
                WHERE t.experiment_id=? AND t.variant_id=? AND t.forecast_instance_id=?""",
                [experiment_id, variant_id, instance_id],
            ).fetchone()
            metadata = json.loads(benchmark_metadata)
            r_period = metadata.get(
                "r_period", metadata.get("official_seasonality")
            )
            if r_period is None:
                raise RuntimeError("benchmark metadata is missing the resolved R period")
            prepared.append(
                {
                    "id": task_id,
                    "context": values,
                    "horizon": horizon,
                    "seasonality": r_period,
                    "model": model,
                    "instance_id": instance_id,
                    "variant_id": variant_id,
                }
            )
        auto_jobs = [job for job in prepared if job["model"] == "auto_arima"]
        chronos_jobs = [job for job in prepared if job["model"] == "chronos_2"]

        def invoke_auto(batch: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any], float]:
            """Purpose: Run one local AutoARIMA forecast batch through R.

            Inputs: Transformed context jobs with horizons and official seasonality;
            AutoARIMA settings come from authoritative configuration.
            Outputs: Batch, forecasts with package/runtime provenance, and elapsed
            seconds; launches R without writing DuckDB.
            """
            jobs = [
                {key: value for key, value in job.items() if key not in {"model", "instance_id", "variant_id"}}
                for job in batch
            ]
            started = time.monotonic()
            response = self._r_worker(
                {
                    "action": "forecast",
                    "settings": self.configuration.auto_arima_settings,
                    "jobs": jobs,
                }
            )
            runtime = time.monotonic() - started
            metadata = {
                "packages": response["packages"],
                "settings": self.configuration.auto_arima_settings,
                "execution_backend": "R/CPU",
                "runtime_seconds": runtime,
                "batch_task_count": len(batch),
            }
            return batch, {"results": response["results"], "metadata": metadata}, runtime

        auto_batches = _batches(
            auto_jobs, int(self.configuration.execution["batch_sizes"]["auto_arima"])
        )

        def commit_response(
            batch: list[dict[str, Any]],
            response: dict[str, Any],
            runtime: float,
        ) -> None:
            """Purpose: Validate and commit one Process 04 model response batch.

            Inputs: Submitted jobs, worker forecast arrays/provenance, and batch runtime.
            Outputs: None; verifies task identity, reads transformation parameters,
            inverse-transforms mean/median/quantile arrays, then commits each forecast.
            """
            result_ids = [item["id"] for item in response["results"]]
            by_id = {item["id"]: item for item in response["results"]}
            if len(result_ids) != len(set(result_ids)) or set(by_id) != {
                job["id"] for job in batch
            }:
                raise RuntimeError("Process 04 worker returned missing, duplicate, or unexpected task IDs")
            metadata = response["metadata"]
            for job in batch:
                task_id = job["id"]
                instance_id = job["instance_id"]
                variant_id = job["variant_id"]
                model = job["model"]
                result = by_id[task_id]
                method, parameters, transformation_id = self.connection.execute(
                    """SELECT transformation_method, parameters, transformation_id
                    FROM transformed_series WHERE experiment_id=? AND variant_id=? AND forecast_instance_id=?""",
                    [experiment_id, variant_id, instance_id],
                ).fetchone()
                params = json.loads(parameters)
                mean = inverse(result["mean"], method, params)
                median = inverse(result["median"], method, params)
                quantiles = [inverse(values, method, params) for values in result["quantiles"]]
                validate_forecast_capability(
                    list(mean),
                    list(median),
                    list(self.quantiles),
                    [list(values) for values in quantiles],
                    "probabilistic",
                )
                result_metadata = metadata
                if "requested_method_id" in result:
                    result_metadata = {
                        **metadata,
                        "forecast_method": {
                            "requested_method_id": result["requested_method_id"],
                            "executed_method_id": result["executed_method_id"],
                            "fallback_used": result["fallback_used"],
                            "fallback_reason": result["fallback_reason"],
                            "provenance": result["provenance"],
                        },
                    }
                forecast_id = f"forecast/{json_fingerprint({'experiment': experiment_id, 'variant': variant_id, 'instance': instance_id, 'candidate': model})[:32]}"
                model_revision = (
                    self.config["models"][model].get("revision")
                    or metadata.get("packages", {}).get("forecast")
                )

                def insert(
                    mean=mean,
                    median=median,
                    quantiles=quantiles,
                    forecast_id=forecast_id,
                    model_revision=model_revision,
                    transformation_id=transformation_id,
                    instance_id=instance_id,
                    variant_id=variant_id,
                    model=model,
                    metadata=result_metadata,
                ):
                    """Purpose: Write one original-scale base forecast transactionally.

                    Inputs: Captured horizon arrays, model/lineage IDs, revision, and
                    execution metadata from the validated worker response.
                    Outputs: None; inserts an idempotent ``forecasts`` row with hash.
                    """
                    self.connection.execute(
                        """INSERT INTO forecasts
                        (forecast_id, experiment_id, variant_id, forecast_instance_id,
                         candidate, model_revision, parent_result_id, scale, mean,
                         median, quantile_levels, quantiles, runtime_seconds,
                         execution_metadata, content_hash, created_at,
                         forecast_capability)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 'original', ?, ?, ?, ?, ?, ?, ?,
                                current_timestamp, 'probabilistic')
                        ON CONFLICT (forecast_id) DO NOTHING""",
                        [
                            forecast_id,
                            experiment_id,
                            variant_id,
                            instance_id,
                            model,
                            model_revision,
                            transformation_id,
                            list(mean),
                            list(median),
                            list(self.quantiles),
                            [list(values) for values in quantiles],
                            metadata.get("runtime_seconds", 0.0),
                            canonical_json(metadata),
                            json_fingerprint({"mean": mean, "quantiles": quantiles}),
                        ],
                    )

                self._commit_task(
                    task_id,
                    attempts[task_id],
                    runtime / len(batch),
                    insert,
                    result_metadata,
                )

        if dask_client is not None:
            from .distributed_execution import (
                autoarima_batch,
                chronos_batch,
                run_batch_groups,
            )

            chronos_pending = deque(
                _length_aware_batches(
                    chronos_jobs, profile.chronos_inference_batch_size
                )
            )

            def pending_chronos() -> Iterable[list[dict[str, Any]]]:
                """Purpose: Feed the mutable Chronos retry queue to Dask scheduling.

                Inputs: Enclosing deque of length-aware context batches.
                Outputs: Batches in queue order; consumes queue state but writes no DB rows.
                """
                while chronos_pending:
                    yield chronos_pending.popleft()

            groups = {}
            if auto_batches:
                groups["auto_arima"] = (
                    autoarima_batch,
                    auto_batches,
                    {"CPU": 1},
                    (
                        self.configuration.auto_arima_settings,
                        self.configuration.execution_paths["r_auto_arima_worker"],
                        float(self.configuration.execution["worker_timeouts_seconds"]["r"]),
                        int(self.configuration.execution["thread_limits"]["r"]),
                    ),
                    settings.dask_max_in_flight,
                )
            if chronos_jobs:
                chronos = self.config["models"]["chronos_2"]
                groups["chronos_2"] = (
                    chronos_batch,
                    pending_chronos(),
                    {"CHRONOS_GPU_SLOT": 1},
                    (
                        chronos["repository"],
                        chronos["revision"],
                        list(self.quantiles),
                        device,
                        chronos["dtype"],
                        chronos["cross_learning"],
                        chronos["predict_batches_jointly"],
                        self.configuration.execution["thread_limits"]["chronos"],
                        self.configuration.execution_paths["chronos_environment"],
                        self.configuration.execution_paths["chronos_worker"],
                        float(
                            self.configuration.execution["worker_timeouts_seconds"]["chronos_startup"]
                        ),
                        float(
                            self.configuration.execution["worker_timeouts_seconds"]["chronos_request"]
                        ),
                    ),
                    settings.dask_max_in_flight,
                )
            for model, batch, response in run_batch_groups(
                dask_client, groups, retries=settings.dask_retries
            ):
                if isinstance(response, BaseException):
                    if model == "chronos_2" and len(batch) > 1:
                        smaller = max(1, len(batch) // 2)
                        for split_batch in reversed(_batches(batch, smaller)):
                            chronos_pending.appendleft(split_batch)
                        continue
                    raise response
                metadata = {
                    **response["worker"],
                    "runtime_seconds": response["runtime_seconds"],
                    "batch_task_count": len(batch),
                    "requested_batch_size": (
                        profile.chronos_inference_batch_size
                        if model == "chronos_2"
                        else len(batch)
                    ),
                }
                commit_response(
                    batch,
                    {"results": response["results"], "metadata": metadata},
                    response["runtime_seconds"],
                )
            return

        def run_chronos(progress: Callable[[], None] = lambda: None) -> None:
            """Purpose: Run local persistent Chronos inference with bounded OOM recovery.

            Inputs: Prepared context batches, stored model/quantile settings, profile
            memory limits and device, plus a callback for concurrent CPU progress.
            Outputs: None; owns a Chronos subprocess, shrinks/requeues OOM batches,
            commits successful forecasts/task state, and always closes the worker.
            """
            if not chronos_jobs:
                return
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
                device,
                "--dtype",
                chronos["dtype"],
                "--internal-cpu-threads",
                str(self.configuration.execution["thread_limits"]["chronos"]),
            ]
            pending = deque(
                _length_aware_batches(
                    chronos_jobs, profile.chronos_inference_batch_size
                )
            )
            retries = {job["id"]: 0 for job in chronos_jobs}
            worker: PersistentChronosWorker | None = None
            generation = 0
            try:
                while pending:
                    validate_system_memory(profile, system_hardware())
                    if worker is None:
                        worker = PersistentChronosWorker(
                            command,
                            startup_timeout=float(
                                self.configuration.execution["worker_timeouts_seconds"]["chronos_startup"]
                            ),
                        )
                        ready = worker.start()
                        generation += 1
                        available = ready["accelerator_memory"].get("available_bytes")
                        threshold = int(profile.accelerator_memory_min_available_gib * GIB)
                        if available is not None and available < threshold:
                            raise RuntimeError(
                                "accelerator memory safety threshold reached before inference"
                            )
                    batch = pending.popleft()
                    batch_id = f"chronos-batch/{uuid.uuid4().hex}"
                    payload_jobs = [
                        {
                            key: value
                            for key, value in job.items()
                            if key not in {"model", "instance_id", "variant_id", "seasonality"}
                        }
                        for job in batch
                    ]
                    response = worker.request(
                        {
                            "command": "predict",
                            "batch_id": batch_id,
                            "jobs": payload_jobs,
                            "horizon": batch[0]["horizon"],
                            "quantile_levels": list(self.quantiles),
                            "inference_batch_size": len(batch),
                            "cross_learning": chronos["cross_learning"],
                            "predict_batches_jointly": chronos["predict_batches_jointly"],
                        },
                        timeout=float(
                            self.configuration.execution["worker_timeouts_seconds"]["chronos_request"]
                        ),
                    )
                    if response.get("type") == "error":
                        if response.get("error_kind") != "out_of_memory":
                            raise RuntimeError(response["error"])
                        worker.close(force=True)
                        worker = None
                        if len(batch) == 1:
                            raise RuntimeError(
                                f"Chronos out of memory at minimum batch size: {response['error']}"
                            )
                        smaller = max(1, len(batch) // 2)
                        for job in batch:
                            retries[job["id"]] += 1
                        for split_batch in reversed(_batches(batch, smaller)):
                            pending.appendleft(split_batch)
                        continue
                    if response.get("type") != "result":
                        raise RuntimeError(f"invalid Chronos worker response: {response}")
                    metadata = {
                        **{key: value for key, value in ready.items() if key != "type"},
                        "execution_backend": ready["accelerator_backend"],
                        "batch_id": batch_id,
                        "requested_batch_size": profile.chronos_inference_batch_size,
                        "effective_batch_size": response["effective_batch_size"],
                        "retry_count": max(retries[job["id"]] for job in batch),
                        "worker_generation": generation,
                        "inference_seconds": response["inference_seconds"],
                        "peak_process_memory_bytes": response["peak_process_memory_bytes"],
                        "accelerator_memory_after": response["accelerator_memory"],
                        "runtime_seconds": response["inference_seconds"],
                    }
                    commit_response(
                        batch,
                        {"results": response["results"], "metadata": metadata},
                        response["inference_seconds"],
                    )
                    progress()
                    available = response["accelerator_memory"].get("available_bytes")
                    threshold = int(profile.accelerator_memory_min_available_gib * GIB)
                    if available is not None and available < threshold and pending:
                        raise RuntimeError(
                            "accelerator memory safety threshold reached after committed batch"
                        )
            finally:
                if worker is not None:
                    worker.close()

        if profile.cpu_gpu_overlap and auto_batches and chronos_jobs:
            with ThreadPoolExecutor(max_workers=profile.autoarima_workers) as executor:
                futures = [
                    (batch, executor.submit(invoke_auto, batch)) for batch in auto_batches
                ]
                committed: set[int] = set()

                def commit_finished_auto() -> None:
                    """Purpose: Drain completed CPU forecasts while Chronos uses the GPU.

                    Inputs: Enclosing AutoARIMA futures and committed-index set.
                    Outputs: None; commits each finished batch once, thereby writing
                    forecast and task state through ``commit_response``.
                    """
                    for index, (_, future) in enumerate(futures):
                        if index in committed or not future.done():
                            continue
                        batch, response, runtime = future.result()
                        commit_response(batch, response, runtime)
                        committed.add(index)

                chronos_error = None
                try:
                    run_chronos(commit_finished_auto)
                except BaseException as error:
                    chronos_error = error
                commit_finished_auto()
                if chronos_error is not None:
                    raise chronos_error
                for index, (_, future) in enumerate(futures):
                    if index in committed:
                        continue
                    batch, response, runtime = future.result()
                    commit_response(batch, response, runtime)
                    committed.add(index)
        else:
            for batch, response, runtime in _run_external_batches(
                invoke_auto, auto_batches, profile.autoarima_workers
            ):
                commit_response(batch, response, runtime)
            run_chronos()

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
        combination_rows = [row for row in rows if row[3] == "equal_weight"]
        jobs = []
        for task_id, instance_id, variant_id, _ in combination_rows:
            model_names = tuple(self.config["models"])
            placeholders = ", ".join("?" for _ in model_names)
            components = self.connection.execute(
                """SELECT candidate, mean, median, quantiles, forecast_id FROM forecasts
                WHERE experiment_id=? AND variant_id=? AND forecast_instance_id=?
                AND candidate IN ("""
                + placeholders
                + ") ORDER BY candidate",
                [experiment_id, variant_id, instance_id, *model_names],
            ).fetchall()
            if len(components) != 2:
                raise RuntimeError("equal-weight combination requires both model forecasts")
            mapped = {row[0]: {"mean": row[1], "median": row[2], "quantiles": row[3], "id": row[4]} for row in components}
            jobs.append(
                {
                    "id": task_id,
                    "left": mapped[model_names[0]],
                    "right": mapped[model_names[1]],
                    "model_names": model_names,
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
            """Purpose: Commit one equal-weight candidate and its Process 05 attempt.

            Inputs: Task row, combined forecast arrays, component mappings, runtime,
            and optional worker provenance.
            Outputs: None; transactionally inserts ensemble/component rows and marks
            the task attempt complete.
            """
            task_id, instance_id, variant_id, candidate = row
            forecast_id = f"forecast/{json_fingerprint({'experiment': experiment_id, 'variant': variant_id, 'instance': instance_id, 'candidate': candidate})[:32]}"

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
                    VALUES (?, ?, ?, ?, 'equal_weight', NULL, NULL, 'original', ?, ?, ?,
                            ?, 0, ?, ?, current_timestamp, 'probabilistic')
                    ON CONFLICT (forecast_id) DO NOTHING""",
                    [
                        forecast_id,
                        experiment_id,
                        variant_id,
                        instance_id,
                        result["mean"],
                        result["median"],
                        list(self.quantiles),
                        result["quantiles"],
                        canonical_json(
                            {
                                "adjustment": (
                                    "monotone_rearrangement"
                                    if result["quantiles_rearranged"]
                                    else "identity"
                                ),
                                "rule": "corresponding means, medians, and quantiles averaged",
                            }
                        ),
                        json_fingerprint(
                            {
                                key: result[key]
                                for key in ("mean", "median", "quantiles")
                            }
                        ),
                    ],
                )
                for name, component in zip(
                    components["model_names"],
                    (components["left"], components["right"]),
                    strict=True,
                ):
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
            if candidate == "equal_weight":
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

        if dask_client is None:
            results = _run_parallel(_combine_job, jobs, workers)
            for row, job, result in zip(
                combination_rows, jobs, results, strict=True
            ):
                commit_combination(row, result, job)
            return

        from .distributed_execution import combine_batch, run_batches

        rows_by_id = {row[0]: row for row in combination_rows}
        jobs_by_id = {job["id"]: job for job in jobs}
        for batch, response in run_batches(
            dask_client,
            combine_batch,
            _batches(jobs, int(self.configuration.execution["batch_sizes"]["combine"])),
            resources={"CPU": 1},
            max_in_flight=settings.dask_max_in_flight,
            retries=settings.dask_retries,
        ):
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

    def _run_06_evaluate(
        self,
        experiment_id: str,
        rows: list[tuple],
        attempts: dict[str, int],
        workers: int,
    ) -> None:
        """Purpose: Execute Process 06 official evaluation of complete forecast matrices.

        Inputs: Experiment evaluation task rows/attempts and worker count; ordered
        forecast arrays, benchmark identity, quantiles, and options come from DuckDB
        and authoritative configuration.
        Outputs: None; validates complete official-position coverage, runs GIFT-Eval
        subprocesses using temporary payloads, and transactionally upserts metrics,
        input fingerprints, provenance, and task completion.
        """
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
            if any(record[5] != "probabilistic" or record[3] is None for record in records):
                raise RuntimeError(
                    "full probabilistic GIFT-Eval does not support mean-only forecasts"
                )
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
                    "evaluation_input_count": len(records),
                    "forecast_input_fingerprint": input_fingerprint,
                    "payload": {
                        "dataset_name": self.configuration.resolved["data"]["dataset_name"],
                        "term": self.configuration.resolved["data"]["benchmark"]["term"],
                        "quantile_levels": list(self.quantiles),
                        "options": self.configuration.evaluation_options,
                        "seasonality": evaluation_seasonality,
                        "forecasts": [
                            {"mean": row[2], "quantiles": row[3]} for row in records
                        ]
                    },
                }
            )

        def invoke(item: dict[str, Any]) -> tuple[dict[str, Any], dict[str, float], float]:
            """Purpose: Evaluate one complete candidate matrix with GIFT-Eval.

            Inputs: Candidate identity and ordered mean/quantile forecast payload.
            Outputs: Input item, official metric mapping, and elapsed seconds; creates
            and removes a temporary JSON file and launches the evaluation subprocess.
            """
            started = time.monotonic()
            with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as stream:
                json.dump(item["payload"], stream)
                path = Path(stream.name)
            try:
                official = self._gift_bridge(
                    "evaluate",
                    "--source-root",
                    str(source_root),
                    "--payload",
                    str(path),
                    timeout=float(
                        self.configuration.execution["worker_timeouts_seconds"]["gift_eval"]
                    ),
                )
            finally:
                path.unlink(missing_ok=True)
            return item, official, time.monotonic() - started

        for item, official, runtime in _run_external_batches(
            invoke, prepared, workers
        ):
            task_id = item["task_id"]
            variant_id = item["variant_id"]
            candidate = item["candidate"]
            evaluation_input_count = item["evaluation_input_count"]
            forecast_input_fingerprint = item["forecast_input_fingerprint"]
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
                    VALUES (?, ?, ?, ?, ?, 'gluonts.model.evaluate_forecasts',
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
                        self.config["benchmark"]["gift_eval_revision"],
                        canonical_json(self.configuration.evaluation_options),
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
                {"execution_backend": "official GIFT-Eval CPU evaluator"},
            )

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
    if forecast_capability == "probabilistic":
        if any(value is None for value in probabilistic):
            raise ValueError("probabilistic forecasts require median, levels, and quantiles")
        numeric_values = [*median, *quantile_levels]
        numeric_values.extend(
            value for quantile in quantiles for value in quantile
        )
        if any(
            not isinstance(value, (int, float)) or not math.isfinite(value)
            for value in numeric_values
        ):
            raise ValueError("probabilistic forecast fields must contain only finite values")
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
                "auto_arima_forec": "auto_arima",
                "ets_forec": "ets",
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
        return {
            "experiment_id": experiment_id,
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
