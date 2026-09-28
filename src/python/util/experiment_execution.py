# ==============================================================================
# experiment_execution.py
#
# Purpose: Plan M4 Daily tasks, execute preprocessing through official evaluation, and export a candidate.
# Inputs: Imported M4 series, experiment configuration, execution profile/settings, and stage selection.
# Outputs: Restartable task/invocation rows, forecasts, official evaluations, status, and candidate exports.
# Run from: Imported; not run directly.
# ==============================================================================

"""Plan and execute the restartable M4 Daily POC 1 pipeline in a single-writer DuckDB."""

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
    validate_system_memory,
)
from .forecast_combination import combine_equal_weight
from .import_execution import repository_root
from .transformations import TransformationResult, inverse, transform
from .provenance import utc_now


# Ordered database stage numbers mapped to invocation gate names.
STAGES = {2: "preprocess", 3: "transform", 4: "forecast", 5: "combine", 6: "evaluate"}


def expected_task_counts(
    instance_count: int, workflow: dict[str, Any]
) -> dict[int, int]:
    """Derive Process 02–06 task totals from selected instances and stored settings."""
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
    """Validate draft/approved GIFT-Eval submission fields and return them key-sorted."""
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
    """Result of planning an executable POC 1 experiment.

    Fields identify the persisted experiment and benchmark, describe its requested
    scope and optional series cap, and report instance, variant, and per-stage task
    counts. Pass a non-dry-run plan to :meth:`POC1Coordinator.run_all`.
    """
    experiment_id: str
    scope: str
    instance_count: int
    variant_count: int
    task_counts: dict[int, int]
    benchmark_configuration: str


@dataclass(frozen=True)
class ExperimentForecast:
    """Read model for one persisted candidate forecast and its held-out target.

    Identity fields locate the experiment, variant, forecast instance, and
    candidate. ``mean`` and ``median`` are horizon-length point forecasts;
    ``quantiles`` aligns with ``quantile_levels`` and ``actual`` contains the
    comparison target. Instances are returned by :func:`get_forecast`.
    """
    forecast_id: str
    experiment_id: str
    variant_id: str
    forecast_instance_id: str
    candidate: str
    mean: tuple[float, ...]
    median: tuple[float, ...]
    quantile_levels: tuple[float, ...]
    quantiles: tuple[tuple[float, ...], ...]
    actual: tuple[float, ...]


def _run_parallel(
    function: Callable[[Any], Any], values: list[Any], workers: int
) -> list[Any]:
    """Run pure computations sequentially or in spawned local workers."""
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
    """Run bounded external jobs without allowing workers to access DuckDB."""
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


class POC1Coordinator:
    """Coordinate planning and stage execution through one writable DuckDB connection.

    Construct with a database path (the default project database is used when
    omitted). Construction migrates the database, loads the fixed M4 Daily
    experiment configuration, and initializes a hardware cache. Use as a context
    manager, call :meth:`plan`, then execute individual gates or :meth:`run_all`;
    task attempts, forecasts, evaluations, and exports are persisted for restart
    and audit. Read-only module helpers serve consumers that do not need a writer.
    """

    def __init__(self, database_path: Path = DEFAULT_DATABASE):
        """Open an initialized database and load its authoritative configuration."""
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
            self.configuration.resolved["models"]["chronos_2"]["quantile_levels"]
        )
        self._hardware_cache: dict[str, dict[str, Any]] = {}

    def close(self) -> None:
        """Close the coordinator's writable DuckDB connection."""
        self.connection.close()

    def __enter__(self) -> "POC1Coordinator":
        """Return the open single-writer coordinator."""
        return self

    def __exit__(self, *_: object) -> None:
        """Close the coordinator's DuckDB connection on context exit."""
        self.close()

    def _gift_bridge(self, *arguments: str, timeout: float = 300.0) -> dict[str, Any]:
        """Run the isolated GIFT-Eval bridge with CLI arguments and decode its JSON response."""
        environment = self.configuration.resolved["evaluation"]["gift_eval"]["environment"]
        command = [
            str(self.root / environment / "bin/python"),
            str(self.root / "src/python/06_evaluate_gift_eval.py"),
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
        """Return the newest imported M4 Daily dataset ID, failing when Stage 1 has not run."""
        row = self.connection.execute(
            "SELECT dataset_id FROM datasets WHERE dataset_name = ? "
            "ORDER BY created_at DESC LIMIT 1",
            [self.configuration.resolved["data"]["dataset_name"]],
        ).fetchone()
        if row is None:
            raise RuntimeError("Foundation Stage 1 M4 Daily import is required")
        return row[0]

    def _validate_official_configuration(self, official: dict[str, Any]) -> None:
        """Require the configured benchmark name and the single POC forecast window."""
        expected_configuration = self.config["benchmark"]["configuration"]
        if official["configuration_name"] != expected_configuration:
            raise RuntimeError(
                f"official configuration {official['configuration_name']} does not match "
                f"configured {expected_configuration}"
            )
        if official["window_count"] != 1:
            raise RuntimeError(
                "POC 1 supports exactly one official forecast window; "
                f"{official['configuration_name']} has {official['window_count']}"
            )

    def _configured_execution(self) -> tuple[ExecutionProfile, ExecutionSettings]:
        """Build sequential process controls exclusively from stored execution settings."""
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
        """Select the configured official prefix and materialize its deterministic task graph."""
        source_root = self.root / self.configuration.source_directory
        availability = self._gift_bridge(
            "describe",
            "--source-root",
            str(source_root),
            "--dataset-name",
            self.configuration.resolved["data"]["dataset_name"],
            "--term",
            self.configuration.resolved["data"]["benchmark"]["term"],
            "--domain",
            self.configuration.resolved["data"]["benchmark"]["domain"],
            "--num-variates",
            str(self.configuration.resolved["data"]["benchmark"]["num_variates"]),
            "--limit",
            "1",
        )
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
            benchmark_id = f"benchmark/{json_fingerprint(benchmark_identity)[:24]}"
            scientific = self.configuration.scientific_configuration
            configuration_hash = self.configuration.scientific_hash
            experiment_id = f"experiment/{json_fingerprint({'benchmark': benchmark_id, 'dataset': dataset_id, 'configuration': configuration_hash})[:24]}"
            existing_counts = {
                int(stage): int(count)
                for stage, count in self.connection.execute(
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
        official = self._gift_bridge(
            "describe",
            "--source-root",
            str(source_root),
            "--dataset-name",
            self.configuration.resolved["data"]["dataset_name"],
            "--term",
            self.configuration.resolved["data"]["benchmark"]["term"],
            "--domain",
            self.configuration.resolved["data"]["benchmark"]["domain"],
            "--num-variates",
            str(self.configuration.resolved["data"]["benchmark"]["num_variates"]),
            "--limit",
            str(limit),
        )
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
                    str(stage): count
                    for stage, count in expected_task_counts(len(instances), self.config).items()
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
                            "official_seasonality": official["seasonality"],
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
            {int(stage): int(count) for stage, count in counts.items()},
            official["configuration_name"],
        )

    def _register_task(
        self,
        experiment_id: str,
        stage: int,
        instance_id: str | None,
        variant_id: str | None,
        candidate: str | None,
    ) -> str:
        """Insert one deterministic pending task if absent and return its task ID."""
        row = self._task_row(
            experiment_id, stage, instance_id, variant_id, candidate
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
        stage: int,
        instance_id: str | None,
        variant_id: str | None,
        candidate: str | None,
    ) -> tuple[str, str, int, str | None, str | None, str | None]:
        """Build the deterministic task ID and normalized database tuple for one stage unit."""
        identity = {
            "experiment": experiment_id,
            "stage": stage,
            "instance": instance_id,
            "variant": variant_id,
            "candidate": candidate,
        }
        task_id = f"poc1-task/{json_fingerprint(identity)[:32]}"
        return (
            task_id,
            experiment_id,
            stage,
            instance_id,
            variant_id,
            candidate,
        )

    def _begin_invocation(
        self,
        experiment_id: str,
        stage: int,
        workers: int,
        device: str,
        batch_size: int,
        profile: ExecutionProfile | None = None,
        overrides: dict[str, Any] | None = None,
        hardware: dict[str, Any] | None = None,
    ) -> str:
        """Record a running gate invocation and reset interrupted tasks for retry."""
        invocation_id = f"poc1-invocation/{uuid.uuid4().hex}"
        self.connection.execute(
            """INSERT INTO experiment_invocations
            (invocation_id, experiment_id, requested_gate, worker_count, device,
             batch_size, environment, machine, started_at, status,
             execution_profile, resolved_execution, execution_overrides, hardware)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', ?, ?, ?, ?)""",
            [
                invocation_id,
                experiment_id,
                STAGES[stage],
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
            [experiment_id, stage],
        )
        self.connection.execute(
            """UPDATE experiment_tasks SET status='pending', last_error='interrupted before completion',
               updated_at=current_timestamp WHERE experiment_id=? AND stage=? AND status='running'""",
            [experiment_id, stage],
        )
        return invocation_id

    def _chronos_hardware(self, requested_device: str) -> dict[str, Any]:
        """Probe the Chronos bridge for the requested accelerator and decode its hardware report."""
        paths = self.configuration.resolved["execution"]["paths"]
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
        """Validate and cache host, accelerator, R, forecast, and model details."""
        requested = profile.required_accelerator or "auto"
        if requested not in self._hardware_cache:
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
            self._hardware_cache[requested] = {
                **system,
                **accelerator,
                "R_version": r_output[0].strip(),
                "forecast_package_version": r_output[1].strip(),
                "model_revision": self.config["models"]["chronos_2"]["revision"],
                "model_dtype": self.config["models"]["chronos_2"]["dtype"],
            }
        return self._hardware_cache[requested]

    def validate_hardware(
        self, profile: ExecutionProfile, run_chronos_smoke: bool = False
    ) -> dict[str, Any]:
        """Report profile hardware and optionally prove Chronos inference on one instance."""
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
        paths = self.configuration.resolved["execution"]["paths"]
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
        """Mark selected tasks running, create attempt rows, and return attempt numbers."""
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
        """Atomically run a result insert and mark its task attempt completed."""
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
        """Atomically mark a task and its current attempt failed with an error."""
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
        """Send cleaning or AutoARIMA jobs to the matching R bridge and decode its response."""
        paths = self.configuration.resolved["execution"]["paths"]
        execution = self.configuration.execution
        timeout = float(execution["worker_timeouts_seconds"]["r"])
        threads = str(execution["thread_limits"]["r"])
        script = (
            paths["r_auto_arima_worker"]
            if payload.get("action") == "forecast"
            else paths["r_preprocess_worker"]
        )
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

    def _pending(self, experiment_id: str, stage: int) -> list[tuple]:
        """Return incomplete task identifiers and candidate dimensions for one experiment stage."""
        return self.connection.execute(
            """SELECT task_id, forecast_instance_id, variant_id, candidate
            FROM experiment_tasks WHERE experiment_id=? AND stage=? AND status!='completed'
            ORDER BY task_id""",
            [experiment_id, stage],
        ).fetchall()

    def _check_gate(self, experiment_id: str, stage: int) -> None:
        """Require every task in the preceding stage to be complete."""
        if stage == 2:
            return
        incomplete = self.connection.execute(
            """SELECT count(*) FROM experiment_tasks
            WHERE experiment_id=? AND stage=? AND status!='completed'""",
            [experiment_id, stage - 1],
        ).fetchone()[0]
        if incomplete:
            raise RuntimeError(f"Stage {stage - 1} gate has {incomplete} incomplete tasks")

    def run_gate(
        self,
        experiment_id: str,
        stage: int,
        workers: int | None = None,
        device: str = "auto",
        batch_size: int | None = None,
        execution: tuple[ExecutionProfile, dict[str, Any]] | None = None,
        execution_settings: ExecutionSettings | None = None,
    ) -> dict[str, Any]:
        """Execute one restartable stage and return its invocation/task summary.

        The execution profile determines stage concurrency and accelerator use;
        sequential settings force all concurrency to one. Only incomplete tasks
        are selected. Their attempts and stage outputs are committed through the
        coordinator, while a failed stage remains retryable and raises
        ``RuntimeError`` after invocation accounting is finalized.
        """
        if (
            stage not in STAGES
            or (workers is not None and workers < 1)
            or (batch_size is not None and batch_size < 1)
        ):
            raise ValueError("stage must be 2..6; workers and batch_size must be positive")
        configured_batches = self.configuration.execution["batch_sizes"]
        preprocess_batch_size = int(batch_size or configured_batches["preprocess"])
        if execution is None:
            profile, configured_settings = self._configured_execution()
            overrides = {}
        else:
            profile, overrides = execution
            configured_settings = ExecutionSettings()
        settings = execution_settings or configured_settings
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
        if settings.mode == "dask" and stage != 6:
            from distributed import Client

            from .distributed_execution import validate_cluster

            if settings.dask_scheduler_address:
                dask_client = Client(
                    settings.dask_scheduler_address,
                    timeout=f"{settings.dask_timeout_seconds}s",
                )
                expected_gpu_name = self.configuration.resolved["execution"]["final_acceptance"]["gpu_name"]
                resolved_device = "cuda"
            else:
                dask_client = Client(
                    n_workers=1,
                    threads_per_worker=1,
                    processes=True,
                    resources={"CPU": 1, "CHRONOS_GPU_SLOT": 1},
                    timeout=f"{settings.dask_timeout_seconds}s",
                )
                expected_gpu_name = None
            expected_commit = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=self.root,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            try:
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
                    chronos_environment=self.configuration.resolved["execution"]["paths"]["chronos_environment"],
                    gift_eval_source_directory=self.configuration.resolved["evaluation"]["gift_eval"]["source_directory"],
                    require_gpu=stage == 4,
                    expected_gpu_name=expected_gpu_name,
                    expected_gpu_workers=settings.dask_expected_gpu_workers,
                )
            except BaseException:
                dask_client.close()
                raise
            hardware = {"coordinator": coordinator_hardware, "dask_workers": cluster}
        stage_workers = {
            2: profile.cleaning_workers,
            3: profile.transformation_workers,
            4: max(profile.autoarima_workers, profile.chronos_processes),
            5: profile.combination_workers,
            6: profile.evaluation_workers,
        }[stage]
        self._check_gate(experiment_id, stage)
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
            stage,
            stage_workers,
            resolved_device,
            {
                2: preprocess_batch_size,
                3: int(configured_batches["transform"]),
                4: int(configured_batches["chronos"]),
                5: int(configured_batches["combine"]),
                6: int(configured_batches["gift_eval"]),
            }[stage],
            profile,
            invocation_overrides,
            {**hardware, "execution_settings": settings.to_dict()},
        )
        rows = self._pending(experiment_id, stage)
        attempts = self._start_tasks(rows, invocation)
        started = time.monotonic()
        failures = []
        try:
            if stage == 2:
                self._stage2(
                    experiment_id,
                    rows,
                    attempts,
                    profile.cleaning_workers,
                    preprocess_batch_size,
                    dask_client,
                    settings,
                )
            elif stage == 3:
                self._stage3(
                    experiment_id,
                    rows,
                    attempts,
                    profile.transformation_workers,
                    dask_client,
                    settings,
                )
            elif stage == 4:
                self._stage4(
                    experiment_id,
                    rows,
                    attempts,
                    profile,
                    resolved_device,
                    dask_client,
                    settings,
                )
            elif stage == 5:
                self._stage5(
                    experiment_id,
                    rows,
                    attempts,
                    profile.combination_workers,
                    dask_client,
                    settings,
                )
            else:
                self._stage6(experiment_id, rows, attempts, profile.evaluation_workers)
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
                [experiment_id, stage],
            ).fetchall()
        )
        status = "completed" if set(counts) <= {"completed"} else "failed"
        summary = {
            "stage": stage,
            "selected": len(rows),
            "skipped": self.connection.execute(
                "SELECT count(*) FROM experiment_tasks WHERE experiment_id=? AND stage=? AND status='completed'",
                [experiment_id, stage],
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
            raise RuntimeError(f"Stage {stage} failed; rerun retries failed tasks")
        return {"invocation_id": invocation, **summary}

    def _stage2(
        self,
        experiment_id: str,
        rows: list[tuple],
        attempts: dict[str, int],
        workers: int,
        batch_size: int,
        dask_client: Any = None,
        settings: ExecutionSettings | None = None,
    ) -> None:
        """Clean raw training contexts in R and atomically persist preprocessed series."""
        jobs = []
        for task_id, instance_id, _, method in rows:
            context, metadata = self.connection.execute(
                """SELECT i.context_target, b.metadata FROM forecast_instances i
                JOIN benchmark_configurations b USING (benchmark_configuration_id)
                WHERE i.forecast_instance_id=?""",
                [instance_id],
            ).fetchone()
            seasonality = json.loads(metadata)["official_seasonality"]
            jobs.append(
                {
                    "id": task_id,
                    "context": context,
                    "method": method,
                    "seasonality": seasonality,
                    "instance_id": instance_id,
                }
            )

        def invoke(batch: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any], float]:
            """Send one cleaning batch to R and return its response with elapsed time."""
            started = time.monotonic()
            response = self._r_worker(
                {"action": "clean", "jobs": [{k: v for k, v in job.items() if k != "instance_id"} for job in batch]}
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
                    self.configuration.resolved["execution"]["paths"]["r_preprocess_worker"],
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
                raise RuntimeError("Stage 2 worker returned missing, duplicate, or unexpected task IDs")
            for job in batch:
                task_id, instance_id, method = job["id"], job["instance_id"], job["method"]
                result = by_id[task_id]
                original = job["context"]
                preprocessing_id = f"preprocessed/{json_fingerprint({'experiment': experiment_id, 'instance': instance_id, 'method': method})[:32]}"

                def insert(
                    preprocessing_id=preprocessing_id,
                    instance_id=instance_id,
                    method=method,
                    original=original,
                    result=result,
                    seasonality=job["seasonality"],
                    packages=response["packages"],
                ):
                    """Insert the cleaned context and its fingerprints as a preprocessed series."""
                    self.connection.execute(
                        """INSERT INTO preprocessed_series VALUES
                        (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, current_timestamp)
                        ON CONFLICT (preprocessing_id) DO NOTHING""",
                        [
                            preprocessing_id,
                            experiment_id,
                            instance_id,
                            method,
                            json_fingerprint(original),
                            json_fingerprint(result["values"]),
                            result["values"],
                            canonical_json({"seasonality": seasonality, "source": "official get_seasonality(freq)"}),
                            canonical_json(packages),
                        ],
                    )

                self._commit_task(
                    task_id,
                    attempts[task_id],
                    runtime / len(batch),
                    insert,
                    response.get("worker"),
                )

    def _stage3(
        self,
        experiment_id: str,
        rows: list[tuple],
        attempts: dict[str, int],
        workers: int,
        dask_client: Any = None,
        settings: ExecutionSettings | None = None,
    ) -> None:
        """Apply configured transformations and persist transformed training contexts."""
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
            """Persist one transformed context and complete its Stage 3 attempt."""
            task_id, instance_id, variant_id, pre_id, method, values = meta
            transformation_id = f"transformed/{json_fingerprint({'experiment': experiment_id, 'variant': variant_id, 'instance': instance_id})[:32]}"

            def insert(result=result, values=values):
                """Insert transformed values, parameters, lineage, and content hashes."""
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
                    "Stage 3 worker returned missing, duplicate, or unexpected task IDs"
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

    def _stage4(
        self,
        experiment_id: str,
        rows: list[tuple],
        attempts: dict[str, int],
        profile: ExecutionProfile,
        device: str,
        dask_client: Any = None,
        settings: ExecutionSettings | None = None,
    ) -> None:
        """Run AutoARIMA and Chronos forecasts, invert transformations, and persist candidate forecasts."""
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
            prepared.append(
                {
                    "id": task_id,
                    "context": values,
                    "horizon": horizon,
                    "seasonality": json.loads(benchmark_metadata)["official_seasonality"],
                    "model": model,
                    "instance_id": instance_id,
                    "variant_id": variant_id,
                }
            )
        auto_jobs = [job for job in prepared if job["model"] == "auto_arima"]
        chronos_jobs = [job for job in prepared if job["model"] == "chronos_2"]

        def invoke_auto(batch: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any], float]:
            """Forecast one AutoARIMA batch in R and attach runtime/package metadata."""
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
            """Validate a model batch, invert its transformations, and commit forecasts."""
            result_ids = [item["id"] for item in response["results"]]
            by_id = {item["id"]: item for item in response["results"]}
            if len(result_ids) != len(set(result_ids)) or set(by_id) != {
                job["id"] for job in batch
            }:
                raise RuntimeError("Stage 4 worker returned missing, duplicate, or unexpected task IDs")
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
                    metadata=metadata,
                ):
                    """Insert one original-scale model forecast with provenance and hash."""
                    self.connection.execute(
                        """INSERT INTO forecasts VALUES
                        (?, ?, ?, ?, ?, ?, ?, 'original', ?, ?, ?, ?, ?, ?, ?, current_timestamp)
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
                    task_id, attempts[task_id], runtime / len(batch), insert, metadata
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
                """Yield queued Chronos batches until the stage queue is empty."""
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
                        self.configuration.resolved["execution"]["paths"]["r_auto_arima_worker"],
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
                        self.configuration.resolved["execution"]["paths"]["chronos_environment"],
                        self.configuration.resolved["execution"]["paths"]["chronos_worker"],
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
            """Serve queued Chronos batches, shrinking on OOM and committing each result."""
            if not chronos_jobs:
                return
            chronos = self.config["models"]["chronos_2"]
            paths = self.configuration.resolved["execution"]["paths"]
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
                    """Commit each completed AutoARIMA future exactly once during GPU work."""
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

    def _stage5(
        self,
        experiment_id: str,
        rows: list[tuple],
        attempts: dict[str, int],
        workers: int,
        dask_client: Any = None,
        settings: ExecutionSettings | None = None,
    ) -> None:
        """Combine eligible base forecasts at equal weight and persist ensemble forecasts."""
        combination_rows = [row for row in rows if row[3] == "equal_weight"]
        jobs = []
        for task_id, instance_id, variant_id, _ in combination_rows:
            components = self.connection.execute(
                """SELECT candidate, mean, median, quantiles, forecast_id FROM forecasts
                WHERE experiment_id=? AND variant_id=? AND forecast_instance_id=?
                AND candidate IN ('auto_arima', 'chronos_2') ORDER BY candidate""",
                [experiment_id, variant_id, instance_id],
            ).fetchall()
            if len(components) != 2:
                raise RuntimeError("equal-weight combination requires both model forecasts")
            mapped = {row[0]: {"mean": row[1], "median": row[2], "quantiles": row[3], "id": row[4]} for row in components}
            jobs.append(
                {
                    "id": task_id,
                    "left": mapped["auto_arima"],
                    "right": mapped["chronos_2"],
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
            """Persist an equal-weight result and complete its Stage 5 attempt."""
            task_id, instance_id, variant_id, candidate = row
            forecast_id = f"forecast/{json_fingerprint({'experiment': experiment_id, 'variant': variant_id, 'instance': instance_id, 'candidate': candidate})[:32]}"

            def insert() -> None:
                """Insert the ensemble forecast and configured component weights."""
                self.connection.execute(
                    """INSERT INTO forecasts VALUES
                    (?, ?, ?, ?, 'equal_weight', NULL, NULL, 'original', ?, ?, ?, ?, 0, ?, ?, current_timestamp)
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
                for name, component in (("auto_arima", components["left"]), ("chronos_2", components["right"])):
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
                raise RuntimeError(f"missing Stage 4 forecast for {candidate}")
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
                    "Stage 5 worker returned missing, duplicate, or unexpected task IDs"
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

    def _stage6(
        self,
        experiment_id: str,
        rows: list[tuple],
        attempts: dict[str, int],
        workers: int,
    ) -> None:
        """Evaluate complete candidate matrices through GIFT-Eval and persist official metrics."""
        source_root = self.root / self.configuration.source_directory
        benchmark_id = self.connection.execute(
            "SELECT benchmark_configuration_id FROM experiments WHERE experiment_id=?",
            [experiment_id],
        ).fetchone()[0]
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
                          f.quantiles, f.content_hash
                   FROM forecasts f JOIN forecast_instances i USING (forecast_instance_id)
                WHERE f.experiment_id=? AND f.variant_id=? AND f.candidate=?
                ORDER BY i.official_position""",
                [experiment_id, variant_id, candidate],
            ).fetchall()
            instance_ids = [record[0] for record in records]
            positions = [int(record[1]) for record in records]
            if (
                len(records) != expected_count
                or len(set(instance_ids)) != expected_count
                or positions != list(range(expected_count))
            ):
                raise RuntimeError(
                    f"Stage 6 requires exactly {expected_count} unique forecasts at "
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
                        "forecasts": [
                            {"mean": row[2], "quantiles": row[3]} for row in records
                        ]
                    },
                }
            )

        def invoke(item: dict[str, Any]) -> tuple[dict[str, Any], dict[str, float], float]:
            """Evaluate one complete candidate payload through the GIFT-Eval bridge."""
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
                """Upsert official metrics and the exact evaluated-input fingerprint."""
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
        """Run Stages 2–6 in order, mark the experiment complete, and return summaries."""
        results = []
        for stage in STAGES:
            results.append(
                self.run_gate(
                    plan.experiment_id,
                    stage,
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

    def status(self, experiment_id: str) -> dict[str, Any]:
        """Return task counts grouped by stage and status for an experiment."""
        rows = self.connection.execute(
            """SELECT stage, status, count(*) FROM experiment_tasks WHERE experiment_id=?
            GROUP BY stage, status ORDER BY stage, status""",
            [experiment_id],
        ).fetchall()
        return {
            "experiment_id": experiment_id,
            "tasks": [
                {"stage": row[0], "stage_name": STAGES[row[0]], "status": row[1], "count": row[2]}
                for row in rows
            ],
        }

    def official_results(self, experiment_id: str) -> list[dict[str, Any]]:
        """Return official metrics and submittability for every evaluated variant/candidate."""
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
        """Write the configured provisional candidate as a non-submittable subset.

        Produces GIFT-Eval ``all_results.csv`` and ``config.json`` beneath
        ``output_root/model_name``, validates required finite metrics and approved
        metadata, records the export, and returns its path and validation summary.
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
    """Read one persisted forecast and its held-out actuals through a read-only connection."""
    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        row = connection.execute(
            """SELECT f.forecast_id, f.forecast_instance_id, f.mean, f.median,
               f.quantile_levels, f.quantiles, i.actual_target
            FROM forecasts f JOIN forecast_instances i USING (forecast_instance_id)
            WHERE f.experiment_id=? AND f.variant_id=? AND i.series_id=? AND f.candidate=?""",
            [experiment_id, variant_id, str(series_id), candidate],
        ).fetchone()
        if row is None:
            raise KeyError("forecast not found")
        return ExperimentForecast(
            row[0], experiment_id, variant_id, row[1], candidate,
            tuple(row[2]), tuple(row[3]), tuple(row[4]),
            tuple(tuple(values) for values in row[5]), tuple(row[6]),
        )
    finally:
        connection.close()


def latest_experiment_id(database_path: Path = DEFAULT_DATABASE) -> str:
    """Resolve the latest experiment without migrating or opening for writes."""
    connection = duckdb.connect(str(Path(database_path).resolve()), read_only=True)
    try:
        row = connection.execute(
            "SELECT experiment_id FROM experiments ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()
        if row is None:
            raise RuntimeError("no POC 1 experiment is planned")
        return row[0]
    finally:
        connection.close()


def experiment_status(
    database_path: Path, experiment_id: str
) -> dict[str, Any]:
    """Read experiment metadata plus grouped task and invocation counts."""
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
                {"stage": row[0], "stage_name": STAGES[row[0]], "status": row[1], "count": row[2]}
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
    """Read stored experiment metadata and Process 01–06 state without opening for writes."""
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
    """Read full official evaluation records through a read-only connection."""
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
