# ==============================================================================
# database.py
#
# Purpose: Versioned DuckDB schema and researcher-facing Stage 1 object interface.
# Inputs: A DuckDB path plus dataset, series, and stage identifiers.
# Outputs: Migrated schema state and immutable researcher-facing query records.
# Run from: Imported; not run directly.
# ==============================================================================

"""Versioned DuckDB schema and researcher-facing Stage 1 object interface."""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from .configuration import (
    PROCESS_NAMES,
    ExperimentConfiguration,
    canonical_json,
    load_experiment_configuration,
    resolve_experiment_configuration,
)


# Code constant: latest DuckDB migration version implemented by this source revision.
SCHEMA_VERSION = 7
# Bootstrap/interface default: legacy library database path; an explicit path from the
# coordinator overrides it, and the path does not define scientific identity.
DEFAULT_DATABASE = Path("data/shapefm.duckdb")


# Code constant: foundation DuckDB schema governed by SCHEMA_VERSION migrations.
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_versions (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    description VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS experiment_configuration (
    configuration_key VARCHAR PRIMARY KEY CHECK (configuration_key = 'experiment'),
    configuration_version INTEGER NOT NULL,
    experiment_name VARCHAR NOT NULL,
    experiment_date DATE NOT NULL,
    experiment_description VARCHAR NOT NULL,
    reproducibility_seed BIGINT NOT NULL,
    original_configuration JSON NOT NULL,
    resolved_configuration JSON NOT NULL,
    scientific_hash VARCHAR NOT NULL,
    configuration_integrity_hash VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS experiment_processes (
    process_id INTEGER PRIMARY KEY CHECK (process_id BETWEEN 1 AND 6),
    process_name VARCHAR NOT NULL,
    status VARCHAR NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    summary JSON,
    last_error VARCHAR
);

CREATE TABLE IF NOT EXISTS execution_events (
    execution_id VARCHAR PRIMARY KEY,
    requested_processes JSON NOT NULL,
    operational_configuration JSON NOT NULL,
    repository_revision VARCHAR,
    machine JSON NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    completed_at TIMESTAMPTZ,
    status VARCHAR NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    summary JSON,
    error VARCHAR
);

CREATE TABLE IF NOT EXISTS datasets (
    dataset_id VARCHAR PRIMARY KEY,
    dataset_name VARCHAR NOT NULL,
    source_system VARCHAR NOT NULL,
    source_revision VARCHAR NOT NULL,
    source_file_hashes JSON NOT NULL,
    import_configuration JSON NOT NULL,
    import_configuration_hash VARCHAR NOT NULL,
    frequency VARCHAR NOT NULL,
    source_metadata JSON,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS series (
    dataset_id VARCHAR NOT NULL,
    series_id VARCHAR NOT NULL,
    source_series_id VARCHAR NOT NULL,
    source_row INTEGER NOT NULL,
    frequency VARCHAR NOT NULL,
    start_timestamp TIMESTAMP NOT NULL,
    target FLOAT[] NOT NULL,
    observation_count INTEGER NOT NULL,
    content_hash VARCHAR NOT NULL,
    source_metadata JSON,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (dataset_id, series_id)
);

CREATE TABLE IF NOT EXISTS evaluation_windows (
    dataset_id VARCHAR NOT NULL,
    series_id VARCHAR NOT NULL,
    window_id VARCHAR NOT NULL,
    split_name VARCHAR NOT NULL,
    train_start INTEGER NOT NULL,
    train_end INTEGER NOT NULL,
    validation_start INTEGER NOT NULL,
    validation_end INTEGER NOT NULL,
    test_start INTEGER NOT NULL,
    test_end INTEGER NOT NULL,
    horizon INTEGER NOT NULL,
    boundary_convention VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (dataset_id, series_id, window_id)
);

CREATE TABLE IF NOT EXISTS runs (
    run_id VARCHAR PRIMARY KEY,
    stage VARCHAR NOT NULL,
    dataset_id VARCHAR NOT NULL,
    configuration JSON NOT NULL,
    configuration_hash VARCHAR NOT NULL,
    git_commit VARCHAR,
    environment JSON NOT NULL,
    machine JSON NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    status VARCHAR NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    summary JSON,
    error_summary VARCHAR
);

CREATE TABLE IF NOT EXISTS run_invocations (
    invocation_id VARCHAR PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    requested_max_series INTEGER,
    worker_count INTEGER NOT NULL,
    environment JSON NOT NULL,
    machine JSON NOT NULL,
    status VARCHAR NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    summary JSON,
    error VARCHAR
);

CREATE TABLE IF NOT EXISTS tasks (
    task_id VARCHAR PRIMARY KEY,
    run_id VARCHAR NOT NULL,
    stage VARCHAR NOT NULL,
    dataset_id VARCHAR NOT NULL,
    series_id VARCHAR NOT NULL,
    status VARCHAR NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    last_error VARCHAR,
    UNIQUE (run_id, series_id)
);

CREATE TABLE IF NOT EXISTS task_attempts (
    attempt_id VARCHAR PRIMARY KEY,
    task_id VARCHAR NOT NULL,
    attempt_number INTEGER NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    status VARCHAR NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    error VARCHAR,
    UNIQUE (task_id, attempt_number)
);
"""


# Code constant: benchmark DuckDB schema governed by SCHEMA_VERSION migrations.
POC1_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS benchmark_configurations (
    benchmark_configuration_id VARCHAR PRIMARY KEY,
    benchmark_revision VARCHAR NOT NULL,
    configuration_name VARCHAR NOT NULL,
    dataset_name VARCHAR NOT NULL,
    frequency VARCHAR NOT NULL,
    term VARCHAR NOT NULL,
    prediction_length INTEGER NOT NULL,
    window_count INTEGER NOT NULL,
    domain VARCHAR NOT NULL,
    num_variates INTEGER NOT NULL,
    metadata JSON NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    UNIQUE (benchmark_revision, configuration_name)
);

CREATE TABLE IF NOT EXISTS experiments (
    experiment_id VARCHAR PRIMARY KEY,
    benchmark_configuration_id VARCHAR NOT NULL,
    dataset_id VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    scientific_configuration JSON NOT NULL,
    configuration_hash VARCHAR NOT NULL,
    scope VARCHAR NOT NULL,
    status VARCHAR NOT NULL CHECK (status IN ('planned', 'running', 'completed', 'failed')),
    provisional_candidate JSON,
    configuration_version INTEGER,
    experiment_date DATE,
    description VARCHAR,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS experiment_variants (
    variant_id VARCHAR PRIMARY KEY,
    experiment_id VARCHAR NOT NULL,
    cleaning_method VARCHAR NOT NULL,
    transformation_method VARCHAR NOT NULL,
    adjustment_method VARCHAR NOT NULL,
    configuration JSON NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    UNIQUE (experiment_id, cleaning_method, transformation_method, adjustment_method)
);

CREATE TABLE IF NOT EXISTS forecast_instances (
    forecast_instance_id VARCHAR PRIMARY KEY,
    benchmark_configuration_id VARCHAR NOT NULL,
    dataset_id VARCHAR NOT NULL,
    series_id VARCHAR NOT NULL,
    variate_id VARCHAR NOT NULL,
    window_id VARCHAR NOT NULL,
    official_position INTEGER NOT NULL,
    context_start INTEGER NOT NULL,
    context_end INTEGER NOT NULL,
    actual_start INTEGER NOT NULL,
    actual_end INTEGER NOT NULL,
    horizon INTEGER NOT NULL,
    context_target FLOAT[] NOT NULL,
    actual_target FLOAT[] NOT NULL,
    identity_metadata JSON NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    UNIQUE (benchmark_configuration_id, series_id, variate_id, window_id)
);

CREATE TABLE IF NOT EXISTS experiment_invocations (
    invocation_id VARCHAR PRIMARY KEY,
    experiment_id VARCHAR NOT NULL,
    requested_gate VARCHAR NOT NULL,
    worker_count INTEGER NOT NULL,
    device VARCHAR,
    batch_size INTEGER,
    environment JSON NOT NULL,
    machine JSON NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    status VARCHAR NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    summary JSON,
    error VARCHAR,
    execution_profile VARCHAR,
    resolved_execution JSON,
    execution_overrides JSON,
    hardware JSON
);

CREATE TABLE IF NOT EXISTS experiment_tasks (
    task_id VARCHAR PRIMARY KEY,
    experiment_id VARCHAR NOT NULL,
    stage INTEGER NOT NULL CHECK (stage BETWEEN 2 AND 6),
    forecast_instance_id VARCHAR,
    variant_id VARCHAR,
    candidate VARCHAR,
    parent_task_id VARCHAR,
    status VARCHAR NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed', 'blocked')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    last_error VARCHAR
);

CREATE TABLE IF NOT EXISTS experiment_task_attempts (
    attempt_id VARCHAR PRIMARY KEY,
    task_id VARCHAR NOT NULL,
    invocation_id VARCHAR NOT NULL,
    attempt_number INTEGER NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    status VARCHAR NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    runtime_seconds DOUBLE,
    resource_usage JSON,
    error VARCHAR,
    UNIQUE (task_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS preprocessed_series (
    preprocessing_id VARCHAR PRIMARY KEY,
    experiment_id VARCHAR NOT NULL,
    forecast_instance_id VARCHAR NOT NULL,
    cleaning_method VARCHAR NOT NULL,
    input_hash VARCHAR NOT NULL,
    output_hash VARCHAR NOT NULL,
    context_target DOUBLE[] NOT NULL,
    method_configuration JSON NOT NULL,
    package_versions JSON NOT NULL,
    parent_result_id VARCHAR,
    official_frequency VARCHAR NOT NULL,
    official_seasonality INTEGER NOT NULL CHECK (official_seasonality > 0),
    preprocessing_status VARCHAR NOT NULL CHECK (preprocessing_status = 'success'),
    missing_count_before INTEGER NOT NULL CHECK (missing_count_before >= 0),
    missing_count_after INTEGER NOT NULL CHECK (missing_count_after >= 0),
    values_changed BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    UNIQUE (experiment_id, forecast_instance_id, cleaning_method)
);

CREATE TABLE IF NOT EXISTS transformed_series (
    transformation_id VARCHAR PRIMARY KEY,
    experiment_id VARCHAR NOT NULL,
    variant_id VARCHAR NOT NULL,
    forecast_instance_id VARCHAR NOT NULL,
    preprocessing_id VARCHAR NOT NULL,
    transformation_method VARCHAR NOT NULL,
    input_hash VARCHAR NOT NULL,
    output_hash VARCHAR NOT NULL,
    transformed_target DOUBLE[] NOT NULL,
    parameters JSON NOT NULL,
    parent_result_id VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    UNIQUE (experiment_id, variant_id, forecast_instance_id)
);

CREATE TABLE IF NOT EXISTS forecasts (
    forecast_id VARCHAR PRIMARY KEY,
    experiment_id VARCHAR NOT NULL,
    variant_id VARCHAR NOT NULL,
    forecast_instance_id VARCHAR NOT NULL,
    candidate VARCHAR NOT NULL,
    model_revision VARCHAR,
    parent_result_id VARCHAR,
    scale VARCHAR NOT NULL,
    mean DOUBLE[] NOT NULL,
    median DOUBLE[],
    quantile_levels DOUBLE[],
    quantiles DOUBLE[][],
    runtime_seconds DOUBLE,
    execution_metadata JSON NOT NULL,
    content_hash VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    forecast_capability VARCHAR NOT NULL DEFAULT 'probabilistic'
        CHECK (forecast_capability IN ('probabilistic', 'mean_only')),
    CHECK (
        (forecast_capability = 'probabilistic' AND median IS NOT NULL
            AND quantile_levels IS NOT NULL AND quantiles IS NOT NULL)
        OR
        (forecast_capability = 'mean_only' AND median IS NULL
            AND quantile_levels IS NULL AND quantiles IS NULL)
    ),
    UNIQUE (experiment_id, variant_id, forecast_instance_id, candidate)
);

CREATE TABLE IF NOT EXISTS reference_forecasts (
    reference_forecast_id VARCHAR PRIMARY KEY,
    dataset_id VARCHAR NOT NULL,
    series_id VARCHAR NOT NULL,
    official_m4_series_id VARCHAR NOT NULL,
    forecast_id VARCHAR NOT NULL,
    submission_id INTEGER NOT NULL,
    submission_rank INTEGER NOT NULL,
    submission_author VARCHAR NOT NULL,
    horizon INTEGER NOT NULL CHECK (horizon > 0),
    mean DOUBLE[] NOT NULL,
    point_semantics VARCHAR NOT NULL,
    forecast_capability VARCHAR NOT NULL CHECK (forecast_capability = 'mean_only'),
    source_metadata JSON NOT NULL,
    content_hash VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    UNIQUE (dataset_id, series_id, forecast_id)
);

CREATE TABLE IF NOT EXISTS forecast_components (
    forecast_id VARCHAR NOT NULL,
    component_forecast_id VARCHAR NOT NULL,
    component_name VARCHAR NOT NULL,
    weight DOUBLE NOT NULL,
    PRIMARY KEY (forecast_id, component_forecast_id)
);

CREATE TABLE IF NOT EXISTS official_evaluations (
    evaluation_id VARCHAR PRIMARY KEY,
    experiment_id VARCHAR NOT NULL,
    variant_id VARCHAR NOT NULL,
    candidate VARCHAR NOT NULL,
    benchmark_configuration_id VARCHAR NOT NULL,
    evaluator VARCHAR NOT NULL,
    evaluator_revision VARCHAR NOT NULL,
    options JSON NOT NULL,
    metrics JSON NOT NULL,
    evaluation_input_count INTEGER NOT NULL,
    forecast_input_fingerprint VARCHAR NOT NULL,
    is_complete_manifest BOOLEAN NOT NULL,
    is_submittable BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    UNIQUE (experiment_id, variant_id, candidate, benchmark_configuration_id)
);

CREATE TABLE IF NOT EXISTS submission_exports (
    export_id VARCHAR PRIMARY KEY,
    experiment_id VARCHAR NOT NULL,
    model_name VARCHAR NOT NULL,
    output_directory VARCHAR NOT NULL,
    manifest_revision VARCHAR NOT NULL,
    validation JSON NOT NULL,
    is_submittable BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);
"""


@dataclass(frozen=True)
class EvaluationWindow:
    """Purpose: Describe persisted train, validation, and test slices for one series.

    Inputs: Window/split IDs, observation-offset boundaries, horizon, and convention.
    Outputs: Immutable evaluation-window state returned with a ``TimeSeries``.
    Notes: Boundaries are observation offsets; horizon is observations per holdout slice.
    """
    window_id: str
    split_name: str
    train_start: int
    train_end: int
    validation_start: int
    validation_end: int
    test_start: int
    test_end: int
    horizon: int
    boundary_convention: str


@dataclass(frozen=True)
class TimeSeries:
    """Purpose: Expose one canonical stored series and its evaluation windows.

    Inputs: Dataset/source identities, frequency and start time, target observations,
    count/content digest, and associated windows.
    Outputs: Immutable researcher-facing scientific series state.
    """
    dataset_id: str
    dataset_name: str
    series_id: str
    source_series_id: str
    frequency: str
    start_timestamp: Any
    target: tuple[float, ...]
    observation_count: int
    content_hash: str
    evaluation_windows: tuple[EvaluationWindow, ...]


@dataclass(frozen=True)
class StageStatus:
    """Purpose: Summarize the latest persisted run for one dataset stage.

    Inputs: Stage/dataset/run identities, run state, invocation/task counts, and volume.
    Outputs: Immutable researcher-facing operational status.
    """
    stage: str
    dataset_id: str
    dataset_name: str
    run_id: str
    run_status: str
    invocation_count: int
    task_counts: dict[str, int]
    series_count: int
    observation_count: int


def migrate_database(path: Path = DEFAULT_DATABASE) -> Path:
    """Purpose: Create or transactionally upgrade the versioned DuckDB schema.

    Inputs: Destination database path; missing parent directories are created.
    Outputs: Absolute database path after all schema-version rows commit.
    """
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(path))
    try:
        connection.execute("BEGIN TRANSACTION")
        connection.execute(SCHEMA_SQL)
        connection.execute(POC1_SCHEMA_SQL)
        connection.execute(
            "ALTER TABLE experiment_invocations ADD COLUMN IF NOT EXISTS execution_profile VARCHAR"
        )
        connection.execute(
            "ALTER TABLE experiment_invocations ADD COLUMN IF NOT EXISTS resolved_execution JSON"
        )
        connection.execute(
            "ALTER TABLE experiment_invocations ADD COLUMN IF NOT EXISTS execution_overrides JSON"
        )
        connection.execute(
            "ALTER TABLE experiment_invocations ADD COLUMN IF NOT EXISTS hardware JSON"
        )
        connection.execute(
            "ALTER TABLE official_evaluations ADD COLUMN IF NOT EXISTS evaluation_input_count INTEGER"
        )
        connection.execute(
            "ALTER TABLE official_evaluations ADD COLUMN IF NOT EXISTS forecast_input_fingerprint VARCHAR"
        )
        connection.execute(
            "ALTER TABLE experiments ADD COLUMN IF NOT EXISTS configuration_version INTEGER"
        )
        connection.execute(
            "ALTER TABLE experiments ADD COLUMN IF NOT EXISTS experiment_date DATE"
        )
        connection.execute(
            "ALTER TABLE experiments ADD COLUMN IF NOT EXISTS description VARCHAR"
        )
        connection.execute(
            "ALTER TABLE experiment_configuration ADD COLUMN IF NOT EXISTS reproducibility_seed BIGINT"
        )
        connection.execute(
            "ALTER TABLE experiment_configuration ADD COLUMN IF NOT EXISTS configuration_integrity_hash VARCHAR"
        )
        connection.execute("ALTER TABLE forecasts ALTER median DROP NOT NULL")
        connection.execute("ALTER TABLE forecasts ALTER quantile_levels DROP NOT NULL")
        connection.execute("ALTER TABLE forecasts ALTER quantiles DROP NOT NULL")
        connection.execute(
            "ALTER TABLE forecasts ADD COLUMN IF NOT EXISTS forecast_capability "
            "VARCHAR DEFAULT 'probabilistic'"
        )
        connection.execute(
            "ALTER TABLE forecasts ALTER forecast_capability SET NOT NULL"
        )
        # Version 7 keeps historical rows readable: newly added provenance columns
        # are nullable on upgraded databases and fully populated for new results.
        for definition in (
            "official_frequency VARCHAR",
            "official_seasonality INTEGER",
            "preprocessing_status VARCHAR",
            "missing_count_before INTEGER",
            "missing_count_after INTEGER",
            "values_changed BOOLEAN",
        ):
            connection.execute(
                f"ALTER TABLE preprocessed_series ADD COLUMN IF NOT EXISTS {definition}"
            )
        connection.execute(
            "INSERT INTO schema_versions (version, description) VALUES (?, ?) "
            "ON CONFLICT (version) DO NOTHING",
            [1, "Foundation Stage 1 canonical GIFT-Eval import"],
        )
        connection.execute(
            "INSERT INTO schema_versions (version, description) VALUES (?, ?) "
            "ON CONFLICT (version) DO NOTHING",
            [2, "POC 1 official GIFT-Eval experiment pipeline"],
        )
        connection.execute(
            "INSERT INTO schema_versions (version, description) VALUES (?, ?) "
            "ON CONFLICT (version) DO NOTHING",
            [3, "POC 1.1 hardware-aware local execution"],
        )
        connection.execute(
            "INSERT INTO schema_versions (version, description) VALUES (?, ?) "
            "ON CONFLICT (version) DO NOTHING",
            [4, "POC 1 full-scope evaluation provenance"],
        )
        connection.execute(
            "INSERT INTO schema_versions (version, description) VALUES (?, ?) "
            "ON CONFLICT (version) DO NOTHING",
            [5, "POC 2 authoritative experiment configuration"],
        )
        connection.execute(
            "INSERT INTO schema_versions (version, description) VALUES (?, ?) "
            "ON CONFLICT (version) DO NOTHING",
            [6, "POC 2 archived M4 point forecasts and capabilities"],
        )
        connection.execute(
            "INSERT INTO schema_versions (version, description) VALUES (?, ?) "
            "ON CONFLICT (version) DO NOTHING",
            [SCHEMA_VERSION, "Gate 1 missingness and Gate 2 preprocessing provenance"],
        )
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    return path


def initialize_experiment_database(
    path: Path, configuration_path: Path
) -> ExperimentConfiguration:
    """Purpose: Atomically create a database with its authoritative configuration.

    Inputs: New database path and researcher-authored configuration JSON path.
    Outputs: Validated configuration after schema, configuration, and six process rows
    commit in a sibling temporary database and are atomically renamed into place.
    """
    configuration = load_experiment_configuration(configuration_path)
    target = path.resolve()
    if target.exists():
        raise FileExistsError(f"experiment database already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    try:
        migrate_database(temporary)
        connection = duckdb.connect(str(temporary))
        try:
            connection.execute("BEGIN TRANSACTION")
            connection.execute(
                """INSERT INTO experiment_configuration
                (configuration_key, configuration_version, experiment_name,
                 experiment_date, experiment_description, reproducibility_seed,
                 original_configuration, resolved_configuration, scientific_hash,
                 configuration_integrity_hash)
                VALUES ('experiment', ?, ?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?)""",
                [
                    configuration.version,
                    configuration.name,
                    configuration.date,
                    configuration.description,
                    configuration.seed,
                    canonical_json(configuration.original),
                    canonical_json(configuration.resolved),
                    configuration.scientific_hash,
                    configuration.configuration_integrity_hash,
                ],
            )
            connection.executemany(
                "INSERT INTO experiment_processes (process_id, process_name, status) VALUES (?, ?, 'pending')",
                list(PROCESS_NAMES.items()),
            )
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        os.replace(temporary, target)
        return configuration
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def load_database_configuration(
    database_path: Path, connection: duckdb.DuckDBPyConnection | None = None
) -> ExperimentConfiguration:
    """Purpose: Reconstruct and integrity-check authoritative configuration in DuckDB.

    Inputs: Database path and optional caller-owned open connection.
    Outputs: Validated configuration whose metadata, resolved JSON, and hashes match;
    missing or inconsistent persisted state raises an exception.
    """
    owned = connection is None
    if owned:
        path = database_path.resolve()
        if not path.is_file():
            raise FileNotFoundError(f"experiment database does not exist: {path}")
        connection = duckdb.connect(str(path), read_only=True)
    try:
        row = connection.execute(
            """SELECT configuration_version, reproducibility_seed, experiment_name,
                      CAST(experiment_date AS VARCHAR), experiment_description,
                      original_configuration, resolved_configuration,
                      scientific_hash, configuration_integrity_hash
               FROM experiment_configuration WHERE configuration_key='experiment'"""
        ).fetchone()
        if row is None:
            raise RuntimeError("database has no authoritative experiment configuration")
        original = json.loads(row[5])
        stored_resolved = json.loads(row[6])
        configuration = resolve_experiment_configuration(original)
        if configuration.version != row[0]:
            raise RuntimeError("stored configuration version does not match original JSON")
        if configuration.seed != row[1]:
            raise RuntimeError("stored reproducibility seed does not match original JSON")
        if (configuration.name, configuration.date, configuration.description) != row[2:5]:
            raise RuntimeError("stored experiment metadata does not match original JSON")
        if configuration.resolved != stored_resolved:
            raise RuntimeError("stored resolved configuration does not match configuration version rules")
        if configuration.scientific_hash != row[7]:
            raise RuntimeError("stored scientific configuration hash is invalid")
        if configuration.configuration_integrity_hash != row[8]:
            raise RuntimeError("stored configuration-integrity hash is invalid")
        return configuration
    finally:
        if owned:
            connection.close()


class ShapeFMDatabase:
    """Purpose: Own an open DuckDB connection for researcher-facing queries.

    Inputs: Resolved database path and explicit read-only/writable mode.
    Outputs: Canonical series, run status, and schema-version query operations.
    Notes: The instance owns and closes its retained connection.
    """

    def __init__(self, path: Path, read_only: bool = True):
        """Purpose: Open and retain the database connection owned by this interface.

        Inputs: Database path and read-only flag; read-only mode requires an existing file.
        Outputs: Initialized connection state; missing files or DuckDB errors propagate.
        """
        self.path = path.resolve()
        if read_only and not self.path.is_file():
            raise FileNotFoundError(f"ShapeFM database does not exist: {self.path}")
        self._connection = duckdb.connect(str(self.path), read_only=read_only)

    @classmethod
    def open(
        cls, path: str | Path = DEFAULT_DATABASE, read_only: bool = True
    ) -> "ShapeFMDatabase":
        """Construct a database interface from a string or path, read-only by default."""
        return cls(Path(path), read_only=read_only)

    def close(self) -> None:
        """Close the retained DuckDB connection."""
        self._connection.close()

    def __enter__(self) -> "ShapeFMDatabase":
        """Return this open interface for context-manager use."""
        return self

    def __exit__(self, *_: object) -> None:
        """Close the DuckDB connection when leaving a context."""
        self.close()

    def _resolve_dataset(self, dataset: str, stage: str = "import") -> tuple[str, str, str]:
        """Purpose: Resolve a dataset selector to its latest run for a stage.

        Inputs: Dataset name or ID and persisted stage name.
        Outputs: Dataset ID, dataset name, and latest run ID; raises ``KeyError`` if absent.
        """
        row = self._connection.execute(
            """
            SELECT d.dataset_id, d.dataset_name, r.run_id
            FROM datasets d
            JOIN runs r ON r.dataset_id = d.dataset_id
            WHERE (d.dataset_name = ? OR d.dataset_id = ?) AND r.stage = ?
            ORDER BY r.ended_at DESC NULLS LAST, r.started_at DESC
            LIMIT 1
            """,
            [dataset, dataset, stage],
        ).fetchone()
        if row is None:
            raise KeyError(f"no {stage!r} dataset found for {dataset!r}")
        return row[0], row[1], row[2]

    def get_series(self, dataset: str, series_id: str) -> TimeSeries:
        """Purpose: Retrieve a canonical series with ordered evaluation windows.

        Inputs: Dataset name/ID and series ID.
        Outputs: ``TimeSeries`` with persisted target values and windows; raises
        ``KeyError`` when the dataset run or series is absent.
        """
        dataset_id, dataset_name, _ = self._resolve_dataset(dataset)
        row = self._connection.execute(
            """
            SELECT series_id, source_series_id, frequency, start_timestamp,
                   target, observation_count, content_hash
            FROM series WHERE dataset_id = ? AND series_id = ?
            """,
            [dataset_id, str(series_id)],
        ).fetchone()
        if row is None:
            raise KeyError(f"series {series_id!r} was not found in {dataset!r}")
        window_rows = self._connection.execute(
            """
            SELECT window_id, split_name, train_start, train_end,
                   validation_start, validation_end, test_start,
                   test_end, horizon, boundary_convention
            FROM evaluation_windows
            WHERE dataset_id = ? AND series_id = ? ORDER BY window_id
            """,
            [dataset_id, str(series_id)],
        ).fetchall()
        return TimeSeries(
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            series_id=row[0],
            source_series_id=row[1],
            frequency=row[2],
            start_timestamp=row[3],
            target=tuple(row[4]),
            observation_count=row[5],
            content_hash=row[6],
            evaluation_windows=tuple(EvaluationWindow(*values) for values in window_rows),
        )

    def stage_status(self, stage: str, dataset: str) -> StageStatus:
        """Purpose: Summarize persisted execution and scientific volume for a stage.

        Inputs: Stage name and dataset name/ID.
        Outputs: Latest run state, invocation/task counts, and series/observation totals.
        """
        dataset_id, dataset_name, run_id = self._resolve_dataset(dataset, stage)
        run_status = self._connection.execute(
            "SELECT status FROM runs WHERE run_id = ?", [run_id]
        ).fetchone()[0]
        invocation_count = self._connection.execute(
            "SELECT count(*) FROM run_invocations WHERE run_id = ?", [run_id]
        ).fetchone()[0]
        counts = dict(
            self._connection.execute(
                "SELECT status, count(*) FROM tasks WHERE run_id = ? GROUP BY status",
                [run_id],
            ).fetchall()
        )
        series_count, observations = self._connection.execute(
            "SELECT count(*), coalesce(sum(observation_count), 0) "
            "FROM series WHERE dataset_id = ?",
            [dataset_id],
        ).fetchone()
        return StageStatus(
            stage=stage,
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            run_id=run_id,
            run_status=run_status,
            invocation_count=invocation_count,
            task_counts={name: int(count) for name, count in counts.items()},
            series_count=series_count,
            observation_count=observations,
        )

    def schema_version(self) -> int:
        """Return the highest migration version recorded in ``schema_versions``."""
        return self._connection.execute(
            "SELECT max(version) FROM schema_versions"
        ).fetchone()[0]
