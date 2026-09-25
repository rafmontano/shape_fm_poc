"""Versioned DuckDB schema and researcher-facing Stage 1 object interface."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb


SCHEMA_VERSION = 3
DEFAULT_DATABASE = Path("data/shapefm.duckdb")


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_versions (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    description VARCHAR NOT NULL
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
    median DOUBLE[] NOT NULL,
    quantile_levels DOUBLE[] NOT NULL,
    quantiles DOUBLE[][] NOT NULL,
    runtime_seconds DOUBLE,
    execution_metadata JSON NOT NULL,
    content_hash VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    UNIQUE (experiment_id, variant_id, forecast_instance_id, candidate)
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
            [SCHEMA_VERSION, "POC 1.1 hardware-aware local execution"],
        )
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    return path


class ShapeFMDatabase:
    """Read-only object interface to the canonical ShapeFM DuckDB database."""

    def __init__(self, path: Path, read_only: bool = True):
        self.path = path.resolve()
        if read_only and not self.path.is_file():
            raise FileNotFoundError(f"ShapeFM database does not exist: {self.path}")
        self._connection = duckdb.connect(str(self.path), read_only=read_only)

    @classmethod
    def open(
        cls, path: str | Path = DEFAULT_DATABASE, read_only: bool = True
    ) -> "ShapeFMDatabase":
        return cls(Path(path), read_only=read_only)

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> "ShapeFMDatabase":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _resolve_dataset(self, dataset: str, stage: str = "import") -> tuple[str, str, str]:
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
        return self._connection.execute(
            "SELECT max(version) FROM schema_versions"
        ).fetchone()[0]
