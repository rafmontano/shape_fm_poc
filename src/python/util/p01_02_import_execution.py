# ==============================================================================
# p01_02_import_execution.py
#
# Purpose: Single-writer Process 01 coordinator with sequential and local worker modes.
# Inputs: Pinned M4 Daily source files, import configuration, source revision, and worker count.
# Outputs: Canonical series/windows plus restartable run, task, attempt, and invocation rows in DuckDB.
# Run from: Imported; not run directly.
# ==============================================================================

"""Single-writer Process 01 coordinator with sequential and local worker modes."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import math
import multiprocessing
import os
import platform
import struct
import subprocess
import sys
import uuid
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import duckdb

from .shared_configuration import (
    ImportValidationError,
    ExperimentConfiguration,
    canonical_import_configuration,
    canonical_json,
    dataset_identity,
    evaluation_window,
    json_fingerprint,
)
from .shared_database import load_database_configuration, migrate_database
from .p01_03_gift_eval_source import (
    ConfiguredGiftEvalSource,
    iter_source_series,
    source_fingerprint,
    source_metadata,
)
from .shared_provenance import sha256_file, utc_now


# Code constant: persistent stage identifier in import run and attempt records.
STAGE = "import"


@dataclass(frozen=True)
class SeriesTask:
    """Purpose: Carry one source series and window contract to an import worker.

    Inputs: Task/dataset/source identities, ordered observations, frequency/start time,
    forecast horizon, window ID, and boundary convention.
    Outputs: Immutable, pickle-safe worker input with no database ownership.
    """
    task_id: str
    dataset_id: str
    series_id: str
    source_series_id: str
    source_row: int
    frequency: str
    start_timestamp: Any
    target: tuple[float | None, ...]
    horizon: int
    window_id: str
    boundary_convention: str


@dataclass(frozen=True)
class SeriesResult:
    """Purpose: Carry canonical series calculations back to the single writer.

    Inputs: Task/source metadata, observations and count, float32-content digest, and
    computed evaluation-window mapping.
    Outputs: Immutable worker result ready for transactional persistence.
    """
    task_id: str
    dataset_id: str
    series_id: str
    source_series_id: str
    source_row: int
    frequency: str
    start_timestamp: Any
    target: tuple[float | None, ...]
    observation_count: int
    content_hash: str
    window: dict[str, Any]


@dataclass(frozen=True)
class WorkerOutcome:
    """Purpose: Transfer worker success or failure without crossing exception objects.

    Inputs: Task ID and exactly one useful result or formatted error string.
    Outputs: Pickle-safe outcome consumed by the coordinator.
    """
    task_id: str
    result: SeriesResult | None
    error: str | None


def compute_series(task: SeriesTask) -> SeriesResult:
    """Purpose: Compute canonical content identity and holdout boundaries for a series.

    Inputs: Immutable task containing source float values and the benchmark horizon.
    Outputs: ``SeriesResult`` with SHA-256 over little-endian float32 bytes and one
    zero-based, end-exclusive validation/test window.
    Notes: Performs no database access and cross-checks window logic centrally.
    """
    content_hash = target_content_hash(task.target)
    window = {
        "window_id": task.window_id,
        "split_name": "validation_and_test",
        "train_start": 0,
        "train_end": len(task.target) - 2 * task.horizon,
        "validation_start": len(task.target) - 2 * task.horizon,
        "validation_end": len(task.target) - task.horizon,
        "test_start": len(task.target) - task.horizon,
        "test_end": len(task.target),
        "horizon": task.horizon,
        "boundary_convention": task.boundary_convention,
    }
    if window != evaluation_window(
        len(task.target),
        {
            "benchmark": {
                "prediction_length": task.horizon,
                "boundary_convention": task.boundary_convention,
            }
        },
        window_id=task.window_id,
    ):
        raise ImportValidationError("worker evaluation-window calculation diverged")
    return SeriesResult(
        task_id=task.task_id,
        dataset_id=task.dataset_id,
        series_id=task.series_id,
        source_series_id=task.source_series_id,
        source_row=task.source_row,
        frequency=task.frequency,
        start_timestamp=task.start_timestamp,
        target=task.target,
        observation_count=len(task.target),
        content_hash=content_hash,
        window=window,
    )


def target_content_hash(target: Iterable[float | None]) -> str:
    """Hash raw source values while preserving the distinct Arrow-null/NaN contract."""
    values = tuple(target)
    if all(value is not None and not math.isnan(value) for value in values):
        packed = struct.pack(f"<{len(values)}f", *values)
    else:
        parts = []
        for value in values:
            if value is None:
                parts.append(b"N")
            elif math.isnan(value):
                parts.append(b"A")
            else:
                parts.append(b"V" + struct.pack("<f", value))
        packed = b"".join(parts)
    return hashlib.sha256(packed).hexdigest()


def worker_entry(task: SeriesTask) -> WorkerOutcome:
    """Purpose: Isolate one worker computation and serialize failures as data.

    Inputs: One ``SeriesTask``.
    Outputs: ``WorkerOutcome`` containing the result or exception type/message.
    """
    try:
        return WorkerOutcome(task.task_id, compute_series(task), None)
    except BaseException as exc:
        return WorkerOutcome(task.task_id, None, f"{type(exc).__name__}: {exc}")


def repository_root() -> Path:
    """Return the repository root containing this utility module."""
    return Path(__file__).resolve().parents[3]


def command_output(command: list[str], timeout_seconds: float = 30.0) -> str | None:
    """Purpose: Best-effort capture a subprocess version string for provenance.

    Inputs: Argument vector and timeout in seconds; execution uses the repository root.
    Outputs: Stripped stdout, or ``None`` on launch, nonzero exit, or timeout.
    """
    try:
        return subprocess.run(
            command,
            cwd=repository_root(),
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None


def execution_provenance() -> tuple[dict[str, Any], dict[str, Any]]:
    """Purpose: Snapshot software and machine identity for an import invocation.

    Inputs: Installed Python/R/tool environments, lockfile, and current host state.
    Outputs: Environment and machine mappings suitable for canonical JSON persistence.
    """
    root = repository_root()
    packages = {}
    for name in ("shape-fm-poc", "duckdb", "pyarrow"):
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            packages[name] = "unknown"
    environment = {
        "python": platform.python_version(),
        "python_executable": sys.executable,
        "python_packages": packages,
        "r": command_output(
            [
                "Rscript",
                "-e",
                "cat(R.version.string, '; renv=', as.character(packageVersion('renv')), "
                "'; DBI=', as.character(packageVersion('DBI')), "
                "'; duckdb=', as.character(packageVersion('duckdb')))",
            ]
        ),
        "uv": command_output([str(root / ".tools/uv/uv"), "--version"]),
        "uv_lock_sha256": sha256_file(root / "uv.lock"),
    }
    machine = {
        "system": platform.system(),
        "release": platform.release(),
        "architecture": platform.machine(),
        "node": platform.node(),
    }
    return environment, machine


class ImportCoordinator:
    """Purpose: Own Process 01 orchestration and the sole DuckDB writer connection.

    Inputs: Initialized experiment database with authoritative configuration.
    Outputs: Restartable task/attempt state and atomically persisted series/windows.
    Notes: Worker processes are side-effect free; this instance owns database writes.
    """

    def __init__(
        self,
        database_path: Path,
        configuration: ExperimentConfiguration | None = None,
    ):
        """Purpose: Acquire coordinator-owned database and configuration state.

        Inputs: Path to an existing initialized experiment database.
        Outputs: Open writable DuckDB connection and validated authoritative configuration.
        """
        if not Path(database_path).resolve().is_file():
            raise FileNotFoundError(
                f"experiment database does not exist: {Path(database_path).resolve()}"
            )
        self.database_path = migrate_database(database_path)
        self.connection = duckdb.connect(str(self.database_path))
        self.configuration = configuration or load_database_configuration(
            self.database_path, self.connection
        )

    def close(self) -> None:
        """Close the owned DuckDB connection."""
        self.connection.close()

    def __enter__(self) -> "ImportCoordinator":
        """Return the open coordinator for context-managed imports."""
        return self

    def __exit__(self, *_: object) -> None:
        """Close the DuckDB connection on context exit."""
        self.close()

    def _prepare(
        self,
        dataset_id: str,
        config_hash: str,
        config: dict[str, Any],
        source_revision: str,
        source_info: dict[str, Any],
        metadata: dict[str, Any],
        workers: int,
    ) -> tuple[str, str]:
        """Purpose: Prepare persistent state for a restartable import invocation.

        Inputs: Dataset/config/source identity and metadata plus worker count.
        Outputs: Deterministic run ID and new invocation ID after provenance upserts and
        reset of interrupted tasks/attempts.
        """
        run_id = f"import/{json_fingerprint({'dataset_id': dataset_id, 'stage': STAGE})[:24]}"
        invocation_id = f"invocation/{uuid.uuid4().hex}"
        environment, machine = execution_provenance()
        canonical_config = canonical_import_configuration(config)
        self.connection.execute(
            """
            INSERT INTO datasets (
                dataset_id, dataset_name, source_system, source_revision,
                source_file_hashes, import_configuration,
                import_configuration_hash, frequency, source_metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (dataset_id) DO NOTHING
            """,
            [
                dataset_id,
                config["dataset_name"],
                config["source_system"],
                source_revision,
                canonical_json(source_info["files"]),
                canonical_json(canonical_config),
                config_hash,
                config["benchmark"]["frequency"],
                canonical_json(metadata),
            ],
        )
        self.connection.execute(
            """
            INSERT INTO runs (
                run_id, stage, dataset_id, configuration, configuration_hash,
                git_commit, environment, machine, started_at, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'running')
            ON CONFLICT (run_id) DO UPDATE SET
                status = 'running', ended_at = NULL, error_summary = NULL,
                environment = excluded.environment, machine = excluded.machine
            """,
            [
                run_id,
                STAGE,
                dataset_id,
                canonical_json(canonical_config),
                config_hash,
                command_output(["git", "rev-parse", "HEAD"]),
                canonical_json(environment),
                canonical_json(machine),
                utc_now(),
            ],
        )
        self.connection.execute(
            """
            INSERT INTO run_invocations (
                invocation_id, run_id, started_at, requested_max_series,
                worker_count, environment, machine, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'running')
            """,
            [
                invocation_id,
                run_id,
                utc_now(),
                config["max_series"],
                workers,
                canonical_json(environment),
                canonical_json(machine),
            ],
        )
        self.connection.execute(
            """
            UPDATE task_attempts SET status = 'failed', ended_at = current_timestamp,
                error = 'interrupted before completion'
            WHERE status = 'running' AND task_id IN
                (SELECT task_id FROM tasks WHERE run_id = ?)
            """,
            [run_id],
        )
        self.connection.execute(
            """
            UPDATE tasks SET status = 'pending', updated_at = current_timestamp,
                last_error = 'interrupted before completion'
            WHERE run_id = ? AND status = 'running'
            """,
            [run_id],
        )
        return run_id, invocation_id

    def _register_task(self, run_id: str, dataset_id: str, series_id: str) -> tuple[str, str]:
        """Purpose: Idempotently register one deterministic per-series task.

        Inputs: Run, dataset, and series IDs.
        Outputs: Task ID and persisted status used to skip completed work.
        """
        task_id = f"task/{json_fingerprint({'run_id': run_id, 'series_id': series_id})[:32]}"
        self.connection.execute(
            """
            INSERT INTO tasks (task_id, run_id, stage, dataset_id, series_id, status)
            VALUES (?, ?, ?, ?, ?, 'pending') ON CONFLICT (task_id) DO NOTHING
            """,
            [task_id, run_id, STAGE, dataset_id, series_id],
        )
        status = self.connection.execute(
            "SELECT status FROM tasks WHERE task_id = ?", [task_id]
        ).fetchone()[0]
        return task_id, status

    def _start_attempt(self, task_id: str) -> int:
        """Purpose: Transactionally begin the next attempt for a task.

        Inputs: Existing task ID.
        Outputs: Incremented attempt number after task and attempt state commit.
        """
        self.connection.execute("BEGIN TRANSACTION")
        try:
            self.connection.execute(
                """
                UPDATE tasks SET status = 'running', attempt_count = attempt_count + 1,
                    started_at = current_timestamp, completed_at = NULL,
                    updated_at = current_timestamp, last_error = NULL
                WHERE task_id = ?
                """,
                [task_id],
            )
            attempt = self.connection.execute(
                "SELECT attempt_count FROM tasks WHERE task_id = ?", [task_id]
            ).fetchone()[0]
            self.connection.execute(
                """
                INSERT INTO task_attempts (
                    attempt_id, task_id, attempt_number, started_at, status
                ) VALUES (?, ?, ?, current_timestamp, 'running')
                """,
                [f"{task_id}/attempt/{attempt}", task_id, attempt],
            )
            self.connection.execute("COMMIT")
            return attempt
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise

    def _record_failure(self, task_id: str, attempt: int, error: str) -> None:
        """Purpose: Transactionally persist a task-attempt failure.

        Inputs: Task ID, attempt number, and formatted error text.
        Outputs: ``None`` after task and attempt rows share failed state.
        """
        self.connection.execute("BEGIN TRANSACTION")
        try:
            self.connection.execute(
                """
                UPDATE tasks SET status = 'failed', updated_at = current_timestamp,
                    last_error = ? WHERE task_id = ?
                """,
                [error, task_id],
            )
            self.connection.execute(
                """
                UPDATE task_attempts SET status = 'failed', ended_at = current_timestamp,
                    error = ? WHERE attempt_id = ?
                """,
                [error, f"{task_id}/attempt/{attempt}"],
            )
            self.connection.execute("COMMIT")
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise

    def start_import_task(self, task: SeriesTask) -> int:
        """Start and durably account for one prepared import task attempt."""
        return self._start_attempt(task.task_id)

    def accept_import_result(
        self, result: SeriesResult, attempt: int, summary: dict[str, Any]
    ) -> None:
        """Atomically accept one result and update this invocation's success count."""
        self._commit_result(result, attempt)
        summary["completed_this_invocation"] += 1

    def fail_import_task(
        self, task: SeriesTask, attempt: int, error: BaseException, summary: dict[str, Any]
    ) -> None:
        """Atomically fail one attempt and update this invocation's failure count."""
        self._record_failure(task.task_id, attempt, f"{type(error).__name__}: {error}")
        summary["failed_this_invocation"] += 1

    def _commit_result(self, result: SeriesResult, attempt: int) -> None:
        """Purpose: Atomically persist scientific output and complete its task attempt.

        Inputs: Computed series result and active attempt number.
        Outputs: ``None`` after idempotent series/window insertion and status updates;
        conflicting canonical data raises ``ImportValidationError`` and rolls back.
        """
        self.connection.execute("BEGIN TRANSACTION")
        try:
            existing = self.connection.execute(
                """
                SELECT source_series_id, source_row, frequency, observation_count,
                       content_hash FROM series WHERE dataset_id = ? AND series_id = ?
                """,
                [result.dataset_id, result.series_id],
            ).fetchone()
            expected = (
                result.source_series_id,
                result.source_row,
                result.frequency,
                result.observation_count,
                result.content_hash,
            )
            if existing is None:
                self.connection.execute(
                    """
                    INSERT INTO series (
                        dataset_id, series_id, source_series_id, source_row,
                        frequency, start_timestamp, target, observation_count,
                        content_hash, source_metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        result.dataset_id,
                        result.series_id,
                        result.source_series_id,
                        result.source_row,
                        result.frequency,
                        result.start_timestamp,
                        list(result.target),
                        result.observation_count,
                        result.content_hash,
                        canonical_json({"source_row": result.source_row}),
                    ],
                )
            elif tuple(existing) != expected:
                raise ImportValidationError(
                    f"existing canonical series differs for {result.series_id}"
                )

            window = result.window
            existing_window = self.connection.execute(
                """
                SELECT split_name, train_start, train_end, validation_start,
                       validation_end, test_start, test_end, horizon,
                       boundary_convention
                FROM evaluation_windows
                WHERE dataset_id = ? AND series_id = ? AND window_id = ?
                """,
                [result.dataset_id, result.series_id, window["window_id"]],
            ).fetchone()
            expected_window = (
                window["split_name"],
                window["train_start"],
                window["train_end"],
                window["validation_start"],
                window["validation_end"],
                window["test_start"],
                window["test_end"],
                window["horizon"],
                window["boundary_convention"],
            )
            if existing_window is None:
                self.connection.execute(
                    """
                    INSERT INTO evaluation_windows (
                        dataset_id, series_id, window_id, split_name,
                        train_start, train_end, validation_start,
                        validation_end, test_start, test_end, horizon,
                        boundary_convention
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        result.dataset_id,
                        result.series_id,
                        window["window_id"],
                        *expected_window,
                    ],
                )
            elif tuple(existing_window) != expected_window:
                raise ImportValidationError(
                    f"existing evaluation window differs for {result.series_id}"
                )

            self.connection.execute(
                """
                UPDATE tasks SET status = 'completed', completed_at = current_timestamp,
                    updated_at = current_timestamp, last_error = NULL WHERE task_id = ?
                """,
                [result.task_id],
            )
            self.connection.execute(
                """
                UPDATE task_attempts SET status = 'completed', ended_at = current_timestamp
                WHERE attempt_id = ?
                """,
                [f"{result.task_id}/attempt/{attempt}"],
            )
            self.connection.execute("COMMIT")
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise

    def _process_batch(
        self,
        tasks: list[SeriesTask],
        executor: ProcessPoolExecutor | None,
    ) -> tuple[int, int]:
        """Purpose: Execute and persist one bounded batch of series tasks.

        Inputs: Tasks and optional process-pool executor (``None`` means local mapping).
        Outputs: Counts of committed successes and recorded failures.
        """
        attempts = {task.task_id: self._start_attempt(task.task_id) for task in tasks}
        outcomes: Iterable[WorkerOutcome]
        outcomes = (
            map(worker_entry, tasks)
            if executor is None
            else executor.map(worker_entry, tasks)
        )
        completed = failed = 0
        for outcome in outcomes:
            attempt = attempts[outcome.task_id]
            if outcome.error is not None or outcome.result is None:
                self._record_failure(outcome.task_id, attempt, outcome.error or "worker failed")
                failed += 1
                continue
            try:
                self._commit_result(outcome.result, attempt)
                completed += 1
            except BaseException as exc:
                self._record_failure(
                    outcome.task_id, attempt, f"{type(exc).__name__}: {exc}"
                )
                failed += 1
        return completed, failed

    def _read_m4_reference_forecasts(
        self, dataset_id: str, config: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Purpose: Ask the bounded R adapter for selected official M4 point forecasts.

        Inputs: Imported dataset identity and authoritative Stage 1 settings.
        Outputs: Validated adapter records for selected series/providers; the subprocess
        reads package data only and neither it nor this method writes DuckDB.
        """
        archived = config.get("archived_forecasts")
        if archived is None:
            return []
        enabled = list(archived["enabled"])
        if not enabled:
            return []
        horizon = int(config["benchmark"]["prediction_length"])
        rows = self.connection.execute(
            """SELECT series_id, source_row, frequency, target
               FROM series WHERE dataset_id=? ORDER BY source_row
               LIMIT ?""",
            [dataset_id, config["max_series"]],
        ).fetchall()
        if any(
            str(row[0]) != str(row[1])
            or row[2] != config["benchmark"]["frequency"]
            or len(row[3]) <= horizon
            for row in rows
        ):
            raise ImportValidationError(
                "canonical series identities, frequency, or lengths are incompatible with M4 Daily"
            )
        payload = {
            "action": "read",
            "dataset_id": dataset_id,
            "dataset_name": config["dataset_name"],
            "forecast_ids": enabled,
            "expected_frequency": config["benchmark"]["frequency"],
            "expected_horizon": horizon,
            "series": [
                {
                    "series_id": row[0],
                    "source_position": row[1],
                    "official_m4_series_id": f"D{row[1] + 1}",
                    "history": list(row[3][:-horizon]),
                    "future": list(row[3][-horizon:]),
                    "horizon": horizon,
                }
                for row in rows
            ],
        }
        worker = self.configuration.execution_paths["r_m4comp2018_worker"]
        try:
            completed = subprocess.run(
                ["Rscript", str(repository_root() / worker)],
                cwd=repository_root(),
                input=canonical_json(payload),
                capture_output=True,
                text=True,
                check=True,
                timeout=float(
                    self.configuration.execution["worker_timeouts_seconds"]["r"]
                ),
                env={**os.environ, "RENV_CONFIG_SYNCHRONIZED_CHECK": "false"},
            )
        except subprocess.CalledProcessError as error:
            detail = (error.stderr or error.stdout or str(error)).strip()
            raise ImportValidationError(
                f"M4comp2018 reference adapter failed: {detail}"
            ) from error
        try:
            records = json.loads(completed.stdout)["records"]
        except (json.JSONDecodeError, KeyError, TypeError) as error:
            raise ImportValidationError(
                "M4comp2018 reference adapter returned invalid JSON"
            ) from error
        expected = {(str(row[0]), provider) for row in rows for provider in enabled}
        observed: set[tuple[str, str]] = set()
        for record in records:
            identity = (str(record.get("series_id")), record.get("forecast_id"))
            mean = record.get("mean")
            if (
                identity not in expected
                or identity in observed
                or record.get("dataset_id") != dataset_id
                or record.get("official_m4_series_id")
                != f"D{int(identity[0]) + 1}"
                or record.get("horizon") != horizon
                or not isinstance(mean, list)
                or len(mean) != horizon
                or any(not isinstance(value, (int, float)) or not math.isfinite(value) for value in mean)
                or record.get("forecast_capability") != "mean_only"
                or record.get("point_semantics") != "M4 competition point forecast"
                or record.get("source_package") != "M4comp2018"
                or record.get("submission_id")
                != archived["providers"][identity[1]]["submission_id"]
                or record.get("submission_rank")
                != {"m4_smyl": 1, "m4_fforma": 2}[identity[1]]
                or not isinstance(record.get("source_package_version"), str)
                or not record["source_package_version"]
                or not isinstance(record.get("source_revision"), str)
                or not record["source_revision"]
                or not isinstance(record.get("content_hash"), str)
                or not record["content_hash"]
            ):
                raise ImportValidationError(
                    "M4comp2018 reference adapter returned incompatible identity or values"
                )
            observed.add(identity)
        if observed != expected:
            raise ImportValidationError(
                "M4comp2018 reference adapter omitted a selected series or provider"
            )
        return records

    def _store_reference_forecasts(self, records: list[dict[str, Any]]) -> tuple[int, int]:
        """Purpose: Atomically insert identical archived forecasts or reject conflicts.

        Inputs: Fully validated adapter records.
        Outputs: Counts of inserted and already-identical records after one transaction.
        """
        inserted = skipped = 0
        self.connection.execute("BEGIN TRANSACTION")
        try:
            for record in records:
                key = [record["dataset_id"], str(record["series_id"]), record["forecast_id"]]
                source_metadata = canonical_json({
                    "source_package": record["source_package"],
                    "source_package_version": record["source_package_version"],
                    "source_revision": record["source_revision"],
                    "scale": "original",
                })
                existing = self.connection.execute(
                    """SELECT official_m4_series_id, submission_id, submission_rank,
                              submission_author, horizon, mean, point_semantics,
                              forecast_capability, source_metadata, content_hash
                       FROM reference_forecasts
                       WHERE dataset_id=? AND series_id=? AND forecast_id=?""",
                    key,
                ).fetchone()
                expected = (
                    record["official_m4_series_id"],
                    int(record["submission_id"]),
                    int(record["submission_rank"]),
                    record["submission_author"],
                    int(record["horizon"]),
                    record["mean"],
                    record["point_semantics"],
                    record["forecast_capability"],
                    source_metadata,
                    record["content_hash"],
                )
                if existing is not None:
                    if tuple(existing) != expected:
                        raise ImportValidationError(
                            "conflicting archived forecast exists for "
                            f"{record['series_id']}/{record['forecast_id']}"
                        )
                    skipped += 1
                    continue
                reference_id = (
                    "reference/"
                    + json_fingerprint(
                        {"dataset_id": key[0], "series_id": key[1], "forecast_id": key[2]}
                    )[:32]
                )
                self.connection.execute(
                    """INSERT INTO reference_forecasts
                    (reference_forecast_id, dataset_id, series_id,
                     official_m4_series_id, forecast_id, submission_id,
                     submission_rank, submission_author, horizon, mean,
                     point_semantics, forecast_capability, source_metadata,
                     content_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [reference_id, *key[:2], record["official_m4_series_id"], key[2],
                     *expected[1:6], expected[6], expected[7],
                     expected[8], expected[9]],
                )
                inserted += 1
            self.connection.execute("COMMIT")
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise
        return inserted, skipped

    def import_m4_daily(
        self,
        source_dir: Path,
        config: dict[str, Any],
        source_revision: str,
        workers: int,
        batch_size: int,
    ) -> dict[str, Any]:
        """Purpose: Run a restartable, provenance-checked M4 Daily import.

        Inputs: Pinned source directory, import contract/revision, worker count, and
        positive batch size.
        Outputs: Run/invocation/dataset IDs and task, series, and observation counts.
        Notes: Re-fingerprints files after iteration; only the coordinator writes DuckDB.
        """
        if workers < 1 or batch_size < 1:
            raise ValueError("workers and batch_size must be at least 1")
        source_before = source_fingerprint(source_dir)
        dataset_id, config_hash = dataset_identity(
            config, source_revision, source_before["files"]
        )
        run_id, invocation_id = self._prepare(
            dataset_id,
            config_hash,
            config,
            source_revision,
            source_before,
            source_metadata(source_dir),
            workers,
        )
        summary = {
            "workers": workers,
            "selected_series": 0,
            "submitted_tasks": 0,
            "completed_this_invocation": 0,
            "skipped_completed": 0,
            "failed_this_invocation": 0,
        }
        executor = (
            ProcessPoolExecutor(
                max_workers=workers,
                mp_context=multiprocessing.get_context("spawn"),
            )
            if workers > 1
            else None
        )
        pending: list[SeriesTask] = []
        try:
            records = iter_source_series(
                source_dir,
                config["benchmark"]["frequency"],
                config["max_series"],
            )
            for source in records:
                summary["selected_series"] += 1
                series_id = source.source_series_id
                task_id, status = self._register_task(run_id, dataset_id, series_id)
                if status == "completed":
                    summary["skipped_completed"] += 1
                    continue
                pending.append(
                    SeriesTask(
                        task_id=task_id,
                        dataset_id=dataset_id,
                        series_id=series_id,
                        source_series_id=source.source_series_id,
                        source_row=source.source_row,
                        frequency=source.frequency,
                        start_timestamp=source.start_timestamp,
                        target=source.target,
                        horizon=config["benchmark"]["prediction_length"],
                        window_id=f"{config['benchmark']['term']}/000",
                        boundary_convention=config["benchmark"]["boundary_convention"],
                    )
                )
                if len(pending) >= batch_size:
                    summary["submitted_tasks"] += len(pending)
                    completed, failed = self._process_batch(pending, executor)
                    summary["completed_this_invocation"] += completed
                    summary["failed_this_invocation"] += failed
                    pending.clear()
            if pending:
                summary["submitted_tasks"] += len(pending)
                completed, failed = self._process_batch(pending, executor)
                summary["completed_this_invocation"] += completed
                summary["failed_this_invocation"] += failed

            source_after = source_fingerprint(source_dir)
            if source_after != source_before:
                raise ImportValidationError("source files changed during import")
            reference_records = self._read_m4_reference_forecasts(dataset_id, config)
            reference_inserted, reference_skipped = self._store_reference_forecasts(
                reference_records
            )
            summary["reference_forecasts_inserted"] = reference_inserted
            summary["reference_forecasts_skipped"] = reference_skipped
            counts = dict(
                self.connection.execute(
                    "SELECT status, count(*) FROM tasks WHERE run_id = ? GROUP BY status",
                    [run_id],
                ).fetchall()
            )
            series_count, observations = self.connection.execute(
                "SELECT count(*), coalesce(sum(observation_count), 0) "
                "FROM series WHERE dataset_id = ?",
                [dataset_id],
            ).fetchone()
            summary.update(
                {
                    "task_counts": counts,
                    "series_count": series_count,
                    "observation_count": observations,
                }
            )
            status = "failed" if counts.get("failed", 0) else "completed"
            self.connection.execute(
                """
                UPDATE runs SET status = ?, ended_at = current_timestamp,
                    summary = ?, error_summary = ? WHERE run_id = ?
                """,
                [
                    status,
                    canonical_json(summary),
                    "one or more tasks failed" if status == "failed" else None,
                    run_id,
                ],
            )
            self.connection.execute(
                """
                UPDATE run_invocations SET status = ?, ended_at = current_timestamp,
                    summary = ?, error = ? WHERE invocation_id = ?
                """,
                [
                    status,
                    canonical_json(summary),
                    "one or more tasks failed" if status == "failed" else None,
                    invocation_id,
                ],
            )
            if status == "failed":
                raise RuntimeError("one or more Process 01 tasks failed; rerun to retry")
            return {
                "run_id": run_id,
                "invocation_id": invocation_id,
                "dataset_id": dataset_id,
                **summary,
            }
        except BaseException as exc:
            self.connection.execute(
                """
                UPDATE runs SET status = 'failed', ended_at = current_timestamp,
                    summary = ?, error_summary = ? WHERE run_id = ?
                """,
                [canonical_json(summary), f"{type(exc).__name__}: {exc}", run_id],
            )
            self.connection.execute(
                """
                UPDATE run_invocations SET status = 'failed',
                    ended_at = current_timestamp, summary = ?, error = ?
                WHERE invocation_id = ?
                """,
                [
                    canonical_json(summary),
                    f"{type(exc).__name__}: {exc}",
                    invocation_id,
                ],
            )
            raise
        finally:
            if executor is not None:
                executor.shutdown()

    def import_configured(self) -> dict[str, Any]:
        """Purpose: Execute import solely from authoritative stored configuration.

        Inputs: Coordinator configuration plus repository-relative pinned source files.
        Outputs: ``import_m4_daily`` summary after source hashes match stored digests;
        mismatch raises ``ImportValidationError`` before import.
        """
        from .p01_01_import_flow import gate1_import_flow

        return gate1_import_flow(self.database_path)

    def begin_configured_import(self, source: ConfiguredGiftEvalSource) -> dict[str, Any]:
        """Prepare restartable storage using one verified configured source snapshot."""
        self._source_before = source.fingerprint()
        self._selected_ids = []
        dataset_id, config_hash = dataset_identity(
            source.settings, source.revision, self._source_before["files"]
        )
        workers = int(self.configuration.execution["import_workers"])
        run_id, invocation_id = self._prepare(
            dataset_id, config_hash, source.settings, source.revision,
            self._source_before, source.metadata(), workers,
        )
        return {
            "run_id": run_id, "invocation_id": invocation_id, "dataset_id": dataset_id,
            "workers": workers, "execution_mode": "coordinator-local-prefect",
            "selected_series": 0, "submitted_tasks": 0, "skipped_completed": 0,
            "completed_this_invocation": 0, "failed_this_invocation": 0,
        }

    def prepare_source_record(self, record: Any, summary: dict[str, Any]) -> SeriesTask | None:
        """Register one source identity or independently verify its accepted result."""
        self._selected_ids.append(record.source_series_id)
        summary["selected_series"] += 1
        task_id, status = self._register_task(
            summary["run_id"], summary["dataset_id"], record.source_series_id
        )
        settings = self.configuration.import_settings
        task = SeriesTask(
            task_id, summary["dataset_id"], record.source_series_id,
            record.source_series_id, record.source_row, record.frequency,
            record.start_timestamp, record.target,
            settings["benchmark"]["prediction_length"],
            f"{settings['benchmark']['term']}/000", settings["benchmark"]["boundary_convention"],
        )
        if status == "completed":
            expected = compute_series(task)
            stored = self.connection.execute(
                """SELECT source_series_id,source_row,frequency,start_timestamp,target,
                          observation_count,content_hash
                   FROM series WHERE dataset_id=? AND series_id=?""",
                [summary["dataset_id"], record.source_series_id],
            ).fetchone()
            window = self.connection.execute(
                """SELECT split_name,train_start,train_end,validation_start,validation_end,
                          test_start,test_end,horizon,boundary_convention
                   FROM evaluation_windows WHERE dataset_id=? AND series_id=? AND window_id=?""",
                [summary["dataset_id"], record.source_series_id, expected.window["window_id"]],
            ).fetchone()
            expected_window = tuple(
                expected.window[key] for key in (
                    "split_name", "train_start", "train_end", "validation_start",
                    "validation_end", "test_start", "test_end", "horizon",
                    "boundary_convention",
                )
            )
            values_match = stored is not None and len(stored[4]) == len(record.target) and all(
                saved == raw or (saved is None and raw is not None and math.isnan(raw))
                for saved, raw in zip(stored[4], record.target, strict=True)
            )
            if (stored is None or stored[:4] != (
                    expected.source_series_id, expected.source_row, expected.frequency,
                    expected.start_timestamp,
                ) or not values_match or stored[5:] != (
                    expected.observation_count, expected.content_hash,
                ) or window != expected_window):
                raise ImportValidationError(
                    "completed import identity, values, hash, or window lineage differs from source"
                )
            summary["skipped_completed"] += 1
            return None
        summary["submitted_tasks"] += 1
        return task

    def finish_configured_import(self, source: ConfiguredGiftEvalSource,
                                 summary: dict[str, Any]) -> dict[str, Any]:
        """Verify source/membership and references before accepting the import run."""
        if source.fingerprint() != self._source_before:
            raise ImportValidationError("source files changed during import")
        self._verify_expected_membership(summary["run_id"], summary["dataset_id"], self._selected_ids)
        references = self._read_m4_reference_forecasts(summary["dataset_id"], source.settings)
        inserted, skipped = self._store_reference_forecasts(references)
        summary.update(reference_forecasts_inserted=inserted, reference_forecasts_skipped=skipped)
        summary["task_counts"] = dict(self.connection.execute(
            "SELECT status, count(*) FROM tasks WHERE run_id=? GROUP BY status", [summary["run_id"]]
        ).fetchall())
        summary["series_count"], summary["observation_count"] = self.connection.execute(
            "SELECT count(*), coalesce(sum(observation_count),0) FROM series WHERE dataset_id=?",
            [summary["dataset_id"]],
        ).fetchone()
        self.verify_import_completion(summary)
        self.record_import_outcome(summary)
        return summary

    def record_import_outcome(self, summary: dict[str, Any], error: str | None = None) -> None:
        """Record success or failure for both parent import and invocation atomically."""
        status = "failed" if error is not None else "completed"
        self.connection.execute("BEGIN TRANSACTION")
        try:
            self.connection.execute(
                "UPDATE runs SET status=?, ended_at=current_timestamp, summary=?, error_summary=? WHERE run_id=?",
                [status, canonical_json(summary), error, summary["run_id"]],
            )
            self.connection.execute(
                "UPDATE run_invocations SET status=?, ended_at=current_timestamp, summary=?, error=? WHERE invocation_id=?",
                [status, canonical_json(summary), error, summary["invocation_id"]],
            )
            self.connection.execute("COMMIT")
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise

    def verify_import_completion(self, summary: dict[str, Any]) -> None:
        """Verify expected configured membership exists as tasks and canonical output."""
        expected = self.configuration.series_count
        completed = int(summary.get("task_counts", {}).get("completed", 0))
        if (
            int(summary.get("selected_series", -1)) != expected
            or int(summary.get("series_count", -1)) != expected
            or completed != expected
        ):
            raise ImportValidationError(
                "configured import did not produce the expected task and series membership"
            )

    def _verify_expected_membership(
        self, run_id: str, dataset_id: str, expected_ids: list[str]
    ) -> None:
        """Compare authoritative source identities with durable tasks and outputs."""
        expected = set(expected_ids)
        task_ids = {
            str(row[0])
            for row in self.connection.execute(
                "SELECT series_id FROM tasks WHERE run_id=? AND status='completed'",
                [run_id],
            ).fetchall()
        }
        stored_ids = {
            str(row[0])
            for row in self.connection.execute(
                "SELECT series_id FROM series WHERE dataset_id=?", [dataset_id]
            ).fetchall()
        }
        if len(expected_ids) != len(expected) or task_ids != expected or stored_ids != expected:
            raise ImportValidationError(
                "configured import durable membership differs from authoritative source records"
            )
