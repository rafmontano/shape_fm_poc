"""Single-writer Stage 1 coordinator with sequential and local worker modes."""

from __future__ import annotations

import hashlib
import importlib.metadata
import multiprocessing
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

from .config import (
    ImportValidationError,
    canonical_import_configuration,
    canonical_json,
    dataset_identity,
    evaluation_window,
    json_fingerprint,
)
from .database import migrate_database
from .gift_eval import iter_source_series, source_fingerprint, source_metadata
from .utils import sha256_file, utc_now


STAGE = "import"
BATCH_SIZE = 64


@dataclass(frozen=True)
class SeriesTask:
    task_id: str
    dataset_id: str
    series_id: str
    source_series_id: str
    source_row: int
    frequency: str
    start_timestamp: Any
    target: tuple[float, ...]
    horizon: int
    boundary_convention: str


@dataclass(frozen=True)
class SeriesResult:
    task_id: str
    dataset_id: str
    series_id: str
    source_series_id: str
    source_row: int
    frequency: str
    start_timestamp: Any
    target: tuple[float, ...]
    observation_count: int
    content_hash: str
    window: dict[str, Any]


@dataclass(frozen=True)
class WorkerOutcome:
    task_id: str
    result: SeriesResult | None
    error: str | None


def compute_series(task: SeriesTask) -> SeriesResult:
    """Pure worker computation: no database access and no side effects."""
    packed = struct.pack(f"<{len(task.target)}f", *task.target)
    content_hash = hashlib.sha256(packed).hexdigest()
    window = {
        "window_id": "short/000",
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


def worker_entry(task: SeriesTask) -> WorkerOutcome:
    try:
        return WorkerOutcome(task.task_id, compute_series(task), None)
    except BaseException as exc:
        return WorkerOutcome(task.task_id, None, f"{type(exc).__name__}: {exc}")


def repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def command_output(command: list[str], timeout_seconds: float = 30.0) -> str | None:
    """Return optional provenance output without blocking scientific work."""
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
    """The sole owner of the writable DuckDB connection."""

    def __init__(self, database_path: Path):
        self.database_path = migrate_database(database_path)
        self.connection = duckdb.connect(str(self.database_path))

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "ImportCoordinator":
        return self

    def __exit__(self, *_: object) -> None:
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

    def _commit_result(self, result: SeriesResult, attempt: int) -> None:
        """Atomically validate/insert scientific data and complete its task."""
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

    def import_m4_daily(
        self,
        source_dir: Path,
        config: dict[str, Any],
        source_revision: str,
        workers: int = 1,
    ) -> dict[str, Any]:
        if workers < 1:
            raise ValueError("workers must be at least 1")
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
            for source in iter_source_series(
                source_dir,
                config["benchmark"]["frequency"],
                config["max_series"],
            ):
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
                        boundary_convention=config["benchmark"]["boundary_convention"],
                    )
                )
                if len(pending) >= BATCH_SIZE:
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

            if source_fingerprint(source_dir) != source_before:
                raise ImportValidationError("source files changed during import")
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
                raise RuntimeError("one or more Stage 1 tasks failed; rerun to retry")
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
