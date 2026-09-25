import json
import shutil
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from subprocess import TimeoutExpired
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc

from shapefm.config import (
    ImportValidationError,
    dataset_identity,
    evaluation_window,
    json_fingerprint,
    validate_config,
)
from shapefm.database import ShapeFMDatabase, migrate_database
from shapefm.orchestration import (
    ImportCoordinator,
    SeriesResult,
    SeriesTask,
    command_output,
    compute_series,
)


def config(max_series=10):
    return {
        "schema_version": "1",
        "dataset_name": "m4_daily",
        "source_system": "Salesforce/GiftEval",
        "max_series": max_series,
        "benchmark": {
            "frequency": "D",
            "term": "short",
            "prediction_length": 14,
            "evaluation_windows": 1,
            "boundary_convention": "zero-based, end-exclusive",
        },
    }


def write_source(path: Path, targets: list[list[float]]) -> None:
    path.mkdir(parents=True)
    table = pa.table(
        {
            "item_id": pa.array([str(i) for i in range(len(targets))]),
            "start": pa.array([datetime(2000, 1, 1)] * len(targets), type=pa.timestamp("s")),
            "freq": pa.array(["D"] * len(targets)),
            "target": pa.array(targets, type=pa.list_(pa.float32())),
        }
    )
    with (path / "data-00000-of-00001.arrow").open("wb") as stream:
        with ipc.new_stream(stream, table.schema) as writer:
            writer.write_table(table)
    (path / "dataset_info.json").write_text(json.dumps({"rows": len(targets)}))
    (path / "state.json").write_text(json.dumps({"format": "arrow"}))


class ConfigurationTests(unittest.TestCase):
    def test_m4_daily_boundaries_are_zero_based_and_end_exclusive(self):
        self.assertEqual(
            evaluation_window(107, config()),
            {
                "window_id": "short/000",
                "split_name": "validation_and_test",
                "train_start": 0,
                "train_end": 79,
                "validation_start": 79,
                "validation_end": 93,
                "test_start": 93,
                "test_end": 107,
                "horizon": 14,
                "boundary_convention": "zero-based, end-exclusive",
            },
        )

    def test_execution_scope_does_not_change_dataset_identity(self):
        files = {"source.arrow": {"sha256": "a"}}
        first, _ = dataset_identity(config(10), "revision", files)
        second, _ = dataset_identity(config(None), "revision", files)
        self.assertEqual(first, second)

    def test_configuration_rejects_changed_horizon(self):
        value = config()
        value["benchmark"]["prediction_length"] = 13
        with self.assertRaises(ImportValidationError):
            validate_config(value)

    def test_json_fingerprint_is_order_independent(self):
        self.assertEqual(json_fingerprint({"a": 1, "b": 2}), json_fingerprint({"b": 2, "a": 1}))


class WorkerTests(unittest.TestCase):
    @patch("shapefm.orchestration.subprocess.run")
    def test_optional_provenance_command_times_out(self, run):
        run.side_effect = TimeoutExpired(["Rscript", "-e", "version"], 0.01)

        self.assertIsNone(
            command_output(["Rscript", "-e", "version"], timeout_seconds=0.01)
        )
        self.assertEqual(run.call_args.kwargs["timeout"], 0.01)

    def test_worker_computes_result_without_database(self):
        task = SeriesTask(
            task_id="task",
            dataset_id="dataset",
            series_id="7",
            source_series_id="7",
            source_row=7,
            frequency="D",
            start_timestamp=datetime(2000, 1, 1),
            target=tuple(float(value) for value in range(31)),
            horizon=14,
            boundary_convention="zero-based, end-exclusive",
        )
        result = compute_series(task)
        self.assertEqual(result.observation_count, 31)
        self.assertEqual(result.window["train_end"], 3)
        self.assertEqual(result.window["validation_start"], 3)
        self.assertEqual(result.window["test_start"], 17)
        self.assertRegex(result.content_hash, r"^[0-9a-f]{64}$")


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.temp)

    def test_migration_creates_only_stage_1_tables(self):
        database = migrate_database(self.temp / "test.duckdb")
        connection = duckdb.connect(str(database), read_only=True)
        names = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        connection.close()
        self.assertEqual(
            names,
            {
                "schema_versions",
                "datasets",
                "series",
                "evaluation_windows",
                "runs",
                "run_invocations",
                "tasks",
                "task_attempts",
            },
        )

    def test_failure_is_preserved_and_retried(self):
        source = self.temp / "source"
        write_source(source, [[float(value) for value in range(10)]])
        database = self.temp / "failed.duckdb"
        for expected_attempts in (1, 2):
            with ImportCoordinator(database) as coordinator:
                with self.assertRaises(RuntimeError):
                    coordinator.import_m4_daily(source, config(1), "revision", workers=1)
            connection = duckdb.connect(str(database), read_only=True)
            task = connection.execute("SELECT status, attempt_count FROM tasks").fetchone()
            attempts = connection.execute("SELECT count(*) FROM task_attempts").fetchone()[0]
            invocations = connection.execute(
                "SELECT count(*) FROM run_invocations WHERE status = 'failed'"
            ).fetchone()[0]
            connection.close()
            self.assertEqual(task, ("failed", expected_attempts))
            self.assertEqual(attempts, expected_attempts)
            self.assertEqual(invocations, expected_attempts)

    def test_full_scope_extends_smoke_dataset_and_run(self):
        source = self.temp / "source"
        write_source(
            source,
            [[float(value + row) for value in range(30)] for row in range(12)],
        )
        database = self.temp / "extension.duckdb"
        with ImportCoordinator(database) as coordinator:
            smoke = coordinator.import_m4_daily(
                source, config(3), "revision", workers=1
            )
            full = coordinator.import_m4_daily(
                source, config(None), "revision", workers=1
            )
        self.assertEqual(smoke["dataset_id"], full["dataset_id"])
        self.assertEqual(smoke["run_id"], full["run_id"])
        self.assertEqual(full["skipped_completed"], 3)
        self.assertEqual(full["completed_this_invocation"], 9)
        connection = duckdb.connect(str(database), read_only=True)
        counts = connection.execute(
            """
            SELECT (SELECT count(*) FROM datasets), (SELECT count(*) FROM runs),
                   (SELECT count(*) FROM run_invocations),
                   (SELECT count(*) FROM tasks WHERE status = 'completed')
            """
        ).fetchone()
        connection.close()
        self.assertEqual(counts, (1, 1, 2, 12))

    def test_series_window_and_task_completion_are_atomic(self):
        source = self.temp / "source"
        write_source(source, [[float(value) for value in range(30)]])
        database = self.temp / "atomic.duckdb"
        with ImportCoordinator(database) as coordinator:
            coordinator.import_m4_daily(source, config(1), "revision", workers=1)
            dataset_id, task_id = coordinator.connection.execute(
                "SELECT dataset_id, task_id FROM tasks"
            ).fetchone()
            coordinator.connection.execute("DELETE FROM evaluation_windows")
            coordinator.connection.execute("DELETE FROM series")
            coordinator.connection.execute("UPDATE tasks SET status = 'failed'")
            attempt = coordinator._start_attempt(task_id)
            bad = SeriesResult(
                task_id=task_id,
                dataset_id=dataset_id,
                series_id="0",
                source_series_id="0",
                source_row=0,
                frequency="D",
                start_timestamp=datetime(2000, 1, 1),
                target=tuple(float(value) for value in range(30)),
                observation_count=30,
                content_hash="hash",
                window={
                    "window_id": None,
                    "split_name": "validation_and_test",
                    "train_start": 0,
                    "train_end": 2,
                    "validation_start": 2,
                    "validation_end": 16,
                    "test_start": 16,
                    "test_end": 30,
                    "horizon": 14,
                    "boundary_convention": "zero-based, end-exclusive",
                },
            )
            with self.assertRaises(duckdb.ConstraintException):
                coordinator._commit_result(bad, attempt)
            series_count = coordinator.connection.execute(
                "SELECT count(*) FROM series"
            ).fetchone()[0]
            self.assertEqual(series_count, 0)
            self.assertEqual(
                coordinator.connection.execute("SELECT status FROM tasks").fetchone()[0],
                "running",
            )


if __name__ == "__main__":
    unittest.main()
