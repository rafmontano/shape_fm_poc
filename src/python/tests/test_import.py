# ==============================================================================
# test_import.py
#
# Purpose: Verify M4 import identity, batching, retries, window construction, and atomic DuckDB persistence with synthetic sources.
# Inputs: unittest fixtures, temporary databases/files, deterministic synthetic records, and mocked process or cluster boundaries.
# Outputs: unittest pass/fail assertions and captured diagnostics; no production artifacts or external services.
# Run from: PYTHONPATH=src/python .tools/uv/uv run --locked --no-sync python -m unittest tests.test_import
# ==============================================================================

"""Verify M4 import identity, batching, retries, window construction, and atomic DuckDB persistence with synthetic sources."""

import json
import math
import shutil
import tempfile
import unittest
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from subprocess import TimeoutExpired
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc

from util.shared_configuration import (
    ImportValidationError,
    ExperimentConfiguration,
    dataset_identity,
    evaluation_window,
    json_fingerprint,
    load_experiment_configuration,
    validate_config,
)
from util.shared_database import ShapeFMDatabase, initialize_experiment_database, migrate_database
from util.p01_03_gift_eval_source import (
    ConfiguredGiftEvalSource,
    iter_source_series,
    source_fingerprint,
)
from util.p01_02_import_execution import (
    ImportCoordinator,
    SeriesResult,
    SeriesTask,
    command_output,
    compute_series,
)


def config(max_series=10):
    """Build a canonical M4 Daily import fixture.

    Purpose: Supply valid import settings while allowing execution-scope variation.
    Inputs: Optional maximum number of source series.
    Outputs: A new configuration mapping; no external state is mutated.
    """
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
    """Materialize a synthetic GiftEval-compatible source.

    Purpose: Provide deterministic Arrow series and minimum import metadata.
    Inputs: Destination directory and ordered target-value sequences.
    Outputs: Creates the directory, Arrow stream, ``dataset_info.json``, and ``state.json``.
    """
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


def initialize_test_database(path: Path) -> None:
    """Create a coordinator-ready test database.

    Purpose: Initialize the production schema with the committed experiment contract.
    Inputs: Destination DuckDB path.
    Outputs: Creates and initializes the DuckDB file at ``path``.
    """
    initialize_experiment_database(
        path,
        Path(__file__).resolve().parents[3] / "config/experiments/poc2_m4_daily_100.json",
    )


class ConfigurationTests(unittest.TestCase):
    """Exercise M4 boundaries, identities, and import validation.

    Purpose: Verify pure configuration behavior independently of persistence.
    Inputs: In-memory canonical and deliberately modified configurations.
    Outputs: Assertions only; this class owns no external side effects.
    """
    def test_m4_daily_boundaries_are_zero_based_and_end_exclusive(self):
        """A 107-point series yields the official 79/14/14 end-exclusive split."""
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
        """Changing only max_series leaves the scientific dataset identity unchanged."""
        files = {"source.arrow": {"sha256": "a"}}
        first, _ = dataset_identity(config(10), "revision", files)
        second, _ = dataset_identity(config(None), "revision", files)
        self.assertEqual(first, second)

    def test_configuration_rejects_changed_horizon(self):
        """M4 Daily configuration requires its official 14-step horizon."""
        value = config()
        value["benchmark"]["prediction_length"] = 13
        with self.assertRaises(ImportValidationError):
            validate_config(value)

    def test_json_fingerprint_is_order_independent(self):
        """JSON fingerprints are invariant to mapping insertion order."""
        self.assertEqual(json_fingerprint({"a": 1, "b": 2}), json_fingerprint({"b": 2, "a": 1}))


class WorkerTests(unittest.TestCase):
    """Exercise import computation and optional provenance subprocess handling.

    Purpose: Verify worker results without database access or real subprocesses.
    Inputs: Synthetic series tasks and a patched subprocess runner.
    Outputs: Assertions and mock call state; this class owns no external side effects.
    """

    @patch("util.p01_02_import_execution.subprocess.run")
    def test_optional_provenance_command_times_out(self, run):
        """Optional provenance commands return None after the configured timeout."""
        run.side_effect = TimeoutExpired(["Rscript", "-e", "version"], 0.01)

        self.assertIsNone(
            command_output(["Rscript", "-e", "version"], timeout_seconds=0.01)
        )
        self.assertEqual(run.call_args.kwargs["timeout"], 0.01)

    def test_worker_computes_result_without_database(self):
        """Series computation produces boundaries and a content hash without database access."""
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
            window_id="short/000",
            boundary_convention="zero-based, end-exclusive",
        )
        result = compute_series(task)
        self.assertEqual(result.observation_count, 31)
        self.assertEqual(result.window["train_end"], 3)
        self.assertEqual(result.window["validation_start"], 3)
        self.assertEqual(result.window["test_start"], 17)
        self.assertRegex(result.content_hash, r"^[0-9a-f]{64}$")


class MissingObservationImportTests(unittest.TestCase):
    """Verify Gate 1 preserves missingness while rejecting infinities."""

    def setUp(self):
        self.temp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.temp)

    def test_missing_and_nan_positions_are_preserved(self):
        """Arrow null and NaN observations survive streaming and canonical storage."""
        source = self.temp / "source"
        values = [float(value) for value in range(30)]
        values[3] = None
        values[7] = float("nan")
        write_source(source, [values])
        streamed = next(iter_source_series(source, "D", 1))
        self.assertIsNone(streamed.target[3])
        self.assertTrue(math.isnan(streamed.target[7]))

        database = self.temp / "missing.duckdb"
        initialize_test_database(database)
        with ImportCoordinator(database) as coordinator:
            result = coordinator.import_m4_daily(
                source, config(1), "revision", workers=1, batch_size=1
            )
        self.assertEqual(result["series_count"], 1)
        with ShapeFMDatabase.open(database) as database_api:
            stored = database_api.get_series("m4_daily", "0").target
        self.assertIsNone(stored[3])
        # DuckDB normalizes IEEE NaN to SQL NULL in FLOAT[]; both original
        # missing positions remain missing and every finite value is unchanged.
        self.assertIsNone(stored[7])
        self.assertEqual(stored[2], 2.0)

    def test_infinite_observation_is_rejected_clearly(self):
        """Positive and negative infinity are malformed rather than missing."""
        for index, value in enumerate((float("inf"), float("-inf"))):
            source = self.temp / f"source-{index}"
            write_source(source, [[1.0, value, 3.0]])
            with self.assertRaisesRegex(ImportValidationError, "cannot be infinite"):
                list(iter_source_series(source, "D", 1))

    def test_configured_source_exposes_bounded_records_and_validated_identity(self):
        """The source object owns path, metadata, fingerprint, and bounded streaming."""
        source = self.temp / "configured" / "m4_daily"
        write_source(source, [[float(value) for value in range(30)]] * 2)
        reference = load_experiment_configuration(
            Path(__file__).resolve().parents[3]
            / "config/experiments/poc2_m4_daily_100.json"
        )
        resolved = deepcopy(reference.resolved)
        resolved["data"]["source"]["directory"] = "configured"
        resolved["data"]["source"]["files"] = {
            name: value["sha256"]
            for name, value in source_fingerprint(source)["files"].items()
        }
        resolved["data"]["selection"]["count"] = 1
        configured = ConfiguredGiftEvalSource(
            ExperimentConfiguration(reference.original, resolved), self.temp
        )
        self.assertEqual(configured.metadata()["dataset_info.json"]["rows"], 2)
        self.assertEqual(len(list(configured.records())), 1)
        self.assertRegex(configured.fingerprint()["fingerprint"], r"^[0-9a-f]{64}$")

    def test_completed_configured_import_rejects_corrupt_value_and_preserves_hash(self):
        """Gate 1 skip rejects altered values without repairing data or changing its hash."""
        source = self.temp / "configured" / "m4_daily"
        write_source(source, [[float(value) for value in range(30)]])
        reference = load_experiment_configuration(
            Path(__file__).resolve().parents[3]
            / "config/experiments/poc2_m4_daily_100.json"
        )
        resolved = deepcopy(reference.resolved)
        resolved["data"]["source"]["directory"] = "configured"
        resolved["data"]["source"]["files"] = {
            name: value["sha256"]
            for name, value in source_fingerprint(source)["files"].items()
        }
        resolved["data"]["selection"]["count"] = 1
        configuration = ExperimentConfiguration(reference.original, resolved)
        configured = ConfiguredGiftEvalSource(configuration, self.temp)
        database = self.temp / "corrupt.duckdb"
        initialize_test_database(database)
        with ImportCoordinator(database, configuration) as coordinator:
            summary = coordinator.begin_configured_import(configured)
            record = next(configured.records())
            task = coordinator.prepare_source_record(record, summary)
            attempt = coordinator.start_import_task(task)
            coordinator.accept_import_result(compute_series(task), attempt, summary)
            original_hash = coordinator.connection.execute(
                "SELECT content_hash FROM series"
            ).fetchone()[0]
            coordinator.connection.execute(
                "UPDATE series SET target=list_transform(target,(x,i)->CASE WHEN i=2 THEN 99.0 ELSE x END)"
            )
            with self.assertRaisesRegex(ImportValidationError, "identity, values, hash"):
                coordinator.prepare_source_record(record, summary)
            self.assertEqual(
                coordinator.connection.execute("SELECT content_hash FROM series").fetchone()[0],
                original_hash,
            )


class DatabaseTests(unittest.TestCase):
    """Exercise import persistence in isolated filesystem state.

    Purpose: Verify migration, retry, extension, and transaction atomicity.
    Inputs: Synthetic source files and a fresh temporary directory per test.
    Outputs: Temporary Arrow/JSON files and DuckDB mutations owned and removed here.
    """
    def setUp(self):
        """Allocate isolated filesystem state.

        Purpose: Own every source and database artifact created by one test.
        Inputs: The system temporary-directory service.
        Outputs: Creates a directory and stores its path in ``self.temp``.
        """
        self.temp = Path(tempfile.mkdtemp())

    def tearDown(self):
        """Remove isolated import-test state.

        Purpose: Prevent generated sources and DuckDB files leaking after a test.
        Inputs: ``self.temp`` created by ``setUp``.
        Outputs: Recursively deletes the directory and all artifacts beneath it.
        """
        shutil.rmtree(self.temp)

    def test_migration_preserves_stage_1_tables_and_adds_poc1(self):
        """Migration exposes both import-stage and POC1 experiment tables."""
        database = migrate_database(self.temp / "test.duckdb")
        connection = duckdb.connect(str(database), read_only=True)
        names = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        connection.close()
        self.assertTrue(
            {
                "schema_versions",
                "datasets",
                "series",
                "evaluation_windows",
                "runs",
                "run_invocations",
                "tasks",
                "task_attempts",
            }.issubset(names)
        )
        self.assertTrue(
            {
                "benchmark_configurations",
                "experiments",
                "experiment_variants",
                "forecast_instances",
                "experiment_invocations",
                "experiment_tasks",
                "experiment_task_attempts",
                "preprocessed_series",
                "transformed_series",
                "forecasts",
                "forecast_components",
                "official_evaluations",
                "submission_exports",
            }.issubset(names)
        )

    def test_failure_is_preserved_and_retried(self):
        """Repeated import failures retain failed invocations and increment attempts."""
        source = self.temp / "source"
        write_source(source, [[float(value) for value in range(10)]])
        database = self.temp / "failed.duckdb"
        initialize_test_database(database)
        for expected_attempts in (1, 2):
            with ImportCoordinator(database) as coordinator:
                with self.assertRaises(RuntimeError):
                    coordinator.import_m4_daily(
                        source, config(1), "revision", workers=1, batch_size=1
                    )
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
        """A full import resumes the smoke run, skips three rows, and adds the remaining nine."""
        source = self.temp / "source"
        write_source(
            source,
            [[float(value + row) for value in range(30)] for row in range(12)],
        )
        database = self.temp / "extension.duckdb"
        initialize_test_database(database)
        with ImportCoordinator(database) as coordinator:
            smoke = coordinator.import_m4_daily(
                source, config(3), "revision", workers=1, batch_size=3
            )
            full = coordinator.import_m4_daily(
                source, config(None), "revision", workers=1, batch_size=12
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
        """An invalid window rolls back its series insert and leaves the task running."""
        source = self.temp / "source"
        write_source(source, [[float(value) for value in range(30)]])
        database = self.temp / "atomic.duckdb"
        initialize_test_database(database)
        with ImportCoordinator(database) as coordinator:
            coordinator.import_m4_daily(
                source, config(1), "revision", workers=1, batch_size=1
            )
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
