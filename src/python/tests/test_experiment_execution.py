# ==============================================================================
# test_experiment_execution.py
#
# Purpose: Verify experiment planning, process prerequisites, restart semantics, task accounting, and forecast retrieval against temporary DuckDB state.
# Inputs: unittest fixtures, temporary databases/files, deterministic synthetic records, and mocked process or cluster boundaries.
# Outputs: unittest pass/fail assertions and captured diagnostics; no production artifacts or external services.
# Run from: PYTHONPATH=src/python .tools/uv/uv run --locked --no-sync python -m unittest tests.test_experiment_execution
# ==============================================================================

"""Verify experiment planning, process prerequisites, restart semantics, task accounting, and forecast retrieval against temporary DuckDB state."""

import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from util.configuration import json_fingerprint
from util.database import initialize_experiment_database
from util.execution_profiles import resolve_execution_profile
from util.experiment_execution import (
    ExperimentCoordinator,
    _batches,
    _combine_job,
    _run_external_batches,
    _run_parallel,
    _transform_job,
    scientific_configuration,
    validated_submission_metadata,
)
from util.transformations import inverse, transform


# Test/calibration value: expected rows derived from the committed 100-series fixture;
# it is an assertion oracle, not a production execution or experiment global.
EXPECTED_100_TASK_COUNTS = {2: 200, 3: 400, 4: 800, 5: 1_200, 6: 12}


def initialize_test_database(path: Path) -> None:
    """Purpose: Create a configured database for experiment coordinator tests.

    Inputs: Destination ``Path`` and the committed 100-series experiment configuration.
    Outputs: None; creates and initializes the DuckDB file at ``path``.
    """
    initialize_experiment_database(
        path,
        Path(__file__).resolve().parents[3] / "config/experiments/poc2_m4_daily_100.json",
    )


class TransformationTests(unittest.TestCase):
    """Purpose: Verify scientific identity and reversible deterministic transformations.

    Inputs: In-memory configuration mappings and deterministic numeric series.
    Outputs: Fingerprint and transformation assertions; no process, database, or file effects.
    """
    def test_provisional_selection_does_not_change_scientific_identity(self):
        """Provisional candidate selection is excluded from the scientific fingerprint."""
        first = {"models": {"a": {"revision": "1"}}, "provisional_candidate": "a"}
        second = {"models": {"a": {"revision": "1"}}, "provisional_candidate": "b"}
        self.assertEqual(
            json_fingerprint(scientific_configuration(first)),
            json_fingerprint(scientific_configuration(second)),
        )

    def test_submission_metadata_does_not_change_scientific_identity(self):
        """Submission metadata is excluded from scientific configuration."""
        first = {"contract_version": "1", "submission_metadata": {"org": "first"}}
        second = {"contract_version": "1", "submission_metadata": {"org": "second"}}
        self.assertEqual(scientific_configuration(first), scientific_configuration(second))

    def test_minmax_standardize_round_trip_is_ordered_and_exact_within_tolerance(self):
        """Min-max standardization restores each input in order within numeric tolerance."""
        source = (-7.5, 2.0, 11.25, 4.5)
        result = transform(source, "minmax_then_standardize")
        restored = inverse(result.values, "minmax_then_standardize", result.parameters)
        for expected, actual in zip(source, restored, strict=True):
            self.assertAlmostEqual(expected, actual, places=12)

    def test_constant_series_has_deterministic_round_trip(self):
        """A constant series maps to zeros and every inverse value maps to its constant."""
        result = transform([3.25] * 5, "minmax_then_standardize")
        self.assertEqual(result.values, (0.0,) * 5)
        self.assertEqual(
            inverse([-100.0, 0.0, 100.0], "minmax_then_standardize", result.parameters),
            (3.25, 3.25, 3.25),
        )

    def test_future_actuals_cannot_change_fitted_parameters(self):
        """Transformation parameters depend only on context and are deterministic."""
        context = [1.0, 4.0, 9.0]
        first = transform(context, "minmax_then_standardize")
        second = transform(context, "minmax_then_standardize")
        future = [10_000.0, -10_000.0]
        self.assertNotIn(max(future), first.parameters.values())
        self.assertEqual(first, second)

    def test_sequential_and_two_worker_transform_paths_are_equal(self):
        """Sequential and two-process transformation return identical ordered results."""
        jobs = [([float(i), float(i + 2), float(i - 3)], "minmax_then_standardize") for i in range(8)]
        self.assertEqual(
            _run_parallel(_transform_job, jobs, 1),
            _run_parallel(_transform_job, jobs, 2),
        )


class ExternalBatchTests(unittest.TestCase):
    """Purpose: Verify forecast combination and external batch execution contracts.

    Inputs: Forecast mappings, numeric batches, and local callback workers.
    Outputs: Ordering, failure, and result assertions; worker processes may be short-lived.
    """
    def test_equal_weight_combination_rearranges_crossed_quantiles(self):
        """Equal-weight combination sorts crossed averaged quantiles and flags the repair."""
        result = _combine_job(
            {
                "left": {
                    "mean": [4.0],
                    "median": [4.0],
                    "quantiles": [[1.0], [3.0], [5.0]],
                },
                "right": {
                    "mean": [8.0],
                    "median": [8.0],
                    "quantiles": [[9.0], [3.0], [7.0]],
                },
                "weights": {"auto_arima": 0.5, "chronos_2": 0.5},
            }
        )
        self.assertEqual(result["mean"], [6.0])
        self.assertEqual(result["median"], [6.0])
        self.assertEqual(result["quantiles"], [[3.0], [5.0], [6.0]])
        self.assertTrue(result["quantiles_rearranged"])

    def test_equal_weight_combination_preserves_ordered_quantiles(self):
        """Already ordered averaged quantiles remain unflagged."""
        result = _combine_job(
            {
                "left": {
                    "mean": [2.0],
                    "median": [2.0],
                    "quantiles": [[1.0], [2.0], [3.0]],
                },
                "right": {
                    "mean": [4.0],
                    "median": [4.0],
                    "quantiles": [[3.0], [4.0], [5.0]],
                },
                "weights": {"auto_arima": 0.5, "chronos_2": 0.5},
            }
        )
        self.assertEqual(result["quantiles"], [[2.0], [3.0], [4.0]])
        self.assertFalse(result["quantiles_rearranged"])

    def test_batches_are_bounded_and_completed_batches_survive_later_failure(self):
        """Batching respects capacity and yields completed work before a later failure."""
        batches = _batches(list(range(7)), 3)
        self.assertEqual([len(batch) for batch in batches], [3, 3, 1])

        def fail_second(batch):
            """Purpose: Simulate a worker failure after one completed batch.

            Inputs: A numeric batch list.
            Outputs: The unchanged list, or RuntimeError when its first value is three; no state effects.
            """
            if batch[0] == 3:
                raise RuntimeError("worker failed")
            return batch

        completed = []
        with self.assertRaisesRegex(RuntimeError, "worker failed"):
            for result in _run_external_batches(fail_second, batches, workers=1):
                completed.append(result)
        self.assertEqual(completed, [[0, 1, 2]])

    def test_external_batches_have_identical_sequential_and_parallel_results(self):
        """Sequential and two-worker external execution produce the same batch sums."""
        batches = _batches(list(range(10)), 2)
        self.assertEqual(
            list(_run_external_batches(sum, batches, workers=1)),
            list(_run_external_batches(sum, batches, workers=2)),
        )


class ContractValidationTests(unittest.TestCase):
    """Purpose: Verify official benchmark and submission metadata contracts.

    Inputs: Benchmark descriptions and draft or approved submission mappings.
    Outputs: Validation results and exception assertions; no process, database, or file effects.
    """
    def test_multi_window_configuration_is_rejected(self):
        """Official execution accepts exactly one evaluation window."""
        coordinator = object.__new__(ExperimentCoordinator)
        coordinator.config = {"benchmark": {"configuration": "m4_daily/D/short"}}
        with self.assertRaisesRegex(RuntimeError, "exactly one"):
            coordinator._validate_official_configuration(
                {"configuration_name": "m4_daily/D/short", "window_count": 2}
            )

    def test_submission_metadata_is_required_and_validated(self):
        """Submission metadata is mandatory and supports valid draft and approved forms."""
        with self.assertRaisesRegex(ValueError, "missing submission metadata"):
            validated_submission_metadata({})
        metadata = {
            "status": "draft",
            "submission_approved": False,
            "model_name": None,
            "model_type": None,
            "model_dtype": "float64-and-float32",
            "model_link": None,
            "code_link": None,
            "org": None,
            "testdata_leakage": None,
            "replication_code_available": "No",
        }
        self.assertFalse(
            validated_submission_metadata({"submission_metadata": metadata})[
                "submission_approved"
            ]
        )
        metadata = {
            "status": "approved",
            "submission_approved": True,
            "model_name": "ShapeFM",
            "model_type": "pretrained",
            "model_dtype": "float32",
            "model_link": "https://example.com/model",
            "code_link": "https://example.com/code",
            "org": "ShapeFM",
            "testdata_leakage": "No",
            "replication_code_available": "Yes",
        }
        self.assertEqual(
            validated_submission_metadata({"submission_metadata": metadata})["org"],
            "ShapeFM",
        )


class TransactionTests(unittest.TestCase):
    """Purpose: Exercise task transactions and retries against coordinator database state.

    Inputs: Synthetic benchmark, task, series, worker-request, and forecast mappings.
    Outputs: Transaction and retry assertions; each test creates then removes a temporary DuckDB tree.
    """
    def setUp(self):
        """Purpose: Provision isolated database state for one transaction test.

        Inputs: The committed experiment configuration loaded by ``initialize_test_database``.
        Outputs: ``directory`` and open ``coordinator`` attributes; creates a temporary DuckDB file.
        """
        self.directory = Path(tempfile.mkdtemp())
        initialize_test_database(self.directory / "poc1.duckdb")
        self.coordinator = ExperimentCoordinator(self.directory / "poc1.duckdb")

    def tearDown(self):
        """Purpose: Release all state provisioned by ``setUp``.

        Inputs: The current open coordinator and temporary-directory attributes.
        Outputs: None; closes DuckDB and recursively removes the temporary directory.
        """
        self.coordinator.close()
        shutil.rmtree(self.directory)

    def _insert_benchmark_and_instances(self):
        """Purpose: Seed the minimum benchmark state required by process tests.

        Inputs: The fixture coordinator's open DuckDB connection.
        Outputs: None; inserts one benchmark row and two forecast-instance rows.
        """
        connection = self.coordinator.connection
        connection.execute(
            """INSERT INTO benchmark_configurations
            (benchmark_configuration_id, benchmark_revision, configuration_name,
             dataset_name, frequency, term, prediction_length, window_count,
             domain, num_variates, metadata)
            VALUES ('benchmark', 'revision', 'm4_daily/D/short', 'm4_daily',
                    'D', 'short', 2, 1, 'Econ/Fin', 1,
                    '{"official_seasonality":1}')"""
        )
        for index in range(2):
            connection.execute(
                """INSERT INTO forecast_instances
                (forecast_instance_id, benchmark_configuration_id, dataset_id,
                 series_id, variate_id, window_id, official_position,
                 context_start, context_end, actual_start, actual_end, horizon,
                 context_target, actual_target, identity_metadata)
                VALUES (?, 'benchmark', 'dataset', ?, '0', 'short/000', ?,
                        0, 3, 3, 5, 2, [1.0, 2.0, 3.0], [4.0, 5.0], '{}')""",
                [f"instance-{index}", str(index), index],
            )

    def test_failed_result_transaction_does_not_complete_task(self):
        """A failed result callback rolls back writes and permits a second successful attempt."""
        connection = self.coordinator.connection
        connection.execute(
            """INSERT INTO experiment_tasks
            (task_id, experiment_id, stage, status) VALUES ('task', 'experiment', 2, 'pending')"""
        )
        invocation = self.coordinator._begin_invocation("experiment", 2, 1, "cpu", 1)
        attempt = self.coordinator._start_tasks([("task", None, None, None)], invocation)["task"]

        def invalid_insert():
            """Purpose: Exercise rollback after a callback performs a database write.

            Inputs: The enclosing fixture's open DuckDB connection.
            Outputs: Always raises RuntimeError after inserting one export in the active transaction.
            """
            connection.execute(
                "INSERT INTO submission_exports VALUES ('export', 'experiment', 'model', 'path', 'revision', '{}', false, current_timestamp)"
            )
            raise RuntimeError("insertion failed")

        with self.assertRaises(RuntimeError):
            self.coordinator._commit_task("task", attempt, 0.0, invalid_insert)
        self.assertEqual(
            connection.execute("SELECT status FROM experiment_tasks WHERE task_id='task'").fetchone()[0],
            "running",
        )
        self.assertEqual(
            connection.execute("SELECT count(*) FROM submission_exports").fetchone()[0], 0
        )
        second_invocation = self.coordinator._begin_invocation(
            "experiment", 2, 1, "cpu", 1
        )
        second_attempt = self.coordinator._start_tasks(
            [("task", None, None, None)], second_invocation
        )["task"]
        self.coordinator._commit_task("task", second_attempt, 0.0, lambda: None)
        self.assertEqual(second_attempt, 2)
        self.assertEqual(
            connection.execute("SELECT status FROM experiment_tasks WHERE task_id='task'").fetchone()[0],
            "completed",
        )
        self.assertEqual(
            connection.execute(
                "SELECT status FROM experiment_task_attempts ORDER BY attempt_number"
            ).fetchall(),
            [("failed",), ("completed",)],
        )

    def test_process_02_batched_failure_preserves_completed_batch_and_retries_rest(self):
        """Process 02 retains its first batch and retries only the failed preprocessing task."""
        self._insert_benchmark_and_instances()
        connection = self.coordinator.connection
        for index in range(2):
            connection.execute(
                """INSERT INTO experiment_tasks
                (task_id, experiment_id, stage, forecast_instance_id, candidate, status)
                VALUES (?, 'experiment', 2, ?, 'identity', 'pending')""",
                [f"task-{index}", f"instance-{index}"],
            )
        calls = 0

        def failing_worker(payload):
            """Purpose: Emulate preprocessing success followed by a batch failure.

            Inputs: A worker payload mapping containing one job with ID and context.
            Outputs: A results/packages mapping or RuntimeError; increments the enclosing call count.
            """
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("second external batch failed")
            job = payload["jobs"][0]
            return {"results": [{"id": job["id"], "values": job["context"]}], "packages": {}}

        self.coordinator._r_worker = failing_worker
        with self.assertRaisesRegex(RuntimeError, "Process 2 failed"):
            self.coordinator.run_process("experiment", 2, workers=1, batch_size=1)
        self.assertEqual(
            connection.execute(
                "SELECT status FROM experiment_tasks ORDER BY task_id"
            ).fetchall(),
            [("completed",), ("failed",)],
        )
        self.assertEqual(connection.execute("SELECT count(*) FROM preprocessed_series").fetchone()[0], 1)

        self.coordinator._r_worker = lambda payload: {
            "results": [
                {"id": job["id"], "values": job["context"]} for job in payload["jobs"]
            ],
            "packages": {},
        }
        result = self.coordinator.run_process(
            "experiment",
            2,
            execution=resolve_execution_profile(
                "sequential_safe", {"cleaning_workers": 2}
            ),
        )
        self.assertEqual(result["selected"], 1)
        self.assertEqual(connection.execute("SELECT count(*) FROM preprocessed_series").fetchone()[0], 2)
        self.assertEqual(
            connection.execute(
                "SELECT attempt_count FROM experiment_tasks ORDER BY task_id"
            ).fetchall(),
            [(1,), (2,)],
        )
        invocations = connection.execute(
            """SELECT execution_profile, resolved_execution, execution_overrides, hardware
            FROM experiment_invocations
            WHERE CAST(execution_overrides AS VARCHAR) != '{}'"""
        ).fetchall()
        invocation = next(
            row for row in invocations
            if json.loads(row[2]).get("cleaning_workers") == 2
        )
        self.assertEqual(invocation[0], "sequential_safe")
        self.assertEqual(json.loads(invocation[1])["cleaning_workers"], 2)
        self.assertEqual(json.loads(invocation[2])["cleaning_workers"], 2)
        self.assertEqual(
            json.loads(invocation[2])["selection"]["count"], 100
        )
        self.assertIn("logical_cpu_count", json.loads(invocation[3]))
        self.assertEqual(
            connection.execute("SELECT task_id FROM experiment_tasks ORDER BY task_id").fetchall(),
            [("task-0",), ("task-1",)],
        )

    def test_process_04_batched_failure_preserves_forecast_and_retries_rest(self):
        """Process 04 retains its first forecast and retries only the failed forecast task."""
        self._insert_benchmark_and_instances()
        connection = self.coordinator.connection
        connection.execute(
            """INSERT INTO experiment_variants
            (variant_id, experiment_id, cleaning_method, transformation_method,
             adjustment_method, configuration)
            VALUES ('variant', 'experiment', 'identity', 'identity', 'identity', '{}')"""
        )
        for index in range(2):
            connection.execute(
                """INSERT INTO transformed_series
                (transformation_id, experiment_id, variant_id,
                 forecast_instance_id, preprocessing_id, transformation_method,
                 input_hash, output_hash, transformed_target, parameters,
                 parent_result_id)
                VALUES (?, 'experiment', 'variant', ?, ?, 'identity', 'in', 'out',
                        [1.0, 2.0, 3.0], '{}', ?)""",
                [f"transformed-{index}", f"instance-{index}", f"pre-{index}", f"pre-{index}"],
            )
            connection.execute(
                """INSERT INTO experiment_tasks
                (task_id, experiment_id, stage, forecast_instance_id, variant_id,
                 candidate, status)
                VALUES (?, 'experiment', 4, ?, 'variant', 'auto_arima', 'pending')""",
                [f"task-{index}", f"instance-{index}"],
            )
        calls = 0
        received_settings = []

        def worker(payload):
            """Purpose: Emulate forecasting success followed by a batch failure.

            Inputs: A worker payload with settings and one forecast job.
            Outputs: A forecast/packages mapping or RuntimeError; records calls and settings in memory.
            """
            nonlocal calls
            calls += 1
            received_settings.append(payload["settings"])
            if calls == 2:
                raise RuntimeError("second external batch failed")
            job = payload["jobs"][0]
            values = [3.0, 3.0]
            return {
                "results": [
                    {"id": job["id"], "mean": values, "median": values, "quantiles": [values] * 9}
                ],
                "packages": {"forecast": "test"},
            }

        self.coordinator._r_worker = worker
        with self.assertRaisesRegex(RuntimeError, "Process 4 failed"):
            self.coordinator.run_process("experiment", 4, workers=1, batch_size=1)
        self.assertEqual(connection.execute("SELECT count(*) FROM forecasts").fetchone()[0], 1)
        self.assertTrue(received_settings)
        self.assertTrue(all(
            settings == self.coordinator.configuration.auto_arima_settings
            for settings in received_settings
        ))
        self.coordinator._r_worker = lambda payload: {
            "results": [
                {
                    "id": job["id"],
                    "mean": [3.0, 3.0],
                    "median": [3.0, 3.0],
                    "quantiles": [[3.0, 3.0]] * 9,
                }
                for job in payload["jobs"]
            ],
            "packages": {"forecast": "test"},
        }
        result = self.coordinator.run_process("experiment", 4, workers=1, batch_size=1)
        self.assertEqual(result["selected"], 1)
        self.assertEqual(connection.execute("SELECT count(*) FROM forecasts").fetchone()[0], 2)
        self.assertEqual(
            connection.execute(
                "SELECT attempt_count FROM experiment_tasks ORDER BY task_id"
            ).fetchall(),
            [(1,), (2,)],
        )

    def test_chronos_oom_restarts_splits_and_preserves_successes(self):
        """Chronos restarts after OOM, splits the batch, and records retry metadata."""
        self._insert_benchmark_and_instances()
        connection = self.coordinator.connection
        connection.execute(
            """INSERT INTO experiment_variants
            (variant_id, experiment_id, cleaning_method, transformation_method,
             adjustment_method, configuration)
            VALUES ('variant', 'experiment', 'identity', 'identity', 'identity', '{}')"""
        )
        for index in range(2):
            connection.execute(
                """INSERT INTO transformed_series
                (transformation_id, experiment_id, variant_id,
                 forecast_instance_id, preprocessing_id, transformation_method,
                 input_hash, output_hash, transformed_target, parameters,
                 parent_result_id)
                VALUES (?, 'experiment', 'variant', ?, ?, 'identity', 'in', 'out',
                        [1.0, 2.0, 3.0], '{}', ?)""",
                [f"transformed-{index}", f"instance-{index}", f"pre-{index}", f"pre-{index}"],
            )
            connection.execute(
                """INSERT INTO experiment_tasks
                (task_id, experiment_id, stage, forecast_instance_id, variant_id,
                 candidate, status)
                VALUES (?, 'experiment', 4, ?, 'variant', 'chronos_2', 'pending')""",
                [f"task-{index}", f"instance-{index}"],
            )

        class OOMThenSuccessWorker:
            """Purpose: Emulate a restartable Chronos worker that reports one OOM.

            Inputs: Worker commands and request mappings with batch IDs and jobs.
            Outputs: Ready/error/result mappings; records class-level calls without external resources.
            """
            starts = 0
            requests = 0
            commands = []
            payloads = []

            def __init__(self, command, startup_timeout):
                """Purpose: Capture worker startup arguments for later assertions.

                Inputs: Command sequence and numeric startup timeout.
                Outputs: Initialized instance state; appends the command to class request history.
                """
                self.command = command
                self.startup_timeout = startup_timeout
                type(self).commands.append(command)

            def start(self):
                """Purpose: Report deterministic accelerator readiness after a logical start.

                Inputs: Existing class-level start counter.
                Outputs: Chronos-ready mapping; increments ``starts`` and launches no process.
                """
                type(self).starts += 1
                return {
                    "type": "ready",
                    "accelerator_backend": "test",
                    "accelerator_memory": {"available_bytes": None},
                    "model_load_count": 1,
                    "model_load_seconds": 0.01,
                }

            def request(self, payload, timeout):
                """Purpose: Return one OOM response before serving split retry requests.

                Inputs: Chronos request mapping and timeout value.
                Outputs: Error or result mapping; records payloads and increments request state.
                """
                type(self).requests += 1
                type(self).payloads.append(payload)
                if type(self).requests == 1:
                    return {
                        "type": "error",
                        "batch_id": payload["batch_id"],
                        "error_kind": "out_of_memory",
                        "error": "simulated OOM",
                    }
                values = [3.0, 3.0]
                return {
                    "type": "result",
                    "batch_id": payload["batch_id"],
                    "results": [
                        {
                            "id": job["id"],
                            "mean": values,
                            "median": values,
                            "quantiles": [values] * 9,
                        }
                        for job in payload["jobs"]
                    ],
                    "effective_batch_size": len(payload["jobs"]),
                    "inference_seconds": 0.1,
                    "peak_process_memory_bytes": 100,
                    "accelerator_memory": {"available_bytes": None},
                }

            def close(self, force=False):
                """Purpose: Satisfy the worker cleanup protocol for the in-memory fake.

                Inputs: Optional boolean force flag.
                Outputs: None; performs no process, file, or database cleanup.
                """
                return None

        self.coordinator.execution_hardware = lambda profile: {"accelerator_backend": "test"}
        execution = resolve_execution_profile(
            "sequential_safe", {"chronos_inference_batch_size": 2}
        )
        with patch(
            "util.experiment_execution.PersistentChronosWorker", OOMThenSuccessWorker
        ):
            result = self.coordinator.run_process("experiment", 4, execution=execution)
        self.assertEqual(result["counts"], {"completed": 2})
        self.assertEqual(OOMThenSuccessWorker.starts, 2)
        chronos = self.coordinator.config["models"]["chronos_2"]
        self.assertTrue(all(
            payload["quantile_levels"] == list(self.coordinator.quantiles)
            and payload["cross_learning"] == chronos["cross_learning"]
            and payload["predict_batches_jointly"] == chronos["predict_batches_jointly"]
            for payload in OOMThenSuccessWorker.payloads
        ))
        self.assertTrue(all(
            chronos["revision"] in command
            and chronos["dtype"] in command
            and str(self.coordinator.configuration.execution["thread_limits"]["chronos"])
            in command
            for command in OOMThenSuccessWorker.commands
        ))
        self.assertEqual(connection.execute("SELECT count(*) FROM forecasts").fetchone()[0], 2)
        metadata = [
            json.loads(row[0])
            for row in connection.execute("SELECT execution_metadata FROM forecasts").fetchall()
        ]
        self.assertTrue(all(value["retry_count"] == 1 for value in metadata))
        self.assertTrue(all(value["effective_batch_size"] == 1 for value in metadata))


class ConfiguredPlanningTests(unittest.TestCase):
    """Purpose: Exercise authoritative 100-series planning against seeded dataset state.

    Inputs: GiftEval-style descriptions, committed configuration, and a seeded dataset row.
    Outputs: Plan/task assertions; each test creates then removes a temporary DuckDB tree.
    """
    def setUp(self):
        """Purpose: Provision planning state and its required dataset record.

        Inputs: The committed 100-series experiment configuration.
        Outputs: Temporary-directory and coordinator attributes; creates and seeds DuckDB.
        """
        self.directory = Path(tempfile.mkdtemp())
        initialize_test_database(self.directory / "poc1.duckdb")
        self.coordinator = ExperimentCoordinator(self.directory / "poc1.duckdb")
        self.coordinator.connection.execute(
            """INSERT INTO datasets
            (dataset_id, dataset_name, source_system, source_revision,
             source_file_hashes, import_configuration,
             import_configuration_hash, frequency)
            VALUES ('dataset', 'm4_daily', 'test', 'revision', '{}', '{}',
                    'configuration', 'D')"""
        )

    def tearDown(self):
        """Purpose: Release all configured-planning fixture state.

        Inputs: The current coordinator and temporary-directory attributes.
        Outputs: None; closes DuckDB and recursively removes the temporary directory.
        """
        self.coordinator.close()
        shutil.rmtree(self.directory)

    @staticmethod
    def _description(limit: int) -> dict:
        """Purpose: Build a deterministic GiftEval-style M4 Daily description.

        Inputs: Integer number of official-prefix instances to include.
        Outputs: Description mapping with benchmark metadata and synthetic context/actual arrays.
        """
        return {
            "configuration_name": "m4_daily/D/short",
            "dataset_name": "m4_daily",
            "frequency": "D",
            "term": "short",
            "prediction_length": 14,
            "window_count": 1,
            "seasonality": 1,
            "domain": "Econ/Fin",
            "num_variates": 1,
            "available_instances": 4_227,
            "instances": [
                {
                    "official_position": index,
                    "item_id": str(index),
                    "variate_id": "0",
                    "window_id": "short/000",
                    "start": "2000-01-01",
                    "forecast_start": "2000-01-04",
                    "context": [1.0, 2.0, 3.0],
                    "actual": [4.0] * 14,
                }
                for index in range(limit)
            ],
        }

    def _gift_bridge(self, *arguments, **_kwargs):
        """Purpose: Emulate the GiftEval describe bridge for planning tests.

        Inputs: Command-style arguments containing ``--limit``; ignored keyword arguments.
        Outputs: A description mapping from ``_description``; no process or file side effects.
        """
        limit = int(arguments[arguments.index("--limit") + 1])
        return self._description(limit)

    def test_dry_plan_selects_configured_official_prefix(self):
        """The dry plan reports the configured prefix and configuration-derived counts."""
        self.coordinator._gift_bridge = self._gift_bridge
        plan = self.coordinator.plan(dry_run=True)
        self.assertEqual(plan["selection"], {"method": "first_official", "count": 100})
        self.assertEqual(plan["series_count"], 100)
        self.assertEqual(plan["forecast_instances"], 100)
        self.assertEqual(plan["candidate_forecast_rows"], 1_200)
        self.assertEqual(plan["official_evaluation_rows"], 12)
        self.assertEqual(
            plan["task_counts"],
            {"2": 200, "3": 400, "4": 800, "5": 1_200, "6": 12},
        )

    def test_plan_rejects_duplicate_official_series(self):
        """Planning rejects a bridge response that duplicates one configured prefix series."""
        def duplicate_bridge(*arguments, **_kwargs):
            """Purpose: Produce an invalid bridge response with a duplicate official series.

            Inputs: Command-style bridge arguments and ignored keyword arguments.
            Outputs: Description mapping; mutates its final item ID when the requested limit is 100.
            """
            result = self._gift_bridge(*arguments, **_kwargs)
            if int(arguments[arguments.index("--limit") + 1]) == 100:
                result["instances"][-1]["item_id"] = "0"
            return result

        self.coordinator._gift_bridge = duplicate_bridge
        with self.assertRaisesRegex(RuntimeError, "99 distinct series"):
            self.coordinator.plan(dry_run=True)

    def test_replanning_configured_experiment_does_not_duplicate_tasks(self):
        """Repeated planning returns one identity and one deterministic task graph."""
        self.coordinator._gift_bridge = self._gift_bridge
        planned = self.coordinator.plan()
        repeated = self.coordinator.plan()
        self.assertEqual(planned.experiment_id, repeated.experiment_id)
        self.assertEqual(planned.scope, "first_official:100")
        self.assertEqual(planned.instance_count, 100)
        self.assertEqual(planned.task_counts, EXPECTED_100_TASK_COUNTS)
        self.assertEqual(repeated.task_counts, EXPECTED_100_TASK_COUNTS)
        self.assertEqual(
            self.coordinator.connection.execute(
                "SELECT count(*) FROM experiment_tasks WHERE experiment_id=?",
                [planned.experiment_id],
            ).fetchone()[0],
            sum(EXPECTED_100_TASK_COUNTS.values()),
        )


if __name__ == "__main__":
    unittest.main()
