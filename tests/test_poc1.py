import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from shapefm.config import json_fingerprint
from shapefm.execution import resolve_execution_profile
from shapefm.poc1 import (
    POC1Coordinator,
    _batches,
    _run_external_batches,
    _run_parallel,
    _transform_job,
    scientific_configuration,
    validated_submission_metadata,
)
from shapefm.transformations import inverse, transform


class TransformationTests(unittest.TestCase):
    def test_provisional_selection_does_not_change_scientific_identity(self):
        first = {"models": {"a": {"revision": "1"}}, "provisional_candidate": "a"}
        second = {"models": {"a": {"revision": "1"}}, "provisional_candidate": "b"}
        self.assertEqual(
            json_fingerprint(scientific_configuration(first)),
            json_fingerprint(scientific_configuration(second)),
        )

    def test_submission_metadata_does_not_change_scientific_identity(self):
        first = {"contract_version": "1", "submission_metadata": {"org": "first"}}
        second = {"contract_version": "1", "submission_metadata": {"org": "second"}}
        self.assertEqual(scientific_configuration(first), scientific_configuration(second))

    def test_minmax_standardize_round_trip_is_ordered_and_exact_within_tolerance(self):
        source = (-7.5, 2.0, 11.25, 4.5)
        result = transform(source, "minmax_then_standardize")
        restored = inverse(result.values, "minmax_then_standardize", result.parameters)
        for expected, actual in zip(source, restored, strict=True):
            self.assertAlmostEqual(expected, actual, places=12)

    def test_constant_series_has_deterministic_round_trip(self):
        result = transform([3.25] * 5, "minmax_then_standardize")
        self.assertEqual(result.values, (0.0,) * 5)
        self.assertEqual(
            inverse([-100.0, 0.0, 100.0], "minmax_then_standardize", result.parameters),
            (3.25, 3.25, 3.25),
        )

    def test_future_actuals_cannot_change_fitted_parameters(self):
        context = [1.0, 4.0, 9.0]
        first = transform(context, "minmax_then_standardize")
        second = transform(context, "minmax_then_standardize")
        future = [10_000.0, -10_000.0]
        self.assertNotIn(max(future), first.parameters.values())
        self.assertEqual(first, second)

    def test_sequential_and_two_worker_transform_paths_are_equal(self):
        jobs = [([float(i), float(i + 2), float(i - 3)], "minmax_then_standardize") for i in range(8)]
        self.assertEqual(
            _run_parallel(_transform_job, jobs, 1),
            _run_parallel(_transform_job, jobs, 2),
        )


class ExternalBatchTests(unittest.TestCase):
    def test_batches_are_bounded_and_completed_batches_survive_later_failure(self):
        batches = _batches(list(range(7)), 3)
        self.assertEqual([len(batch) for batch in batches], [3, 3, 1])

        def fail_second(batch):
            if batch[0] == 3:
                raise RuntimeError("worker failed")
            return batch

        completed = []
        with self.assertRaisesRegex(RuntimeError, "worker failed"):
            for result in _run_external_batches(fail_second, batches, workers=1):
                completed.append(result)
        self.assertEqual(completed, [[0, 1, 2]])

    def test_external_batches_have_identical_sequential_and_parallel_results(self):
        batches = _batches(list(range(10)), 2)
        self.assertEqual(
            list(_run_external_batches(sum, batches, workers=1)),
            list(_run_external_batches(sum, batches, workers=2)),
        )


class ContractValidationTests(unittest.TestCase):
    def test_multi_window_configuration_is_rejected(self):
        coordinator = object.__new__(POC1Coordinator)
        coordinator.config = {"benchmark": {"configuration": "m4_daily/D/short"}}
        with self.assertRaisesRegex(RuntimeError, "exactly one"):
            coordinator._validate_official_configuration(
                {"configuration_name": "m4_daily/D/short", "window_count": 2}
            )

    def test_submission_metadata_is_required_and_validated(self):
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
    def setUp(self):
        self.directory = Path(tempfile.mkdtemp())
        self.coordinator = POC1Coordinator(self.directory / "poc1.duckdb")

    def tearDown(self):
        self.coordinator.close()
        shutil.rmtree(self.directory)

    def _insert_benchmark_and_instances(self):
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
        connection = self.coordinator.connection
        connection.execute(
            """INSERT INTO experiment_tasks
            (task_id, experiment_id, stage, status) VALUES ('task', 'experiment', 2, 'pending')"""
        )
        invocation = self.coordinator._begin_invocation("experiment", 2, 1, "cpu", 1)
        attempt = self.coordinator._start_tasks([("task", None, None, None)], invocation)["task"]

        def invalid_insert():
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

    def test_stage2_batched_failure_preserves_completed_batch_and_retries_rest(self):
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
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("second external batch failed")
            job = payload["jobs"][0]
            return {"results": [{"id": job["id"], "values": job["context"]}], "packages": {}}

        self.coordinator._r_worker = failing_worker
        with self.assertRaisesRegex(RuntimeError, "Stage 2 failed"):
            self.coordinator.run_gate("experiment", 2, workers=1, batch_size=1)
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
        result = self.coordinator.run_gate(
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
            if json.loads(row[2]) == {"cleaning_workers": 2}
        )
        self.assertEqual(invocation[0], "sequential_safe")
        self.assertEqual(json.loads(invocation[1])["cleaning_workers"], 2)
        self.assertEqual(json.loads(invocation[2]), {"cleaning_workers": 2})
        self.assertIn("logical_cpu_count", json.loads(invocation[3]))
        self.assertEqual(
            connection.execute("SELECT task_id FROM experiment_tasks ORDER BY task_id").fetchall(),
            [("task-0",), ("task-1",)],
        )

    def test_stage4_batched_failure_preserves_forecast_and_retries_rest(self):
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

        def worker(payload):
            nonlocal calls
            calls += 1
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
        with self.assertRaisesRegex(RuntimeError, "Stage 4 failed"):
            self.coordinator.run_gate("experiment", 4, workers=1, batch_size=1)
        self.assertEqual(connection.execute("SELECT count(*) FROM forecasts").fetchone()[0], 1)
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
        result = self.coordinator.run_gate("experiment", 4, workers=1, batch_size=1)
        self.assertEqual(result["selected"], 1)
        self.assertEqual(connection.execute("SELECT count(*) FROM forecasts").fetchone()[0], 2)
        self.assertEqual(
            connection.execute(
                "SELECT attempt_count FROM experiment_tasks ORDER BY task_id"
            ).fetchall(),
            [(1,), (2,)],
        )

    def test_chronos_oom_restarts_splits_and_preserves_successes(self):
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
            starts = 0
            requests = 0

            def __init__(self, command):
                self.command = command

            def start(self):
                type(self).starts += 1
                return {
                    "type": "ready",
                    "accelerator_backend": "test",
                    "accelerator_memory": {"available_bytes": None},
                    "model_load_count": 1,
                    "model_load_seconds": 0.01,
                }

            def request(self, payload):
                type(self).requests += 1
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
                return None

        self.coordinator.execution_hardware = lambda profile: {"accelerator_backend": "test"}
        execution = resolve_execution_profile(
            "sequential_safe", {"chronos_inference_batch_size": 2}
        )
        with patch("shapefm.poc1.PersistentChronosWorker", OOMThenSuccessWorker):
            result = self.coordinator.run_gate("experiment", 4, execution=execution)
        self.assertEqual(result["counts"], {"completed": 2})
        self.assertEqual(OOMThenSuccessWorker.starts, 2)
        self.assertEqual(connection.execute("SELECT count(*) FROM forecasts").fetchone()[0], 2)
        metadata = [
            json.loads(row[0])
            for row in connection.execute("SELECT execution_metadata FROM forecasts").fetchall()
        ]
        self.assertTrue(all(value["retry_count"] == 1 for value in metadata))
        self.assertTrue(all(value["effective_batch_size"] == 1 for value in metadata))


if __name__ == "__main__":
    unittest.main()
