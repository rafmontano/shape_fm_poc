# ==============================================================================
# test_forecast_flow.py
# Purpose: Exercise Prefect forecast retry, validation and durable resume contracts.
# Inputs: Two asymmetric synthetic series in an isolated DuckDB and bounded providers.
# Outputs: Assertions; never production data or a production cluster.
# Run from: PYTHONPATH=src/python .venv/bin/python -m unittest tests.test_forecast_flow
# ==============================================================================

"""Bounded forecast flow contracts using the real storage and retrieval path."""

import unittest

from util.configuration import canonical_json
from util.experiment_execution import get_forecast
from util.forecast_flow import run_ordinary_forecast_flow
from util.forecast_provider import LocalAutoArimaProvider
from util.forecast_storage import ForecastStorage
from util.process_storage import ProcessStorage
from tests import test_experiment_execution as fixtures


class ForecastFlowTests(unittest.TestCase):
    """Own two-job storage fixtures; each test preserves independent expectations."""

    def setUp(self):
        """Reuse bounded database fixtures without changing production configuration."""
        self.fixture = fixtures.TransactionTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.fixture._insert_benchmark_and_instances()
        self.coordinator = self.fixture.coordinator
        connection = self.coordinator.connection
        connection.execute(
            """INSERT INTO experiments (experiment_id, benchmark_configuration_id,
               dataset_id, name, scientific_configuration, configuration_hash, scope, status)
               VALUES ('experiment','benchmark','dataset','fixture',?,'fixture',
                       'first_official:2','planned')""",
            [canonical_json({"models": {"auto_arima": {}}})],
        )
        connection.execute(
            """INSERT INTO experiment_variants (variant_id, experiment_id,
               cleaning_method, transformation_method, adjustment_method, configuration)
               VALUES ('variant','experiment','identity','identity','identity','{}')"""
        )
        for index in range(2):
            connection.execute(
                """INSERT INTO transformed_series (transformation_id, experiment_id,
                   variant_id, forecast_instance_id, preprocessing_id, transformation_method,
                   input_hash, output_hash, transformed_target, parameters, parent_result_id)
                   VALUES (?, 'experiment', 'variant', ?, ?, 'identity', 'fixture',
                           'fixture', [1.0, 2.0, 5.0], '{}', ?)""",
                [f"transform-{index}", f"instance-{index}", f"pre-{index}", f"pre-{index}"],
            )
            connection.execute(
                """INSERT INTO experiment_tasks (task_id, experiment_id, stage,
                   forecast_instance_id, variant_id, candidate, status)
                   VALUES (?, 'experiment', 4, ?, 'variant', 'auto_arima', 'pending')""",
                [f"task-{index}", f"instance-{index}"],
            )

    @staticmethod
    def response(payload):
        """Return asymmetric horizon values with no database dependency."""
        return {
            "packages": {"forecast": "fixture"},
            "results": [
                {"id": job["id"], "mean": [3.0, 8.0], "median": [3.0, 8.0],
                 "quantiles": [[3.0, 8.0]] * 9}
                for job in payload["jobs"]
            ],
        }

    def run_flow(self, provider, retries=0, storage_type=ForecastStorage, max_in_flight=1,
                 autoarima_max_in_flight=None):
        """Run the actual flow over only incomplete durable fixture identities."""
        rows = self.coordinator._pending("experiment", 4)
        invocation = self.coordinator._begin_invocation("experiment", 4, 1, "cpu", 1)
        attempts = self.coordinator._start_tasks(rows, invocation)
        storage = storage_type(self.coordinator, "experiment", attempts)
        run_ordinary_forecast_flow(
            scheduler_address=None, storage=storage, rows=rows,
            auto_provider=provider, chronos_provider=None,
            auto_batch_size=1, chronos_batch_size=1, max_in_flight=max_in_flight,
            autoarima_max_in_flight=(max_in_flight if autoarima_max_in_flight is None
                                     else autoarima_max_in_flight),
            cpu_gpu_overlap=False, distributed=False, retries=retries,
        )

    def test_retry_budget_and_stored_values(self):
        """Two retries permit three attempts, not two or a multiplied gate budget."""
        calls = []

        def transient(payload):
            """Bounded functional fault injector needs no reusable state object."""
            calls.append(payload["jobs"][0]["id"])
            if len(calls) <= 2:
                raise RuntimeError("injected transient")
            return self.response(payload)

        self.run_flow(LocalAutoArimaProvider(transient, {}), retries=2)
        self.assertEqual(calls, ["task-0", "task-0", "task-0", "task-1"])
        self.assertEqual(self.coordinator.connection.execute(
            "SELECT mean FROM forecasts ORDER BY forecast_instance_id"
        ).fetchall(), [([3.0, 8.0],), ([3.0, 8.0],)])
        self.coordinator.close()
        path = self.fixture.directory / "poc1.duckdb"
        self.assertEqual(ProcessStorage(path).validate(4)["expected_task_count"], 2)
        self.assertEqual(get_forecast(path, "experiment", "variant", "0", "auto_arima").mean,
                         (3.0, 8.0))

    def test_missing_task_and_result_are_rejected(self):
        """Deleting both rows cannot hide missing work behind surviving task counts."""
        self.run_flow(LocalAutoArimaProvider(self.response, {}))
        connection = self.coordinator.connection
        connection.execute("DELETE FROM forecasts WHERE forecast_instance_id='instance-1'")
        connection.execute("DELETE FROM experiment_tasks WHERE task_id='task-1'")
        self.coordinator.close()
        with self.assertRaisesRegex(RuntimeError, "expected=2"):
            ProcessStorage(self.fixture.directory / "poc1.duckdb").validate(4)

    def test_local_batches_overlap_without_passing_storage(self):
        """Two admitted tasks must enter compute together, not run sequentially."""
        import cloudpickle
        import threading

        provider = LocalAutoArimaProvider.from_configuration(self.coordinator.configuration)
        restored = cloudpickle.loads(cloudpickle.dumps(provider))
        self.assertEqual(restored.settings, provider.settings)
        barrier = threading.Barrier(2, timeout=10)

        def concurrent(payload):
            """A bounded concurrency probe fails if the flow serializes both tasks."""
            barrier.wait()
            return self.response(payload)

        self.run_flow(LocalAutoArimaProvider(concurrent, {}), max_in_flight=2)
        self.assertEqual(self.coordinator.connection.execute(
            "SELECT count(*) FROM forecasts"
        ).fetchone()[0], 2)

    def test_autoarima_cap_is_independent_of_larger_global_cap(self):
        """The CPU-specific cap counts submitted tasks until terminal completion."""
        import threading

        lock = threading.Lock()
        active = 0
        peak = 0

        def bounded(payload):
            """Measure actual task overlap without introducing scheduler behavior."""
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            threading.Event().wait(0.05)
            with lock:
                active -= 1
            return self.response(payload)

        self.run_flow(LocalAutoArimaProvider(bounded, {}), max_in_flight=3,
                      autoarima_max_in_flight=1)
        self.assertEqual(peak, 1)

    def test_zero_retry_budget_runs_once(self):
        """A stored zero budget must not inherit an implicit framework retry."""
        calls = []

        def fail(payload):
            """Record a permanent fault in this isolated invocation."""
            calls.append(payload["jobs"][0]["id"])
            raise RuntimeError("permanent failure")

        with self.assertRaisesRegex(RuntimeError, "permanent failure"):
            self.run_flow(LocalAutoArimaProvider(fail, {}), retries=0)
        self.assertEqual(calls, ["task-0"])

    def test_coordinator_resolves_stored_and_explicit_retry_budget(self):
        """Normal coordinator dispatch must pass the effective rather than stored policy."""
        from unittest.mock import patch
        from util.execution_profiles import ExecutionSettings

        # The historical v1 fixture stores one retry. Explicit zero/two settings
        # must replace it, not add to it or get overwritten by it.
        for override, expected in ((ExecutionSettings(mode="local", dask_retries=0), 1),
                                   (ExecutionSettings(mode="local", dask_retries=2), 3),
                                   (None, 2)):
            calls = []

            def failure(payload):
                """Always fail one bounded task, making every extra retry observable."""
                calls.append(payload["jobs"][0]["id"])
                raise RuntimeError("bounded fault")

            with (patch.object(LocalAutoArimaProvider, "from_configuration",
                               return_value=LocalAutoArimaProvider(failure, {})),
                  patch.object(self.coordinator, "execution_hardware", return_value={}),
                  self.assertRaisesRegex(RuntimeError, "Process 4 failed")):
                self.coordinator.run_process("experiment", 4, workers=1,
                                             execution_settings=override)
            self.assertEqual(len(calls), expected)

    def test_refill_does_not_wait_for_blocked_gpu(self, overlap=True):
        """Blocked GPU batches cannot occupy capacity needed by three CPU batches."""
        import threading
        from util.forecast_flow import ordinary_forecast_flow

        released = threading.Event()
        gpu_started = threading.Event()
        cpu_seen = set()
        gpu_seen = set()
        lock = threading.Lock()
        connection = self.coordinator.connection
        connection.execute(
            """INSERT INTO forecast_instances
            (forecast_instance_id, benchmark_configuration_id, dataset_id,
             series_id, variate_id, window_id, official_position,
             context_start, context_end, actual_start, actual_end, horizon,
             context_target, actual_target, identity_metadata)
            VALUES ('instance-2','benchmark','dataset','2','0','short/000',2,
                    0,3,3,5,2,[1.0,2.0,3.0],[4.0,5.0],'{}')"""
        )
        connection.execute(
            """INSERT INTO transformed_series (transformation_id, experiment_id,
               variant_id, forecast_instance_id, preprocessing_id, transformation_method,
               input_hash, output_hash, transformed_target, parameters, parent_result_id)
               VALUES ('transform-2','experiment','variant','instance-2','pre-2','identity',
                       'fixture','fixture',[1.0,2.0,5.0],'{}','pre-2')"""
        )
        connection.execute(
            """INSERT INTO experiment_tasks(task_id,experiment_id,stage,forecast_instance_id,
               variant_id,candidate,status) VALUES
               ('task-2','experiment',4,'instance-2','variant','auto_arima','pending'),
               ('gpu-0','experiment',4,'instance-0','variant','chronos_2','pending'),
               ('gpu-1','experiment',4,'instance-1','variant','chronos_2','pending')"""
        )

        class Provider:
            """Controlled latency only; actual Prefect futures and DuckDB remain in use."""
            def forecast(inner, model, batch):
                """Release both GPU calls only after every CPU batch has progressed."""
                if model == "chronos_2":
                    gpu_started.set()
                elif overlap and batch[0]["id"] == "task-0":
                    self.assertTrue(gpu_started.wait(10), "overlap was silently serialized")
                if model == "chronos_2" and not overlap:
                    self.assertTrue(released.is_set(), "GPU submitted before CPU phase completed")
                if model == "chronos_2" and not released.wait(10):
                    raise RuntimeError("fixed-wave head-of-line blocking")
                with lock:
                    if model == "auto_arima":
                        cpu_seen.add(batch[0]["id"])
                        if len(cpu_seen) == 3:
                            released.set()
                    else:
                        gpu_seen.add(batch[0]["id"])
                result = ForecastFlowTests.response({"jobs": batch})
                return {**result, "metadata": {}, "runtime_seconds": 0}

        rows = self.coordinator._pending("experiment", 4)
        invocation = self.coordinator._begin_invocation("experiment", 4, 1, "cpu", 1)
        # Use Prefect's local task runner to test submission/refill deterministically;
        # Dask resource routing is covered separately, not asserted by this test.
        self.coordinator._start_tasks(rows, invocation)
        ordinary_forecast_flow(
            self.coordinator.database_path, "experiment",
            storage_type=ForecastStorage, auto_provider=Provider(), chronos_provider=Provider(),
            auto_batch_size=1, chronos_batch_size=1, max_in_flight=2,
            autoarima_max_in_flight=1,
            cpu_gpu_overlap=overlap, distributed=True, retries=0,
        )
        self.assertTrue(released.is_set())
        self.assertEqual(cpu_seen, {"task-0", "task-1", "task-2"})
        self.assertEqual(gpu_seen, {"gpu-0", "gpu-1"})
        self.assertEqual(connection.execute("SELECT count(*) FROM forecasts").fetchone()[0], 5)

    def test_no_overlap_keeps_cpu_gpu_phases_separate(self):
        """Disabling overlap retains the phase boundary under the same refill logic."""
        self.test_refill_does_not_wait_for_blocked_gpu(overlap=False)

    def test_coordinator_propagates_profile_autoarima_cap(self):
        """The real coordinator supplies the model cap separately from total slots."""
        from unittest.mock import patch, MagicMock
        from util.execution_profiles import resolve_execution_profile, ExecutionSettings

        profile, _ = resolve_execution_profile("poc2_seasonal_recovery")
        with patch("util.forecast_flow.run_ordinary_forecast_flow") as dispatch:
            self.coordinator._run_04_forecast("experiment", [], {}, profile, "cuda",
                MagicMock(), ExecutionSettings(mode="dask", dask_scheduler_address="tcp://fixture",
                                               dask_max_in_flight=23))
        self.assertEqual(dispatch.call_args.kwargs["autoarima_max_in_flight"], 8)
        self.assertEqual(dispatch.call_args.kwargs["max_in_flight"], 23)

    def test_wrong_horizon_does_not_commit(self):
        """A finite forecast with the wrong horizon must still fail validation."""
        def wrong(payload):
            """Inject a shape fault without changing expected fixture values."""
            response = self.response(payload)
            response["results"][0]["mean"] = [3.0]
            return response

        with self.assertRaisesRegex(RuntimeError, "horizon mismatch"):
            self.run_flow(LocalAutoArimaProvider(wrong, {}))
        self.assertEqual(self.coordinator.connection.execute(
            "SELECT count(*) FROM forecasts"
        ).fetchone()[0], 0)

    def test_commit_before_ack_resume_preserves_accepted_forecasts(self):
        """Resume the same database and never recompute a committed first job."""
        class LostAcknowledgement(ForecastStorage):
            """Inject failure strictly after the real storage transaction commits."""
            def commit_response(self, batch, response):
                """Commit once and simulate loss of the coordinator acknowledgement."""
                super().commit_response(batch, response)
                raise RuntimeError("acknowledgement lost")

        with self.assertRaisesRegex(RuntimeError, "acknowledgement lost"):
            self.run_flow(LocalAutoArimaProvider(self.response, {}), storage_type=LostAcknowledgement)
        before = self.coordinator.connection.execute(
            "SELECT forecast_id, content_hash, created_at FROM forecasts"
        ).fetchone()
        seen = []

        def remaining(payload):
            """Record exactly which durable identities resume computes."""
            seen.extend(job["id"] for job in payload["jobs"])
            return self.response(payload)

        self.run_flow(LocalAutoArimaProvider(remaining, {}))
        self.assertEqual(seen, ["task-1"])
        self.assertEqual(self.coordinator.connection.execute(
            "SELECT forecast_id, content_hash, created_at FROM forecasts WHERE forecast_id=?",
            [before[0]],
        ).fetchone(), before)
        self.run_flow(LocalAutoArimaProvider(remaining, {}))
        self.assertEqual(seen, ["task-1"])


if __name__ == "__main__":
    unittest.main()
