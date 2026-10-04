# ==============================================================================
# test_forecast_flow.py
# Purpose: Exercise Prefect forecast retry, validation and durable resume contracts.
# Inputs: Two asymmetric synthetic series in an isolated DuckDB and bounded providers.
# Outputs: Assertions; never production data or a production cluster.
# Run from: PYTHONPATH=src/python .venv/bin/python -m unittest tests.test_forecast_flow
# ==============================================================================

"""Bounded forecast flow contracts using the real storage and retrieval path."""

import json
import unittest

from util.shared_configuration import canonical_json
from util.shared_experiment_execution import get_forecast
from util.p04_00_forecast_contract import ForecastContract
from util.p04_01_forecast_flow import run_ordinary_forecast_flow
from util.p04_02_forecast_provider import LocalAutoArimaProvider
from util.p04_03_forecast_storage import ForecastStorage
from util.shared_process_storage import ProcessStorage
from tests import test_experiment_execution as fixtures


class ForecastContractTests(unittest.TestCase):
    """Exercise exact common fields and capability-specific output invariants."""

    @staticmethod
    def request(capability="probabilistic"):
        """Build one asymmetric valid request without production dependencies."""
        return {
            "contract_version": "forecast-v1", "experiment_id": "experiment",
            "task_id": "task", "forecast_instance_id": "instance",
            "variant_id": "variant", "dataset_id": "dataset", "series_id": "series",
            "model_id": "auto_arima" if capability == "probabilistic" else "ses",
            "required_capability": capability, "context": [1.0, 4.0, 2.0],
            "horizon": 2, "frequency": "D", "seasonal_period": 7,
            "input_scale": "transformed",
            "quantile_levels": [0.025, 0.5, 0.975]
            if capability == "probabilistic" else None,
            "seed": 1234, "model_settings": {},
        }

    def test_probabilistic_success_requires_exact_levels_and_median(self):
        """No provider may omit tail levels, interpolate rows, or disagree at q0.5."""
        contract = ForecastContract()
        request = self.request()
        result = fixtures.forecast_result(
            request, [4.0, 8.0], quantiles=[[1.0, 2.0], [4.0, 8.0], [7.0, 12.0]]
        )
        self.assertIs(contract.validate_result(request, result), result)
        for mutation, message in (
            (lambda value: value["quantile_levels"].pop(), "levels do not match"),
            (lambda value: value.__setitem__("median", [4.0, 9.0]), "does not equal q0.5"),
            (lambda value: value["quantiles"].__setitem__(2, [0.0, 12.0]), "cross"),
        ):
            broken = {**result, "quantile_levels": list(result["quantile_levels"]),
                      "median": list(result["median"]),
                      "quantiles": [list(row) for row in result["quantiles"]]}
            mutation(broken)
            with self.assertRaisesRegex(ValueError, message):
                contract.validate_result(request, broken)

    def test_mean_only_success_and_error_use_exact_common_fields(self):
        """Point forecasts keep probabilistic fields null and errors retain identities."""
        contract = ForecastContract()
        request = self.request("mean_only")
        result = {
            "contract_version": "forecast-v1", "status": "success",
            "experiment_id": "experiment", "task_id": "task",
            "forecast_instance_id": "instance", "variant_id": "variant",
            "requested_model_id": "ses", "executed_model_id": "ses",
            "forecast_capability": "mean_only", "output_scale": "transformed",
            "horizon": 2, "mean": [2.0, 3.0], "median": None,
            "quantile_levels": None, "quantiles": None, "fallback_used": False,
            "fallback_reason": None, "runtime_seconds": 0.01, "provenance": {},
        }
        self.assertIs(contract.validate_result(request, result), result)
        with self.assertRaisesRegex(ValueError, "null probabilistic fields"):
            contract.validate_result(request, {**result, "median": [2.0, 3.0]})
        error = {
            "contract_version": "forecast-v1", "status": "error",
            "experiment_id": "experiment", "task_id": "task",
            "forecast_instance_id": "instance", "variant_id": "variant",
            "requested_model_id": "ses", "error_type": "fit_error",
            "error_message": "controlled failure", "provenance": {},
        }
        self.assertIs(contract.validate_result(request, error), error)
        with self.assertRaisesRegex(ValueError, "exactly"):
            contract.validate_result(request, {**error, "extra": True})

    def test_only_stlm_accepts_the_fixed_autoarima_fallback_identity(self):
        """The common envelope rejects the removed pool-wide seasonal-naive fallback."""
        contract = ForecastContract()
        request = {**self.request(), "model_id": "stlm_ar"}
        result = fixtures.forecast_result(
            request, [4.0, 8.0], fallback=True, executed_model="auto_arima"
        )
        result["provenance"] = {
            "package": "forecast", "package_version": "fixture",
            "settings": {"selected_branch": "auto_arima_d0_D0", "d": 0, "D": 0},
            "original_stl_error": result["fallback_reason"],
        }
        self.assertIs(contract.validate_result(request, result), result)
        with self.assertRaisesRegex(ValueError, "only stlm_ar"):
            contract.validate_result(
                {**request, "model_id": "ets"},
                {**result, "requested_model_id": "ets", "executed_model_id": "snaive"},
            )

    def test_chronos_adapter_rearranges_crossings_per_horizon(self):
        """Provider-side rearrangement preserves ordered columns and reports changes."""
        crossed = [[1.0, 8.0], [4.0, 3.0], [7.0, 6.0]]
        arranged, changed = ForecastContract.noncrossing_quantiles(crossed)
        self.assertEqual(arranged, [[1.0, 3.0], [4.0, 6.0], [7.0, 8.0]])
        self.assertTrue(changed)
        ordered = [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]
        self.assertEqual(
            ForecastContract.noncrossing_quantiles(ordered), (ordered, False)
        )


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
                fixtures.forecast_result(job, [3.0, 8.0])
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
        requests = []

        def transient(payload):
            """Bounded functional fault injector needs no reusable state object."""
            calls.append(payload["jobs"][0]["task_id"])
            requests.append(payload["jobs"][0].copy())
            if len(calls) <= 2:
                raise RuntimeError("injected transient")
            return self.response(payload)

        self.run_flow(LocalAutoArimaProvider(transient, {}), retries=2)
        self.assertEqual(calls, ["task-0", "task-0", "task-0", "task-1"])
        self.assertEqual(requests[0], requests[1])
        self.assertEqual(requests[1], requests[2])
        self.assertEqual(requests[0]["model_id"], "auto_arima")
        self.assertEqual(requests[0]["model_settings"], requests[2]["model_settings"])
        self.assertEqual(self.coordinator.connection.execute(
            "SELECT mean FROM forecasts ORDER BY forecast_instance_id"
        ).fetchall(), [([3.0, 8.0],), ([3.0, 8.0],)])
        self.coordinator.close()
        path = self.fixture.directory / "poc1.duckdb"
        self.assertEqual(ProcessStorage(path).validate(4)["expected_task_count"], 2)
        self.assertEqual(get_forecast(path, "experiment", "variant", "0", "auto_arima").mean,
                         (3.0, 8.0))

    def test_stlm_fallback_identity_and_reason_survive_storage_json(self):
        """Coordinator storage retains the selected fixed fallback and original STL error."""
        job = ForecastContractTests.request()
        job.update({
            "experiment_id": "experiment", "task_id": "task-0",
            "forecast_instance_id": "instance-0", "variant_id": "variant",
            "dataset_id": "dataset", "series_id": "0", "model_id": "stlm_ar",
        })
        result = fixtures.forecast_result(
            job, [3.0, 8.0], fallback=True, executed_model="auto_arima"
        )
        result["provenance"] = {
            "package": "forecast", "package_version": "fixture",
            "settings": {"selected_branch": "auto_arima_d0_D0", "d": 0, "D": 0},
            "original_stl_error": "y is not a seasonal ts object",
        }
        result["fallback_reason"] = "y is not a seasonal ts object"
        self.coordinator.config["models"]["stlm_ar"] = {"package": "forecast"}
        self.coordinator.connection.execute(
            "UPDATE experiment_tasks SET candidate='stlm_ar' WHERE task_id='task-0'"
        )
        row = next(item for item in self.coordinator._pending("experiment", 4)
                   if item[0] == "task-0")
        invocation = self.coordinator._begin_invocation("experiment", 4, 1, "cpu", 1)
        attempts = self.coordinator._start_tasks([row], invocation)
        storage = ForecastStorage(self.coordinator, "experiment", attempts)
        storage.commit_response([job], {
            "results": [result], "metadata": {"packages": {"forecast": "fixture"}},
            "runtime_seconds": 0.1,
        })
        stored = self.coordinator.connection.execute(
            "SELECT candidate, execution_metadata FROM forecasts"
        ).fetchone()
        contract = json.loads(stored[1])["forecast_contract"]
        self.assertEqual(stored[0], "stlm_ar")
        self.assertEqual(contract["requested_model_id"], "stlm_ar")
        self.assertEqual(contract["executed_model_id"], "auto_arima")
        self.assertTrue(contract["fallback_used"])
        self.assertEqual(contract["fallback_reason"], "y is not a seasonal ts object")
        self.assertEqual(
            contract["provenance"]["settings"],
            {"D": 0, "d": 0, "selected_branch": "auto_arima_d0_D0"},
        )

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
            calls.append(payload["jobs"][0]["task_id"])
            raise RuntimeError("permanent failure")

        with self.assertRaisesRegex(RuntimeError, "permanent failure"):
            self.run_flow(LocalAutoArimaProvider(fail, {}), retries=0)
        self.assertEqual(calls, ["task-0"])

    def test_coordinator_resolves_stored_and_explicit_retry_budget(self):
        """Normal coordinator dispatch must pass the effective rather than stored policy."""
        from unittest.mock import patch
        from util.shared_execution_profiles import ExecutionSettings

        # The historical v1 fixture stores one retry. Explicit zero/two settings
        # must replace it, not add to it or get overwritten by it.
        for override, expected in ((ExecutionSettings(mode="local", dask_retries=0), 1),
                                   (ExecutionSettings(mode="local", dask_retries=2), 3),
                                   (None, 2)):
            calls = []

            def failure(payload):
                """Always fail one bounded task, making every extra retry observable."""
                calls.append(payload["jobs"][0]["task_id"])
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
        from util.p04_01_forecast_flow import ordinary_forecast_flow

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
                elif overlap and batch[0]["task_id"] == "task-0":
                    self.assertTrue(gpu_started.wait(10), "overlap was silently serialized")
                if model == "chronos_2" and not overlap:
                    self.assertTrue(released.is_set(), "GPU submitted before CPU phase completed")
                if model == "chronos_2" and not released.wait(10):
                    raise RuntimeError("fixed-wave head-of-line blocking")
                with lock:
                    if model == "auto_arima":
                        cpu_seen.add(batch[0]["task_id"])
                        if len(cpu_seen) == 3:
                            released.set()
                    else:
                        gpu_seen.add(batch[0]["task_id"])
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
        from util.shared_execution_profiles import resolve_execution_profile, ExecutionSettings

        profile, _ = resolve_execution_profile("poc2_seasonal_recovery")
        with patch("util.p04_01_forecast_flow.run_ordinary_forecast_flow") as dispatch:
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

        with self.assertRaisesRegex(ValueError, "finite horizon vector"):
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
            seen.extend(job["task_id"] for job in payload["jobs"])
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
