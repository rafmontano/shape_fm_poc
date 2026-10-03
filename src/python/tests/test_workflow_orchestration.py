# ==============================================================================
# test_workflow_orchestration.py
#
# Purpose: Verify Prefect hierarchy, dependency failure, retry, skip, and no-cache contracts.
# Inputs: Bounded in-memory runner doubles and Prefect's isolated test harness.
# Outputs: unittest assertions and temporary Prefect operational state only.
# Run from: PYTHONPATH=src/python .tools/uv/uv run --locked --no-sync python -m unittest tests.test_workflow_orchestration
# ==============================================================================

"""Verify ShapeFM's thin Prefect orchestration contracts."""

import unittest
import tempfile
from pathlib import Path

from prefect.testing.utilities import prefect_test_harness

from util import shared_workflow_orchestration as workflows


class WorkflowOrchestrationTests(unittest.TestCase):
    """Exercise real local Prefect flows with bounded deterministic runner doubles."""

    @staticmethod
    def _inspect(process_id: int) -> dict:
        """Return deterministic accepted input state for one test gate."""
        return {
            "process_id": process_id,
            "status_before": "pending",
            "output_validated": True,
        }

    @staticmethod
    def _validate(process_id: int, result: dict) -> dict:
        """Return deterministic committed-output validation for one test gate."""
        if result.get("process_id") != process_id:
            raise RuntimeError("mismatched test gate result")
        return {"process_id": process_id, "output_validated": True}

    def test_experiment_runs_named_gates_in_order_and_skips_completed_work(self):
        """Only outstanding selected gates execute, in prerequisite order."""
        calls: list[int] = []
        recorded: list[str] = []

        def runner(process_id: int) -> dict:
            """Record one gate invocation and return a deterministic summary."""
            calls.append(process_id)
            return {"process_id": process_id, "status": "completed"}

        with prefect_test_harness():
            result = workflows.experiment_flow(
                (1, 2, 3),
                (1,),
                self._inspect,
                runner,
                self._validate,
                retries=0,
                recorder=recorded.append,
            )

        self.assertEqual(calls, [2, 3])
        self.assertEqual(
            [item["status"] for item in result["processes"]],
            ["skipped_completed", "completed", "completed"],
        )
        self.assertEqual(recorded, [result["prefect_flow_run_id"]])
        self.assertTrue(result["processes"][1]["prefect_task_run_id"])
        self.assertTrue(result["processes"][1]["prefect_gate_flow_run_id"])
        self.assertEqual(
            set(result["processes"][1]["prefect_substeps"]),
            {"inspect_task_run_id", "execute_task_run_id", "validate_task_run_id"},
        )
        self.assertTrue(result["processes"][1]["validation"]["output_validated"])

    def test_compute_owned_gate_failure_is_not_retried_by_outer_writer(self):
        """An outer writer gate is never repeated after compute owns retries."""
        attempts = {2: 0, 3: 0}

        def runner(process_id: int) -> dict:
            """Fail Process 02 once, then return successful gate summaries."""
            attempts[process_id] += 1
            if process_id == 2 and attempts[process_id] == 1:
                raise RuntimeError("transient test fault")
            return {"process_id": process_id, "status": "completed"}

        with prefect_test_harness(), self.assertRaisesRegex(
            RuntimeError, "transient test fault"
        ):
            workflows.experiment_flow(
                (2, 3), (), self._inspect, runner, self._validate, retries=1
            )

        self.assertEqual(attempts, {2: 1, 3: 0})

    def test_permanent_gate_failure_blocks_downstream_and_parent_success(self):
        """An exhausted required gate prevents its dependant from starting."""
        calls: list[int] = []

        def runner(process_id: int) -> dict:
            """Raise a controlled permanent failure from the first selected gate."""
            calls.append(process_id)
            raise RuntimeError("permanent test fault")

        with prefect_test_harness(), self.assertRaisesRegex(
            RuntimeError, "permanent test fault"
        ):
            workflows.experiment_flow(
                (2, 3), (), self._inspect, runner, self._validate, retries=1
            )

        self.assertEqual(calls, [2])

    def test_bounded_compute_flow_drains_success_and_failure_results(self):
        """Named framework tasks retain successful batches when another batch fails."""
        batches = [
            [{"id": "good", "values": [1.0, 2.0], "method": "identity"}],
            [{"id": "bad", "values": [1.0, 2.0], "method": "not-a-method"}],
        ]
        with prefect_test_harness():
            outcomes = list(workflows.run_gate_compute_flow(
                process_id=3, batches=batches, options={}, scheduler_address=None,
                retries=0, max_in_flight=2, local_workers=2,
            ))
        self.assertEqual(len(outcomes), 2)
        successful = [item for item in outcomes if "response" in item]
        failed = [item for item in outcomes if "error" in item]
        self.assertEqual(successful[0]["response"]["results"][0]["id"], "good")
        self.assertEqual(failed[0]["batch"][0]["id"], "bad")

    def test_large_scientific_batch_stays_out_of_flow_parameters(self):
        """A payload above Prefect's 512 KiB limit still computes through the flow."""
        values = [1234.5] * 100_000
        values[0], values[-1] = -3.0, 17.0
        with prefect_test_harness():
            outcomes = list(workflows.run_gate_compute_flow(
                process_id=3,
                batches=[[{"id": "large", "values": values, "method": "identity"}]],
                options={}, scheduler_address=None, retries=0,
                max_in_flight=1, local_workers=1,
            ))
        self.assertEqual(outcomes[0]["response"]["results"][0]["values"], values)

    def test_invalid_committed_output_blocks_downstream_gate(self):
        """A failed post-commit contract check prevents downstream execution."""
        calls: list[int] = []

        def runner(process_id: int) -> dict:
            """Return nominal execution output for each invoked gate."""
            calls.append(process_id)
            return {"process_id": process_id, "status": "completed"}

        def validator(process_id: int, _result: dict) -> dict:
            """Reject the first gate as if its stored output were mismatched."""
            if process_id == 2:
                raise RuntimeError("stored output mismatch")
            return {"process_id": process_id, "output_validated": True}

        with prefect_test_harness(), self.assertRaisesRegex(
            RuntimeError, "stored output mismatch"
        ):
            workflows.experiment_flow(
                (2, 3), (), self._inspect, runner, validator, retries=0
            )

        self.assertEqual(calls, [2])

    def test_resume_skips_commit_when_acknowledgement_was_lost(self):
        """DuckDB completion remains authoritative after post-commit flow failure."""
        calls = 0
        committed: set[int] = set()

        def runner(process_id: int) -> dict:
            """Simulate one repeat-safe scientific commit."""
            nonlocal calls
            calls += 1
            committed.add(process_id)
            return {"process_id": process_id, "status": "completed"}

        def lost_acknowledgement(_process_id: int, _result: dict) -> dict:
            """Fail after the simulated commit but before flow acknowledgement."""
            raise RuntimeError("acknowledgement lost")

        with prefect_test_harness(), self.assertRaisesRegex(
            RuntimeError, "acknowledgement lost"
        ):
            workflows.experiment_flow(
                (2,), (), self._inspect, runner, lost_acknowledgement, retries=0
            )

        with prefect_test_harness():
            resumed = workflows.experiment_flow(
                (2,), tuple(committed), self._inspect, runner, self._validate, retries=0
            )

        self.assertEqual(calls, 1)
        self.assertEqual(resumed["processes"][0]["status"], "skipped_completed")
        self.assertTrue(resumed["processes"][0]["validation"]["output_validated"])
        self.assertTrue(
            resumed["processes"][0]["prefect_substeps"]["inspect_task_run_id"]
        )

    def test_window_preparation_is_a_restartable_prefect_workflow(self):
        """The optional S1 route returns both workflow and task identities."""
        calls = 0

        def runner() -> dict:
            """Return one bounded preparation summary without external writes."""
            nonlocal calls
            calls += 1
            return {"preparation_id": "preparation/test", "total_windows": 2}

        def inspector() -> dict:
            """Return deterministic parent/child input validation."""
            return {"process_01_status": "completed"}

        def validator(result: dict) -> dict:
            """Return deterministic persisted-window validation."""
            return {
                "preparation_id": result["preparation_id"],
                "persisted_windows": result["total_windows"],
                "output_validated": True,
            }

        with prefect_test_harness():
            result = workflows.window_preparation_flow(
                inspector, runner, validator, retries=0
            )

        self.assertEqual(calls, 1)
        self.assertEqual(result["preparation_id"], "preparation/test")
        self.assertTrue(result["prefect_flow_run_id"])
        self.assertTrue(result["prefect_task_run_id"])
        self.assertTrue(result["validation"]["output_validated"])

    def test_scientific_task_results_are_not_persisted_or_cached(self):
        """Workflow tasks explicitly disable Prefect result persistence and caching."""
        for prefect_task in (
            workflows.record_workflow_identity,
            workflows.inspect_gate,
            workflows.execute_gate,
            workflows.validate_gate,
            workflows.compute_clean_batch,
            workflows.compute_transform_batch,
            workflows.compute_combine_batch,
            workflows.compute_evaluation,
            workflows.inspect_window_preparation,
            workflows.execute_window_preparation,
            workflows.validate_window_preparation,
        ):
            with self.subTest(task=prefect_task.name):
                self.assertFalse(prefect_task.persist_result)
                self.assertIs(prefect_task.cache_policy, workflows.NO_CACHE)

    def test_overlapping_writer_workflow_is_refused_and_lock_is_recoverable(self):
        """A second writer fails immediately, while a later writer can recover."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "experiment.duckdb"
            with workflows.research_writer_locks((database,)):
                with self.assertRaisesRegex(RuntimeError, "another writer workflow"):
                    with workflows.research_writer_locks((database,)):
                        self.fail("overlapping writer lock unexpectedly succeeded")
            with workflows.research_writer_locks((database,)):
                pass


if __name__ == "__main__":
    unittest.main()
