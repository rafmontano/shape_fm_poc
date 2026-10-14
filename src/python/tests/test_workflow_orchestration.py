"""Bounded retained Process 04–06 Prefect contracts; no scientific execution.

Process 03 workflow/compute tests live at its public MVP boundary, not here.
"""

import unittest

from prefect.testing.utilities import prefect_test_harness

from util import shared_workflow_orchestration as workflows


class WorkflowOrchestrationTests(unittest.TestCase):
    def test_compute_owned_gate_failure_is_not_retried_by_outer_writer(self):
        """A failed Forecast writer is not repeated and blocks Combine."""
        calls = []

        def runner(process_id):
            calls.append(process_id)
            raise RuntimeError("transient test fault")

        with prefect_test_harness(), self.assertRaisesRegex(RuntimeError, "transient test fault"):
            workflows.experiment_flow(
                (4, 5), (),
                lambda process_id: {"process_id": process_id, "status_before": "pending"},
                runner, lambda process_id, result: result, retries=1,
            )
        self.assertEqual(calls, [4])

    def test_scientific_task_results_are_not_persisted_or_cached(self):
        """DuckDB, not a Prefect result cache, remains the accepted output owner."""
        for prefect_task in (
            workflows.record_workflow_identity,
            workflows.inspect_gate,
            workflows.execute_gate,
            workflows.validate_gate,
            workflows.compute_directional_dtw_batch,
            workflows.compute_combine_batch,
            workflows.compute_evaluation,
        ):
            with self.subTest(task=prefect_task.name):
                self.assertFalse(prefect_task.persist_result)
                self.assertIs(prefect_task.cache_policy, workflows.NO_CACHE)


if __name__ == "__main__":
    unittest.main()
