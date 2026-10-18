"""Process 06 coordinator delivery and accepted-state resume, without science/SSH."""

from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
import subprocess
import unittest
from unittest.mock import Mock, patch

from p06_evaluate import flow, publication
from util.runtime import FinalResearchSummary


class OutputDeliveryTests(unittest.TestCase):
    """Exercise declared-file transport and durable publication restart boundaries."""

    def setUp(self):
        self.root = Path("/research/shape_fm")
        self.context = SimpleNamespace(request=SimpleNamespace(root=self.root),
            execution=SimpleNamespace(settings={}))
        self.summary = FinalResearchSummary({}, tuple(str(self.root / name) for name in (
            "results/tables/table_1.csv", "results/figures/figure_2_cd_daily.pdf")))

    def configure_workers(self):
        self.context.execution.settings = {"coordinator": "ubuntu_primary", "workers": [
            {"machine": "ubuntu_primary"}, {"machine": "macbook_pro"},
            {"machine": "macbook_pro"}]}

    @patch.object(publication.subprocess, "run")
    def test_sequential_and_single_machine_need_no_transfer(self, run):
        publication.deliver_published_outputs(self.context, self.summary)
        self.context.execution.settings = {"coordinator": "ubuntu_primary",
            "workers": [{"machine": "ubuntu_primary"}]}
        publication.deliver_published_outputs(self.context, self.summary)
        run.assert_not_called()

    @patch.object(publication.subprocess, "run")
    def test_distinct_hosts_receive_only_declared_relative_files(self, run):
        self.configure_workers()
        publication.deliver_published_outputs(self.context, self.summary)
        run.assert_called_once()
        args, kwargs = run.call_args
        self.assertEqual(args[0], ["rsync", "-rtR", "-e", "ssh -o BatchMode=yes",
            "results/tables/table_1.csv", "results/figures/figure_2_cd_daily.pdf",
            "monta@RMMacbookPro.local:/Users/monta/Documents/Projects/shape_fm_poc/"])
        self.assertEqual(kwargs, {"cwd": self.root, "check": True, "timeout": 120})

    @patch.object(publication.subprocess, "run")
    def test_unavailable_host_warns_and_other_hosts_continue(self, run):
        self.context.execution.settings = {"workers": [
            {"machine": "macbook_pro"}, {"machine": "ubuntu_primary"}]}
        run.side_effect = [subprocess.CalledProcessError(255, "rsync"), None]
        with self.assertLogs(publication.LOGGER, level="WARNING") as logs:
            publication.deliver_published_outputs(self.context, self.summary)
        self.assertIn("delivery pending for macbook_pro", logs.output[0])
        self.assertEqual(run.call_count, 2)

    @patch.object(publication.subprocess, "run")
    def test_completed_process_retries_delivery_without_regeneration(self, run):
        self.configure_workers()
        self.context.state = Mock()
        self.context.state.is_process_accepted.return_value = True
        self.context.state.is_subflow_accepted.return_value = True
        self.context.evaluate = Mock()
        self.context.evaluate.research_results.return_value = "stored-results"
        self.context.evaluate.published_summary.return_value = self.summary
        self.context.tracking = Mock()
        self.context.tracking.process.side_effect = lambda *args: nullcontext()
        self.context.tracking.subflow.side_effect = lambda *args: nullcontext()
        run.side_effect = [OSError("offline"), None]
        with patch.object(flow, "publish_approved_outputs", flow.publish_approved_outputs.fn):
            with self.assertLogs(publication.LOGGER, level="WARNING"):
                self.assertEqual(flow.run_evaluate.fn(self.context), "stored-results")
            self.assertEqual(flow.run_evaluate.fn(self.context), "stored-results")
        self.assertEqual(run.call_count, 2)
        self.context.evaluate.publish_outputs.assert_not_called()
        self.context.state.accept_subflow.assert_not_called()
        self.context.state.accept_process.assert_not_called()
        self.context.tracking.publish_final.assert_not_called()


if __name__ == "__main__":
    unittest.main()
