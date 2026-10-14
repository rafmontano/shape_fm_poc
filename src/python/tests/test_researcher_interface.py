"""Non-scientific CLI/provider wiring for the shared six-process entry point.

Mocked execution and rendering verify canonical defaults and JSON contracts;
no scientific process, native calculation or output regeneration is invoked.
"""

import io
import json
import subprocess
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import experiment
from util.providers import resolve_r_period
from util.runtime import CODE_ROOT, FinalResearchSummary


class ResearcherInterfaceTests(unittest.TestCase):
    """Protect researcher-visible defaults after retiring the old entry points."""

    def test_normal_run_uses_canonical_repository_root(self):
        """The ordinary command must not select a review/test experiment package."""
        output = io.StringIO()
        with patch.object(experiment, "run_experiment", return_value="stored-id") as execute:
            with redirect_stdout(output):
                self.assertEqual(experiment.main(["run", "--processes", "5-6"]), 0)
        request = execute.call_args.args[0]
        self.assertEqual(request.root, CODE_ROOT)
        self.assertEqual(request.processes, (5, 6))
        self.assertEqual(json.loads(output.getvalue()), "stored-id")

    def test_export_returns_serializable_summary_from_publication(self):
        """Export exposes metrics and declared paths, not an unserializable object."""
        summary = FinalResearchSummary({"model.da": 0.7}, ("results/tables/table_1.csv",))
        output = io.StringIO()
        with patch("p06_evaluate.publication.publish_stored_outputs", return_value=summary):
            with redirect_stdout(output):
                self.assertEqual(experiment.main(["export"]), 0)
        self.assertEqual(json.loads(output.getvalue()),
                         {"metrics": {"model.da": 0.7}, "artifacts": ["results/tables/table_1.csv"]})

    def test_period_transport_uses_existing_process_owned_cli(self):
        """The pinned-environment request preserves frequency and optional override."""
        configuration = json.loads((CODE_ROOT / "config/experiment.json").read_text())
        for override in (None, 7):
            with self.subTest(override=override):
                configuration["pipeline"]["r_period_override"] = override
                response = {"frequency": "W", "r_period": override or 52}
                with patch("util.providers.subprocess.run", return_value=subprocess.CompletedProcess(
                        [], 0, json.dumps(response), "")) as invoke:
                    self.assertEqual(resolve_r_period(configuration, "W"), response)
                command = invoke.call_args.args[0]
                self.assertEqual(command[0], str(CODE_ROOT / "environments/gift-eval/.venv/bin/python"))
                script = CODE_ROOT / "src/python/p02_preprocess/period.py"
                self.assertTrue(script.is_file())
                self.assertEqual(command[1:], [str(script), "--frequency", "W"] +
                                 ([] if override is None else ["--override", "7"]))


if __name__ == "__main__":
    unittest.main()
