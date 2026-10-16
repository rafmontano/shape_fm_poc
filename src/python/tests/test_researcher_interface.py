"""Non-scientific CLI/provider wiring for the shared six-process entry point.

Mocked execution and rendering verify canonical defaults and JSON contracts;
no scientific process, native calculation or output regeneration is invoked.
"""

import base64
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import experiment
from p02_preprocess.application import resolve_r_period
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

    def test_failed_r_request_identifies_large_integer_without_concealing_error(self):
        """A malformed nested field remains rejected and is locatable without its decimal value."""
        from util.providers import run_r

        value = 1 << 16000
        payload = {"series": [{"context": [1.25, value]}]}
        with self.assertRaisesRegex(ValueError, "integer string conversion") as failure:
            run_r("unused.R", payload, timeout=1, threads=1)
        notes = "\n".join(failure.exception.__notes__)
        self.assertIn("$['series'][0]['context'][1]", notes)
        self.assertIn("type=int", notes)
        self.assertIn("size_bits=16001", notes)
        self.assertIs(payload["series"][0]["context"][1], value)

    def test_period_transport_uses_existing_process_owned_cli(self):
        """The pinned-environment request preserves frequency and optional override."""
        configuration = json.loads((CODE_ROOT / "config/experiment.json").read_text())
        for override in (None, 7):
            with self.subTest(override=override):
                configuration["pipeline"]["r_period_override"] = override
                response = {"frequency": "W", "r_period": override or 52}
                with patch("p02_preprocess.application.subprocess.run", return_value=subprocess.CompletedProcess(
                        [], 0, json.dumps(response), "")) as invoke:
                    self.assertEqual(resolve_r_period(configuration, "W"), response)
                command = invoke.call_args.args[0]
                self.assertEqual(command[0], str(CODE_ROOT / "environments/gift-eval/.venv/bin/python"))
                script = CODE_ROOT / "src/python/p02_preprocess/period.py"
                self.assertTrue(script.is_file())
                self.assertEqual(command[1:], [str(script), "--frequency", "W"] +
                                 ([] if override is None else ["--override", "7"]))

    def test_model_publication_uses_semantic_paths_without_changing_bytes(self):
        """Already-produced bytes publish by model/frequency/horizon, not run ID."""
        from p04_forecast.application import Application

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            accepted = {}
            context = SimpleNamespace(request=SimpleNamespace(root=root), configuration={},
                repository=SimpleNamespace(accept=lambda subflow, rows: accepted.update(rows)))
            content = b"opaque provider bytes; no model is fitted or deserialized"
            cases = [("directional_dtw", None, "model.pickle"),
                     ("directional_mantis_rf", 1, "horizon_01.pickle"),
                     ("directional_mantis_rf", 14, "horizon_14.pickle")]
            for model, horizon, filename in cases:
                key = f"D/{model}/{horizon or 'all'}"
                Application(context).accept_decision_models([(key, {"model": model, "horizon": horizon,
                    "artifact": base64.b64encode(content).decode("ascii")})])
                relative = f"models/{model}/daily/{filename}"
                self.assertEqual(accepted[key]["path"], relative)
                self.assertEqual((root / relative).read_bytes(), content)
                self.assertEqual(accepted[key]["size"], len(content))
                self.assertEqual(accepted[key]["sha256"], hashlib.sha256(content).hexdigest())
            self.assertEqual(len(list((root / "models").rglob("*.pickle"))), 3)
            self.assertEqual(list(root.rglob("*.partial")), [])

    def test_researcher_import_sets_canonical_prefect_home_before_settings_load(self):
        """A fresh entry-point import resolves Prefect settings inside hidden runtime."""
        response = subprocess.run([sys.executable, "-B", "-c",
            "import experiment; from prefect.settings import get_current_settings; "
            "print(get_current_settings().home)"], cwd=CODE_ROOT, capture_output=True,
            text=True, check=True, env={**os.environ, "PREFECT_HOME": "/unused-prefect-home",
                                      "PYTHONPATH": str(CODE_ROOT / "src/python")})
        self.assertEqual(Path(response.stdout.strip()), CODE_ROOT / ".runtime/prefect")


if __name__ == "__main__":
    unittest.main()
