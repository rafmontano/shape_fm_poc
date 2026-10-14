"""Process 02 regressions through the public experiment flow.

Asymmetric historical/future fixtures protect cleaning scope, native scientific
formulas, canonical preservation and database-only no-work restart.
"""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from experiment import run_experiment, status
from tests.test_mvp_import import configuration, write_source
from util.runtime import CODE_ROOT, ExperimentRequest


class MVPPreprocessTests(unittest.TestCase):
    """Prepared histories exclude test actuals and match the configured R function."""

    def test_native_cleaning_scope_and_database_only_restart(self):
        """Both modes preserve historical length; different test futures cannot leak."""
        for method in ("standard", "robust"):
            with self.subTest(method=method), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                history = [float(2 * index + index % 7) for index in range(90)]
                history[18] = None
                history[42] = 99999.0
                targets = [history + [float(100000 + index) for index in range(14)],
                           history + [float(-200000 - index) for index in range(14)]]
                write_source(root, targets)
                configuration(root, 2)
                path = root / "config/experiment.json"
                document = json.loads(path.read_text())
                document["pipeline"]["preprocessing"]["default"] = method
                path.write_text(json.dumps(document))
                profile = "local_dask" if method == "robust" else "sequential_safe"
                request = ExperimentRequest((1, 2), root, execution_profile=profile)
                run_experiment(request)
                with duckdb.connect(str(root / "results/experiment.duckdb"), read_only=True) as connection:
                    before = connection.execute("SELECT * FROM atomic_results ORDER BY subflow_id,atomic_key").fetchall()
                    rows = [json.loads(value) for value, in connection.execute(
                        "SELECT result FROM atomic_results WHERE subflow_id='02.01' ORDER BY atomic_key").fetchall()]
                    canonical = [json.loads(value)["target"] for value, in connection.execute(
                        "SELECT result FROM atomic_results WHERE subflow_id='01.02' ORDER BY atomic_key").fetchall()]
                self.assertEqual(canonical, targets)
                self.assertEqual([row["history_end"] for row in rows], [90, 90])
                self.assertEqual(rows[0]["values"], rows[1]["values"])
                self.assertEqual(rows[0]["parameters"]["r_period"], 1)
                reference = subprocess.run(["Rscript", "-e", '''
                    suppressPackageStartupMessages(library(forecast))
                    suppressPackageStartupMessages(library(jsonlite))
                    request <- fromJSON(file("stdin"), simplifyVector=FALSE)
                    values <- vapply(request$history, function(value) if (is.null(value)) NA_real_ else value, numeric(1))
                    series <- ts(values, frequency=1)
                    result <- if (request$method == "standard") forecast::na.interp(series) else forecast::tsclean(series)
                    cat(toJSON(as.numeric(result), digits=17))
                '''], input=json.dumps({"history": history, "method": method}), cwd=CODE_ROOT,
                    check=True, capture_output=True, text=True, timeout=60,
                    env={**os.environ, "RENV_CONFIG_SYNCHRONIZED_CHECK": "false"})
                self.assertEqual(rows[0]["values"], json.loads(reference.stdout))
                path.unlink()
                with patch("p02_preprocess.application.run_r", side_effect=AssertionError("accepted cleaning repeated")), \
                     patch("p02_preprocess.application.resolve_r_period", side_effect=AssertionError("period resolved on restart")):
                    run_experiment(request)
                with duckdb.connect(str(root / "results/experiment.duckdb"), read_only=True) as connection:
                    self.assertEqual(before, connection.execute(
                        "SELECT * FROM atomic_results ORDER BY subflow_id,atomic_key").fetchall())
                self.assertEqual(status(root)["accepted_milestones"], ["01", "01.01", "01.02", "01.03", "02", "02.01", "02.02"])
                self.assertFalse((root / "results/windows.duckdb").exists())
