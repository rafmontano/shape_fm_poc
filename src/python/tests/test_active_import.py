# ==============================================================================
# test_active_import.py
# Purpose: Exercise fixed paths, durable import boundaries and database-only restart.
# Inputs: Isolated temporary deterministic GIFT-Eval-shaped three-series Arrow fixtures.
# Outputs: Public-entry/status assertions; no later processes or remote workers.
# Run from: PYTHONPATH=src/python .venv/bin/python -m unittest tests.test_active_import
# ==============================================================================

"""Bounded Process 01 integration, plus child metadata fixtures (not Process 03)."""

import hashlib
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path
from unittest.mock import patch

import duckdb

from tests.test_main import MAIN, ROOT
from tests.test_import import write_source
from util.shared_active_paths import ActivePaths
from util.shared_database import initialize_experiment_database
from util.p00_02_researcher_cli import ResearcherCLI
from util.p01_02_import_execution import compute_series
from util.p01_03_gift_eval_source import source_fingerprint
from util.shared_process_storage import ProcessStorage


class ActiveImportTests(unittest.TestCase):
    """Real local Process 01 workflow with injected faults at provider boundaries."""

    def setUp(self):
        """Make a three-item asymmetric canonical input fixture."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.paths = ActivePaths(Path(temporary.name))
        source = self.paths.root / "source/m4_daily"
        write_source(source, [[float(i * i + offset) for i in range(30 + offset)] for offset in (0, 2, 5)])
        configuration = json.loads((ROOT / "config/experiments/poc2_m4_daily_100_paper_tables.json").read_text())
        configuration["data"]["source"]["directory"] = str(source.parent)
        configuration["data"]["source"]["files"] = {
            name: value["sha256"] for name, value in source_fingerprint(source)["files"].items()
        }
        configuration["data"]["selection"]["count"] = 3
        configuration["execution"]["default"]["dask_retries"] = 0
        self.paths.configuration.parent.mkdir()
        self.paths.configuration.write_text(json.dumps(configuration))

    def command(self, action, *arguments, expected=0):
        """Run the normal entry point with only the permitted temporary-path override."""
        output, error = io.StringIO(), io.StringIO()
        with redirect_stdout(output), redirect_stderr(error):
            code = MAIN.main([action, "--database", str(self.paths.parent), *arguments])
        self.assertEqual(code, expected, error.getvalue())
        return output.getvalue() if code == 0 else error.getvalue()

    def rows(self, sql):
        """Read fixture facts independently of status interpretation."""
        with duckdb.connect(str(self.paths.parent), read_only=True) as connection:
            return connection.execute(sql).fetchall()

    def test_fixed_paths_and_test_only_overrides(self):
        """Normal defaults are fixed; experiment suffixes and non-temporary overrides fail."""
        cli = ResearcherCLI()
        self.assertEqual(cli.request(["run", "--processes", "1"]).database, ROOT / "results/experiment.duckdb")
        self.assertEqual(cli.request(["status"]).database, ROOT / "results/experiment.duckdb")
        self.assertEqual(ActivePaths().child, ROOT / "results/windows.duckdb")
        self.assertEqual(ActivePaths().configuration, ROOT / "config/experiment.json")
        self.assertEqual(ActivePaths.from_parent(self.paths.parent).child, self.paths.child)
        for path in (ROOT / "results/poc2_20261014.duckdb", Path.home() / "results/experiment.duckdb"):
            with self.assertRaises(ValueError):
                cli.request(["run", "--database", str(path)])
        with self.assertRaises(ValueError):
            cli.request(["prepare-windows", "--database", str(self.paths.parent),
                         "--windows-database", str(self.paths.parent)])

    def test_bootstrap_resume_without_json_and_no_overwrite(self):
        """Real bootstrap stores both documents; restart does no provider/item work."""
        first = json.loads(self.command("run", "--processes", "1"))
        self.assertEqual(first["execution"]["processes"][0]["status"], "completed")
        self.assertEqual(self.rows("SELECT count(*) FROM series"), [(3,)])
        self.assertEqual(self.rows("SELECT count(*) FROM reference_forecasts"), [(0,)])
        self.assertFalse(self.paths.child.exists())
        steps = self.rows("SELECT substep_id,status,planned_items,completed_items,failed_items,attempt_count FROM process_substeps ORDER BY substep_id")
        self.assertEqual(steps, [("01.01", "completed", 3, 3, 0, 1), ("01.02", "completed", 3, 3, 0, 1)])
        stored = self.rows("SELECT original_configuration,resolved_configuration FROM experiment_configuration")[0]
        self.assertEqual(json.loads(stored[0]), json.loads(self.paths.configuration.read_text()))
        self.assertEqual(json.loads(stored[1])["data"]["selection"]["count"], 3)
        before = self.paths.parent.read_bytes()
        self.assertIn("only valid when creating", self.command(
            "run", "--processes", "1", "--configuration", str(self.paths.configuration), expected=1))
        self.assertEqual(self.paths.parent.read_bytes(), before)
        attempts = self.rows("SELECT * FROM task_attempts ORDER BY attempt_id")
        self.paths.configuration.unlink()
        with (patch("util.p01_03_gift_eval_source.ConfiguredGiftEvalSource.records", side_effect=AssertionError("source reread")),
              patch("util.p01_01_import_flow.compute_import_series", side_effect=AssertionError("recomputed"))):
            restart = json.loads(self.command("run", "--processes", "1"))
        self.assertEqual(restart["execution"]["processes"][0]["status"], "skipped_completed")
        self.assertEqual(self.rows("SELECT * FROM task_attempts ORDER BY attempt_id"), attempts)
        self.assertEqual(self.rows("SELECT substep_id,status,planned_items,completed_items,failed_items,attempt_count FROM process_substeps ORDER BY substep_id"), steps)
        status = json.loads(self.command("status"))["status"]
        self.assertEqual([row["status"] for row in status["processes"]], ["completed"] + ["pending"] * 5)
        self.assertEqual(sorted(path.name for path in self.paths.parent.parent.iterdir()), ["experiment.duckdb"])
        with duckdb.connect(str(self.paths.parent)) as connection:
            connection.execute("UPDATE series SET target=[99.0] WHERE series_id='0'")
        with self.assertRaisesRegex(RuntimeError, "01.01.*mismatch"):
            ProcessStorage(self.paths.parent).validate(1)

    def test_item_failure_resumes_only_missing_items(self):
        """One injected item failure reports its location and preserves the earlier item."""
        def fail_second(job):
            """Keep the first real calculation; fail the asymmetric second source item."""
            if job.series_id == "1":
                raise RuntimeError("injected item failure")
            return compute_series(job)

        with patch("util.p01_01_import_flow.compute_import_series", side_effect=fail_second):
            self.command("run", "--processes", "1", expected=1)
        self.assertEqual(self.rows("SELECT status,completed_items,failed_items,failure_item FROM process_substeps WHERE substep_id='01.01'"), [("failed", 1, 1, "1")])
        self.assertIn("Process 01 substep 01.01 item 1", self.command("status", "--format", "human"))
        self.command("run", "--processes", "1")
        self.assertEqual(self.rows("SELECT series_id,attempt_count,status FROM tasks ORDER BY series_id"),
                         [("0", 1, "completed"), ("1", 2, "completed"), ("2", 1, "completed")])

    def test_acceptance_failure_preserves_canonical_boundary(self):
        """Acceptance retry skips completed canonical computation and records its own attempt."""
        with patch("util.p01_02_import_execution.ImportCoordinator.finish_configured_import",
                   side_effect=RuntimeError("injected acceptance failure")):
            self.command("run", "--processes", "1", expected=1)
        self.assertEqual(self.rows("SELECT substep_id,status FROM process_substeps ORDER BY substep_id"),
                         [("01.01", "completed"), ("01.02", "failed")])
        self.assertIn("Process 01 substep 01.02", self.command("status", "--format", "human"))
        with patch("util.p01_01_import_flow.compute_import_series", side_effect=AssertionError("canonical repeated")):
            self.command("run", "--processes", "1")
        self.assertEqual(self.rows("SELECT attempt_count FROM process_substeps ORDER BY substep_id"), [(1,), (2,)])

    def test_database_roles_and_status_preserve_both_files(self):
        """Pending child, missing-required child, and incompatible child are distinct."""
        initialize_experiment_database(self.paths.parent, self.paths.configuration)
        human = self.command("status", "--format", "human")
        self.assertIn("Main database: results/experiment.duckdb — available", human)
        self.assertIn("Windows database: results/windows.duckdb — not created (Process 03 pending)", human)
        with duckdb.connect(str(self.paths.parent)) as connection:
            connection.execute("UPDATE experiment_processes SET status='completed' WHERE process_id=3")
        self.assertIn("recovery error: required child is missing", self.command("status", "--format", "human"))
        with duckdb.connect(str(self.paths.child)) as child:
            child.execute("CREATE TABLE preparation_metadata (preparation_id VARCHAR,definition_hash VARCHAR,parent_scientific_hash VARCHAR,parent_configuration_hash VARCHAR)")
            child.execute("INSERT INTO preparation_metadata VALUES ('wrong','wrong','wrong','wrong')")
        before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in (self.paths.parent, self.paths.child)}
        for _ in range(2):
            self.assertIn("recovery error: incompatible child linkage", self.command("status", "--format", "human"))
            self.command("status", "--format", "json")
        self.assertEqual({path: hashlib.sha256(path.read_bytes()).hexdigest() for path in before}, before)

    def test_orphan_child_blocks_parent_bootstrap(self):
        """Never silently attach or overwrite a previous experiment's child."""
        self.paths.child.parent.mkdir()
        self.paths.child.write_bytes(b"preserved orphan")
        self.assertIn("orphan windows", self.command("run", "--processes", "1", expected=1))
        self.assertFalse(self.paths.parent.exists())
        self.assertEqual(self.paths.child.read_bytes(), b"preserved orphan")
