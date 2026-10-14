# ==============================================================================
# test_progress.py
# Purpose: Exercise read-only JSON/human status on bounded legacy-state fixtures.
# Inputs: Disposable DuckDB files; no scientific execution or remote services.
# Outputs: Assertions for recorded progress, compatibility and byte preservation.
# Run from: PYTHONPATH=src/python .venv/bin/python -m unittest tests.test_progress
# ==============================================================================

"""Public status contracts; fixtures stand in for later scientific process states."""

import hashlib
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import duckdb

from tests.test_main import MAIN, ROOT
from util.shared_database import initialize_experiment_database


class ProgressTests(unittest.TestCase):
    """Keep fixture setup separate from read-only public command assertions."""

    def setUp(self):
        """Create only schema/configuration in a disposable database, not a run."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.database = Path(temporary.name).resolve() / "results/experiment.duckdb"
        initialize_experiment_database(
            self.database, ROOT / "config/experiment.json"
        )
        with duckdb.connect(str(self.database)) as connection:
            connection.execute("UPDATE experiment_processes SET updated_at='2026-01-01'")

    def status(self, format=None, identity=None):
        """Invoke the actual entry point and capture terminal presentation."""
        arguments = ["status", "--database", str(self.database)]
        if format:
            arguments += ["--format", format]
        if identity:
            arguments += ["--experiment-id", identity]
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(MAIN.main(arguments), 0)
        return output.getvalue()

    def populated(self):
        """Store asymmetric counts, later activity, historical errors and decoy identities."""
        with duckdb.connect(str(self.database)) as connection:
            connection.execute("""
                INSERT INTO runs (run_id,stage,dataset_id,configuration,configuration_hash,
                    environment,machine,started_at,ended_at,status)
                VALUES ('import','import','daily','{}','hash','{}','{}',
                    '2026-01-02','2026-01-03','completed');
                INSERT INTO tasks (task_id,run_id,stage,dataset_id,series_id,status,updated_at)
                VALUES ('i1','import','import','daily','s1','completed','2026-01-03'),
                       ('i2','import','import','daily','s2','completed','2026-01-03'),
                       ('decoy','other','import','other','s3','failed','2026-02-01');
                INSERT INTO series (dataset_id,series_id,source_series_id,source_row,frequency,
                    start_timestamp,target,observation_count,content_hash)
                VALUES ('daily','s1','s1',0,'D','2026-01-01',[1,2],2,'hash'),
                       ('daily','s2','s2',1,'D','2026-01-01',[4,7],2,'hash');
                INSERT INTO experiments (experiment_id,benchmark_configuration_id,dataset_id,
                    name,scientific_configuration,configuration_hash,scope,status,updated_at)
                VALUES ('selected','benchmark','daily','selected','{}','hash','fixture','running','2026-01-04'),
                       ('other','benchmark','daily','other','{}','hash','fixture','failed','2026-01-03');
                INSERT INTO experiment_tasks (task_id,experiment_id,stage,status,updated_at,last_error)
                VALUES ('t1','selected',4,'completed','2026-01-04',NULL),
                       ('t2','selected',4,'failed','2026-01-05','provider failed'),
                       ('t3','selected',4,'failed','2026-01-05','older failure'),
                       ('wrong','other',4,'failed','2026-02-01','wrong experiment');
                INSERT INTO experiment_task_attempts
                    (attempt_id,task_id,invocation_id,attempt_number,started_at,ended_at,status,error)
                VALUES ('attempt','t2','invocation',1,'2026-01-05','2026-01-06','failed','native worker exited');
                INSERT INTO execution_events
                    (execution_id,requested_processes,operational_configuration,repository_revision,
                     machine,started_at,completed_at,status)
                VALUES ('exec','[1]','{}','fixture-revision','{}','2026-01-02','2026-01-07','completed');
                INSERT INTO paper_metric_candidates VALUES
                    ('a','selected','Daily','smyl',0.1,0.2,'input','output','{}'),
                    ('b','selected','Daily','smyl',0.3,0.4,'input','output','{}'),
                    ('c','selected','Daily','base',NULL,NULL,'input','output','{}'),
                    ('d','other','Daily','smyl',0.5,0.6,'input','output','{}');
                INSERT INTO paper_table_reports (experiment_id,report,input_fingerprint,result_fingerprint)
                VALUES ('selected','{"fixture":true}','input','deliberately-unverified');
            """)

    def test_pending_running_completed_failed_and_unrecorded(self):
        """Stored states, including stale running, never imply substeps or a live ETA."""
        for state in ("pending", "running", "completed", "failed"):
            with self.subTest(state=state):
                with duckdb.connect(str(self.database)) as connection:
                    connection.execute("UPDATE experiment_processes SET status=?", [state])
                text = self.status("human")
                for number, name in enumerate(("Import", "Preprocess", "Transform/window preparation", "Forecast", "Combine", "Evaluate"), 1):
                    self.assertIn(f"Process {number:02d} {name}: {state}", text)
                self.assertEqual(text.count("substeps: not recorded"), 5)
                self.assertIn("ETA: not recorded", text)
                self.assertIn("does not prove live work", text)
                self.assertIn(f"processes): {state}", text)
                self.assertIn("Active process (recorded): " +
                              ("01, 02, 03, 04, 05, 06" if state == "running" else "none recorded"), text)
        with duckdb.connect(str(self.database)) as connection:
            connection.execute("DELETE FROM experiment_processes WHERE process_id=3")
            connection.execute("DROP TABLE paper_metric_candidates")
        text = self.status("human")
        self.assertIn("Process 03 Transform/window preparation: not recorded", text)
        self.assertIn("Process 06 candidates: not recorded (table absent)", text)

    def test_counts_activity_error_context_and_unvalidated_report(self):
        """Report selected counts and the newest error independently of latest activity."""
        self.populated()
        text = self.status("human", "selected")
        self.assertIn("Imported series: 2", text)
        self.assertIn("Items: 2 / 100", text)
        self.assertIn("tasks: completed=1, failed=2;", text)
        self.assertIn("Source revision (latest execution): fixture-revision", text)
        self.assertIn("execution exec processes=[1] | completed", text)
        self.assertIn("attempt attempt task=t2 invocation=invocation | failed | native worker exited", text)
        self.assertNotIn("wrong experiment", text)
        self.assertIn("Process 06 candidates: 3", text)
        self.assertIn("Daily / smyl: 2 stored; 2 sensitivity candidates", text)
        self.assertIn("Daily / base: 1 stored; 0 sensitivity candidates", text)
        status = json.loads(self.status("json", "selected"))["status"]
        self.assertEqual(status["experiment"]["paper_tables"], {
            "diagnostic": "m4_paper_tables_v1", "scope": "Daily-only Table2; no All",
            "ex_post": True, "deployable": False, "sensitivity_candidates": 2,
            "report": {"fixture": True},
        })
        with duckdb.connect(str(self.database)) as connection:
            connection.execute("""INSERT INTO paper_execution_batches VALUES
                ('batch','selected','["a","b"]','{}','2026-01-08')""")
        self.assertIn("Process 6 batch batch | committed", self.status("human", "selected"))

    def test_default_json_compatibility(self):
        """Default and explicit JSON retain the existing envelope and status schema."""
        default = json.loads(self.status())
        explicit = json.loads(self.status("json"))
        self.assertEqual(default["status"], explicit["status"])
        self.assertEqual(set(default), {"invocation", "status"})
        self.assertNotIn("format", default["invocation"]["arguments"])
        self.assertEqual(set(default["status"]), {
            "configuration_version", "name", "date", "description", "seed",
            "scientific_hash", "configuration_integrity_hash", "resolved_configuration", "processes",
            "database_roles", "import_progress",
        })
        self.assertEqual(default["status"]["configuration_version"], 12)
        self.assertEqual([row["process_id"] for row in default["status"]["processes"]], list(range(1, 7)))
        self.assertEqual(default["status"]["processes"][0], {
            "process_id": 1, "name": "import", "status": "pending", "started_at": None,
            "completed_at": None, "summary": None, "error": None,
        })

    def test_repeated_status_is_read_only_and_never_runs_science(self):
        """Enforce read-only opens and identical database bytes across both formats."""
        self.populated()
        before = hashlib.sha256(self.database.read_bytes()).hexdigest()
        files = sorted(path.name for path in self.database.parent.iterdir())
        connect = duckdb.connect

        def read_only_connect(*args, **kwargs):
            """Fail before a status connection can acquire write access."""
            self.assertTrue(kwargs.get("read_only"))
            return connect(*args, **kwargs)

        with (patch("duckdb.connect", side_effect=read_only_connect),
              patch("util.p00_01_researcher_actions.experiment_flow", side_effect=AssertionError("scheduled")),
              patch("util.shared_process_storage.ProcessStorage.validate", side_effect=AssertionError("validated")),
              patch("util.p06_02_table_storage.TableStorage.summary", side_effect=AssertionError("validated"))):
            for _ in range(2):
                self.status("json")
                self.status("human")
        self.assertEqual(hashlib.sha256(self.database.read_bytes()).hexdigest(), before)
        self.assertEqual(sorted(path.name for path in self.database.parent.iterdir()), files)

    def test_missing_database_is_not_created(self):
        """Read-only status cannot create an absent experiment database."""
        self.database.unlink()
        with self.assertRaises(duckdb.IOException):
            self.status("human")
        self.assertFalse(self.database.exists())
