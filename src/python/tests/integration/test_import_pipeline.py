# ==============================================================================
# test_import_pipeline.py
#
# Purpose: Verify Process 01 imports the pinned local M4 Daily sample into the expected schema, identities, windows, and counts.
# Inputs: unittest fixtures, temporary databases/files, deterministic synthetic records, and mocked process or cluster boundaries.
# Outputs: unittest pass/fail assertions and captured diagnostics; no production artifacts or external services.
# Run from: PYTHONPATH=src/python .tools/uv/uv run --locked --no-sync python -m unittest tests.integration.test_import_pipeline
# ==============================================================================

"""Verify Process 01 imports the pinned local M4 Daily sample into the expected schema, identities, windows, and counts."""

import math
import tempfile
import unittest
from pathlib import Path

import duckdb

from util.shared_configuration import load_experiment_configuration
from util.shared_database import ShapeFMDatabase, initialize_experiment_database
from util.p01_03_gift_eval_source import iter_source_series
from util.p01_02_import_execution import ImportCoordinator


class Process01ImportTests(unittest.TestCase):
    """Purpose: Verify deterministic Process 01 import, restart, parallelism, and source fidelity.

    Inputs: Pinned M4 Daily source files and the committed import configuration.
    Outputs: Import/schema assertions; tests create temporary DuckDB files and worker processes.
    """

    @classmethod
    def setUpClass(cls):
        """Purpose: Load shared source paths and bounded import settings for this class.

        Inputs: Repository paths and the committed 100-series experiment configuration.
        Outputs: Class attributes for source, config, and revision; reads configuration only.
        """
        cls.root = Path(__file__).resolve().parents[4]
        cls.source = cls.root / "data/source/gift_eval/m4_daily"
        configuration = load_experiment_configuration(
            cls.root / "config/experiments/poc2_m4_daily_100.json"
        )
        cls.config = configuration.import_settings
        cls.config["max_series"] = 10
        cls.revision = configuration.resolved["data"]["source"]["revision"]

    @staticmethod
    def snapshot(path: Path):
        """Purpose: Capture deterministic import state from a completed database.

        Inputs: Path to an existing DuckDB import database.
        Outputs: Tuple of ordered row records and count values; opens then closes read-only DuckDB.
        """
        connection = duckdb.connect(str(path), read_only=True)
        rows = connection.execute(
            """
            SELECT s.series_id, s.target, s.content_hash,
                   w.train_start, w.train_end, w.validation_start,
                   w.validation_end, w.test_start, w.test_end
            FROM series s JOIN evaluation_windows w USING (dataset_id, series_id)
            ORDER BY s.series_id
            """
        ).fetchall()
        counts = connection.execute(
            "SELECT (SELECT count(*) FROM series), "
            "(SELECT count(*) FROM evaluation_windows), "
            "(SELECT count(*) FROM tasks WHERE status = 'completed')"
        ).fetchone()
        connection.close()
        return rows, counts

    def test_sequential_restart_parallel_and_source_equality(self):
        """Sequential and parallel imports agree, restarts skip work, and targets remain exact."""
        with tempfile.TemporaryDirectory() as directory:
            sequential = Path(directory) / "sequential.duckdb"
            parallel = Path(directory) / "parallel.duckdb"
            initialize_experiment_database(
                sequential,
                self.root / "config/experiments/poc2_m4_daily_100.json",
            )
            with ImportCoordinator(sequential) as coordinator:
                first = coordinator.import_m4_daily(
                    self.source, self.config, self.revision, workers=1, batch_size=10
                )
                second = coordinator.import_m4_daily(
                    self.source, self.config, self.revision, workers=1, batch_size=10
                )
            self.assertEqual(first["series_count"], 10)
            self.assertEqual(first["observation_count"], 7291)
            self.assertEqual(first["completed_this_invocation"], 10)
            self.assertEqual(second["skipped_completed"], 10)
            self.assertEqual(second["submitted_tasks"], 0)
            connection = duckdb.connect(str(sequential), read_only=True)
            invocation_count = connection.execute(
                "SELECT count(*) FROM run_invocations"
            ).fetchone()[0]
            connection.close()
            self.assertEqual(invocation_count, 2)

            initialize_experiment_database(
                parallel,
                self.root / "config/experiments/poc2_m4_daily_100.json",
            )
            with ImportCoordinator(parallel) as coordinator:
                parallel_result = coordinator.import_m4_daily(
                    self.source, self.config, self.revision, workers=2, batch_size=10
                )
            self.assertEqual(parallel_result["completed_this_invocation"], 10)
            self.assertEqual(self.snapshot(sequential), self.snapshot(parallel))
            self.assertEqual(self.snapshot(sequential)[1], (10, 10, 10))

            with ShapeFMDatabase.open(sequential) as database:
                first_series = database.get_series("m4_daily", "0")
            self.assertEqual(first_series.observation_count, 1020)
            window = first_series.evaluation_windows[0]
            self.assertEqual(
                (
                    window.train_start,
                    window.train_end,
                    window.validation_start,
                    window.validation_end,
                    window.test_start,
                    window.test_end,
                ),
                (0, 992, 992, 1006, 1006, 1020),
            )

            source_rows = list(iter_source_series(self.source, "D", 10))
            with ShapeFMDatabase.open(sequential) as database:
                for source in source_rows:
                    canonical = database.get_series("m4_daily", source.source_series_id)
                    self.assertEqual(canonical.target, source.target)

    def test_complete_source_has_4227_unchanged_finite_series(self):
        """The pinned M4 Daily source remains complete and unchanged at Gate 1."""
        rows = list(iter_source_series(self.source, "D", None))
        self.assertEqual(len(rows), 4_227)
        self.assertEqual(rows[0].source_series_id, "0")
        self.assertEqual(rows[-1].source_series_id, "4226")
        self.assertTrue(
            all(
                value is not None and math.isfinite(value)
                for row in rows
                for value in row.target
            )
        )


if __name__ == "__main__":
    unittest.main()
