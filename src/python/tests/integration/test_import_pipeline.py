# ==============================================================================
# test_import_pipeline.py
#
# Purpose: Verify Stage 1 imports the pinned local M4 Daily sample into the expected schema, identities, windows, and counts.
# Inputs: unittest fixtures, temporary databases/files, deterministic synthetic records, and mocked process or cluster boundaries.
# Outputs: unittest pass/fail assertions and captured diagnostics; no production artifacts or external services.
# Run from: PYTHONPATH=src/python .tools/uv/uv run --locked --no-sync python -m unittest tests.integration.test_import_pipeline
# ==============================================================================

"""Verify Stage 1 imports the pinned local M4 Daily sample into the expected schema, identities, windows, and counts."""

import tempfile
import unittest
from pathlib import Path

import duckdb

from util.configuration import load_config
from util.database import ShapeFMDatabase
from util.gift_eval_source import iter_source_series
from util.import_execution import ImportCoordinator


class Stage1ImportTests(unittest.TestCase):
    """Verify deterministic Stage 1 import, restart, parallelism, and source fidelity."""

    @classmethod
    def setUpClass(cls):
        """Load the pinned ten-series source configuration and source revision."""
        cls.root = Path(__file__).resolve().parents[4]
        cls.source = cls.root / "data/source/gift_eval/m4_daily"
        cls.config = load_config(cls.root / "config/imports/m4_daily.json", 10)
        cls.revision = "30841734ac5cfddbd0c3bad6d09d2b6b32becbb0"

    @staticmethod
    def snapshot(path: Path):
        """Read ordered series/window rows and import counts from a completed database."""
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
            with ImportCoordinator(sequential) as coordinator:
                first = coordinator.import_m4_daily(
                    self.source, self.config, self.revision, workers=1
                )
                second = coordinator.import_m4_daily(
                    self.source, self.config, self.revision, workers=1
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

            with ImportCoordinator(parallel) as coordinator:
                parallel_result = coordinator.import_m4_daily(
                    self.source, self.config, self.revision, workers=2
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


if __name__ == "__main__":
    unittest.main()
