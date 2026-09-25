"""Stage 1 integration checks using the pinned project-local M4 Daily source."""

import tempfile
import unittest
from pathlib import Path

import duckdb

from shapefm.config import load_config
from shapefm.database import ShapeFMDatabase
from shapefm.gift_eval import iter_source_series
from shapefm.orchestration import ImportCoordinator


class Stage1ImportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = Path(__file__).resolve().parents[2]
        cls.source = cls.root / "data/source/gift_eval/m4_daily"
        cls.config = load_config(cls.root / "config/imports/m4_daily.json", 10)
        cls.revision = "30841734ac5cfddbd0c3bad6d09d2b6b32becbb0"

    @staticmethod
    def snapshot(path: Path):
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
