# ==============================================================================
# test_seasonal_period_tuning.py
#
# Purpose: Verify v3 tuning configuration, fold isolation, masking, selection, and schema.
# Inputs: Approved configuration, synthetic scores/labels, and temporary DuckDB files.
# Outputs: unittest assertions only; no production experiment is modified.
# Run from: PYTHONPATH=src/python .tools/uv/uv run --locked --no-sync python -m unittest tests.test_seasonal_period_tuning
# ==============================================================================

import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb

from util.configuration import load_experiment_configuration
from util.database import SCHEMA_VERSION, initialize_experiment_database
from util.seasonal_period_tuning import (
    _forecast_job,
    historical_folds,
    original_scale_mae,
    select_period_policy,
)


ROOT = Path(__file__).resolve().parents[3]
CONFIGURATION = ROOT / "config/experiments/poc2_m4_daily_100_period_tuning.json"


class SeasonalPeriodTuningTests(unittest.TestCase):
    """Exercise scientific policy helpers and versioned persistence boundaries."""

    def test_v3_configuration_is_opt_in_and_keeps_periods_independent(self) -> None:
        """The approved document selects AutoARIMA/ETS without changing scoring seasonality."""
        configuration = load_experiment_configuration(CONFIGURATION)
        self.assertEqual(configuration.version, 3)
        self.assertEqual(set(configuration.workflow["models"]), {"auto_arima", "ets"})
        self.assertIsNone(configuration.r_period_override)
        self.assertEqual(configuration.evaluation_seasonality(1), 1)
        self.assertEqual(configuration.seasonal_period_tuning["validation_windows"], 3)

    def test_three_expanding_folds_end_before_official_test(self) -> None:
        """Fold validation spans are exact, disjoint, h-spaced and end at history length."""
        folds = historical_folds(length=100, horizon=10)
        self.assertEqual(
            [(fold.train_end, fold.validation_start, fold.validation_end) for fold in folds],
            [(70, 70, 80), (80, 80, 90), (90, 90, 100)],
        )
        self.assertTrue(all(fold.train_end == fold.validation_start for fold in folds))
        self.assertEqual(historical_folds(length=30, horizon=10), ())

    def test_missing_validation_labels_are_masked_not_zeroed(self) -> None:
        """MAE excludes missing labels and preserves their count as explicit evidence."""
        mae, count = original_scale_mae([2.0, 100.0, 8.0], [1.0, None, 11.0])
        self.assertEqual(count, 2)
        self.assertEqual(mae, 2.0)
        self.assertEqual(original_scale_mae([1.0], [None]), (None, 0))

    def test_selection_prefers_estimated_but_retains_baseline_for_ties_or_gaps(self) -> None:
        """Only a complete strictly lower three-fold mean permits estimated policy."""
        selected = select_period_policy(
            [3.0, 3.0, 3.0], [2.0, 2.0, 2.0], [True] * 3, [True] * 3
        )
        self.assertEqual(selected["selected_policy"], "estimated")
        tied = select_period_policy(
            [2.0, 2.0, 2.0], [2.0, 2.0, 2.0], [True] * 3, [True] * 3
        )
        self.assertEqual(tied["selected_policy"], "baseline")
        incomplete = select_period_policy(
            [2.0, 2.0, 2.0], [1.0, None, 1.0], [True] * 3, [True, False, True]
        )
        self.assertEqual(incomplete["status"], "inconclusive")

    def test_selection_is_independent_of_parallel_completion_order(self) -> None:
        """Controlled inputs produce identical policy decisions in sequential and threaded calls."""
        arguments = ([4.0, 3.0, 2.0], [3.0, 2.0, 1.0], [True] * 3, [True] * 3)
        sequential = [select_period_policy(*arguments) for _ in range(8)]
        with ThreadPoolExecutor(max_workers=4) as executor:
            parallel = list(executor.map(lambda _: select_period_policy(*arguments), range(8)))
        self.assertEqual(parallel, sequential)

    def test_models_select_independently_and_worker_payload_excludes_actuals(self) -> None:
        """Model policies may differ while the R request contains training input only."""
        auto = select_period_policy(
            [4.0, 4.0, 4.0], [2.0, 2.0, 2.0], [True] * 3, [True] * 3
        )
        ets = select_period_policy(
            [2.0, 2.0, 2.0], [4.0, 4.0, 4.0], [True] * 3, [True] * 3
        )
        self.assertEqual(auto["selected_policy"], "estimated")
        self.assertEqual(ets["selected_policy"], "baseline")
        job = _forecast_job("id", "dataset", "series", "ets", [1.0, 2.0], 2, 1, {})
        self.assertFalse(
            {"actual", "actuals", "future", "future_actuals", "labels"}.intersection(job)
        )

    def test_schema_migration_adds_inspectable_tuning_tables(self) -> None:
        """A new v3 database exposes schema v8 and all four dedicated evidence tables."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "tuning.duckdb"
            initialize_experiment_database(database, CONFIGURATION)
            connection = duckdb.connect(str(database), read_only=True)
            try:
                version = connection.execute("SELECT max(version) FROM schema_versions").fetchone()[0]
                tables = {
                    row[0]
                    for row in connection.execute("SHOW TABLES").fetchall()
                }
            finally:
                connection.close()
        self.assertEqual(version, SCHEMA_VERSION)
        self.assertTrue(
            {
                "seasonal_tuning_folds",
                "seasonal_period_candidates",
                "seasonal_tuning_validations",
                "seasonal_period_selections",
            }.issubset(tables)
        )


if __name__ == "__main__":
    unittest.main()
