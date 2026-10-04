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
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import duckdb

from util.shared_configuration import load_experiment_configuration
from util.shared_database import SCHEMA_VERSION, initialize_experiment_database
from util.p04_04_seasonal_period_tuning import (
    _compute_tuning_group,
    _forecast_job,
    _worker_results,
    historical_folds,
    original_scale_mae,
    run_distributed_tuned_forecasts,
    run_tuned_forecasts,
    seasonal_period_tuning_flow,
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

    def test_selection_is_independent_of_completion_order(self) -> None:
        """Controlled scores produce the same decision without test-owned threads."""
        arguments = ([4.0, 3.0, 2.0], [3.0, 2.0, 1.0], [True] * 3, [True] * 3)
        sequential = [select_period_policy(*arguments) for _ in range(8)]
        reordered = [select_period_policy(*arguments) for _ in reversed(range(8))]
        self.assertEqual(reordered, sequential)

    def test_resume_worker_output_rejects_missing_duplicate_and_unexpected_ids(self) -> None:
        """Stored/resumed groups cannot commit ambiguous worker output."""
        expected = {"a", "b"}
        for results in ([{"id": "a"}], [{"id": "a"}, {"id": "a"}],
                        [{"id": "a"}, {"id": "c"}]):
            with self.subTest(results=results), self.assertRaises(RuntimeError):
                _worker_results({"results": results}, expected)

    def test_distributed_wrapper_propagates_settings_without_serializing_coordinator(self) -> None:
        """The public adapter supplies path/data only and binds the existing scheduler."""
        coordinator = SimpleNamespace(database_path=Path("experiment.duckdb"))
        client = SimpleNamespace(scheduler=SimpleNamespace(address="tcp://scheduler:8786"))
        selected = MagicMock()
        with patch.object(seasonal_period_tuning_flow, "with_options", return_value=selected) as options:
            run_distributed_tuned_forecasts(
                coordinator, "experiment", [("task",)], {"task": 2}, client,
                SimpleNamespace(dask_retries=3), SimpleNamespace(),
            )
        runner = options.call_args.kwargs["task_runner"]
        self.assertEqual(runner.address, "tcp://scheduler:8786")
        selected.assert_called_once_with(
            Path("experiment.duckdb"), "experiment", [("task",)], {"task": 2},
            unittest.mock.ANY, 3, True,
        )
        self.assertNotIn(coordinator, selected.call_args.args)

    def test_local_wrapper_uses_named_flow_with_one_serial_task(self) -> None:
        """The approved local exception shares the flow and adds no scheduler."""
        coordinator = SimpleNamespace(database_path=Path("experiment.duckdb"))
        selected = MagicMock()
        with patch.object(seasonal_period_tuning_flow, "with_options", return_value=selected) as options:
            run_tuned_forecasts(coordinator, "experiment", [("task",)], {"task": 1})
        runner = options.call_args.kwargs["task_runner"]
        self.assertEqual(runner._max_workers, 1)
        selected.assert_called_once_with(
            Path("experiment.duckdb"), "experiment", [("task",)], {"task": 1},
            None, 0, False,
        )

    def test_shared_compute_reuses_stored_evidence_and_preserves_method_identity(self) -> None:
        """Cached folds/scores drive selection while only the bounded final native call runs."""
        folds = []
        candidates = {"ets": {}}
        validations = {"ets": {}}
        for number in range(1, 4):
            folds.append({
                "fold_id": f"fold-{number}", "fold_number": number,
                "train_start": 0, "train_end": number + 2,
                "validation_start": number + 2, "validation_end": number + 3,
                "horizon": 1, "raw_training_hash": "raw", "prepared_training_hash": "prepared",
                "context": [1.0, 2.0, 3.0], "preprocessing_id": f"prep-{number}",
                "transformation_method": "identity", "parameters": {}, "actual": [4.0],
                "preparation_metadata": {"transformation_parameters": {}},
            })
            candidate_id = f"candidate-{number}"
            candidates["ets"][str(number)] = {
                "candidate_id": candidate_id, "fold_id": f"fold-{number}", "model": "ets",
                "baseline_period": 1, "estimated_period": 2, "seasonal_strength": 0.8,
                "eligible": True, "reason": None, "package_versions": {"forecast": "8.23"},
            }
            validations["ets"][str(number)] = {
                "baseline": {"mae": 1.0, "status": "success"},
                "estimated": {"mae": 0.5, "status": "fallback" if number == 2 else "success"},
            }
        payload = {
            "group_id": "instance/variant/ets", "experiment_id": "experiment",
            "instance_id": "instance", "variant_id": "variant",
            "raw_context": [1.0] * 6, "horizon": 1, "dataset_id": "dataset",
            "series_id": "series", "cleaning": "standard", "transformation_method": "identity",
            "full_prepared": [1.0, 2.0, 3.0], "transformation_parameters": {},
            "transformation_id": "transformation", "baseline_period": 1,
            "tuning": {"validation_windows": 3, "minimum_cycles": 2},
            "folds": folds, "candidates": candidates, "validations": validations,
            "tasks": [{"task_id": "task", "model": "ets", "settings": {}}],
        }
        native = {
            "packages": {"forecast": "8.23"},
            "results": [{
                "id": "task/final", "mean": [9.0], "median": [8.0],
                "quantiles": [[7.0], [10.0]], "requested_method_id": "ets_forec",
                "executed_method_id": "ets_forec", "fallback_used": False,
                "fallback_reason": None, "provenance": {"seed": "fixed"},
            }],
        }
        with patch("util.p04_04_seasonal_period_tuning._run_tuning_r", return_value=native) as worker:
            result = _compute_tuning_group(payload, "preprocess.R", "forecast.R", 30.0, 1)
        model = result["models"][0]
        self.assertEqual(worker.call_count, 1)
        self.assertEqual(model["candidates"], [])
        self.assertEqual(model["validations"], [])
        self.assertEqual(model["selection"]["selected_policy"], "baseline")
        self.assertEqual(model["selection"]["status"], "inconclusive")
        self.assertEqual(model["forecast"]["mean"], [9.0])
        self.assertEqual(model["forecast"]["forecast_method"]["executed_method_id"], "ets_forec")
        self.assertFalse(model["forecast"]["forecast_method"]["fallback_used"])

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
