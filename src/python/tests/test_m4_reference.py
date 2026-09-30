"""Tests for archived M4 storage, Gate 4 retrieval, and common mean lookup."""

from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import duckdb

from util.configuration import ImportValidationError
from util.database import SCHEMA_VERSION, migrate_database
from util.experiment_execution import (
    ExperimentCoordinator,
    get_forecast_mean,
    validate_forecast_capability,
)
from util.import_execution import ImportCoordinator
from util.m4_submission import retrieve_m4_submission


class M4ReferenceTests(unittest.TestCase):
    """Exercise the separate mean-only archive and retrieval boundaries."""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = migrate_database(Path(self.temp.name) / "m4.duckdb")
        connection = duckdb.connect(str(self.database))
        connection.execute(
            """INSERT INTO benchmark_configurations VALUES
            ('benchmark', 'revision', 'm4_daily/D/short', 'm4_daily', 'D', 'short',
             2, 1, 'Econ/Fin', 1, '{}', current_timestamp)"""
        )
        connection.execute(
            """INSERT INTO experiments
            (experiment_id, benchmark_configuration_id, dataset_id, name,
             scientific_configuration, configuration_hash, scope, status)
            VALUES ('experiment', 'benchmark', 'dataset', 'test', '{}', 'hash',
                    'first_official:1', 'planned')"""
        )
        for variant, cleaning, transformation in (
            ("robust-variant", "robust", "identity"),
            ("transformed-variant", "robust", "minmax_then_standardize"),
        ):
            connection.execute(
                """INSERT INTO experiment_variants VALUES
                (?, 'experiment', ?, ?, 'identity', '{}', current_timestamp)""",
                [variant, cleaning, transformation],
            )
        connection.execute(
            """INSERT INTO forecast_instances VALUES
            ('instance', 'benchmark', 'dataset', '0', '0', 'short/000', 0,
             0, 3, 3, 5, 2, [1.0,2.0,3.0], [4.0,5.0], '{}', current_timestamp)"""
        )
        connection.execute(
            """INSERT INTO evaluation_windows
            (dataset_id, series_id, window_id, split_name, train_start, train_end,
             validation_start, validation_end, test_start, test_end, horizon,
             boundary_convention)
            VALUES ('dataset', '0', 'short/000', 'validation_and_test', 0, 1,
                    1, 3, 3, 5, 2, 'zero-based, end-exclusive')"""
        )
        connection.execute(
            """INSERT INTO series
            (dataset_id, series_id, source_series_id, source_row, frequency,
             start_timestamp, target, observation_count, content_hash,
             source_metadata)
            VALUES ('dataset', '0', '0', 0, 'D', current_timestamp,
                    [1.0,2.0,3.0,4.0,5.0], 5, 'series-hash', '{}')"""
        )
        self.records = [
            self.record("m4_smyl", 118, 1, "Smyl", [10.0, 11.0], "smyl-hash"),
            self.record(
                "m4_fforma", 245, 2, "Montero-Manso, et al.",
                [20.0, 21.0], "fforma-hash",
            ),
        ]
        holder = ImportCoordinator.__new__(ImportCoordinator)
        holder.connection = connection
        self.assertEqual(holder._store_reference_forecasts(self.records), (2, 0))
        self.assertEqual(holder._store_reference_forecasts(self.records), (0, 2))
        connection.execute(
            """INSERT INTO forecasts
            (forecast_id, experiment_id, variant_id, forecast_instance_id,
             candidate, scale, mean, median, quantile_levels, quantiles,
             execution_metadata, content_hash, forecast_capability)
            VALUES ('generated', 'experiment', 'robust-variant', 'instance',
                    'auto_arima', 'original', [30.0,31.0], [30.0,31.0],
                    [0.5], [[30.0,31.0]], '{}', 'generated-hash', 'probabilistic')"""
        )
        connection.close()

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def record(forecast_id, submission_id, rank, author, mean, content_hash):
        return {
            "dataset_id": "dataset", "series_id": "0",
            "official_m4_series_id": "D1", "forecast_id": forecast_id,
            "submission_id": submission_id, "submission_rank": rank,
            "submission_author": author, "horizon": 2, "mean": mean,
            "point_semantics": "M4 competition point forecast",
            "forecast_capability": "mean_only", "source_package": "M4comp2018",
            "source_package_version": "0.2.0", "source_revision": "revision",
            "content_hash": content_hash,
        }

    @staticmethod
    def request(forecast_id="m4_smyl", **changes):
        value = {
            "experiment_id": "experiment", "variant_id": "official_reference",
            "forecast_instance_id": "instance", "dataset_id": "dataset",
            "series_id": "0", "window_id": "short/000",
            "forecast_id": forecast_id, "horizon": 2,
        }
        value.update(changes)
        return value

    def test_migration_and_storage_contract(self):
        connection = duckdb.connect(str(self.database), read_only=True)
        self.assertEqual(
            connection.execute("SELECT max(version) FROM schema_versions").fetchone()[0],
            SCHEMA_VERSION,
        )
        row = connection.execute(
            """SELECT mean, forecast_capability, source_metadata
               FROM reference_forecasts WHERE forecast_id='m4_smyl'"""
        ).fetchone()
        connection.close()
        self.assertEqual(row[0], [10.0, 11.0])
        self.assertEqual(row[1], "mean_only")
        self.assertEqual(json.loads(row[2])["scale"], "original")

    def test_version_five_forecast_schema_migrates_without_losing_rows(self):
        legacy = Path(self.temp.name) / "legacy.duckdb"
        connection = duckdb.connect(str(legacy))
        connection.execute(
            """CREATE TABLE schema_versions (
                version INTEGER PRIMARY KEY, applied_at TIMESTAMP DEFAULT current_timestamp,
                description VARCHAR NOT NULL
            )"""
        )
        connection.execute("INSERT INTO schema_versions VALUES (5, current_timestamp, 'legacy')")
        connection.execute(
            """CREATE TABLE forecasts (
                forecast_id VARCHAR PRIMARY KEY, median DOUBLE[] NOT NULL,
                quantile_levels DOUBLE[] NOT NULL, quantiles DOUBLE[][] NOT NULL
            )"""
        )
        connection.execute(
            "INSERT INTO forecasts VALUES ('kept', [1.0], [0.5], [[1.0]])"
        )
        connection.close()
        migrate_database(legacy)
        connection = duckdb.connect(str(legacy), read_only=True)
        columns = {
            row[1]: row for row in connection.execute("PRAGMA table_info('forecasts')").fetchall()
        }
        self.assertEqual(connection.execute("SELECT count(*) FROM forecasts").fetchone()[0], 1)
        self.assertIn("forecast_capability", columns)
        self.assertFalse(columns["median"][3])
        self.assertEqual(
            connection.execute("SELECT max(version) FROM schema_versions").fetchone()[0],
            SCHEMA_VERSION,
        )
        self.assertEqual(
            connection.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name='reference_forecasts'"
            ).fetchone()[0],
            1,
        )
        connection.close()

    def test_conflicting_restart_is_rejected(self):
        connection = duckdb.connect(str(self.database))
        holder = ImportCoordinator.__new__(ImportCoordinator)
        holder.connection = connection
        changed = [dict(self.records[0], mean=[99.0, 99.0])]
        with self.assertRaisesRegex(ImportValidationError, "conflicting"):
            holder._store_reference_forecasts(changed)
        connection.close()

    def test_gate1_sends_only_selected_identity_and_validates_response(self):
        connection = duckdb.connect(str(self.database))
        holder = ImportCoordinator.__new__(ImportCoordinator)
        holder.connection = connection
        holder.configuration = SimpleNamespace(
            execution_paths={"r_m4comp2018_worker": "src/r/01_02_import_m4comp2018.R"},
            execution={"worker_timeouts_seconds": {"r": 30}},
        )
        config = {
            "dataset_name": "m4_daily", "max_series": 1,
            "benchmark": {"frequency": "D", "prediction_length": 2},
            "archived_forecasts": {
                "enabled": ["m4_smyl"],
                "providers": {"m4_smyl": {"submission_id": 118}},
            },
        }
        response = dict(self.records[0])
        with patch("util.import_execution.subprocess.run") as run:
            run.return_value = subprocess.CompletedProcess(
                ["Rscript"], 0, json.dumps({"records": [response]}), ""
            )
            records = holder._read_m4_reference_forecasts("dataset", config)
        self.assertEqual(records, [response])
        payload = json.loads(run.call_args.kwargs["input"])
        self.assertEqual(payload["forecast_ids"], ["m4_smyl"])
        self.assertEqual(
            payload["series"],
            [{
                "series_id": "0", "source_position": 0,
                "official_m4_series_id": "D1", "history": [1.0, 2.0, 3.0],
                "future": [4.0, 5.0], "horizon": 2,
            }],
        )
        connection.close()

    def test_gate4_retrieves_each_exact_mean_with_provenance(self):
        smyl = retrieve_m4_submission(self.database, self.request())
        fforma = retrieve_m4_submission(self.database, self.request("m4_fforma"))
        self.assertEqual(smyl["mean"], [10.0, 11.0])
        self.assertEqual(fforma["mean"], [20.0, 21.0])
        self.assertEqual(smyl["horizon"], 2)
        self.assertEqual(smyl["forecast_capability"], "mean_only")
        self.assertEqual(smyl["provenance"]["submission_id"], 118)
        self.assertEqual(smyl["requested_provider"], smyl["executed_provider"])

    def test_gate4_rejects_incompatible_requests_without_fallback(self):
        cases = (
            ({"series_id": "1"}, "instance"),
            ({"horizon": 3}, "instance"),
            ({"window_id": "other/000"}, "final window"),
            ({"variant_id": "transformed-variant"}, "official_reference"),
            ({"forecast_id": "m4_other"}, "unapproved"),
        )
        for changes, pattern in cases:
            with self.subTest(changes=changes):
                with self.assertRaisesRegex(ValueError, pattern):
                    retrieve_m4_submission(self.database, self.request(**changes))

    def test_common_query_returns_same_type_for_live_and_archived(self):
        observed = [
            get_forecast_mean(
                self.database,
                "dataset",
                "0",
                "auto_arima_forec",
                "experiment",
                preprocessing_mode="robust",
                variant_id="robust-variant",
            ),
            get_forecast_mean(self.database, "dataset", "0", "m4_smyl"),
            get_forecast_mean(self.database, "dataset", "0", "m4_fforma"),
        ]
        self.assertEqual(observed, [(30.0, 31.0), (10.0, 11.0), (20.0, 21.0)])
        self.assertTrue(all(isinstance(value, tuple) for value in observed))

    def test_ambiguous_mode_requires_variant(self):
        """A mode alone cannot silently choose between transformed variants."""
        connection = duckdb.connect(str(self.database))
        connection.execute(
            """INSERT INTO forecasts
            (forecast_id, experiment_id, variant_id, forecast_instance_id,
             candidate, scale, mean, median, quantile_levels, quantiles,
             execution_metadata, content_hash, forecast_capability)
            VALUES ('generated-transformed', 'experiment', 'transformed-variant',
                    'instance', 'auto_arima', 'original', [40.0,41.0],
                    [40.0,41.0], [0.5], [[40.0,41.0]], '{}',
                    'generated-transformed-hash', 'probabilistic')"""
        )
        connection.close()
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            get_forecast_mean(
                self.database,
                "dataset",
                "0",
                "auto_arima_forec",
                "experiment",
                preprocessing_mode="robust",
            )

    def test_capability_fields_are_all_or_none(self):
        validate_forecast_capability([1.0], [1.0], [0.5], [[1.0]], "probabilistic")
        validate_forecast_capability([1.0], None, None, None, "mean_only")
        with self.assertRaisesRegex(ValueError, "require"):
            validate_forecast_capability([1.0], None, [0.5], [[1.0]], "probabilistic")
        with self.assertRaisesRegex(ValueError, "omit"):
            validate_forecast_capability([1.0], [1.0], None, None, "mean_only")
        connection = duckdb.connect(str(self.database))
        connection.execute(
            """INSERT INTO forecasts
            (forecast_id, experiment_id, variant_id, forecast_instance_id,
             candidate, scale, mean, median, quantile_levels, quantiles,
             execution_metadata, content_hash, forecast_capability)
            VALUES ('mean-only', 'experiment', 'robust-variant', 'instance',
                    'mean-only', 'original', [1.0,2.0], NULL, NULL, NULL,
                    '{}', 'mean-only-hash', 'mean_only')"""
        )
        with self.assertRaises(duckdb.ConstraintException):
            connection.execute(
                """INSERT INTO forecasts
                (forecast_id, experiment_id, variant_id, forecast_instance_id,
                 candidate, scale, mean, median, quantile_levels, quantiles,
                 execution_metadata, content_hash, forecast_capability)
                VALUES ('partial', 'experiment', 'robust-variant', 'instance',
                        'partial', 'original', [1.0,2.0], [1.0,2.0], NULL, NULL,
                        '{}', 'partial-hash', 'mean_only')"""
            )
        connection.close()

    def test_provider_contains_no_fitting_or_fallback(self):
        source = (Path(__file__).parents[1] / "util/m4_submission.py").read_text()
        self.assertNotIn("forecast::", source)
        self.assertNotIn("fallback_", source.lower())

    def test_full_gift_eval_rejects_mean_only_forecast(self):
        connection = duckdb.connect(str(self.database))
        connection.execute(
            """INSERT INTO experiment_tasks
            (task_id, experiment_id, stage, forecast_instance_id, variant_id,
             candidate, status)
            VALUES ('stage2', 'experiment', 2, 'instance', NULL, 'identity', 'completed')"""
        )
        connection.execute(
            """INSERT INTO forecasts
            (forecast_id, experiment_id, variant_id, forecast_instance_id,
             candidate, scale, mean, median, quantile_levels, quantiles,
             execution_metadata, content_hash, forecast_capability)
            VALUES ('mean-only-eval', 'experiment', 'robust-variant', 'instance',
                    'mean-only-eval', 'original', [1.0,2.0], NULL, NULL, NULL,
                    '{}', 'mean-only-eval-hash', 'mean_only')"""
        )
        coordinator = ExperimentCoordinator.__new__(ExperimentCoordinator)
        coordinator.connection = connection
        coordinator.root = Path(self.temp.name)
        coordinator.configuration = SimpleNamespace(source_directory=Path("source"))
        coordinator.quantiles = (0.1, 0.5, 0.9)
        with self.assertRaisesRegex(RuntimeError, "does not support mean-only"):
            coordinator._run_06_evaluate(
                "experiment",
                [("task", None, "robust-variant", "mean-only-eval")],
                {"task": 1},
                1,
            )
        connection.close()


if __name__ == "__main__":
    unittest.main()
