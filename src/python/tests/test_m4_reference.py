"""Tests for archived M4 storage, Gate 4 retrieval, and common mean lookup."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import duckdb

from util.shared_database import SCHEMA_VERSION, migrate_database
from util.shared_experiment_execution import (
    ExperimentCoordinator,
    get_forecast_mean,
    validate_forecast_capability,
)
from util.p04_05_m4_submission import retrieve_m4_submission


class M4ReferenceLoadingTests(unittest.TestCase):
    """Bounded 100-series provider/coordinator fixture, without other Process 04 work."""

    def test_100_canonical_series_archive_and_restart(self):
        root = Path(__file__).resolve().parents[3]
        with tempfile.TemporaryDirectory() as directory:
            database = migrate_database(Path(directory) / "references.duckdb")
            with duckdb.connect(str(database)) as connection:
                connection.execute("""INSERT INTO experiments
                    (experiment_id,benchmark_configuration_id,dataset_id,name,
                     scientific_configuration,configuration_hash,scope,status)
                    VALUES ('fixture','benchmark','canonical','fixture','{}','hash','100','planned')""")
                # Asymmetric identities and deliberately unrelated canonical values.
                # Their only role here is identity/position; no comparison is allowed.
                for position in range(100):
                    identity = str(99 - position)
                    connection.execute("""INSERT INTO series
                        (dataset_id,series_id,source_series_id,source_row,frequency,
                         start_timestamp,target,observation_count,content_hash,source_metadata)
                        VALUES ('canonical',?,?,?,'D',current_timestamp,?,30,'fixture','{}')""",
                        [identity, identity, position, [-1000.0 - position] * 30])
                    connection.execute("""INSERT INTO evaluation_windows
                        (dataset_id,series_id,window_id,split_name,train_start,train_end,
                         validation_start,validation_end,test_start,test_end,horizon,boundary_convention)
                        VALUES ('canonical',?,'short/000','validation_and_test',0,2,2,16,16,30,14,
                                'zero-based, end-exclusive')""", [identity])
                # The real archived vectors remain; x/xx are inaccessible even if
                # a provider accidentally reintroduces history/future comparison.
                bridge = Path(directory) / "reference_fixture.R"
                bridge.write_text(f'''source({json.dumps(str(root / "src/r/04_03_forecast_m4_reference.R"))})
objects <- new.env()
data("M4", package="M4comp2018", envir=objects)
data("submission_info", package="M4comp2018", envir=objects)
vectors <- lapply(objects$M4, function(entry) {{
  e <- new.env()
  e$st <- entry$st; e$period <- entry$period; e$pt_ff <- entry$pt_ff
  for (name in c("x", "xx")) makeActiveBinding(name, function(...) stop("time-series access forbidden"), e)
  e
}})
payload <- jsonlite::fromJSON(file("stdin"), simplifyVector=FALSE)
result <- forecast_m4_reference(payload$jobs, vectors, objects$submission_info,
                                as.character(packageVersion("M4comp2018")))
cat(jsonlite::toJSON(result, auto_unbox=TRUE, digits=17, null="null"))
''')
                coordinator = ExperimentCoordinator.__new__(ExperimentCoordinator)
                coordinator.root = root
                coordinator.connection = connection
                coordinator.configuration = SimpleNamespace(
                    resolved={"archived_forecasts": {"enabled": ["m4_smyl", "m4_fforma"]}},
                    execution_paths={"r_m4comp2018_worker": str(bridge)},
                    execution={"worker_timeouts_seconds": {"r": 120}, "thread_limits": {"r": 1}},
                )
                self.assertEqual(coordinator.store_m4_references("fixture"), 200)
                self.assertEqual(connection.execute(
                    "SELECT forecast_id,submission_id,count(*) FROM reference_forecasts GROUP BY ALL ORDER BY forecast_id"
                ).fetchall(), [("m4_fforma", 245, 100), ("m4_smyl", 118, 100)])
                for provider, expected in (
                    ("m4_smyl", [2030.245625, 2027.124375, 2030.594375]),
                    ("m4_fforma", [2030.78274281549, 2031.8173540627, 2032.85253254582]),
                ):
                    official, mean, capability = connection.execute(
                        "SELECT official_m4_series_id,mean,forecast_capability FROM reference_forecasts WHERE series_id='99' AND forecast_id=?",
                        [provider],
                    ).fetchone()
                    self.assertEqual((official, capability), ("D1", "mean_only"))
                    for observed, wanted in zip(mean[:3], expected):
                        self.assertAlmostEqual(observed, wanted, places=8)
                connection.execute("DELETE FROM reference_forecasts WHERE series_id='0' AND forecast_id='m4_fforma'")
                coordinator.configuration.execution_paths["r_m4comp2018_worker"] = "src/r/04_03_forecast_m4_reference.R"
                self.assertEqual(coordinator.store_m4_references("fixture"), 1)
                with patch.object(coordinator, "_r_worker", side_effect=AssertionError("provider on completed restart")):
                    self.assertEqual(coordinator.store_m4_references("fixture"), 0)
                self.assertEqual(connection.execute("SELECT count(*) FROM reference_forecasts").fetchone()[0], 200)
                print("Archived-reference fixture: 100 canonical series → 200 references; partial restart 1; completed restart 0")


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
        # Stored-reference consumers are independent of the canonical import path.
        for record in self.records:
            connection.execute(
                """INSERT INTO reference_forecasts
                (reference_forecast_id, dataset_id, series_id, official_m4_series_id,
                 forecast_id, submission_id, submission_rank, submission_author,
                 horizon, mean, point_semantics, forecast_capability,
                 source_metadata, content_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    f"reference/{record['forecast_id']}", record["dataset_id"],
                    record["series_id"], record["official_m4_series_id"],
                    record["forecast_id"], record["submission_id"],
                    record["submission_rank"], record["submission_author"],
                    record["horizon"], record["mean"], record["point_semantics"],
                    record["forecast_capability"],
                    json.dumps({
                        "source_package": record["source_package"],
                        "source_package_version": record["source_package_version"],
                        "source_revision": record["source_revision"], "scale": "original",
                    }),
                    record["content_hash"],
                ],
            )
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
        source = (Path(__file__).parents[1] / "util/p04_05_m4_submission.py").read_text()
        self.assertNotIn("forecast::", source)
        self.assertNotIn("fallback_", source.lower())

    def test_mean_only_forecast_uses_eight_mean_metrics(self):
        """Process 06 selects the mean profile and stores finite point metrics."""
        connection = duckdb.connect(str(self.database))
        connection.execute(
            """INSERT INTO experiment_tasks
            (task_id, experiment_id, stage, forecast_instance_id, variant_id,
             candidate, status)
            VALUES ('stage3', 'experiment', 3, 'instance', NULL, 'identity', 'completed')"""
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
        coordinator.configuration = SimpleNamespace(
            source_directory=Path("source"),
            resolved={"data": {"dataset_name": "m4_daily",
                               "benchmark": {"term": "short"}},
                      "evaluation": {"gift_eval": {"environment": "gift-env"}}},
            evaluation_options={"axis": None, "mask_invalid_label": True,
                                "allow_nan_forecast": False,
                                "seasonality": "pinned GluonTS benchmark seasonality",
                                "batch_size": 16},
            execution={"worker_timeouts_seconds": {"gift_eval": 60}, "dask_retries": 0},
        )
        coordinator.config = {"benchmark": {"gift_eval_revision": "revision"}}
        coordinator.quantiles = (0.1, 0.5, 0.9)
        coordinator._commit_task = lambda task_id, attempt, runtime, insert, *_: insert()
        captured = []
        metrics = {
            name: float(index) for index, name in enumerate(
                ("MSE[mean]", "MAE[mean]", "MASE[mean]", "MAPE[mean]",
                 "sMAPE[mean]", "RMSE[mean]", "NRMSE[mean]", "ND[mean]"), 1
            )
        }

        def evaluate(**kwargs):
            """Capture the prepared profile while emulating a finite bridge result."""
            captured.extend(kwargs["batches"])
            item = kwargs["batches"][0][0]
            return [{"batch": [item], "response": {
                "results": [{"id": item["task_id"], "official": metrics}],
                "runtime_seconds": 0.1, "worker": {"hostname": "fixture"},
            }}]

        with patch("util.shared_workflow_orchestration.run_gate_compute_flow",
                   side_effect=evaluate):
            coordinator._run_06_evaluate(
                "experiment",
                [("task", None, "robust-variant", "mean-only-eval")],
                {"task": 1},
                1,
            )
        item = captured[0][0]
        self.assertEqual(item["evaluation_profile"], "mean_based_v1")
        self.assertEqual(item["forecast_capability"], "mean_only")
        self.assertIsNone(item["payload"]["quantile_levels"])
        stored = connection.execute(
            "SELECT options, metrics FROM official_evaluations"
        ).fetchone()
        self.assertEqual(json.loads(stored[0])["evaluation_profile"], "mean_based_v1")
        self.assertEqual(json.loads(stored[1]), metrics)
        connection.close()


if __name__ == "__main__":
    unittest.main()
