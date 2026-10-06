"""Focused local integration contracts for the ID 027/028 comparison workflow."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from util.shared_configuration import load_experiment_configuration
from util.shared_database import SCHEMA_VERSION, initialize_experiment_database, migrate_database
from util.shared_experiment_execution import ExperimentCoordinator, expected_task_counts


ROOT = Path(__file__).resolve().parents[3]
V10 = ROOT / "config/experiments/poc2_m4_daily_100_directional_dtw.json"
V11 = ROOT / "config/experiments/poc2_m4_daily_100_directional_dtw_mantis_rf.json"


class DirectionalComparisonPipelineTests(unittest.TestCase):
    """Protect additive configuration, storage and stable prediction semantics."""

    def test_v10_contract_is_unchanged(self):
        configuration = load_experiment_configuration(V10)
        self.assertEqual(configuration.version, 10)
        self.assertEqual(
            configuration.scientific_hash,
            "a67ee2d36f3fddaca18a996f7291b1a6385cf99e0f85490d294d64ce6d525802",
        )
        self.assertEqual(
            expected_task_counts(100, configuration.workflow),
            {2: 100, 3: 100, 4: 1401, 5: 1, 6: 14},
        )

    def test_v11_resolves_two_reusable_components_and_independent_horizons(self):
        configuration = load_experiment_configuration(V11)
        self.assertEqual(configuration.version, 11)
        self.assertEqual(
            set(configuration.workflow["models"]),
            {"directional_dtw", "directional_mantis_rf"},
        )
        self.assertEqual(
            expected_task_counts(100, configuration.workflow),
            {2: 100, 3: 100, 4: 1430, 5: 1, 6: 28},
        )
        self.assertEqual(
            configuration.resolved["derived"]["expected_directional_prediction_rows"],
            2800,
        )
        self.assertEqual(
            configuration.workflow["models"]["directional_mantis_rf"]["classifier"],
            "random_forest",
        )

    def test_v10_database_migrates_additively_without_rewriting_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "v10.duckdb"
            configuration = initialize_experiment_database(database, V10)
            before = configuration.configuration_integrity_hash
            migrate_database(database)
            with duckdb.connect(str(database), read_only=True) as connection:
                version = connection.execute("SELECT max(version) FROM schema_versions").fetchone()[0]
                stored = connection.execute(
                    "SELECT configuration_integrity_hash FROM experiment_configuration"
                ).fetchone()[0]
                tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT table_name FROM information_schema.tables"
                    ).fetchall()
                }
            self.assertEqual(version, SCHEMA_VERSION)
            self.assertEqual(stored, before)
            self.assertIn("directional_predictions", tables)
            self.assertIn("model_directional_predictions", tables)

    def test_model_neutral_prediction_insert_or_verify_rejects_same_id_conflict(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "v11.duckdb"
            initialize_experiment_database(database, V11)
            record = {
                "prediction_id": "directional-prediction/logical",
                "experiment_id": "experiment/a",
                "model_definition_id": "model/a",
                "evaluation_input_id": "input/a",
                "horizon": 1,
                "prediction": 0,
                "classifier_run_id": "classifier-run/a",
                "training_fingerprint": "training",
                "evaluation_fingerprint": "evaluation",
                "output_fingerprint": "output-a",
                "content_hash": "content-a",
            }
            with ExperimentCoordinator(database) as coordinator:
                coordinator._insert_or_verify("model_directional_predictions", record)
                coordinator._insert_or_verify("model_directional_predictions", record)
                conflict = {**record, "prediction": 1, "output_fingerprint": "output-b",
                            "content_hash": "content-b"}
                with self.assertRaisesRegex(RuntimeError, "conflicting accepted"):
                    coordinator._insert_or_verify(
                        "model_directional_predictions", conflict
                    )

    def test_v11_plan_uses_one_process_03_and_independent_horizon_tasks(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "plan.duckdb"
            initialize_experiment_database(database, V11)
            with duckdb.connect(str(database)) as connection:
                connection.execute(
                    """INSERT INTO datasets VALUES
                       ('dataset/test', 'm4_daily', 'fixture', 'revision', '{}', '{}',
                        'hash', 'D', '{}', current_timestamp)"""
                )

            def official(limit: int):
                return {
                    "configuration_name": "m4_daily/D/short",
                    "dataset_name": "m4_daily", "frequency": "D", "term": "short",
                    "prediction_length": 14, "window_count": 1,
                    "domain": "Econ/Fin", "num_variates": 1,
                    "available_instances": 100, "r_period": 7,
                    "r_period_source": "pinned frequency",
                    "evaluation_seasonality": 7,
                    "gluonts_default_seasonality": 7,
                    "instances": [
                        {
                            "item_id": f"D{index + 1}", "variate_id": "0",
                            "window_id": "short/000", "official_position": index,
                            "context": [float(offset) for offset in range(64)],
                            "actual": [float(64 + offset) for offset in range(14)],
                            "start": "2000-01-01", "forecast_start": "2000-03-05",
                        }
                        for index in range(limit)
                    ],
                }

            with ExperimentCoordinator(database) as coordinator:
                with patch.object(
                    coordinator,
                    "_gift_bridge",
                    side_effect=lambda *arguments, **_: official(
                        int(arguments[arguments.index("--limit") + 1])
                    ),
                ):
                    plan = coordinator.plan()
                counts = dict(coordinator.connection.execute(
                    """SELECT stage, count(*) FROM experiment_tasks
                       WHERE experiment_id=? GROUP BY stage""",
                    [plan.experiment_id],
                ).fetchall())
                process_4 = {
                    row[0]: int(row[1])
                    for row in coordinator.connection.execute(
                        """SELECT candidate, count(*) FROM experiment_tasks
                           WHERE experiment_id=? AND stage=4 GROUP BY candidate""",
                        [plan.experiment_id],
                    ).fetchall()
                }
                process_6 = {
                    row[0] for row in coordinator.connection.execute(
                        """SELECT candidate FROM experiment_tasks
                           WHERE experiment_id=? AND stage=6""",
                        [plan.experiment_id],
                    ).fetchall()
                }
            self.assertEqual(counts, {2: 100, 3: 100, 4: 1430, 5: 1, 6: 28})
            self.assertEqual(process_4["directional_mantis_rf:representations"], 1)
            self.assertEqual(
                sum(count for candidate, count in process_4.items()
                    if candidate.startswith("directional_mantis_rf:train:h")),
                14,
            )
            self.assertEqual(
                sum(count for candidate, count in process_4.items()
                    if candidate.startswith("directional_mantis_rf:predict:h")),
                14,
            )
            self.assertEqual(
                len({candidate for candidate in process_6
                     if candidate.startswith("directional_mantis_rf:h")}),
                14,
            )
            with ExperimentCoordinator(database) as coordinator:
                coordinator.connection.execute(
                    """UPDATE experiment_tasks SET status='completed'
                       WHERE experiment_id=? AND stage=4
                         AND (candidate='directional_dtw:train'
                              OR candidate LIKE 'directional_mantis_rf:train:h%')""",
                    [plan.experiment_id],
                )
                with patch(
                    "util.shared_model_storage.ModelStorage.exists",
                    side_effect=lambda model, _frequency, scope: not (
                        model == "directional_mantis_rf" and scope == 7
                    ),
                ):
                    coordinator._reopen_missing_fitted_model_tasks(plan.experiment_id)
                training_statuses = dict(
                    coordinator.connection.execute(
                        """SELECT candidate, status FROM experiment_tasks
                           WHERE experiment_id=? AND stage=4
                             AND (candidate='directional_dtw:train'
                                  OR candidate LIKE 'directional_mantis_rf:train:h%')""",
                        [plan.experiment_id],
                    ).fetchall()
                )
            self.assertEqual(
                training_statuses["directional_mantis_rf:train:h07"], "pending"
            )
            self.assertTrue(
                all(
                    status == "completed"
                    for candidate, status in training_statuses.items()
                    if candidate != "directional_mantis_rf:train:h07"
                )
            )


if __name__ == "__main__":
    unittest.main()
