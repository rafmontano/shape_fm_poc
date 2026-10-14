"""Test independent gate validation and failed-state safety."""

import json
import math
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import duckdb

from util.shared_configuration import canonical_json
from util.shared_database import initialize_experiment_database
from util.shared_model_storage import ModelStorage
from util.shared_process_storage import ProcessStorage

ROOT = Path(__file__).resolve().parents[3]
from tests.configuration_fixtures import configuration_path
CONFIGURATION = configuration_path(version=1)


class ProcessStorageTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / "experiment.duckdb"
        initialize_experiment_database(self.database, CONFIGURATION)

    def tearDown(self):
        self.temporary.cleanup()

    def test_directional_validator_inspects_all_models_without_deserialization(self):
        """The real v11 validator checks fifteen opaque files and scientific SQL counts."""
        models = ModelStorage(self.temporary.name, "opaque")
        scopes = [("directional_dtw", "all_horizons")] + [
            ("directional_mantis_rf", horizon) for horizon in range(1, 15)
        ]
        for model, scope in scopes:
            path = models.path(model, "D", scope)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"cprovider_not_installed\nModel\n.")
        connection = mock.Mock()
        connection.execute.return_value.fetchone.side_effect = [
            (100,), (1430, 1430), (1,), (1,), (595, 0), (495, 100),
            (1,), (595,), (14,), (896,), (14,), (2800,), (1400,), (1400,),
            (0,), (0,),
        ]
        configuration = mock.Mock(model_storage={"root": "unused", "experiment": "opaque"})
        with mock.patch("util.shared_database.load_database_configuration", return_value=configuration), \
             mock.patch("util.shared_model_storage.ModelStorage", return_value=models), \
             mock.patch.object(models, "inspect", wraps=models.inspect) as inspection, \
             mock.patch("util.shared_model_storage.joblib.load", side_effect=AssertionError("coordinator deserialized")):
            result = ProcessStorage(self.database)._validate_directional(connection, 4, 11)
        self.assertTrue(result["output_validated"])
        self.assertEqual(inspection.call_args_list, [mock.call(model, "D", scope) for model, scope in scopes])

    def test_zero_surviving_tasks_cannot_validate_expected_forecasts(self):
        connection = duckdb.connect(str(self.database))
        try:
            configuration = json.dumps({"models": {"auto_arima": {}, "chronos_2": {}}})
            connection.execute("INSERT INTO experiments (experiment_id, benchmark_configuration_id, dataset_id, name, scientific_configuration, configuration_hash, scope, status) VALUES ('e','b','d','n',?,'h','s','planned')", [configuration])
            connection.execute("INSERT INTO experiment_variants VALUES ('v','e','standard','none','none','{}',current_timestamp)")
            connection.execute("INSERT INTO forecast_instances VALUES ('i','b','d','s','0','w',0,0,1,1,2,1,[1.0],[2.0],'{}',current_timestamp)")
        finally:
            connection.close()
        with self.assertRaisesRegex(RuntimeError, "expected=2"):
            ProcessStorage(self.database).validate(4)
        storage = ProcessStorage(self.database)
        self.assertTrue(storage.forecast_requires_gpu(True))
        with duckdb.connect(str(self.database)) as connection:
            connection.execute("""INSERT INTO experiment_tasks(task_id,experiment_id,stage,
                forecast_instance_id,variant_id,candidate,status)
                VALUES ('gpu','e',4,'i','v','chronos_2','completed')""")
        self.assertFalse(storage.forecast_requires_gpu(True))
        with duckdb.connect(str(self.database)) as connection:
            connection.execute("UPDATE experiment_tasks SET status='failed' WHERE task_id='gpu'")
        self.assertTrue(storage.forecast_requires_gpu(True))
        with duckdb.connect(str(self.database)) as connection:
            connection.execute(
                """UPDATE experiment_tasks
                   SET candidate='directional_mantis_rf:representations', status='completed'
                   WHERE task_id='gpu'"""
            )
        self.assertFalse(storage.forecast_requires_gpu(True))
        with duckdb.connect(str(self.database)) as connection:
            connection.execute("UPDATE experiment_tasks SET status='pending' WHERE task_id='gpu'")
        self.assertTrue(storage.forecast_requires_gpu(False))

    def test_float64_ulp_boundaries_signed_zero_and_subnormals(self):
        value = 1.0
        for steps in range(7):
            if steps:
                value = math.nextafter(value, math.inf)
            result = ProcessStorage.compare_float64([1.0, -0.0], [value, 0.0])
            self.assertEqual(result["maximum_ulp_distance"], steps)
            self.assertEqual(result["affected_value_count"], int(steps != 0))
            self.assertEqual(result["classification"], "computationally_equivalent" if steps <= 4
                             else "numeric_reproducibility_warning")
        tiny = math.nextafter(0.0, 1.0)
        self.assertEqual(ProcessStorage.compare_float64([-tiny], [tiny])["maximum_ulp_distance"], 2)
        largest = float.fromhex("0x1.fffffffffffffp+1023")
        overflow = ProcessStorage.compare_float64([-largest], [largest])
        self.assertEqual(overflow["classification"], "numeric_reproducibility_warning")
        self.assertEqual(overflow["maximum_relative_difference"], 2.0)
        canonical_json(overflow)
        for invalid in (None, math.nan, math.inf, -math.inf, True):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(RuntimeError, "finite"):
                ProcessStorage.compare_float64([1.0], [invalid])

    def test_scientific_impact_requires_researcher_review_even_below_four_ulps(self):
        record = ProcessStorage.compare_float64([1.0], [math.nextafter(1.0, math.inf)])
        for effect in ("directional_label", "selected_candidate", "selected_lambda",
                       "model_prediction", "classification", "rank", "reporting_precision",
                       "population_impact"):
            evidence = ProcessStorage.numerical_review([record], {effect: True})
            self.assertEqual(evidence["classification"], "researcher_review_required")
        self.assertEqual(ProcessStorage.numerical_review([record], {"rank": False})["classification"],
                         "computationally_equivalent")

if __name__ == "__main__":
    unittest.main()
