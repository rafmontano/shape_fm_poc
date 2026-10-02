"""Test independent gate validation and failed-state safety."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import duckdb

from util.configuration import canonical_json, json_fingerprint
from util.database import initialize_experiment_database
from util.process_storage import ProcessStorage
from util.researcher_actions import ProcessAction
from util.transformations import transform

ROOT = Path(__file__).resolve().parents[3]
CONFIGURATION = ROOT / "config/experiments/poc2_m4_daily_100.json"


class ProcessStorageTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / "experiment.duckdb"
        initialize_experiment_database(self.database, CONFIGURATION)

    def tearDown(self):
        self.temporary.cleanup()

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

    def test_failed_fresh_validation_leaves_gate_failed_not_completed(self):
        storage = ProcessStorage(self.database)
        action = ProcessAction()
        storage.transition(2, "completed", {})
        result = {"process_id": 2, "status": "completed", "summary": {}}
        with mock.patch.object(
            storage, "validate", side_effect=RuntimeError("invalid output")
        ):
            with self.assertRaisesRegex(RuntimeError, "invalid output"):
                action._validate(storage, 2, result)
        self.assertEqual(storage.process_rows()[1][1], "failed")

    def test_partial_resume_revalidates_predecessor_output(self):
        storage = ProcessStorage(self.database)
        storage.transition(1, "completed", {"series_count": 1})
        with self.assertRaisesRegex(RuntimeError, "missing stored output"):
            ProcessAction()._inspect(storage, 2)

    def _insert_gate_fixture(self) -> None:
        """Insert one valid Gate 2/3 chain; a class would add no value to this test fixture."""
        connection = duckdb.connect(str(self.database))
        source = [1.0, 2.0, 3.0]
        transformed = transform(source, "identity")
        try:
            connection.execute("INSERT INTO experiments (experiment_id,benchmark_configuration_id,dataset_id,name,scientific_configuration,configuration_hash,scope,status) VALUES ('e','b','d','n','{}','h','s','planned')")
            connection.execute("INSERT INTO experiment_variants VALUES ('v','e','standard','identity','none','{}',current_timestamp)")
            connection.execute("INSERT INTO forecast_instances VALUES ('i','b','d','s','0','w',0,0,3,3,6,3,?,[4.0,5.0,6.0],'{}',current_timestamp)", [source])
            connection.execute("INSERT INTO experiment_tasks (task_id,experiment_id,stage,forecast_instance_id,variant_id,candidate,status) VALUES ('t2','e',2,'i',NULL,'standard','completed'),('t3','e',3,'i','v',NULL,'completed')")
            connection.execute("INSERT INTO preprocessed_series VALUES ('p','e','i','standard',?,?,?,'{}','{}',NULL,'D',1,'success',0,0,false,current_timestamp)", [json_fingerprint(source), json_fingerprint(source), source])
            connection.execute("INSERT INTO transformed_series VALUES ('x','e','v','i','p','identity',?,?,?,?,? ,current_timestamp)", [json_fingerprint(source), json_fingerprint(transformed.values), list(transformed.values), canonical_json(transformed.parameters), 'p'])
        finally:
            connection.close()

    def test_gate2_altered_value_with_historical_hash_is_rejected(self):
        self._insert_gate_fixture()
        connection = duckdb.connect(str(self.database))
        connection.execute("UPDATE preprocessed_series SET context_target=[1.0,9.0,3.0]")
        connection.close()
        with self.assertRaisesRegex(RuntimeError, "preprocessing"):
            ProcessStorage(self.database).validate(2)

    def test_gate2_native_integer_hash_is_preserved(self):
        """Historical R JSON integer encoding survives DuckDB DOUBLE conversion."""
        self._insert_gate_fixture()
        fingerprint = json_fingerprint([1, 2, 3])
        connection = duckdb.connect(str(self.database))
        connection.execute("UPDATE preprocessed_series SET output_hash=?", [fingerprint])
        connection.close()
        self.assertTrue(ProcessStorage(self.database).validate(2)["output_validated"])
        connection = duckdb.connect(str(self.database), read_only=True)
        self.assertEqual(connection.execute("SELECT output_hash FROM preprocessed_series").fetchone()[0], fingerprint)
        connection.close()

    def test_gate2_wrong_parent_and_gate3_values_are_rejected(self):
        """Matching counts cannot mask wrong lineage or changed transformed values."""
        self._insert_gate_fixture()
        connection = duckdb.connect(str(self.database))
        connection.execute("UPDATE preprocessed_series SET parent_result_id='wrong'")
        connection.execute("UPDATE transformed_series SET transformed_target=[1,9,3]")
        connection.close()
        for stage, message in ((2, "preprocessing"), (3, "transformation")):
            with self.assertRaisesRegex(RuntimeError, message):
                ProcessStorage(self.database).validate(stage)

    def test_gate3_wrong_lineage_is_rejected(self):
        self._insert_gate_fixture()
        connection = duckdb.connect(str(self.database))
        connection.execute("UPDATE transformed_series SET parent_result_id='wrong'")
        connection.close()
        with self.assertRaisesRegex(RuntimeError, "transformation"):
            ProcessStorage(self.database).validate(3)

    def test_gate3_missing_task_and_output_is_rejected(self):
        self._insert_gate_fixture()
        connection = duckdb.connect(str(self.database))
        connection.execute("DELETE FROM transformed_series")
        connection.execute("DELETE FROM experiment_tasks WHERE stage=3")
        connection.close()
        with self.assertRaisesRegex(RuntimeError, "incomplete tasks or missing"):
            ProcessStorage(self.database).validate(3)


if __name__ == "__main__":
    unittest.main()
