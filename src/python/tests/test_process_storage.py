"""Test independent gate validation and failed-state safety."""

import json
import math
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import duckdb

from util.shared_configuration import canonical_json, json_fingerprint
from util.shared_database import initialize_experiment_database
from util.shared_model_storage import ModelStorage
from util.shared_process_storage import ProcessStorage
from util.p00_01_researcher_actions import ProcessAction
from util.shared_transformations import TransformationResult, transform

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

    def test_gate3_cross_host_numeric_warning_continues_without_rewriting(self):
        self._insert_gate_fixture()
        original = transform([1.0, 2.0, 3.0], "identity")
        with duckdb.connect(str(self.database), read_only=True) as c:
            before = c.execute("SELECT transformed_target,output_hash FROM transformed_series").fetchall()
            scientific = c.execute("SELECT configuration_hash FROM experiments").fetchall()
        for steps in (0, 1, 4, 5):
            value = original.values[0]
            for _ in range(steps):
                value = math.nextafter(value, math.inf)
            recalculated = TransformationResult((value, *original.values[1:]), original.parameters)
            with mock.patch("util.shared_process_storage.transform", return_value=recalculated):
                result = ProcessStorage(self.database).validate(3)["numerical_reproducibility"]
            self.assertEqual(result["maximum_ulp_distance"], steps)
            self.assertEqual(result["warning_count"], int(steps > 4))
            self.assertEqual(len(result["records"]), 1)
        with duckdb.connect(str(self.database), read_only=True) as c:
            self.assertEqual(c.execute("SELECT transformed_target,output_hash FROM transformed_series").fetchall(), before)
            self.assertEqual(c.execute("SELECT configuration_hash FROM experiments").fetchall(), scientific)

    def test_gate3_integrity_is_not_a_numeric_warning(self):
        self._insert_gate_fixture()
        changes = [
            ("input_hash", "'wrong'", "hash"),
            ("output_hash", "'wrong'", "hash"),
            ("transformed_target", "[1.0,2.0]", "shape"),
            ("transformed_target", "[1.0,NULL,3.0]", "shape"),
            ("transformed_target", "[1.0,9.0,3.0]", "hash"),
            ("forecast_instance_id", "'wrong'", "missing"),
            ("variant_id", "'wrong'", "missing"),
            ("experiment_id", "'wrong'", "missing"),
            ("parent_result_id", "'wrong'", "lineage"),
            ("transformation_method", "'standardise_sample_v1'", "lineage"),
            ("parameters", "'{\"unexpected\":1}'", "parameters"),
        ]
        for field, value, message in changes:
            with self.subTest(field=field, value=value):
                with duckdb.connect(str(self.database)) as c:
                    old = c.execute(f"SELECT {field} FROM transformed_series").fetchone()[0]
                    c.execute(f"UPDATE transformed_series SET {field}={value}")
                with self.assertRaisesRegex(RuntimeError, message):
                    ProcessStorage(self.database).validate(3)
                with duckdb.connect(str(self.database)) as c:
                    c.execute(f"UPDATE transformed_series SET {field}=?", [old])

    def test_scientific_impact_requires_researcher_review_even_below_four_ulps(self):
        record = ProcessStorage.compare_float64([1.0], [math.nextafter(1.0, math.inf)])
        for effect in ("directional_label", "selected_candidate", "selected_lambda",
                       "model_prediction", "classification", "rank", "reporting_precision",
                       "population_impact"):
            evidence = ProcessStorage.numerical_review([record], {effect: True})
            self.assertEqual(evidence["classification"], "researcher_review_required")
        self.assertEqual(ProcessStorage.numerical_review([record], {"rank": False})["classification"],
                         "computationally_equivalent")

    def test_cross_host_changed_direction_marks_review_without_stopping(self):
        self._insert_gate_fixture()
        future = [3.0, 5.0, 6.0]
        with duckdb.connect(str(self.database)) as c:
            c.execute("UPDATE forecast_instances SET actual_target=?", [future])
        origin = math.nextafter(math.nextafter(3.0, -math.inf), -math.inf)
        alternative = TransformationResult((1.0, 2.0, origin), {})
        with mock.patch("util.shared_process_storage.transform", return_value=alternative):
            evidence = ProcessStorage(self.database).validate(3)["numerical_reproducibility"]
        self.assertEqual(evidence["maximum_ulp_distance"], 2)
        self.assertEqual(evidence["directional_label_changes"], 1)
        self.assertEqual(evidence["classification"], "researcher_review_required")

    def test_execution_persists_operational_validation_only(self):
        self._insert_gate_fixture()
        action = ProcessAction()
        wrapper = mock.Mock()
        wrapper.run.return_value = {"selected": 1}
        with mock.patch.object(action.registry, "load", return_value=wrapper):
            result = action._execute(ProcessStorage(self.database), None, None, 3)
        self.assertEqual(result["summary"]["numerical_reproducibility"]["maximum_ulp_distance"], 0)
        with duckdb.connect(str(self.database), read_only=True) as c:
            summary = json.loads(c.execute("SELECT summary FROM experiment_processes WHERE process_id=3").fetchone()[0])
            self.assertEqual(summary, result["summary"])
            self.assertEqual(c.execute("SELECT configuration_hash FROM experiments").fetchone()[0], "h")

    def test_fitted_numeric_state_is_compared_but_recipe_metadata_is_exact(self):
        self._insert_gate_fixture()
        fitted = transform([1.0, 2.0, 3.0], "standardise_sample_v1")
        with duckdb.connect(str(self.database)) as c:
            c.execute("UPDATE experiment_variants SET transformation_method='standardise_sample_v1'")
            c.execute("UPDATE transformed_series SET transformation_method='standardise_sample_v1', transformed_target=?,output_hash=?,parameters=?",
                      [list(fitted.values), json_fingerprint(fitted.values), canonical_json(fitted.parameters)])
        alternative = {**fitted.parameters, "scale": math.nextafter(fitted.parameters["scale"], math.inf)}
        from util.shared_transformations import apply_transformation
        recalculated = TransformationResult(apply_transformation([1.0, 2.0, 3.0], alternative), alternative)
        with mock.patch("util.shared_process_storage.transform", return_value=recalculated):
            result = ProcessStorage(self.database).validate(3)["numerical_reproducibility"]
        self.assertEqual(result["classification"], "computationally_equivalent")
        self.assertEqual(result["records"][0]["fitted_state_comparison"]["maximum_ulp_distance"], 1)
        with duckdb.connect(str(self.database)) as c:
            c.execute("UPDATE transformed_series SET parameters=?", [canonical_json({**fitted.parameters, "count": 2})])
        with self.assertRaisesRegex(RuntimeError, "parameters"):
            ProcessStorage(self.database).validate(3)


if __name__ == "__main__":
    unittest.main()
