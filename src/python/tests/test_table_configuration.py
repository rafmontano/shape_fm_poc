# ==============================================================================
# test_table_configuration.py
# Purpose: Protect v12 configuration and mixed upstream task boundaries locally.
# Inputs: Reference JSON, synthetic official inputs and mocked Process 06 owner.
# Outputs: Assertions and temporary DuckDBs; no scientific experiment execution.
# Run from: PYTHONPATH=src/python .venv/bin/python -m unittest tests.test_table_configuration
# ==============================================================================
"""Bounded config/coordination tests; synthetic fixtures do not prove reproduction."""

import tempfile
import json
import unittest
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import duckdb

from util.shared_configuration import (
    ExperimentConfigurationError, load_experiment_configuration,
    resolve_experiment_configuration,
)
from util.shared_database import initialize_experiment_database, load_database_configuration
from util.shared_experiment_execution import ExperimentCoordinator, expected_task_counts
from util.shared_execution_profiles import ExecutionSettings, resolve_execution_profile
from util.shared_process_storage import ProcessStorage

# Test fixture: fresh document, never an accepted research database.
CONFIG = Path(__file__).resolve().parents[3] / "config/experiments/poc2_m4_daily_100_paper_tables.json"


class TableConfigurationTests(unittest.TestCase):
    """Exercise configuration authority and mixed-branch integration without providers."""

    def test_science_and_operational_identity(self):
        """All table choices enter science; operational batches/topology do not."""
        config = load_experiment_configuration(CONFIG)
        changed = deepcopy(config.original)
        changed["execution"]["default"]["batch_sizes"]["table_sensitivity"] = 7
        changed["execution"]["default"]["dask_max_in_flight"] = 2
        self.assertEqual(config.scientific_hash, resolve_experiment_configuration(changed).scientific_hash)
        changed = deepcopy(config.original)
        changed["evaluation"]["table_reproduction"]["directional_report_models"].remove("m4_smyl_oracle")
        self.assertNotEqual(config.scientific_hash, resolve_experiment_configuration(changed).scientific_hash)
        changed["evaluation"]["table_reproduction"]["grid"]["lambda_up"]["stop"] = 1.125
        with self.assertRaises(ExperimentConfigurationError):
            resolve_experiment_configuration(changed)
        profile, _ = resolve_execution_profile("sequential_safe", {"table_sensitivity_batch_size": 7})
        self.assertEqual(profile.table_sensitivity_batch_size, 7)

    def test_figure_models_and_settings_require_explicit_approved_scope(self):
        """Figure subsets enter science; unsupported models and unpreserved CD settings fail."""
        config = load_experiment_configuration(CONFIG)
        changed = deepcopy(config.original)
        changed["evaluation"]["table_reproduction"]["figure_2"]["horizon_models"].reverse()
        self.assertNotEqual(config.scientific_hash, resolve_experiment_configuration(changed).scientific_hash)
        for model in ("naive2", "m4_smyl_mantis", "m4_smyl_oracle", "xgboost"):
            changed = deepcopy(config.original)
            changed["evaluation"]["table_reproduction"]["figure_2"]["horizon_models"].append(model)
            with self.assertRaisesRegex(ExperimentConfigurationError, "Figure 2"):
                resolve_experiment_configuration(changed)
        changed = deepcopy(config.original)
        changed["evaluation"]["table_reproduction"]["figure_2"]["cd_settings"]["alpha"] = .1
        with self.assertRaisesRegex(ExperimentConfigurationError, "historical scmamp"):
            resolve_experiment_configuration(changed)

    def test_storage_round_trip_and_scope(self):
        """Creation stores v12 unchanged; a future full Daily scope needs no code branch."""
        config = load_experiment_configuration(CONFIG)
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "config.duckdb"
            initialize_experiment_database(database, CONFIG)
            stored = load_database_configuration(database)
            self.assertEqual(stored.original, config.original)
            self.assertEqual(stored.scientific_hash, config.scientific_hash)
        changed = deepcopy(config.original)
        full = load_experiment_configuration(CONFIG.with_name("poc2_m4_daily_full_paper_tables.json"))
        self.assertEqual(full.series_count, 4227)
        self.assertEqual(expected_task_counts(100, config.workflow),
                         {2: 100, 3: 100, 4: 2830, 5: 1500, 6: 46})

    def test_complete_pool_and_resolved_counts(self):
        """Reuse approved model contracts and count each mixed branch explicitly."""
        approved = load_experiment_configuration(CONFIG.with_name(
            "poc2_m4_daily_100_forecast_contract_m4_comb.json"))
        for filename, count in ((CONFIG.name, 100), ("poc2_m4_daily_full_paper_tables.json", 4227)):
            config = load_experiment_configuration(CONFIG.with_name(filename))
            self.assertEqual(config.evaluation_options, approved.evaluation_options)
            self.assertEqual(len(config.original["models"]), 16)
            for model, settings in approved.original["models"].items():
                self.assertEqual(config.original["models"][model], settings)
            self.assertEqual(config.original["pipeline"]["combination"], approved.original["pipeline"]["combination"])
            derived = config.resolved["derived"]
            self.assertEqual(derived["expected_task_counts"], {
                "1": count, "2": count, "3": count, "4": count * 28 + 30,
                "5": count * 15, "6": 46})
            self.assertEqual(derived["expected_forecast_rows"], count * 15)
            self.assertEqual(derived["expected_directional_prediction_rows"], count * 28)
            self.assertEqual(derived["expected_evaluation_rows"], 45)
            self.assertEqual(derived["expected_directional_training_tasks"], 15)
            self.assertEqual(derived["expected_directional_prediction_tasks"], count * 14 + 14)
            self.assertEqual(derived["expected_gift_evaluation_tasks"], 17)
            self.assertEqual(derived["expected_paper_table_tasks"], 1)
        changed = deepcopy(config.original)
        changed["data"]["selection"].pop("expected_source_total")
        with self.assertRaises(ExperimentConfigurationError):
            resolve_experiment_configuration(changed)
        for model in approved.original["models"]:
            changed = deepcopy(config.original)
            changed["models"].pop(model)
            with self.assertRaises(ExperimentConfigurationError):
                resolve_experiment_configuration(changed)
        changed = deepcopy(config.original)
        changed["models"]["auto_arima"]["settings"]["stepwise"] = True
        with self.assertRaises(ExperimentConfigurationError):
            resolve_experiment_configuration(changed)
        changed = deepcopy(config.original)
        changed["pipeline"]["combination"] = {"method": "none", "weights": {}}
        with self.assertRaises(ExperimentConfigurationError):
            resolve_experiment_configuration(changed)

    def test_full_source_count_guard_checks_unsliced_arrow_rows(self):
        """A shortened or enlarged source fails even when a bounded reader could succeed."""
        import pyarrow as pa
        import pyarrow.ipc as ipc
        from util.p01_03_gift_eval_source import ConfiguredGiftEvalSource, SOURCE_ARROW_NAME
        from util.shared_configuration import ImportValidationError

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "daily").mkdir()
            configuration = SimpleNamespace(source_directory=Path("."),
                resolved={"data": {"dataset_name": "daily", "selection": {"expected_source_total": 3}}},
                import_settings={"benchmark": {"frequency": "D"}, "max_series": 3})
            source = ConfiguredGiftEvalSource(configuration, root)
            for count in (2, 3, 4):
                table = pa.table({"value": list(range(count))})
                with pa.OSFile(str(source.path / SOURCE_ARROW_NAME), "wb") as stream:
                    with ipc.new_stream(stream, table.schema) as writer:
                        writer.write_table(table)
                with patch("util.p01_03_gift_eval_source.iter_source_series", return_value=iter(())) as reader:
                    if count == 3:
                        self.assertEqual(list(source.records()), [])
                        reader.assert_called_once_with(source.path, "D", 3)
                    else:
                        with self.assertRaises(ImportValidationError):
                            source.records()
                        reader.assert_not_called()

    def test_plan_preserves_directional_tasks_and_adds_point_tasks(self):
        """The real planner adds two point tasks per instance and one paper-table task."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "plan.duckdb"
            initialize_experiment_database(database, CONFIG)
            with duckdb.connect(str(database)) as connection:
                connection.execute("""INSERT INTO datasets VALUES
                    ('dataset/test','m4_daily','fixture','revision','{}','{}',
                     'hash','D','{}',current_timestamp)""")

            def official(*args, **kwargs):
                """Return deterministic official-shaped histories, not downloaded inputs."""
                return {"configuration_name": "m4_daily/D/short", "dataset_name": "m4_daily",
                    "frequency": "D", "term": "short", "prediction_length": 14,
                    "window_count": 1, "domain": "Econ/Fin", "num_variates": 1,
                    "available_instances": 100, "r_period": 1,
                    "r_period_source": "fixture", "evaluation_seasonality": 1,
                    "gluonts_default_seasonality": 1,
                    "instances": [{"item_id": f"D{i+1}", "variate_id": "0",
                        "window_id": "short/000", "official_position": i,
                        "context": list(range(80)), "actual": list(range(80, 94)),
                        "start": "2000-01-01", "forecast_start": "2000-03-21"}
                        for i in range(100)]}

            with ExperimentCoordinator(database) as coordinator:
                with patch.object(coordinator, "_gift_bridge", side_effect=official):
                    plan = coordinator.plan()
                    coordinator.plan()
                counts = dict(coordinator.connection.execute(
                    "SELECT stage,count(*) FROM experiment_tasks GROUP BY stage").fetchall())
                self.assertEqual(counts, {2: 100, 3: 100, 4: 2830, 5: 1500, 6: 46})
                candidates = dict(coordinator.connection.execute(
                    "SELECT candidate,count(*) FROM experiment_tasks WHERE stage=5 GROUP BY candidate").fetchall())
                self.assertEqual(candidates, {**{model: 100 for model in
                    load_experiment_configuration(CONFIG).original["models"]
                    if not model.startswith("directional_")}, "m4_comb": 100})
                self.assertEqual(coordinator.connection.execute(
                    "SELECT count(*) FROM experiment_tasks WHERE stage=6 AND candidate='paper_tables'"
                ).fetchone()[0], 1)
                expected, _ = ProcessStorage._contracts(4)
                self.assertEqual(coordinator.connection.execute(f"SELECT count(*) FROM ({expected})").fetchone()[0], 1400)
                callback = Mock()
                with patch.dict("sys.modules", {"util.p06_01_table_flow": SimpleNamespace(run_table_evaluation=callback)}):
                    settings = ExecutionSettings(mode="sequential")
                    coordinator._run_06_evaluate(plan.experiment_id, [], {}, 1, settings)
                callback.assert_called_once_with(coordinator=coordinator,
                    experiment_id=plan.experiment_id, rows=[], attempts={}, workers=1, settings=settings)

    def test_mixed_process_03_retains_both_preparation_branches(self):
        """Directional preparation executes before ordinary full-history transformation."""
        config = load_experiment_configuration(CONFIG)
        coordinator = object.__new__(ExperimentCoordinator)
        coordinator.configuration = config
        coordinator._run_03_directional_preparation = Mock()
        coordinator.connection = Mock()
        values = list(range(80))
        coordinator.connection.execute.return_value.fetchone.side_effect = [
            ("robust", "standardise_sample_v1"), ("preprocessing", values)]
        rows = [("task", "instance", "variant", None)]
        with patch("util.shared_workflow_orchestration.run_gate_compute_flow", return_value=[]) as compute:
            coordinator._run_03_transform("experiment", rows, {"task": 1}, 1)
        coordinator._run_03_directional_preparation.assert_called_once()
        self.assertEqual(compute.call_args.kwargs["batches"][0][0]["values"], values)

    def test_process_06_validation_delegates_to_table_owner(self):
        """The ordinary ProcessStorage boundary invokes the new validator read-only."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "validation.duckdb"
            initialize_experiment_database(database, CONFIG)
            with duckdb.connect(str(database)) as connection:
                # Retain science for ordinary validation after table delegation.
                connection.execute("ALTER TABLE experiments RENAME TO original_experiments")
                connection.execute("CREATE TABLE experiments AS SELECT 'experiment/test' experiment_id, ? scientific_configuration",
                    [json.dumps(load_experiment_configuration(CONFIG).scientific_configuration)])
            storage = Mock()
            storage.validate.return_value = {"output_validated": True}
            constructor = Mock(return_value=storage)
            with patch.dict("sys.modules", {"util.p06_02_table_storage": SimpleNamespace(TableStorage=constructor)}), \
                 patch.object(ProcessStorage, "_validate_directional") as accepted:
                # The empty ordinary fixture is intentionally incomplete, but
                # table validation must still be invoked before that failure.
                with self.assertRaisesRegex(RuntimeError, "incomplete tasks"):
                    ProcessStorage(database).validate(6)
            self.assertEqual(accepted.call_args.args[1:], (6, 12))
            storage.validate.assert_called_once_with("experiment/test")

    def test_mixed_process_04_routes_only_point_rows_to_ordinary_flow(self):
        """Accepted directional providers run first; point work uses the existing flow."""
        coordinator = object.__new__(ExperimentCoordinator)
        coordinator.configuration = load_experiment_configuration(CONFIG)
        coordinator.config = coordinator.configuration.workflow
        coordinator.root = CONFIG.parents[2]
        coordinator.quantiles = tuple(coordinator.config["models"]["chronos_2"]["quantile_levels"])
        coordinator.connection = Mock()
        coordinator._run_04_directional_dtw = Mock()
        coordinator._run_04_directional_mantis_rf = Mock()
        coordinator._store_v11_dtw_predictions = Mock()
        rows = [("dtw", None, "variant", "directional_dtw:train"),
                ("mantis", None, "variant", "directional_mantis_rf:train:h01"),
                ("naive", "instance", "variant", "naive2"),
                ("chronos", "instance", "variant", "chronos_2")]
        profile, _ = resolve_execution_profile("sequential_safe")
        with patch("util.p04_01_forecast_flow.run_ordinary_forecast_flow") as compute, \
             patch("util.p04_02_forecast_provider.LocalAutoArimaProvider.from_configuration"), \
             patch("util.p04_02_forecast_provider.LocalChronosProvider"):
            coordinator._run_04_forecast("experiment", rows, {}, profile, "cpu")
        coordinator._run_04_directional_dtw.assert_called_once()
        coordinator._run_04_directional_mantis_rf.assert_called_once()
        self.assertEqual(compute.call_args.kwargs["rows"], rows[2:])
