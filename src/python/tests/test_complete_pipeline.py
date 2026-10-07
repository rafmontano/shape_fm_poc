"""Bounded complete-scope planning and evaluator routing; no scientific execution."""

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from tests import test_experiment_execution as fixtures
from util.shared_database import initialize_experiment_database
from util.shared_experiment_execution import ExperimentCoordinator, expected_task_counts
from util.shared_process_storage import ProcessStorage


class CompletePipelineTests(unittest.TestCase):
    """Exercise the existing coordinator, with external compute boundaries mocked."""

    def test_complete_plan_contains_every_ordinary_and_combination_candidate(self):
        root = Path(__file__).resolve().parents[3]
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "experiment.duckdb"
            initialize_experiment_database(database, root / "config/experiments/poc2_m4_daily_100_paper_tables.json")
            with ExperimentCoordinator(database) as coordinator:
                coordinator.connection.execute("""INSERT INTO datasets
                    (dataset_id,dataset_name,source_system,source_revision,source_file_hashes,
                     import_configuration,import_configuration_hash,frequency)
                    VALUES ('dataset','m4_daily','test','revision','{}','{}','configuration','D')""")
                def bridge(*args):
                    description = fixtures.ConfiguredPlanningTests._description(int(args[args.index('--limit') + 1]))
                    description.update(r_period=1, r_period_source="daily_nonseasonal_policy")
                    return description
                coordinator._gift_bridge = bridge
                ordinary = {m for m in coordinator.config["models"] if not m.startswith("directional_")}
                self.assertEqual(len(ordinary), 14)
                self.assertEqual(coordinator.config["combination"]["method"], "m4_comb")
                dry = coordinator.plan(dry_run=True)
                self.assertEqual(dry["official_evaluation_rows"], 17)
                self.assertEqual(expected_task_counts(100, coordinator.config),
                                 {2: 100, 3: 100, 4: 2830, 5: 1500, 6: 46})
                plan = coordinator.plan()
                self.assertEqual(plan.task_counts, {2: 100, 3: 100, 4: 2830, 5: 1500, 6: 46})
                for stage, candidates in ((4, ordinary), (5, ordinary | {"m4_comb"})):
                    counts = dict(coordinator.connection.execute(
                        "SELECT candidate,count(*) FROM experiment_tasks WHERE stage=? AND NOT starts_with(candidate,'directional_') GROUP BY candidate", [stage]).fetchall())
                    self.assertEqual(counts, {m: 100 for m in candidates})
                official = {r[0] for r in coordinator.connection.execute(
                    "SELECT candidate FROM experiment_tasks WHERE stage=6 AND candidate!='paper_tables' AND NOT starts_with(candidate,'directional_')").fetchall()}
                self.assertEqual(official, ordinary | {"m4_comb", "m4_smyl", "m4_fforma"})
                forecast_rows = [(m, "instance", "variant", m) for m in sorted(ordinary)]
                with patch.object(coordinator, "_store_v11_dtw_predictions"), patch(
                    "util.p04_02_forecast_provider.LocalAutoArimaProvider.from_configuration"), patch(
                    "util.p04_02_forecast_provider.LocalChronosProvider"), patch(
                    "util.p04_01_forecast_flow.run_ordinary_forecast_flow") as forecast_flow:
                    coordinator._run_04_forecast(
                        plan.experiment_id, forecast_rows, {},
                        SimpleNamespace(autoarima_workers=1, chronos_inference_batch_size=1,
                                        cpu_gpu_overlap=False), "cpu")
                self.assertEqual(forecast_flow.call_args.kwargs["rows"], forecast_rows)
                coordinator.connection.execute("UPDATE experiment_tasks SET status='completed'")
            with patch.object(ProcessStorage, "_validate_directional"), patch(
                "util.p06_02_table_storage.TableStorage.validate", return_value={"output_validated": True}):
                with self.assertRaisesRegex(RuntimeError, "missing stored output"):
                    ProcessStorage(database).validate(6)

    def test_table_dispatch_does_not_swallow_official_or_archive_scoring(self):
        coordinator = object.__new__(ExperimentCoordinator)
        coordinator.root = Path("/unused")
        coordinator.configuration = SimpleNamespace(
            version=12, source_directory="source", evaluation_options={},
            resolved={"data": {"dataset_name": "m4_daily", "benchmark": {"term": "short"}},
                      "evaluation": {"gift_eval": {"environment": "environment"}}},
            execution={"worker_timeouts_seconds": {"gift_eval": 1}, "dask_retries": 0})
        coordinator.quantiles = [0.5]
        coordinator.connection = Mock()
        def query(sql, parameters):
            if "SELECT benchmark_configuration_id" in sql:
                return SimpleNamespace(fetchone=lambda: ("benchmark",))
            if "SELECT metadata" in sql:
                return SimpleNamespace(fetchone=lambda: (json.dumps({"evaluation_seasonality": 1}),))
            if "count(DISTINCT" in sql:
                return SimpleNamespace(fetchone=lambda: (1,))
            return SimpleNamespace(fetchall=lambda: [("instance", 0, [4.0] * 14, None, "hash", "mean_only")])
        coordinator.connection.execute.side_effect = query
        rows = [(name, None, "official_reference" if name.startswith("m4_") and name != "m4_comb" else "variant", name)
                for name in ("paper_tables", "directional_dtw:h01", "directional_mantis_rf:h01",
                             "ses", "m4_comb", "m4_smyl", "m4_fforma")]
        with patch("util.p06_01_table_flow.run_table_evaluation") as tables, patch(
            "util.shared_workflow_orchestration.run_gate_compute_flow", return_value=[]) as compute:
            coordinator._run_06_evaluate("experiment", rows, {}, 1)
        self.assertEqual([r[3] for r in tables.call_args.kwargs["rows"]],
                         ["paper_tables", "directional_dtw:h01", "directional_mantis_rf:h01"])
        batches = compute.call_args.kwargs["batches"]
        self.assertEqual([b[0]["candidate"] for b in batches], ["ses", "m4_comb", "m4_smyl", "m4_fforma"])
        for batch in batches:
            self.assertEqual(batch[0]["payload"]["evaluation_profile"], "mean_based_v1")
            self.assertEqual(batch[0]["payload"]["forecasts"][0]["mean"], [4.0] * 14)
