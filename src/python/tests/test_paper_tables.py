# ==============================================================================
# test_paper_tables.py
# Purpose: Verify bounded Process 06 science, real Prefect/Dask parity and persistence.
# Inputs: Two asymmetric synthetic series, isolated DuckDB and local CPU workers.
# Outputs: Assertions; temporary operational/test state only, no benchmark execution.
# Run from: PYTHONPATH=src/python .venv/bin/python -m unittest tests.test_paper_tables
# ==============================================================================
"""Synthetic tests establish implementation behaviour, never paper reproduction."""

import json
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

import duckdb
from distributed import Client, LocalCluster
from prefect.testing.utilities import prefect_test_harness

from util.shared_configuration import canonical_json, json_fingerprint, load_experiment_configuration
from util.shared_database import migrate_database
from util.shared_execution_profiles import ExecutionSettings
from util.shared_experiment_execution import ExperimentCoordinator
from util.p06_01_table_flow import TableEvaluation, evaluate_table_batch, install_table_inputs, run_table_evaluation
from util.p06_02_table_storage import TableStorage
from util.p06_03_table_reports import TableReports, M4_FREQUENCIES, POINT_MODELS
from util.shared_workflow_orchestration import run_gate_compute_flow

# Test fixture: centrally approved v12 science; no model execution or source download.
ROOT = Path(__file__).resolve().parents[3]
CONFIG = load_experiment_configuration(ROOT / "config/experiments/poc2_m4_daily_100_paper_tables.json")
SCIENCE = CONFIG.table_reproduction


def input_fixture():
    """Return two asymmetric original-scale cohorts with both disagreement directions."""
    rows = []
    for sid, origin, actual, smyl, mantis in (
        ("D1", 10, [12, 9, 13, 8] * 3 + [15, 11], [11, 8, 12, 9] * 3 + [13, 9], 1),
        ("D2", 20, [17, 22, 18, 23] * 3 + [16, 19], [18, 21, 19, 22] * 3 + [17, 21], 0),
    ):
        means = {"naive2": [origin] * 14, "m4_smyl": smyl,
                 "m4_fforma": [v + 0.25 for v in smyl],
                 "chronos_2": [v - 0.5 for v in smyl]}
        directions = {"directional_mantis_rf": [mantis] * 14,
                      "directional_dtw": [1 - mantis] * 14}
        rows.append({"series_id": sid, "forecast_instance_id": f"instance/{sid}",
                     "context": list(range(origin - 9, origin + 1)), "actual": actual,
                     "means": means, "directions": directions,
                     "sources": {m: {"id": f"source/{sid}/{m}", "hash": json_fingerprint(v)}
                                 for m, v in {**means, **directions}.items()}})
    return {"frequency": "Daily", "series": rows}


def install_fixture(inputs, client=None):
    """Install only the read-only worker inputs, once per cohort/worker."""
    payload = {"frequency": inputs["frequency"], "series": [
        {k: s[k] for k in ("series_id", "context", "actual", "means", "directions")}
        for s in inputs["series"]]}
    fingerprint = json_fingerprint(payload)
    install_table_inputs(fingerprint, payload)
    if client:
        client.run(install_table_inputs, fingerprint, payload)
    return {"paper_tables": True, "dataset_fingerprint": fingerprint, "timeout": 60}


class PaperTableTests(unittest.TestCase):
    """Check divergent interpretations and the real selected-result storage boundary."""

    def setUp(self):
        """Keep focused worker-cache inputs in automatically removed test storage."""
        cache = tempfile.TemporaryDirectory()
        self.addCleanup(cache.cleanup)
        boundary = patch("util.shared_distributed_execution._immutable_cache_directory",
                         return_value=Path(cache.name))
        boundary.start()
        self.addCleanup(boundary.stop)

    def test_grid_batches_and_dependency_invalidation(self):
        """Exact surface identities are unique; changes invalidate only dependent science."""
        inputs = input_fixture()
        evaluation = TableEvaluation("experiment/test", inputs, SCIENCE)
        jobs = evaluation.jobs()
        self.assertEqual(len(jobs), 1057)
        self.assertEqual(len({evaluation.identity(j)["candidate_id"] for j in jobs}), 1057)
        surface = [j for j in jobs if j["lambda_up"] is not None]
        self.assertEqual(len(evaluation.batches(surface, 25)), 42)
        self.assertEqual({j["lambda_up"] for j in surface}, {round(1 + i * .005, 3) for i in range(25)})
        self.assertEqual({j["lambda_down"] for j in surface}, {round(1 - i * .005, 3) for i in range(21)})
        changed = deepcopy(inputs)
        changed["series"][0]["directions"]["directional_mantis_rf"][0] = 0
        other = TableEvaluation("experiment/test", changed, SCIENCE)
        for job in jobs:
            dependent = job["lambda_up"] is not None or job["model"] == "directional_mantis_rf"
            self.assertEqual(evaluation.identity(job) == other.identity(job), not dependent)
        changed = deepcopy(inputs)
        changed["series"][0]["means"]["naive2"][0] += 1
        other = TableEvaluation("experiment/test", changed, SCIENCE)
        changed_grid = deepcopy(SCIENCE)
        changed_grid["grid"]["version"] = "synthetic/changed-grid"
        grid_evaluation = TableEvaluation("experiment/test", inputs, changed_grid)
        for job in jobs:
            self.assertEqual(evaluation.identity(job) == other.identity(job),
                             job["model"].startswith("directional_"))
            self.assertEqual(evaluation.identity(job) == grid_evaluation.identity(job),
                             job["lambda_up"] is None)

    def test_all_selection_tie_levels(self):
        """No hardcoded historical pair or coarse/tolerance tie can pass these orderings."""
        def row(owa, up, down):
            """Return one selection-only candidate, independent of implementation output."""
            return {"metrics": {"owa": owa}, "lambda_up": up, "lambda_down": down}
        cases = [
            ([row(.9, 1.12, .9), row(1, 1, 1)], (.9, 1.12, .9)),
            ([row(1, 1.1, .9), row(1, 1.01, 1)], (1, 1.01, 1)),
            ([row(1, 1.125, 1), row(1, 1, .875)], (1, 1, .875)),
            # Symmetric upward departures force stable lambda ordering.
            ([row(1, 1.125, .875), row(1, .875, .875)], (1, .875, .875)),
            ([row(1, 1.125, 1.125), row(1, 1.125, .875)], (1, 1.125, .875)),
        ]
        for rows, expected in cases:
            result = TableReports.select(rows[::-1])
            self.assertEqual((result["metrics"]["owa"], result["lambda_up"], result["lambda_down"]), expected)

    def test_ranks_ties_incomplete_and_duplicate_models(self):
        """Higher accuracy is better; average ties and missing-model reports are explicit."""
        rows = []
        for h, scores in ((1, (4, 4, 2)), (2, (2, 4, 2))):
            for model, correct in zip(("a", "b", "c"), scores):
                rows.append({"frequency": "Daily", "model": model, "horizon": h,
                             "correct_count": correct, "evaluation_count": 4,
                             "accuracy": correct / 4, "result_fingerprint": f"{model}/{h}/{correct}"})
        result = TableReports.directional_profile(rows, ["a", "b", "c"], {"Daily": [1, 2]})
        self.assertEqual(result["mean_ranks"], {"a": 2, "b": 1.25, "c": 2.75})
        incomplete = TableReports.directional_profile(rows[:-1], ["a", "b", "c"], {"Daily": [1, 2]})
        self.assertIsNone(incomplete["mean_ranks"])
        self.assertEqual(incomplete["incomplete_models"], {"c": [("Daily", 2)]})
        self.assertNotEqual(incomplete["matrix_fingerprint"], result["matrix_fingerprint"])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            TableReports.directional_profile(rows + rows[:1], ["a", "b", "c"], {"Daily": [1, 2]})

    def test_complete_all_recalculates_owa_and_no_daily_all(self):
        """Weighted components, not weighted OWAs, establish the synthetic POC3 boundary."""
        rows = []
        for i, frequency in enumerate(M4_FREQUENCIES, 1):
            for model, factor in (("naive2", 1), ("m4_smyl", 2)):
                rows.append({"frequency": frequency, "model": model, "evaluation_count": i,
                             "metrics": {"smape": i * factor, "mase": (7 - i) / factor,
                                         "da": i / 10, "owa": 99}})
        with self.assertRaisesRegex(ValueError, "complete"):
            TableReports.aggregate_all([r for r in rows if r["frequency"] == "Daily"])
        aggregate = TableReports.aggregate_all(rows)
        smyl = next(r for r in aggregate if r["model"] == "m4_smyl")
        self.assertEqual(smyl["metrics"]["owa"], 1.25)
        self.assertEqual(smyl["metrics"]["smape"], 2 * 91 / 21)
        self.assertEqual(smyl["metrics"]["mase"], 56 / 42)
        self.assertAlmostEqual(smyl["metrics"]["da"], 91 / 210)
        with self.assertRaisesRegex(ValueError, "duplicate"):
            TableReports.aggregate_all(rows + rows[:1])

    def test_multifrequency_directional_retrieval(self):
        """Figure 5-shaped stored profiles require no classifier or POC3 data execution."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profiles.duckdb"
            migrate_database(path)
            with duckdb.connect(str(path)) as c:
                storage = TableStorage(c)
                for frequency, horizon in zip(M4_FREQUENCIES, (48, 14, 13, 18, 8, 6)):
                    storage.save_directional({"experiment_id": "synthetic/profiles", "frequency": frequency,
                        "correct_counts": [1] * horizon, "evaluation_count": 2,
                        "input_fingerprint": "synthetic/input"}, "synthetic/model")
                rows = storage.directional_rows("synthetic/profiles")
                self.assertEqual(len(rows), 107)
                self.assertEqual({r["frequency"] for r in rows}, set(M4_FREQUENCIES))
                self.assertTrue(all(r["accuracy"] == .5 for r in rows))

    def test_canonical_storage_input_contract(self):
        """SQL source joins use original means/raw contexts and every stored prediction horizon."""
        with duckdb.connect(":memory:") as c:
            # Only columns projected by this read-only boundary are needed here.
            for sql in (
                "CREATE TABLE experiments(experiment_id VARCHAR,benchmark_configuration_id VARCHAR)",
                "CREATE TABLE forecast_instances(forecast_instance_id VARCHAR,series_id VARCHAR,dataset_id VARCHAR,context_target DOUBLE[],actual_target DOUBLE[],horizon INTEGER,benchmark_configuration_id VARCHAR,official_position INTEGER)",
                "CREATE TABLE directional_model_definitions(model_definition_id VARCHAR,experiment_id VARCHAR)",
                "CREATE TABLE directional_composite_model_definitions(model_definition_id VARCHAR,experiment_id VARCHAR)",
                "CREATE TABLE reference_forecasts(reference_forecast_id VARCHAR,mean DOUBLE[],content_hash VARCHAR,dataset_id VARCHAR,series_id VARCHAR,forecast_id VARCHAR)",
                "CREATE TABLE forecasts(forecast_id VARCHAR,mean DOUBLE[],content_hash VARCHAR,experiment_id VARCHAR,forecast_instance_id VARCHAR,candidate VARCHAR,scale VARCHAR)",
                "CREATE TABLE directional_evaluation_inputs(evaluation_input_id VARCHAR,forecast_instance_id VARCHAR)",
                "CREATE TABLE model_directional_predictions(horizon INTEGER,prediction INTEGER,content_hash VARCHAR,experiment_id VARCHAR,model_definition_id VARCHAR,evaluation_input_id VARCHAR)",
            ):
                c.execute(sql)
            c.execute("INSERT INTO experiments VALUES ('e','benchmark')")
            c.execute("INSERT INTO directional_model_definitions VALUES ('dtw','e')")
            c.execute("INSERT INTO directional_composite_model_definitions VALUES ('mantis','e')")
            fixture = input_fixture()
            for position, s in enumerate(fixture["series"]):
                c.execute("INSERT INTO forecast_instances VALUES (?,?,?,?,?,14,'benchmark',?)",
                          [s["forecast_instance_id"], s["series_id"], "dataset", s["context"], s["actual"], position])
                c.execute("INSERT INTO directional_evaluation_inputs VALUES (?,?)",
                          [s["series_id"], s["forecast_instance_id"]])
                for model, mean in s["means"].items():
                    if model.startswith("m4_"):
                        c.execute("INSERT INTO reference_forecasts VALUES (?,?,'hash','dataset',?,?)",
                                  [model + s["series_id"], mean, s["series_id"], model])
                    else:
                        c.execute("INSERT INTO forecasts VALUES (?,?,'hash','e',?,?,'original')",
                                  [model + s["series_id"], mean, s["forecast_instance_id"], model])
                for model, definition in (("directional_dtw", "dtw"), ("directional_mantis_rf", "mantis")):
                    for horizon, prediction in enumerate(s["directions"][model], 1):
                        c.execute("INSERT INTO model_directional_predictions VALUES (?,?,'hash','e',?,?)",
                                  [horizon, prediction, definition, s["series_id"]])
            config = deepcopy(CONFIG)
            config.resolved["data"]["selection"]["count"] = 2
            loaded = TableStorage(c).load_inputs("e", config)
            self.assertEqual(loaded["frequency"], "Daily")
            for expected, observed in zip(fixture["series"], loaded["series"], strict=True):
                for field in ("context", "actual", "means", "directions"):
                    self.assertEqual(observed[field], expected[field])
            c.execute("DELETE FROM model_directional_predictions WHERE model_definition_id='mantis' AND horizon=14")
            with self.assertRaisesRegex(RuntimeError, "directions"):
                TableStorage(c).load_inputs("e", config)

    def test_sequential_local_dask_real_kernel_parity(self):
        """Full 1057-result parity traverses the actual Prefect CPU task/R adapter on local Dask."""
        inputs = input_fixture()
        evaluation = TableEvaluation("experiment/test", inputs, SCIENCE)
        jobs = evaluation.jobs()
        options = install_fixture(inputs)
        # One sequential call uses the identical kernel, not another evaluator.
        sequential = evaluate_table_batch(jobs, options)
        expected = {evaluation.record(r, j)["candidate_id"]: evaluation.record(r, j)
                    for r, j in zip(sequential["results"], jobs, strict=True)}
        with prefect_test_harness(), LocalCluster(n_workers=2, threads_per_worker=1, processes=False,
                resources={"CPU": 1}, dashboard_address=None) as cluster, Client(cluster) as client:
            install_fixture(inputs, client)
            observed = {}
            outcomes = run_gate_compute_flow(process_id=6, batches=evaluation.batches(jobs, 25),
                options=options, scheduler_address=client.scheduler.address, retries=0,
                max_in_flight=2, local_workers=1)
            for outcome in outcomes:
                self.assertNotIn("error", outcome)
                response = outcome["response"]
                self.assertEqual(response["worker"]["execution_backend"], "Dask Distributed")
                self.assertEqual(response["worker"]["resources"]["CPU"], 1)
                self.assertNotIn("CHRONOS_GPU_SLOT", response["worker"]["resources"])
                for r, j in zip(response["results"], outcome["batch"], strict=True):
                    record = evaluation.record(r, j)
                    self.assertNotIn(record["candidate_id"], observed)
                    observed[record["candidate_id"]] = record
        self.assertEqual(observed, expected)
        self.assertEqual(evaluation.select(list(observed.values())), evaluation.select(list(expected.values())))
        changed_host = deepcopy(sequential["results"][0])
        changed_host["worker"] = {"hostname": "another host"}
        changed_host["runtime_seconds"] = 987
        self.assertEqual(evaluation.record(changed_host, jobs[0]), evaluation.record(sequential["results"][0], jobs[0]))

    def test_storage_restart_reconciliation_and_transaction(self):
        """Actual DuckDB schema/Process06 completion retains only selected means and reuses aggregates."""
        inputs = input_fixture()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "paper.duckdb"
            migrate_database(path)
            migrate_database(path)
            c = duckdb.connect(str(path))
            config = deepcopy(CONFIG)
            config.resolved["data"]["selection"]["count"] = 2
            c.execute("""INSERT INTO experiment_configuration
                (configuration_key,configuration_version,experiment_name,experiment_date,
                 experiment_description,reproducibility_seed,original_configuration,
                 resolved_configuration,scientific_hash,configuration_integrity_hash)
                VALUES ('experiment',12,'fixture','2026-10-07','fixture',1234,?,?,?,?)""",
                [canonical_json(config.original), canonical_json(config.resolved), config.scientific_hash,
                 config.configuration_integrity_hash])
            coordinator = object.__new__(ExperimentCoordinator)
            coordinator.connection = c
            coordinator.configuration = config
            coordinator._active_dask_client = None
            coordinator.table_sensitivity_batch_size = 25
            # Minimal task source uses the real completion writer/schema.
            for name in ["paper_tables"]:
                c.execute("""INSERT INTO experiment_tasks
                  (task_id,experiment_id,stage,candidate,status) VALUES (?,?,6,?,'running')""",
                  [name, "experiment/test", name])
            rows = [("paper_tables", None, None, "paper_tables")]
            storage = TableStorage(c)
            with patch.object(TableStorage, "load_inputs", return_value=inputs), prefect_test_harness():
                save = storage.save_batch

                def interrupted(store, records, provenance):
                    """Simulate an interruption after a durable completion-order batch."""
                    save(records, provenance)
                    if any(r["lambda_up"] is not None for r in records):
                        raise RuntimeError("synthetic interruption")

                with patch.object(TableStorage, "save_batch", interrupted), self.assertRaisesRegex(
                        RuntimeError, "synthetic interruption"):
                    run_table_evaluation(coordinator=coordinator, experiment_id="experiment/test", rows=rows,
                        attempts={"paper_tables": 1}, workers=1, settings=ExecutionSettings(mode="sequential"))
                completed = {r[0] for r in c.execute("SELECT candidate_id FROM paper_metric_candidates").fetchall()}
                self.assertTrue(completed)
                self.assertGreater(c.execute("SELECT count(*) FROM paper_metric_candidates WHERE lambda_up IS NOT NULL").fetchone()[0], 0)
                submitted = []

                def resumed(**kwargs):
                    """Inspect real runner submissions, then execute the unchanged Prefect path."""
                    submitted.extend(j for batch in kwargs["batches"] for j in batch if not j["retain_means"])
                    return run_gate_compute_flow(**kwargs)

                with patch("util.shared_workflow_orchestration.run_gate_compute_flow", side_effect=resumed):
                    run_table_evaluation(coordinator=coordinator, experiment_id="experiment/test", rows=rows,
                        attempts={"paper_tables": 1}, workers=1, settings=ExecutionSettings(mode="sequential"))
                evaluation = TableEvaluation("experiment/test", inputs, SCIENCE)
                self.assertFalse(completed & {evaluation.identity(j)["candidate_id"] for j in submitted})
                self.assertEqual(len(submitted), 1057 - len(completed))
            # The complete validator also requires the accepted 28 task identities.
            for model in ("directional_dtw", "directional_mantis_rf"):
                for h in range(1, 15):
                    name = f"{model}:h{h:02d}"
                    c.execute("""INSERT INTO experiment_tasks
                        (task_id,experiment_id,stage,candidate,status) VALUES (?,?,6,?,'completed')""",
                        [name, "experiment/test", name])
            with patch.object(TableStorage, "load_inputs", return_value=inputs):
                self.assertEqual(storage.validate("experiment/test"), {
                    "output_validated": True, "paper_candidates": 1057,
                    "selected_models": 9, "directional_rows": 126})
            records = [storage.candidate(evaluation.identity(j)["candidate_id"]) for j in evaluation.jobs()[:2]]
            batches_before = c.execute("SELECT count(*) FROM paper_execution_batches").fetchone()[0]
            invalid = {**records[1], "candidate_id": "invalid", "result_fingerprint": "wrong"}
            with self.assertRaisesRegex(RuntimeError, "fingerprint"):
                storage.save_batch([records[0], invalid], {})
            self.assertEqual(c.execute("SELECT count(*) FROM paper_execution_batches").fetchone()[0], batches_before)
            report = storage.summary("experiment/test")["report"]
            self.assertEqual(len(report["Table1"]), 7)
            self.assertEqual([len(report["Table2"][p]) for p in "ABC"], [8, 7, 7])
            for row in report["Table1"]:
                for panel, expected in (("A", row["metrics"]["da"]), ("B", row["metrics"]["owa"]),
                                         ("C", row["improvement_vs_smyl_pct"]["owa"])):
                    self.assertEqual(next(cell["value"] for cell in report["Table2"][panel]
                                          if cell["model"] == row["model"]), expected)
            self.assertEqual(c.execute("SELECT count(*) FROM paper_metric_candidates").fetchone()[0], 1057)
            self.assertEqual(c.execute("SELECT count(*) FROM paper_diagnostic_means").fetchone()[0], 6)
            self.assertEqual(c.execute("SELECT count(*) FROM paper_directional_results").fetchone()[0], 126)
            self.assertEqual(c.execute("SELECT count(*) FROM forecasts").fetchone()[0], 0)
            diagnostic = storage.diagnostic_mean("experiment/test", "m4_smyl_oracle", "instance/D1")
            self.assertTrue(diagnostic["ex_post"])
            self.assertEqual(diagnostic["metadata"]["oracle"]["method_id"], "smyl_scalar_smape_oracle_v1")
            with patch.object(TableStorage, "load_inputs", return_value=inputs), patch(
                "util.shared_workflow_orchestration.run_gate_compute_flow", side_effect=AssertionError("restart computed")):
                run_table_evaluation(coordinator=coordinator, experiment_id="experiment/test", rows=[],
                                     attempts={}, workers=1)
            evaluation = TableEvaluation("experiment/test", inputs, SCIENCE)
            self.assertTrue(all(storage.candidate(evaluation.identity(j)["candidate_id"]) for j in evaluation.jobs()))
            row = c.execute("SELECT candidate_id FROM paper_metric_candidates LIMIT 1").fetchone()[0]
            c.execute("UPDATE paper_metric_candidates SET result_fingerprint='tampered' WHERE candidate_id=?", [row])
            with self.assertRaisesRegex(RuntimeError, "fingerprint"):
                storage.candidate(row)
            c.close()
