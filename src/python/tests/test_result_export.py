# ==============================================================================
# test_result_export.py
# Purpose: Prove read-only Process 06 export parity, rendering, safety and regeneration.
# Inputs: Temporary full-schema synthetic result store; existing locked R packages.
# Outputs: Test assertions and temporary derived figures; never accepted research data.
# Run from: PYTHONPATH=src/python .venv/bin/python -m unittest tests.test_result_export
# ==============================================================================
"""Synthetic stored-result fixtures test reporting, not historical Figure 2 reproduction."""

import csv
import hashlib
import io
import json
import shutil
import tempfile
import unittest
from copy import deepcopy
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import duckdb

from tests.test_main import MAIN
from tests.test_paper_tables import CONFIG, input_fixture, install_fixture
from util.shared_configuration import canonical_json, json_fingerprint
from util.shared_database import initialize_experiment_database, load_database_configuration
from util.p06_01_table_flow import TableEvaluation, evaluate_table_batch
from util.p06_02_table_storage import TableStorage
from util.p06_04_result_export import ResultExport, DERIVED_FILES, ROOT


def insert(connection, table, row):
    """Insert explicit synthetic source records into the real schema with no fitting."""
    connection.execute(f"INSERT INTO {table} ({','.join(row)}) VALUES ({','.join('?' for _ in row)})",
                       list(row.values()))


def stored_fixture(directory):
    """Create canonical sources and complete Process 06 science once; exports never recalculate it."""
    original = deepcopy(CONFIG.original)
    original["data"]["selection"]["count"] = 2
    original["experiment"]["name"] = "fixture"
    original["execution"]["model_storage"]["experiment"] = "fixture"
    config_path = directory / "configuration.json"
    config_path.write_text(canonical_json(original))
    database = directory / "fixture.duckdb"
    initialize_experiment_database(database, config_path)
    configuration = load_database_configuration(database)
    inputs = input_fixture()
    with duckdb.connect(str(database)) as c:
        insert(c, "experiments", {"experiment_id": "synthetic/export", "benchmark_configuration_id": "benchmark",
            "dataset_id": "dataset", "name": configuration.name, "description": configuration.description,
            "scientific_configuration": canonical_json(configuration.resolved),
            "configuration_hash": configuration.scientific_hash, "scope": "synthetic", "status": "completed"})
        insert(c, "experiment_variants", {"variant_id": "variant", "experiment_id": "synthetic/export",
            "cleaning_method": "robust", "transformation_method": "standardise_sample_v1",
            "adjustment_method": "identity", "configuration": "{}"})
        insert(c, "directional_model_definitions", {"model_definition_id": "dtw", "experiment_id": "synthetic/export",
            "variant_id": "variant", "scientific_definition": "{}", "repository_revision": "fixture",
            "scientific_source_fingerprint": "fixture", "worker_file_fingerprint": "fixture",
            "classifier_lock_fingerprint": "fixture", "runtime_versions": "{}", "numeric_dtype": "float64",
            "reference_library_fingerprint": "fixture", "preparation_fingerprint": "fixture",
            "implementation_fingerprint": "fixture", "content_hash": "fixture"})
        insert(c, "directional_composite_model_definitions", {"model_definition_id": "mantis",
            "experiment_id": "synthetic/export", "variant_id": "variant", "representation_definition_id": "representation",
            "classifier_definition_id": "classifier", "scientific_definition": "{}",
            "preparation_fingerprint": "fixture", "membership_fingerprint": "fixture", "content_hash": "fixture"})
        for position, s in enumerate(inputs["series"]):
            instance, sid = s["forecast_instance_id"], s["series_id"]
            insert(c, "forecast_instances", {"forecast_instance_id": instance, "benchmark_configuration_id": "benchmark",
                "dataset_id": "dataset", "series_id": sid, "variate_id": "0", "window_id": "window",
                "official_position": position, "context_start": 0, "context_end": len(s["context"]),
                "actual_start": len(s["context"]), "actual_end": len(s["context"]) + 14, "horizon": 14,
                "context_target": s["context"], "actual_target": s["actual"], "identity_metadata": "{}"})
            insert(c, "directional_evaluation_inputs", {"evaluation_input_id": sid, "experiment_id": "synthetic/export",
                "variant_id": "variant", "forecast_instance_id": instance, "preparation_id": "fixture",
                "preparation_fingerprint": "fixture", "raw_input_hash": "fixture", "cleaned_input_hash": "fixture",
                "transformed_input_hash": "fixture", "transformed_input": s["context"],
                "label_reference": s["context"][-1], "preprocessing_provenance": "{}",
                "package_versions": "{}", "content_hash": "fixture"})
            for model, mean in s["means"].items():
                if model.startswith("m4_"):
                    insert(c, "reference_forecasts", {"reference_forecast_id": f"{sid}/{model}", "dataset_id": "dataset",
                        "series_id": sid, "official_m4_series_id": sid, "forecast_id": model, "submission_id": 1,
                        "submission_rank": 1, "submission_author": "synthetic", "horizon": 14, "mean": mean,
                        "point_semantics": "mean", "forecast_capability": "mean_only", "source_metadata": "{}",
                        "content_hash": json_fingerprint(mean)})
                else:
                    insert(c, "forecasts", {"forecast_id": f"{sid}/{model}", "experiment_id": "synthetic/export",
                        "variant_id": "variant", "forecast_instance_id": instance, "candidate": model,
                        "scale": "original", "mean": mean, "forecast_capability": "mean_only",
                        "execution_metadata": "{}", "content_hash": json_fingerprint(mean)})
            for model, definition in (("directional_dtw", "dtw"), ("directional_mantis_rf", "mantis")):
                for h, value in enumerate(s["directions"][model], 1):
                    insert(c, "model_directional_predictions", {"prediction_id": f"{sid}/{model}/{h}",
                        "experiment_id": "synthetic/export", "model_definition_id": definition,
                        "evaluation_input_id": sid, "horizon": h, "prediction": value,
                        "training_fingerprint": "fixture", "evaluation_fingerprint": "fixture",
                        "output_fingerprint": "fixture", "content_hash": json_fingerprint(value)})
        store = TableStorage(c)
        inputs = store.load_inputs("synthetic/export", configuration)
        evaluation = TableEvaluation("synthetic/export", inputs, configuration.table_reproduction)
        options = install_fixture(inputs)
        jobs = evaluation.jobs()
        response = evaluate_table_batch(jobs, options)
        records = [evaluation.record(r, j) for r, j in zip(response["results"], jobs, strict=True)]
        store.save_batch(records, {"source": "synthetic fixture only"})
        selected = evaluation.select(records)
        directions = []
        for model, record in selected.items():
            retained = []
            if model in ("m4_smyl_mantis", "chronos_2_mantis", "m4_smyl_oracle"):
                job = {"model": record["model"], "lambda_up": record["lambda_up"],
                       "lambda_down": record["lambda_down"], "retain_means": True}
                retained = evaluate_table_batch([job], options)["results"][0]["adjusted_means"]
            store.save_selected(record, model, retained, inputs["series"])
            directions.extend(store.save_directional(record, model))
        store.save_report("synthetic/export", json_fingerprint(inputs), evaluation.report(selected, directions))
        for model, definition in (("directional_dtw", "dtw"), ("directional_mantis_rf", "mantis")):
            for h, correct in enumerate(selected[model]["correct_counts"], 1):
                name = f"{model}:h{h:02d}"
                insert(c, "experiment_tasks", {"task_id": name, "experiment_id": "synthetic/export",
                    "stage": 6, "candidate": name, "status": "completed"})
                insert(c, "model_directional_evaluations", {"directional_evaluation_id": name,
                    "experiment_id": "synthetic/export", "model_definition_id": definition, "horizon": h,
                    "correct_count": correct, "evaluation_count": 2, "accuracy": correct / 2,
                    "prediction_fingerprint": "fixture", "content_hash": "fixture"})
        insert(c, "experiment_tasks", {"task_id": "paper", "experiment_id": "synthetic/export",
            "stage": 6, "candidate": "paper_tables", "status": "completed"})
        ordinary = [m for m in configuration.resolved["models"] if not m.startswith("directional_")]
        for model in [*ordinary, "m4_comb", "m4_smyl", "m4_fforma"]:
            variant = "official_reference" if model in ("m4_smyl", "m4_fforma") else "variant"
            insert(c, "experiment_tasks", {"task_id": model, "experiment_id": "synthetic/export",
                "variant_id": variant, "stage": 6, "candidate": model, "status": "completed"})
            insert(c, "official_evaluations", {"evaluation_id": model, "experiment_id": "synthetic/export",
                "variant_id": variant, "candidate": model, "benchmark_configuration_id": "benchmark",
                "evaluator": "synthetic", "evaluator_revision": "fixture", "options": "{}",
                "metrics": '{"MASE":1}', "evaluation_input_count": 2,
                "forecast_input_fingerprint": json_fingerprint(model),
                "is_complete_manifest": False, "is_submittable": False})
        c.execute("UPDATE experiment_processes SET status='completed' WHERE process_id=6")
    return database


class ResultExportTests(unittest.TestCase):
    """Real read-only SQL and real locked rendering, with scientific execution forbidden during export."""

    @classmethod
    def setUpClass(cls):
        """Seed one bounded scientific fixture; each test receives its own disposable database copy."""
        cls.fixture_directory = tempfile.TemporaryDirectory()
        with patch("util.shared_distributed_execution._immutable_cache_directory",
                   return_value=Path(cls.fixture_directory.name)):
            cls.fixture = stored_fixture(Path(cls.fixture_directory.name))

    @classmethod
    def tearDownClass(cls):
        """Remove the temporary source store after all read-only tests."""
        cls.fixture_directory.cleanup()

    def setUp(self):
        """Prepare isolated database and output paths without touching accepted data."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name).resolve() / "results"
        self.directory.mkdir()
        boundary = patch("util.p00_02_researcher_cli.RESULTS_ROOT", self.directory)
        boundary.start()
        self.addCleanup(boundary.stop)
        self.database = self.directory / "fixture.duckdb"
        shutil.copyfile(self.fixture, self.database)
        self.output = self.database.with_suffix("")

    def test_entry_export_parity_readonly_and_recreation(self):
        """Exact stored CSV/JSON parity and byte-stable regeneration traverse the actual entry action."""
        before = self.database.read_bytes()
        connect = duckdb.connect
        with connect(str(self.database), read_only=True) as c:
            snapshot = TableStorage(c).export_snapshot(self.database)
        self.output.mkdir()
        (self.output / "notes.txt").write_text("preserve me")
        with patch("util.p06_04_result_export.duckdb.connect", wraps=connect) as opened, \
             patch("util.p06_01_table_flow.evaluate_table_batch", side_effect=AssertionError("science repeated")), \
             patch("util.shared_workflow_orchestration.run_gate_compute_flow", side_effect=AssertionError("scheduled")), \
             redirect_stdout(io.StringIO()) as stdout:
            self.assertEqual(MAIN.main(["export", "--database", str(self.database)]), 0)
        self.assertFalse(json.loads(stdout.getvalue())["export"]["scientific_execution"])
        self.assertTrue(all(call.kwargs.get("read_only") is True for call in opened.call_args_list))
        self.assertEqual(before, self.database.read_bytes())
        self.assertEqual((self.output / "notes.txt").read_text(), "preserve me")
        for relative in DERIVED_FILES:
            self.assertGreater((self.output / relative).stat().st_size, 0)
        def read(name):
            """Read unrounded CSV values for independent parity comparisons."""
            with (self.output / "tables" / f"{name}.csv").open() as stream:
                return list(csv.DictReader(stream))
        for actual, expected in zip(read("table_1"), snapshot["report"]["Table1"], strict=True):
            self.assertEqual(actual["model"], expected["model"])
            for metric, value in expected["metrics"].items():
                self.assertEqual(float(actual[metric]), value)
            for metric, value in expected["improvement_vs_smyl_pct"].items():
                self.assertEqual(float(actual[f"{metric}_improvement_vs_smyl_pct"]), value)
            for key in ("frequency", "model", "candidate_id", "lambda_up", "lambda_down",
                        "evaluation_count", "input_fingerprint", "result_fingerprint"):
                self.assertEqual(actual[key], "" if expected[key] is None else str(expected[key]))
        expected2 = [{"panel": p, **r} for p, rows in snapshot["report"]["Table2"].items() for r in rows]
        for actual, expected in zip(read("table_2_daily"), expected2, strict=True):
            self.assertEqual({**actual, "value": float(actual["value"])}, expected)
        for actual, expected in zip(read("directional_accuracy_by_horizon"), snapshot["directional_rows"], strict=True):
            self.assertEqual(actual, {k: str(v) for k, v in expected.items()})
        matrix = read("cd_input_daily")
        models = snapshot["figure_settings"]["cd_models"]
        self.assertEqual(list(matrix[0]), ["dataset", *models])
        self.assertEqual([r["dataset"] for r in matrix], [f"Daily_{h}" for h in range(1, 15)])
        by_key = {(r["horizon"], r["model"]): r["accuracy"] for r in snapshot["directional_rows"]}
        for horizon, row in enumerate(matrix, 1):
            for model in models:
                self.assertEqual(float(row[model]), by_key[(horizon, model)])
        ranks = read("cd_mean_ranks_daily")
        self.assertEqual(len(ranks), 5)
        for row in ranks:
            self.assertEqual(float(row["mean_rank"]),
                             snapshot["report"]["figure_2"]["cd"]["mean_ranks"][row["model"]])
        self.assertEqual(set(str(p.relative_to(self.output)) for p in self.output.rglob('*') if p.is_file()),
                         set(DERIVED_FILES) | {"notes.txt"})
        for extension in ("pdf", "png"):
            self.assertEqual((self.output / f"figures/figure_2_cd_daily.{extension}").read_bytes(),
                             (self.output / f"figures/figure_2_cd_daily_paper.{extension}").read_bytes())
        self.assertFalse(list(ROOT.glob("*.pdf")) + list(ROOT.glob("*.png")) + list(ROOT.glob("*.csv")))
        first = {r: (self.output / r).read_bytes() for r in DERIVED_FILES}
        shutil.rmtree(self.output)
        ResultExport(self.database).run()
        for relative in DERIVED_FILES[1:]:
            self.assertEqual(first[relative], (self.output / relative).read_bytes())
        manifests = [json.loads(first["manifest.json"]), json.loads((self.output / "manifest.json").read_text())]
        for manifest in manifests:
            manifest.pop("exported_at")
            self.assertNotIn(str(self.directory), canonical_json(manifest))
            self.assertEqual(manifest["generated_paths"], list(DERIVED_FILES))
            for relative, digest in manifest["generated_files"].items():
                self.assertEqual(digest, hashlib.sha256((self.output / relative).read_bytes()).hexdigest())
        self.assertEqual(*manifests)
        self.assertEqual(before, self.database.read_bytes())

    def test_incomplete_and_invalid_fail_before_publication(self):
        """Missing configured horizons and tampered hashes never publish or overwrite final files."""
        self.output.mkdir()
        marker = self.output / "manifest.json"
        marker.write_text("preserve previous export")
        with duckdb.connect(str(self.database)) as c:
            c.execute("DELETE FROM paper_directional_results WHERE model='m4_fforma' AND horizon=14")
        with patch.object(ResultExport, "_render", side_effect=AssertionError("rendered incomplete")), \
             self.assertRaisesRegex(RuntimeError, "directional"):
            ResultExport(self.database).run()
        self.assertEqual(marker.read_text(), "preserve previous export")
        shutil.copyfile(self.fixture, self.database)
        with duckdb.connect(str(self.database)) as c:
            c.execute("UPDATE paper_directional_results SET result_fingerprint='invalid' WHERE horizon=1")
        with self.assertRaisesRegex(RuntimeError, "fingerprint"):
            ResultExport(self.database).run()
        self.assertEqual(marker.read_text(), "preserve previous export")
        shutil.copyfile(self.fixture, self.database)
        with duckdb.connect(str(self.database)) as c:
            c.execute("UPDATE experiment_processes SET status='pending' WHERE process_id=6")
        with self.assertRaisesRegex(RuntimeError, "completed Process 06"):
            ResultExport(self.database).run()
        self.assertEqual(marker.read_text(), "preserve previous export")

    def test_configured_model_scopes_and_stored_ties(self):
        """Exact scopes exclude unapproved baselines/Oracle; matrix shaping never re-ranks."""
        settings = CONFIG.table_reproduction["figure_2"]
        self.assertEqual(settings["horizon_models"], ["directional_mantis_rf", "chronos_2", "directional_dtw", "m4_smyl"])
        self.assertEqual(set(settings["cd_models"]),
                         {"directional_mantis_rf", "directional_dtw", "m4_fforma", "chronos_2", "m4_smyl"})
        with duckdb.connect(str(self.database), read_only=True) as c:
            report = TableStorage(c).summary("synthetic/export")["report"]
        profile = report["figure_2"]["cd"]
        matrix = ResultExport._matrix(profile)
        self.assertEqual(matrix["models"], settings["cd_models"])
        self.assertEqual(matrix["values"][0], [1, 0, 1, 1, 1])
        # Seven odd blocks tie four methods at rank2.5; six even blocks tie
        # DTW/FFORMA/Chronos/SMYL at rank2.5; block14 has Mantis1 and
        # the four other methods tied at3.5. Independently summed totals:
        self.assertEqual(profile["mean_ranks"], {"directional_mantis_rf": 48.5 / 14,
                         "directional_dtw": 53.5 / 14, "m4_fforma": 36 / 14,
                         "chronos_2": 36 / 14, "m4_smyl": 36 / 14})
        self.assertNotIn("mean_ranks", report["figure_2"]["horizon"])
        self.assertNotIn("mean_ranks", report["directional_profile"])
        duplicate = deepcopy(profile)
        duplicate["matrix"].append(duplicate["matrix"][0])
        with self.assertRaisesRegex(RuntimeError, "duplicate"):
            ResultExport._matrix(duplicate)

    def test_custom_output_and_failed_renderer_isolation(self):
        """Optional path is explicit; renderer failure leaves unrelated files and old outputs intact."""
        custom = self.directory / "custom"
        custom.mkdir()
        (custom / "unrelated.txt").write_text("keep")
        with patch.object(ResultExport, "_render", side_effect=RuntimeError("plot failed")), \
             self.assertRaisesRegex(RuntimeError, "plot failed"):
            ResultExport(self.database, custom).run()
        self.assertEqual(list(custom.iterdir()), [custom / "unrelated.txt"])

    def test_results_boundary_rejects_escape_before_creation(self):
        """Outside, traversal and symlink escapes fail before database open or mkdir."""
        outside = self.directory.parent / "outside"
        symlink = self.directory / "escape"
        symlink.symlink_to(self.directory.parent, target_is_directory=True)
        for path in (outside, self.directory / ".." / "outside", symlink / "outside"):
            with patch("util.p06_04_result_export.duckdb.connect", side_effect=AssertionError("opened")), \
                 self.assertRaises(ValueError):
                ResultExport(self.database, path).run()
            self.assertFalse(outside.exists())
        with self.assertRaises(ValueError):
            ResultExport(self.directory / "nested" / "fixture.duckdb")
        with self.assertRaises(ValueError):
            ResultExport(self.directory.parent / "fixture.duckdb")
        custom = self.directory / "custom" / "presentation"
        ResultExport(self.database, custom).run()
        self.assertTrue((custom / "evidence/execution-summary.json").is_file())
