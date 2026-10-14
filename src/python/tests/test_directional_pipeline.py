"""Focused configuration, storage-integrity, and cache tests for ID 021."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb

from util.shared_configuration import canonical_json, json_fingerprint
from util.shared_database import initialize_experiment_database
from util.shared_distributed_execution import (
    install_immutable_reference_cache,
    source_manifest_fingerprint,
    validate_directional_cluster,
)
from util.shared_experiment_execution import ExperimentCoordinator


ROOT = Path(__file__).resolve().parents[3]
from tests.configuration_fixtures import configuration_path, table_document
CONFIG = configuration_path(capped=True)


class DirectionalPipelineTests(unittest.TestCase):
    """Verify directional identities reject conflicts instead of silently resuming."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "directional.duckdb"
        self.configuration = initialize_experiment_database(self.database, CONFIG)

    def test_fixed_dtw_checkpoints_success_before_failure_and_resumes_missing_only(self):
        """Real task transactions survive a later failed external prediction block."""
        from tests.test_experiment_execution import ConfiguredPlanningTests

        fixture = table_document(capped=True)
        fixture["data"]["selection"] = {"method": "first_official", "count": 100}
        fixture["execution"]["default"]["batch_sizes"]["directional_prediction"] = 1
        path = Path(self.directory.name) / "fixed.json"
        path.write_text(json.dumps(fixture))
        database = Path(self.directory.name) / "fixed.duckdb"
        initialize_experiment_database(database, path)
        runtime = {"aeon": "1.6.0", "numpy": "2.0.2", "numba": "0.61.2",
                   "scikit-learn": "1.7.2", "numeric_dtype": "float64", "numba_threads": 1}
        profile = MagicMock(autoarima_workers=1, dask_swap_growth_limit_gib=0.25)
        profile.coordinator_memory_floor.return_value = 0
        profile.accelerator_host_memory_floor.return_value = 0
        profile.memory_floors_by_hostname.return_value = {}
        submitted = []

        def compute(*, batches, **kwargs):
            identities = {query["identity"] for batch in batches for query in batch[0]["queries"]}
            submitted.append(identities)
            for index, batch in enumerate(batches):
                if len(submitted) == 1 and index == 1:
                    yield {"batch": batch, "error": "bounded prediction failure"}
                    continue
                predictions = [{"evaluation_input_identity": query["identity"],
                    "horizon": horizon, "prediction": 1, "nearest_distance": 0.5,
                    "nearest_reference_identity": "reference/a", "effective_width": 6}
                    for query in batch[0]["queries"] for horizon in batch[0]["horizons"]]
                yield {"batch": batch, "response": {"runtime": runtime,
                    "runtime_seconds": 1, "worker": {"hostname": "fixture"},
                    "resource_usage": {}, "results": [{"predictions": predictions}]}}

        with ExperimentCoordinator(database) as coordinator:
            coordinator.connection.execute("""INSERT INTO datasets
                (dataset_id,dataset_name,source_system,source_revision,source_file_hashes,
                 import_configuration,import_configuration_hash,frequency)
                VALUES ('dataset','m4_daily','test','revision','{}','{}','configuration','D')""")
            with patch.object(coordinator, "_gift_bridge", side_effect=lambda *args:
                ConfiguredPlanningTests._description(int(args[args.index('--limit') + 1]))):
                plan = coordinator.plan()
            rows = coordinator.connection.execute("""SELECT task_id,forecast_instance_id,
                variant_id,candidate FROM experiment_tasks WHERE stage=4
                AND candidate='directional_dtw:predict:h01' ORDER BY task_id LIMIT 2""").fetchall()
            training = coordinator.connection.execute("""SELECT task_id,forecast_instance_id,
                variant_id,candidate FROM experiment_tasks WHERE candidate='directional_dtw:train'""").fetchone()
            coordinator.connection.execute("UPDATE experiment_tasks SET status='completed'")
            for index, row in enumerate(rows):
                coordinator.connection.execute("""INSERT INTO directional_evaluation_inputs
                    (evaluation_input_id,experiment_id,variant_id,forecast_instance_id,
                     preparation_id,preparation_fingerprint,raw_input_hash,cleaned_input_hash,
                     transformed_input_hash,transformed_input,label_reference,
                     preprocessing_provenance,package_versions,content_hash)
                    VALUES (?,?,?,?, 'p','p','r','c','t',?,0,'{}','{}','h')""",
                    [f"input/{index}", plan.experiment_id, row[2], row[1], [0.0] * 64])
            for row in [training, *rows]:
                coordinator.connection.execute("UPDATE experiment_tasks SET status='pending' WHERE task_id=?", [row[0]])
            implementation = {key: "fixture" for key in (
                "implementation_fingerprint", "repository_revision", "scientific_source_fingerprint",
                "worker_file_fingerprint", "classifier_lock_fingerprint", "preparation_fingerprint")}
            implementation["runtime_versions"] = runtime
            with patch.object(coordinator, "_directional_reference_library", return_value=(
                {"references": [{"identity": "reference/a"}]}, "reference", {"training_cohort": {"selected_count": 1}})), patch.object(
                coordinator, "_directional_implementation", return_value=implementation), patch.object(
                coordinator, "_publish_directional_dtw_model", return_value={"fixture": True}), patch(
                "util.shared_distributed_execution.install_immutable_reference_cache", return_value={"fingerprint": "reference"}), patch(
                "util.shared_workflow_orchestration.run_gate_compute_flow", side_effect=compute), patch(
                "util.shared_model_storage.ModelStorage.exists", return_value=True):
                attempts = coordinator._start_tasks([training, *rows], "fixture/first")
                with self.assertRaisesRegex(RuntimeError, "bounded prediction failure"):
                    coordinator._run_04_directional_dtw(plan.experiment_id, [training, *rows], attempts, profile, None, None)
                first = coordinator.connection.execute("SELECT * FROM directional_predictions").fetchall()
                self.assertEqual(len(first), 1)
                pending = coordinator._pending(plan.experiment_id, 4)
                self.assertEqual([row[0] for row in pending], [rows[1][0]])
                attempts = coordinator._start_tasks(pending, "fixture/restart")
                coordinator._run_04_directional_dtw(plan.experiment_id, pending, attempts, profile, None, None)
                self.assertEqual(coordinator._pending(plan.experiment_id, 4), [])
            stored = coordinator.connection.execute("SELECT * FROM directional_predictions").fetchall()
            self.assertEqual(len(stored), 2)
            self.assertIn(first[0], stored)
            self.assertEqual(submitted, [{"input/0", "input/1"}, {"input/1"}])
            self.assertEqual(coordinator.connection.execute("SELECT count(*) FROM directional_calibration_scores").fetchone()[0], 0)
            widths = coordinator.connection.execute("""SELECT horizon,effective_width,
                representative_proportion,calibration_accuracy,tie_rule
                FROM directional_selected_widths ORDER BY horizon""").fetchall()
            self.assertEqual(widths, [(h, 6, 0.10, None, "fixed_configuration") for h in range(1, 15)])

    def test_reference_cache_verifies_values_sources_and_labels(self):
        payload = {
            "references": [
                {
                    "identity": "reference/a",
                    "source_series_identity": "dataset/a",
                    "values": [0.0] * 64,
                    "labels": [0] * 14,
                }
            ]
        }
        fingerprint = json_fingerprint(payload)
        cache = Path(self.directory.name) / "cache"
        cache.mkdir()
        with patch(
            "util.shared_distributed_execution._immutable_cache_directory",
            return_value=cache,
        ):
            install_immutable_reference_cache(fingerprint, payload)
            path = cache / f"{fingerprint}.json"
            changed = {"references": [{**payload["references"][0], "labels": [1] * 14}]}
            path.write_text(canonical_json(changed), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "different content"):
                install_immutable_reference_cache(fingerprint, payload)

    def test_directional_cluster_preflight_accepts_locked_cpu_runtime(self):
        """ID 021 preflight validates aeon directly without the R tuning contract."""
        client = MagicMock()
        client.run.return_value = {
            "tcp://mac": {
                "platform": "Darwin",
                "python_version": "3.12.14",
                "dask_version": "2026.8.0",
                "distributed_version": "2026.8.0",
                "source_manifest": source_manifest_fingerprint({}),
                "source_mismatches": [],
                "classifier_lock_fingerprint": "locked",
                "classifier_runtime": {
                    "runtime": {
                        "aeon": "1.6.0",
                        "numpy": "2.0.2",
                        "numba": "0.61.2",
                        "scikit-learn": "1.7.2",
                        "numeric_dtype": "float64",
                    }
                },
                "resources": {"CPU": 1},
            },
            "tcp://ubuntu": {
                "platform": "Linux",
                "python_version": "3.12.3",
                "dask_version": "2026.8.0",
                "distributed_version": "2026.8.0",
                "source_manifest": source_manifest_fingerprint({}),
                "source_mismatches": [],
                "classifier_lock_fingerprint": "locked",
                "classifier_runtime": {
                    "runtime": {
                        "aeon": "1.6.0",
                        "numpy": "2.0.2",
                        "numba": "0.61.2",
                        "scikit-learn": "1.7.2",
                        "numeric_dtype": "float64",
                    }
                },
                "resources": {"CPU": 1, "TUNING_R_SLOT": 1},
            },
        }
        reports = validate_directional_cluster(
            client,
            expected_workers=2,
            expected_mac_workers=1,
            expected_ubuntu_workers=1,
            timeout=10,
            expected_manifest={},
            classifiers_environment="environments/classifiers/.venv",
            worker_script="src/python/04_04_directional_dtw.py",
            classifiers_lock="environments/classifiers/uv.lock",
            expected_lock_fingerprint="locked",
        )
        self.assertEqual(set(reports), {"tcp://mac", "tcp://ubuntu"})
        client.wait_for_workers.assert_called_once_with(2, timeout=10)

    def test_every_directional_record_uses_insert_or_verify(self):
        records = {
            "directional_evaluation_inputs": {
                "evaluation_input_id": "input/a", "experiment_id": "experiment/a",
                "variant_id": "variant/a", "forecast_instance_id": "instance/a",
                "preparation_id": "preparation/a", "preparation_fingerprint": "p",
                "raw_input_hash": "r", "cleaned_input_hash": "c",
                "transformed_input_hash": "t", "transformed_input": [0.0] * 64,
                "label_reference": 0.0, "preprocessing_provenance": canonical_json({"a": 1}),
                "package_versions": canonical_json({"R": "x"}), "content_hash": "h",
            },
            "directional_actual_labels": {
                "actual_label_id": "actual/a", "experiment_id": "experiment/a",
                "evaluation_input_id": "input/a", "definition_id": "directional_strict_v1",
                "labels": [0] * 14, "content_hash": "h",
            },
            "directional_model_definitions": {
                "model_definition_id": "model/a", "experiment_id": "experiment/a",
                "variant_id": "variant/a", "scientific_definition": canonical_json({"a": 1}),
                "repository_revision": "revision", "scientific_source_fingerprint": "source",
                "worker_file_fingerprint": "worker", "classifier_lock_fingerprint": "lock",
                "runtime_versions": canonical_json({"aeon": "1.6.0"}),
                "numeric_dtype": "float64", "reference_library_fingerprint": "reference",
                "preparation_fingerprint": "preparation", "implementation_fingerprint": "implementation",
                "content_hash": "h",
            },
            "directional_calibration_scores": {
                "calibration_score_id": "score/a", "model_definition_id": "model/a",
                "effective_width": 0, "representative_proportion": 0.0, "horizon": 1,
                "correct_count": 1, "evaluation_count": 1, "accuracy": 1.0,
                "candidate_policy": canonical_json({"a": 1}), "content_hash": "h",
            },
            "directional_selected_widths": {
                "selected_width_id": "width/a", "model_definition_id": "model/a",
                "horizon": 1, "effective_width": 0, "representative_proportion": 0.0,
                "calibration_accuracy": 1.0, "tie_rule": "smallest_effective_width",
                "content_hash": "h",
            },
            "directional_predictions": {
                "prediction_id": "prediction/a", "experiment_id": "experiment/a",
                "model_definition_id": "model/a", "evaluation_input_id": "input/a",
                "horizon": 1, "prediction": 0, "nearest_reference_identity": "reference/a",
                "nearest_distance": 0.0, "effective_width": 0,
                "execution_metadata": canonical_json({"host": "mac"}), "content_hash": "h",
            },
            "directional_evaluations": {
                "directional_evaluation_id": "evaluation/a", "experiment_id": "experiment/a",
                "model_definition_id": "model/a", "horizon": 1, "correct_count": 1,
                "evaluation_count": 1, "accuracy": 1.0,
                "prediction_fingerprint": "predictions", "content_hash": "h",
            },
        }
        with ExperimentCoordinator(self.database) as coordinator:
            for table, record in records.items():
                with self.subTest(table=table):
                    coordinator._insert_or_verify(table, record)
                    coordinator._insert_or_verify(table, record)
                    conflict = dict(record)
                    conflict["content_hash"] = "conflict"
                    with self.assertRaisesRegex(RuntimeError, "conflicting accepted"):
                        coordinator._insert_or_verify(table, conflict)
                    stored = coordinator.connection.execute(
                        f"SELECT content_hash FROM {table} WHERE {next(iter(record))}=?",
                        [next(iter(record.values()))],
                    ).fetchone()[0]
                    self.assertEqual(stored, "h")

if __name__ == "__main__":
    unittest.main()
