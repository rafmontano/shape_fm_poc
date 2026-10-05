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
from util.shared_experiment_execution import ExperimentCoordinator, expected_task_counts
from util.window_preparation import _target_content_hash


ROOT = Path(__file__).resolve().parents[3]
CONFIG = ROOT / "config/experiments/poc2_m4_daily_100_directional_dtw.json"


class DirectionalPipelineTests(unittest.TestCase):
    """Verify directional identities reject conflicts instead of silently resuming."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "directional.duckdb"
        self.configuration = initialize_experiment_database(self.database, CONFIG)

    def test_directional_task_cardinalities_have_no_forecast_combination_rows(self):
        self.assertEqual(
            expected_task_counts(100, self.configuration.workflow),
            {2: 100, 3: 100, 4: 1401, 5: 1, 6: 14},
        )

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

    def test_fresh_database_process_03_completes_without_ambiguous_columns(self):
        """The real fresh parent/child path completes bounded directional preparation."""
        fixture = json.loads(CONFIG.read_text(encoding="utf-8"))
        fixture["execution"]["default"]["batch_sizes"]["preprocess"] = 100
        fixture["execution"]["default"]["batch_sizes"]["window_preparation"] = 16
        fixture_path = Path(self.directory.name) / "fixture.json"
        fixture_path.write_text(json.dumps(fixture), encoding="utf-8")
        database = Path(self.directory.name) / "fresh-process-03.duckdb"
        initialize_experiment_database(database, fixture_path)
        connection = duckdb.connect(str(database))
        try:
            connection.execute("BEGIN TRANSACTION")
            connection.execute(
                """INSERT INTO datasets VALUES
                   ('dataset/test', 'm4_daily', 'fixture', 'fixture-revision', '{}',
                    '{}', 'fixture-hash', 'D', '{}', current_timestamp)"""
            )
            for index in range(100):
                series_id = f"D{index + 1}"
                values = [float(index + offset % 11) for offset in range(106)]
                connection.execute(
                    """INSERT INTO series VALUES
                       ('dataset/test', ?, ?, ?, 'D', TIMESTAMP '2000-01-01', ?, 106,
                        ?, '{}', current_timestamp)""",
                    [series_id, series_id, index, values, _target_content_hash(values)],
                )
                connection.execute(
                    """INSERT INTO evaluation_windows VALUES
                       ('dataset/test', ?, 'short/000', 'validation_and_test',
                        0, 78, 78, 92, 92, 106, 14,
                        'zero-based, end-exclusive', current_timestamp)""",
                    [series_id],
                )
            connection.execute(
                """UPDATE experiment_processes SET status='completed',
                   completed_at=current_timestamp WHERE process_id=1"""
            )
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

        def official(limit: int) -> dict[str, object]:
            return {
                "configuration_name": "m4_daily/D/short",
                "dataset_name": "m4_daily",
                "frequency": "D",
                "term": "short",
                "prediction_length": 14,
                "window_count": 1,
                "domain": "Econ/Fin",
                "num_variates": 1,
                "available_instances": 100,
                "r_period": 7,
                "r_period_source": "pinned frequency",
                "evaluation_seasonality": 7,
                "gluonts_default_seasonality": 7,
                "instances": [
                    {
                        "item_id": f"D{index + 1}",
                        "variate_id": "0",
                        "window_id": "short/000",
                        "official_position": index,
                        "context": [float(index + offset % 11) for offset in range(64)],
                        "actual": [float(index + 20 + offset) for offset in range(14)],
                        "start": "2000-01-01",
                        "forecast_start": "2000-03-05",
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
            coordinator.run_process(plan.experiment_id, 2)
            try:
                result = coordinator.run_process(plan.experiment_id, 3)
            except RuntimeError as error:
                detail = coordinator.connection.execute(
                    """SELECT last_error FROM experiment_tasks
                       WHERE stage=3 AND last_error IS NOT NULL LIMIT 1"""
                ).fetchone()
                self.fail(f"Process 03 failed: {detail[0] if detail else error}")
            self.assertEqual(result["counts"], {"completed": 100})
            self.assertEqual(
                coordinator.connection.execute(
                    "SELECT count(*) FROM directional_evaluation_inputs"
                ).fetchone()[0],
                100,
            )
            self.assertEqual(
                coordinator.connection.execute(
                    "SELECT count(*) FROM directional_actual_labels"
                ).fetchone()[0],
                100,
            )


if __name__ == "__main__":
    unittest.main()
