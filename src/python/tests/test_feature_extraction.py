# ==============================================================================
# test_feature_extraction.py
#
# Purpose: Verify optional ID 014 feature persistence, reuse, and isolation.
# Inputs: Two disposable prepared Daily windows and the native R feature provider.
# Outputs: unittest assertions; no accepted database or research result is changed.
# Run from: PYTHONPATH=src/python .tools/uv/uv run --locked --no-sync python -m unittest tests.test_feature_extraction
# ==============================================================================

"""Focused unit and connected tests for optional reusable feature extraction."""

from __future__ import annotations

import json
import tempfile
import time
import tracemalloc
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from util.feature_extraction import (
    FEATURE_SET_ID,
    FeatureExtractionCoordinator,
    feature_provenance_fingerprint,
    get_prepared_features,
)
from util.p00_01_researcher_actions import FeatureExtractionAction
from util.shared_database import initialize_experiment_database
from util.shared_distributed_execution import feature_provider_description
from util.window_preparation import (
    WindowPreparationCoordinator,
    _target_content_hash,
)


ROOT = Path(__file__).resolve().parents[3]
CONFIGURATION = (
    ROOT / "config/experiments/poc2_m4_daily_100_rolling_windows_corrected.json"
)
LOCAL_WINDOW_LIMITS = {"max_series": 100, "max_windows": 2}


class _ImmediateFeatureFuture:
    """Small completed-future stand-in for coordinator paging tests."""

    def __init__(self, response: dict):
        self.response = response

    def result(self) -> dict:
        return self.response


class _SuccessfulFeatureTask:
    """Record submitted IDs and return schema-valid rows without invoking R."""

    def __init__(self, provider: dict):
        self.provider = provider
        self.submitted_ids: list[str] = []

    def submit(self, batch: list[dict], *_: object) -> _ImmediateFeatureFuture:
        self.submitted_ids.extend(job["window_id"] for job in batch)
        return _ImmediateFeatureFuture(
            {
                "provider": self.provider,
                "worker": {"execution_backend": "paging-test", "hostname": "fixture"},
                "results": [
                    {
                        "id": job["window_id"],
                        "status": "success",
                        "feature_names": self.provider["feature_names"],
                        "feature_values": [0.0] * len(self.provider["feature_names"]),
                        **{
                            key: value
                            for key, value in job.items()
                            if key not in {"window_id", "transformed_input"}
                        },
                    }
                    for job in batch
                ],
            }
        )


class FeatureExtractionTests(unittest.TestCase):
    """Exercise the complete optional local route over disposable data."""

    def setUp(self) -> None:
        """Create two prepared windows without asking for feature extraction."""
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.parent = root / "parent.duckdb"
        self.child = root / "windows.duckdb"
        document = json.loads(CONFIGURATION.read_text())
        document["execution"]["default"]["batch_sizes"]["window_preparation"] = 2
        configuration_path = root / "configuration.json"
        configuration_path.write_text(json.dumps(document))
        initialize_experiment_database(self.parent, configuration_path)
        parent = duckdb.connect(str(self.parent))
        try:
            parent.execute("BEGIN TRANSACTION")
            parent.execute(
                """INSERT INTO datasets VALUES
                ('dataset/features', 'm4_daily', 'fixture', 'fixture-revision', '{}',
                 '{}', 'fixture-hash', 'D', '{}', current_timestamp)"""
            )
            for index in range(100):
                if index == 0:
                    values = [20.0 + step / 7 + (step % 5) for step in range(106)]
                elif index == 1:
                    values = [7.0] * 106
                else:
                    values = [float(index + step) for step in range(20)]
                train_end = 78 if index < 2 else 20
                parent.execute(
                    """INSERT INTO series VALUES
                    ('dataset/features', ?, ?, ?, 'D', TIMESTAMP '2000-01-01', ?, ?,
                     ?, '{}', current_timestamp)""",
                    [
                        str(index), f"D{index + 1}", index, values, len(values),
                        _target_content_hash(values),
                    ],
                )
                parent.execute(
                    """INSERT INTO evaluation_windows VALUES
                    ('dataset/features', ?, 'short/000', 'validation_and_test',
                     0, ?, ?, ?, ?, ?, 14,
                     'zero-based, end-exclusive', current_timestamp)""",
                    [str(index), train_end, train_end, train_end, train_end, train_end],
                )
            parent.execute(
                """UPDATE experiment_processes SET status='completed',
                   completed_at=current_timestamp WHERE process_id=1"""
            )
            parent.execute("COMMIT")
        except BaseException:
            parent.execute("ROLLBACK")
            raise
        finally:
            parent.close()
        with WindowPreparationCoordinator(self.parent, self.child) as coordinator:
            prepared = coordinator.run(
                source_manifest_hash="window-fixture-source",
                local_limits=LOCAL_WINDOW_LIMITS,
            )
        self.assertEqual(prepared["total_windows"], 2)

    def tearDown(self) -> None:
        """Delete fixture-owned databases and configuration."""
        self.temporary.cleanup()

    def _source_state(self) -> tuple[list[tuple], list[tuple]]:
        """Return parent series and child split rows used to prove isolation."""
        parent = duckdb.connect(str(self.parent), read_only=True)
        child = duckdb.connect(str(self.child), read_only=True)
        try:
            return (
                parent.execute(
                    "SELECT series_id, content_hash FROM series ORDER BY series_id"
                ).fetchall(),
                child.execute(
                    "SELECT series_key, partition FROM series_membership ORDER BY series_key"
                ).fetchall(),
            )
        finally:
            child.close()
            parent.close()

    def test_connected_persistence_retrieval_restart_failure_and_bounds(self) -> None:
        """One opt-in run is bounded, reusable, failure-explicit, and source-neutral."""
        child = duckdb.connect(str(self.child), read_only=True)
        try:
            optional_tables = child.execute(
                """SELECT table_name FROM information_schema.tables
                   WHERE table_name IN ('feature_set_definitions', 'window_features')"""
            ).fetchall()
            window_ids = [
                row[0]
                for row in child.execute(
                    "SELECT window_id FROM prepared_windows ORDER BY series_key"
                ).fetchall()
            ]
        finally:
            child.close()
        self.assertEqual(optional_tables, [])
        source_before = self._source_state()

        tracemalloc.start()
        started = time.monotonic()
        try:
            first = FeatureExtractionAction().run(
                self.parent, self.child, local_max_windows=2
            )
        finally:
            elapsed = time.monotonic() - started
            _, peak_bytes = tracemalloc.get_traced_memory()
            tracemalloc.stop()
        self.assertEqual(first["total_windows"], 2)
        self.assertEqual(first["submitted_feature_rows"], 2)
        self.assertEqual(first["successful_feature_rows"], 2)
        self.assertTrue(first["feature_preparation_complete"])
        self.assertLess(elapsed, 120.0)
        self.assertLess(peak_bytes, 192 * 1024 * 1024)
        self.assertEqual(self._source_state(), source_before)

        varying = get_prepared_features(self.child, window_id=window_ids[0])
        constant = get_prepared_features(self.child, window_id=window_ids[1])
        self.assertEqual(varying.feature_set_id, FEATURE_SET_ID)
        self.assertEqual(len(varying.feature_names), 42)
        self.assertEqual(len(varying.feature_values), 42)
        self.assertEqual(constant.feature_values[varying.feature_names.index("entropy")], 0)
        self.assertEqual(varying.period, 1)

        second = FeatureExtractionAction().run(
            self.parent, self.child, local_max_windows=2
        )
        self.assertEqual(second["submitted_feature_rows"], 0)
        self.assertEqual(second["reused_feature_rows"], 2)
        self.assertEqual(second["array_rows_read"], 0)
        self.assertEqual(second["page_count"], 0)

        provider = feature_provider_description()
        failed_provider = {
            **provider,
            "dependencies": {**provider["dependencies"], "fixture_failure": "1"},
        }
        failed_provenance = feature_provenance_fingerprint(
            failed_provider, "failure-source"
        )
        with FeatureExtractionCoordinator(self.parent, self.child) as coordinator:
            periods, _ = coordinator._periods_by_series()
            coordinator._configure_pending_selection(periods, {failed_provenance})
            jobs, _ = coordinator._pending_page(None, 1)
            job = jobs[0]
            succeeded, failed = coordinator._commit_response(
                {
                    "provider": failed_provider,
                    "worker": {"execution_backend": "test", "hostname": "fixture"},
                    "results": [
                        {
                            "id": job["window_id"],
                            "status": "failed",
                            "error": "fixture extraction failure",
                            **{
                                key: value
                                for key, value in job.items()
                                if key not in {"window_id", "transformed_input"}
                            },
                        }
                    ],
                },
                "failure-source",
                {failed_provenance},
            )
            changed_provider = {
                **provider,
                "dependencies": {**provider["dependencies"], "changed": "2"},
            }
            changed = feature_provenance_fingerprint(changed_provider, first["source_manifest"])
            coordinator._configure_pending_selection(periods, {changed})
            self.assertEqual(coordinator._pending_count(), 2)
        self.assertEqual((succeeded, failed), (0, 1))
        child = duckdb.connect(str(self.child), read_only=True)
        try:
            failure = child.execute(
                """SELECT status, feature_values, last_error FROM window_features
                   WHERE provenance_fingerprint=?""",
                [failed_provenance],
            ).fetchone()
            feature_rows = child.execute(
                """SELECT count(*) FROM window_features
                   WHERE status='success'"""
            ).fetchone()[0]
        finally:
            child.close()
        self.assertEqual(failure, ("failed", None, "fixture extraction failure"))
        self.assertEqual(feature_rows, 2)
        self.assertEqual(self._source_state(), source_before)

    def test_pending_pages_bound_arrays_process_once_and_restart_without_reads(self) -> None:
        """More than one page stays bounded and a reused restart reads no arrays."""
        child = duckdb.connect(str(self.child))
        try:
            original = child.execute(
                """SELECT * EXCLUDE (window_id, window_ordinal, created_at)
                   FROM prepared_windows ORDER BY series_key LIMIT 1"""
            ).fetchone()
            for index in range(5):
                child.execute(
                    """INSERT INTO prepared_windows
                       SELECT ?, preparation_id, series_key, ?, input_start, input_end,
                              future_start, future_end, transformed_input,
                              transformation_state, preprocessing_provenance,
                              package_versions, worker_provenance, input_hash,
                              cleaned_hash, transformed_hash, current_timestamp
                       FROM prepared_windows ORDER BY series_key LIMIT 1""",
                    [f"paging-{index}", 100 + index],
                )
            before_windows = child.execute(
                """SELECT window_id, transformed_hash FROM prepared_windows
                   ORDER BY series_key, window_ordinal"""
            ).fetchall()
            before_labels = child.execute(
                "SELECT window_id, labels FROM window_directional_labels ORDER BY window_id"
            ).fetchall()
            before_membership = child.execute(
                "SELECT series_key, partition FROM series_membership ORDER BY series_key"
            ).fetchall()
        finally:
            child.close()
        self.assertIsNotNone(original)

        provider = feature_provider_description()
        task = _SuccessfulFeatureTask(provider)
        run_arguments = {
            "provider_descriptions": [provider],
            "source_manifest_hash": "paging-source",
            "local_max_windows": 7,
            "distributed": False,
            "memory_safety": None,
            "max_in_flight": 1,
            "retries": 0,
        }
        with FeatureExtractionCoordinator(self.parent, self.child) as coordinator:
            with patch(
                "util.feature_extraction.compute_feature_batch.with_options",
                return_value=task,
            ), patch(
                "util.feature_extraction.as_completed",
                side_effect=lambda values: iter(values),
            ):
                first = coordinator.run(**run_arguments)

        self.assertEqual(first["total_windows"], 7)
        self.assertEqual(first["page_size"], 2)
        self.assertEqual(first["page_count"], 4)
        self.assertEqual(first["array_rows_read"], 7)
        self.assertEqual(first["max_page_rows"], 2)
        self.assertEqual(first["max_live_payload_rows"], 2)
        self.assertEqual(len(task.submitted_ids), 7)
        self.assertEqual(len(set(task.submitted_ids)), 7)

        restart_task = _SuccessfulFeatureTask(provider)
        with FeatureExtractionCoordinator(self.parent, self.child) as coordinator:
            with patch.object(
                coordinator,
                "_pending_page",
                wraps=coordinator._pending_page,
            ) as page_reader, patch(
                "util.feature_extraction.compute_feature_batch.with_options",
                return_value=restart_task,
            ), patch(
                "util.feature_extraction.as_completed",
                side_effect=lambda values: iter(values),
            ):
                restarted = coordinator.run(**run_arguments)
        self.assertEqual(restarted["reused_feature_rows"], 7)
        self.assertEqual(restarted["submitted_feature_rows"], 0)
        self.assertEqual(restarted["array_rows_read"], 0)
        self.assertEqual(restarted["page_count"], 0)
        self.assertEqual(restart_task.submitted_ids, [])
        self.assertEqual(page_reader.call_count, 1)

        child = duckdb.connect(str(self.child), read_only=True)
        try:
            after_windows = child.execute(
                """SELECT window_id, transformed_hash FROM prepared_windows
                   ORDER BY series_key, window_ordinal"""
            ).fetchall()
            after_labels = child.execute(
                "SELECT window_id, labels FROM window_directional_labels ORDER BY window_id"
            ).fetchall()
            after_membership = child.execute(
                "SELECT series_key, partition FROM series_membership ORDER BY series_key"
            ).fetchall()
        finally:
            child.close()
        self.assertEqual(after_windows, before_windows)
        self.assertEqual(after_labels, before_labels)
        self.assertEqual(after_membership, before_membership)

    def test_local_limit_rejection_precedes_array_page_reads(self) -> None:
        """An oversized complete request fails before schema or array selection."""
        child = duckdb.connect(str(self.child))
        try:
            for index in range(3):
                child.execute(
                    """INSERT INTO prepared_windows
                       SELECT ?, preparation_id, series_key, ?, input_start, input_end,
                              future_start, future_end, transformed_input,
                              transformation_state, preprocessing_provenance,
                              package_versions, worker_provenance, input_hash,
                              cleaned_hash, transformed_hash, current_timestamp
                       FROM prepared_windows ORDER BY series_key LIMIT 1""",
                    [f"limit-{index}", 200 + index],
                )
        finally:
            child.close()
        provider = feature_provider_description()
        with FeatureExtractionCoordinator(self.parent, self.child) as coordinator:
            with patch.object(
                coordinator, "_pending_page", side_effect=AssertionError("array read")
            ):
                with self.assertRaisesRegex(RuntimeError, "exceeds its bound: 5/4"):
                    coordinator.run(
                        provider_descriptions=[provider],
                        source_manifest_hash="limit-source",
                        local_max_windows=4,
                        distributed=False,
                        memory_safety=None,
                        max_in_flight=1,
                        retries=0,
                    )
        child = duckdb.connect(str(self.child), read_only=True)
        try:
            feature_tables = child.execute(
                """SELECT count(*) FROM information_schema.tables
                   WHERE table_name='window_features'"""
            ).fetchone()[0]
        finally:
            child.close()
        self.assertEqual(feature_tables, 0)

    def test_invalid_local_bounds_fail_without_creating_feature_tables(self) -> None:
        """The optional route cannot silently run an unbounded local workload."""
        with self.assertRaisesRegex(RuntimeError, "local feature extraction requires"):
            FeatureExtractionAction().run(self.parent, self.child)
        child = duckdb.connect(str(self.child), read_only=True)
        try:
            count = child.execute(
                """SELECT count(*) FROM information_schema.tables
                   WHERE table_name='window_features'"""
            ).fetchone()[0]
        finally:
            child.close()
        self.assertEqual(count, 0)


if __name__ == "__main__":
    unittest.main()
