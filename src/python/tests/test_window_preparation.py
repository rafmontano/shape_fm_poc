# ==============================================================================
# test_window_preparation.py
#
# Purpose: Verify approved rolling boundaries, S1 membership, storage, and resume.
# Inputs: Small synthetic fixtures, v5 configuration, R preprocessing, and DuckDB.
# Outputs: unittest assertions and disposable parent/child databases only.
# Run from: PYTHONPATH=src/python .tools/uv/uv run --locked --no-sync python -m unittest tests.test_window_preparation
# ==============================================================================

"""Focused tests for combined IDs 011 and 016."""

from __future__ import annotations

import socket
import tempfile
import unittest
from pathlib import Path

import duckdb

from util.configuration import load_experiment_configuration
from util.database import initialize_experiment_database
from util.window_preparation import (
    WindowPreparationCoordinator,
    complete_window_count,
    get_prepared_window,
    rolling_window_inputs,
    s1_partition,
)


ROOT = Path(__file__).resolve().parents[3]
CONFIGURATION = ROOT / "config/experiments/poc2_m4_daily_100_rolling_windows.json"


class RollingWindowUnitTests(unittest.TestCase):
    """Verify frequency settings, tsai boundaries, and deterministic S1 behavior."""

    def test_all_frequency_settings_resolve_approved_strides(self) -> None:
        """All ten approved W/H settings derive stride W+H in resolved config."""
        configuration = load_experiment_configuration(CONFIGURATION)
        expected = {
            "10S": (512, 60, 572),
            "5T": (512, 48, 560),
            "10T": (512, 48, 560),
            "15T": (512, 48, 560),
            "H": (256, 48, 304),
            "D": (64, 14, 78),
            "W": (64, 13, 77),
            "M": (64, 18, 82),
            "Q": (32, 8, 40),
            "Y": (16, 6, 22),
        }
        observed = {
            frequency: (
                values["input_length"],
                values["future_horizon"],
                values["stride"],
            )
            for frequency, values in configuration.resolved["pipeline"][
                "window_preparation"
            ]["frequencies"].items()
        }
        self.assertEqual(observed, expected)
        self.assertEqual(
            configuration.original["pipeline"]["window_preparation"]["split"]["seed"],
            123,
        )
        self.assertEqual(configuration.seed, 1234)

    def test_daily_example_matches_independent_reference(self) -> None:
        """L=156 produces exactly the two approved complete Daily blocks."""
        windows = rolling_window_inputs(range(156), 64, 14)
        boundaries = [
            (
                item["input_start"],
                item["input_end"],
                item["future_start"],
                item["future_end"],
            )
            for item in windows
        ]
        self.assertEqual(boundaries, [(0, 64, 64, 78), (78, 142, 142, 156)])
        self.assertEqual(windows[0]["input"], [float(value) for value in range(64)])
        self.assertEqual(windows[1]["input"], [float(value) for value in range(78, 142)])

    def test_exact_short_remainder_and_offset_boundaries(self) -> None:
        """Only full blocks are returned, aligned to each permitted segment start."""
        self.assertEqual(complete_window_count(78, 64, 14), 1)
        self.assertEqual(complete_window_count(77, 64, 14), 0)
        self.assertEqual(complete_window_count(155, 64, 14), 1)
        self.assertEqual(rolling_window_inputs(range(77), 64, 14), ())
        window = rolling_window_inputs(range(100), 64, 14, offset=50)[0]
        self.assertEqual(
            (window["input_start"], window["input_end"], window["future_start"], window["future_end"]),
            (50, 114, 114, 128),
        )

    def test_missing_positions_are_retained_and_infinity_is_rejected(self) -> None:
        """Gate 2 receives missing input positions while malformed infinity fails."""
        values = [float(value) for value in range(78)]
        values[5] = float("nan")
        self.assertIsNone(rolling_window_inputs(values, 64, 14)[0]["input"][5])
        values[5] = float("inf")
        with self.assertRaisesRegex(ValueError, "infinity"):
            rolling_window_inputs(values, 64, 14)

    def test_constant_and_missing_inputs_are_cleaned_then_standardised(self) -> None:
        """The real worker keeps fitted state finite for risky constant/missing inputs."""
        from util.distributed_execution import window_preparation_batch

        context = [4.0] * 64
        context[3] = None
        response = window_preparation_batch(
            [
                {
                    "id": "series/constant",
                    "preprocessing_mode": "robust",
                    "transformation": "standardise_sample_v1",
                    "seasonality": 1,
                    "windows": [
                        {
                            "window_id": "window/constant",
                            "window_ordinal": 0,
                            "input_start": 0,
                            "input_end": 64,
                            "future_start": 64,
                            "future_end": 78,
                            "input": context,
                        }
                    ],
                }
            ],
            "src/r/02_01_preprocess_series.R",
            60,
            1,
            {
                "mac_hostname": socket.gethostname(),
                "mac_minimum_available_gib": 0.1,
                "ubuntu_minimum_available_gib": 0.1,
                "fit_budget_gib": 0.1,
                "admission_timeout_seconds": 10.0,
                "poll_interval_seconds": 0.05,
                "breach_grace_seconds": 1.0,
                "swap_growth_limit_gib": 1.0,
            },
        )
        window = response["results"][0]["windows"][0]
        self.assertEqual(window["preprocessing"]["missing_count_before"], 1)
        self.assertEqual(window["preprocessing"]["missing_count_after"], 0)
        self.assertTrue(window["transformation_state"]["constant"])
        self.assertEqual(window["transformed_input"], [0.0] * 64)
        self.assertIn("memory_safety", response["worker"])
        self.assertEqual(response["worker"]["memory_safety"]["safety_responses"], 0)

    def test_s1_exact_counts_namespaces_and_placement_independence(self) -> None:
        """Explicit rounding gives disjoint series membership independent of order."""
        identities = [("dataset-a", str(index)) for index in range(10)]
        membership, provenance = s1_partition(identities, 0.8, 123)
        self.assertEqual(list(membership.values()).count("test"), 2)
        self.assertEqual(list(membership.values()).count("train"), 8)
        repeated, _ = s1_partition(list(reversed(identities)), 0.8, 123)
        self.assertEqual(membership, repeated)
        namespaced, _ = s1_partition(
            [("dataset-a", "0"), ("dataset-b", "0")], 0.8, 123
        )
        self.assertEqual(len(namespaced), 2)
        self.assertEqual(provenance["generator_version"], "1.0.1")
        for size, expected_test in ((2, 1), (5, 1), (10, 2), (11, 2)):
            split, _ = s1_partition(
                [("dataset", str(index)) for index in range(size)], 0.8, 123
            )
            self.assertEqual(list(split.values()).count("test"), expected_test)
        with self.assertRaisesRegex(ValueError, "at least two"):
            s1_partition([], 0.8, 123)
        with self.assertRaisesRegex(ValueError, "at least two"):
            s1_partition([("dataset", "0")], 0.8, 123)


class WindowPersistenceTests(unittest.TestCase):
    """Exercise normalized parent/child writes, retrieval, and duplicate-free resume."""

    def setUp(self) -> None:
        """Create a fresh v5 parent with 100 short canonical Daily series."""
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.parent = root / "parent.duckdb"
        self.child = root / "windows.duckdb"
        initialize_experiment_database(self.parent, CONFIGURATION)
        connection = duckdb.connect(str(self.parent))
        try:
            connection.execute("BEGIN TRANSACTION")
            connection.execute(
                """INSERT INTO datasets VALUES
                ('dataset/test', 'm4_daily', 'fixture', 'fixture-revision', '{}',
                 '{}', 'fixture-hash', 'D', '{}', current_timestamp)"""
            )
            for index in range(100):
                values = [float(index + step) for step in range(106)]
                connection.execute(
                    """INSERT INTO series VALUES
                    ('dataset/test', ?, ?, ?, 'D', TIMESTAMP '2000-01-01', ?, 106,
                     ?, '{}', current_timestamp)""",
                    [str(index), f"D{index + 1}", index, values, f"hash-{index}"],
                )
                connection.execute(
                    """INSERT INTO evaluation_windows VALUES
                    ('dataset/test', ?, 'short/000', 'validation_and_test',
                     0, 78, 78, 92, 92, 106, 14,
                     'zero-based, end-exclusive', current_timestamp)""",
                    [str(index)],
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

    def tearDown(self) -> None:
        """Delete all fixture-owned files."""
        self.temporary.cleanup()

    def test_preparation_retrieval_and_restart_preserve_membership(self) -> None:
        """A second run skips completed series and preserves every persisted identity."""
        with WindowPreparationCoordinator(self.parent, self.child) as coordinator:
            first = coordinator.run(source_manifest_hash="fixture-manifest")
        self.assertEqual(first["eligible"], 100)
        self.assertEqual(first["zero_window"], 0)
        self.assertEqual(first["series_by_partition"], {"train": 80, "test": 20})
        self.assertEqual(first["total_windows"], 100)
        self.assertEqual(first["periods"]["D"]["r_period"], 1)

        child = duckdb.connect(str(self.child), read_only=True)
        try:
            before = child.execute(
                """SELECT count(*), min(window_id), max(window_id),
                          sum(attempt_count) FROM prepared_windows
                   CROSS JOIN (SELECT sum(attempt_count) attempt_count FROM window_tasks)"""
            ).fetchone()
            membership = child.execute(
                "SELECT series_key, partition FROM series_membership ORDER BY series_key"
            ).fetchall()
        finally:
            child.close()

        with WindowPreparationCoordinator(self.parent, self.child) as coordinator:
            second = coordinator.run(source_manifest_hash="fixture-manifest")
        self.assertEqual(second["membership_fingerprint"], first["membership_fingerprint"])
        self.assertEqual(second["membership_source"], "persisted_resume")
        self.assertEqual(second["worker_series_counts"], {})

        child = duckdb.connect(str(self.child), read_only=True)
        try:
            after = child.execute(
                """SELECT count(*), min(window_id), max(window_id),
                          sum(attempt_count) FROM prepared_windows
                   CROSS JOIN (SELECT sum(attempt_count) attempt_count FROM window_tasks)"""
            ).fetchone()
            self.assertEqual(
                child.execute(
                    "SELECT series_key, partition FROM series_membership ORDER BY series_key"
                ).fetchall(),
                membership,
            )
        finally:
            child.close()
        self.assertEqual(after, before)

        # Simulate interruption after all child commits but before the parent run
        # record was marked complete. Resume must reconcile without reprocessing.
        parent = duckdb.connect(str(self.parent))
        try:
            parent.execute(
                "UPDATE window_preparation_runs SET status='running', completed_at=NULL"
            )
        finally:
            parent.close()
        with WindowPreparationCoordinator(self.parent, self.child) as coordinator:
            reconciled = coordinator.run(source_manifest_hash="fixture-manifest")
        self.assertEqual(reconciled["worker_series_counts"], {})

        selected = get_prepared_window(
            self.parent,
            self.child,
            dataset_id="dataset/test",
            series_id="0",
            window_ordinal=0,
        )
        self.assertEqual((selected.input_start, selected.input_end), (0, 64))
        self.assertEqual((selected.future_start, selected.future_end), (64, 78))
        self.assertEqual(selected.future, tuple(float(value) for value in range(64, 78)))
        self.assertEqual(len(selected.transformed_input), 64)
        self.assertEqual(
            set(selected.transformation_state),
            {"recipe", "version", "centre", "scale", "count", "constant"},
        )

        parent = duckdb.connect(str(self.parent), read_only=True)
        try:
            self.assertEqual(
                parent.execute("SELECT status FROM window_preparation_runs").fetchone()[0],
                "completed",
            )
            self.assertEqual(parent.execute("SELECT count(*) FROM series").fetchone()[0], 100)
        finally:
            parent.close()

    def test_resume_rejects_changed_source_manifest(self) -> None:
        """A child created from one tested source cannot resume under another."""
        with WindowPreparationCoordinator(self.parent, self.child) as coordinator:
            coordinator.run(source_manifest_hash="reviewed-source-a")
        with WindowPreparationCoordinator(self.parent, self.child) as coordinator:
            with self.assertRaisesRegex(RuntimeError, "source manifest"):
                coordinator.run(source_manifest_hash="different-source-b")


if __name__ == "__main__":
    unittest.main()
