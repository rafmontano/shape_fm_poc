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

import math
import socket
import tempfile
import time
import tracemalloc
import unittest
import json
import shutil
from pathlib import Path
from unittest.mock import patch

import duckdb

from util import shared_distributed_execution
from util.shared_configuration import (
    json_fingerprint,
    load_experiment_configuration,
    resolve_experiment_configuration,
)
from util.shared_database import initialize_experiment_database, load_database_configuration
from util.window_preparation import (
    EXACT_LABEL_REFERENCE_SOURCE,
    EXACT_REFERENCE_UNAVAILABLE_REASON,
    WindowPreparationCoordinator,
    _target_content_hash,
    complete_window_count,
    compute_window_preparation_batch,
    get_prepared_window,
    rolling_window_inputs,
    run_window_preparation_flow,
    s1_partition,
)


ROOT = Path(__file__).resolve().parents[3]
CONFIGURATION = ROOT / "config/experiments/poc2_m4_daily_100_rolling_windows_corrected.json"
LEGACY_CONFIGURATION = ROOT / "config/experiments/poc2_m4_daily_100_rolling_windows.json"
VERSION_11_CONFIGURATION = (
    ROOT / "config/experiments/poc2_m4_daily_100_directional_dtw_mantis_rf.json"
)
STANDARDISATION_CONFIGURATION = (
    ROOT / "config/experiments/poc2_m4_daily_100_standardised.json"
)
LOCAL_TEST_LIMITS = {"max_series": 100, "max_windows": 200}


class RollingWindowUnitTests(unittest.TestCase):
    """Verify frequency settings, tsai boundaries, and deterministic S1 behavior."""

    def test_named_compute_task_passes_continuous_memory_safety(self) -> None:
        """The Prefect task is a storage-free adapter preserving worker safeguards."""
        safety = {"minimum_available_bytes": 123, "poll_interval_seconds": 0.25}
        with patch(
            "util.window_preparation.window_preparation_batch",
            return_value={"results": [], "worker": {"hostname": "test"}},
        ) as worker, patch("util.window_preparation.get_run_context") as context:
            context.return_value.task_run.run_count = 3
            response = compute_window_preparation_batch.fn(
                [{"id": "bounded"}], Path("worker.R"), 30.0, 1, safety
            )
        self.assertEqual(response["worker"]["hostname"], "test")
        worker.assert_called_once_with(
            [{"id": "bounded"}], Path("worker.R"), 30.0, 1, safety,
            retry_count=2,
        )
        self.assertEqual(compute_window_preparation_batch.name, "compute window-preparation batch")

    def test_training_and_official_inputs_share_the_exact_bounded_contract(self) -> None:
        """Equal raw 64-vectors clean identically; full-history slicing is not accepted."""
        raw = [float(index % 7) for index in range(64)]
        common = {
            "preprocessing_mode": "robust",
            "transformation": "standardise_sample_v1",
            "seasonality": 7,
        }
        jobs = [
            {
                **common,
                "id": identity,
                "windows": [{"window_id": identity, "input": raw}],
            }
            for identity in ("training-reference", "official-input")
        ]
        response = shared_distributed_execution.window_preparation_batch(
            jobs, "src/r/02_01_preprocess_series.R", 180.0, 1
        )
        training = response["results"][0]["windows"][0]
        official = response["results"][1]["windows"][0]
        self.assertEqual(training["cleaned_hash"], official["cleaned_hash"])
        self.assertEqual(training["transformed_input"], official["transformed_input"])
        self.assertEqual(training["prepared_reference"], official["prepared_reference"])

        full_history = [1000.0] * 64 + raw
        separately_cleaned = shared_distributed_execution.clean_batch(
            [{"id": "full", "context": full_history, "mode": "robust", "seasonality": 7}],
            "src/r/02_01_preprocess_series.R",
            180.0,
            1,
        )["results"][0]["values"][-64:]
        separately_transformed = shared_distributed_execution.transform(
            separately_cleaned, "standardise_sample_v1"
        )
        self.assertNotEqual(
            official["transformed_input"], list(separately_transformed.values)
        )

    def test_flow_runner_requires_explicit_local_bounds(self) -> None:
        """The migrated entry cannot silently turn distributed work into local work."""
        arguments = {
            "scheduler_address": None,
            "parent_database": Path("parent.duckdb"),
            "windows_database": Path("windows.duckdb"),
            "source_manifest_hash": "source",
            "memory_safety": None,
            "execution_profile": None,
            "retries": 1,
        }
        with self.assertRaisesRegex(RuntimeError, "explicit local limits"):
            run_window_preparation_flow(**arguments)
        with self.assertRaisesRegex(ValueError, "cannot use local limits"):
            run_window_preparation_flow(
                **{**arguments, "scheduler_address": "tcp://scheduler:8786"},
                local_limits=LOCAL_TEST_LIMITS,
            )

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

    def test_valid_changed_window_size_is_resolved_and_persisted(self) -> None:
        """Version 6 accepts configured W/H values and reloads the derived stride."""
        document = json.loads(CONFIGURATION.read_text())
        document["pipeline"]["window_preparation"]["frequencies"]["D"] = {
            "input_length": 32,
            "future_horizon": 7,
        }
        configuration = resolve_experiment_configuration(document)
        self.assertEqual(
            configuration.resolved["pipeline"]["window_preparation"]["frequencies"]["D"],
            {"input_length": 32, "future_horizon": 7, "stride": 39},
        )
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "changed.json"
            database = Path(directory) / "changed.duckdb"
            config_path.write_text(json.dumps(document))
            initialize_experiment_database(database, config_path)
            stored = load_database_configuration(database)
            self.assertEqual(
                stored.resolved["pipeline"]["window_preparation"]["frequencies"]["D"]["stride"],
                39,
            )

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

    def test_coordinator_jobs_use_tsai_windows_for_every_configured_frequency(self) -> None:
        """The actual job path uses tsai outputs across chunks, offsets, and resume gaps."""
        configuration = load_experiment_configuration(CONFIGURATION)
        frequencies = configuration.resolved["pipeline"]["window_preparation"][
            "frequencies"
        ]
        sources: dict[str, list[float]] = {}
        series = []
        for series_key, (frequency, settings) in enumerate(frequencies.items(), start=1):
            stride = settings["stride"]
            values = [float(series_key * 10000 + index) for index in range(3 + 5 * stride + 2)]
            sources[str(series_key)] = values
            series.append(
                {
                    "dataset_id": "dataset/test",
                    "series_id": str(series_key),
                    "series_key": series_key,
                    "frequency": frequency,
                    "usable_start": 3,
                    "window_count": 5,
                }
            )

        class Rows:
            """Supply the minimal DuckDB cursor contract exercised by _jobs()."""

            def __init__(self, rows):
                self.rows = rows

            def fetchall(self):
                return self.rows

            def fetchone(self):
                return self.rows[0] if self.rows else None

        class Parent:
            """Resolve bounded list_slice reads from in-memory source vectors."""

            def execute(self, query, parameters):
                del query
                start, end, _, series_id = parameters
                return Rows([(sources[series_id][start - 1 : end],)])

        class Child:
            """Expose every series as pending and one persisted resume ordinal."""

            def execute(self, query, parameters):
                if "FROM window_tasks" in query:
                    return Rows([(item["series_key"],) for item in series])
                series_key = parameters[1]
                return Rows([(1,)]) if series_key == 1 else Rows([])

        coordinator = WindowPreparationCoordinator.__new__(WindowPreparationCoordinator)
        coordinator.parent = Parent()
        coordinator.child = Child()
        coordinator.preparation_id = "preparation/test"
        coordinator.definition = configuration.resolved["pipeline"]["window_preparation"]
        periods = {frequency: {"r_period": 1} for frequency in frequencies}
        with patch(
            "util.window_preparation.rolling_window_inputs",
            wraps=rolling_window_inputs,
        ) as generate:
            jobs = list(coordinator._jobs(series, periods, windows_per_job=2))

        self.assertEqual(generate.call_count, len(frequencies) * 3)
        by_series = {item["series_key"]: [] for item in series}
        for job in jobs:
            by_series[job["series_key"]].extend(job["windows"])
        for item in series:
            settings = frequencies[item["frequency"]]
            expected_ordinals = [0, 2, 3, 4] if item["series_key"] == 1 else list(range(5))
            self.assertEqual(
                [window["window_ordinal"] for window in by_series[item["series_key"]]],
                expected_ordinals,
            )
            for window in by_series[item["series_key"]]:
                ordinal = window["window_ordinal"]
                start = 3 + ordinal * settings["stride"]
                self.assertEqual(
                    (
                        window["input_start"],
                        window["input_end"],
                        window["future_start"],
                        window["future_end"],
                    ),
                    (
                        start,
                        start + settings["input_length"],
                        start + settings["input_length"],
                        start + settings["stride"],
                    ),
                )
                self.assertEqual(
                    window["input"],
                    sources[str(item["series_key"])][
                        start : start + settings["input_length"]
                    ],
                )

    def test_constant_and_missing_inputs_are_cleaned_then_standardised(self) -> None:
        """The real worker keeps fitted state finite for risky constant/missing inputs."""
        from util.shared_distributed_execution import window_preparation_batch

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
        self.assertEqual(window["prepared_reference"], 4.0)
        self.assertIn("memory_safety", response["worker"])
        self.assertEqual(response["worker"]["memory_safety"]["safety_responses"], 0)

    def test_saved_reference_is_the_actual_cleaner_output(self) -> None:
        """The outlier fixture saves R's final cleaned value, never an inverse estimate."""
        observed_cleaning = []
        run_r = shared_distributed_execution._run_r

        def capture_cleaning(*args, **kwargs):
            """Retain the native R result used by the production worker boundary."""
            response = run_r(*args, **kwargs)
            observed_cleaning.append(response["results"][0]["values"])
            return response

        job = {
            "id": "series/outlier",
            "preprocessing_mode": "robust",
            "transformation": "standardise_sample_v1",
            "seasonality": 1,
            "windows": [
                {
                    "window_id": "window/outlier",
                    "window_ordinal": 0,
                    "input_start": 0,
                    "input_end": 64,
                    "future_start": 64,
                    "future_end": 78,
                    "input": [9999.0] + [float(value) for value in range(1, 64)],
                }
            ],
        }
        with patch(
            "util.shared_distributed_execution._run_r", side_effect=capture_cleaning
        ):
            response = shared_distributed_execution.window_preparation_batch(
                [job], "src/r/02_01_preprocess_series.R", 60, 1
            )
        window = response["results"][0]["windows"][0]
        self.assertEqual(len(observed_cleaning), 1)
        self.assertIn(observed_cleaning[0][-1], {57, 58})
        self.assertEqual(window["prepared_reference"], observed_cleaning[0][-1])
        self.assertEqual(window["cleaned_hash"], json_fingerprint(observed_cleaning[0]))

    def test_s1_exact_counts_namespaces_and_placement_independence(self) -> None:
        """Explicit rounding gives disjoint series membership independent of order."""
        identities = [("dataset-a", str(index)) for index in range(10)]
        membership, provenance = s1_partition(identities, 0.8, 123)
        self.assertEqual(list(membership.values()).count("test"), 1)
        self.assertEqual(list(membership.values()).count("train"), 9)
        repeated, _ = s1_partition(list(reversed(identities)), 0.8, 123)
        self.assertEqual(membership, repeated)
        namespaced, _ = s1_partition(
            [("dataset-a", "0"), ("dataset-b", "0")], 0.8, 123
        )
        self.assertEqual(len(namespaced), 2)
        self.assertEqual(provenance["generator_version"], "1.0.1")
        for size, expected_test in ((2, 1), (5, 1), (10, 1), (11, 2), (100, 19), (4227, 845)):
            split, _ = s1_partition(
                [("dataset", str(index)) for index in range(size)], 0.8, 123
            )
            self.assertEqual(list(split.values()).count("test"), expected_test)
        legacy, legacy_provenance = s1_partition(
            identities, 0.8, 123, "decimal_floor_v1"
        )
        self.assertEqual(list(legacy.values()).count("test"), 2)
        self.assertEqual(legacy_provenance["test_count_rule"], "decimal_floor_v1")
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
        # Scientific settings match production. A larger local-only test batch
        # avoids starting one R process per synthetic series; distributed
        # production keeps one job per Dask task to expose both worker pools.
        fixture_configuration = json.loads(CONFIGURATION.read_text())
        fixture_configuration["execution"]["default"]["batch_sizes"][
            "window_preparation"
        ] = 16
        fixture_path = root / "fixture-configuration.json"
        fixture_path.write_text(json.dumps(fixture_configuration))
        initialize_experiment_database(self.parent, fixture_path)
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
                if index == 0:
                    values[70] = None
                elif index == 1:
                    values = [8.76] * 106
                connection.execute(
                    """INSERT INTO series VALUES
                    ('dataset/test', ?, ?, ?, 'D', TIMESTAMP '2000-01-01', ?, 106,
                     ?, '{}', current_timestamp)""",
                    [str(index), f"D{index + 1}", index, values, _target_content_hash(values)],
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

    def test_configuration_without_rolling_capability_is_rejected_and_closed(self) -> None:
        """A centrally valid non-rolling configuration fails without leaking its writer."""
        parent = self.parent.with_name("standardisation-only.duckdb")
        child = self.child.with_name("unused-windows.duckdb")
        initialize_experiment_database(parent, STANDARDISATION_CONFIGURATION)
        with self.assertRaisesRegex(
            ValueError, "complete pipeline.window_preparation capability"
        ):
            WindowPreparationCoordinator(parent, child)
        with duckdb.connect(str(parent)) as connection:
            self.assertEqual(
                connection.execute("SELECT max(version) FROM schema_versions").fetchone(),
                (11,),
            )

    def test_version_11_executes_real_process_03_window_preparation(self) -> None:
        """The version-11 parent runs the bounded preparation path to durable output."""
        parent = self.parent.with_name("version-11.duckdb")
        child = self.child.with_name("version-11-windows.duckdb")
        document = json.loads(VERSION_11_CONFIGURATION.read_text())
        document["execution"]["default"]["batch_sizes"]["window_preparation"] = 16
        configuration_path = self.parent.with_name("version-11.json")
        configuration_path.write_text(json.dumps(document))
        initialize_experiment_database(parent, configuration_path)
        connection = duckdb.connect(str(parent))
        try:
            fixture_path = str(self.parent).replace("'", "''")
            connection.execute(f"ATTACH '{fixture_path}' AS fixture (READ_ONLY)")
            connection.execute("INSERT INTO datasets SELECT * FROM fixture.datasets")
            connection.execute("INSERT INTO series SELECT * FROM fixture.series")
            connection.execute(
                "INSERT INTO evaluation_windows SELECT * FROM fixture.evaluation_windows"
            )
            connection.execute("DETACH fixture")
            connection.execute(
                """UPDATE experiment_processes SET status='completed',
                   completed_at=current_timestamp WHERE process_id=1"""
            )
        finally:
            connection.close()

        with WindowPreparationCoordinator(parent, child) as coordinator:
            result = coordinator.run(
                source_manifest_hash="version-11-fixture",
                local_limits=LOCAL_TEST_LIMITS,
            )

        self.assertEqual(result["eligible"], 100)
        self.assertEqual(result["total_windows"], 100)
        self.assertEqual(result["directional_labels"], 100)
        with duckdb.connect(str(child), read_only=True) as connection:
            self.assertEqual(
                connection.execute("SELECT count(*) FROM prepared_windows").fetchone(),
                (100,),
            )

    def test_preparation_retrieval_and_restart_preserve_membership(self) -> None:
        """A second run skips completed series and preserves every persisted identity."""
        original_generator = rolling_window_inputs
        changed_input = [-1.0] + [float(value) for value in range(1, 64)]

        def changed_first_tsai_window(*args, **kwargs):
            """Make one tsai result distinctive so persistence provenance is observable."""
            windows = [
                {**window, "input": list(window["input"])}
                for window in original_generator(*args, **kwargs)
            ]
            if kwargs.get("offset") == 0 and windows and windows[0]["input"][0] == 0.0:
                windows[0]["input"] = changed_input
            return tuple(windows)

        with WindowPreparationCoordinator(self.parent, self.child) as coordinator:
            with patch(
                "util.window_preparation.rolling_window_inputs",
                side_effect=changed_first_tsai_window,
            ) as generate:
                tracemalloc.start()
                started = time.monotonic()
                try:
                    first = coordinator.run(
                        source_manifest_hash="fixture-manifest",
                        local_limits=LOCAL_TEST_LIMITS,
                    )
                finally:
                    elapsed = time.monotonic() - started
                    _, peak_bytes = tracemalloc.get_traced_memory()
                    tracemalloc.stop()
        self.assertEqual(generate.call_count, 100)
        self.assertEqual(first["eligible"], 100)
        self.assertEqual(first["zero_window"], 0)
        self.assertEqual(first["series_by_partition"], {"train": 81, "test": 19})
        self.assertEqual(first["total_windows"], 100)
        self.assertEqual(first["directional_labels"], 100)
        self.assertEqual(first["stored_directional_label_rows"], 100)
        self.assertTrue(first["directional_labels_complete"])
        self.assertEqual(first["directional_labels_unavailable"], 0)
        self.assertIsNone(first["directional_labels_unavailable_reason"])
        self.assertEqual(first["backfilled_directional_labels"], 0)
        self.assertEqual(first["label_definition_id"], "directional_strict_v1")
        self.assertEqual(first["periods"]["D"]["r_period"], 1)
        self.assertLess(elapsed, 120.0)
        self.assertLess(peak_bytes, 192 * 1024 * 1024)

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
            labels_before = child.execute(
                """SELECT window_id, definition_id, labels, reference_value, reference_source
                   FROM window_directional_labels ORDER BY window_id"""
            ).fetchall()
            self.assertEqual(len(labels_before), 100)
            self.assertEqual({row[4] for row in labels_before}, {EXACT_LABEL_REFERENCE_SOURCE})
            self.assertEqual(
                child.execute(
                    """SELECT version, rule, reference_policy, target_policy
                       FROM directional_label_definitions"""
                ).fetchone(),
                (
                    1,
                    "1 when value > reference; otherwise 0; missing value unavailable",
                    "last prepared input value on original scale",
                    "untouched parent future positions; missing remains unavailable",
                ),
            )
            parent = duckdb.connect(str(self.parent), read_only=True)
            try:
                series_zero_key = parent.execute(
                    """SELECT s.series_key FROM series_lookup s
                       JOIN dataset_lookup d USING (dataset_key)
                       WHERE d.dataset_id='dataset/test' AND s.series_id='0'"""
                ).fetchone()[0]
            finally:
                parent.close()
            self.assertEqual(
                child.execute(
                    "SELECT input_hash FROM prepared_windows WHERE series_key=?",
                    [series_zero_key],
                ).fetchone()[0],
                json_fingerprint(changed_input),
            )
        finally:
            child.close()

        with WindowPreparationCoordinator(self.parent, self.child) as coordinator:
            second = coordinator.run(
                source_manifest_hash="fixture-manifest", local_limits=LOCAL_TEST_LIMITS
            )
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
            self.assertEqual(
                child.execute(
                    """SELECT window_id, definition_id, labels, reference_value, reference_source
                       FROM window_directional_labels ORDER BY window_id"""
                ).fetchall(),
                labels_before,
            )
        finally:
            child.close()
        self.assertEqual(after, before)

        exact = get_prepared_window(
            self.parent,
            self.child,
            dataset_id="dataset/test",
            series_id="1",
            window_ordinal=0,
        )
        self.assertEqual(exact.label_reference, exact.future[0])
        self.assertEqual(exact.directional_labels, (0,) * 14)
        self.assertIsNone(exact.label_unavailable_reason)
        inverse_reference = math.nextafter(exact.label_reference, -math.inf)

        # Reproduce the strict-tie failure in an accepted inverse-derived row.
        # Retrieval and restart must report it, not trust or rewrite it.
        child = duckdb.connect(str(self.child))
        try:
            inverse_row = child.execute(
                """SELECT window_id, labels FROM window_directional_labels
                   WHERE reference_value=?""",
                [exact.label_reference],
            ).fetchone()
            manufactured = list(inverse_row[1])
            manufactured[0] = 1
            child.execute(
                """UPDATE window_directional_labels
                   SET labels=?, reference_value=?, reference_source=? WHERE window_id=?""",
                [
                    manufactured,
                    inverse_reference,
                    "reconstructed from persisted affine state for schema-v2 compatibility",
                    inverse_row[0],
                ],
            )
        finally:
            child.close()
        unavailable = get_prepared_window(
            self.parent,
            self.child,
            dataset_id="dataset/test",
            series_id="1",
            window_ordinal=0,
        )
        self.assertIsNone(unavailable.directional_labels)
        self.assertIsNone(unavailable.label_reference)
        self.assertEqual(unavailable.label_unavailable_reason, EXACT_REFERENCE_UNAVAILABLE_REASON)
        with WindowPreparationCoordinator(self.parent, self.child) as coordinator:
            inverse_restart = coordinator.run(
                source_manifest_hash="fixture-manifest", local_limits=LOCAL_TEST_LIMITS
            )
        self.assertEqual(inverse_restart["directional_labels"], 99)
        self.assertEqual(inverse_restart["stored_directional_label_rows"], 100)
        self.assertFalse(inverse_restart["directional_labels_complete"])
        self.assertEqual(inverse_restart["directional_labels_unavailable"], 1)
        child = duckdb.connect(str(self.child), read_only=True)
        try:
            self.assertEqual(
                child.execute(
                    """SELECT labels[1], reference_value, reference_source
                       FROM window_directional_labels WHERE window_id=?""",
                    [inverse_row[0]],
                ).fetchone(),
                (
                    1,
                    inverse_reference,
                    "reconstructed from persisted affine state for schema-v2 compatibility",
                ),
            )
        finally:
            child.close()

        # Emulate an accepted schema-v2 child. Resume must preserve every window
        # and membership while leaving labels unavailable without exact references.
        child = duckdb.connect(str(self.child))
        try:
            child.execute("DROP TABLE window_directional_labels")
            child.execute("DROP TABLE directional_label_definitions")
            child.execute("DELETE FROM window_schema_versions WHERE version=3")
        finally:
            child.close()
        with WindowPreparationCoordinator(self.parent, self.child) as coordinator:
            migrated = coordinator.run(
                source_manifest_hash="fixture-manifest", local_limits=LOCAL_TEST_LIMITS
            )
        self.assertEqual(migrated["backfilled_directional_labels"], 0)
        self.assertEqual(migrated["directional_labels"], 0)
        self.assertEqual(migrated["stored_directional_label_rows"], 0)
        self.assertFalse(migrated["directional_labels_complete"])
        self.assertEqual(migrated["directional_labels_unavailable"], 100)
        self.assertEqual(
            migrated["directional_labels_unavailable_reason"],
            EXACT_REFERENCE_UNAVAILABLE_REASON,
        )
        self.assertEqual(migrated["worker_series_counts"], {})
        child = duckdb.connect(str(self.child), read_only=True)
        try:
            self.assertEqual(
                child.execute("SELECT count(*) FROM window_directional_labels").fetchone()[0],
                0,
            )
            self.assertEqual(
                child.execute(
                    "SELECT series_key, partition FROM series_membership ORDER BY series_key"
                ).fetchall(),
                membership,
            )
        finally:
            child.close()

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
            reconciled = coordinator.run(
                source_manifest_hash="fixture-manifest", local_limits=LOCAL_TEST_LIMITS
            )
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
        expected_future = [float(value) for value in range(64, 78)]
        expected_future[6] = None
        self.assertEqual(selected.future, tuple(expected_future))
        self.assertIsNone(selected.directional_labels)
        self.assertEqual(selected.label_definition_id, "directional_strict_v1")
        self.assertIsNone(selected.label_reference)
        self.assertEqual(
            selected.label_unavailable_reason,
            EXACT_REFERENCE_UNAVAILABLE_REASON,
        )
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
            coordinator.run(
                source_manifest_hash="reviewed-source-a", local_limits=LOCAL_TEST_LIMITS
            )
        with WindowPreparationCoordinator(self.parent, self.child) as coordinator:
            with self.assertRaisesRegex(RuntimeError, "source manifest"):
                coordinator.run(
                    source_manifest_hash="different-source-b",
                    local_limits=LOCAL_TEST_LIMITS,
                )

    def test_direct_execution_guard_rejects_before_writes_or_worker_launch(self) -> None:
        """Malformed, oversized, and falsely distributed direct calls have no side effects."""
        invalid_calls = (
            (None, None, "requires explicit"),
            ({}, None, "requires explicit"),
            ({"max_series": 100}, None, "requires explicit"),
            ({"max_series": 0, "max_windows": 1}, None, "positive integer"),
            ({"max_series": True, "max_windows": 1}, None, "positive integer"),
            ({"max_series": 101, "max_windows": 200}, None, "safety policy"),
            ({"max_series": 100, "max_windows": 201}, None, "safety policy"),
            (LOCAL_TEST_LIMITS, "poc2_seasonal_recovery", "cannot claim"),
        )
        for limits, profile, message in invalid_calls:
            with self.subTest(limits=limits, profile=profile):
                with (
                    WindowPreparationCoordinator(self.parent, self.child) as coordinator,
                    patch("util.window_preparation.window_preparation_batch") as worker,
                    self.assertRaisesRegex((RuntimeError, ValueError), message),
                ):
                    coordinator.run(
                        source_manifest_hash="fixture-manifest",
                        local_limits=limits,
                        execution_profile=profile,
                    )
                worker.assert_not_called()
                self.assertFalse(self.child.exists())
        parent = duckdb.connect(str(self.parent), read_only=True)
        try:
            self.assertEqual(parent.execute("SELECT count(*) FROM series_lookup").fetchone()[0], 0)
            self.assertEqual(
                parent.execute("SELECT count(*) FROM window_preparation_runs").fetchone()[0], 0
            )
        finally:
            parent.close()

    def test_explicit_local_bounds_are_enforced_before_writes(self) -> None:
        """A declared focused limit blocks an oversized workload without child state."""
        with WindowPreparationCoordinator(self.parent, self.child) as coordinator:
            with self.assertRaisesRegex(RuntimeError, "before writes"):
                coordinator.run(
                    source_manifest_hash="fixture-manifest",
                    local_limits={"max_series": 10, "max_windows": 10},
                )
        self.assertFalse(self.child.exists())
        parent = duckdb.connect(str(self.parent), read_only=True)
        try:
            self.assertEqual(parent.execute("SELECT count(*) FROM series_lookup").fetchone()[0], 0)
            self.assertEqual(
                parent.execute("SELECT count(*) FROM window_preparation_runs").fetchone()[0], 0
            )
        finally:
            parent.close()

    def test_retrieval_rejects_wrong_parent_with_same_series_identifiers(self) -> None:
        """Changed observations cannot be paired to a child by numeric aliases alone."""
        with WindowPreparationCoordinator(self.parent, self.child) as coordinator:
            coordinator.run(
                source_manifest_hash="fixture-manifest", local_limits=LOCAL_TEST_LIMITS
            )
        wrong_parent = self.parent.with_name("wrong-parent.duckdb")
        shutil.copy2(self.parent, wrong_parent)
        connection = duckdb.connect(str(wrong_parent))
        try:
            values = [999.0] * 106
            connection.execute(
                """UPDATE series SET target=?, content_hash=?
                   WHERE dataset_id='dataset/test' AND series_id='0'""",
                [values, _target_content_hash(values)],
            )
        finally:
            connection.close()
        with self.assertRaisesRegex(RuntimeError, "source identity"):
            get_prepared_window(
                wrong_parent,
                self.child,
                dataset_id="dataset/test",
                series_id="0",
                window_ordinal=0,
            )

    def test_long_series_is_chunked_and_partial_resume_is_exact(self) -> None:
        """One long source is bounded into chunks and resumes missing chunks exactly."""
        values = [float(step) for step in range(78 * 40 + 28)]
        connection = duckdb.connect(str(self.parent))
        try:
            connection.execute(
                """UPDATE series SET target=?, observation_count=?, content_hash=?
                   WHERE dataset_id='dataset/test' AND series_id='0'""",
                [values, len(values), _target_content_hash(values)],
            )
            connection.execute(
                """UPDATE evaluation_windows SET train_end=?, validation_start=?,
                   validation_end=?, test_start=?, test_end=?
                   WHERE dataset_id='dataset/test' AND series_id='0'""",
                [78 * 40, 78 * 40, 78 * 40 + 14, 78 * 40 + 14, len(values)],
            )
        finally:
            connection.close()
        with WindowPreparationCoordinator(self.parent, self.child) as coordinator:
            first = coordinator.run(
                source_manifest_hash="fixture-manifest", local_limits=LOCAL_TEST_LIMITS
            )
        self.assertEqual(first["memory_bounds"]["windows_per_job"], 16)
        self.assertEqual(first["memory_bounds"]["jobs_per_batch"], 16)
        self.assertEqual(first["memory_bounds"]["maximum_windows_per_batch"], 256)
        child = duckdb.connect(str(self.child))
        try:
            series_key = child.execute(
                "SELECT series_key FROM series_membership ORDER BY usable_length DESC LIMIT 1"
            ).fetchone()[0]
            expected = child.execute(
                """SELECT window_ordinal, transformed_hash FROM prepared_windows
                   WHERE series_key=? ORDER BY window_ordinal""",
                [series_key],
            ).fetchall()
            self.assertEqual(len(expected), 40)
            self.assertEqual(
                child.execute(
                    "SELECT attempt_count FROM window_tasks WHERE series_key=?", [series_key]
                ).fetchone()[0],
                3,
            )
            child.execute(
                "DELETE FROM prepared_windows WHERE series_key=? AND window_ordinal>=32",
                [series_key],
            )
            child.execute(
                "UPDATE window_tasks SET status='pending' WHERE series_key=?", [series_key]
            )
        finally:
            child.close()
        with WindowPreparationCoordinator(self.parent, self.child) as coordinator:
            resumed = coordinator.run(
                source_manifest_hash="fixture-manifest", local_limits=LOCAL_TEST_LIMITS
            )
        self.assertTrue(resumed["worker_series_counts"])
        child = duckdb.connect(str(self.child), read_only=True)
        try:
            observed = child.execute(
                """SELECT window_ordinal, transformed_hash FROM prepared_windows
                   WHERE series_key=? ORDER BY window_ordinal""",
                [series_key],
            ).fetchall()
            self.assertEqual(observed, expected)
            self.assertEqual(
                child.execute("SELECT count(*) FROM prepared_windows").fetchone()[0], 139
            )
        finally:
            child.close()


if __name__ == "__main__":
    unittest.main()
