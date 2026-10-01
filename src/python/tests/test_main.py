# ==============================================================================
# test_main.py
#
# Purpose: Verify researcher CLI parsing, plan/status/results rendering, dispatch, and failure exit codes with mocked coordinators.
# Inputs: unittest fixtures, temporary databases/files, deterministic synthetic records, and mocked process or cluster boundaries.
# Outputs: unittest pass/fail assertions and captured diagnostics; no production artifacts or external services.
# Run from: PYTHONPATH=src/python .tools/uv/uv run --locked --no-sync python -m unittest tests.test_main
# ==============================================================================

"""Verify researcher CLI parsing, plan/status/results rendering, dispatch, and failure exit codes with mocked coordinators."""

import importlib.util
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import duckdb


# Test/calibration value: repository root derived from this fixture's location.
ROOT = Path(__file__).resolve().parents[3]
# Test/calibration value: import specification derived from ROOT for isolated CLI tests.
SPEC = importlib.util.spec_from_file_location("shapefm_main", ROOT / "src/python/00_main.py")
assert SPEC is not None and SPEC.loader is not None
# Test/calibration value: in-memory CLI module loaded from SPEC; tests patch its interfaces.
MAIN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MAIN)


@dataclass(frozen=True)
class StoredForecast:
    """Represent an immutable forecast returned by CLI test doubles.

    Purpose: Supply the fields serialized by the results action.
    Inputs: Forecast identity, candidate name, and ordered mean values.
    Outputs: Immutable fixture state only; this data object owns no side effects.
    """
    forecast_id: str
    candidate: str
    mean: tuple[float, ...]


@dataclass(frozen=True)
class StoredPreparedWindow:
    """Supply the fields serialized by the prepared-window results route."""

    window_id: str
    dataset_id: str
    series_id: str
    window_ordinal: int


class MainResultsTests(unittest.TestCase):
    """Exercise CLI result selection, JSON rendering, and read-only failures.

    Purpose: Verify results dispatch without invoking production coordinators.
    Inputs: Temporary database paths, selectors, and patched result providers.
    Outputs: Captured stdout/stderr and assertions; tests own temporary files only.
    """
    def run_main(self, arguments):
        """Invoke the CLI while capturing its process-style result.

        Purpose: Patch deterministic invocation metadata and isolate CLI stream output.
        Inputs: Command-line argument sequence accepted by ``MAIN.main``.
        Outputs: Return ``(status, stdout, stderr)`` strings; mutate no persistent
        state beyond side effects deliberately exercised by the selected action.
        """
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            patch.object(MAIN, "invocation_record", return_value={"action": "results"}),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            status = MAIN.main(arguments)
        return status, stdout.getvalue(), stderr.getvalue()

    def test_results_parser_and_existing_actions_are_unchanged(self):
        """The results command and existing commands retain their documented defaults."""
        parser = MAIN.build_parser()
        results = parser.parse_args(["results"])
        self.assertEqual(results.action, "results")
        self.assertEqual(results.database, MAIN.DEFAULT_DATABASE)
        self.assertIsNone(results.experiment_id)
        self.assertIsNone(results.variant_id)
        self.assertIsNone(results.series_id)
        self.assertIsNone(results.candidate)

        plan = parser.parse_args(["plan"])
        self.assertEqual(plan.configuration, MAIN.DEFAULT_CONFIGURATION)
        run = parser.parse_args(["run", "--database", "experiment.duckdb"])
        self.assertEqual(run.processes, tuple(range(1, 7)))
        self.assertIsNone(run.execution_profile)
        self.assertIsNone(run.local_heavy_exception)
        recovery = parser.parse_args(
            [
                "run",
                "--database",
                "experiment.duckdb",
                "--processes",
                "4",
                "--execution-profile",
                "poc2_seasonal_recovery",
            ]
        )
        self.assertEqual(recovery.processes, (4,))
        self.assertEqual(recovery.execution_profile, "poc2_seasonal_recovery")
        exception = parser.parse_args(
            [
                "run",
                "--database",
                "experiment.duckdb",
                "--processes",
                "4",
                "--local-heavy-exception",
                "researcher-approval/example",
            ]
        )
        self.assertEqual(exception.local_heavy_exception, "researcher-approval/example")
        status = parser.parse_args(["status"])
        self.assertEqual(status.database, MAIN.DEFAULT_DATABASE)
        self.assertIsNone(status.experiment_id)
        acceptance = parser.parse_args(["test"])
        self.assertEqual(acceptance.database, MAIN.DEFAULT_DATABASE)
        self.assertEqual(acceptance.report, MAIN.DEFAULT_REPORT)

    def test_default_results_use_latest_experiment_and_official_results(self):
        """Unqualified results report official evaluations for the latest experiment."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "results.duckdb"
            database.touch()
            evaluations = [{"evaluation_id": "evaluation/1", "metrics": {"MASE": 1.0}}]
            with (
                patch.object(MAIN, "latest_experiment_id", return_value="experiment/latest") as latest,
                patch.object(MAIN, "official_results", return_value=evaluations) as official,
                patch.object(MAIN, "get_forecast") as forecast,
                patch.object(MAIN, "ExperimentCoordinator") as coordinator,
            ):
                status, stdout, stderr = self.run_main(
                    ["results", "--database", str(database)]
                )
            self.assertEqual(status, 0, stderr)
            self.assertEqual(
                json.loads(stdout),
                {
                    "experiment_id": "experiment/latest",
                    "invocation": {"action": "results"},
                    "results": evaluations,
                },
            )
            latest.assert_called_once_with(database.resolve())
            official.assert_called_once_with(database.resolve(), "experiment/latest")
            forecast.assert_not_called()
            coordinator.assert_not_called()

    def test_explicit_experiment_skips_latest_resolution(self):
        """An explicit experiment ID bypasses latest-experiment lookup."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "results.duckdb"
            database.touch()
            with (
                patch.object(MAIN, "latest_experiment_id") as latest,
                patch.object(
                    MAIN,
                    "official_results",
                    return_value=[{"evaluation_id": "evaluation/explicit"}],
                ) as official,
            ):
                status, stdout, stderr = self.run_main(
                    [
                        "results",
                        "--database",
                        str(database),
                        "--experiment-id",
                        "experiment/explicit",
                    ]
                )
            self.assertEqual(status, 0, stderr)
            self.assertEqual(json.loads(stdout)["experiment_id"], "experiment/explicit")
            latest.assert_not_called()
            official.assert_called_once_with(database.resolve(), "experiment/explicit")

    def test_complete_forecast_selectors_return_stored_forecast(self):
        """A complete selector tuple returns the matching forecast as JSON."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "results.duckdb"
            database.touch()
            stored = StoredForecast("forecast/1", "equal_weight", (1.0, 2.0))
            with (
                patch.object(MAIN, "latest_experiment_id", return_value="experiment/1"),
                patch.object(MAIN, "official_results") as official,
                patch.object(MAIN, "get_forecast", return_value=stored) as forecast,
                patch.object(MAIN, "ExperimentCoordinator") as coordinator,
            ):
                status, stdout, stderr = self.run_main(
                    [
                        "results",
                        "--database",
                        str(database),
                        "--variant-id",
                        "variant/1",
                        "--series-id",
                        "0",
                        "--candidate",
                        "equal_weight",
                    ]
                )
            self.assertEqual(status, 0, stderr)
            output = json.loads(stdout)
            self.assertEqual(output["experiment_id"], "experiment/1")
            self.assertEqual(output["forecast"]["forecast_id"], "forecast/1")
            self.assertEqual(output["forecast"]["mean"], [1.0, 2.0])
            forecast.assert_called_once_with(
                database.resolve(),
                "experiment/1",
                "variant/1",
                "0",
                "equal_weight",
            )
            official.assert_not_called()
            coordinator.assert_not_called()

    def test_complete_window_selectors_return_prepared_window(self):
        """The results action exposes the read-only prepared-window contract."""
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory) / "parent.duckdb"
            child = Path(directory) / "windows.duckdb"
            selected = StoredPreparedWindow("window/1", "dataset/1", "7", 2)
            with patch.object(
                MAIN, "get_prepared_window", return_value=selected
            ) as retrieve:
                status, stdout, stderr = self.run_main(
                    [
                        "results",
                        "--database",
                        str(parent),
                        "--windows-database",
                        str(child),
                        "--dataset-id",
                        "dataset/1",
                        "--series-id",
                        "7",
                        "--window-ordinal",
                        "2",
                    ]
                )
            self.assertEqual(status, 0, stderr)
            self.assertEqual(json.loads(stdout)["prepared_window"]["window_id"], "window/1")
            retrieve.assert_called_once_with(
                parent.resolve(),
                child,
                dataset_id="dataset/1",
                series_id="7",
                window_ordinal=2,
            )

    def test_prepare_windows_action_dispatches_explicitly(self):
        """Preparation is opt-in and forwards parent, child, and profile exactly."""
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory) / "parent.duckdb"
            child = Path(directory) / "windows.duckdb"
            parent.touch()
            with patch.object(
                MAIN,
                "run_window_preparation",
                return_value={"preparation_id": "preparation/1"},
            ) as prepare:
                status, stdout, stderr = self.run_main(
                    [
                        "prepare-windows",
                        "--database",
                        str(parent),
                        "--windows-database",
                        str(child),
                        "--execution-profile",
                        "poc2_seasonal_recovery",
                    ]
                )
            self.assertEqual(status, 0, stderr)
            self.assertEqual(
                json.loads(stdout)["window_preparation"]["preparation_id"],
                "preparation/1",
            )
            prepare.assert_called_once_with(
                parent.resolve(), child.resolve(), "poc2_seasonal_recovery", None, None
            )

    def test_unbounded_local_window_preparation_is_rejected_before_io(self):
        """Omitting both approved profile and local bounds fails before database access."""
        with self.assertRaisesRegex(RuntimeError, "local window preparation requires"):
            MAIN.run_window_preparation(
                Path("does-not-exist.duckdb"), Path("not-created.duckdb")
            )
        self.assertFalse(Path("not-created.duckdb").exists())

    def test_incomplete_forecast_selectors_fail_before_database_access(self):
        """Partial forecast selectors fail before querying or creating a database."""
        selectors = {
            "--variant-id": "variant/1",
            "--series-id": "0",
            "--candidate": "equal_weight",
        }
        combinations = (
            ("--variant-id",),
            ("--series-id",),
            ("--candidate",),
            ("--variant-id", "--series-id"),
            ("--variant-id", "--candidate"),
            ("--series-id", "--candidate"),
        )
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "missing.duckdb"
            for selected in combinations:
                arguments = ["results", "--database", str(database)]
                for option in selected:
                    arguments.extend((option, selectors[option]))
                with (
                    self.subTest(selected=selected),
                    patch.object(MAIN, "latest_experiment_id") as latest,
                    patch.object(MAIN, "official_results") as official,
                    patch.object(MAIN, "get_forecast") as forecast,
                    patch.object(MAIN, "ExperimentCoordinator") as coordinator,
                ):
                    status, _, stderr = self.run_main(arguments)
                    self.assertEqual(status, 1)
                    self.assertIn("must be supplied together", stderr)
                    latest.assert_not_called()
                    official.assert_not_called()
                    forecast.assert_not_called()
                    coordinator.assert_not_called()
                    self.assertFalse(database.exists())

    def test_missing_database_fails_without_creating_it(self):
        """Results fail clearly when the database is absent and leave no new file."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "missing.duckdb"
            with (
                patch.object(MAIN, "latest_experiment_id") as latest,
                patch.object(MAIN, "official_results") as official,
                patch.object(MAIN, "ExperimentCoordinator") as coordinator,
            ):
                status, _, stderr = self.run_main(
                    ["results", "--database", str(database)]
                )
            self.assertEqual(status, 1)
            self.assertIn("database does not exist", stderr)
            self.assertFalse(database.exists())
            latest.assert_not_called()
            official.assert_not_called()
            coordinator.assert_not_called()

    def test_missing_experiment_fails_clearly(self):
        """Latest-experiment lookup errors are reported with a failing status."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "results.duckdb"
            database.touch()
            with patch.object(
                MAIN,
                "latest_experiment_id",
                side_effect=RuntimeError("no experiment is planned"),
            ):
                status, _, stderr = self.run_main(
                    ["results", "--database", str(database)]
                )
            self.assertEqual(status, 1)
            self.assertIn("no experiment is planned", stderr)

    def test_missing_official_evaluations_fail_clearly(self):
        """An experiment without official evaluations produces a clear failure."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "results.duckdb"
            database.touch()
            with patch.object(MAIN, "official_results", return_value=[]):
                status, _, stderr = self.run_main(
                    [
                        "results",
                        "--database",
                        str(database),
                        "--experiment-id",
                        "experiment/without-results",
                    ]
                )
            self.assertEqual(status, 1)
            self.assertIn("no official evaluations", stderr)

    def test_missing_forecast_fails_clearly(self):
        """A missing selected forecast produces a clear failure."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "results.duckdb"
            database.touch()
            with patch.object(
                MAIN, "get_forecast", side_effect=KeyError("forecast not found")
            ):
                status, _, stderr = self.run_main(
                    [
                        "results",
                        "--database",
                        str(database),
                        "--experiment-id",
                        "experiment/1",
                        "--variant-id",
                        "variant/1",
                        "--series-id",
                        "0",
                        "--candidate",
                        "equal_weight",
                    ]
                )
            self.assertEqual(status, 1)
            self.assertIn("forecast not found", stderr)


class ProcessWrapperTests(unittest.TestCase):
    """Verify explicit numbered-wrapper discovery and validation.

    Purpose: Ensure Processes 01–06 map to complete repository-controlled modules.
    Inputs: The committed wrapper files and temporary malformed wrapper fixtures.
    Outputs: Loader contract assertions; production files and databases are unchanged.
    """

    def test_all_process_wrappers_exist_and_load_with_the_required_contract(self):
        """Each process maps to its numbered file with a matching callable contract."""
        expected = {
            1: "01_import.py",
            2: "02_preprocess.py",
            3: "03_transform.py",
            4: "04_forecast.py",
            5: "05_combine.py",
            6: "06_evaluate.py",
        }
        self.assertEqual(
            {key: value.name for key, value in MAIN.PROCESS_WRAPPER_PATHS.items()},
            expected,
        )
        for process_id, path in MAIN.PROCESS_WRAPPER_PATHS.items():
            with self.subTest(process_id=process_id):
                self.assertTrue(path.is_file())
                wrapper = MAIN.load_process_wrapper(process_id)
                self.assertEqual(wrapper.PROCESS_NUMBER, process_id)
                self.assertTrue(callable(wrapper.run))

    def test_missing_and_mismatched_wrappers_fail_clearly(self):
        """Missing files and wrong process declarations are rejected before dispatch."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            missing = root / "missing.py"
            with (
                patch.dict(MAIN.PROCESS_WRAPPER_PATHS, {1: missing}),
                self.assertRaisesRegex(FileNotFoundError, "Process 01 wrapper does not exist"),
            ):
                MAIN.load_process_wrapper(1)

            mismatched = root / "mismatched.py"
            mismatched.write_text("PROCESS_NUMBER = 2\ndef run(database):\n    return {}\n")
            with (
                patch.dict(MAIN.PROCESS_WRAPPER_PATHS, {1: mismatched}),
                self.assertRaisesRegex(RuntimeError, "declares PROCESS_NUMBER=2"),
            ):
                MAIN.load_process_wrapper(1)

            incomplete = root / "incomplete.py"
            incomplete.write_text("PROCESS_NUMBER = 1\nrun = None\n")
            with (
                patch.dict(MAIN.PROCESS_WRAPPER_PATHS, {1: incomplete}),
                self.assertRaisesRegex(RuntimeError, "has no callable run"),
            ):
                MAIN.load_process_wrapper(1)


class MainRunTests(unittest.TestCase):
    """Exercise configured run creation, resume, prerequisites, and event state.

    Purpose: Verify process dispatch and durable lifecycle records with mocked workers.
    Inputs: The reference configuration and per-test temporary DuckDB paths.
    Outputs: DuckDB process/event mutations and assertions; this class owns cleanup.
    """

    def setUp(self):
        """Create isolated run-test state.

        Purpose: Provide paths for disposable configuration and DuckDB artifacts.
        Inputs: The committed reference configuration path.
        Outputs: Sets ``temporary``, ``root``, and ``configuration`` on the fixture.
        """
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.configuration = ROOT / "config/experiments/poc2_m4_daily_100.json"

    def tearDown(self):
        """Release all run-test filesystem state.

        Purpose: Prevent databases and copied configurations leaking between tests.
        Inputs: The ``TemporaryDirectory`` owned by this fixture.
        Outputs: Deletes its directory and every contained file.
        """
        self.temporary.cleanup()

    def test_new_database_requires_configuration_without_creating_a_file(self):
        """A fresh path without --configuration fails before DuckDB is opened."""
        database = self.root / "missing.duckdb"
        with self.assertRaisesRegex(ValueError, "configuration is required"):
            MAIN.run_configured_processes(database, None, (1,))
        self.assertFalse(database.exists())

    def test_tuning_run_without_profile_is_rejected_before_wrapper_dispatch(self):
        """The ordinary CLI cannot silently run the 100-series tuning case locally."""
        database = self.root / "tuning.duckdb"
        tuning = ROOT / "config/experiments/poc2_m4_daily_100_period_tuning.json"
        MAIN.initialize_experiment_database(database, tuning)
        with (
            patch.object(MAIN, "load_process_wrapper") as loader,
            self.assertRaisesRegex(RuntimeError, "requires --execution-profile"),
        ):
            MAIN.run_configured_processes(database, None, (4,))
        loader.assert_not_called()

    def test_local_heavy_exception_is_explicit_and_recorded(self):
        """A bounded exception records its approval reference in execution provenance."""
        database = self.root / "exception.duckdb"
        tuning = ROOT / "config/experiments/poc2_m4_daily_100_period_tuning.json"
        MAIN.initialize_experiment_database(database, tuning)
        connection = duckdb.connect(str(database))
        try:
            connection.execute(
                "UPDATE experiment_processes SET status='completed', completed_at=current_timestamp"
            )
        finally:
            connection.close()
        result = MAIN.run_configured_processes(
            database,
            None,
            (4,),
            local_heavy_exception="researcher-approval/test-only",
        )
        self.assertEqual(result["processes"][0]["status"], "skipped_completed")
        connection = duckdb.connect(str(database), read_only=True)
        try:
            operational = json.loads(
                connection.execute(
                    "SELECT operational_configuration FROM execution_events"
                ).fetchone()[0]
            )
        finally:
            connection.close()
        self.assertEqual(
            operational["execution"]["profile_version"], 1
        )

    def test_existing_database_rejects_a_competing_configuration(self):
        """Resume rejects JSON so stored configuration remains the sole authority."""
        database = self.root / "existing.duckdb"
        MAIN.initialize_experiment_database(database, self.configuration)
        with self.assertRaisesRegex(ValueError, "only valid when creating"):
            MAIN.run_configured_processes(database, self.configuration, (1,))

    def test_process_one_initializes_database_and_records_execution_event(self):
        """A configured Process 01 run persists completion and execution evidence."""
        database = self.root / "experiment.duckdb"
        process_run = MagicMock(return_value={
            "selected_series": 100,
            "series_count": 100,
        })
        wrapper = SimpleNamespace(run=process_run)
        with patch.object(MAIN, "load_process_wrapper", return_value=wrapper) as loader:
            result = MAIN.run_configured_processes(
                database, self.configuration, (1,)
            )
        loader.assert_called_once_with(1)
        process_run.assert_called_once_with(database)
        self.assertEqual(result["configuration"]["source"], "DuckDB")
        self.assertEqual(result["processes"][0]["status"], "completed")
        connection = duckdb.connect(str(database), read_only=True)
        try:
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM experiment_processes WHERE process_id=1"
                ).fetchone()[0],
                "completed",
            )
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM execution_events"
                ).fetchone()[0],
                "completed",
            )
        finally:
            connection.close()

    def test_resume_uses_duckdb_and_skips_completed_processes_without_json(self):
        """Removing creation JSON cannot affect a restart that has no incomplete work."""
        configuration = self.root / "creation.json"
        configuration.write_bytes(self.configuration.read_bytes())
        database = self.root / "resume.duckdb"
        MAIN.initialize_experiment_database(database, configuration)
        connection = duckdb.connect(str(database))
        try:
            connection.execute(
                "UPDATE experiment_processes SET status='completed', completed_at=current_timestamp"
            )
        finally:
            connection.close()
        configuration.unlink()
        with patch.object(MAIN, "load_process_wrapper") as loader:
            result = MAIN.run_configured_processes(
                database, None, tuple(range(1, 7))
            )
        self.assertEqual(
            [item["status"] for item in result["processes"]],
            ["skipped_completed"] * 6,
        )
        loader.assert_not_called()

    def test_resume_closes_interrupted_parent_records_before_zero_work_skip(self):
        """A later sole-coordinator run records stale execution/invocation interruption."""
        database = self.root / "interrupted.duckdb"
        MAIN.initialize_experiment_database(database, self.configuration)
        connection = duckdb.connect(str(database))
        try:
            connection.execute(
                "UPDATE experiment_processes SET status='completed', completed_at=current_timestamp"
            )
            connection.execute(
                """INSERT INTO execution_events
                (execution_id, requested_processes, operational_configuration,
                 machine, status) VALUES ('old-execution', '[4]', '{}', '{}', 'running')"""
            )
            connection.execute(
                """INSERT INTO experiment_invocations
                (invocation_id, experiment_id, requested_gate, worker_count,
                 device, batch_size, environment, machine, started_at, status)
                VALUES ('old-invocation', 'experiment', 'forecast', 1, 'cpu', 1,
                        '{}', '{}', current_timestamp, 'running')"""
            )
        finally:
            connection.close()

        result = MAIN.run_configured_processes(database, None, (4,))
        self.assertEqual(result["processes"], [{"process_id": 4, "status": "skipped_completed"}])
        connection = duckdb.connect(str(database), read_only=True)
        try:
            self.assertEqual(
                connection.execute(
                    "SELECT status, completed_at IS NOT NULL, error FROM execution_events WHERE execution_id='old-execution'"
                ).fetchone(),
                (
                    "failed",
                    True,
                    "interrupted before completion; recovered by a later run",
                ),
            )
            self.assertEqual(
                connection.execute(
                    "SELECT status, ended_at IS NOT NULL, error FROM experiment_invocations WHERE invocation_id='old-invocation'"
                ).fetchone(),
                (
                    "failed",
                    True,
                    "interrupted before completion; recovered by a later run",
                ),
            )
        finally:
            connection.close()

    def test_selected_wrappers_run_in_process_order(self):
        """Selected wrappers execute once each in ascending requested order."""
        database = self.root / "ordered.duckdb"
        calls: list[tuple[int, Path]] = []

        def load_wrapper(process_id):
            """Return a test wrapper that records its process and database."""
            return SimpleNamespace(
                run=lambda path, selected=process_id: (
                    calls.append((selected, path)) or {"process": selected}
                )
            )

        with patch.object(MAIN, "load_process_wrapper", side_effect=load_wrapper):
            result = MAIN.run_configured_processes(
                database, self.configuration, (1, 2, 3)
            )
        self.assertEqual(calls, [(1, database), (2, database), (3, database)])
        self.assertEqual(
            [item["process_id"] for item in result["processes"]], [1, 2, 3]
        )

    def test_incomplete_process_prerequisite_is_rejected(self):
        """Process 04 cannot execute while stored Processes 01–03 remain incomplete."""
        database = self.root / "prerequisite.duckdb"
        MAIN.initialize_experiment_database(database, self.configuration)
        with self.assertRaisesRegex(RuntimeError, "requires completed Processes 01, 02, 03"):
            MAIN.run_configured_processes(database, None, (4,))

    def test_failed_process_and_execution_event_remain_retryable(self):
        """A worker failure records failed process/event state and preserves a future retry path."""
        database = self.root / "failed.duckdb"
        wrapper = SimpleNamespace(
            run=MagicMock(side_effect=RuntimeError("synthetic import failure"))
        )
        with (
            patch.object(MAIN, "load_process_wrapper", return_value=wrapper),
            self.assertRaisesRegex(RuntimeError, "synthetic import failure"),
        ):
            MAIN.run_configured_processes(database, self.configuration, (1,))
        connection = duckdb.connect(str(database), read_only=True)
        try:
            self.assertEqual(
                connection.execute(
                    "SELECT status, last_error FROM experiment_processes WHERE process_id=1"
                ).fetchone(),
                ("failed", "RuntimeError: synthetic import failure"),
            )
            self.assertEqual(
                connection.execute(
                    "SELECT status, error FROM execution_events"
                ).fetchone(),
                ("failed", "RuntimeError: synthetic import failure"),
            )
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
