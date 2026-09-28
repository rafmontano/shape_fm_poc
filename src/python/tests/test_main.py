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
from unittest.mock import MagicMock, patch

import duckdb


# ROOT: repository root resolved from this source file.
ROOT = Path(__file__).resolve().parents[3]
# SPEC: loaded researcher-entry-point module fixture used by these tests.
SPEC = importlib.util.spec_from_file_location("shapefm_main", ROOT / "src/python/00_main.py")
assert SPEC is not None and SPEC.loader is not None
# MAIN: loaded researcher-entry-point module fixture used by these tests.
MAIN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MAIN)


@dataclass(frozen=True)
class StoredForecast:
    """Minimal stored forecast returned by mocks, with identity, candidate, and mean values."""
    forecast_id: str
    candidate: str
    mean: tuple[float, ...]


class MainResultsTests(unittest.TestCase):
    """Verify CLI result selection, JSON rendering, and read-only failure handling."""
    def run_main(self, arguments):
        """Invoke the CLI with a fixed invocation record and capture status and output streams."""
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
                patch.object(MAIN, "POC1Coordinator") as coordinator,
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
                patch.object(MAIN, "POC1Coordinator") as coordinator,
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
                    patch.object(MAIN, "POC1Coordinator") as coordinator,
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
                patch.object(MAIN, "POC1Coordinator") as coordinator,
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


class MainRunTests(unittest.TestCase):
    """Verify creation, DuckDB-only resume, prerequisites, skipping, and event state."""

    def setUp(self):
        """Create a temporary directory and retain the complete reference configuration."""
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.configuration = ROOT / "config/experiments/poc2_m4_daily_100.json"

    def tearDown(self):
        """Remove temporary databases and copied configuration documents."""
        self.temporary.cleanup()

    def test_new_database_requires_configuration_without_creating_a_file(self):
        """A fresh path without --configuration fails before DuckDB is opened."""
        database = self.root / "missing.duckdb"
        with self.assertRaisesRegex(ValueError, "configuration is required"):
            MAIN.run_configured_processes(database, None, (1,))
        self.assertFalse(database.exists())

    def test_existing_database_rejects_a_competing_configuration(self):
        """Resume rejects JSON so stored configuration remains the sole authority."""
        database = self.root / "existing.duckdb"
        MAIN.initialize_experiment_database(database, self.configuration)
        with self.assertRaisesRegex(ValueError, "only valid when creating"):
            MAIN.run_configured_processes(database, self.configuration, (1,))

    def test_process_one_initializes_database_and_records_execution_event(self):
        """A configured Process 01 run persists completion and execution evidence."""
        database = self.root / "experiment.duckdb"
        coordinator = MagicMock()
        coordinator.__enter__.return_value.import_configured.return_value = {
            "selected_series": 100,
            "series_count": 100,
        }
        with patch.object(MAIN, "ImportCoordinator", return_value=coordinator):
            result = MAIN.run_configured_processes(
                database, self.configuration, (1,)
            )
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
        with (
            patch.object(MAIN, "ImportCoordinator") as import_coordinator,
            patch.object(MAIN, "POC1Coordinator") as experiment_coordinator,
        ):
            result = MAIN.run_configured_processes(
                database, None, tuple(range(1, 7))
            )
        self.assertEqual(
            [item["status"] for item in result["processes"]],
            ["skipped_completed"] * 6,
        )
        import_coordinator.assert_not_called()
        experiment_coordinator.assert_not_called()

    def test_incomplete_process_prerequisite_is_rejected(self):
        """Process 04 cannot execute while stored Processes 01–03 remain incomplete."""
        database = self.root / "prerequisite.duckdb"
        MAIN.initialize_experiment_database(database, self.configuration)
        with self.assertRaisesRegex(RuntimeError, "requires completed Processes 01, 02, 03"):
            MAIN.run_configured_processes(database, None, (4,))

    def test_failed_process_and_execution_event_remain_retryable(self):
        """A worker failure records failed process/event state and preserves a future retry path."""
        database = self.root / "failed.duckdb"
        coordinator = MagicMock()
        coordinator.__enter__.return_value.import_configured.side_effect = RuntimeError(
            "synthetic import failure"
        )
        with (
            patch.object(MAIN, "ImportCoordinator", return_value=coordinator),
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
