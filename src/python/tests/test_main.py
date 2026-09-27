import importlib.util
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("shapefm_main", ROOT / "src/python/00_main.py")
assert SPEC is not None and SPEC.loader is not None
MAIN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MAIN)


@dataclass(frozen=True)
class StoredForecast:
    forecast_id: str
    candidate: str
    mean: tuple[float, ...]


class MainResultsTests(unittest.TestCase):
    def run_main(self, arguments):
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
        parser = MAIN.build_parser()
        results = parser.parse_args(["results"])
        self.assertEqual(results.action, "results")
        self.assertEqual(results.database, MAIN.DEFAULT_DATABASE)
        self.assertIsNone(results.experiment_id)
        self.assertIsNone(results.variant_id)
        self.assertIsNone(results.series_id)
        self.assertIsNone(results.candidate)

        plan = parser.parse_args(["plan"])
        self.assertEqual(plan.series_limit, MAIN.SERIES_LIMIT)
        self.assertEqual(plan.database, MAIN.DEFAULT_PLAN_DATABASE)
        status = parser.parse_args(["status"])
        self.assertEqual(status.database, MAIN.DEFAULT_DATABASE)
        self.assertIsNone(status.experiment_id)
        acceptance = parser.parse_args(["test"])
        self.assertEqual(acceptance.database, MAIN.DEFAULT_DATABASE)
        self.assertEqual(acceptance.report, MAIN.DEFAULT_REPORT)

    def test_default_results_use_latest_experiment_and_official_results(self):
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


if __name__ == "__main__":
    unittest.main()
