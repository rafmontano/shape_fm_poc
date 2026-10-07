"""Bounded tests for the thin researcher entry point and owning CLI/actions."""

import importlib.util
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import make_dataclass
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import duckdb

import util.p00_01_researcher_actions as action_module
from util.shared_configuration import load_experiment_configuration
from util.shared_database import initialize_experiment_database
from util.window_preparation import WindowPreparationCoordinator
from util.p00_01_researcher_actions import (
    FeatureExtractionAction,
    ProcessAction,
    ResearcherActions,
    WindowPreparationAction,
)
from util.p00_02_researcher_cli import DEFAULT_DATABASE, InvocationProvenance, ResearcherCLI

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("shapefm_main", ROOT / "src/python/00_main.py")
assert SPEC and SPEC.loader
MAIN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MAIN)


class ResearcherCLITests(unittest.TestCase):
    def test_commands_and_defaults_are_preserved(self):
        cli = ResearcherCLI()
        results = cli.request(["results"])
        self.assertEqual(results.action, "results")
        self.assertEqual(results.database, DEFAULT_DATABASE.resolve())
        run = cli.request(["run", "--database", "x.duckdb", "--processes", "2-4"])
        self.assertEqual(run.value("processes"), (2, 3, 4))

    def test_help_does_not_construct_actions_or_provenance(self):
        output = io.StringIO()
        with (patch.object(MAIN, "ResearcherActions") as actions,
              patch.object(MAIN, "InvocationProvenance") as provenance,
              redirect_stdout(output)):
            self.assertEqual(MAIN.main([]), 0)
        self.assertIn("prepare-windows", output.getvalue())
        self.assertIn("prepare-features", output.getvalue())
        actions.assert_not_called()
        provenance.assert_not_called()

    def test_main_only_dispatches_and_presents(self):
        request = ResearcherCLI().request(["status"])
        cli = MagicMock()
        cli.request.return_value = request
        actions = MagicMock()
        actions.dispatch.return_value = {"status": {"ok": True}}
        provenance = MagicMock()
        provenance.record.return_value = {"action": "status"}
        with (patch.object(MAIN, "ResearcherCLI", return_value=cli),
              patch.object(MAIN, "ResearcherActions", return_value=actions),
              patch.object(MAIN, "InvocationProvenance", return_value=provenance)):
            self.assertEqual(MAIN.main(["status"]), 0)
        actions.dispatch.assert_called_once_with(request, {"action": "status"})
        cli.present.assert_called_once_with({"status": {"ok": True}})

    def test_action_error_preserves_exit_and_stderr_contract(self):
        stderr = io.StringIO()
        with (patch.object(MAIN.InvocationProvenance, "record", return_value={}),
              patch.object(MAIN.ResearcherActions, "dispatch", side_effect=ValueError("bad request")),
              redirect_stderr(stderr)):
            self.assertEqual(MAIN.main(["status"]), 1)
        self.assertEqual(stderr.getvalue(), "error: bad request\n")


class ActionContractTests(unittest.TestCase):
    def test_partial_forecast_selector_fails_before_storage(self):
        request = ResearcherCLI().request([
            "results", "--database", "missing.duckdb", "--series-id", "one"
        ])
        with self.assertRaisesRegex(ValueError, "must be supplied together"):
            ResearcherActions().dispatch(request, {})

    def test_run_dispatch_forwards_the_existing_signature(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "experiment.duckdb"
            process = MagicMock()
            process.run.return_value = {"execution_id": "execution/1"}
            actions = ResearcherActions(process_action=process)
            request = ResearcherCLI().request([
                "run", "--database", str(database), "--configuration", "config.json",
                "--processes", "4", "--local-heavy-exception", "approval/test",
            ])
            with patch("util.p00_02_researcher_cli.RESULTS_ROOT", Path(directory).resolve()):
                result = actions.dispatch(request, {"action": "run"})
        self.assertEqual(result["execution"]["execution_id"], "execution/1")
        process.run.assert_called_once_with(
            database.resolve(), Path("config.json").resolve(), (4,), None, "approval/test"
        )


class RestoredResultsAndParserTests(unittest.TestCase):
    """Restore the baseline read-only selector, parser, and failure scenarios."""

    def setUp(self):
        """Create an action dispatcher and deterministic invocation evidence."""
        self.actions = ResearcherActions()
        self.invocation = {"action": "results"}

    def request(self, arguments):
        """Parse one request through the actual CLI owner."""
        return ResearcherCLI().request(arguments)

    def test_all_parser_defaults_and_execution_overrides_are_preserved(self):
        """Plan, run, status, results, and test retain baseline defaults/options."""
        cli = ResearcherCLI()
        self.assertIsNotNone(cli.request(["plan"]).value("configuration"))
        run = cli.request(["run", "--database", "x"])
        self.assertEqual(run.value("processes"), tuple(range(1, 7)))
        self.assertIsNone(run.value("execution_profile"))
        profile = cli.request([
            "run", "--database", "x", "--processes", "4",
            "--execution-profile", "poc2_seasonal_recovery",
        ])
        self.assertEqual(profile.value("execution_profile"), "poc2_seasonal_recovery")
        exception = cli.request([
            "run", "--database", "x", "--processes", "4",
            "--local-heavy-exception", "approval/example",
        ])
        self.assertEqual(exception.value("local_heavy_exception"), "approval/example")
        self.assertEqual(cli.request(["status"]).database, DEFAULT_DATABASE.resolve())
        self.assertIsNotNone(cli.request(["test"]).value("report"))

    def test_invocation_public_arguments_include_database(self):
        """Public invocation evidence retains the selected database argument."""
        request = self.request(["status", "--database", "selected.duckdb"])
        with patch.object(InvocationProvenance, "_git", return_value="revision"):
            record = InvocationProvenance().record(request)
        self.assertEqual(record["arguments"]["database"], "selected.duckdb")
        self.assertEqual(record["database"], str(Path("selected.duckdb").resolve()))

    def test_default_and_explicit_official_results_routes(self):
        """Default results resolve latest while explicit identity skips lookup."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "results.duckdb"
            initialize_experiment_database(database, ROOT / "config/experiments/poc2_m4_daily_100.json")
            with (
                patch.object(action_module, "latest_experiment_id", return_value="latest") as latest,
                patch.object(action_module, "official_results", return_value=[{"id": 1}]) as official,
            ):
                output = self.actions.dispatch(
                    self.request(["results", "--database", str(database)]), self.invocation
                )
                explicit = self.actions.dispatch(
                    self.request(["results", "--database", str(database),
                                  "--experiment-id", "explicit"]), self.invocation
                )
        self.assertEqual(output["experiment_id"], "latest")
        self.assertEqual(explicit["experiment_id"], "explicit")
        latest.assert_called_once_with(database.resolve())
        self.assertEqual(official.call_count, 2)

    def test_complete_forecast_selectors_return_stored_forecast(self):
        """A complete forecast selector uses the actual forecast provider owner."""
        fixture_type = make_dataclass("StoredForecast", ["forecast_id", "candidate", "mean"])
        stored = fixture_type("forecast/1", "model", (1.0, 2.0))
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "results.duckdb"
            database.touch()
            with patch.object(action_module, "get_forecast", return_value=stored) as provider:
                output = self.actions.dispatch(self.request([
                    "results", "--database", str(database), "--experiment-id", "experiment/1",
                    "--variant-id", "variant/1", "--series-id", "0", "--candidate", "model",
                ]), self.invocation)
        self.assertEqual(output["forecast"]["mean"], (1.0, 2.0))
        provider.assert_called_once_with(database.resolve(), "experiment/1", "variant/1", "0", "model")

    def test_complete_window_selectors_return_prepared_window(self):
        """Complete child selectors use the prepared-window provider owner."""
        fixture_type = make_dataclass(
            "StoredWindow",
            ["window_id", "dataset_id", "series_id", "window_ordinal"],
        )
        stored = fixture_type("window/1", "dataset/1", "7", 2)
        with patch.object(action_module, "get_prepared_window", return_value=stored) as provider:
            output = self.actions.dispatch(self.request([
                "results", "--database", "parent.duckdb", "--windows-database", "child.duckdb",
                "--dataset-id", "dataset/1", "--series-id", "7", "--window-ordinal", "2",
            ]), self.invocation)
        self.assertEqual(output["prepared_window"]["window_id"], "window/1")
        provider.assert_called_once()

    def test_prepare_windows_dispatches_exact_arguments(self):
        """Preparation remains opt-in and forwards parent, child, profile, and bounds."""
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory) / "parent.duckdb"
            parent.touch()
            child = Path(directory) / "child.duckdb"
            window = MagicMock()
            window.run.return_value = {"preparation_id": "preparation/1"}
            actions = ResearcherActions(window_action=window)
            output = actions.dispatch(self.request([
                "prepare-windows", "--database", str(parent), "--windows-database", str(child),
                "--execution-profile", "poc2_seasonal_recovery",
            ]), {})
        self.assertEqual(output["window_preparation"]["preparation_id"], "preparation/1")
        window.run.assert_called_once_with(parent.resolve(), child.resolve(),
                                           "poc2_seasonal_recovery", None, None)

    def test_prepare_features_is_explicit_and_forwards_profile_or_bound(self):
        """Optional extraction has its own command and does not alter window preparation."""
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory) / "parent.duckdb"
            child = Path(directory) / "child.duckdb"
            parent.touch()
            child.touch()
            feature = MagicMock()
            feature.run.return_value = {"feature_set_id": "fforma_base_v1"}
            actions = ResearcherActions(feature_action=feature)
            output = actions.dispatch(self.request([
                "prepare-features", "--database", str(parent),
                "--windows-database", str(child), "--local-max-windows", "2",
            ]), {})
        self.assertEqual(
            output["feature_preparation"]["feature_set_id"], "fforma_base_v1"
        )
        feature.run.assert_called_once_with(parent.resolve(), child.resolve(), None, 2)

    def test_feature_result_extends_the_existing_window_selector(self):
        """A named feature set is retrieved only against the selected window identity."""
        window_type = make_dataclass(
            "StoredWindow", ["window_id", "dataset_id", "series_id", "window_ordinal"]
        )
        feature_type = make_dataclass(
            "StoredFeatures", ["window_id", "feature_set_id", "feature_values"]
        )
        with (
            patch.object(
                action_module,
                "get_prepared_window",
                return_value=window_type("window/1", "dataset/1", "7", 2),
            ),
            patch.object(
                action_module,
                "get_prepared_features",
                return_value=feature_type("window/1", "fforma_base_v1", (1.0,)),
            ) as provider,
        ):
            output = self.actions.dispatch(self.request([
                "results", "--database", "parent.duckdb",
                "--windows-database", "child.duckdb", "--dataset-id", "dataset/1",
                "--series-id", "7", "--window-ordinal", "2",
                "--feature-set-id", "fforma_base_v1",
            ]), self.invocation)
        self.assertEqual(output["prepared_features"]["feature_values"], (1.0,))
        provider.assert_called_once_with(
            Path("child.duckdb"),
            window_id="window/1",
            feature_set_id="fforma_base_v1",
        )

    def test_unbounded_local_window_preparation_fails_before_io(self):
        """Local preparation requires both explicit focused bounds."""
        with self.assertRaisesRegex(RuntimeError, "local window preparation requires"):
            WindowPreparationAction().run(Path("missing-parent"), Path("missing-child"))
        self.assertFalse(Path("missing-child").exists())

    def test_unbounded_local_feature_extraction_fails_before_io(self):
        """Feature calculation is neither implicit nor an unbounded local fallback."""
        with self.assertRaisesRegex(RuntimeError, "local feature extraction requires"):
            FeatureExtractionAction().run(Path("missing-parent"), Path("missing-child"))
        self.assertFalse(Path("missing-child").exists())

    def test_partial_forecast_combinations_fail_before_database_access(self):
        """Every incomplete forecast selector combination fails before providers."""
        options = [("--variant-id", "v"), ("--series-id", "s"), ("--candidate", "c")]
        for mask in range(1, 7):
            arguments = ["results", "--database", "missing.duckdb"]
            for index, pair in enumerate(options):
                if mask & (1 << index):
                    arguments.extend(pair)
            with self.subTest(mask=mask), self.assertRaisesRegex(ValueError, "supplied together"):
                self.actions.dispatch(self.request(arguments), self.invocation)

    def test_missing_database_experiment_results_and_forecast_fail_clearly(self):
        """All baseline read-only absence modes preserve clear failures."""
        with self.assertRaisesRegex(FileNotFoundError, "database does not exist"):
            self.actions.dispatch(self.request(["results", "--database", "missing.duckdb"]), {})
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "results.duckdb"
            initialize_experiment_database(database, ROOT / "config/experiments/poc2_m4_daily_100.json")
            with patch.object(action_module, "latest_experiment_id",
                              side_effect=RuntimeError("no experiment is planned")):
                with self.assertRaisesRegex(RuntimeError, "no experiment"):
                    self.actions.dispatch(self.request(["results", "--database", str(database)]), {})
            with patch.object(action_module, "official_results", return_value=[]):
                with self.assertRaisesRegex(RuntimeError, "no official evaluations"):
                    self.actions.dispatch(self.request([
                        "results", "--database", str(database), "--experiment-id", "empty",
                    ]), {})
            with patch.object(action_module, "get_forecast", side_effect=KeyError("forecast not found")):
                with self.assertRaisesRegex(KeyError, "forecast not found"):
                    self.actions.dispatch(self.request([
                        "results", "--database", str(database), "--experiment-id", "one",
                        "--variant-id", "v", "--series-id", "s", "--candidate", "c",
                    ]), {})


class RestoredProcessActionTests(unittest.TestCase):
    """Restore baseline process loading, lifecycle, recovery, and failure coverage."""

    def setUp(self):
        """Create isolated database paths and select the bounded reference configuration."""
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        boundary = patch("util.p00_02_researcher_cli.RESULTS_ROOT", self.root.resolve())
        boundary.start()
        self.addCleanup(boundary.stop)
        self.configuration = ROOT / "config/experiments/poc2_m4_daily_100.json"

    def tearDown(self):
        """Remove every test-owned database and configuration copy."""
        self.temporary.cleanup()

    def test_all_numbered_wrappers_load_with_required_contract(self):
        """Every explicit wrapper exists and declares its matching process number."""
        registry = action_module.ProcessWrapperRegistry()
        self.assertEqual(
            {key: value.name for key, value in action_module.PROCESS_WRAPPER_PATHS.items()},
            {number: f"0{number}_{name}.py" for number, name in action_module.PROCESS_NAMES.items()},
        )
        for process_id in range(1, 7):
            wrapper = registry.load(process_id)
            self.assertEqual(wrapper.PROCESS_NUMBER, process_id)
            self.assertTrue(callable(wrapper.run))

    def test_missing_mismatched_and_incomplete_wrappers_fail(self):
        """Registry validation rejects each malformed wrapper contract."""
        registry = action_module.ProcessWrapperRegistry()
        missing = self.root / "missing.py"
        with patch.dict(action_module.PROCESS_WRAPPER_PATHS, {1: missing}):
            with self.assertRaisesRegex(FileNotFoundError, "does not exist"):
                registry.load(1)
        mismatched = self.root / "mismatch.py"
        mismatched.write_text("PROCESS_NUMBER=2\ndef run(database): return {}\n")
        with patch.dict(action_module.PROCESS_WRAPPER_PATHS, {1: mismatched}):
            with self.assertRaisesRegex(RuntimeError, "mismatched"):
                registry.load(1)
        incomplete = self.root / "incomplete.py"
        incomplete.write_text("PROCESS_NUMBER=1\nrun=None\n")
        with patch.dict(action_module.PROCESS_WRAPPER_PATHS, {1: incomplete}):
            with self.assertRaisesRegex(RuntimeError, "no callable run"):
                registry.load(1)

    def test_new_database_requires_configuration_without_creation(self):
        """A missing creation configuration fails before DuckDB creates a file."""
        database = self.root / "missing.duckdb"
        with self.assertRaisesRegex(ValueError, "configuration is required"):
            ProcessAction().run(database, None, (1,))
        self.assertFalse(database.exists())

    def test_unavailable_enabled_machine_blocks_before_database_creation(self):
        """Distributed endpoint failure cannot create new scientific state."""
        database = self.root / "unreachable.duckdb"
        configuration = (
            ROOT / "config/experiments/"
            "poc2_m4_daily_100_directional_dtw_mantis_rf.json"
        )
        cluster = MagicMock()
        cluster.start.side_effect = RuntimeError("enabled machine is unreachable")
        with (
            patch(
                "util.shared_distributed_cluster.ManagedTuningCluster",
                return_value=cluster,
            ),
            patch.object(action_module, "initialize_experiment_database") as initialize,
            self.assertRaisesRegex(RuntimeError, "enabled machine is unreachable"),
        ):
            ProcessAction().run(
                database,
                configuration,
                (4,),
                execution_profile="poc2_seasonal_recovery",
            )
        initialize.assert_not_called()
        self.assertFalse(database.exists())
        cluster.stop.assert_called_once()

    def test_existing_database_rejects_competing_configuration(self):
        """Resume keeps stored DuckDB configuration authoritative."""
        database = self.root / "existing.duckdb"
        initialize_experiment_database(database, self.configuration)
        with self.assertRaisesRegex(ValueError, "only valid when creating"):
            ProcessAction().run(database, self.configuration, (1,))

    def test_tuning_requires_profile_before_wrapper_dispatch(self):
        """Heavy tuning cannot silently use the local ordinary route."""
        database = self.root / "tuning.duckdb"
        tuning = ROOT / "config/experiments/poc2_m4_daily_100_period_tuning.json"
        initialize_experiment_database(database, tuning)
        registry = MagicMock()
        with self.assertRaisesRegex(RuntimeError, "requires --execution-profile"):
            ProcessAction(registry).run(database, None, (4,))
        registry.load.assert_not_called()

    def test_profile_import_keeps_gate_one_local_contract(self):
        """A multi-gate profile must not pass compute-only arguments to import."""
        registry = MagicMock()
        registry.load.return_value.run = MagicMock(return_value={"series_count": 100})
        storage = MagicMock()
        storage.database = Path("bounded.duckdb")
        ProcessAction(registry)._execute(storage, (object(), {}), object(), 1)
        registry.load.return_value.run.assert_called_once_with(storage.database)
        storage.validate.assert_called_once_with(1, {"series_count": 100})

    def test_profile_action_propagates_resolved_gpu_topology(self):
        """The normal action starts the approved GPU topology and propagates exact counts."""
        configuration = SimpleNamespace(
            seasonal_period_tuning=None,
            resolved={"models": {"autoarima": {}, "chronos_2": {}}},
            execution={"dask_timeout_seconds": 60},
        )
        cluster = MagicMock()
        cluster.scheduler_address = "tcp://scheduler:8786"
        cluster.start.return_value = {"resolved_topology": {"total_workers": 38}}
        with patch("util.shared_distributed_cluster.ManagedTuningCluster", return_value=cluster) as factory:
            execution, settings, selected, evidence = ProcessAction()._execution(
                configuration, (4,), "poc2_seasonal_recovery", None
            )
        factory.assert_called_once_with(
            execution[0], configuration=configuration, requires_gpu=True
        )
        self.assertIs(selected, cluster)
        self.assertEqual(settings.dask_expected_workers, 38)
        self.assertEqual(settings.dask_expected_gpu_workers, 15)
        self.assertEqual(evidence["resolved_topology"]["total_workers"], 38)

    def test_directional_comparison_starts_one_mantis_gpu_process(self):
        """Version 11 narrows logical GPU capacity to its approved single process."""
        configuration = load_experiment_configuration(
            ROOT / "config/experiments/poc2_m4_daily_100_directional_dtw_mantis_rf.json"
        )
        cluster = MagicMock()
        cluster.scheduler_address = "tcp://scheduler:8786"
        cluster.start.return_value = {"resolved_topology": {"total_workers": 24}}
        with patch(
            "util.shared_distributed_cluster.ManagedTuningCluster",
            return_value=cluster,
        ) as factory:
            execution, settings, selected, evidence = ProcessAction()._execution(
                configuration,
                (4,),
                "poc2_seasonal_recovery",
                None,
                requires_gpu=True,
            )
        factory.assert_called_once_with(
            execution[0], configuration=configuration, requires_gpu=True, gpu_workers=1
        )
        self.assertIs(selected, cluster)
        self.assertEqual(settings.dask_expected_workers, 24)
        self.assertEqual(settings.dask_expected_gpu_workers, 1)
        self.assertEqual(evidence["resolved_topology"]["total_workers"], 24)

    def test_process_one_records_event_revision_and_completion(self):
        """A bounded Gate 1 run records repository revision and completed lifecycle."""
        database = self.root / "run.duckdb"
        registry = MagicMock()
        registry.load.return_value = SimpleNamespace(
            run=MagicMock(return_value={"series_count": 0})
        )
        with patch.object(action_module.ProcessStorage, "validate",
                          return_value={"output_validated": True, "stored_task_count": 0}):
            result = ProcessAction(registry).run(database, self.configuration, (1,))
        self.assertEqual(result["processes"][0]["status"], "completed")
        connection = duckdb.connect(str(database), read_only=True)
        try:
            row = connection.execute(
                "SELECT status, repository_revision FROM execution_events"
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual(row[0], "completed")
        self.assertTrue(row[1])

    def test_resume_skips_completed_processes_without_creation_json(self):
        """Resume reads DuckDB and skips all accepted gates after JSON removal."""
        creation = self.root / "creation.json"
        creation.write_bytes(self.configuration.read_bytes())
        database = self.root / "resume.duckdb"
        initialize_experiment_database(database, creation)
        connection = duckdb.connect(str(database))
        try:
            connection.execute("UPDATE experiment_processes SET status='completed'")
        finally:
            connection.close()
        creation.unlink()
        registry = MagicMock()
        with patch.object(action_module.ProcessStorage, "validate",
                          return_value={"output_validated": True, "stored_task_count": 0}):
            result = ProcessAction(registry).run(database, None, tuple(range(1, 7)))
        self.assertEqual([row["status"] for row in result["processes"]],
                         ["skipped_completed"] * 6)
        registry.load.assert_not_called()

    def test_completed_process_rejects_missing_output_contract(self):
        """A completed process cannot bypass authoritative output validation."""
        database = self.root / "missing-output.duckdb"
        initialize_experiment_database(database, self.configuration)
        connection = duckdb.connect(str(database))
        try:
            connection.execute("UPDATE experiment_processes SET status='completed' WHERE process_id=1")
        finally:
            connection.close()
        with patch.object(action_module.ProcessStorage, "validate",
                          side_effect=RuntimeError("missing stored output")):
            with self.assertRaisesRegex(RuntimeError, "missing stored output"):
                ProcessAction().run(database, None, (1,))

    def test_interrupted_execution_and_invocation_are_recovered(self):
        """A later sole coordinator closes both stale parent record kinds."""
        database = self.root / "interrupted.duckdb"
        initialize_experiment_database(database, self.configuration)
        connection = duckdb.connect(str(database))
        try:
            connection.execute("UPDATE experiment_processes SET status='completed'")
            connection.execute("""INSERT INTO execution_events
                (execution_id, requested_processes, operational_configuration, machine, status)
                VALUES ('old', '[4]', '{}', '{}', 'running')""")
            connection.execute("""INSERT INTO experiment_invocations
                (invocation_id, experiment_id, requested_gate, worker_count, device,
                 batch_size, environment, machine, started_at, status)
                VALUES ('invocation', 'experiment', 'forecast', 1, 'cpu', 1,
                        '{}', '{}', current_timestamp, 'running')""")
        finally:
            connection.close()
        with patch.object(action_module.ProcessStorage, "validate",
                          return_value={"output_validated": True}):
            ProcessAction().run(database, None, (4,))
        connection = duckdb.connect(str(database), read_only=True)
        try:
            self.assertEqual(connection.execute(
                "SELECT status FROM execution_events WHERE execution_id='old'"
            ).fetchone(), ("failed",))
            self.assertEqual(connection.execute(
                "SELECT status FROM experiment_invocations WHERE invocation_id='invocation'"
            ).fetchone(), ("failed",))
        finally:
            connection.close()

    def test_selected_wrappers_run_in_order_and_prerequisite_is_enforced(self):
        """Selected gates run in order while an omitted incomplete predecessor blocks."""
        database = self.root / "ordered.duckdb"
        calls = []
        registry = MagicMock()
        registry.load.side_effect = lambda process_id: SimpleNamespace(
            run=lambda path, selected=process_id: calls.append(selected) or {"process": selected}
        )
        with patch.object(action_module.ProcessStorage, "validate",
                          return_value={"output_validated": True}):
            ProcessAction(registry).run(database, self.configuration, (1, 2, 3))
        self.assertEqual(calls, [1, 2, 3])
        blocked = self.root / "blocked.duckdb"
        initialize_experiment_database(blocked, self.configuration)
        with self.assertRaisesRegex(RuntimeError, "requires completed Processes 01, 02, 03"):
            ProcessAction().run(blocked, None, (4,))

    def test_failed_gate_records_retryable_process_and_event(self):
        """A wrapper failure leaves failed gate and execution evidence for resume."""
        database = self.root / "failed.duckdb"
        registry = MagicMock()
        registry.load.return_value = SimpleNamespace(
            run=MagicMock(side_effect=RuntimeError("synthetic import failure"))
        )
        with self.assertRaisesRegex(RuntimeError, "synthetic import failure"):
            ProcessAction(registry).run(database, self.configuration, (1,))
        connection = duckdb.connect(str(database), read_only=True)
        try:
            self.assertEqual(connection.execute(
                "SELECT status FROM experiment_processes WHERE process_id=1"
            ).fetchone(), ("failed",))
            self.assertEqual(connection.execute(
                "SELECT status FROM execution_events"
            ).fetchone(), ("failed",))
        finally:
            connection.close()

    def test_process_03_failure_closes_database_and_allows_resume(self):
        """A controlled preparation failure preserves prior gates and reopens cleanly."""
        database = self.root / "recoverable.duckdb"
        windows = self.root / "recoverable.windows.duckdb"
        configuration = (
            ROOT / "config/experiments/"
            "poc2_m4_daily_100_directional_dtw_mantis_rf.json"
        )
        registry = MagicMock()
        registry.load.return_value = SimpleNamespace(run=MagicMock(return_value={}))
        with patch.object(
            action_module.ProcessStorage,
            "validate",
            return_value={"output_validated": True},
        ):
            ProcessAction(registry).run(database, configuration, (1, 2))

        def controlled_failure(path):
            with WindowPreparationCoordinator(path, windows):
                raise RuntimeError("controlled Process 03 failure")

        registry.load.return_value = SimpleNamespace(run=controlled_failure)
        with (
            patch.object(
                action_module.ProcessStorage,
                "validate",
                return_value={"output_validated": True},
            ),
            self.assertRaisesRegex(RuntimeError, "controlled Process 03 failure"),
        ):
            ProcessAction(registry).run(database, None, (3,))

        with duckdb.connect(str(database), read_only=True) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT process_id, status FROM experiment_processes "
                    "WHERE process_id<=3 ORDER BY process_id"
                ).fetchall(),
                [(1, "completed"), (2, "completed"), (3, "failed")],
            )
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM execution_events ORDER BY started_at"
                ).fetchall(),
                [("completed",), ("failed",)],
            )

        registry.load.return_value = SimpleNamespace(run=MagicMock(return_value={}))
        with patch.object(
            action_module.ProcessStorage,
            "validate",
            return_value={"output_validated": True},
        ):
            resumed = ProcessAction(registry).run(database, None, (3,))
        self.assertEqual(resumed["processes"][0]["status"], "completed")
        with duckdb.connect(str(database), read_only=True) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM experiment_processes WHERE process_id=3"
                ).fetchone(),
                ("completed",),
            )


if __name__ == "__main__":
    unittest.main()
