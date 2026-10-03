# ==============================================================================
# test_configuration.py
#
# Purpose: Verify complete experiment validation, atomic database creation, and DuckDB authority.
# Inputs: Reference experiment JSON copies, temporary paths, and deliberate stored-config corruption.
# Outputs: unittest assertions only; temporary databases are removed with their directories.
# Run from: PYTHONPATH=src/python .tools/uv/uv run --locked --no-sync python -m unittest tests.test_configuration
# ==============================================================================

"""Verify the versioned JSON-to-DuckDB experiment configuration contract."""

import json
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path

import duckdb

from util.shared_configuration import (
    ExperimentConfigurationError,
    load_experiment_configuration,
    resolve_experiment_configuration,
)
from util.shared_database import (
    initialize_experiment_database,
    load_database_configuration,
)


# Test/calibration value: repository fixture authored in the committed experiment JSON;
# tests do not override production configuration authority.
REFERENCE_CONFIGURATION = (
    Path(__file__).resolve().parents[3]
    / "config/experiments/poc2_m4_daily_100.json"
)
RESOLVED_PERIOD_CONFIGURATION = (
    Path(__file__).resolve().parents[3]
    / "config/experiments/poc2_m4_daily_100_resolved_period.json"
)


class ExperimentConfigurationTests(unittest.TestCase):
    """Exercise experiment validation, derivation, persistence, and resume.

    Purpose: Verify JSON-to-DuckDB authority and integrity boundaries.
    Inputs: The reference contract, modified copies, and temporary database paths.
    Outputs: Assertions and temporary DuckDB/file mutations owned by individual tests.
    """

    def test_r_pool_contract_preserves_legacy_and_stored_identity(self):
        """Nine native methods plan separately; v7 never changes old AutoARIMA defaults."""
        from util.shared_configuration import R_MODEL_METHODS
        configuration = load_experiment_configuration(
            REFERENCE_CONFIGURATION.parent / "poc2_m4_daily_100_r_pool.json")
        self.assertEqual(tuple(configuration.resolved["models"]), tuple(R_MODEL_METHODS))
        self.assertEqual(configuration.resolved["derived"]["expected_task_counts"]["4"], 3600)
        self.assertFalse(configuration.r_model_settings("auto_arima")["stepwise"])
        self.assertTrue(load_experiment_configuration(REFERENCE_CONFIGURATION).auto_arima_settings["stepwise"])
        self.assertEqual(configuration.r_model_settings("ets"), {"opt_crit": "mae"})
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "pool.duckdb"
            initialize_experiment_database(
                database, REFERENCE_CONFIGURATION.parent / "poc2_m4_daily_100_r_pool.json")
            self.assertEqual(load_database_configuration(database).scientific_hash,
                             configuration.scientific_hash)
        invalid = deepcopy(configuration.original)
        invalid["models"]["nnetar"]["settings"] = {"repeats": 1}
        with self.assertRaisesRegex(ExperimentConfigurationError, "R pool settings"):
            resolve_experiment_configuration(invalid)

    def test_complete_document_derives_current_cardinalities(self) -> None:
        """The reference contract derives all Process 01–06 and result counts."""
        configuration = load_experiment_configuration(REFERENCE_CONFIGURATION)
        self.assertEqual(configuration.version, 1)
        self.assertEqual(configuration.name, "poc2_m4_daily_100")
        self.assertEqual(configuration.date, "2026-09-28")
        self.assertEqual(configuration.seed, 1234)
        self.assertEqual(
            configuration.resolved["pipeline"]["preprocessing"],
            {"default": "robust", "modes": ["standard", "robust"]},
        )
        self.assertEqual(
            configuration.resolved["data"]["benchmark"]["seasonality"], 7
        )
        self.assertEqual(
            configuration.resolved["derived"]["expected_task_counts"],
            {"1": 100, "2": 200, "3": 400, "4": 800, "5": 1200, "6": 12},
        )
        self.assertEqual(
            configuration.resolved["execution"]["final_acceptance"]["workers"],
            {"mac_cpu": 1, "ubuntu_cpu": 0, "ubuntu_gpu": 1, "total": 2},
        )

    def test_v2_default_and_override_are_distinct_valid_experiments(self) -> None:
        """Version 2 defaults to bridge resolution and permits a positive R override."""
        default = load_experiment_configuration(RESOLVED_PERIOD_CONFIGURATION)
        self.assertEqual(default.version, 2)
        self.assertIsNone(default.r_period_override)
        self.assertEqual(default.evaluation_seasonality(1), 1)
        self.assertNotIn("seasonality", default.resolved["data"]["benchmark"])

        overridden_document = deepcopy(default.original)
        overridden_document["experiment"]["name"] += "_period_7"
        overridden_document["pipeline"]["r_period_override"] = 7
        overridden = resolve_experiment_configuration(overridden_document)
        self.assertEqual(overridden.r_period_override, 7)
        self.assertEqual(overridden.evaluation_seasonality(1), 1)
        self.assertNotEqual(overridden.scientific_hash, default.scientific_hash)

    def test_v2_rejects_invalid_period_overrides(self) -> None:
        """Null or a positive integer are the only accepted v2 override values."""
        original = json.loads(RESOLVED_PERIOD_CONFIGURATION.read_text(encoding="utf-8"))
        for invalid in (True, 0, -1, 1.5, "7"):
            value = deepcopy(original)
            value["pipeline"]["r_period_override"] = invalid
            with self.subTest(invalid=invalid), self.assertRaisesRegex(
                ExperimentConfigurationError, "r_period_override"
            ):
                resolve_experiment_configuration(value)

    def test_v1_period_7_semantics_remain_interpretable(self) -> None:
        """Existing v1 documents retain their coupled period-7 interpretation."""
        legacy = load_experiment_configuration(REFERENCE_CONFIGURATION)
        self.assertEqual(legacy.r_period_override, 7)
        self.assertEqual(legacy.evaluation_seasonality(1), 7)

    def test_invalid_document_leaves_no_database_file(self) -> None:
        """Missing mandatory metadata fails before even an empty DuckDB file exists."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            value = json.loads(REFERENCE_CONFIGURATION.read_text(encoding="utf-8"))
            del value["experiment"]["date"]
            invalid = root / "invalid.json"
            invalid.write_text(json.dumps(value), encoding="utf-8")
            database = root / "experiment.duckdb"
            with self.assertRaisesRegex(ExperimentConfigurationError, "date"):
                initialize_experiment_database(database, invalid)
            self.assertFalse(database.exists())

    def test_seed_must_be_a_non_negative_integer(self) -> None:
        """Negative and boolean seeds are rejected before database creation."""
        original = json.loads(REFERENCE_CONFIGURATION.read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for seed in (-1, True):
                value = deepcopy(original)
                value["reproducibility"]["seed"] = seed
                invalid = root / f"seed-{seed}.json"
                invalid.write_text(json.dumps(value), encoding="utf-8")
                database = root / f"seed-{seed}.duckdb"
                with self.assertRaisesRegex(ExperimentConfigurationError, "seed"):
                    initialize_experiment_database(database, invalid)
                self.assertFalse(database.exists())

    def test_archived_provider_selection_is_bounded_to_approved_ids(self) -> None:
        """Either approved archived provider can be enabled, but no other ID can."""
        original = json.loads(REFERENCE_CONFIGURATION.read_text(encoding="utf-8"))
        fforma = deepcopy(original)
        fforma["archived_forecasts"]["enabled"] = ["m4_fforma"]
        self.assertEqual(
            resolve_experiment_configuration(fforma).resolved["archived_forecasts"]["enabled"],
            ["m4_fforma"],
        )
        invalid = deepcopy(original)
        invalid["archived_forecasts"]["enabled"] = ["m4_other"]
        with self.assertRaisesRegex(ExperimentConfigurationError, "approved"):
            resolve_experiment_configuration(invalid)

    def test_database_preserves_original_resolved_metadata_and_process_state(self) -> None:
        """Creation stores both documents, required metadata, digest, and six pending processes."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "experiment.duckdb"
            expected = initialize_experiment_database(
                database, REFERENCE_CONFIGURATION
            )
            connection = duckdb.connect(str(database), read_only=True)
            try:
                row = connection.execute(
                    """SELECT configuration_version, experiment_name,
                              CAST(experiment_date AS VARCHAR), experiment_description,
                              reproducibility_seed, original_configuration,
                              resolved_configuration, scientific_hash,
                              configuration_integrity_hash
                       FROM experiment_configuration"""
                ).fetchone()
                processes = connection.execute(
                    "SELECT process_id, process_name, status FROM experiment_processes ORDER BY process_id"
                ).fetchall()
            finally:
                connection.close()
            self.assertEqual(row[:4], (1, expected.name, expected.date, expected.description))
            self.assertEqual(row[4], 1234)
            self.assertEqual(json.loads(row[5]), expected.original)
            self.assertEqual(json.loads(row[6]), expected.resolved)
            self.assertEqual(row[7], expected.scientific_hash)
            self.assertEqual(row[8], expected.configuration_integrity_hash)
            self.assertEqual(
                processes,
                [
                    (1, "import", "pending"),
                    (2, "preprocess", "pending"),
                    (3, "transform", "pending"),
                    (4, "forecast", "pending"),
                    (5, "combine", "pending"),
                    (6, "evaluate", "pending"),
                ],
            )

    def test_existing_database_resumes_after_original_json_is_removed(self) -> None:
        """DuckDB alone reconstructs the typed contract after its creation-time JSON disappears."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            configuration_path = root / "experiment.json"
            configuration_path.write_bytes(REFERENCE_CONFIGURATION.read_bytes())
            database = root / "experiment.duckdb"
            created = initialize_experiment_database(database, configuration_path)
            configuration_path.unlink()
            loaded = load_database_configuration(database)
            self.assertEqual(loaded, created)

    def test_stored_scientific_hash_cannot_be_silently_changed(self) -> None:
        """A modified stored digest is detected before a coordinator consumes settings."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "experiment.duckdb"
            initialize_experiment_database(database, REFERENCE_CONFIGURATION)
            connection = duckdb.connect(str(database))
            try:
                connection.execute(
                    "UPDATE experiment_configuration SET scientific_hash='changed'"
                )
            finally:
                connection.close()
            with self.assertRaisesRegex(RuntimeError, "scientific configuration hash"):
                load_database_configuration(database)

    def test_scientific_fingerprint_has_the_approved_boundary(self) -> None:
        """Metadata and workers are excluded, while seed and model settings alter identity."""
        original = json.loads(REFERENCE_CONFIGURATION.read_text(encoding="utf-8"))
        baseline = resolve_experiment_configuration(original).scientific_hash
        for section, field, replacement in (
            ("experiment", "name", "renamed"),
            ("experiment", "date", "2026-09-29"),
            ("experiment", "description", "Another objective"),
        ):
            changed = deepcopy(original)
            changed[section][field] = replacement
            self.assertEqual(resolve_experiment_configuration(changed).scientific_hash, baseline)
        changed = deepcopy(original)
        changed["execution"]["default"]["process_workers"]["2"] = 2
        self.assertEqual(resolve_experiment_configuration(changed).scientific_hash, baseline)
        changed = deepcopy(original)
        changed["reproducibility"]["seed"] = 1235
        self.assertNotEqual(resolve_experiment_configuration(changed).scientific_hash, baseline)
        changed = deepcopy(original)
        changed["models"]["chronos_2"]["revision"] = "0" * 40
        self.assertNotEqual(resolve_experiment_configuration(changed).scientific_hash, baseline)

    def test_legacy_worker_paths_resolve_without_rewriting_identity_documents(self) -> None:
        """Stored pre-rename paths execute current substeps without changing history."""
        current_document = json.loads(
            REFERENCE_CONFIGURATION.read_text(encoding="utf-8")
        )
        legacy_document = deepcopy(current_document)
        legacy_paths = legacy_document["execution"]["paths"]
        legacy_paths.update(
            {
                "chronos_worker": "src/python/04_forecast_chronos.py",
                "r_preprocess_worker": "src/r/02_preprocess_series.R",
                "r_auto_arima_worker": "src/r/04_forecast_auto_arima.R",
            }
        )
        current = resolve_experiment_configuration(current_document)
        legacy = resolve_experiment_configuration(legacy_document)
        self.assertEqual(legacy.scientific_hash, current.scientific_hash)
        self.assertNotEqual(
            legacy.configuration_integrity_hash,
            current.configuration_integrity_hash,
        )
        self.assertEqual(
            legacy.resolved["execution"]["paths"]["chronos_worker"],
            "src/python/04_forecast_chronos.py",
        )
        self.assertEqual(
            legacy.execution_paths,
            current.resolved["execution"]["paths"],
        )

    def test_configuration_integrity_detects_resolved_document_tampering(self) -> None:
        """Even valid non-scientific stored edits fail complete-document integrity checks."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "experiment.duckdb"
            initialize_experiment_database(database, REFERENCE_CONFIGURATION)
            connection = duckdb.connect(str(database))
            try:
                original, resolved = (
                    json.loads(value)
                    for value in connection.execute(
                        """SELECT original_configuration, resolved_configuration
                           FROM experiment_configuration"""
                    ).fetchone()
                )
                original["experiment"]["description"] = "tampered"
                resolved["experiment"]["description"] = "tampered"
                connection.execute(
                    """UPDATE experiment_configuration
                       SET experiment_description=?, original_configuration=?,
                           resolved_configuration=?""",
                    ["tampered", json.dumps(original), json.dumps(resolved)],
                )
            finally:
                connection.close()
            with self.assertRaisesRegex(RuntimeError, "configuration-integrity"):
                load_database_configuration(database)


if __name__ == "__main__":
    unittest.main()
