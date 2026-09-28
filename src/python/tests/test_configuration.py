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

from util.configuration import (
    ExperimentConfigurationError,
    load_experiment_configuration,
    resolve_experiment_configuration,
)
from util.database import (
    initialize_experiment_database,
    load_database_configuration,
)


# Test/calibration value: repository fixture authored in the committed experiment JSON;
# tests do not override production configuration authority.
REFERENCE_CONFIGURATION = (
    Path(__file__).resolve().parents[3]
    / "config/experiments/poc2_m4_daily_100.json"
)


class ExperimentConfigurationTests(unittest.TestCase):
    """Exercise experiment validation, derivation, persistence, and resume.

    Purpose: Verify JSON-to-DuckDB authority and integrity boundaries.
    Inputs: The reference contract, modified copies, and temporary database paths.
    Outputs: Assertions and temporary DuckDB/file mutations owned by individual tests.
    """

    def test_complete_document_derives_current_cardinalities(self) -> None:
        """The reference contract derives all Process 01–06 and result counts."""
        configuration = load_experiment_configuration(REFERENCE_CONFIGURATION)
        self.assertEqual(configuration.version, 1)
        self.assertEqual(configuration.name, "poc2_m4_daily_100")
        self.assertEqual(configuration.date, "2026-09-28")
        self.assertEqual(configuration.seed, 1234)
        self.assertEqual(
            configuration.resolved["derived"]["expected_task_counts"],
            {"1": 100, "2": 200, "3": 400, "4": 800, "5": 1200, "6": 12},
        )
        self.assertEqual(
            configuration.resolved["execution"]["final_acceptance"]["workers"],
            {"mac_cpu": 1, "ubuntu_cpu": 0, "ubuntu_gpu": 1, "total": 2},
        )

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
