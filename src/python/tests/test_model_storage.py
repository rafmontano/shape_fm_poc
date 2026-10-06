# ==============================================================================
# test_model_storage.py
#
# Purpose: Verify atomic fitted-model lifecycle, overwrite safety, and file authority.
# Inputs: Temporary local model roots and trusted small project-owned Python objects.
# Outputs: Bounded unittest evidence; no durable model or research artifacts.
# Run from: Imported by unittest discovery; not run directly in production.
# ==============================================================================

"""Focused external fitted-model storage tests for POC2 ID 026."""

from __future__ import annotations

import hashlib
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from util.shared_model_storage import ModelStorage, ModelStorageError


class ModelStorageTests(unittest.TestCase):
    """Protect real-file existence, skip, safe replacement, and load failures."""

    def setUp(self) -> None:
        """Create one isolated configured model root for each test."""
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.storage = ModelStorage(self.directory.name, "experiment")

    def test_approved_paths_save_load_and_skip_existing(self):
        """The two logical scopes resolve exactly and existing files skip replacement."""
        dtw = self.storage.save(
            {"references": [1, 2], "widths": list(range(14))},
            "directional_dtw", "D", "all_horizons",
        )
        self.assertEqual(
            dtw["relative_path"],
            "experiment/directional_dtw/D/model_all_horizons.joblib",
        )
        skipped = self.storage.save(
            {"changed": True}, "directional_dtw", "D", "all_horizons"
        )
        self.assertEqual(skipped["status"], "skipped_existing")
        self.assertEqual(
            self.storage.load("directional_dtw", "D", "all_horizons")["references"],
            [1, 2],
        )
        rf = self.storage.save({"horizon": 3}, "directional_mantis_rf", "D", 3)
        self.assertEqual(
            rf["relative_path"],
            "experiment/directional_mantis_rf/D/model_h03.joblib",
        )

    def test_overwrite_replaces_only_after_valid_temporary_save(self):
        """A successful overwrite replaces content and a failed one preserves it."""
        self.storage.save({"version": 1}, "directional_mantis_rf", "D", 4)
        replaced = self.storage.save(
            {"version": 2}, "directional_mantis_rf", "D", 4, overwrite=True
        )
        self.assertEqual(replaced["status"], "overwritten")
        self.assertEqual(
            self.storage.load("directional_mantis_rf", "D", 4), {"version": 2}
        )
        with patch("util.shared_model_storage.joblib.dump", side_effect=OSError("full")):
            with self.assertRaisesRegex(ModelStorageError, "failed to save"):
                self.storage.save(
                    {"version": 3}, "directional_mantis_rf", "D", 4,
                    overwrite=True,
                )
        self.assertEqual(
            self.storage.load("directional_mantis_rf", "D", 4), {"version": 2}
        )

    def test_manual_deletion_and_unreadable_files_fail_exact_scope(self):
        """Filesystem absence and corruption override any external completion claim."""
        self.storage.save({"horizon": 5}, "directional_mantis_rf", "D", 5)
        path = self.storage.path("directional_mantis_rf", "D", 5)
        path.unlink()
        self.assertFalse(self.storage.exists("directional_mantis_rf", "D", 5))
        with self.assertRaisesRegex(ModelStorageError, "directional_mantis_rf/D/5"):
            self.storage.load("directional_mantis_rf", "D", 5)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"not-joblib")
        with self.assertRaisesRegex(ModelStorageError, "invalid or unreadable"):
            self.storage.load("directional_mantis_rf", "D", 5)

    def test_worker_staged_file_is_validated_and_atomically_adopted(self):
        """A transferred joblib file enters the authoritative store through the same guard."""
        with tempfile.TemporaryDirectory() as worker_directory:
            worker = ModelStorage(worker_directory, "experiment")
            saved = worker.save({"worker": "ubuntu"}, "directional_mantis_rf", "D", 6)
            source = worker.path("directional_mantis_rf", "D", 6)
            with patch("util.shared_model_storage.joblib.load", side_effect=AssertionError("coordinator deserialized")):
                evidence = self.storage.adopt(
                    source, "directional_mantis_rf", "D", 6,
                    size_bytes=saved["size_bytes"], sha256=saved["sha256"],
                )
            self.assertEqual(evidence["size_bytes"], saved["size_bytes"])
            self.assertEqual(evidence["sha256"], saved["sha256"])
        self.assertEqual(evidence["status"], "trained")
        self.assertEqual(
            self.storage.load("directional_mantis_rf", "D", 6),
            {"worker": "ubuntu"},
        )

    def test_inspection_requires_no_provider_and_reports_exact_scope(self):
        """An unavailable provider prevents loading, not metadata-only inspection."""
        path = self.storage.path("directional_dtw", "D", "all_horizons")
        path.parent.mkdir(parents=True)
        contents = b"cprovider_not_installed\nModel\n."
        path.write_bytes(contents)
        with self.assertRaisesRegex(ModelStorageError, "provider_not_installed"):
            self.storage.load("directional_dtw", "D", "all_horizons")
        with patch("util.shared_model_storage.joblib.load", side_effect=AssertionError("deserialized")):
            evidence = self.storage.inspect("directional_dtw", "D", "all_horizons")
        self.assertEqual(evidence["size_bytes"], len(contents))
        self.assertEqual(evidence["sha256"], hashlib.sha256(contents).hexdigest())
        with patch.object(Path, "read_bytes", side_effect=PermissionError("denied")):
            with self.assertRaisesRegex(ModelStorageError, "directional_dtw/D/all_horizons"):
                self.storage.inspect("directional_dtw", "D", "all_horizons")
        path.unlink()
        with self.assertRaisesRegex(ModelStorageError, "missing fitted model directional_dtw/D/all_horizons"):
            self.storage.inspect("directional_dtw", "D", "all_horizons")

    def test_adoption_rejects_incoming_and_copied_corruption_preserving_existing(self):
        """Both sides of transfer verification preserve the prior artifact on failure."""
        saved = self.storage.save({"valid": True}, "directional_mantis_rf", "D", 7)
        source = Path(self.directory.name) / "incoming.joblib"
        source.write_bytes(b"worker-owned-bytes")
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        original_copy = shutil.copyfile

        def corrupt_copy(src, dest):
            """Simulate corruption after the incoming checksum has passed."""
            original_copy(src, dest)
            Path(dest).write_bytes(b"corrupted-copy")

        for size, checksum, copy in (
            (source.stat().st_size + 1, digest, original_copy),
            (source.stat().st_size, "0" * 64, original_copy),
            (source.stat().st_size, digest, corrupt_copy),
        ):
            with self.subTest(size=size, checksum=checksum, copy=copy.__name__):
                with patch("util.shared_model_storage.shutil.copyfile", side_effect=copy):
                    with self.assertRaisesRegex(ModelStorageError, "failed to adopt fitted model directional_mantis_rf/D/7"):
                        self.storage.adopt(source, "directional_mantis_rf", "D", 7,
                                           size_bytes=size, sha256=checksum, overwrite=True)
                self.assertEqual(self.storage.inspect("directional_mantis_rf", "D", 7)["sha256"], saved["sha256"])
                self.assertEqual(self.storage.load("directional_mantis_rf", "D", 7), {"valid": True})
                self.assertFalse(list(self.storage.path("directional_mantis_rf", "D", 7).parent.glob("*.tmp")))


if __name__ == "__main__":
    unittest.main()
