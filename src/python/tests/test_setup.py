# ==============================================================================
# test_setup.py
#
# Purpose: Verify setup argument, archive, download, platform, and read-only safety contracts.
# Inputs: Temporary repository fixtures, command stubs, and scripts/setup_support.py.
# Outputs: Assertions that setup is complete, non-destructive, atomic, and diagnostically clear.
# Run from: PYTHONPATH=src/python .tools/uv/uv run --locked --no-sync python -m unittest tests.test_setup
# ==============================================================================

"""Focused safety and interface tests for the complete setup facility."""

from __future__ import annotations

import importlib.util
import io
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[3]
SETUP_SHELL = ROOT / "scripts/setup.sh"
SUPPORT_PATH = ROOT / "scripts/setup_support.py"
SPEC = importlib.util.spec_from_file_location("setup_support", SUPPORT_PATH)
assert SPEC and SPEC.loader
setup_support = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = setup_support
SPEC.loader.exec_module(setup_support)


def make_repository(root: Path) -> Path:
    """Create the minimum immutable markers accepted as a ShapeFM root."""
    root.mkdir()
    (root / ".git").mkdir()
    (root / "scripts").mkdir()
    for path in ("shape_fm_poc.Rproj", "pyproject.toml", "renv.lock", "scripts/setup.sh"):
        (root / path).write_text("fixture\n")
    return root


def shell(command: str) -> subprocess.CompletedProcess[str]:
    """Source setup.sh and run a function without executing live installation."""
    return subprocess.run(
        ["bash", "-c", f"source {SETUP_SHELL!s}; {command}"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def tree_state(root: Path) -> list[tuple[str, bytes | None]]:
    """Capture names and file bytes so a read-only test detects any mutation."""
    state = []
    for path in sorted(root.rglob("*")):
        state.append((str(path.relative_to(root)), path.read_bytes() if path.is_file() else None))
    return state


class DownloadResponse(io.BytesIO):
    """Provide the context-manager interface returned by urlopen."""

    def __enter__(self) -> "DownloadResponse":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


class SetupInterfaceTests(unittest.TestCase):
    """Verify the shell command surface without invoking any installation action."""

    def test_default_operation_equals_install_all(self) -> None:
        """No arguments and explicit install all select the same complete operation."""
        implicit = shell("parse_arguments; printf '%s:%s:%s' \"$OPERATION\" \"$TARGET\" \"$CONFIRM_ARCHIVE\"")
        explicit = shell("parse_arguments install all; printf '%s:%s:%s' \"$OPERATION\" \"$TARGET\" \"$CONFIRM_ARCHIVE\"")
        self.assertEqual(implicit.returncode, 0)
        self.assertEqual(explicit.returncode, 0)
        self.assertEqual(implicit.stdout, "install:all:false")
        self.assertEqual(explicit.stdout, implicit.stdout)

    def test_selective_or_invalid_operations_are_rejected(self) -> None:
        """The public interface never permits a partial dependency installation."""
        for arguments in (
            "install core",
            "install tensorflow",
            "verify all",
            "rebuild core --confirm-delete",
        ):
            with self.subTest(arguments=arguments):
                completed = shell(f"parse_arguments {arguments}")
                self.assertNotEqual(completed.returncode, 0)
                self.assertIn("unsupported or selective operation", completed.stderr)

    def test_rebuild_requires_confirmation_flag(self) -> None:
        """A full rebuild cannot reach archival without the exact confirmation flag."""
        completed = shell("parse_arguments rebuild all")
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("requires exactly", completed.stderr)

    def test_default_install_path_does_not_archive(self) -> None:
        """Normal setup invokes complete install but never the rebuild archiver."""
        completed = shell(
            "validate_repository_root(){ :; }; validate_settings(){ :; }; "
            "install_all(){ echo installed-all; }; "
            "archive_existing_installation(){ echo archived; }; main"
        )
        self.assertEqual(completed.returncode, 0)
        self.assertEqual(completed.stdout.strip(), "installed-all")
        self.assertNotIn("archived", completed.stdout)


class SetupGitIgnoreTests(unittest.TestCase):
    """Verify setup products stay untracked without hiding authored project files."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        (self.root / ".gitignore").write_text((ROOT / ".gitignore").read_text())
        subprocess.run(["git", "init", "--quiet"], cwd=self.root, check=True)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def is_ignored(self, relative_path: str) -> bool:
        """Ask Git whether the repository ignore contract covers one fixture path."""
        path = self.root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixture\n")
        completed = subprocess.run(
            ["git", "check-ignore", "--quiet", "--no-index", relative_path],
            cwd=self.root,
            check=False,
        )
        return completed.returncode == 0

    def test_active_setup_folders_and_partial_downloads_are_ignored(self) -> None:
        """Every active installation, generated output, and cache remains outside Git."""
        ignored_paths = (
            ".venv/bin/python",
            "environments/mantis/.venv/bin/python",
            "renv/library/platform/package/DESCRIPTION",
            "renv/staging/package/DESCRIPTION",
            "renv/python/environment/bin/python",
            "renv/cellar/package/DESCRIPTION",
            "renv/sandbox/platform/package/DESCRIPTION",
            "renv/lock/install.lock",
            "data/source/dataset.arrow",
            "models/model.safetensors",
            "results/installation-report.txt",
            ".cache/downloads/uv-installer.sh",
            ".cache/downloads/asset.part",
            ".cache/huggingface/hub/download.incomplete",
            ".tools/uv/uv",
            ".tools/cache/wheels/package.whl",
            ".tools/python/.temp/interpreter.part",
            ".tools/renv-cache/v5/package",
        )
        for relative_path in ignored_paths:
            with self.subTest(path=relative_path):
                self.assertTrue(self.is_ignored(relative_path))

    def test_every_rebuild_move_has_active_and_dated_ignore_coverage(self) -> None:
        """The archiver's source-of-truth paths cannot leak before or after a move."""
        for relative_path in setup_support.ARCHIVE_PATHS:
            active = f"{relative_path}/fixture"
            source = Path(relative_path)
            archived = source.with_name(f"{source.name}_290926") / "fixture"
            with self.subTest(path=relative_path):
                self.assertTrue(self.is_ignored(active))
                self.assertTrue(self.is_ignored(str(archived)))

    def test_dated_rebuild_archives_are_ignored(self) -> None:
        """All supported `_DDMMYY` archive forms remain outside Git status."""
        archived_paths = (
            ".venv_290926/bin/python",
            "environments/tensorflow/.venv_290926/bin/python",
            "renv/library_290926/platform/package",
            "data_290926/source/dataset.arrow",
            "models_290926/model.safetensors",
            "results_290926/report.json",
            ".cache_290926/downloads/asset.part",
            ".tools/uv_290926/uv",
            ".tools/cache_290926/wheels/package.whl",
            ".tools/python_290926/interpreter/bin/python",
            ".tools/renv-cache_290926/v5/package",
        )
        for relative_path in archived_paths:
            with self.subTest(path=relative_path):
                self.assertTrue(self.is_ignored(relative_path))

    def test_authored_files_and_near_miss_names_remain_trackable(self) -> None:
        """Narrow rules do not hide manifests, locks, source, configuration, or tests."""
        trackable_paths = (
            "pyproject.toml",
            "uv.lock",
            "environments/mantis/pyproject.toml",
            "environments/mantis/uv.lock",
            "scripts/setup.sh",
            "src/python/setup_feature.py",
            "src/python/tests/test_setup_feature.py",
            "config/dependencies/models.json",
            "docs/environment.md",
            "renv/activate.R",
            ".tools/README.md",
            "data-notes.md",
            "models_manifest.json",
            "results-summary.md",
            "data_backup/source.csv",
        )
        for relative_path in trackable_paths:
            with self.subTest(path=relative_path):
                self.assertFalse(self.is_ignored(relative_path))


class ArchiveSafetyTests(unittest.TestCase):
    """Exercise rebuild planning and moves only inside temporary repositories."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.root = make_repository(self.base / "project")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_repository_root_requires_all_markers(self) -> None:
        """A similarly named directory cannot pass the safety boundary."""
        self.assertTrue(setup_support.repository_root_is_valid(self.root))
        (self.root / "shape_fm_poc.Rproj").unlink()
        with self.assertRaisesRegex(setup_support.SetupError, "repository-root validation failed"):
            setup_support.require_repository_root(self.root)

    def test_archive_names_use_one_ddmmyy_suffix(self) -> None:
        """Every planned move uses the supplied six-digit rebuild date."""
        (self.root / "data").mkdir()
        (self.root / "results").mkdir()
        plan = setup_support.build_archive_plan(self.root, "290926", ("data", "results"))
        self.assertEqual(
            [move.destination.name for move in plan], ["data_290926", "results_290926"]
        )

    def test_archive_collision_aborts_before_moves(self) -> None:
        """An earlier dated archive blocks the whole plan instead of merging into it."""
        (self.root / "data").mkdir()
        (self.root / "data_290926").mkdir()
        with self.assertRaisesRegex(setup_support.SetupError, "already exists"):
            setup_support.build_archive_plan(self.root, "290926", ("data",))
        self.assertTrue((self.root / "data").is_dir())

    def test_rebuild_moves_instead_of_deleting(self) -> None:
        """Executing a fixture plan preserves content under its dated sibling."""
        (self.root / "models").mkdir()
        (self.root / "models/weights.bin").write_bytes(b"weights")
        plan = setup_support.build_archive_plan(self.root, "290926", ("models",))
        setup_support.execute_archive_plan(self.root, plan)
        self.assertFalse((self.root / "models").exists())
        self.assertEqual((self.root / "models_290926/weights.bin").read_bytes(), b"weights")

    def test_archiver_refuses_paths_outside_repository(self) -> None:
        """Even a manually constructed move cannot escape the repository boundary."""
        outside = self.base / "outside"
        outside.mkdir()
        move = setup_support.ArchiveMove(outside, self.base / "outside_290926")
        with self.assertRaisesRegex(setup_support.SetupError, "outside repository"):
            setup_support.execute_archive_plan(self.root, [move])
        self.assertTrue(outside.is_dir())

    def test_move_failure_names_the_affected_folder(self) -> None:
        """A partial filesystem failure identifies the source and stops immediately."""
        (self.root / "data").mkdir()
        plan = setup_support.build_archive_plan(self.root, "290926", ("data",))

        def fail_move(source: Path, destination: Path) -> None:
            raise OSError("fixture failure")

        with self.assertRaisesRegex(setup_support.SetupError, r"failed to archive folder .*data"):
            setup_support.execute_archive_plan(self.root, plan, mover=fail_move)


class DownloadAndPlatformTests(unittest.TestCase):
    """Verify retry, atomic publication, validation, and platform decisions."""

    def test_download_retries_then_atomically_publishes(self) -> None:
        """Recoverable failures retry and only a validated temporary file becomes final."""
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "asset.bin"
            calls = 0

            def opener(url: str, timeout: int) -> DownloadResponse:
                nonlocal calls
                calls += 1
                if calls < 3:
                    raise OSError("temporary network failure")
                self.assertEqual(timeout, 7200)
                return DownloadResponse(b"valid asset")

            outcome = setup_support.download_with_retry(
                "https://example.test/asset", destination, attempts=3, delay=0, opener=opener
            )
            self.assertEqual(outcome, "downloaded")
            self.assertEqual(calls, 3)
            self.assertEqual(destination.read_bytes(), b"valid asset")
            self.assertFalse((Path(directory) / "asset.bin.part").exists())

    def test_invalid_download_never_becomes_final(self) -> None:
        """Content validation failure removes temporary bytes after all attempts."""
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "asset.bin"
            with self.assertRaisesRegex(setup_support.SetupError, "after 3 attempts"):
                setup_support.download_with_retry(
                    "https://example.test/asset",
                    destination,
                    attempts=3,
                    delay=0,
                    validator=lambda path: False,
                    opener=lambda *args, **kwargs: DownloadResponse(b"invalid"),
                )
            self.assertFalse(destination.exists())
            self.assertFalse((Path(directory) / "asset.bin.part").exists())

    def test_platform_selection_and_cpu_fallback(self) -> None:
        """GPU absence on supported Linux is an explicit CPU mode, not ambiguity."""
        cases = (
            ("Darwin", "arm64", False, "macos-arm64-metal"),
            ("Linux", "x86_64", True, "linux-x86_64-cuda"),
            ("Linux", "x86_64", False, "linux-x86_64-cpu"),
            ("Windows", "x86_64", False, "unsupported"),
        )
        for system, machine, nvidia, expected in cases:
            with self.subTest(system=system, nvidia=nvidia):
                self.assertEqual(
                    setup_support.platform_selection(system, machine, nvidia), expected
                )


class VerificationSafetyTests(unittest.TestCase):
    """Prove complete verification orchestration does not alter project files."""

    def test_child_failure_reports_corrective_detail(self) -> None:
        """Generic R trailers do not hide the actionable package error."""
        completed = subprocess.CompletedProcess(
            [], 1, "", "Error: project R library is missing M4comp2018\nExecution halted\n"
        )
        self.assertEqual(
            setup_support.failure_detail(completed),
            "Error: project R library is missing M4comp2018",
        )

    def test_verification_orchestration_is_read_only(self) -> None:
        """The repository fixture remains byte-identical through verification."""
        with tempfile.TemporaryDirectory() as directory:
            root = make_repository(Path(directory) / "project")
            for relative in (
                "data",
                "models",
                "results",
                ".cache",
                ".tools/uv",
                "environments/gift-eval",
                "environments/chronos-2",
                "environments/mantis",
                "environments/classifiers",
                "environments/tensorflow",
            ):
                (root / relative).mkdir(parents=True, exist_ok=True)
            (root / ".tools/uv/uv").write_text("fixture\n")
            passing = setup_support.Check("fixture", "PASS", "unchanged")
            before = tree_state(root)
            with (
                patch.object(setup_support, "verify_submodule", return_value=passing),
                patch.object(setup_support, "verify_r", return_value=passing),
                patch.object(setup_support, "verify_lock_consistency", return_value=passing),
                patch.object(setup_support, "verify_python_environment", return_value=(passing, {})),
                patch.object(setup_support, "verify_models", return_value=[passing]),
                patch.object(setup_support, "verify_data", return_value=passing),
                patch.object(setup_support, "detect_nvidia_driver", return_value=(False, "not detected")),
                patch.object(
                    setup_support,
                    "run_read_only",
                    return_value=subprocess.CompletedProcess([], 0, "uv 0.12.18\n", ""),
                ),
            ):
                checks, metadata = setup_support.verify_installation(root)
            self.assertEqual(tree_state(root), before)
            self.assertFalse(any(check.status == "FAIL" for check in checks))
            self.assertEqual(metadata["nvidia"], "not detected")


if __name__ == "__main__":
    unittest.main()
