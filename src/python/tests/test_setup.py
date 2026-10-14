"""Focused setup interface, atomic acquisition and offline readiness tests."""

from __future__ import annotations

import importlib.util
import hashlib
import io
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[3]
SETUP_SHELL = ROOT / "scripts/setup.sh"
SPEC = importlib.util.spec_from_file_location("setup_support", ROOT / "scripts/setup_support.py")
assert SPEC and SPEC.loader
setup_support = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = setup_support
SPEC.loader.exec_module(setup_support)


def tree_state(root: Path) -> list[tuple[str, bytes | None]]:
    """Capture names and bytes to detect mutations."""
    return [(str(path.relative_to(root)), path.read_bytes() if path.is_file() else None)
            for path in sorted(root.rglob("*"))]


def make_repository(root: Path) -> Path:
    """Create a minimum root fixture."""
    root.mkdir()
    (root / ".git").mkdir()
    (root / "scripts").mkdir()
    for name in ("shape_fm_poc.Rproj", "pyproject.toml", "uv.lock", "renv.lock", "scripts/setup.sh"):
        (root / name).write_text("fixture")
    return root


def shell(command: str) -> subprocess.CompletedProcess[str]:
    """Exercise shell functions without running installation."""
    return subprocess.run(["bash", "-c", f'source "{SETUP_SHELL}"; {command}'],
                          cwd=ROOT, text=True, capture_output=True, check=False)


class SetupInterfaceTests(unittest.TestCase):
    def test_supported_operations(self):
        for arguments, expected in (("", "install"), ("verify", "verify"), ("--help", "help")):
            with self.subTest(arguments=arguments):
                result = shell(f'parse_arguments {arguments}; echo "$OPERATION"')
                self.assertEqual(result.returncode, 0)
                self.assertEqual(result.stdout.strip(), expected)

    def test_aliases_and_extra_arguments_rejected(self):
        for arguments in ("install all", "rebuild all --confirm-delete", "install", "verify all", "--help extra"):
            with self.subTest(arguments=arguments):
                self.assertNotEqual(shell(f"parse_arguments {arguments}").returncode, 0)

    def test_verify_never_enters_installation(self):
        result = shell('validate_repository_root(){ :; }; validate_settings(){ :; }; '
                       'install_all(){ echo forbidden; }; verify_installation(){ echo offline; }; main verify')
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), "offline")

    def test_dynamic_locked_projects(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, files in (("future", ("pyproject.toml", "uv.lock")),
                                ("incomplete", ("pyproject.toml",))):
                project = root / "environments" / name
                project.mkdir(parents=True)
                for filename in files:
                    (project / filename).write_text("fixture")
            self.assertEqual(setup_support.python_projects(root), [root, root / "environments/future"])


class AcquisitionTests(unittest.TestCase):
    def test_download_retries_and_reuses_valid_asset(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "asset"
            responses = [OSError("temporary"), OSError("temporary"), io.BytesIO(b"valid")]
            with patch.object(setup_support.os, "replace", wraps=setup_support.os.replace) as publish:
                with patch("urllib.request.urlopen", side_effect=responses) as opener:
                    self.assertEqual(setup_support.download_with_retry(
                        "https://example.test", destination, delay=0, opener=opener), "downloaded")
                publish.assert_called_once()
            self.assertEqual(destination.read_bytes(), b"valid")
            self.assertEqual(setup_support.download_with_retry(
                "https://example.test", destination,
                opener=lambda *a, **k: self.fail("network on reuse")), "reused")

    def test_invalid_download_not_published(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "asset"
            with self.assertRaisesRegex(setup_support.SetupError, "after 3 attempts"):
                setup_support.download_with_retry("https://example.test", destination, delay=0,
                    validator=lambda path: False, opener=lambda *a, **k: io.BytesIO(b"invalid"))
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_model_offline_reuse_and_shared_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            root = make_repository(Path(directory) / "project").resolve()
            snapshot = root / "snapshot"
            snapshot.mkdir()
            (snapshot / "weights").write_bytes(b"weights")
            manifest = root / "models.json"
            manifest.write_text(json.dumps({"model": {"repository": "owner/model", "revision": "pin",
                "required_files": {"weights": hashlib.sha256(b"weights").hexdigest()}}}))
            with patch("huggingface_hub.snapshot_download", return_value=str(snapshot)) as download:
                self.assertEqual(setup_support.acquire_model(root, manifest, "model"), snapshot)
            download.assert_called_once_with(repo_id="owner/model", revision="pin",
                cache_dir=root / ".cache/huggingface", local_files_only=True)
            from huggingface_hub.errors import LocalEntryNotFoundError
            with patch("huggingface_hub.snapshot_download", side_effect=[
                LocalEntryNotFoundError("not cached"), str(snapshot)
            ]) as download:
                self.assertEqual(setup_support.acquire_model(root, manifest, "model"), snapshot)
            self.assertTrue(download.call_args_list[0].kwargs["local_files_only"])
            self.assertNotIn("local_files_only", download.call_args_list[1].kwargs)


class SourceAcquisitionTests(unittest.TestCase):
    """Verify source-only acquisition and reuse without network or model execution."""

    def test_verified_download_cleans_local_metadata_and_reuses_source(self):
        from tests.test_import import write_source
        from util import p01_04_gift_eval_acquisition as acquisition

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = root / "fixture/m4_daily"
            write_source(fixture, [[float(i) for i in range(31)]])
            expected = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                        for p in fixture.iterdir()}
            dependency = json.loads((ROOT / "config/dependencies/gift_eval.json").read_text())
            dependency["dataset"]["phase_0_1_sha256"] = expected
            source = root / "data/gift_eval"
            shared_cache = root / ".cache/huggingface"
            shared_cache.mkdir(parents=True)
            (shared_cache / "retained").write_bytes(b"existing shared cache")

            def download(**arguments):
                self.assertEqual(arguments["cache_dir"], shared_cache)
                shutil.copytree(fixture, arguments["local_dir"] / "m4_daily")
                metadata = arguments["local_dir"] / ".cache/huggingface/download"
                metadata.mkdir(parents=True)
                (metadata / "fixture.lock").write_text("download metadata")

            with (patch.object(acquisition, "repository_root", return_value=root),
                  patch.object(acquisition, "snapshot_download", side_effect=download)):
                result = acquisition.acquire(source, dependency, "m4_daily")
            self.assertEqual(result["outcome"], "downloaded")
            self.assertEqual(sorted(p.name for p in source.iterdir()), ["m4_daily"])
            self.assertEqual({p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                              for p in (source / "m4_daily").iterdir()}, expected)
            self.assertEqual((shared_cache / "retained").read_bytes(), b"existing shared cache")
            before = tree_state(source)
            with patch.object(acquisition, "snapshot_download", side_effect=AssertionError("redownload")):
                repeated = acquisition.acquire(source, dependency, "m4_daily")
            self.assertEqual(repeated["outcome"], "already_valid")
            self.assertEqual(tree_state(source), before)
            (source / "m4_daily/state.json").write_text("{}")
            with self.assertRaisesRegex(acquisition.ImportValidationError, "hashes do not match"):
                acquisition.verify_source(source, dependency, "m4_daily")


class VerificationTests(unittest.TestCase):
    def test_child_offline_settings(self):
        with patch.object(setup_support.subprocess, "run") as run:
            setup_support.run_read_only(["python", "-B", "-c", "import duckdb"], ROOT)
        environment = run.call_args.kwargs["env"]
        for name in ("PYTHONDONTWRITEBYTECODE", "HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE", "HF_DATASETS_OFFLINE"):
            self.assertEqual(environment[name], "1")
        self.assertEqual(environment["HF_HOME"], str(ROOT / ".cache/huggingface"))

    def test_readiness_is_read_only_and_does_not_hash_assets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = make_repository(Path(directory) / "project")
            manifests = root / "config/dependencies"
            manifests.mkdir(parents=True)
            (manifests / "models.json").write_text(json.dumps({"model": {
                "repository": "owner/model", "revision": "pin", "required_files": {"weights": "unused"}}}))
            (manifests / "gift_eval.json").write_text(json.dumps({"dataset": {
                "default_local_source_directory": "data", "phase_0_1_subset": "subset",
                "phase_0_1_sha256": {"data.arrow": "unused"}}}))
            before = tree_state(root)
            with (patch.object(setup_support, "check_source"),
                  patch.object(setup_support, "run_read_only", return_value=subprocess.CompletedProcess([], 0, "uv 0.12.18 (build metadata)\n", "")) as run,
                  patch.object(setup_support, "sha256_file", side_effect=AssertionError("deep audit"))):
                failures = setup_support.verify_installation(root)
            self.assertEqual(failures, ["model model", "GIFT-Eval data"])
            self.assertEqual(tree_state(root), before)
            commands = str(run.call_args_list)
            for forbidden in ("snapshot_download", "from_pretrained", "cuda", "lock", "sync"):
                self.assertNotIn(forbidden, commands)


if __name__ == "__main__":
    unittest.main()
