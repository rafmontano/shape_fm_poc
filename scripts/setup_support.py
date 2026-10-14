#!/usr/bin/env python3
"""Atomic asset acquisition and small offline readiness checks for setup.sh."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import time
import urllib.request
from pathlib import Path
from typing import Callable, Sequence


class SetupError(RuntimeError):
    """Identify an acquisition or readiness failure."""


def require_repository_root(root: Path) -> Path:
    """Reject a destination that is not this project's root."""
    root = root.resolve()
    if not all((root / name).exists() for name in (
        ".git", "shape_fm_poc.Rproj", "pyproject.toml", "renv.lock", "scripts/setup.sh"
    )):
        raise SetupError(f"repository-root validation failed: {root}")
    return root


def require_inside_repository(root: Path, path: Path) -> Path:
    """Keep acquisition destinations inside the repository."""
    path = path.resolve()
    if not path.is_relative_to(root.resolve()):
        raise SetupError(f"refusing path outside repository: {path}")
    return path


def python_projects(root: Path) -> list[Path]:
    """Discover retained independently locked environments, root first."""
    return [root] + [path for path in sorted((root / "environments").glob("*"))
                     if path.is_dir()
                     and (path / "pyproject.toml").is_file()
                     and (path / "uv.lock").is_file()]


def nonempty_file(path: Path) -> bool:
    """Check asset presence without loading it."""
    return path.is_file() and path.stat().st_size > 0


def file_contains(text: str) -> Callable[[Path], bool]:
    """Validate the bootstrap installer before publication."""
    return lambda path: nonempty_file(path) and text.encode() in path.read_bytes()


def download_with_retry(url: str, destination: Path, *, attempts: int = 3,
                        timeout: int = 7200, delay: float = 5,
                        validator: Callable[[Path], bool] = nonempty_file,
                        opener: Callable = urllib.request.urlopen) -> str:
    """Reuse valid assets; publish only validated bytes after bounded retries."""
    if attempts < 3 or timeout <= 0 or delay < 0:
        raise SetupError("invalid download retry settings")
    if validator(destination):
        return "reused"
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".part")
    for attempt in range(attempts):
        try:
            with opener(url, timeout=timeout) as response, temporary.open("wb") as output:
                shutil.copyfileobj(response, output)
            if not validator(temporary):
                raise SetupError("downloaded file failed validation")
            os.replace(temporary, destination)
            return "downloaded"
        except (OSError, SetupError) as exc:
            temporary.unlink(missing_ok=True)
            if attempt + 1 == attempts:
                raise SetupError(f"download failed after {attempts} attempts: {url}: {exc}") from exc
            time.sleep(delay)
    raise AssertionError("unreachable")


def sha256_file(path: Path) -> str:
    """Check acquired model identity before accepting it."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def acquire_model(root: Path, manifest_path: Path, model_key: str) -> Path:
    """Try the shared cache offline first, then acquire a pinned atomic snapshot."""
    root = require_repository_root(root)
    model = json.loads(require_inside_repository(root, manifest_path).read_text())[model_key]
    from huggingface_hub import snapshot_download
    from huggingface_hub.errors import LocalEntryNotFoundError

    arguments = dict(repo_id=model["repository"], revision=model["revision"],
                     cache_dir=root / ".cache/huggingface")

    def valid(snapshot: Path) -> bool:
        return all(nonempty_file(snapshot / name) and sha256_file(snapshot / name) == digest
                   for name, digest in model["required_files"].items())

    try:
        snapshot = Path(snapshot_download(**arguments, local_files_only=True))
        if valid(snapshot):
            return snapshot
    except LocalEntryNotFoundError:
        pass
    snapshot = Path(snapshot_download(**arguments))
    if not valid(snapshot):
        raise SetupError(f"{model_key}: required model files missing or checksum mismatch")
    return snapshot


def run_read_only(command: Sequence[str], root: Path) -> subprocess.CompletedProcess[str]:
    """Run only local checks, disabling bytecode, hub access and R autoloading."""
    environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", HF_HUB_OFFLINE="1",
                       TRANSFORMERS_OFFLINE="1", HF_DATASETS_OFFLINE="1",
                       HF_HOME=str(root / ".cache/huggingface"),
                       HF_HUB_CACHE=str(root / ".cache/huggingface"),
                       RENV_CONFIG_AUTOLOADER_ENABLED="FALSE")
    return subprocess.run(command, cwd=root, env=environment, text=True,
                          capture_output=True, timeout=120, check=False)


def failure_detail(completed: subprocess.CompletedProcess[str]) -> str:
    """Keep useful errors rather than the generic R trailer."""
    lines = [line for line in (completed.stderr or completed.stdout).strip().splitlines()
             if line.strip() != "Execution halted"]
    return lines[-1] if lines else "command failed"


def check_source(root: Path) -> None:
    """Require the manifest-pinned GIFT checkout, without fetching."""
    code = json.loads((root / "config/dependencies/gift_eval.json").read_text())["code"]
    result = run_read_only(["git", "-C", str(root / code["path"]), "rev-parse", "HEAD"], root)
    if result.returncode or result.stdout.strip() != code["commit"]:
        raise SetupError("GIFT-Eval checkout is missing or differs from the pinned revision")


def verify_installation(root: Path) -> list[str]:
    """Check small imports and pinned asset presence only; never resolve or load models."""
    root = require_repository_root(root)
    failures = []

    def check(label: str, action: Callable[[], None]) -> None:
        try:
            action()
            print(f"[ready] {label}")
        except (OSError, SetupError, subprocess.SubprocessError) as exc:
            failures.append(label)
            print(f"[missing] {label}: {exc}")

    def command(arguments: list[str]) -> None:
        result = run_read_only(arguments, root)
        if result.returncode:
            raise SetupError(failure_detail(result))

    def uv() -> None:
        result = run_read_only([str(root / ".tools/uv/uv"), "--version"], root)
        if result.returncode or result.stdout.split()[:2] != ["uv", "0.12.18"]:
            raise SetupError("project-local uv 0.12.18 is required")

    check("uv", uv)
    check("GIFT-Eval checkout", lambda: check_source(root))
    check("R imports", lambda: command(["Rscript", "--vanilla", "scripts/setup_r.R", "verify"]))
    imports = {".": "duckdb, pyarrow, dask, distributed, prefect, prefect_dask",
               "gift-eval": "gift_eval", "chronos-2": "chronos", "mantis": "mantis",
               "classifiers": "sklearn, sktime, aeon"}
    for project in python_projects(root):
        label = "." if project == root else project.name
        check(f"Python {label}", lambda project=project, label=label: command([
            str(project / ".venv/bin/python"), "-B", "-c",
            f"import {imports.get(label, 'importlib')}"
        ]))

    def presence(paths: list[Path]) -> None:
        missing = [str(path.relative_to(root)) for path in paths if not nonempty_file(path)]
        if missing:
            raise SetupError(", ".join(missing))

    models = json.loads((root / "config/dependencies/models.json").read_text())
    for name, model in models.items():
        snapshot = (root / ".cache/huggingface" /
                    ("models--" + model["repository"].replace("/", "--")) /
                    "snapshots" / model["revision"])
        check(f"model {name}", lambda snapshot=snapshot, model=model:
              presence([snapshot / name for name in model["required_files"]]))
    data = json.loads((root / "config/dependencies/gift_eval.json").read_text())["dataset"]
    source = root / data["default_local_source_directory"] / data["phase_0_1_subset"]
    check("GIFT-Eval data", lambda: presence([source / name for name in data["phase_0_1_sha256"]]))
    print(f"Readiness: {len(failures)} missing prerequisite(s)")
    return failures


def main(argv: Sequence[str] | None = None) -> int:
    """Dispatch internal setup operations."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("projects", "check-source", "verify", "acquire-model"):
        child = commands.add_parser(name)
        child.add_argument("--root", type=Path, required=True)
        if name == "acquire-model":
            child.add_argument("--manifest", type=Path, required=True)
            child.add_argument("--model", required=True)
    download = commands.add_parser("download")
    download.add_argument("--url", required=True)
    download.add_argument("--destination", type=Path, required=True)
    download.add_argument("--attempts", type=int, default=3)
    download.add_argument("--timeout", type=int, default=7200)
    download.add_argument("--delay", type=float, default=5)
    download.add_argument("--contains")
    args = parser.parse_args(argv)
    try:
        if args.command == "download":
            root = require_repository_root(Path.cwd())
            download_with_retry(args.url, require_inside_repository(root, args.destination),
                                attempts=args.attempts, timeout=args.timeout, delay=args.delay,
                                validator=file_contains(args.contains) if args.contains else nonempty_file)
        else:
            root = require_repository_root(args.root)
            if args.command == "projects":
                print("\n".join(str(path.relative_to(root)) for path in python_projects(root)))
            elif args.command == "check-source":
                check_source(root)
            elif args.command == "acquire-model":
                print(acquire_model(root, args.manifest, args.model))
            else:
                return int(bool(verify_installation(root)))
    except (OSError, SetupError, subprocess.SubprocessError) as exc:
        print(f"setup error: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
