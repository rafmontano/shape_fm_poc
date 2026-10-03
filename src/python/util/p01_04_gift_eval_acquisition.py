# ==============================================================================
# p01_04_gift_eval_acquisition.py
#
# Purpose: Acquire and verify the immutable, revision-pinned GIFT-Eval source data.
# Inputs: Dependency-lock metadata, acquisition scope, and a GIFT-Eval source directory.
# Outputs: Verified source files and `source-manifest.json`; validation details on stdout.
# Run from: Developer utility: `.venv/bin/python -m util.p01_04_gift_eval_acquisition <m4_daily|complete|verify>` with `PYTHONPATH=src/python`.
# ==============================================================================

"""Acquire and verify the immutable, revision-pinned GIFT-Eval source data."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Sequence

import pyarrow as pa
import pyarrow.ipc as ipc
from huggingface_hub import HfApi, snapshot_download

from .shared_configuration import ImportValidationError
from .shared_provenance import atomic_write_json, sha256_file, utc_now


# Code constant: acquisition-protocol manifest filename; callers cannot override it.
MANIFEST_NAME = "source-manifest.json"


def repository_root() -> Path:
    """Return the repository root containing this utility module."""
    return Path(__file__).resolve().parents[3]


def load_dependency(path: Path) -> dict[str, Any]:
    """Decode the dependency-lock JSON file at `path`."""
    with path.open("r", encoding="utf-8") as stream:
        return json.load(stream)


def verify_code(dependency: dict[str, Any]) -> dict[str, str]:
    """Purpose: Verify the local GIFT-Eval code checkout against dependency-lock metadata.

    Inputs: Decoded dependency configuration containing repository, path, and commit.
    Outputs: Repository/commit/path identity, or ``ImportValidationError`` on absence or mismatch.
    Side effects: Runs a read-only Git subprocess in the configured checkout.
    """
    code = dependency["code"]
    checkout = repository_root() / code["path"]
    try:
        actual = subprocess.run(
            ["git", "-C", str(checkout), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise ImportValidationError(
            "GIFT-Eval submodule is not initialized; run "
            "git submodule update --init --recursive"
        ) from exc
    if actual != code["commit"]:
        raise ImportValidationError(
            f"GIFT-Eval submodule is at {actual}, expected {code['commit']}"
        )
    return {"repository": code["repository"], "commit": actual, "path": str(checkout)}


def required_fingerprints(
    source_root: Path,
    dataset_names: Sequence[str],
    required_names: Sequence[str],
) -> dict[str, dict[str, Any]]:
    """Purpose: Validate and fingerprint required files in a local dataset snapshot.

    Inputs: Source root, selected dataset directories, and required JSON/Arrow filenames from the lock.
    Outputs: Source-relative file records containing byte sizes and SHA-256 hashes.
    Side effects: Reads every required file and fully decodes JSON or Arrow content.
    """
    files: dict[str, dict[str, Any]] = {}
    for dataset_name in dataset_names:
        dataset_dir = source_root / dataset_name
        for name in required_names:
            path = dataset_dir / name
            if not path.is_file():
                raise ImportValidationError(f"required source file is missing: {path}")
            if name.endswith(".json"):
                try:
                    with path.open("r", encoding="utf-8") as stream:
                        json.load(stream)
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise ImportValidationError(f"invalid JSON metadata: {path}") from exc
            elif name.endswith(".arrow"):
                try:
                    with pa.memory_map(str(path), "r") as source:
                        table = ipc.open_stream(source).read_all()
                except (OSError, pa.ArrowException) as exc:
                    raise ImportValidationError(f"invalid Arrow source: {path}") from exc
                if table.num_rows == 0:
                    raise ImportValidationError(f"Arrow source contains no rows: {path}")
            relative = path.relative_to(source_root).as_posix()
            files[relative] = {
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
    return files


def expected_phase_files(dependency: dict[str, Any]) -> dict[str, str]:
    """Expand the locked phase-0/1 subset hashes into source-root-relative file names."""
    subset = dependency["dataset"]["phase_0_1_subset"]
    return {
        f"{subset}/{name}": digest
        for name, digest in dependency["dataset"]["phase_0_1_sha256"].items()
    }


def hashes_match(files: dict[str, dict[str, Any]], expected: dict[str, str]) -> bool:
    """Return whether the observed file set and every SHA-256 exactly match the lock."""
    return set(files) == set(expected) and all(
        files[name]["sha256"] == digest for name, digest in expected.items()
    )


def manifest_matches(
    manifest_path: Path,
    repository: str,
    revision: str,
    scope: str,
    files: dict[str, dict[str, Any]],
) -> bool:
    """Return whether an existing manifest exactly identifies repository, revision, scope, and files."""
    try:
        with manifest_path.open("r", encoding="utf-8") as stream:
            manifest = json.load(stream)
    except (FileNotFoundError, json.JSONDecodeError):
        return False
    return (
        manifest.get("repository") == repository
        and manifest.get("revision") == revision
        and manifest.get("scope") == scope
        and manifest.get("files") == files
    )


def remote_dataset_names(repository: str, revision: str) -> list[str]:
    """Purpose: Discover dataset directories present in a pinned Hugging Face snapshot.

    Inputs: Dataset repository ID and immutable revision from dependency configuration.
    Outputs: Sorted top-level directories containing the expected Arrow shard.
    Side effects: Performs a Hugging Face API request.
    """
    files = HfApi().list_repo_files(
        repository, repo_type="dataset", revision=revision
    )
    names = {
        Path(name).parent.as_posix()
        for name in files
        if name.endswith("/data-00000-of-00001.arrow")
    }
    if not names:
        raise ImportValidationError("the pinned snapshot contains no dataset Arrow files")
    return sorted(names)


def verify_source(
    source_root: Path, dependency: dict[str, Any], scope: str
) -> dict[str, Any]:
    """Purpose: Validate a local GIFT-Eval dataset snapshot for the requested acquisition scope.

    Inputs: Source directory, dependency-lock mapping, and ``m4_daily`` or complete scope.
    Outputs: Manifest content with repository/revision, dataset directories, and file fingerprints.
    Side effects: Reads local source files and queries Hugging Face for complete-scope directory names.
    """
    dataset = dependency["dataset"]
    names = (
        [dataset["phase_0_1_subset"]]
        if scope == "m4_daily"
        else remote_dataset_names(dataset["repository"], dataset["revision"])
    )
    files = required_fingerprints(source_root, names, dataset["required_files"])
    if scope == "m4_daily" and not hashes_match(files, expected_phase_files(dependency)):
        raise ImportValidationError(
            "M4 Daily source hashes do not match the pinned validated snapshot"
        )
    return {
        "repository": dataset["repository"],
        "revision": dataset["revision"],
        "scope": scope,
        "dataset_directories": names,
        "files": files,
    }


def acquire(source_root: Path, dependency: dict[str, Any], scope: str) -> dict[str, Any]:
    """Purpose: Acquire, verify, and manifest an immutable GIFT-Eval dataset snapshot.

    Inputs: Destination root, dependency-lock metadata, and ``m4_daily`` or complete scope.
    Outputs: Verified manifest data, manifest path, and ``already_valid``/``downloaded`` outcome.
    Side effects: May create directories, download snapshot files/cache data, and atomically write the manifest.
    """
    source_root = source_root.resolve()
    manifest_path = source_root / MANIFEST_NAME
    try:
        verified = verify_source(source_root, dependency, scope)
    except ImportValidationError:
        verified = None
    if verified is not None:
        outcome = "already_valid"
    else:
        dataset = dependency["dataset"]
        source_root.mkdir(parents=True, exist_ok=True)
        snapshot_download(
            repo_id=dataset["repository"],
            repo_type="dataset",
            revision=dataset["revision"],
            local_dir=source_root,
            cache_dir=repository_root() / "data" / ".cache" / "huggingface",
            allow_patterns=[f"{dataset['phase_0_1_subset']}/*"]
            if scope == "m4_daily"
            else None,
        )
        verified = verify_source(source_root, dependency, scope)
        outcome = "downloaded"

    manifest_current = manifest_matches(
        manifest_path,
        verified["repository"],
        verified["revision"],
        scope,
        verified["files"],
    )
    if not manifest_current:
        atomic_write_json(
            manifest_path,
            {
                **verified,
                "source_directory": str(source_root),
                "verified_at": utc_now(),
            },
        )
    return {**verified, "manifest": str(manifest_path), "outcome": outcome}


def build_parser() -> argparse.ArgumentParser:
    """Purpose: Define the GIFT-Eval acquisition and verification command line.

    Inputs: Repository-relative default dependency lock and later CLI arguments.
    Outputs: Parser for scope, optional source root, and dependency configuration path.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scope", choices=("m4_daily", "complete", "verify"))
    parser.add_argument("--source-root", type=Path)
    # Bootstrap/interface default: repository dependency-lock path used before
    # source discovery; ``--dependency`` overrides it and it is not scientific identity.
    parser.add_argument(
        "--dependency",
        type=Path,
        default=Path("config/dependencies/gift_eval.json"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Purpose: Parse and dispatch GIFT-Eval source acquisition or verification.

    Inputs: Optional arguments; defaults originate in the dependency lock and repository layout.
    Outputs: Manifest JSON on stdout with status 0, or an error on stderr with status 1.
    Side effects: May access Hugging Face, download files, write ``source-manifest.json``, and run Git.
    """
    args = build_parser().parse_args(argv)
    try:
        dependency = load_dependency(args.dependency)
        source_root = args.source_root or repository_root() / dependency["dataset"][
            "default_local_source_directory"
        ]
        result = (
            verify_source(source_root, dependency, "m4_daily")
            if args.scope == "verify"
            else acquire(source_root, dependency, args.scope)
        )
        result["code"] = verify_code(dependency)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (ImportValidationError, OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
