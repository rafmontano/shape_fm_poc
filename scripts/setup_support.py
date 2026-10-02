#!/usr/bin/env python3
# ==============================================================================
# setup_support.py
#
# Purpose: Provide testable archive, download, platform, and installation-verification logic for setup.sh.
# Inputs: Repository root, setup subcommand arguments, committed lock/manifests, and installed local environments.
# Outputs: Archive plans, atomic downloads, and a human-readable installation report on stdout.
# Run from: Invoked by scripts/setup.sh; not run directly by researchers.
# ==============================================================================

"""Safety-critical support for the ShapeFM installation entry point."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Sequence


# Code constant: project-owned folders that a full rebuild archives. Nested
# virtual environments retain their parent environment definitions and locks.
ARCHIVE_PATHS = (
    ".venv",
    "environments/gift-eval/.venv",
    "environments/chronos-2/.venv",
    "environments/mantis/.venv",
    "environments/classifiers/.venv",
    "environments/tensorflow/.venv",
    "renv/library",
    "data",
    "models",
    "results",
    ".cache",
    ".tools/cache",
    ".tools/python",
    ".tools/renv-cache",
)

# Code constant: every independently locked Python environment and the imports
# that prove its stated responsibility. Expected exact versions mirror the
# direct pins in the corresponding pyproject files.
PYTHON_ENVIRONMENTS = {
    "core": {
        "project": ".",
        "python": ".venv/bin/python",
        "versions": {
            "dask": "2026.8.0",
            "distributed": "2026.8.0",
            "prefect": "3.8.7",
            "prefect-dask": "0.3.7",
        },
        "imports": (
            "import dask, distributed, duckdb, importlib.metadata, prefect, prefect_dask, pyarrow",
            "values={'dask': dask.__version__, 'distributed': distributed.__version__, "
            "'duckdb': duckdb.__version__, 'pyarrow': pyarrow.__version__, "
            "'prefect': prefect.__version__, "
            "'prefect-dask': importlib.metadata.version('prefect-dask')}",
        ),
    },
    "gift-eval": {
        "project": "environments/gift-eval",
        "python": "environments/gift-eval/.venv/bin/python",
        "versions": {},
        "imports": (
            "import gift_eval",
            "values={'gift_eval': getattr(gift_eval, '__version__', 'local-submodule')}",
        ),
    },
    "chronos-2": {
        "project": "environments/chronos-2",
        "python": "environments/chronos-2/.venv/bin/python",
        "versions": {"chronos": "2.2.2"},
        "imports": (
            "import importlib.metadata, torch",
            "values={'chronos': importlib.metadata.version('chronos-forecasting'), "
            "'torch': torch.__version__}",
        ),
        "torch": True,
    },
    "mantis": {
        "project": "environments/mantis",
        "python": "environments/mantis/.venv/bin/python",
        "versions": {"mantis": "1.1.0", "torch": "2.12.0"},
        "imports": (
            "import importlib.metadata, mantis, torch",
            "values={'mantis': importlib.metadata.version('mantis-tsfm'), "
            "'torch': torch.__version__}",
        ),
        "torch": True,
    },
    "conventional-classifiers": {
        "project": "environments/classifiers",
        "python": "environments/classifiers/.venv/bin/python",
        "versions": {"sklearn": "1.7.2", "sktime": "0.40.1"},
        "imports": (
            "import sklearn, sktime; "
            "from sktime.classification.distance_based import KNeighborsTimeSeriesClassifier; "
            "from sktime.classification.sklearn import RotationForest; "
            "from sktime.classification.kernel_based import RocketClassifier",
            "values={'sklearn': sklearn.__version__, 'sktime': sktime.__version__}",
        ),
    },
    "tensorflow-classifiers": {
        "project": "environments/tensorflow",
        "python": "environments/tensorflow/.venv/bin/python",
        "versions": {},
        "platform_versions": {
            "Darwin": {"tensorflow": "2.18.1", "tensorflow_metal": "1.2.0"},
            "Linux": {"tensorflow": "2.21.0"},
        },
        "imports": (
            "import importlib.metadata, tensorflow as tf; "
            "from sktime.classification.deep_learning import InceptionTimeClassifier",
            "gpus=tf.config.list_physical_devices('GPU'); "
            "device='/GPU:0' if gpus else '/CPU:0'; "
            "ctx=tf.device(device); ctx.__enter__(); result=tf.reduce_sum(tf.constant([1.0,2.0])); "
            "ctx.__exit__(None,None,None); "
            "values={'tensorflow': tf.__version__, 'built_with_cuda': tf.test.is_built_with_cuda(), "
            "'tensorflow_metal': next((item.version for item in importlib.metadata.distributions() "
            "if item.metadata['Name'].lower() == 'tensorflow-metal'), None), "
            "'devices': [item.name for item in gpus], 'selected_device': device, "
            "'operation_result': float(result.numpy())}",
        ),
        "tensorflow": True,
    },
}


class SetupError(RuntimeError):
    """Identify a setup safety, download, or verification failure."""


@dataclass(frozen=True)
class ArchiveMove:
    """Describe one source-to-dated-destination rebuild move."""

    source: Path
    destination: Path


@dataclass(frozen=True)
class Check:
    """Represent one report line and whether failure blocks installation."""

    name: str
    status: str
    detail: str
    hard_failure: bool = True


def repository_root_is_valid(root: Path) -> bool:
    """Return whether a path has the immutable markers of this repository root."""
    root = root.resolve()
    return (
        (root / ".git").exists()
        and (root / "shape_fm_poc.Rproj").is_file()
        and (root / "pyproject.toml").is_file()
        and (root / "renv.lock").is_file()
        and (root / "scripts/setup.sh").is_file()
    )


def require_repository_root(root: Path) -> Path:
    """Resolve and validate the repository root or raise a corrective error."""
    resolved = root.resolve()
    if not repository_root_is_valid(resolved):
        raise SetupError(
            f"repository-root validation failed: {resolved}; expected ShapeFM markers"
        )
    return resolved


def require_inside_repository(root: Path, path: Path) -> Path:
    """Return a resolved project path, refusing paths outside the repository."""
    root = root.resolve()
    resolved = path.resolve(strict=False)
    if resolved != root and root not in resolved.parents:
        raise SetupError(f"refusing path outside repository: {path}")
    return resolved


def archive_name(path: Path, date_text: str) -> Path:
    """Return the sibling ``original-name_DDMMYY`` archive destination."""
    if len(date_text) != 6 or not date_text.isdigit():
        raise SetupError(f"archive date must use DDMMYY: {date_text}")
    return path.with_name(f"{path.name}_{date_text}")


def build_archive_plan(
    root: Path,
    date_text: str,
    relative_paths: Iterable[str] = ARCHIVE_PATHS,
) -> list[ArchiveMove]:
    """Build and collision-check the complete rebuild plan before any move.

    Only existing project-owned paths enter the plan. Every source and destination
    is resolved beneath the repository, and any dated collision aborts the entire
    preflight so no earlier archive can be overwritten or merged.
    """
    root = require_repository_root(root)
    plan: list[ArchiveMove] = []
    for relative in relative_paths:
        source = require_inside_repository(root, root / relative)
        if not source.exists():
            continue
        destination = require_inside_repository(root, archive_name(source, date_text))
        if destination.exists():
            raise SetupError(f"archive destination already exists: {destination}")
        plan.append(ArchiveMove(source, destination))
    return plan


def execute_archive_plan(
    root: Path,
    plan: Sequence[ArchiveMove],
    mover: Callable[[Path, Path], object] = shutil.move,
) -> None:
    """Move every preflighted project folder, stopping at the exact failed source."""
    root = require_repository_root(root)
    for move in plan:
        require_inside_repository(root, move.source)
        require_inside_repository(root, move.destination)
        try:
            mover(move.source, move.destination)
        except OSError as exc:
            raise SetupError(f"failed to archive folder {move.source}: {exc}") from exc


def nonempty_file(path: Path) -> bool:
    """Return whether a download candidate is a nonempty regular file."""
    return path.is_file() and path.stat().st_size > 0


def file_contains(required_text: str) -> Callable[[Path], bool]:
    """Build a validator that requires nonempty text content before publication."""
    required_bytes = required_text.encode()
    return lambda path: nonempty_file(path) and required_bytes in path.read_bytes()


def download_with_retry(
    url: str,
    destination: Path,
    *,
    attempts: int = 3,
    timeout: int = 7200,
    delay: float = 5.0,
    validator: Callable[[Path], bool] = nonempty_file,
    opener: Callable[..., object] = urllib.request.urlopen,
) -> str:
    """Download to a temporary file, validate, and atomically publish after retries.

    A valid existing destination is reused. Failed or incomplete ``.part`` files
    are removed and never presented as final assets. The final exception includes
    the URL and retry count without exposing headers or credentials.
    """
    if attempts < 3:
        raise SetupError("download attempts must be at least three")
    if validator(destination):
        return "reused"
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f"{destination.name}.part")
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        temporary.unlink(missing_ok=True)
        print(f"[download] attempt {attempt}/{attempts}: {url}", flush=True)
        try:
            with opener(url, timeout=timeout) as response, temporary.open("wb") as output:
                shutil.copyfileobj(response, output)
            if not validator(temporary):
                raise SetupError("downloaded file failed validation")
            os.replace(temporary, destination)
            return "downloaded"
        except (OSError, SetupError) as exc:
            last_error = exc
            temporary.unlink(missing_ok=True)
            if attempt < attempts:
                time.sleep(delay)
    raise SetupError(f"download failed after {attempts} attempts: {url}: {last_error}")


def platform_selection(system: str, machine: str, nvidia_available: bool) -> str:
    """Classify supported Apple, NVIDIA Linux, and CPU-only installation modes."""
    normalized_system = system.lower()
    normalized_machine = machine.lower()
    if normalized_system == "darwin" and normalized_machine == "arm64":
        return "macos-arm64-metal"
    if normalized_system == "linux" and normalized_machine in {"x86_64", "amd64"}:
        return "linux-x86_64-cuda" if nvidia_available else "linux-x86_64-cpu"
    return "unsupported"


def sha256_file(path: Path) -> str:
    """Return the SHA-256 digest of one file without changing it."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def acquire_model(root: Path, manifest_path: Path, model_key: str) -> Path:
    """Acquire one immutable model snapshot and validate its required checksums.

    Hugging Face writes downloads through incomplete temporary files and publishes
    cache blobs atomically. This function additionally withholds success until
    every required model file matches the committed identity manifest.
    """
    root = require_repository_root(root)
    manifest_path = require_inside_repository(root, manifest_path)
    manifest = json.loads(manifest_path.read_text())[model_key]
    from huggingface_hub import snapshot_download

    snapshot = Path(
        snapshot_download(
            repo_id=manifest["repository"],
            revision=manifest["revision"],
            cache_dir=root / ".cache/huggingface",
        )
    )
    failures = []
    for filename, expected in manifest["required_files"].items():
        candidate = snapshot / filename
        if not candidate.is_file():
            failures.append(f"missing {filename}")
        elif sha256_file(candidate) != expected:
            failures.append(f"checksum mismatch {filename}")
    if failures:
        raise SetupError(f"{model_key} model validation failed: {'; '.join(failures)}")
    return snapshot


def run_read_only(
    command: Sequence[str], root: Path, environment: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    """Run a verification command with bytecode and network-backed model access disabled."""
    values = os.environ.copy()
    values.update(
        {
            "PYTHONDONTWRITEBYTECODE": "1",
            "HF_HUB_OFFLINE": "1",
            "TRANSFORMERS_OFFLINE": "1",
            "TOKENIZERS_PARALLELISM": "false",
            "TF_CPP_MIN_LOG_LEVEL": "2",
        }
    )
    if environment:
        values.update(environment)
    return subprocess.run(
        command,
        cwd=root,
        env=values,
        text=True,
        capture_output=True,
        timeout=300,
        check=False,
    )


def failure_detail(completed: subprocess.CompletedProcess[str]) -> str:
    """Return the most useful child-process diagnostic instead of a generic trailer."""
    lines = (completed.stderr or completed.stdout).strip().splitlines()
    useful = [line for line in lines if line.strip() not in {"Execution halted"}]
    return useful[-1] if useful else (lines[-1] if lines else "command failed")


def installed_nvidia_paths(python: Path, root: Path) -> tuple[str, str]:
    """Discover pip-installed NVIDIA libraries/binaries from one isolated environment."""
    code = (
        "import glob, json, site; p=site.getsitepackages()[0]; "
        "print(json.dumps({'libraries': sorted(glob.glob(p+'/nvidia/*/lib')), "
        "'executables': sorted(glob.glob(p+'/nvidia/*/bin'))}))"
    )
    completed = run_read_only([str(python), "-B", "-c", code], root)
    if completed.returncode != 0:
        return "", ""
    values = json.loads(completed.stdout)
    return ":".join(values["libraries"]), ":".join(values["executables"])


def verify_python_environment(root: Path, name: str, definition: dict) -> tuple[Check, dict]:
    """Validate one environment's lock, imports, versions, and small device operation."""
    project = root / str(definition["project"])
    python = root / str(definition["python"])
    lock = project / "uv.lock"
    pyproject = project / "pyproject.toml"
    if not pyproject.is_file() or not lock.is_file() or not python.is_file():
        return Check(name, "FAIL", "missing pyproject, uv.lock, or local virtual environment"), {}

    imports, assignment = definition["imports"]
    if definition.get("torch"):
        assignment += (
            "; cuda=torch.cuda.is_available(); "
            "mps=hasattr(torch.backends, 'mps') and torch.backends.mps.is_available(); "
            "device='cuda' if cuda else ('mps' if mps else 'cpu'); "
            "result=torch.tensor([1.0,2.0], device=device).sum().item(); "
            "values.update({'cuda_available': cuda, 'mps_available': mps, "
            "'cuda_runtime': torch.version.cuda, 'selected_device': device, "
            "'device_name': torch.cuda.get_device_name(0) if cuda else "
            "('Apple Metal' if mps else 'CPU'), 'operation_result': result})"
        )
    code = (
        f"import json,sys; {imports}; {assignment}; "
        "values['python']=sys.version.split()[0]; print(json.dumps(values, sort_keys=True))"
    )
    process_environment: dict[str, str] = {}
    if definition.get("torch") or definition.get("tensorflow"):
        libraries, executables = installed_nvidia_paths(python, root)
        if platform.system() == "Linux" and libraries:
            # These paths apply only to this child. TensorFlow and PyTorch never
            # receive one another's environment-specific NVIDIA libraries.
            process_environment["LD_LIBRARY_PATH"] = libraries
        if executables:
            process_environment["PATH"] = f"{executables}:{os.environ.get('PATH', '')}"
    completed = run_read_only([str(python), "-B", "-c", code], root, process_environment)
    if completed.returncode != 0:
        return Check(name, "FAIL", failure_detail(completed)), {}
    try:
        values = json.loads(completed.stdout.strip().splitlines()[-1])
    except (json.JSONDecodeError, IndexError) as exc:
        return Check(name, "FAIL", f"invalid validation output: {exc}"), {}
    expected_versions = dict(definition["versions"])
    expected_versions.update(definition.get("platform_versions", {}).get(platform.system(), {}))
    mismatches = [
        f"{package}={values.get(package)!r} expected {expected}"
        for package, expected in expected_versions.items()
        if str(values.get(package)) != expected
    ]
    if mismatches:
        return Check(name, "FAIL", "; ".join(mismatches)), values
    detail = ", ".join(f"{key}={value}" for key, value in sorted(values.items()))
    return Check(name, "PASS", detail), values


def verify_lock_consistency(root: Path, uv: Path, project: str) -> Check:
    """Ask uv to check one lock without resolving, downloading, or changing files."""
    completed = run_read_only(
        [str(uv), "lock", "--check", "--offline", "--project", str(root / project)],
        root,
    )
    label = "core" if project == "." else Path(project).name
    if completed.returncode:
        return Check(f"lock:{label}", "FAIL", failure_detail(completed))
    return Check(f"lock:{label}", "PASS", "committed uv.lock is current")


def verify_submodule(root: Path) -> Check:
    """Validate the GIFT-Eval submodule checkout against its committed manifest."""
    manifest = json.loads((root / "config/dependencies/gift_eval.json").read_text())
    expected = manifest["code"]["commit"]
    path = root / manifest["code"]["path"]
    if not (path / ".git").exists():
        return Check("submodule:gift-eval", "FAIL", "submodule is not initialized")
    completed = run_read_only(["git", "-C", str(path), "rev-parse", "HEAD"], root)
    actual = completed.stdout.strip()
    if completed.returncode or actual != expected:
        return Check("submodule:gift-eval", "FAIL", f"revision={actual or 'unavailable'} expected={expected}")
    return Check("submodule:gift-eval", "PASS", f"revision={actual}")


def verify_models(root: Path) -> list[Check]:
    """Validate pinned model snapshots and required-file checksums without downloading."""
    manifest = json.loads((root / "config/dependencies/models.json").read_text())
    cache = root / ".cache/huggingface/hub"
    checks: list[Check] = []
    for name, model in manifest.items():
        repository = model["repository"].replace("/", "--")
        snapshot = cache / f"models--{repository}" / "snapshots" / model["revision"]
        failures = []
        for filename, expected in model["required_files"].items():
            path = snapshot / filename
            if not path.is_file():
                failures.append(f"missing {filename}")
            elif sha256_file(path) != expected:
                failures.append(f"checksum mismatch {filename}")
        checks.append(
            Check(
                f"model:{name}",
                "FAIL" if failures else "PASS",
                "; ".join(failures) if failures else f"revision={model['revision']}",
            )
        )
    return checks


def verify_data(root: Path) -> Check:
    """Validate required GIFT-Eval data files against committed SHA-256 identities."""
    manifest = json.loads((root / "config/dependencies/gift_eval.json").read_text())
    dataset = manifest["dataset"]
    source = root / dataset["default_local_source_directory"] / dataset["phase_0_1_subset"]
    failures = []
    for filename, expected in dataset["phase_0_1_sha256"].items():
        path = source / filename
        if not path.is_file():
            failures.append(f"missing {filename}")
        elif sha256_file(path) != expected:
            failures.append(f"checksum mismatch {filename}")
    return Check(
        "data:m4_daily",
        "FAIL" if failures else "PASS",
        "; ".join(failures) if failures else f"revision={dataset['revision']}",
    )


def verify_r(root: Path) -> Check:
    """Run the read-only R lock/package/M4 object validator."""
    completed = run_read_only(
        ["Rscript", "--vanilla", "scripts/setup_r.R", "verify"], root
    )
    if completed.returncode:
        version = completed.stdout.strip().splitlines()[-1:] or ["R version unavailable"]
        return Check("R environment", "FAIL", f"{version[0]}; {failure_detail(completed)}")
    return Check("R environment", "PASS", completed.stdout.strip().splitlines()[-1])


def detect_nvidia_driver(root: Path) -> tuple[bool, str]:
    """Diagnose, but never install or change, the system NVIDIA driver."""
    executable = shutil.which("nvidia-smi")
    if not executable:
        return False, "not detected"
    completed = run_read_only(
        [executable, "--query-gpu=name,driver_version", "--format=csv,noheader"], root
    )
    if completed.returncode:
        return False, "nvidia-smi present but unavailable"
    return True, completed.stdout.strip().replace("\n", "; ")


def verify_installation(root: Path) -> tuple[list[Check], dict]:
    """Perform the complete read-only installation audit and collect report metadata."""
    root = require_repository_root(root)
    checks: list[Check] = []
    required_directories = (
        "data",
        "models",
        "results",
        ".cache",
        "environments/gift-eval",
        "environments/chronos-2",
        "environments/mantis",
        "environments/classifiers",
        "environments/tensorflow",
    )
    missing = [item for item in required_directories if not (root / item).is_dir()]
    checks.append(
        Check(
            "repository structure",
            "FAIL" if missing else "PASS",
            f"missing: {', '.join(missing)}" if missing else "required directories present",
        )
    )
    checks.append(verify_submodule(root))
    checks.append(verify_r(root))

    uv = root / ".tools/uv/uv"
    if not uv.is_file():
        checks.append(Check("uv", "FAIL", "project-local uv 0.12.18 is missing"))
    else:
        for definition in PYTHON_ENVIRONMENTS.values():
            checks.append(verify_lock_consistency(root, uv, str(definition["project"])))

    environment_details: dict[str, dict] = {}
    for name, definition in PYTHON_ENVIRONMENTS.items():
        check, details = verify_python_environment(root, name, definition)
        checks.append(check)
        environment_details[name] = details

    checks.extend(verify_models(root))
    checks.append(verify_data(root))
    nvidia_available, nvidia_detail = detect_nvidia_driver(root)
    selected_platform = platform_selection(platform.system(), platform.machine(), nvidia_available)
    if selected_platform == "unsupported":
        checks.append(Check("platform", "FAIL", f"unsupported {platform.system()} {platform.machine()}"))
    else:
        checks.append(Check("platform", "PASS", selected_platform))
    checks.append(Check("NVIDIA driver", "PASS" if nvidia_available else "WARN", nvidia_detail, False))

    accelerators = []
    for environment in ("chronos-2", "mantis"):
        details = environment_details.get(environment, {})
        if details:
            accelerators.append(f"{environment}:{details.get('selected_device', 'checked by torch')}")
    tensorflow = environment_details.get("tensorflow-classifiers", {})
    if tensorflow:
        accelerators.append(f"tensorflow:{tensorflow.get('selected_device', 'unknown')}")
    selected_devices = [
        details.get("selected_device")
        for details in environment_details.values()
        if "selected_device" in details
    ]
    if selected_devices and all(device in {"cpu", "/CPU:0"} for device in selected_devices):
        checks.append(
            Check(
                "accelerator workloads",
                "WARN",
                "no CUDA or Apple accelerator is visible; compatible workloads use CPU fallback",
                False,
            )
        )
    uv_version = "unavailable"
    if uv.is_file():
        completed = run_read_only([str(uv), "--version"], root)
        if completed.returncode == 0:
            uv_version = completed.stdout.strip()
    metadata = {
        "platform": platform.platform(),
        "architecture": platform.machine(),
        "python": platform.python_version(),
        "uv": uv_version,
        "nvidia": nvidia_detail,
        "accelerator": ", ".join(accelerators) or "unavailable; CPU fallback",
        "environments": environment_details,
    }
    return checks, metadata


def print_report(checks: Sequence[Check], metadata: dict) -> None:
    """Print a readable, non-sensitive installation report."""
    print("ShapeFM installation report")
    print(f"Date (UTC): {datetime.now(timezone.utc).isoformat()}")
    print(f"Platform: {metadata['platform']}")
    print(f"CPU architecture: {metadata['architecture']}")
    print(f"Setup Python: {metadata['python']}")
    print(f"uv: {metadata['uv']}")
    print(f"Detected accelerator: {metadata['accelerator']}")
    print(f"NVIDIA driver: {metadata['nvidia']}")
    print()
    for check in checks:
        print(f"[{check.status}] {check.name}: {check.detail}")
    failures = sum(check.status == "FAIL" and check.hard_failure for check in checks)
    warnings = sum(check.status == "WARN" for check in checks)
    print()
    print(f"Summary: {failures} failure(s), {warnings} warning(s)")


def build_parser() -> argparse.ArgumentParser:
    """Define support-only archive, download, and verification commands."""
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    archive = subparsers.add_parser("archive")
    archive.add_argument("--root", type=Path, required=True)
    archive.add_argument("--date", required=True)
    archive.add_argument("--execute", action="store_true")
    download = subparsers.add_parser("download")
    download.add_argument("--url", required=True)
    download.add_argument("--destination", type=Path, required=True)
    download.add_argument("--attempts", type=int, default=3)
    download.add_argument("--timeout", type=int, default=7200)
    download.add_argument("--delay", type=float, default=5.0)
    download.add_argument("--contains")
    model = subparsers.add_parser("acquire-model")
    model.add_argument("--root", type=Path, required=True)
    model.add_argument("--manifest", type=Path, required=True)
    model.add_argument("--model", required=True)
    verify = subparsers.add_parser("verify")
    verify.add_argument("--root", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Dispatch one setup-support operation with clear nonzero failures."""
    args = build_parser().parse_args(argv)
    try:
        if args.command == "archive":
            plan = build_archive_plan(args.root, args.date)
            print("Complete archive plan:")
            if not plan:
                print("  (no existing project-managed folders)")
            for move in plan:
                print(f"  {move.source} -> {move.destination}")
            if args.execute:
                execute_archive_plan(args.root, plan)
                print(f"Archived {len(plan)} folder(s).")
        elif args.command == "download":
            root = require_repository_root(Path.cwd())
            destination = require_inside_repository(root, args.destination)
            outcome = download_with_retry(
                args.url,
                destination,
                attempts=args.attempts,
                timeout=args.timeout,
                delay=args.delay,
                validator=file_contains(args.contains) if args.contains else nonempty_file,
            )
            print(f"Download {outcome}: {destination}")
        elif args.command == "acquire-model":
            snapshot = acquire_model(args.root, args.manifest, args.model)
            print(f"Validated model snapshot: {snapshot}")
        else:
            checks, metadata = verify_installation(args.root)
            print_report(checks, metadata)
            return 1 if any(check.status == "FAIL" and check.hard_failure for check in checks) else 0
        return 0
    except (OSError, SetupError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
        print(f"setup error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
