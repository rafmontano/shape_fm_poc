# ==============================================================================
# distributed_execution.py
#
# Purpose: Serializable ShapeFM Dask workers and bounded coordinator submission.
# Inputs: Serializable job batches, Dask clients/workers, execution settings, and model identities.
# Outputs: Serializable worker results/provenance and bounded completion iterators; never DuckDB writes.
# Run from: Imported; not run directly.
# ==============================================================================

"""Serializable ShapeFM Dask workers and bounded coordinator submission."""

from __future__ import annotations

import atexit
import fcntl
import hashlib
import json
import os
import platform
import signal
import socket
import subprocess
import threading
import time
import uuid
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager, nullcontext
from pathlib import Path
from typing import Any

import dask
import distributed
from distributed import Client, Future, as_completed, get_worker

from .configuration import json_fingerprint
from .forecast_combination import combine_equal_weight
from .transformations import transform


# Code constant: pinned Dask protocol release required on coordinator and workers.
EXPECTED_DASK_VERSION = "2026.8.0"
# Code constant: repository root derived from this source path; it has no override.
ROOT = Path(__file__).resolve().parents[3]
# Code constant: Dask protocol label for one logical Chronos GPU slot.
CHRONOS_GPU_RESOURCE = "CHRONOS_GPU_SLOT"
# Code constant: one resource token admits a memory-intensive seasonal R task.
TUNING_R_RESOURCE = "TUNING_R_SLOT"
# Code constant: large AutoARIMA work is admitted only on Ubuntu workers.
AUTOARIMA_R_RESOURCE = "AUTOARIMA_R_SLOT"
# Code constants: deterministic host-placement resources for recovery evidence.
MAC_TUNING_R_RESOURCE = "MAC_TUNING_R_SLOT"
UBUNTU_TUNING_R_RESOURCE = "UBUNTU_TUNING_R_SLOT"
# Machine-local coordination directory for cross-worker R-memory reservations.
TUNING_RESERVATION_DIRECTORY = Path("/tmp/shapefm-r-tuning-reservations")
# One physical GPU has one machine-wide startup gate. It protects model loading,
# not inference, and therefore does not turn logical GPU slots into a serial queue.
CHRONOS_GPU_STARTUP_LOCK = Path("/tmp/shapefm-chronos-gpu-startup.lock")


def package_version_probe(package: str) -> str:
    """Return one installed package version through a serializable worker probe."""
    import importlib.metadata

    return importlib.metadata.version(package)


def _worker_provenance(
    retry_count: int = 0, worker: Any = None
) -> dict[str, Any]:
    """Purpose: Describe one Dask execution attempt. Inputs: ``retry_count`` is the zero-based coordinator retry count; ``worker`` is a Dask Worker-like object, defaulting to the current worker. Outputs: A serializable mapping of backend, host, worker identity, total resource capacities, and retry count; samples current worker state without mutating it."""
    worker = worker or get_worker()
    state = getattr(worker, "state", None)
    resources = dict(getattr(state, "total_resources", {}) or {})
    return {
        "execution_backend": "Dask Distributed",
        "hostname": socket.gethostname(),
        "dask_worker": worker.address,
        "dask_worker_name": str(worker.name),
        "resources": resources,
        "retry_count": retry_count,
    }


def worker_resource_snapshot(dask_worker: Any = None) -> dict[str, Any]:
    """Purpose: Sample resource telemetry on a Dask worker. Inputs: ``dask_worker`` is a Dask Worker-like object or ``None`` for the current worker. Outputs: A mapping with CPU percent and memory/spill counters in bytes, plus NVIDIA utilization percent, byte capacities, and name for GPU-slot workers; invokes ``nvidia-smi`` and samples process-global psutil state."""
    import psutil

    worker = dask_worker or get_worker()
    state = getattr(worker, "state", None)
    resources = dict(getattr(state, "total_resources", {}) or {})
    memory = psutil.virtual_memory()
    swap = psutil.swap_memory()
    spilled = getattr(getattr(worker, "data", None), "spilled_total", None)
    snapshot: dict[str, Any] = {
        "hostname": socket.gethostname(),
        "worker": worker.address,
        "resources": resources,
        "cpu_percent": psutil.cpu_percent(interval=None),
        "system_available_memory_bytes": int(memory.available),
        "swap_used_bytes": int(swap.used),
        "dask_spilled_memory_bytes": int(getattr(spilled, "memory", 0)),
        "dask_spilled_disk_bytes": int(getattr(spilled, "disk", 0)),
    }
    if resources.get(CHRONOS_GPU_RESOURCE, 0) >= 1:
        query = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=utilization.gpu,memory.free,memory.total,name",
                "--format=csv,noheader,nounits",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout.strip().split(", ")
        snapshot["gpu"] = {
            "utilization_percent": float(query[0]),
            "available_memory_bytes": int(float(query[1]) * 1024 * 1024),
            "total_memory_bytes": int(float(query[2]) * 1024 * 1024),
            "name": query[3],
        }
    return snapshot


def _run_r(
    payload: dict[str, Any],
    script: str,
    timeout: float,
    threads: int,
    memory_monitor: "TuningMemoryMonitor | None" = None,
) -> dict[str, Any]:
    """Execute a bounded R bridge, optionally under continuous memory safety.

    The monitor owns only this process group. A sustained host-floor or swap
    breach terminates that group and raises a retryable infrastructure error;
    it is never converted into a scientific model fallback.
    """
    command = ["Rscript", str(ROOT / script)]
    environment = {
        **os.environ,
        "OMP_NUM_THREADS": str(threads),
        "OPENBLAS_NUM_THREADS": str(threads),
        "RENV_CONFIG_SYNCHRONIZED_CHECK": "false",
    }
    if memory_monitor is None:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            input=json.dumps(payload),
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=environment,
        )
        return json.loads(completed.stdout)
    memory_monitor.raise_if_unsafe()
    process = subprocess.Popen(
        command,
        cwd=ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=environment,
        start_new_session=True,
    )
    memory_monitor.register_process(process)
    try:
        stdout, stderr = process.communicate(input=json.dumps(payload), timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=2)
        raise
    finally:
        memory_monitor.clear_process(process)
    memory_monitor.raise_if_unsafe()
    if process.returncode:
        raise subprocess.CalledProcessError(
            process.returncode, command, output=stdout, stderr=stderr
        )
    return json.loads(stdout)


def repository_source_manifest() -> dict[str, str]:
    """Return SHA-256 identities for every nonignored repository source file.

    The manifest deliberately includes tracked and relevant untracked files so a
    reviewed dirty Mac tree can be synchronized and verified without a temporary
    commit. Git-ignored environments, databases, results, caches, and secrets are
    excluded by ``git ls-files --exclude-standard``.
    """
    listed = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    ).stdout.split(b"\0")
    runtime_prefixes = ("src/python/", "src/r/", "config/", "scripts/", "environments/")
    runtime_files = {
        "config/execution_profiles.json",
        "config/experiments/poc2_m4_daily_100_period_tuning.json",
        "config/experiments/poc2_m4_daily_100_rolling_windows.json",
        "config/experiments/poc2_m4_daily_100_rolling_windows_corrected.json",
        "pyproject.toml",
        "uv.lock",
        "renv.lock",
    }
    manifest: dict[str, str] = {}
    for raw_path in listed:
        if not raw_path:
            continue
        relative = raw_path.decode("utf-8")
        if relative not in runtime_files and not relative.startswith(runtime_prefixes):
            continue
        path = ROOT / relative
        if path.is_file():
            manifest[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return manifest


def source_manifest_fingerprint(manifest: dict[str, str]) -> str:
    """Return one deterministic digest for a path-to-content-digest manifest."""
    return hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def tuning_worker_preflight(
    expected_manifest: dict[str, str], dask_worker: Any = None
) -> dict[str, Any]:
    """Probe only dependencies used by AutoARIMA/ETS seasonal tuning.

    Unlike the general Gate 4 preflight, this check does not inspect Chronos,
    model caches, CUDA, GIFT-Eval, or DuckDB. It validates the exact synchronized
    source files plus pinned Python/Dask and R packages loaded by CPU workers.
    """
    local_manifest = {
        relative: hashlib.sha256((ROOT / relative).read_bytes()).hexdigest()
        if (ROOT / relative).is_file()
        else "missing"
        for relative in expected_manifest
    }
    r_packages = json.loads(
        _command(
            "Rscript",
            "-e",
            'cat(jsonlite::toJSON(list(R=as.character(getRversion()), '
            'forecast=as.character(packageVersion("forecast")), '
            'jsonlite=as.character(packageVersion("jsonlite")), '
            'tsfeatures=as.character(packageVersion("tsfeatures"))), auto_unbox=TRUE))',
        ).splitlines()[-1]
    )
    return {
        **_worker_provenance(worker=dask_worker),
        "source_manifest": source_manifest_fingerprint(local_manifest),
        "source_mismatches": sorted(
            relative
            for relative, digest in expected_manifest.items()
            if local_manifest.get(relative) != digest
        ),
        "python_version": platform.python_version(),
        "dask_version": dask.__version__,
        "distributed_version": distributed.__version__,
        "r_packages": r_packages,
        "memory": worker_resource_snapshot(dask_worker),
    }


def validate_tuning_cluster(
    client: Client,
    *,
    expected_workers: int,
    expected_mac_workers: int,
    expected_ubuntu_workers: int,
    expected_tuning_workers: int,
    timeout: float,
    expected_manifest: dict[str, str],
) -> dict[str, dict[str, Any]]:
    """Require the approved CPU topology, exact source, and tuning dependencies."""
    client.wait_for_workers(expected_workers, timeout=timeout)
    reports = client.run(tuning_worker_preflight, expected_manifest)
    failures: list[str] = []
    local_hostname = socket.gethostname()
    mac_workers = sum(report["hostname"] == local_hostname for report in reports.values())
    ubuntu_workers = len(reports) - mac_workers
    tuning_workers = sum(
        report["resources"].get(TUNING_R_RESOURCE, 0) == 1
        for report in reports.values()
    )
    expected_topology = (
        expected_workers,
        expected_mac_workers,
        expected_ubuntu_workers,
        expected_tuning_workers,
    )
    actual_topology = (len(reports), mac_workers, ubuntu_workers, tuning_workers)
    if actual_topology != expected_topology:
        failures.append(
            "worker topology "
            f"total/mac/ubuntu/tuning={actual_topology}, expected {expected_topology}"
        )
    expected_source = source_manifest_fingerprint(expected_manifest)
    expected_r = {
        "R": "4.6.1",
        "forecast": "8.24.0",
        "jsonlite": "2.0.0",
        "tsfeatures": "1.1.1",
    }
    for address, report in reports.items():
        if report["source_manifest"] != expected_source or report["source_mismatches"]:
            failures.append(
                f"{address}: stale source files {report['source_mismatches'][:10]}"
            )
        for field, value in {
            "python_version": "3.12.14",
            "dask_version": EXPECTED_DASK_VERSION,
            "distributed_version": EXPECTED_DASK_VERSION,
        }.items():
            if report.get(field) != value:
                failures.append(
                    f"{address}: {field}={report.get(field)!r}, expected {value!r}"
                )
        for package, version in expected_r.items():
            if report["r_packages"].get(package) != version:
                failures.append(
                    f"{address}: R {package}={report['r_packages'].get(package)!r}, expected {version!r}"
                )
        if report["resources"].get("CPU", 0) != 1:
            failures.append(f"{address}: worker does not advertise exactly one CPU")
        is_mac = report["hostname"] == local_hostname
        if report["resources"].get(TUNING_R_RESOURCE, 0) != 1:
            failures.append(f"{address}: worker is not eligible for tuning work")
        autoarima_slots = report["resources"].get(AUTOARIMA_R_RESOURCE, 0)
        if is_mac and autoarima_slots:
            failures.append(f"{address}: Mac worker incorrectly advertises AutoARIMA capacity")
        if not is_mac and autoarima_slots != 1:
            failures.append(f"{address}: Ubuntu worker lacks AutoARIMA capacity")
    if failures:
        raise RuntimeError("seasonal tuning Dask preflight failed:\n" + "\n".join(failures))
    return reports


@contextmanager
def tuning_memory_reservation(
    minimum_available_gib: float,
    fit_budget_gib: float,
    timeout_seconds: float = 600.0,
    poll_interval_seconds: float = 0.5,
    breach_grace_seconds: float = 5.0,
    swap_growth_limit_gib: float = 0.25,
) -> Iterator["TuningMemoryMonitor"]:
    """Reserve host capacity before launching an R child process.

    A filesystem lock serializes admission across separate Dask worker processes.
    Live reservations are charged against currently available memory, preventing
    simultaneous workers from all admitting against the same pre-launch sample.
    """
    import psutil

    TUNING_RESERVATION_DIRECTORY.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock_path = TUNING_RESERVATION_DIRECTORY / "admission.lock"
    reservation_path = TUNING_RESERVATION_DIRECTORY / f"{os.getpid()}-{uuid.uuid4().hex}.json"
    deadline = time.monotonic() + timeout_seconds
    waited = 0.0
    while True:
        with lock_path.open("a+", encoding="utf-8") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            reserved = 0.0
            for path in TUNING_RESERVATION_DIRECTORY.glob("*.json"):
                try:
                    record = json.loads(path.read_text(encoding="utf-8"))
                    os.kill(int(record["pid"]), 0)
                    reserved += float(record["fit_budget_gib"])
                except (OSError, ValueError, KeyError, json.JSONDecodeError):
                    path.unlink(missing_ok=True)
            available = psutil.virtual_memory().available / 1024**3
            if available - reserved >= minimum_available_gib + fit_budget_gib:
                reservation_path.write_text(
                    json.dumps(
                        {
                            "pid": os.getpid(),
                            "fit_budget_gib": fit_budget_gib,
                            "hostname": socket.gethostname(),
                        }
                    ),
                    encoding="utf-8",
                )
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
                break
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        if time.monotonic() >= deadline:
            raise ResourceSafetyInterruption(
                "R tuning memory admission timed out: "
                f"{available:.2f} GiB available, {reserved:.2f} GiB reserved, "
                f"{minimum_available_gib:.2f} GiB floor and {fit_budget_gib:.2f} GiB fit budget"
            )
        time.sleep(poll_interval_seconds)
        waited += poll_interval_seconds
    monitor = TuningMemoryMonitor(
        minimum_available_gib=minimum_available_gib,
        poll_interval_seconds=poll_interval_seconds,
        breach_grace_seconds=breach_grace_seconds,
        swap_growth_limit_gib=swap_growth_limit_gib,
        admission={
            "hostname": socket.gethostname(),
            "available_gib_at_admission": available,
            "reserved_gib_before_admission": reserved,
            "fit_budget_gib": fit_budget_gib,
            "minimum_available_gib": minimum_available_gib,
            "throttled_seconds": waited,
        },
    )
    monitor.start()
    try:
        yield monitor
    except BaseException:
        raise
    else:
        # Close the sampling race at context exit. A final host sample records
        # pressure that began after the last child response; only an already
        # sustained breach raises here, preserving the configured grace period.
        monitor.sample_once()
        monitor.raise_if_unsafe()
    finally:
        monitor.stop()
        reservation_path.unlink(missing_ok=True)


class ResourceSafetyInterruption(RuntimeError):
    """Identify a retryable infrastructure interruption caused by host pressure."""


class TuningMemoryMonitor:
    """Continuously protect a host floor and only terminate its owned R process.

    Inputs are profile-owned GiB thresholds and timing controls. The monitor
    records host/child extrema for provenance and never inspects or kills an
    unrelated process.
    """

    def __init__(
        self,
        *,
        minimum_available_gib: float,
        poll_interval_seconds: float,
        breach_grace_seconds: float,
        swap_growth_limit_gib: float,
        admission: dict[str, Any] | None = None,
        probe: Callable[[], dict[str, float]] | None = None,
        minimum_accelerator_available_gib: float | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Store thresholds and injectable probes without starting monitoring."""
        self.minimum_available_gib = minimum_available_gib
        self.poll_interval_seconds = poll_interval_seconds
        self.breach_grace_seconds = breach_grace_seconds
        self.swap_growth_limit_gib = swap_growth_limit_gib
        self.admission = dict(admission or {})
        self.minimum_accelerator_available_gib = minimum_accelerator_available_gib
        self._probe = probe or self._host_probe
        self._clock = clock
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._process: subprocess.Popen[str] | None = None
        initial = self._probe()
        self.initial_swap_used_gib = initial["swap_used_gib"]
        self.min_available_gib = initial["available_gib"]
        self.current_available_gib = initial["available_gib"]
        self.max_swap_used_gib = initial["swap_used_gib"]
        self.min_accelerator_available_gib = initial.get("accelerator_available_gib")
        self.current_accelerator_available_gib = initial.get("accelerator_available_gib")
        self.peak_owned_process_tree_rss_gib = 0.0
        self._breach_started: float | None = None
        self._unsafe_reason: str | None = None
        self.safety_responses = 0

    @staticmethod
    def _host_probe() -> dict[str, float]:
        """Sample current host available RAM and used swap in GiB."""
        import psutil

        return {
            "available_gib": psutil.virtual_memory().available / 1024**3,
            "swap_used_gib": psutil.swap_memory().used / 1024**3,
        }

    def register_process(self, process: subprocess.Popen[str]) -> None:
        """Register the one R process group this monitor may terminate."""
        with self._lock:
            if self._process is not None:
                raise RuntimeError("memory monitor already owns an R process")
            self._process = process
            unsafe = self._unsafe_reason is not None
        if unsafe:
            self._cancel_owned(process)

    def clear_process(self, process: subprocess.Popen[str]) -> None:
        """Forget a completed R process without affecting another process."""
        with self._lock:
            if self._process is process:
                self._process = None

    def _owned_rss_gib(self, process: subprocess.Popen[str] | None) -> float:
        """Return RSS for the registered R process tree, tolerating process exit."""
        if process is None:
            return 0.0
        import psutil

        try:
            root = psutil.Process(process.pid)
            return sum(
                item.memory_info().rss for item in [root, *root.children(recursive=True)]
            ) / 1024**3
        except (psutil.Error, OSError):
            return 0.0

    def sample_once(self) -> None:
        """Sample safety state and trigger bounded owned-process cancellation."""
        now = self._clock()
        try:
            snapshot = self._probe()
        except BaseException as error:
            self._mark_unsafe(f"resource probe failed closed: {error}")
            return
        with self._lock:
            process = self._process
        available = snapshot["available_gib"]
        swap_used = snapshot["swap_used_gib"]
        self.current_available_gib = available
        self.min_available_gib = min(self.min_available_gib, available)
        self.max_swap_used_gib = max(self.max_swap_used_gib, swap_used)
        accelerator_available = snapshot.get("accelerator_available_gib")
        self.current_accelerator_available_gib = accelerator_available
        if accelerator_available is not None:
            self.min_accelerator_available_gib = min(
                self.min_accelerator_available_gib
                if self.min_accelerator_available_gib is not None
                else accelerator_available,
                accelerator_available,
            )
        self.peak_owned_process_tree_rss_gib = max(
            self.peak_owned_process_tree_rss_gib, self._owned_rss_gib(process)
        )
        reasons = []
        if available < self.minimum_available_gib:
            reasons.append(
                f"available memory {available:.2f} GiB below {self.minimum_available_gib:.2f} GiB floor"
            )
        if swap_used - self.initial_swap_used_gib > self.swap_growth_limit_gib:
            reasons.append(
                f"swap grew {swap_used - self.initial_swap_used_gib:.2f} GiB"
            )
        if (
            self.minimum_accelerator_available_gib is not None
            and (accelerator_available is None
                 or accelerator_available < self.minimum_accelerator_available_gib)
        ):
            reasons.append(
                "accelerator free memory unavailable or below "
                f"{self.minimum_accelerator_available_gib:.2f} GiB floor"
            )
        if not reasons:
            self._breach_started = None
            return
        if self._breach_started is None:
            self._breach_started = now
            return
        if now - self._breach_started < self.breach_grace_seconds or self._unsafe_reason:
            return
        self._unsafe_reason = "; ".join(reasons)
        self.safety_responses += 1
        self._cancel_owned(process)

    def _mark_unsafe(self, reason: str) -> None:
        """Fail closed immediately when a safety probe itself is unavailable."""
        with self._lock:
            process = self._process
        if self._unsafe_reason is None:
            self._unsafe_reason = reason
            self.safety_responses += 1
        self._cancel_owned(process)

    @staticmethod
    def _cancel_owned(process: subprocess.Popen[str] | None) -> None:
        """Boundedly terminate only the registered child process group."""
        if process is not None and process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=2)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGKILL)

    def _run(self) -> None:
        """Poll until stopped, retaining the first sustained unsafe condition."""
        while not self._stop.wait(self.poll_interval_seconds):
            self.sample_once()

    def start(self) -> None:
        """Start one daemon monitor thread; repeated starts are rejected."""
        if self._thread is not None:
            raise RuntimeError("memory monitor is already started")
        self._thread = threading.Thread(
            target=self._run, name="shapefm-r-memory-monitor", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop and join the monitor without raising over an active exception."""
        self._stop.set()
        if self._thread is not None:
            # NVIDIA probes take at most ten seconds plus bounded child shutdown.
            self._thread.join()
            self._thread = None

    def raise_if_unsafe(self) -> None:
        """Raise a retryable resource interruption after a sustained breach."""
        if self._unsafe_reason:
            raise ResourceSafetyInterruption(
                "resource safety interruption: " + self._unsafe_reason
            )

    def raise_if_current_pressure(self) -> None:
        """Reject startup on one fresh below-floor sample, independent of grace."""
        if self.current_available_gib < self.minimum_available_gib or (
            self.minimum_accelerator_available_gib is not None
            and (self.current_accelerator_available_gib is None
                 or self.current_accelerator_available_gib
                 < self.minimum_accelerator_available_gib)
        ):
            raise ResourceSafetyInterruption("resource pressure blocks model startup")

    def evidence(self) -> dict[str, Any]:
        """Return admission, ongoing telemetry extrema, and safety-response count."""
        return {
            **self.admission,
            "memory_poll_interval_seconds": self.poll_interval_seconds,
            "memory_breach_grace_seconds": self.breach_grace_seconds,
            "swap_growth_limit_gib": self.swap_growth_limit_gib,
            "minimum_available_gib_during_fit": self.min_available_gib,
            "initial_swap_used_gib": self.initial_swap_used_gib,
            "maximum_swap_used_gib": self.max_swap_used_gib,
            "peak_owned_process_tree_rss_gib": self.peak_owned_process_tree_rss_gib,
            "safety_responses": self.safety_responses,
            "unsafe_reason": self._unsafe_reason,
            "minimum_accelerator_available_gib": self.minimum_accelerator_available_gib,
            "minimum_accelerator_available_gib_during_work": self.min_accelerator_available_gib,
        }


def _gpu_host_probe() -> dict[str, float]:
    """Sample host pressure and the sole physical NVIDIA GPU with a bounded probe."""
    snapshot = TuningMemoryMonitor._host_probe()
    output = subprocess.run(
        ["nvidia-smi", "--query-gpu=memory.free", "--format=csv,noheader,nounits", "-i", "0"],
        check=True, capture_output=True, text=True, timeout=10,
    ).stdout.strip().splitlines()
    if len(output) != 1:
        raise RuntimeError("expected exactly one physical GPU sample")
    snapshot["accelerator_available_gib"] = float(output[0]) / 1024
    import math
    if any(not math.isfinite(value) or value < 0 for value in snapshot.values()):
        raise RuntimeError("invalid GPU/host memory sample")
    return snapshot


@contextmanager
def gpu_startup_admission(monitor: TuningMemoryMonitor, timeout_seconds: float,
                          poll_interval_seconds: float) -> Iterator[dict[str, Any]]:
    """Pressure-gate and serialize model startup, releasing immediately after ready."""
    deadline = time.monotonic() + timeout_seconds
    CHRONOS_GPU_STARTUP_LOCK.parent.mkdir(parents=True, exist_ok=True)
    with CHRONOS_GPU_STARTUP_LOCK.open("a+") as lock:
        while True:
            monitor.sample_once()
            monitor.raise_if_unsafe()
            try:
                monitor.raise_if_current_pressure()
            except ResourceSafetyInterruption:
                if time.monotonic() >= deadline:
                    raise ResourceSafetyInterruption(
                        "Chronos GPU startup pressure admission timed out"
                    )
                time.sleep(poll_interval_seconds)
                continue
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise ResourceSafetyInterruption("Chronos GPU startup admission timed out")
                time.sleep(poll_interval_seconds)
                continue
            try:
                # Recheck under the shared gate: another startup may have used
                # memory after the pre-lock sample and before lock acquisition.
                monitor.sample_once()
                monitor.raise_if_unsafe()
                monitor.raise_if_current_pressure()
            except BaseException:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
                raise
            monitor.admission["startup_throttled_seconds"] = (
                time.monotonic() - (deadline - timeout_seconds)
            )
            break
        try:
            yield monitor.admission
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def clean_batch(
    batch: list[dict[str, Any]],
    script: str,
    timeout: float,
    threads: int,
    retry_count: int = 0,
) -> dict[str, Any]:
    """Purpose: Preprocess a serializable batch through the bounded R worker.

    Inputs: Jobs with IDs, contexts, approved modes, and official seasonality plus
    worker execution controls. Outputs: Finite model inputs, preprocessing provenance,
    package versions, runtime, and worker identity; no database access occurs here.
    """
    started = time.monotonic()
    response = _run_r(
        {
            "action": "preprocess",
            "jobs": [
                {
                    key: value
                    for key, value in job.items()
                    if key not in {"instance_id", "official_frequency"}
                }
                for job in batch
            ],
        },
        script,
        timeout,
        threads,
    )
    runtime = time.monotonic() - started
    return {
        "results": response["results"],
        "packages": response["packages"],
        "runtime_seconds": runtime,
        "worker": _worker_provenance(retry_count),
    }


def transform_batch(batch: list[dict[str, Any]], retry_count: int = 0) -> dict[str, Any]:
    """Purpose: Apply configured transformations to one worker batch. Inputs: ``batch`` is coordinator-originated job mappings containing ID, numeric series values, and an allowed transformation method; ``retry_count`` is the zero-based Dask attempt. Outputs: Serializable transformed one-dimensional values and parameters per ID, elapsed seconds, and sampled worker provenance."""
    started = time.monotonic()
    results = []
    for job in batch:
        result = transform(job["values"], job["method"])
        results.append(
            {
                "id": job["id"],
                "values": list(result.values),
                "parameters": result.parameters,
            }
        )
    return {
        "results": results,
        "runtime_seconds": time.monotonic() - started,
        "worker": _worker_provenance(retry_count),
    }


def window_preparation_batch(
    batch: list[dict[str, Any]],
    script: str,
    timeout: float,
    threads: int,
    memory_safety: dict[str, Any] | None = None,
    retry_count: int = 0,
) -> dict[str, Any]:
    """Clean and standardise complete windows without reading either DuckDB file.

    Each outer job represents one source series and contains only bounded input
    windows. Future observations are represented by positions and never enter this
    worker. The R boundary applies the configured Gate 2 mode to each input, then
    Python fits ``standardise_sample_v1`` independently to the cleaned input.
    """
    started = time.monotonic()
    r_jobs = []
    metadata: dict[str, dict[str, Any]] = {}
    for series_job in batch:
        for window in series_job["windows"]:
            window_id = window["window_id"]
            metadata[window_id] = window
            r_jobs.append(
                {
                    "id": window_id,
                    "context": window["input"],
                    "mode": series_job["preprocessing_mode"],
                    "seasonality": series_job["seasonality"],
                }
            )
    monitor_context = nullcontext(None)
    if memory_safety is not None:
        is_mac = socket.gethostname() == memory_safety["mac_hostname"]
        monitor_context = tuning_memory_reservation(
            minimum_available_gib=float(
                memory_safety["mac_minimum_available_gib"]
                if is_mac
                else memory_safety["ubuntu_minimum_available_gib"]
            ),
            fit_budget_gib=float(memory_safety["fit_budget_gib"]),
            timeout_seconds=float(memory_safety["admission_timeout_seconds"]),
            poll_interval_seconds=float(memory_safety["poll_interval_seconds"]),
            breach_grace_seconds=float(memory_safety["breach_grace_seconds"]),
            swap_growth_limit_gib=float(memory_safety["swap_growth_limit_gib"]),
        )
    with monitor_context as memory_monitor:
        response = _run_r(
            {"action": "preprocess", "jobs": r_jobs},
            script,
            timeout,
            threads,
            memory_monitor=memory_monitor,
        )
    cleaned = {item["id"]: item for item in response["results"]}
    if set(cleaned) != set(metadata) or len(cleaned) != len(response["results"]):
        raise RuntimeError("window preprocessing returned mismatched window identities")
    by_series: dict[str, list[dict[str, Any]]] = {
        job["id"]: [] for job in batch
    }
    for series_job in batch:
        for window in series_job["windows"]:
            prepared = cleaned[window["window_id"]]
            transformed = transform(
                prepared["values"], series_job["transformation"]
            )
            by_series[series_job["id"]].append(
                {
                    **{key: value for key, value in window.items() if key != "input"},
                    "input_hash": json_fingerprint(window["input"]),
                    "cleaned_hash": json_fingerprint(prepared["values"]),
                    "transformed_hash": json_fingerprint(transformed.values),
                    "transformed_input": list(transformed.values),
                    "transformation_state": transformed.parameters,
                    "preprocessing": {
                        key: prepared[key]
                        for key in (
                            "preprocessing_mode",
                            "r_period",
                            "status",
                            "missing_count_before",
                            "missing_count_after",
                            "values_changed",
                        )
                    },
                }
            )
    try:
        worker = _worker_provenance(retry_count)
    except ValueError:
        # Focused sequential checks deliberately call the same scientific worker
        # contract without constructing a Dask cluster.
        worker = {
            "execution_backend": "local",
            "hostname": socket.gethostname(),
            "retry_count": retry_count,
        }
    if memory_monitor is not None:
        worker["memory_safety"] = memory_monitor.evidence()
    return {
        "results": [
            {
                "id": job["id"],
                # Historical focused callers used the numeric ID directly.
                "series_key": job.get("series_key", job["id"]),
                "windows": by_series[job["id"]],
            }
            for job in batch
        ],
        "packages": response["packages"],
        "runtime_seconds": time.monotonic() - started,
        "worker": worker,
    }


def autoarima_batch(
    batch: list[dict[str, Any]],
    settings: dict[str, Any],
    script: str,
    timeout: float,
    threads: int,
    retry_count: int = 0,
    memory_safety: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Purpose: Forecast one batch with the R AutoARIMA bridge. Inputs: ``batch`` contains coordinator jobs with IDs, numeric contexts, horizons, and seasonality; ``settings`` is resolved AutoARIMA configuration; ``script`` is repository-relative; timeout is seconds, threads is the R limit, and retry count is zero-based. Outputs: Forecast result mappings, elapsed seconds, and worker/package/settings provenance; launches one R subprocess and strips routing-only fields."""
    started = time.monotonic()
    monitor_context = nullcontext(None)
    if memory_safety is not None:
        is_mac = socket.gethostname() == memory_safety["mac_hostname"]
        monitor_context = tuning_memory_reservation(
            minimum_available_gib=float(
                memory_safety["mac_minimum_available_gib"] if is_mac
                else memory_safety["ubuntu_minimum_available_gib"]
            ),
            fit_budget_gib=float(memory_safety["fit_budget_gib"]),
            timeout_seconds=float(memory_safety["admission_timeout_seconds"]),
            poll_interval_seconds=float(memory_safety["poll_interval_seconds"]),
            breach_grace_seconds=float(memory_safety["breach_grace_seconds"]),
            swap_growth_limit_gib=float(memory_safety["swap_growth_limit_gib"]),
        )
    with monitor_context as memory_monitor:
        response = _run_r({
            "action": "forecast",
            "settings": settings,
            "jobs": [
                {
                    key: value
                    for key, value in job.items()
                    if key not in {"model", "instance_id", "variant_id"}
                }
                for job in batch
            ],
        }, script, timeout, threads, memory_monitor=memory_monitor)
    runtime = time.monotonic() - started
    return {
        "results": response["results"],
        "runtime_seconds": runtime,
        "worker": {
            **_worker_provenance(retry_count),
            "packages": response["packages"],
            "settings": settings,
            "batch_task_count": len(batch),
            **({"memory_safety": memory_monitor.evidence()}
               if memory_monitor is not None else {}),
        },
    }


def combine_batch(batch: list[dict[str, Any]], retry_count: int = 0) -> dict[str, Any]:
    """Purpose: Combine paired forecasts for a worker batch. Inputs: ``batch`` contains IDs, left/right forecast mappings, and two combination weights from the coordinator; ``retry_count`` is the zero-based Dask attempt. Outputs: Combined forecasts per ID, elapsed seconds, and sampled worker provenance; performs no external writes."""
    started = time.monotonic()
    results = []
    for job in batch:
        combination = combine_equal_weight(
            job["left"], job["right"], job["weights"]
        )
        results.append(
            {
                "id": job["id"],
                **combination,
            }
        )
    return {
        "results": results,
        "runtime_seconds": time.monotonic() - started,
        "worker": _worker_provenance(retry_count),
    }


# Process-global lock serializes access to one cached Chronos subprocess per Dask worker.
_chronos_lock = threading.Lock()
# Cached worker client and its model/revision/device key; both are replaced together.
_chronos_worker: Any = None
_chronos_key: tuple[str, str, str, str, int, str, str, float, float] | None = None
# Monotonic process-local restart counter reported with forecast provenance.
_chronos_generation = 0


def _close_chronos() -> None:
    """Purpose: Dispose of the process-global Chronos bridge. Inputs: None; uses the module-owned worker, key, and lock. Outputs: None; force-terminates any child process and clears cached process identity state."""
    global _chronos_worker, _chronos_key
    with _chronos_lock:
        if _chronos_worker is not None:
            _chronos_worker.close(force=True)
        _chronos_worker = None
        _chronos_key = None


atexit.register(_close_chronos)


def _get_chronos(
    model: str,
    revision: str,
    device: str,
    dtype: str,
    internal_cpu_threads: int,
    environment: str,
    worker_script: str,
    startup_timeout: float,
    request_timeout: float,
    memory_monitor: TuningMemoryMonitor | None = None,
) -> tuple[Any, int]:
    """Purpose: Acquire the Dask process's keyed persistent Chronos bridge. Inputs: Model/revision/device/dtype strings, positive internal CPU threads, repository-relative environment and worker paths, and startup/request timeouts in seconds. Outputs: The owned ``PersistentChronosWorker`` and monotonic generation number; under a global lock, starts a subprocess or force-replaces one whose full configuration key differs."""
    global _chronos_worker, _chronos_key, _chronos_generation
    from .execution_profiles import PersistentChronosWorker

    key = (
        model,
        revision,
        device,
        dtype,
        internal_cpu_threads,
        environment,
        worker_script,
        startup_timeout,
        request_timeout,
    )
    with _chronos_lock:
        if (_chronos_worker is None or _chronos_key != key
                or (memory_monitor is not None
                    and _chronos_worker.memory_monitor is not memory_monitor)):
            if _chronos_worker is not None:
                _chronos_worker.close(force=True)
            command = [
                str(ROOT / environment / "bin/python"),
                str(ROOT / worker_script),
                "serve",
                "--model",
                model,
                "--revision",
                revision,
                "--device",
                device,
                "--dtype",
                dtype,
                "--internal-cpu-threads",
                str(internal_cpu_threads),
            ]
            _chronos_worker = PersistentChronosWorker(
                command, startup_timeout=startup_timeout, memory_monitor=memory_monitor
            )
            _chronos_worker.start()
            _chronos_key = key
            _chronos_generation += 1
        return _chronos_worker, _chronos_generation


def chronos_batch(
    batch: list[dict[str, Any]],
    model: str,
    revision: str,
    quantile_levels: list[float],
    device: str,
    dtype: str,
    cross_learning: bool,
    predict_batches_jointly: bool,
    internal_cpu_threads: int,
    environment: str,
    worker_script: str,
    startup_timeout: float,
    request_timeout: float,
    retry_count: int = 0,
    accelerator_safety: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Purpose: Forecast one homogeneous batch through the cached Chronos subprocess. Inputs: ``batch`` contains coordinator jobs with IDs, numeric contexts, and a common positive horizon; model identity, quantiles in [0,1], device/dtype and batching flags configure inference; thread count and timeouts are bounded execution controls; retry count is zero-based. Outputs: Forecasts, elapsed seconds, worker/process generation, effective batch size, inference seconds, and memory telemetry; may start/restart or close the cached subprocess on out-of-memory errors."""
    started = time.monotonic()
    monitor = None
    safety_evidence: dict[str, Any] = {}
    if accelerator_safety is not None:
        required = ("minimum_available_gib", "host_minimum_available_gib",
                    "admission_timeout_seconds", "poll_interval_seconds",
                    "breach_grace_seconds", "swap_growth_limit_gib")
        invalid = [key for key in required
                   if not isinstance(accelerator_safety.get(key), (int, float))
                   or accelerator_safety[key] <= 0]
        if invalid:
            raise ResourceSafetyInterruption(
                "Chronos accelerator protection requires positive controls: "
                + ", ".join(invalid)
            )
        try:
            monitor = TuningMemoryMonitor(
                minimum_available_gib=float(accelerator_safety["host_minimum_available_gib"]),
                minimum_accelerator_available_gib=float(accelerator_safety["minimum_available_gib"]),
                poll_interval_seconds=float(accelerator_safety["poll_interval_seconds"]),
                breach_grace_seconds=float(accelerator_safety["breach_grace_seconds"]),
                swap_growth_limit_gib=float(accelerator_safety["swap_growth_limit_gib"]),
                probe=_gpu_host_probe,
            )
        except BaseException as error:
            raise ResourceSafetyInterruption(f"Chronos GPU safety probe failed closed: {error}") from error
        monitor.start()
    pending = deque([(batch, 0)])
    results: list[dict[str, Any]] = []
    subdivisions: list[int] = []
    try:
        while pending:
            current, subdivision = pending.popleft()
            if monitor is not None:
                with gpu_startup_admission(
                    monitor, float(accelerator_safety["admission_timeout_seconds"]),
                    float(accelerator_safety["poll_interval_seconds"]),
                ):
                    worker, generation = _get_chronos(
                        model, revision, device, dtype, internal_cpu_threads, environment,
                        worker_script, startup_timeout, request_timeout, monitor,
                    )
                    # The startup gate is not released until a fresh post-ready sample.
                    monitor.sample_once()
                    monitor.raise_if_unsafe()
                    monitor.raise_if_current_pressure()
            else:
                worker, generation = _get_chronos(
                    model, revision, device, dtype, internal_cpu_threads, environment,
                    worker_script, startup_timeout, request_timeout,
                )
            try:
                response = worker.request({
            "command": "predict",
            "batch_id": f"chronos-batch/{uuid.uuid4().hex}",
            "jobs": [
                {
                    key: value
                    for key, value in job.items()
                    if key
                    not in {"model", "instance_id", "variant_id", "seasonality"}
                }
                for job in current
            ],
            "horizon": current[0]["horizon"],
            "quantile_levels": quantile_levels,
            "inference_batch_size": len(current),
            "cross_learning": cross_learning,
            "predict_batches_jointly": predict_batches_jointly,
                }, timeout=request_timeout)
            except BaseException:
                if monitor is not None:
                    monitor.raise_if_unsafe()
                raise
            if monitor is not None:
                monitor.sample_once()
                monitor.raise_if_unsafe()
            if response.get("type") == "result":
                results.extend(response["results"])
                subdivisions.append(subdivision)
                continue
            error = response.get("error", f"invalid Chronos response: {response}")
            if (response.get("error_kind") != "out_of_memory" or len(current) == 1
                    or cross_learning or predict_batches_jointly):
                if response.get("error_kind") == "out_of_memory":
                    _close_chronos()
                raise RuntimeError(error)
            _close_chronos()
            midpoint = max(1, len(current) // 2)
            splits = [current[index:index + midpoint]
                      for index in range(0, len(current), midpoint)]
            pending.extendleft((split, subdivision + 1) for split in reversed(splits))
        if monitor is not None:
            monitor.sample_once()
            monitor.raise_if_unsafe()
            safety_evidence = monitor.evidence()
    finally:
        if monitor is not None:
            # Protected models never remain resident without their monitor.
            _close_chronos()
            monitor.stop()
    runtime = time.monotonic() - started
    return {
        "results": results,
        "runtime_seconds": runtime,
        "worker": {
            **_worker_provenance(retry_count),
            **{key: value for key, value in worker.ready.items() if key != "type"},
            "worker_generation": generation,
            "effective_batch_size": response["effective_batch_size"],
            "inference_seconds": response["inference_seconds"],
            "peak_process_memory_bytes": response["peak_process_memory_bytes"],
            "accelerator_memory_after": response["accelerator_memory"],
            "oom_subdivision_depth": max(subdivisions, default=0),
            "accelerator_safety": safety_evidence,
        },
    }


def _command(*arguments: str, timeout: float = 30.0) -> str:
    """Purpose: Execute a preflight command from the repository root. Inputs: ``arguments`` are executable and argv strings; ``timeout`` is seconds. Outputs: Stripped stdout text; starts and waits for a subprocess and raises on nonzero status or timeout."""
    return subprocess.run(
        list(arguments),
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    ).stdout.strip()


def worker_preflight(
    configuration_hash: str,
    chronos_repository: str,
    chronos_revision: str,
    chronos_environment: str,
    gift_eval_source_directory: str,
    dask_worker: Any = None,
) -> dict[str, Any]:
    """Purpose: Probe one Dask worker for cluster compatibility. Inputs: Coordinator configuration hash, pinned Chronos repository/revision, repository-relative Python environment and GIFT-Eval checkout paths, plus an optional Dask Worker object. Outputs: Serializable host/resources, Git state, Python/Dask/R/Chronos versions, checkpoint and CUDA evidence; launches Python, R, and Git subprocesses and reads the local model cache."""
    chronos_script = """
import importlib.metadata as metadata, json, os, pathlib, sys, torch
cache = pathlib.Path(os.environ.get('HF_HOME', pathlib.Path.home() / '.cache/huggingface')) / 'hub'
root = cache / ('models--' + sys.argv[1].replace('/', '--'))
revision = sys.argv[2]
print(json.dumps({
    'chronos_forecasting': metadata.version('chronos-forecasting'),
    'torch': torch.__version__,
    'torch_cuda': torch.version.cuda,
    'cuda_available': torch.cuda.is_available(),
    'cuda_name': torch.cuda.get_device_name(0) if torch.cuda.is_available() else None,
    'checkpoint_revision': revision,
    'checkpoint_present': (root / 'snapshots' / revision / 'model.safetensors').exists(),
}))
"""
    chronos = json.loads(
        _command(
            str(ROOT / chronos_environment / "bin/python"),
            "-c",
            chronos_script,
            chronos_repository,
            chronos_revision,
            timeout=60,
        )
    )
    r_packages = json.loads(
        _command(
            "Rscript",
            "-e",
            'cat(jsonlite::toJSON(list(R=as.character(getRversion()), '
            'renv=as.character(packageVersion("renv")), '
            'DBI=as.character(packageVersion("DBI")), '
            'duckdb=as.character(packageVersion("duckdb")), '
            'forecast=as.character(packageVersion("forecast")), '
            'jsonlite=as.character(packageVersion("jsonlite"))), auto_unbox=TRUE))',
        )
    )
    return {
        **_worker_provenance(worker=dask_worker),
        "git_commit": _command("git", "rev-parse", "HEAD"),
        "git_dirty": bool(_command("git", "status", "--porcelain", "--untracked-files=all")),
        "source_manifest": repository_source_manifest(),
        "python_version": platform.python_version(),
        "dask_version": dask.__version__,
        "distributed_version": distributed.__version__,
        "configuration_hash": configuration_hash,
        "gift_eval_revision": _command(
            "git",
            "-C",
            str(ROOT / gift_eval_source_directory),
            "rev-parse",
            "HEAD",
        ),
        "r_packages": r_packages,
        "chronos": chronos,
    }


def validate_cluster(
    client: Client,
    *,
    expected_workers: int,
    timeout: float,
    expected_commit: str,
    expected_configuration_hash: str,
    expected_gift_eval_revision: str,
    expected_chronos_revision: str,
    expected_chronos_version: str,
    chronos_repository: str,
    chronos_environment: str,
    gift_eval_source_directory: str,
    require_gpu: bool,
    expected_gpu_name: str | None,
    expected_gpu_workers: int = 1,
    expected_manifest: dict[str, str] | None = None,
    expected_topology: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    """Purpose: Enforce the coordinator's Dask cluster identity contract. Inputs: A distributed ``Client``; positive expected worker/GPU counts; timeout in seconds; pinned commit, configuration, source, checkpoint and package identities; dependency paths; and GPU requirements/name. Outputs: Reports keyed by worker address when all checks pass; waits for workers, remotely runs hardware/software probes, and raises one aggregated error on mismatch."""
    if expected_gpu_workers < 0 or (require_gpu and expected_gpu_workers < 1):
        raise ValueError("expected_gpu_workers must be nonnegative and positive for GPU work")
    client.wait_for_workers(expected_workers, timeout=timeout)
    reports = client.run(
        worker_preflight,
        expected_configuration_hash,
        chronos_repository,
        expected_chronos_revision,
        chronos_environment,
        gift_eval_source_directory,
    )
    failures = []
    gpu_workers = 0
    for address, report in reports.items():
        expected = {
            "git_commit": expected_commit,
            "python_version": "3.12.14",
            "dask_version": EXPECTED_DASK_VERSION,
            "distributed_version": EXPECTED_DASK_VERSION,
            "configuration_hash": expected_configuration_hash,
            "gift_eval_revision": expected_gift_eval_revision,
        }
        if expected_manifest is None:
            # Historical callers retain their clean-tree contract. The normal
            # researcher route supplies the reviewed, synchronized source bytes.
            expected["git_dirty"] = False
        elif report.get("source_manifest") != expected_manifest:
            observed = report.get("source_manifest", {})
            changed = sorted(path for path in observed.keys() | expected_manifest.keys()
                             if observed.get(path) != expected_manifest.get(path))
            failures.append(f"{address}: source manifest mismatch: {changed}")
        for field, value in expected.items():
            if report.get(field) != value:
                failures.append(
                    f"{address}: {field}={report.get(field)!r}, expected {value!r}"
                )
        r_expected = {
            "R": "4.6.1",
            "renv": "1.2.4",
            "forecast": "8.24.0",
            "jsonlite": "2.0.0",
        }
        for field, value in r_expected.items():
            if report["r_packages"].get(field) != value:
                failures.append(
                    f"{address}: R {field}={report['r_packages'].get(field)!r}, expected {value!r}"
                )
        chronos = report["chronos"]
        if chronos["chronos_forecasting"] != expected_chronos_version:
            failures.append(f"{address}: unexpected Chronos version")
        if chronos["checkpoint_revision"] != expected_chronos_revision or not chronos[
            "checkpoint_present"
        ]:
            failures.append(f"{address}: pinned Chronos checkpoint is unavailable")
        if report["resources"].get(CHRONOS_GPU_RESOURCE, 0) >= 1:
            gpu_workers += 1
            if (not chronos["cuda_available"]
                    or (expected_gpu_name and chronos["cuda_name"] != expected_gpu_name)):
                failures.append(f"{address}: GPU worker lacks the required CUDA device")
    if len(reports) != expected_workers:
        failures.append(f"registered {len(reports)} workers, expected {expected_workers}")
    if expected_topology is not None:
        local = socket.gethostname()
        cpu = [r for r in reports.values() if r["resources"].get("CPU") == 1]
        actual = (sum(r["hostname"] == local for r in cpu),
                  sum(r["hostname"] != local for r in cpu), gpu_workers)
        expected = tuple(expected_topology[key] for key in
                         ("mac_cpu_workers", "ubuntu_cpu_workers", "ubuntu_gpu_workers"))
        if actual != expected or len(cpu) + gpu_workers != len(reports):
            failures.append(f"CPU/GPU topology {actual}, expected {expected}")
        if any(r["hostname"] == local and r["resources"].get(CHRONOS_GPU_RESOURCE)
               for r in reports.values()):
            failures.append("GPU workers must run on Ubuntu, not the coordinator")
        for address, report in reports.items():
            resources = report["resources"]
            expected_autoarima = int(report["hostname"] != local and resources.get("CPU") == 1)
            if resources.get(AUTOARIMA_R_RESOURCE, 0) != expected_autoarima:
                failures.append(f"{address}: incorrect approved AutoARIMA resource routing")
    if require_gpu and gpu_workers != expected_gpu_workers:
        failures.append(
            f"registered {gpu_workers} GPU workers, expected exactly "
            f"{expected_gpu_workers}"
        )
    if failures:
        raise RuntimeError("Dask worker preflight failed:\n" + "\n".join(failures))
    return reports


def _batch_key(batch: list[dict[str, Any]]) -> str:
    """Purpose: Build a deterministic Dask task-key prefix. Inputs: A nonempty ordered batch of job mappings with string ``id`` values. Outputs: A string containing the first ID, batch length, and a 12-hex SHA-256-derived fingerprint; has no side effects."""
    identifiers = [job["id"] for job in batch]
    digest = json_fingerprint(identifiers)[:12]
    return f"{identifiers[0]}/batch-{len(identifiers)}-{digest}"


def _future_error(result: Any) -> BaseException | None:
    """Purpose: Normalize Dask completion failures. Inputs: A completion result of any type, including an exception or three-item ``exc_info`` tuple. Outputs: The represented ``BaseException`` or ``None`` for a successful/nonstandard value; has no side effects."""
    if isinstance(result, BaseException):
        return result
    if (
        isinstance(result, tuple)
        and len(result) == 3
        and isinstance(result[0], type)
        and issubclass(result[0], BaseException)
        and isinstance(result[1], BaseException)
    ):
        return result[1]
    return None


def run_batches(
    client: Client,
    function: Callable[..., dict[str, Any]],
    batches: Iterable[list[dict[str, Any]]],
    *,
    resources: dict[str, float],
    max_in_flight: int,
    retries: int,
    extra_arguments: tuple[Any, ...] = (),
) -> Iterator[tuple[list[dict[str, Any]], dict[str, Any]]]:
    """Purpose: Execute homogeneous Dask batches with bounded concurrency and explicit retries. Inputs: A distributed ``Client``, serializable worker callable, iterable of job batches, Dask resource quantities, positive in-flight limit, nonnegative retry count, and positional worker arguments. Outputs: An iterator of original batch/result pairs in completion order; submits uniquely keyed impure tasks, retries failed batches up to the limit, releases consumed futures, and cancels/releases pending work on exit."""
    pending_batches = iter(batches)
    future_batches: dict[Future, tuple[list[dict[str, Any]], int]] = {}
    completed = as_completed(with_results=True, raise_errors=False)

    def submit_batch(batch: list[dict[str, Any]], retry_count: int) -> None:
        """Purpose: Register one batch attempt with Dask. Inputs: A job-mapping batch and zero-based retry count. Outputs: None; submits one impure future with requested resources and records it in closure-owned completion state."""
        future = client.submit(
            function,
            batch,
            *extra_arguments,
            retry_count,
            key=f"{_batch_key(batch)}/dask-attempt-{retry_count}",
            resources=resources,
            retries=0,
            pure=False,
        )
        future_batches[future] = (batch, retry_count)
        completed.add(future)

    def submit_one() -> bool:
        """Purpose: Advance the pending batch iterator once. Inputs: None; consumes closure-owned input. Outputs: ``True`` after submitting attempt zero or ``False`` when exhausted; mutates pending-future state."""
        try:
            batch = next(pending_batches)
        except StopIteration:
            return False
        submit_batch(batch, 0)
        return True

    for _ in range(max_in_flight):
        if not submit_one():
            break
    try:
        while future_batches:
            future, result = next(completed)
            batch, retry_count = future_batches.pop(future)
            error = _future_error(result)
            if error is not None:
                future.release()
                if retry_count < retries:
                    submit_batch(batch, retry_count + 1)
                    continue
                raise error
            yield batch, result
            future.release()
            submit_one()
    finally:
        for future in future_batches:
            future.cancel()
            future.release()


def run_batch_groups(
    client: Client,
    groups: dict[
        str,
        tuple[
            Callable[..., dict[str, Any]],
            Iterable[list[dict[str, Any]]],
            dict[str, float],
            tuple[Any, ...],
            int,
        ],
    ],
    *,
    retries: int,
) -> Iterator[tuple[str, list[dict[str, Any]], dict[str, Any] | BaseException]]:
    """Purpose: Execute named heterogeneous Dask queues concurrently with per-group bounds. Inputs: A distributed ``Client``; group mappings of worker callable, batch iterable, resource quantities, positional arguments, and positive in-flight limit; plus a nonnegative retry limit. Outputs: Completion-order tuples of group name, original batch, and result or terminal exception; retries each failed attempt, releases futures, and cancels/releases outstanding work on exit."""
    iterators = {name: iter(specification[1]) for name, specification in groups.items()}
    future_batches: dict[Future, tuple[str, list[dict[str, Any]], int]] = {}
    completed = as_completed(with_results=True, raise_errors=False)

    def submit_batch(
        name: str, batch: list[dict[str, Any]], retry_count: int
    ) -> None:
        """Purpose: Register one named-group batch attempt with Dask. Inputs: Existing group name, job-mapping batch, and zero-based retry count. Outputs: None; submits an impure resource-constrained future and updates closure-owned completion state."""
        function, _, resources, arguments, _ = groups[name]
        future = client.submit(
            function,
            batch,
            *arguments,
            retry_count,
            key=f"{_batch_key(batch)}/dask-attempt-{retry_count}",
            resources=resources,
            retries=0,
            pure=False,
        )
        future_batches[future] = (name, batch, retry_count)
        completed.add(future)

    def submit_one(name: str) -> bool:
        """Purpose: Advance one named group's batch iterator. Inputs: An existing group name. Outputs: ``True`` after submitting attempt zero or ``False`` at exhaustion; mutates pending-future state."""
        try:
            batch = next(iterators[name])
        except StopIteration:
            return False
        submit_batch(name, batch, 0)
        return True

    for name, specification in groups.items():
        for _ in range(specification[4]):
            if not submit_one(name):
                break
    try:
        while future_batches:
            future, result = next(completed)
            name, batch, retry_count = future_batches.pop(future)
            error = _future_error(result)
            if error is not None and retry_count < retries:
                future.release()
                submit_batch(name, batch, retry_count + 1)
                continue
            yield name, batch, error or result
            future.release()
            submit_one(name)
    finally:
        for future in future_batches:
            future.cancel()
            future.release()
