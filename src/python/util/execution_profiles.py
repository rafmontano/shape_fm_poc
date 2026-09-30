# ==============================================================================
# execution_profiles.py
#
# Purpose: Hardware-aware local execution profiles and resource provenance.
# Inputs: Named profile JSON, per-run overrides, host metrics, and Chronos bridge requests.
# Outputs: Validated profiles/settings, hardware snapshots, and managed Chronos subprocess responses.
# Run from: Imported; not run directly.
# ==============================================================================

"""Hardware-aware local execution profiles and resource provenance."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import selectors
import subprocess
import threading
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, Literal

from .import_execution import repository_root


# Code constant: IEC bytes per gibibyte used by memory-limit calculations.
GIB = 1024**3
# Execution-global policy identity: the sole approved distributed profile for
# ordinary heavy seasonal tuning. Historical snapshots remain stored in DuckDB.
APPROVED_HEAVY_TUNING_PROFILE = "poc2_seasonal_recovery"
APPROVED_HEAVY_TUNING_PROFILE_VERSION = 2
# Code constant: profile fields admitted by the execution-override interface.
WORKER_FIELDS = (
    "cleaning_workers",
    "transformation_workers",
    "autoarima_workers",
    "chronos_processes",
    "chronos_inference_batch_size",
    "combination_workers",
    "evaluation_workers",
)


@dataclass(frozen=True)
class ExecutionProfile:
    """Purpose: Own an immutable named hardware execution profile. Inputs: Construction fields specify accelerator identity, positive stage/process and inference-batch counts, overlap policy, memory safety floors in GiB, one database writer, and optional platform-specific Dask limits. Outputs: A value object whose fields are consumed as execution settings and serialized as provenance; it owns no processes or mutable resources."""
    name: str
    required_accelerator: str | None
    expected_accelerator_name: str | None
    cleaning_workers: int
    transformation_workers: int
    autoarima_workers: int
    chronos_processes: int
    chronos_inference_batch_size: int
    combination_workers: int
    evaluation_workers: int
    cpu_gpu_overlap: bool
    system_memory_min_available_gib: float
    accelerator_memory_min_available_gib: float
    database_writers: int
    profile_version: int = 1
    dask_mac_cpu_workers: int | None = None
    dask_ubuntu_cpu_workers: int | None = None
    dask_ubuntu_gpu_workers: int | None = None
    dask_max_in_flight: int | None = None
    dask_mac_tuning_workers: int | None = None
    dask_ubuntu_tuning_workers: int | None = None
    dask_mac_worker_memory_gib: int | None = None
    dask_ubuntu_worker_memory_gib: int | None = None
    dask_mac_memory_min_available_gib: float | None = None
    dask_ubuntu_memory_min_available_gib: float | None = None
    dask_autoarima_max_in_flight: int | None = None
    dask_ets_max_in_flight: int | None = None
    dask_autoarima_fit_budget_gib: float | None = None
    dask_ets_fit_budget_gib: float | None = None
    dask_memory_admission_timeout_seconds: float | None = None
    dask_memory_poll_interval_seconds: float | None = None
    dask_memory_breach_grace_seconds: float | None = None
    dask_swap_growth_limit_gib: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return this object's dict representation for serialization and provenance comparison."""
        return asdict(self)

    @property
    def fingerprint(self) -> str:
        """Return a deterministic SHA-256 identity for the effective profile."""
        encoded = json.dumps(
            self.to_dict(), sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class ExecutionSettings:
    """Purpose: Own immutable non-scientific routing controls for one run. Inputs: ``mode`` is ``sequential``, ``local``, or ``dask``; scheduler address is optional; timeout is seconds; expected worker counts and in-flight limit are positive; retries are nonnegative. Outputs: A validated settings value serialized into execution provenance; it owns no client or worker resources."""

    # Execution global: creation defaults are owned here, CLI/profile overrides are
    # validated by the coordinator, and effective values are recorded per execution.
    mode: Literal["sequential", "local", "dask"] = "local"
    dask_scheduler_address: str | None = None
    dask_timeout_seconds: float = 60.0
    dask_expected_workers: int = 1
    dask_expected_gpu_workers: int = 1
    dask_max_in_flight: int = 8
    dask_retries: int = 2

    def __post_init__(self) -> None:
        """Purpose: Validate newly constructed execution settings. Inputs: The instance's mode, timeout in seconds, worker counts, in-flight bound, and retry count. Outputs: None; raises ``ValueError`` for unsupported modes or invalid bounds without changing frozen state."""
        if self.mode not in {"sequential", "local", "dask"}:
            raise ValueError("execution mode must be sequential, local, or dask")
        if self.dask_timeout_seconds <= 0:
            raise ValueError("Dask timeout must be positive")
        if (
            self.dask_expected_workers < 1
            or self.dask_expected_gpu_workers < 1
            or self.dask_max_in_flight < 1
        ):
            raise ValueError("Dask worker, GPU-worker, and in-flight limits must be positive")
        if self.dask_retries < 0:
            raise ValueError("Dask retries cannot be negative")

    def to_dict(self) -> dict[str, Any]:
        """Return this object's dict representation for serialization and provenance comparison."""
        return asdict(self)


def load_execution_profiles(path: Path | None = None) -> dict[str, ExecutionProfile]:
    """Purpose: Load named hardware profiles from JSON. Inputs: ``path`` is an optional filesystem ``Path``; ``None`` selects the repository configuration file. Outputs: A mapping from JSON profile names to immutable ``ExecutionProfile`` objects; reads one UTF-8 file and performs no writes."""
    path = path or repository_root() / "config/execution_profiles.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    return {name: ExecutionProfile(name=name, **values) for name, values in raw.items()}


def resolve_execution_profile(
    name: str, overrides: dict[str, Any] | None = None
) -> tuple[ExecutionProfile, dict[str, Any]]:
    """Purpose: Resolve and validate an effective hardware profile. Inputs: ``name`` selects a repository JSON profile and ``overrides`` maps dataclass field names to runtime values, with ``None`` values ignored; worker/batch counts must be positive and memory floors are GiB. Outputs: The replaced immutable profile and cleaned applied-override mapping; reads profile JSON and raises on unknown names/fields or violated accelerator, writer, and bound contracts."""
    profiles = load_execution_profiles()
    if name not in profiles:
        raise ValueError(
            f"unknown execution profile {name!r}; available: {', '.join(sorted(profiles))}"
        )
    clean_overrides = {
        key: value for key, value in (overrides or {}).items() if value is not None
    }
    unknown = sorted(set(clean_overrides) - set(ExecutionProfile.__dataclass_fields__))
    if unknown:
        raise ValueError(f"unknown execution overrides: {', '.join(unknown)}")
    if (
        profiles[name].required_accelerator is not None
        and "required_accelerator" in clean_overrides
        and clean_overrides["required_accelerator"]
        != profiles[name].required_accelerator
    ):
        raise ValueError(
            f"profile {name} requires {profiles[name].required_accelerator}; "
            "its accelerator cannot be overridden"
        )
    profile = replace(profiles[name], **clean_overrides)
    for field in WORKER_FIELDS:
        if getattr(profile, field) < 1:
            raise ValueError(f"{field} must be positive")
    if profile.database_writers != 1:
        raise ValueError("ShapeFM requires exactly one database writer")
    if profile.profile_version < 1:
        raise ValueError("profile_version must be positive")
    if profile.required_accelerator in {"mps", "cuda"} and profile.chronos_processes != 1:
        raise ValueError("MPS and CUDA profiles permit exactly one Chronos process")
    if profile.system_memory_min_available_gib < 0 or profile.accelerator_memory_min_available_gib < 0:
        raise ValueError("memory safety thresholds cannot be negative")
    for field in (
        "dask_mac_cpu_workers",
        "dask_ubuntu_cpu_workers",
        "dask_ubuntu_gpu_workers",
        "dask_max_in_flight",
        "dask_mac_tuning_workers",
        "dask_ubuntu_tuning_workers",
        "dask_mac_worker_memory_gib",
        "dask_ubuntu_worker_memory_gib",
        "dask_autoarima_max_in_flight",
        "dask_ets_max_in_flight",
    ):
        value = getattr(profile, field)
        minimum = 1 if field == "dask_max_in_flight" else 0
        if value is not None and value < minimum:
            raise ValueError(f"{field} must be at least {minimum} when configured")
    if (
        profile.dask_mac_tuning_workers is not None
        and profile.dask_mac_cpu_workers is not None
        and profile.dask_mac_tuning_workers > profile.dask_mac_cpu_workers
    ):
        raise ValueError("Mac tuning workers cannot exceed the Mac CPU pool")
    if (
        profile.dask_ubuntu_tuning_workers is not None
        and profile.dask_ubuntu_cpu_workers is not None
        and profile.dask_ubuntu_tuning_workers > profile.dask_ubuntu_cpu_workers
    ):
        raise ValueError("Ubuntu tuning workers cannot exceed the Ubuntu CPU pool")
    for field in (
        "dask_mac_memory_min_available_gib",
        "dask_ubuntu_memory_min_available_gib",
        "dask_autoarima_fit_budget_gib",
        "dask_ets_fit_budget_gib",
        "dask_memory_admission_timeout_seconds",
        "dask_memory_poll_interval_seconds",
        "dask_memory_breach_grace_seconds",
        "dask_swap_growth_limit_gib",
    ):
        value = getattr(profile, field)
        if value is not None and value <= 0:
            raise ValueError(f"{field} must be positive when configured")
    return profile, clean_overrides


def validate_heavy_tuning_execution(
    profile: ExecutionProfile,
    settings: ExecutionSettings,
    overrides: dict[str, Any],
) -> None:
    """Require the approved distributed profile or a recorded local exception.

    Inputs: Effective profile/settings and invocation overrides. A local
    exception must contain a nonempty researcher approval reference.
    Outputs: None; raises before hardware startup or scientific work when the
    heavy execution route is unapproved or differs from central configuration.
    """
    exception = overrides.get("local_heavy_exception")
    if exception is not None:
        reference = exception.get("approval_reference") if isinstance(exception, dict) else None
        if not isinstance(reference, str) or not reference.strip():
            raise RuntimeError("local heavy execution requires a recorded approval reference")
        if settings.mode not in {"local", "sequential"}:
            raise RuntimeError("a local heavy exception cannot be combined with Dask execution")
        return
    approved, _ = resolve_execution_profile(APPROVED_HEAVY_TUNING_PROFILE)
    if (
        profile.name != APPROVED_HEAVY_TUNING_PROFILE
        or profile.profile_version != APPROVED_HEAVY_TUNING_PROFILE_VERSION
        or profile.fingerprint != approved.fingerprint
        or settings.mode != "dask"
        or not settings.dask_scheduler_address
    ):
        raise RuntimeError(
            "heavy seasonal tuning requires the approved distributed execution "
            f"profile {APPROVED_HEAVY_TUNING_PROFILE} v"
            f"{APPROVED_HEAVY_TUNING_PROFILE_VERSION}; an unavailable Ubuntu host "
            "does not authorize local execution"
        )


def _sysctl_int(name: str) -> int | None:
    """Purpose: Probe one integer macOS kernel property. Inputs: ``name`` is a ``sysctl`` key string. Outputs: The parsed integer or ``None`` when unavailable/malformed; launches ``/usr/sbin/sysctl`` with a five-second timeout."""
    try:
        return int(
            subprocess.run(
                ["/usr/sbin/sysctl", "-n", name],
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout.strip()
        )
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


def _linux_memory() -> dict[str, int]:
    """Purpose: Probe current Linux host memory. Inputs: None; data originates from ``/proc/meminfo`` counters reported in KiB. Outputs: Total/available RAM and total/free swap in bytes, or an empty mapping when the file cannot be parsed; reads but does not modify procfs."""
    values: dict[str, int] = {}
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            key, value = line.split(":", 1)
            values[key] = int(value.strip().split()[0]) * 1024
    except (OSError, ValueError):
        return {}
    return {
        "total_bytes": values.get("MemTotal", 0),
        "available_bytes": values.get("MemAvailable", 0),
        "swap_total_bytes": values.get("SwapTotal", 0),
        "swap_free_bytes": values.get("SwapFree", 0),
    }


def _mac_memory() -> dict[str, int]:
    """Purpose: Probe current macOS host memory. Inputs: None; data originates from ``hw.memsize``, ``vm_stat`` page counters, and ``vm.swapusage`` MiB values. Outputs: Total/estimated-available RAM and total/free swap in bytes, using zero for failed subprobes; launches bounded ``sysctl`` and ``vm_stat`` subprocesses."""
    total = _sysctl_int("hw.memsize") or 0
    available = 0
    try:
        output = subprocess.run(
            ["/usr/bin/vm_stat"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout
        page_size = int(output.splitlines()[0].split("page size of ")[1].split()[0])
        pages = {}
        for line in output.splitlines()[1:]:
            if ":" in line:
                key, value = line.split(":", 1)
                pages[key] = int(value.strip().rstrip("."))
        available = page_size * sum(
            pages.get(key, 0)
            for key in ("Pages free", "Pages inactive", "Pages speculative", "Pages purgeable")
        )
    except (OSError, ValueError, IndexError, subprocess.SubprocessError):
        pass
    swap_total = swap_free = 0
    try:
        swap = subprocess.run(
            ["/usr/sbin/sysctl", "-n", "vm.swapusage"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout
        values = {
            key: float(value)
            for key, value in re.findall(r"(total|free) = ([0-9.]+)M", swap)
        }
        swap_total = int(values.get("total", 0) * 1024**2)
        swap_free = int(values.get("free", 0) * 1024**2)
    except (OSError, ValueError, subprocess.SubprocessError):
        pass
    return {
        "total_bytes": total,
        "available_bytes": available,
        "swap_total_bytes": swap_total,
        "swap_free_bytes": swap_free,
    }


def physical_cpu_count() -> int | None:
    """Purpose: Detect physical CPU cores on supported hosts. Inputs: None; uses macOS ``hw.physicalcpu`` or Linux physical/core ID pairs from ``/proc/cpuinfo``. Outputs: A positive core count or ``None`` when unsupported/unavailable; may launch ``sysctl`` or read procfs."""
    if platform.system() == "Darwin":
        return _sysctl_int("hw.physicalcpu")
    if platform.system() == "Linux":
        try:
            pairs = set()
            physical = core = None
            for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
                if line.startswith("physical id"):
                    physical = line.split(":", 1)[1].strip()
                elif line.startswith("core id"):
                    core = line.split(":", 1)[1].strip()
                elif not line and physical is not None and core is not None:
                    pairs.add((physical, core))
                    physical = core = None
            if physical is not None and core is not None:
                pairs.add((physical, core))
            return len(pairs) or None
        except OSError:
            return None
    return None


def cpu_model() -> str | None:
    """Purpose: Detect the host CPU model identity. Inputs: None; uses Linux ``/proc/cpuinfo``, macOS ``machdep.cpu.brand_string``, then ``platform.processor``. Outputs: A nonempty model string or ``None``; reads procfs or launches a five-second ``sysctl`` probe."""
    if platform.system() == "Linux":
        try:
            for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
        except (OSError, IndexError):
            pass
    elif platform.system() == "Darwin":
        try:
            return subprocess.run(
                ["/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string"],
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout.strip() or None
        except (OSError, subprocess.SubprocessError):
            pass
    return platform.processor() or None


def system_hardware() -> dict[str, Any]:
    """Purpose: Capture host hardware and runtime provenance. Inputs: None; values originate from platform APIs and CPU/memory probes. Outputs: A mapping of OS/release, architecture, CPU model, physical/logical counts, current byte-valued memory counters, and Python version; samples mutable host state and may invoke platform probes."""
    return {
        "operating_system": platform.system(),
        "operating_system_release": platform.release(),
        "architecture": platform.machine(),
        "cpu_model": cpu_model(),
        "physical_cpu_count": physical_cpu_count(),
        "logical_cpu_count": os.cpu_count(),
        "system_memory": system_memory(),
        "python_version": platform.python_version(),
    }


def system_memory() -> dict[str, int]:
    """Purpose: Dispatch the current-host memory probe. Inputs: None; operating-system identity comes from ``platform.system``. Outputs: Linux/macOS RAM and swap counters in bytes, or an empty mapping on unsupported systems; samples current host state."""
    return _mac_memory() if platform.system() == "Darwin" else _linux_memory()


def validate_system_memory(profile: ExecutionProfile, hardware: dict[str, Any]) -> None:
    """Purpose: Enforce a profile's host-memory safety floor. Inputs: An ``ExecutionProfile`` with a GiB minimum and a hardware snapshot containing ``system_memory.available_bytes``. Outputs: None; raises ``RuntimeError`` when a measurable byte value is below the converted threshold and does not mutate inputs."""
    available = hardware["system_memory"].get("available_bytes", 0)
    threshold = int(profile.system_memory_min_available_gib * GIB)
    if available and available < threshold:
        raise RuntimeError(
            f"system memory safety threshold reached: {available / GIB:.2f} GiB "
            f"available, {profile.system_memory_min_available_gib:.2f} GiB required"
        )


class PersistentChronosWorker:
    """Purpose: Own one persistent line-delimited JSON Chronos subprocess. Inputs: Construction receives an argv list, startup timeout in seconds, and bounded stderr capacity in bytes; requests later supply protocol mappings. Outputs: Ready/forecast response mappings and diagnostics; owns the child process, pipes, readiness state, stderr buffer/lock, and daemon drain thread until ``close`` or context exit."""

    def __init__(
        self,
        command: list[str],
        startup_timeout: float = 300.0,
        stderr_tail_bytes: int = 32 * 1024,
    ):
        """Purpose: Initialize an unstarted Chronos process owner. Inputs: ``command`` is the complete argv string list, ``startup_timeout`` is seconds, and ``stderr_tail_bytes`` is the positive retention bound. Outputs: None; stores configuration and creates lock/buffer state without launching a subprocess."""
        self.command = command
        self.startup_timeout = startup_timeout
        self.stderr_tail_bytes = stderr_tail_bytes
        self.process: subprocess.Popen[str] | None = None
        self.ready: dict[str, Any] | None = None
        self._stderr_tail = b""
        self._stderr_lock = threading.Lock()
        self._stderr_thread: threading.Thread | None = None

    @property
    def stderr_tail(self) -> str:
        """Return the most recent captured worker stderr lines for failure diagnostics."""
        with self._stderr_lock:
            return self._stderr_tail.decode(errors="replace")

    def _drain_stderr(self, stream: Any) -> None:
        """Purpose: Drain child stderr without blocking the protocol. Inputs: ``stream`` is the owned text stderr pipe exposing a binary buffer. Outputs: None; reads until EOF/error and updates the lock-protected trailing byte buffer, discarding older diagnostics beyond its bound."""
        try:
            while True:
                chunk = stream.buffer.read1(4096)
                if not chunk:
                    return
                with self._stderr_lock:
                    self._stderr_tail = (self._stderr_tail + chunk)[
                        -self.stderr_tail_bytes :
                    ]
        except (OSError, ValueError):
            return

    def _diagnostic(self, message: str, wait_for_stderr: bool = False) -> str:
        """Purpose: Format a Chronos failure diagnostic. Inputs: ``message`` is the primary error text and ``wait_for_stderr`` requests up to a one-second drain-thread join. Outputs: The message optionally suffixed with captured stderr; may wait for the owned thread but does not alter process state."""
        thread = self._stderr_thread
        if wait_for_stderr and thread is not None:
            thread.join(timeout=1)
        tail = self.stderr_tail.strip()
        return f"{message}\nstderr tail:\n{tail}" if tail else message

    def start(self) -> dict[str, Any]:
        """Purpose: Start and handshake with the configured Chronos bridge. Inputs: Stored argv and startup timeout in seconds. Outputs: The decoded ``ready`` metadata mapping; creates the child with three pipes and a daemon stderr thread, stores process/readiness state, and force-closes partial state before raising on startup failure."""
        if self.process is not None:
            raise RuntimeError("Chronos worker is already running")
        self.process = subprocess.Popen(
            self.command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        if self.process.stderr is None:
            self.close(force=True)
            raise RuntimeError("Chronos worker stderr pipe was not created")
        self._stderr_thread = threading.Thread(
            target=self._drain_stderr,
            args=(self.process.stderr,),
            name="shapefm-chronos-stderr",
            daemon=True,
        )
        self._stderr_thread.start()
        try:
            message = self._read(self.startup_timeout)
        except BaseException:
            self.close(force=True)
            raise
        if message.get("type") != "ready":
            self.close(force=True)
            raise RuntimeError(
                self._diagnostic(f"Chronos worker failed to start: {message}")
            )
        self.ready = message
        return message

    def _read(self, timeout: float) -> dict[str, Any]:
        """Purpose: Receive one worker protocol message. Inputs: ``timeout`` is the maximum wait in seconds and the owned process must be running with stdout. Outputs: One decoded JSON-object line; temporarily registers stdout with a selector and raises with diagnostics on timeout, EOF, or malformed JSON."""
        if self.process is None or self.process.stdout is None:
            raise RuntimeError("Chronos worker is not running")
        selector = selectors.DefaultSelector()
        selector.register(self.process.stdout, selectors.EVENT_READ)
        try:
            if not selector.select(timeout):
                raise TimeoutError(
                    self._diagnostic(
                        f"Chronos worker did not respond within {timeout} seconds"
                    )
                )
            line = self.process.stdout.readline()
        finally:
            selector.close()
        if not line:
            raise RuntimeError(
                self._diagnostic(
                    "Chronos worker exited unexpectedly", wait_for_stderr=True
                )
            )
        return json.loads(line)

    def request(self, payload: dict[str, Any], timeout: float = 1800.0) -> dict[str, Any]:
        """Purpose: Perform one synchronous Chronos protocol request. Inputs: ``payload`` is a JSON-serializable command mapping with a batch ID and ``timeout`` is seconds. Outputs: The decoded response mapping with the same batch ID; writes/flushed one compact JSON line to child stdin and waits on stdout."""
        if self.process is None or self.process.stdin is None:
            raise RuntimeError("Chronos worker is not running")
        self.process.stdin.write(json.dumps(payload, separators=(",", ":")) + "\n")
        self.process.stdin.flush()
        response = self._read(timeout)
        if response.get("batch_id") != payload.get("batch_id"):
            raise RuntimeError("Chronos worker returned a mismatched batch identifier")
        return response

    def close(self, force: bool = False) -> None:
        """Purpose: Release all resources owned by the Chronos bridge client. Inputs: ``force`` selects immediate termination instead of graceful protocol shutdown. Outputs: None; atomically forgets the process, requests shutdown or escalates through terminate/kill with bounded waits, joins the stderr thread, and closes all pipes; repeated calls are safe."""
        process, self.process = self.process, None
        if process is None:
            return
        if not force and process.poll() is None and process.stdin is not None:
            try:
                process.stdin.write('{"command":"shutdown"}\n')
                process.stdin.flush()
                process.wait(timeout=10)
            except (BrokenPipeError, subprocess.TimeoutExpired):
                force = True
        if force and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        thread, self._stderr_thread = self._stderr_thread, None
        if thread is not None:
            thread.join(timeout=5)
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None:
                stream.close()

    def __enter__(self) -> "PersistentChronosWorker":
        """Start the bridge and return this context-managed client."""
        self.start()
        return self

    def __exit__(self, *_: object) -> None:
        """Close the bridge when leaving its coordinator-owned context."""
        self.close()
