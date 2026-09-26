"""Hardware-aware local execution profiles and resource provenance."""

from __future__ import annotations

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

from .orchestration import repository_root


GIB = 1024**3
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
    dask_mac_cpu_workers: int | None = None
    dask_ubuntu_cpu_workers: int | None = None
    dask_max_in_flight: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExecutionSettings:
    """Invocation-only execution routing, excluded from scientific identity."""

    mode: Literal["sequential", "local", "dask"] = "local"
    dask_scheduler_address: str | None = None
    dask_timeout_seconds: float = 60.0
    dask_expected_workers: int = 1
    dask_max_in_flight: int = 8
    dask_retries: int = 2

    def __post_init__(self) -> None:
        if self.mode not in {"sequential", "local", "dask"}:
            raise ValueError("execution mode must be sequential, local, or dask")
        if self.dask_timeout_seconds <= 0:
            raise ValueError("Dask timeout must be positive")
        if self.dask_expected_workers < 1 or self.dask_max_in_flight < 1:
            raise ValueError("Dask worker and in-flight limits must be positive")
        if self.dask_retries < 0:
            raise ValueError("Dask retries cannot be negative")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def load_execution_profiles(path: Path | None = None) -> dict[str, ExecutionProfile]:
    path = path or repository_root() / "config/execution_profiles.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    return {name: ExecutionProfile(name=name, **values) for name, values in raw.items()}


def resolve_execution_profile(
    name: str, overrides: dict[str, Any] | None = None
) -> tuple[ExecutionProfile, dict[str, Any]]:
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
    if profile.required_accelerator in {"mps", "cuda"} and profile.chronos_processes != 1:
        raise ValueError("MPS and CUDA profiles permit exactly one Chronos process")
    if profile.system_memory_min_available_gib < 0 or profile.accelerator_memory_min_available_gib < 0:
        raise ValueError("memory safety thresholds cannot be negative")
    for field in (
        "dask_mac_cpu_workers",
        "dask_ubuntu_cpu_workers",
        "dask_max_in_flight",
    ):
        value = getattr(profile, field)
        if value is not None and value < 1:
            raise ValueError(f"{field} must be positive when configured")
    return profile, clean_overrides


def _sysctl_int(name: str) -> int | None:
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
    return _mac_memory() if platform.system() == "Darwin" else _linux_memory()


def validate_system_memory(profile: ExecutionProfile, hardware: dict[str, Any]) -> None:
    available = hardware["system_memory"].get("available_bytes", 0)
    threshold = int(profile.system_memory_min_available_gib * GIB)
    if available and available < threshold:
        raise RuntimeError(
            f"system memory safety threshold reached: {available / GIB:.2f} GiB "
            f"available, {profile.system_memory_min_available_gib:.2f} GiB required"
        )


class PersistentChronosWorker:
    """Coordinator-owned client for one model-loaded accelerator subprocess."""

    def __init__(
        self,
        command: list[str],
        startup_timeout: float = 300.0,
        stderr_tail_bytes: int = 32 * 1024,
    ):
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
        with self._stderr_lock:
            return self._stderr_tail.decode(errors="replace")

    def _drain_stderr(self, stream: Any) -> None:
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
        thread = self._stderr_thread
        if wait_for_stderr and thread is not None:
            thread.join(timeout=1)
        tail = self.stderr_tail.strip()
        return f"{message}\nstderr tail:\n{tail}" if tail else message

    def start(self) -> dict[str, Any]:
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
        if self.process is None or self.process.stdin is None:
            raise RuntimeError("Chronos worker is not running")
        self.process.stdin.write(json.dumps(payload, separators=(",", ":")) + "\n")
        self.process.stdin.flush()
        response = self._read(timeout)
        if response.get("batch_id") != payload.get("batch_id"):
            raise RuntimeError("Chronos worker returned a mismatched batch identifier")
        return response

    def close(self, force: bool = False) -> None:
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
        self.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
