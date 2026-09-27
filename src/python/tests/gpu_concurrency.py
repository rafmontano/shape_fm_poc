#!/usr/bin/env python3
"""Isolated one-versus-fifteen logical GPU-worker calibration."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shlex
import socket
import statistics
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

import duckdb
from distributed import Client, Future, as_completed

from shapefm.calibration import _scientific_comparison
from shapefm.config import json_fingerprint
from shapefm.dask_execution import chronos_batch, validate_cluster
from shapefm.poc1 import QUANTILES, _length_aware_batches, scientific_configuration
from shapefm.transformations import inverse


ACCEPTED_REVISION = "597232a2b9e0ad67550480607b44ec4cbe542f43"
EXPECTED_TASKS = 400
WARM_REPETITIONS = 3
GIB = 1024**3
UBUNTU_MEMORY_HEADROOM_BYTES = 16 * GIB
DEFAULT_DATABASE = ROOT / "results/poc2_acceptance.duckdb"
DEFAULT_ACCEPTANCE_REPORT = ROOT / "results/poc2_acceptance_report.json"
DEFAULT_OUTPUT = ROOT / "results/poc2_gpu_concurrency_1_vs_15.json"


@dataclass(frozen=True)
class GpuCalibrationSettings:
    physical_gpu_count: int = 1
    gpu_worker_processes: int = 1
    gpu_max_in_flight_batches: int = 1
    gpu_memory_headroom_gib: float = 4.0
    telemetry_interval_seconds: float = 0.2
    worker_memory_limit_gib: int = 4
    chronos_batch_size: int = 16

    def validate(self) -> None:
        integer_values = {
            "physical_gpu_count": self.physical_gpu_count,
            "gpu_worker_processes": self.gpu_worker_processes,
            "gpu_max_in_flight_batches": self.gpu_max_in_flight_batches,
            "worker_memory_limit_gib": self.worker_memory_limit_gib,
            "chronos_batch_size": self.chronos_batch_size,
        }
        for name, value in integer_values.items():
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if self.gpu_max_in_flight_batches < self.gpu_worker_processes:
            raise ValueError(
                "gpu_max_in_flight_batches must be at least gpu_worker_processes"
            )
        if self.gpu_memory_headroom_gib < 4:
            raise ValueError("gpu_memory_headroom_gib must be at least 4 GiB")
        if not 0.1 <= self.telemetry_interval_seconds <= 0.25:
            raise ValueError(
                "telemetry_interval_seconds must be between 0.1 and 0.25 seconds"
            )
        declared = self.gpu_worker_processes * self.worker_memory_limit_gib
        if declared * GIB > 128_000_000_000 - UBUNTU_MEMORY_HEADROOM_BYTES:
            raise ValueError(
                "configured GPU-worker system-memory ceilings do not preserve "
                "16 GiB Ubuntu headroom"
            )


def control_settings() -> GpuCalibrationSettings:
    return GpuCalibrationSettings()


def candidate_settings() -> GpuCalibrationSettings:
    return GpuCalibrationSettings(
        physical_gpu_count=1,
        gpu_worker_processes=15,
        gpu_max_in_flight_batches=15,
    )


def _run(
    arguments: list[str],
    *,
    timeout: float = 30,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        arguments,
        cwd=ROOT,
        capture_output=True,
        check=check,
        text=True,
        timeout=timeout,
    )


def _git(*arguments: str) -> str:
    return _run(["git", *arguments]).stdout.strip()


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def _telemetry_summary(
    samples: list[dict[str, Any]],
    errors: list[str],
    initial_swap_used_bytes: int | None,
    unsafe_reason: str | None,
) -> dict[str, Any]:
    gpu_utilisation = [float(item["gpu_utilisation_percent"]) for item in samples]
    gpu_used = [int(item["gpu_memory_used_bytes"]) for item in samples]
    gpu_free = [int(item["gpu_memory_free_bytes"]) for item in samples]
    powers = [float(item["gpu_power_watts"]) for item in samples if item["gpu_power_watts"] is not None]
    temperatures = [
        float(item["gpu_temperature_celsius"])
        for item in samples
        if item["gpu_temperature_celsius"] is not None
    ]
    available = [int(item["system_available_memory_bytes"]) for item in samples]
    swap = [int(item["swap_used_bytes"]) for item in samples]
    return {
        "sample_count": len(samples),
        "sampling_errors": errors,
        "sampling_interval_seconds": (
            samples[0]["configured_interval_seconds"] if samples else None
        ),
        "gpu_utilisation_percent": {
            "mean": statistics.fmean(gpu_utilisation) if gpu_utilisation else None,
            "median": statistics.median(gpu_utilisation) if gpu_utilisation else None,
            "p95": _percentile(gpu_utilisation, 0.95),
            "maximum": max(gpu_utilisation, default=None),
        },
        "gpu_memory": {
            "mean_used_bytes": statistics.fmean(gpu_used) if gpu_used else None,
            "maximum_used_bytes": max(gpu_used, default=None),
            "mean_free_bytes": statistics.fmean(gpu_free) if gpu_free else None,
            "minimum_free_bytes": min(gpu_free, default=None),
        },
        "gpu_power_watts": {
            "mean": statistics.fmean(powers) if powers else None,
            "maximum": max(powers, default=None),
        },
        "gpu_temperature_celsius": {
            "mean": statistics.fmean(temperatures) if temperatures else None,
            "maximum": max(temperatures, default=None),
        },
        "minimum_system_available_memory_bytes": min(available, default=None),
        "initial_swap_used_bytes": initial_swap_used_bytes,
        "maximum_swap_used_bytes": max(swap, default=None),
        "swap_growth_bytes": (
            max(0, max(swap) - initial_swap_used_bytes)
            if swap and initial_swap_used_bytes is not None
            else None
        ),
        "unsafe_reason": unsafe_reason,
        "passed": bool(samples) and not errors and unsafe_reason is None,
    }


class _RemoteGpuTelemetry:
    """Sample the one physical Ubuntu GPU independently of busy Dask workers."""

    def __init__(
        self,
        cluster: "_GpuCluster",
        settings: GpuCalibrationSettings,
        unsafe_callback: Callable[[str], None],
    ):
        self.cluster = cluster
        self.settings = settings
        self.unsafe_callback = unsafe_callback
        self.samples: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self.unsafe_reason: str | None = None
        self.initial_swap_used_bytes: int | None = None
        self._process: subprocess.Popen[str] | None = None
        self._thread: threading.Thread | None = None
        self._first_sample = threading.Event()
        self._stopping = threading.Event()

    def _reader(self) -> None:
        assert self._process is not None and self._process.stdout is not None
        try:
            for line in self._process.stdout:
                try:
                    sample = json.loads(line)
                except json.JSONDecodeError as error:
                    self.errors.append(f"JSONDecodeError: {error}: {line.strip()}")
                    self._trigger("GPU telemetry returned invalid JSON")
                    return
                self.samples.append(sample)
                self._first_sample.set()
                swap = int(sample["swap_used_bytes"])
                if self.initial_swap_used_bytes is None:
                    self.initial_swap_used_bytes = swap
                reason = None
                if int(sample["gpu_memory_free_bytes"]) < int(
                    self.settings.gpu_memory_headroom_gib * GIB
                ):
                    reason = "free GPU memory fell below the configured headroom"
                elif int(sample["system_available_memory_bytes"]) < UBUNTU_MEMORY_HEADROOM_BYTES:
                    reason = "Ubuntu available system memory fell below 16 GiB"
                elif swap > self.initial_swap_used_bytes:
                    reason = "Ubuntu swap use grew during the active GPU window"
                if reason:
                    self._trigger(reason)
                    return
            if not self._stopping.is_set() and self.unsafe_reason is None:
                stderr = ""
                if self._process.stderr is not None:
                    stderr = self._process.stderr.read().strip()
                self.errors.append(
                    "remote GPU telemetry stopped unexpectedly"
                    + (f": {stderr}" if stderr else "")
                )
                self._trigger("GPU telemetry sampler stopped unexpectedly")
        except BaseException as error:
            self.errors.append(f"{type(error).__name__}: {error}")
            self._trigger("GPU telemetry sampler failed")

    def _trigger(self, reason: str) -> None:
        if self.unsafe_reason is not None:
            return
        self.unsafe_reason = reason
        self.unsafe_callback(reason)

    def __enter__(self) -> "_RemoteGpuTelemetry":
        script = r'''import json, subprocess, time
interval = float(__import__("sys").argv[1])
while True:
    fields = subprocess.run([
        "nvidia-smi",
        "--query-gpu=utilization.gpu,memory.used,memory.free,memory.total,power.draw,temperature.gpu",
        "--format=csv,noheader,nounits",
    ], check=True, capture_output=True, text=True, timeout=5).stdout.strip().splitlines()
    if len(fields) != 1:
        raise RuntimeError(f"expected one physical GPU, found {len(fields)}")
    values = [value.strip() for value in fields[0].split(",")]
    memory = {}
    with open("/proc/meminfo", encoding="utf-8") as stream:
        for line in stream:
            name, value = line.split(":", 1)
            memory[name] = int(value.strip().split()[0]) * 1024
    def number(value):
        return None if value in {"[N/A]", "N/A"} else float(value)
    print(json.dumps({
        "at_unix_seconds": time.time(),
        "configured_interval_seconds": interval,
        "gpu_utilisation_percent": float(values[0]),
        "gpu_memory_used_bytes": int(float(values[1]) * 1024 * 1024),
        "gpu_memory_free_bytes": int(float(values[2]) * 1024 * 1024),
        "gpu_memory_total_bytes": int(float(values[3]) * 1024 * 1024),
        "gpu_power_watts": number(values[4]),
        "gpu_temperature_celsius": number(values[5]),
        "system_available_memory_bytes": memory["MemAvailable"],
        "swap_used_bytes": memory["SwapTotal"] - memory["SwapFree"],
    }), flush=True)
    time.sleep(interval)
'''
        command = self.cluster.ssh_arguments(
            f"python3 -u -c {shlex.quote(script)} "
            f"{self.settings.telemetry_interval_seconds}"
        )
        self._process = subprocess.Popen(
            command,
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self._thread = threading.Thread(
            target=self._reader,
            name="shapefm-gpu-concurrency-telemetry",
            daemon=True,
        )
        self._thread.start()
        if not self._first_sample.wait(10):
            self.errors.append("GPU telemetry produced no sample within 10 seconds")
            self._trigger("GPU telemetry sampler did not start")
        return self

    def __exit__(self, *_: object) -> None:
        self._stopping.set()
        if self._process is not None and self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=3)
        if self._thread is not None:
            self._thread.join(timeout=3)

    def summary(self) -> dict[str, Any]:
        return _telemetry_summary(
            self.samples,
            self.errors,
            self.initial_swap_used_bytes,
            self.unsafe_reason,
        )


class _SchedulerSafetyMonitor:
    def __init__(
        self,
        client: Client,
        workers: set[str],
        interval_seconds: float,
        unsafe_callback: Callable[[str], None],
    ):
        self.client = client
        self.workers = workers
        self.interval_seconds = interval_seconds
        self.unsafe_callback = unsafe_callback
        self.errors: list[str] = []
        self.maximum_spill_bytes = 0
        self.observed_workers = set(workers)
        self.unsafe_reason: str | None = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                current = self.client.scheduler_info()["workers"]
                addresses = set(current)
                self.observed_workers.update(addresses)
                if addresses != self.workers:
                    self._trigger("Dask worker loss, replacement, or addition detected")
                    return
                spill = 0
                for details in current.values():
                    spilled = details.get("metrics", {}).get("spilled_bytes", {})
                    spill += (
                        int(spilled.get("memory", 0))
                        + int(spilled.get("disk", 0))
                        if isinstance(spilled, dict)
                        else int(spilled)
                    )
                self.maximum_spill_bytes = max(self.maximum_spill_bytes, spill)
                if spill > 0:
                    self._trigger("Dask spilling detected")
                    return
            except BaseException as error:
                self.errors.append(f"{type(error).__name__}: {error}")
                self._trigger("Dask safety sampler failed")
                return

    def _trigger(self, reason: str) -> None:
        if self.unsafe_reason is not None:
            return
        self.unsafe_reason = reason
        self.unsafe_callback(reason)

    def __enter__(self) -> "_SchedulerSafetyMonitor":
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._stop.set()
        self._thread.join(timeout=3)

    def summary(self) -> dict[str, Any]:
        return {
            "sampling_errors": self.errors,
            "initial_workers": sorted(self.workers),
            "observed_workers": sorted(self.observed_workers),
            "worker_loss_replacement_or_addition": self.observed_workers != self.workers,
            "maximum_dask_spill_bytes": self.maximum_spill_bytes,
            "unsafe_reason": self.unsafe_reason,
            "passed": not self.errors
            and self.unsafe_reason is None
            and self.observed_workers == self.workers
            and self.maximum_spill_bytes == 0,
        }


class _GpuCluster:
    def __init__(self, label: str, settings: GpuCalibrationSettings):
        self.label = label
        self.settings = settings
        self.ubuntu_host = os.environ.get(
            "SHAPEFM_UBUNTU_HOST", "rafmontano@WSUbuntu1.local"
        )
        self.ubuntu_root = os.environ.get(
            "SHAPEFM_UBUNTU_ROOT",
            "/home/rafmontano/Documents/PhD/2026/projects/shape_fm_poc",
        )
        self.mac_host = os.environ.get("SHAPEFM_MAC_HOST", "RMMacbookPro.local")
        self.bind_host = os.environ.get("SHAPEFM_MAC_BIND_HOST") or _run(
            ["ipconfig", "getifaddr", "en0"], check=False
        ).stdout.strip()
        if not self.bind_host:
            raise RuntimeError("set SHAPEFM_MAC_BIND_HOST to the Mac LAN IPv4 address")
        self.scheduler_address = "tcp://127.0.0.1:8786"
        self.worker_address = f"tcp://{self.mac_host}:8786"
        self.runtime = ROOT / "data/dask"
        self.scheduler: subprocess.Popen[Any] | None = None
        self._stop_lock = threading.Lock()
        self._stopped = False

    def ssh_arguments(self, command: str) -> list[str]:
        return [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            self.ubuntu_host,
            command,
        ]

    def ssh(self, command: str, timeout: float = 30) -> str:
        return _run(self.ssh_arguments(command), timeout=timeout).stdout.strip()

    def preflight(self) -> dict[str, Any]:
        local_revision = _git("rev-parse", "HEAD")
        if _git("status", "--porcelain", "--untracked-files=all"):
            raise RuntimeError("Mac worktree must be clean before GPU calibration")
        remote = self.ssh(
            "set -eu; "
            f"cd {shlex.quote(self.ubuntu_root)}; "
            "test -z \"$(git status --porcelain --untracked-files=all)\"; "
            "test -x .tools/uv/uv; test -x .venv/bin/python; "
            "test -x environments/chronos-2/.venv/bin/python; "
            "test ! -e data/dask/gpu-concurrency.pid; "
            "! pgrep -f '[g]pu-concurrency-worker' >/dev/null; "
            "! ss -ltn | grep -Eq ':(8786|8787)[[:space:]]'; "
            "printf '%s\\n%s\\n%s\\n' \"$(git rev-parse HEAD)\" "
            "\"$(git -C external/gift-eval rev-parse HEAD)\" \"$(hostname)\"; "
            "nvidia-smi --list-gpus"
        ).splitlines()
        if len(remote) < 4 or remote[0] != local_revision:
            raise RuntimeError(
                f"Mac/Ubuntu revision mismatch: Mac={local_revision}, Ubuntu={remote}"
            )
        physical_gpu_count = len(remote[3:])
        if physical_gpu_count != self.settings.physical_gpu_count:
            raise RuntimeError(
                f"found {physical_gpu_count} physical GPUs, expected "
                f"{self.settings.physical_gpu_count}"
            )
        return {
            "mac_revision": local_revision,
            "ubuntu_revision": remote[0],
            "gift_eval_revision": remote[1],
            "ubuntu_hostname": remote[2],
            "physical_gpu_count": physical_gpu_count,
            "physical_gpu_descriptions": remote[3:],
        }

    def start(self) -> tuple[Client, dict[str, Any]]:
        self.runtime.mkdir(parents=True, exist_ok=True)
        scheduler_log = (
            self.runtime / f"gpu-concurrency-{self.label}-scheduler.log"
        ).open("a", encoding="utf-8")
        self.scheduler = subprocess.Popen(
            [
                str(ROOT / ".tools/uv/uv"),
                "run",
                "--locked",
                "dask",
                "scheduler",
                "--host",
                "0.0.0.0",
                "--port",
                "8786",
                "--dashboard-address",
                "127.0.0.1:8787",
            ],
            cwd=ROOT,
            stdin=subprocess.DEVNULL,
            stdout=scheduler_log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        scheduler_log.close()
        time.sleep(2)
        remote_root = shlex.quote(self.ubuntu_root)
        self.ssh(
            "set -eu; "
            f"cd {remote_root}; mkdir -p data/dask; "
            f"nohup env CUDA_VISIBLE_DEVICES=0 .tools/uv/uv run --locked dask worker "
            f"{shlex.quote(self.worker_address)} "
            f"--nworkers {self.settings.gpu_worker_processes} --nthreads 1 "
            f"--name gpu-concurrency-worker-{self.label} --resources GPU=1 "
            f"--memory-limit {self.settings.worker_memory_limit_gib}GiB --no-dashboard "
            f">data/dask/gpu-concurrency-{self.label}-worker.log 2>&1 </dev/null & "
            "echo $! >data/dask/gpu-concurrency.pid"
        )
        client = Client(self.scheduler_address, timeout="180s")
        config = json.loads(
            (ROOT / "config/experiments/poc1.json").read_text(encoding="utf-8")
        )
        try:
            reports = validate_cluster(
                client,
                expected_workers=self.settings.gpu_worker_processes,
                expected_gpu_workers=self.settings.gpu_worker_processes,
                timeout=180,
                expected_commit=_git("rev-parse", "HEAD"),
                expected_configuration_hash=json_fingerprint(
                    scientific_configuration(config)
                ),
                require_gpu=True,
            )
            if any(
                report["hostname"] == socket.gethostname()
                or report["resources"].get("GPU", 0) != 1
                or report["resources"].get("CPU", 0) != 0
                for report in reports.values()
            ):
                raise RuntimeError("GPU calibration registered a non-Ubuntu or CPU worker")
            return client, reports
        except BaseException:
            client.close(timeout=5)
            self.stop()
            raise

    def stop(self) -> None:
        with self._stop_lock:
            if self._stopped:
                return
            self._stopped = True
        try:
            client = Client(self.scheduler_address, timeout="3s")
            try:
                client.shutdown()
            finally:
                client.close(timeout=3)
        except BaseException:
            pass
        if self.scheduler is not None and self.scheduler.poll() is None:
            self.scheduler.terminate()
            try:
                self.scheduler.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.scheduler.kill()
                self.scheduler.wait(timeout=5)
        try:
            self.ssh(
                f"cd {shlex.quote(self.ubuntu_root)}; "
                "if test -s data/dask/gpu-concurrency.pid; then "
                "kill \"$(cat data/dask/gpu-concurrency.pid)\" 2>/dev/null || true; "
                "rm -f data/dask/gpu-concurrency.pid; fi; "
                "pkill -f '[g]pu-concurrency-worker' 2>/dev/null || true",
                timeout=15,
            )
        except BaseException:
            pass

    def confirm_stopped(self) -> dict[str, Any]:
        local_ports = _run(
            ["lsof", "-nP", "-iTCP:8786", "-iTCP:8787", "-sTCP:LISTEN"],
            check=False,
        ).stdout.strip()
        remote = self.ssh(
            f"cd {shlex.quote(self.ubuntu_root)}; "
            "test ! -e data/dask/gpu-concurrency.pid; "
            "! pgrep -f '[g]pu-concurrency-worker' >/dev/null; echo clean"
        )
        return {
            "local_acceptance_ports_closed": not local_ports,
            "remote_gpu_worker_processes_stopped": remote == "clean",
        }


def _load_workload(
    database: Path, acceptance_report: Path
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], dict[str, Any]]:
    accepted = json.loads(acceptance_report.read_text(encoding="utf-8"))
    if not accepted.get("acceptance_passed") or accepted.get("repository_revision") != ACCEPTED_REVISION:
        raise RuntimeError("accepted POC2 report is missing or does not match the baseline")
    experiment_id = accepted["initial_run"]["plan"]["experiment_id"]
    connection = duckdb.connect(str(database.resolve()), read_only=True)
    try:
        rows = connection.execute(
            """SELECT t.task_id, ts.transformed_target, i.horizon,
                      ts.transformation_method, CAST(ts.parameters AS VARCHAR),
                      f.mean, f.median, f.quantiles, f.content_hash,
                      i.series_id, i.official_position
               FROM experiment_tasks t
               JOIN transformed_series ts
                 ON ts.experiment_id=t.experiment_id
                AND ts.variant_id=t.variant_id
                AND ts.forecast_instance_id=t.forecast_instance_id
               JOIN forecast_instances i
                 ON i.forecast_instance_id=t.forecast_instance_id
               JOIN forecasts f
                 ON f.experiment_id=t.experiment_id
                AND f.variant_id=t.variant_id
                AND f.forecast_instance_id=t.forecast_instance_id
                AND f.candidate=t.candidate
               WHERE t.experiment_id=? AND t.stage=4 AND t.candidate='chronos_2'
               ORDER BY t.task_id""",
            [experiment_id],
        ).fetchall()
    finally:
        connection.close()
    if len(rows) != EXPECTED_TASKS or len({row[0] for row in rows}) != EXPECTED_TASKS:
        raise RuntimeError(
            f"expected {EXPECTED_TASKS} unique accepted Chronos tasks, found {len(rows)}"
        )
    if len({row[9] for row in rows}) != 100 or sorted({row[10] for row in rows}) != list(range(100)):
        raise RuntimeError("accepted workload is not the first 100 official M4 Daily series")
    jobs = [
        {"id": row[0], "context": row[1], "horizon": int(row[2])}
        for row in rows
    ]
    references = {
        row[0]: {
            "transformation_method": row[3],
            "transformation_parameters": json.loads(row[4]),
            "mean": row[5],
            "median": row[6],
            "quantiles": row[7],
            "content_hash": row[8],
        }
        for row in rows
    }
    metadata = {
        "accepted_baseline_revision": accepted["repository_revision"],
        "accepted_forecast_fingerprint": accepted["initial_run"]["evidence"][
            "forecast_fingerprint"
        ],
        "experiment_id": experiment_id,
        "series_count": 100,
        "preprocessing_variant_count": 4,
        "chronos_task_count": len(rows),
        "model_revision": accepted["model_revision"],
        "source_revision": accepted["source_revision"],
    }
    return jobs, references, metadata


def _original_scale_result(
    result: dict[str, Any], reference: dict[str, Any]
) -> dict[str, Any]:
    method = reference["transformation_method"]
    parameters = reference["transformation_parameters"]
    return {
        "mean": list(inverse(result["mean"], method, parameters)),
        "median": list(inverse(result["median"], method, parameters)),
        "quantiles": [
            list(inverse(values, method, parameters))
            for values in result["quantiles"]
        ],
    }


def _run_repetition(
    client: Client,
    settings: GpuCalibrationSettings,
    workers: list[str],
    batches: list[list[dict[str, Any]]],
    references: dict[str, dict[str, Any]],
    repetition: str,
    cluster: _GpuCluster,
) -> dict[str, Any]:
    unsafe_reasons: list[str] = []
    stop_lock = threading.Lock()

    def unsafe(reason: str) -> None:
        with stop_lock:
            if unsafe_reasons:
                return
            unsafe_reasons.append(reason)
        cluster.stop()

    started = time.monotonic()
    outputs: dict[str, dict[str, Any]] = {}
    observed_ids: list[str] = []
    future_batches: dict[Future, tuple[list[dict[str, Any]], float]] = {}
    completion = as_completed(with_results=True, raise_errors=False)
    per_worker: dict[str, dict[str, Any]] = {}
    batch_measurements: list[dict[str, Any]] = []
    model_instances: dict[str, dict[str, Any]] = {}
    pending = iter(batches)
    submitted = 0
    maximum_in_flight = 0

    def submit(batch: list[dict[str, Any]], worker: str | None = None) -> None:
        nonlocal submitted, maximum_in_flight
        key = f"gpu-concurrency/{repetition}/{submitted}/{uuid.uuid4().hex}"
        options: dict[str, Any] = {}
        if worker is not None:
            options.update(workers=[worker], allow_other_workers=False)
        future = client.submit(
            chronos_batch,
            batch,
            "amazon/chronos-2",
            "29ec3766d36d6f73f0696f85560a422f50e8498c",
            list(QUANTILES),
            "cuda",
            0,
            key=key,
            resources={"GPU": 1},
            retries=0,
            pure=False,
            **options,
        )
        future_batches[future] = (batch, time.monotonic())
        completion.add(future)
        submitted += 1
        maximum_in_flight = max(maximum_in_flight, len(future_batches))

    telemetry = _RemoteGpuTelemetry(cluster, settings, unsafe)
    scheduler_monitor = _SchedulerSafetyMonitor(
        client, set(workers), settings.telemetry_interval_seconds, unsafe
    )
    failure = None
    try:
        with telemetry, scheduler_monitor:
            for worker in workers:
                try:
                    submit(next(pending), worker)
                except StopIteration:
                    break
            while len(future_batches) < settings.gpu_max_in_flight_batches:
                try:
                    submit(next(pending))
                except StopIteration:
                    break
            while future_batches:
                future, response = next(completion)
                batch, dispatched_at = future_batches.pop(future)
                if isinstance(response, BaseException):
                    raise response
                result_ids = [item["id"] for item in response["results"]]
                if len(result_ids) != len(set(result_ids)) or set(result_ids) != {
                    item["id"] for item in batch
                }:
                    raise RuntimeError("Chronos batch returned invalid task IDs")
                now = time.monotonic()
                worker = response["worker"]
                address = worker["dask_worker"]
                contribution = per_worker.setdefault(
                    address,
                    {
                        "worker_name": worker["dask_worker_name"],
                        "hostname": worker["hostname"],
                        "completed_tasks": 0,
                        "completed_batches": 0,
                    },
                )
                contribution["completed_tasks"] += len(batch)
                contribution["completed_batches"] += 1
                model_instances[address] = {
                    "model_load_count": worker["model_load_count"],
                    "model_load_seconds": worker["model_load_seconds"],
                    "worker_generation": worker["worker_generation"],
                    "peak_process_memory_bytes": worker["peak_process_memory_bytes"],
                }
                for result in response["results"]:
                    observed_ids.append(result["id"])
                    outputs[result["id"]] = _original_scale_result(
                        result, references[result["id"]]
                    )
                batch_measurements.append(
                    {
                        "worker": address,
                        "task_count": len(batch),
                        "effective_batch_size": worker["effective_batch_size"],
                        "dispatch_to_completion_seconds": now - dispatched_at,
                        "worker_runtime_seconds": response["runtime_seconds"],
                        "inference_seconds": worker["inference_seconds"],
                        "completed_at_seconds": now - started,
                    }
                )
                future.release()
                if unsafe_reasons:
                    raise RuntimeError(unsafe_reasons[0])
                try:
                    submit(next(pending))
                except StopIteration:
                    pass
    except BaseException as error:
        failure = f"{type(error).__name__}: {error}"
    finally:
        for future in future_batches:
            future.cancel()
            future.release()
    wall = time.monotonic() - started
    telemetry_report = telemetry.summary()
    scheduler_report = scheduler_monitor.summary()
    expected_ids = set(references)
    duplicate_count = len(observed_ids) - len(set(observed_ids))
    task_set_complete = set(observed_ids) == expected_ids and duplicate_count == 0
    comparison = _scientific_comparison(
        outputs,
        {
            key: {
                field: value
                for field, value in reference.items()
                if field in {"mean", "median", "quantiles"}
            }
            for key, reference in references.items()
        },
    )
    exact_hashes = {
        key: json_fingerprint(
            {"mean": value["mean"], "quantiles": value["quantiles"]}
        )
        for key, value in sorted(outputs.items())
    }
    exact_mismatches = sorted(
        key
        for key, value in exact_hashes.items()
        if value != references[key]["content_hash"]
    )
    completed_times = sorted(
        item["completed_at_seconds"] for item in batch_measurements
    )
    return {
        "repetition": repetition,
        "complete": failure is None and task_set_complete,
        "failure": failure,
        "early_stop_reason": unsafe_reasons[0] if unsafe_reasons else None,
        "gpu_stage_wall_seconds": wall,
        "tasks_per_second": len(observed_ids) / wall if wall else 0.0,
        "completed_task_count": len(observed_ids),
        "unique_task_count": len(set(observed_ids)),
        "duplicate_task_count": duplicate_count,
        "missing_task_count": len(expected_ids - set(observed_ids)),
        "batch_count": len(batch_measurements),
        "effective_batch_sizes": [
            item["effective_batch_size"] for item in batch_measurements
        ],
        "maximum_in_flight_batches": maximum_in_flight,
        "dask_retry_count": 0,
        "batch_measurements": batch_measurements,
        "batch_completion_gap_seconds": [
            right - left for left, right in zip(completed_times, completed_times[1:])
        ],
        "worker_contribution": dict(sorted(per_worker.items())),
        "all_expected_workers_contributed": set(per_worker) == set(workers)
        and all(item["completed_tasks"] > 0 for item in per_worker.values()),
        "model_instance_count": len(model_instances),
        "model_instances": dict(sorted(model_instances.items())),
        "scientific_comparison": comparison,
        "exact_output_fingerprint": json_fingerprint(exact_hashes),
        "exact_content_hash_mismatch_count": len(exact_mismatches),
        "exact_content_hash_mismatch_task_ids": exact_mismatches,
        "telemetry": telemetry_report,
        "scheduler_safety": scheduler_report,
        "resource_safe": telemetry_report["passed"] and scheduler_report["passed"],
    }


def _configuration_decisions(
    settings: GpuCalibrationSettings,
    measurements: list[dict[str, Any]],
) -> dict[str, Any]:
    scientific = all(
        item["complete"]
        and item["scientific_comparison"]["equivalent"]
        and item["completed_task_count"] == EXPECTED_TASKS
        and item["unique_task_count"] == EXPECTED_TASKS
        and item["duplicate_task_count"] == 0
        for item in measurements
    )
    safe = all(item["resource_safe"] for item in measurements)
    all_workers = all(item["all_expected_workers_contributed"] for item in measurements)
    return {
        "scientifically_equivalent": scientific,
        "resource_safe": safe,
        "all_logical_gpu_workers_contributed": all_workers,
        "expected_gpu_worker_processes": settings.gpu_worker_processes,
    }


def _run_configuration(
    label: str,
    settings: GpuCalibrationSettings,
    jobs: list[dict[str, Any]],
    references: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    settings.validate()
    cluster = _GpuCluster(label, settings)
    report: dict[str, Any] = {
        "label": label,
        "settings": asdict(settings),
        "physical_gpu_count": settings.physical_gpu_count,
        "gpu_worker_processes": settings.gpu_worker_processes,
        "worker_memory_budget": {
            "per_process_configured_ceiling_gib": settings.worker_memory_limit_gib,
            "total_configured_ceiling_gib": (
                settings.worker_memory_limit_gib * settings.gpu_worker_processes
            ),
            "rationale": (
                "4 GiB per logical worker is about 2.35 times the observed "
                "approximately 1.7 GB one-worker process footprint; the "
                "15-worker candidate declares 60 GiB and preserves more than "
                "16 GiB headroom on the 128 GB Ubuntu host."
            ),
        },
        "measurements": [],
    }
    client: Client | None = None
    try:
        report["preflight"] = cluster.preflight()
        client, workers = cluster.start()
        addresses = sorted(workers)
        batches = _length_aware_batches(jobs, settings.chronos_batch_size)
        report["registered_workers"] = workers
        report["total_batch_count"] = len(batches)
        report["batch_sizes"] = [len(batch) for batch in batches]
        repetitions = ["cold_start", *[f"warm_{index}" for index in range(1, 4)]]
        for repetition in repetitions:
            measurement = _run_repetition(
                client,
                settings,
                addresses,
                batches,
                references,
                repetition,
                cluster,
            )
            report["measurements"].append(measurement)
            if measurement["failure"] or not measurement["resource_safe"]:
                break
        report["cold_start"] = report["measurements"][0]
        warm = [
            item for item in report["measurements"] if item["repetition"].startswith("warm_")
        ]
        report["warm_repetitions"] = warm
        report["warm_median_tasks_per_second"] = (
            statistics.median(item["tasks_per_second"] for item in warm)
            if len(warm) == WARM_REPETITIONS
            else None
        )
        report["warm_median_wall_seconds"] = (
            statistics.median(item["gpu_stage_wall_seconds"] for item in warm)
            if len(warm) == WARM_REPETITIONS
            else None
        )
        report["decisions"] = _configuration_decisions(settings, report["measurements"])
    except BaseException as error:
        report["failure"] = f"{type(error).__name__}: {error}"
        report["decisions"] = {
            "scientifically_equivalent": False,
            "resource_safe": False,
            "all_logical_gpu_workers_contributed": False,
            "expected_gpu_worker_processes": settings.gpu_worker_processes,
        }
    finally:
        if client is not None:
            try:
                client.close(timeout=5)
            except BaseException:
                pass
        cluster.stop()
        try:
            report["shutdown"] = cluster.confirm_stopped()
        except BaseException as error:
            report["shutdown"] = {
                "confirmed": False,
                "error": f"{type(error).__name__}: {error}",
            }
    return report


def _environment_identity(root: Path) -> dict[str, Any]:
    config = json.loads(
        (root / "config/experiments/poc1.json").read_text(encoding="utf-8")
    )
    return {
        "git_revision": _git("rev-parse", "HEAD"),
        "gift_eval_revision": _git(
            "-C", "external/gift-eval", "rev-parse", "HEAD"
        ),
        "scientific_configuration_hash": json_fingerprint(
            scientific_configuration(config)
        ),
        "model_repository": config["models"]["chronos_2"]["repository"],
        "model_revision": config["models"]["chronos_2"]["revision"],
        "python": sys.version,
    }


def run_experiment(database: Path, acceptance_report: Path, output: Path) -> dict[str, Any]:
    database = database.resolve()
    acceptance_report = acceptance_report.resolve()
    output = output.resolve()
    if database != DEFAULT_DATABASE.resolve() or acceptance_report != DEFAULT_ACCEPTANCE_REPORT.resolve():
        raise ValueError("GPU calibration must read the accepted POC2 database and report")
    jobs, references, workload = _load_workload(database, acceptance_report)
    report: dict[str, Any] = {
        "experiment": "poc2_gpu_concurrency_1_vs_15",
        "accepted_baseline_revision": ACCEPTED_REVISION,
        "generated_at_unix_seconds": time.time(),
        "command": [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:]],
        "accepted_database_opened_read_only": True,
        "accepted_report_opened_read_only": True,
        "environment": _environment_identity(ROOT),
        "workload": workload,
        "physical_gpu_count": 1,
        "configurations": {},
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    control = _run_configuration("control_1_worker", control_settings(), jobs, references)
    report["configurations"]["control"] = control
    output.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    if not control["decisions"]["scientifically_equivalent"] or not control["decisions"][
        "resource_safe"
    ]:
        report["early_stop_reason"] = "one-worker control failed"
    else:
        candidate = _run_configuration(
            "candidate_15_workers", candidate_settings(), jobs, references
        )
        report["configurations"]["candidate"] = candidate
        control_rate = control["warm_median_tasks_per_second"]
        candidate_rate = candidate.get("warm_median_tasks_per_second")
        faster = bool(
            control_rate
            and candidate_rate
            and candidate_rate >= control_rate * 1.05
        )
        scientifically_equivalent = candidate["decisions"][
            "scientifically_equivalent"
        ]
        resource_safe = candidate["decisions"]["resource_safe"]
        recommended = scientifically_equivalent and resource_safe and faster
        control_utilisation = statistics.median(
            item["telemetry"]["gpu_utilisation_percent"]["mean"]
            for item in control["warm_repetitions"]
        )
        candidate_utilisation = (
            statistics.median(
                item["telemetry"]["gpu_utilisation_percent"]["mean"]
                for item in candidate["warm_repetitions"]
            )
            if len(candidate["warm_repetitions"]) == WARM_REPETITIONS
            else None
        )
        utilisation_increased = bool(
            candidate_utilisation is not None
            and candidate_utilisation > control_utilisation
        )
        report["comparison"] = {
            "control_warm_median_tasks_per_second": control_rate,
            "candidate_warm_median_tasks_per_second": candidate_rate,
            "throughput_ratio": (
                candidate_rate / control_rate
                if control_rate and candidate_rate is not None
                else None
            ),
            "meaningful_improvement_threshold": 0.05,
            "control_warm_median_gpu_utilisation_percent": control_utilisation,
            "candidate_warm_median_gpu_utilisation_percent": candidate_utilisation,
            "gpu_utilisation_increased": utilisation_increased,
        }
        report["decisions"] = {
            "scientifically_equivalent": scientifically_equivalent,
            "resource_safe": resource_safe,
            "faster_than_control": faster,
            "recommended_for_adoption": recommended,
        }
        if recommended and utilisation_increased:
            report["conclusion"] = (
                "The 15-logical-worker theory was supported for utilisation, "
                "throughput, scientific equivalence, and resource safety."
            )
        elif recommended:
            report["conclusion"] = (
                "The 15-logical-worker theory was supported for throughput, "
                "scientific equivalence, and resource safety, but sampled GPU "
                "utilisation did not increase."
            )
        else:
            report["conclusion"] = (
                "The 15-logical-worker theory was not supported for adoption."
            )
    report["report_sha256_without_this_field"] = hashlib.sha256(
        json.dumps(report, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    output.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument(
        "--acceptance-report", type=Path, default=DEFAULT_ACCEPTANCE_REPORT
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    try:
        report = run_experiment(args.database, args.acceptance_report, args.output)
    except BaseException as error:
        print(f"error: {type(error).__name__}: {error}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "output": str(args.output.resolve()),
                "decisions": report.get("decisions"),
                "conclusion": report.get("conclusion"),
            },
            indent=2,
        )
    )
    return 0 if report.get("decisions", {}).get("resource_safe") else 1


if __name__ == "__main__":
    raise SystemExit(main())
