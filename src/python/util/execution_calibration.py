# ==============================================================================
# execution_calibration.py
#
# Purpose: Controlled local calibration using an isolated temporary DuckDB database.
# Inputs: Execution profile, representative contexts, temporary database data, and optional Dask client.
# Outputs: Timing, memory, scientific-equivalence, and recommended worker/batch calibration evidence.
# Run from: Imported; not run directly.
# ==============================================================================

"""Controlled local calibration using an isolated temporary DuckDB database."""

from __future__ import annotations

import json
import math
import os
import platform
import resource
import socket
import subprocess
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import duckdb

from .database import DEFAULT_DATABASE, load_database_configuration
from .execution_profiles import (
    GIB,
    ExecutionProfile,
    PersistentChronosWorker,
    system_memory,
)
from .import_execution import repository_root


# Test/calibration value: developer-owned search grids; calibration code changes them,
# and they never override experiment or recorded execution settings.
CALIBRATION_CANDIDATES = {
    "mac_m1pro_10core_16gb": {
        "cpu_workers": [1, 2, 4],
        "chronos_batch_sizes": [1, 2, 4, 8, 16],
    },
    "ubuntu_3950x_16core_128gb_rtx5090": {
        "cpu_workers": [1, 4, 8, 12, 16],
        "chronos_batch_sizes": [8, 16, 32, 64],
    },
}
class _MemorySampler:
    """Purpose: Own periodic host-memory sampling for one calibration block. Inputs: Construction accepts a polling interval in seconds. Outputs: Raw snapshots and an aggregate summary; owns mutable samples, a stop event, and a daemon thread from context entry through exit."""
    def __init__(self, interval_seconds: float = 0.25):
        """Purpose: Initialize one unused memory sampler. Inputs: ``interval_seconds`` is the positive polling period in seconds. Outputs: None; creates empty sample state, a stop event, and no thread or probes yet."""
        self.interval_seconds = interval_seconds
        self.samples: list[dict[str, int]] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _sample(self) -> None:
        """Purpose: Capture current host memory. Inputs: None; data comes from ``system_memory``. Outputs: None; appends one byte-valued RAM/swap mapping to owned sample state."""
        self.samples.append(system_memory())

    def _run(self) -> None:
        """Purpose: Drive periodic memory sampling. Inputs: Owned interval and stop event. Outputs: None; blocks the daemon thread and appends snapshots until stop is signaled."""
        while not self._stop.wait(self.interval_seconds):
            self._sample()

    def __enter__(self) -> "_MemorySampler":
        """Capture the baseline and start the daemon sampling thread."""
        self._sample()
        self._thread = threading.Thread(
            target=self._run,
            name="shapefm-calibration-memory",
            daemon=True,
        )
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        """Stop the sampler thread and capture a final memory snapshot."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        self._sample()

    def summary(self) -> dict[str, Any]:
        """Purpose: Aggregate the sampler's captured host state. Inputs: Owned memory snapshots ordered by capture time. Outputs: First/last mappings, minimum available RAM bytes, maximum used swap bytes, and count; raises if no samples exist and performs no new probe."""
        if not self.samples:
            raise RuntimeError("calibration memory sampler captured no samples")
        swap_used = [
            max(0, sample.get("swap_total_bytes", 0) - sample.get("swap_free_bytes", 0))
            for sample in self.samples
        ]
        return {
            "system_memory_before": self.samples[0],
            "system_memory_after": self.samples[-1],
            "minimum_system_memory_available_bytes": min(
                sample.get("available_bytes", 0) for sample in self.samples
            ),
            "maximum_swap_used_bytes": max(swap_used, default=0),
            "memory_sample_count": len(self.samples),
        }


def _system_memory_rejection(
    profile: ExecutionProfile, memory: dict[str, Any]
) -> str | None:
    """Purpose: Evaluate host-memory calibration safety. Inputs: An ``ExecutionProfile`` carrying an available-memory floor in GiB and a sampler summary containing minimum available bytes. Outputs: A human-readable rejection reason for unavailable/below-threshold memory, otherwise ``None``; has no side effects."""
    available = memory["minimum_system_memory_available_bytes"]
    threshold = int(profile.system_memory_min_available_gib * GIB)
    if available <= 0:
        return "system available memory could not be measured"
    if available < threshold:
        return (
            f"system memory safety threshold reached: {available / GIB:.2f} GiB "
            f"minimum available, {profile.system_memory_min_available_gib:.2f} GiB required"
        )
    return None


def _accelerator_memory_rejection(
    profile: ExecutionProfile, available_values: list[int]
) -> str | None:
    """Purpose: Evaluate accelerator-memory calibration safety. Inputs: An ``ExecutionProfile`` carrying a GiB floor and sampled available-memory integers in bytes. Outputs: A rejection reason when samples are absent or their minimum is below the floor, otherwise ``None``; has no side effects."""
    if not available_values:
        return "accelerator available memory could not be measured"
    available = min(available_values)
    threshold = int(profile.accelerator_memory_min_available_gib * GIB)
    if available < threshold:
        return (
            f"accelerator memory safety threshold reached: {available / GIB:.2f} GiB "
            f"minimum available, {profile.accelerator_memory_min_available_gib:.2f} GiB required"
        )
    return None


def _recommended_setting(
    measurements: list[dict[str, Any]], kind: str, setting: str, equivalence: str
) -> int | None:
    """Purpose: Select a safe calibration setting. Inputs: Measurement mappings, required ``kind``, integer setting-field name, and boolean equivalence-field name. Outputs: The setting from the highest tasks/second safe, successful, equivalent candidate, preferring smaller settings on ties, or ``None``; does not mutate measurements."""
    accepted = [
        item
        for item in measurements
        if item["kind"] == kind
        and item["safe"]
        and not item["failure"]
        and item[equivalence]
    ]
    if not accepted:
        return None
    return max(accepted, key=lambda item: item["tasks_per_second"])[setting]


def _forecast_comparison(first: dict[str, Any], second: dict[str, Any]) -> dict[str, Any]:
    """Purpose: Compare two forecast payloads numerically. Inputs: Forecast mappings with equal-shape mean, median, and nested quantile sequences of numeric horizon values. Outputs: Maximum absolute/relative differences and equivalence at 1e-5 relative/absolute tolerances; shape mismatch returns infinite differences and false."""
    first_values = first["mean"] + first["median"] + sum(first["quantiles"], [])
    second_values = second["mean"] + second["median"] + sum(second["quantiles"], [])
    if len(first_values) != len(second_values):
        return {"max_abs": float("inf"), "max_relative": float("inf"), "equivalent": False}
    pairs = [(float(left), float(right)) for left, right in zip(first_values, second_values, strict=True)]
    return {
        "max_abs": max((abs(left - right) for left, right in pairs), default=0.0),
        "max_relative": max(
            (abs(left - right) / max(abs(left), 1e-12) for left, right in pairs),
            default=0.0,
        ),
        "equivalent": all(
            math.isclose(left, right, rel_tol=1e-5, abs_tol=1e-5)
            for left, right in pairs
        ),
    }


def _chronos_context_responses(
    worker: PersistentChronosWorker,
    contexts: list[dict[str, Any]],
    batch_size: int,
    horizon: int,
    quantile_levels: list[float],
) -> list[dict[str, Any]]:
    """Purpose: Measure Chronos inference across representative contexts. Inputs: A running ``PersistentChronosWorker``; context mappings with labels and one-dimensional numeric series; positive batch size/horizon; and quantile levels in [0,1]. Outputs: Ordered protocol response mappings, stopping after an error; sends one repeated-job request per context and advances worker/process state."""
    responses = []
    for context in contexts:
        jobs = [
            {
                "id": f"chronos-{batch_size}-{context['label']}-{index}",
                "context": context["context"],
            }
            for index in range(batch_size)
        ]
        response = worker.request(
            {
                "command": "predict",
                "batch_id": f"calibration/{uuid.uuid4().hex}",
                "jobs": jobs,
                "horizon": horizon,
                "quantile_levels": quantile_levels,
                "inference_batch_size": batch_size,
            }
        )
        responses.append(response)
        if response.get("type") == "error":
            break
    return responses


def _chronos_reference_forecasts(
    worker: PersistentChronosWorker,
    contexts: list[dict[str, Any]],
    horizon: int,
    quantile_levels: list[float],
) -> dict[str, dict[str, Any]]:
    """Purpose: Generate batch-size-one Chronos references. Inputs: A running persistent worker, labeled numeric context mappings, positive horizon, and quantile levels in [0,1]. Outputs: Forecast mappings keyed by context label; sends subprocess requests and raises unless every context returns exactly one result."""
    responses = _chronos_context_responses(
        worker, contexts, batch_size=1, horizon=horizon, quantile_levels=quantile_levels
    )
    if len(responses) != len(contexts):
        raise RuntimeError("Chronos batch-size-1 reference did not cover every context")
    references = {}
    for context, response in zip(contexts, responses, strict=True):
        if response.get("type") != "result" or len(response.get("results", [])) != 1:
            raise RuntimeError(f"Chronos batch-size-1 reference failed: {response}")
        references[context["label"]] = response["results"][0]
    return references


def _chronos_differences(
    contexts: list[dict[str, Any]],
    responses: list[dict[str, Any]],
    references: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Purpose: Compare batched Chronos forecasts with references. Inputs: Ordered context mappings, corresponding successful response mappings, and references keyed by label. Outputs: One numeric comparison mapping per zipped context/response pair; performs no worker requests or mutation."""
    return [
        _forecast_comparison(references[context["label"]], response["results"][0])
        for context, response in zip(contexts, responses, strict=True)
    ]


def representative_contexts(database_path: Path) -> list[dict[str, Any]]:
    """Purpose: Select representative local-calibration series. Inputs: ``database_path`` is a configured DuckDB file containing M4 Daily series and evaluation windows. Outputs: Three mappings labeled short/median/long with series ID, one-dimensional numeric training context, and observation length; opens the canonical database read-only and closes it, raising unless all ranks exist."""
    configuration = load_database_configuration(database_path)
    dataset_name = configuration.resolved["data"]["dataset_name"]
    connection = duckdb.connect(str(database_path.resolve()), read_only=True)
    try:
        rows = connection.execute(
            """WITH ranked AS (
                SELECT s.series_id, s.target, w.test_start,
                       row_number() OVER (ORDER BY s.observation_count, s.series_id) AS position,
                       count(*) OVER () AS total
                FROM series s JOIN evaluation_windows w USING (dataset_id, series_id)
                JOIN datasets d USING (dataset_id)
                WHERE d.dataset_name=?
            )
            SELECT series_id, target[:test_start], test_start
            FROM ranked WHERE position IN (1, CAST(ceil(total / 2.0) AS INTEGER), total)
            ORDER BY position""",
            [dataset_name],
        ).fetchall()
    finally:
        connection.close()
    if len(rows) != 3:
        raise RuntimeError("calibration requires short, median, and long M4 Daily contexts")
    labels = ("short", "median", "long")
    return [
        {"label": label, "series_id": row[0], "context": row[1], "length": row[2]}
        for label, row in zip(labels, rows, strict=True)
    ]


def _r_forecast(
    root: Path, job: dict[str, Any], settings: dict[str, Any]
) -> dict[str, Any]:
    """Purpose: Execute one calibration AutoARIMA forecast. Inputs: ``root`` is the repository directory, ``job`` is a JSON-serializable context/horizon/seasonality mapping, and ``settings`` is resolved model configuration. Outputs: Decoded JSON response; launches one R subprocess with single-thread limits, repository cwd, and a 1,800-second timeout."""
    completed = subprocess.run(
        ["Rscript", str(root / "src/r/04_01_forecast_auto_arima.R")],
        cwd=root,
        input=json.dumps({"action": "forecast", "settings": settings, "jobs": [job]}),
        check=True,
        capture_output=True,
        text=True,
        timeout=1800,
        env={
            **os.environ,
            "OMP_NUM_THREADS": "1",
            "OPENBLAS_NUM_THREADS": "1",
            "RENV_CONFIG_SYNCHRONIZED_CHECK": "false",
        },
    )
    return json.loads(completed.stdout)


def _resolved_r_period(root: Path, configuration) -> int:
    """Resolve the configured R period through the pinned GIFT-Eval bridge.

    Purpose: Keep calibration consistent with experiment planning without adding
    GluonTS to the lean coordinator environment or duplicating its frequency map.
    Inputs: Repository root and a validated ``ExperimentConfiguration``.
    Outputs: Positive integer period; launches one short pinned-environment process.
    """
    paths = configuration.resolved
    command = [
        str(root / paths["evaluation"]["gift_eval"]["environment"] / "bin/python"),
        str(root / "src/python/06_01_evaluate_gift_eval.py"),
        "resolve-period",
        "--frequency",
        paths["data"]["benchmark"]["frequency"],
    ]
    if configuration.r_period_override is not None:
        command.extend(["--override", str(configuration.r_period_override)])
    completed = subprocess.run(
        command,
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return int(json.loads(completed.stdout)["r_period"])


def _peak_children_memory_bytes() -> int:
    """Purpose: Probe cumulative child-process peak resident memory. Inputs: None; data comes from ``RUSAGE_CHILDREN`` and is bytes on macOS or KiB elsewhere. Outputs: Peak RSS normalized to bytes; samples process resource state without resetting the cumulative counter."""
    value = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    return int(value if platform.system() == "Darwin" else value * 1024)


def calibrate(
    profile: ExecutionProfile,
    hardware: dict[str, Any],
    database_path: Path = DEFAULT_DATABASE,
    output: Path | None = None,
) -> dict[str, Any]:
    """Purpose: Calibrate local AutoARIMA concurrency and Chronos batch size. Inputs: A supported ``ExecutionProfile``, caller-supplied hardware snapshot, configured canonical DuckDB path, and optional report ``Path``. Outputs: Timing, byte-valued memory, safety/equivalence measurements and recommendations; reads canonical data, launches R/Chronos subprocesses, samples host/accelerator state, writes only an isolated temporary DuckDB, removes it, and optionally creates the JSON report."""
    if profile.name not in CALIBRATION_CANDIDATES:
        raise ValueError(f"no calibration grid is defined for {profile.name}")
    root = repository_root()
    configuration = load_database_configuration(database_path)
    config = configuration.resolved
    execution = configuration.execution
    paths = configuration.execution_paths
    benchmark = config["data"]["benchmark"]
    r_period = _resolved_r_period(root, configuration)
    contexts = representative_contexts(database_path)
    candidates = CALIBRATION_CANDIDATES[profile.name]
    started = time.monotonic()
    measurements = []
    stopped_early_reasons = []
    with tempfile.TemporaryDirectory(prefix="shapefm-calibration-") as directory:
        calibration_database = Path(directory) / "calibration.duckdb"
        connection = duckdb.connect(str(calibration_database))
        connection.execute(
            "CREATE TABLE measurements (kind VARCHAR, setting INTEGER, measurement JSON)"
        )
        for workers in candidates["cpu_workers"]:
            jobs = [
                {
                    "id": f"auto-{index}",
                    "context": contexts[index % 3]["context"],
                    "horizon": benchmark["prediction_length"],
                    "seasonality": r_period,
                }
                for index in range(12)
            ]
            run_started = time.monotonic()
            failure = None
            outputs = []
            preflight_rejection = None
            with _MemorySampler() as memory_sampler:
                preflight_rejection = _system_memory_rejection(
                    profile, memory_sampler.summary()
                )
                if preflight_rejection is None:
                    try:
                        with ThreadPoolExecutor(max_workers=workers) as executor:
                            outputs = list(
                                executor.map(
                                    lambda job: _r_forecast(
                                        root,
                                        job,
                                        config["models"]["auto_arima"]["settings"],
                                    ),
                                    jobs,
                                )
                            )
                    except BaseException as error:
                        failure = f"{type(error).__name__}: {error}"
            elapsed = time.monotonic() - run_started
            memory = memory_sampler.summary()
            safety_rejection = preflight_rejection or _system_memory_rejection(
                profile, memory
            )
            differences = []
            if not failure and outputs:
                for context_index in range(3):
                    forecasts = [
                        output["results"][0]
                        for index, output in enumerate(outputs)
                        if index % 3 == context_index
                    ]
                    differences.extend(
                        _forecast_comparison(forecasts[0], forecast)
                        for forecast in forecasts[1:]
                    )
            max_difference = max((item["max_abs"] for item in differences), default=0.0)
            max_relative = max((item["max_relative"] for item in differences), default=0.0)
            measurement = {
                "kind": "autoarima",
                "workers": workers,
                "task_count": len(jobs) if outputs else 0,
                "wall_clock_seconds": elapsed,
                "tasks_per_second": len(jobs) / elapsed
                if not failure and preflight_rejection is None
                else 0.0,
                "observed_child_process_peak_rss_bytes": _peak_children_memory_bytes(),
                "memory_metric_scope": "maximum RSS observed for one child process, not aggregate concurrent RSS",
                **memory,
                "safe": safety_rejection is None,
                "safety_rejection_reason": safety_rejection,
                "failure": failure,
                "retries": 0,
                "repeat_max_abs_difference": max_difference,
                "repeat_max_relative_difference": max_relative,
                "strictly_equivalent": not failure and all(
                    item["equivalent"] for item in differences
                ),
                "equivalence_relative_tolerance": 1e-5,
                "equivalence_absolute_tolerance": 1e-5,
            }
            measurements.append(measurement)
            connection.execute(
                "INSERT INTO measurements VALUES ('autoarima', ?, ?)",
                [workers, json.dumps(measurement)],
            )
            if safety_rejection:
                stopped_early_reasons.append(
                    f"AutoARIMA workers={workers}: {safety_rejection}"
                )
                break
            if failure:
                stopped_early_reasons.append(
                    f"AutoARIMA workers={workers}: {failure}"
                )
                break

        chronos = config["models"]["chronos_2"]
        command = [
            str(root / "environments/chronos-2/.venv/bin/python"),
            str(root / "src/python/04_02_forecast_chronos.py"),
            "serve",
            "--model",
            chronos["repository"],
            "--revision",
            chronos["revision"],
            "--device",
            profile.required_accelerator or "auto",
        ]
        with PersistentChronosWorker(command) as worker:
            chronos_reference = _chronos_reference_forecasts(
                worker,
                contexts,
                benchmark["prediction_length"],
                chronos["quantile_levels"],
            )
            for batch_size in candidates["chronos_batch_sizes"]:
                responses = []
                failure = None
                candidate_started = time.monotonic()
                preflight_rejection = None
                with _MemorySampler() as memory_sampler:
                    preflight_rejection = _system_memory_rejection(
                        profile, memory_sampler.summary()
                    )
                    if preflight_rejection is None:
                        responses = _chronos_context_responses(
                            worker,
                            contexts,
                            batch_size,
                            benchmark["prediction_length"],
                            chronos["quantile_levels"],
                        )
                        if responses and responses[-1].get("type") == "error":
                            failure = responses[-1].get("error")
                wall_clock = time.monotonic() - candidate_started
                memory = memory_sampler.summary()
                elapsed = sum(response.get("inference_seconds", 0.0) for response in responses)
                task_count = batch_size * len(responses) if not failure else 0
                memory_values = [
                    response.get("peak_process_memory_bytes")
                    for response in responses
                    if response.get("peak_process_memory_bytes") is not None
                ]
                last_response = responses[-1] if responses else None
                accelerator_memories = [
                    worker.ready.get("accelerator_memory") or {}
                ] + [response.get("accelerator_memory") or {} for response in responses]
                accelerator_available = [
                    value
                    for value in (
                        item.get("available_bytes") for item in accelerator_memories
                    )
                    if value is not None
                ]
                safety_rejections = [
                    rejection
                    for rejection in (
                        preflight_rejection or _system_memory_rejection(profile, memory),
                        _accelerator_memory_rejection(profile, accelerator_available),
                    )
                    if rejection
                ]
                safety_rejection = "; ".join(safety_rejections) or None
                differences = (
                    _chronos_differences(contexts, responses, chronos_reference)
                    if not failure and responses
                    else []
                )
                max_difference = max((item["max_abs"] for item in differences), default=0.0)
                max_relative = max((item["max_relative"] for item in differences), default=0.0)
                measurement = {
                    "kind": "chronos_2",
                    "batch_size": batch_size,
                    "length_groups": [context["label"] for context in contexts[:len(responses)]],
                    "task_count": task_count,
                    "model_loading_seconds": worker.ready["model_load_seconds"],
                    "model_load_count": worker.ready["model_load_count"],
                    "inference_seconds": elapsed,
                    "wall_clock_seconds": wall_clock,
                    "tasks_per_second": task_count / elapsed if elapsed and not failure else 0.0,
                    "observed_worker_peak_rss_bytes": max(memory_values) if memory_values else None,
                    "memory_metric_scope": "peak RSS reported by the one persistent Chronos worker",
                    **memory,
                    "minimum_accelerator_memory_available_bytes": min(
                        accelerator_available, default=None
                    ),
                    "accelerator_memory_after": (
                        last_response.get("accelerator_memory")
                        if last_response
                        else worker.ready.get("accelerator_memory")
                    ),
                    "safe": safety_rejection is None,
                    "safety_rejection_reason": safety_rejection,
                    "failure": failure,
                    "retries": 0,
                    "effective_batch_size": (
                        last_response.get("effective_batch_size") if last_response else None
                    ),
                    "max_abs_difference_from_batch_one": max_difference,
                    "max_relative_difference_from_batch_one": max_relative,
                    "strictly_equivalent_to_batch_one": not failure and all(
                        item["equivalent"] for item in differences
                    ),
                    "equivalence_relative_tolerance": 1e-5,
                    "equivalence_absolute_tolerance": 1e-5,
                }
                measurements.append(measurement)
                connection.execute(
                    "INSERT INTO measurements VALUES ('chronos_2', ?, ?)",
                    [batch_size, json.dumps(measurement)],
                )
                if safety_rejection:
                    stopped_early_reasons.append(
                        f"Chronos batch_size={batch_size}: {safety_rejection}"
                    )
                    break
                if failure:
                    stopped_early_reasons.append(
                        f"Chronos batch_size={batch_size}: {failure}"
                    )
                    break
        isolated_rows = connection.execute("SELECT count(*) FROM measurements").fetchone()[0]
        connection.close()

    report = {
        "profile": profile.to_dict(),
        "hardware": hardware,
        "representative_contexts": [
            {key: value for key, value in item.items() if key != "context"}
            for item in contexts
        ],
        "measurements": measurements,
        "recommendation": {
            "autoarima_workers": _recommended_setting(
                measurements, "autoarima", "workers", "strictly_equivalent"
            ),
            "chronos_inference_batch_size": _recommended_setting(
                measurements,
                "chronos_2",
                "batch_size",
                "strictly_equivalent_to_batch_one",
            ),
            "applied_to_committed_profile": False,
        },
        "stopped_early_reason": "; ".join(stopped_early_reasons) or None,
        "isolated_database_measurement_count": isolated_rows,
        "total_wall_clock_seconds": time.monotonic() - started,
    }
    if output is not None:
        output = output.resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


class _DistributedResourceSampler:
    """Purpose: Own periodic coordinator and Dask-worker telemetry for one calibration run. Inputs: Construction accepts a distributed client and polling interval in seconds. Outputs: Raw samples/failures and aggregate stability/resource evidence; owns mutable state, a stop event, and daemon thread but not the caller-owned client."""
    def __init__(self, client: Any, interval_seconds: float = 1.0):
        """Purpose: Initialize an unused distributed sampler. Inputs: ``client`` is a connected Dask Client-like object and ``interval_seconds`` is a positive polling period. Outputs: None; retains but does not own/close the client and creates empty sample/failure state plus thread controls."""
        self.client = client
        self.interval_seconds = interval_seconds
        self.samples: list[dict[str, Any]] = []
        self.failures: list[str] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _sample(self) -> None:
        """Purpose: Capture one distributed resource observation. Inputs: The retained Dask client and current coordinator/worker state. Outputs: None; remotely invokes worker probes, samples coordinator CPU/RAM/swap, records collection seconds, and appends the combined mapping."""
        import psutil

        from .distributed_execution import worker_resource_snapshot

        started = time.monotonic()
        workers = self.client.run(worker_resource_snapshot)
        memory = psutil.virtual_memory()
        swap = psutil.swap_memory()
        self.samples.append(
            {
                "sample_seconds": time.monotonic() - started,
                "coordinator": {
                    "hostname": socket.gethostname(),
                    "cpu_percent": psutil.cpu_percent(interval=None),
                    "system_available_memory_bytes": int(memory.available),
                    "swap_used_bytes": int(swap.used),
                },
                "workers": workers,
            }
        )

    def _run(self) -> None:
        """Purpose: Drive periodic distributed telemetry. Inputs: Owned interval, stop event, and client. Outputs: None; appends samples until stopped and converts any probe exception into retained failure text rather than terminating the daemon thread."""
        while not self._stop.wait(self.interval_seconds):
            try:
                self._sample()
            except BaseException as error:
                self.failures.append(f"{type(error).__name__}: {error}")

    def __enter__(self) -> "_DistributedResourceSampler":
        """Capture baseline state and start periodic distributed sampling."""
        self._sample()
        self._thread = threading.Thread(
            target=self._run,
            name="shapefm-distributed-calibration-telemetry",
            daemon=True,
        )
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        """Stop periodic collection and capture final cluster state."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        try:
            self._sample()
        except BaseException as error:
            self.failures.append(f"{type(error).__name__}: {error}")

    def summary(self, initial_workers: set[str], final_workers: set[str]) -> dict[str, Any]:
        """Purpose: Aggregate distributed calibration telemetry. Inputs: Sets of worker-address strings sampled before and after the run plus owned timestamped host/worker snapshots. Outputs: Per-host CPU percentages and byte memory extrema, GPU percentages/bytes/name, spill bytes/persistence, sampling failures/latency, worker sets, and restart count; does not perform new probes."""
        hosts: dict[str, dict[str, list[float]]] = {}
        observed_workers: set[str] = set()
        gpu_samples: list[dict[str, Any]] = []
        spill_peak = 0
        final_spill = 0
        for sample_index, sample in enumerate(self.samples):
            values = [sample["coordinator"], *sample["workers"].values()]
            for item in values:
                host = hosts.setdefault(
                    item["hostname"],
                    {"cpu": [], "available": [], "swap": []},
                )
                host["cpu"].append(float(item["cpu_percent"]))
                host["available"].append(
                    float(item["system_available_memory_bytes"])
                )
                host["swap"].append(float(item["swap_used_bytes"]))
                if item.get("worker"):
                    observed_workers.add(item["worker"])
                    spilled = item["dask_spilled_memory_bytes"] + item[
                        "dask_spilled_disk_bytes"
                    ]
                    spill_peak = max(spill_peak, spilled)
                    if sample_index == len(self.samples) - 1:
                        final_spill += spilled
                if item.get("gpu"):
                    gpu_samples.append(item["gpu"])
        return {
            "sample_count": len(self.samples),
            "sample_failures": self.failures,
            "maximum_sample_seconds": max(
                (item["sample_seconds"] for item in self.samples), default=None
            ),
            "hosts": {
                hostname: {
                    "mean_cpu_utilisation_percent": sum(values["cpu"])
                    / len(values["cpu"]),
                    "maximum_cpu_utilisation_percent": max(values["cpu"]),
                    "minimum_system_available_memory_bytes": int(
                        min(values["available"])
                    ),
                    "maximum_swap_used_bytes": int(max(values["swap"])),
                }
                for hostname, values in sorted(hosts.items())
            },
            "gpu": {
                "mean_utilisation_percent": (
                    sum(item["utilization_percent"] for item in gpu_samples)
                    / len(gpu_samples)
                    if gpu_samples
                    else None
                ),
                "maximum_utilisation_percent": max(
                    (item["utilization_percent"] for item in gpu_samples),
                    default=None,
                ),
                "minimum_available_memory_bytes": min(
                    (item["available_memory_bytes"] for item in gpu_samples),
                    default=None,
                ),
                "name": gpu_samples[0]["name"] if gpu_samples else None,
            },
            "dask_spilling": {
                "peak_bytes_per_worker": spill_peak,
                "final_total_bytes": final_spill,
                "persistent": final_spill > 0,
            },
            "initial_workers": sorted(initial_workers),
            "final_workers": sorted(final_workers),
            "observed_workers": sorted(observed_workers),
            "worker_restarts": len(observed_workers - initial_workers)
            + len(initial_workers - final_workers),
        }


def _distributed_contexts(database_path: Path, count: int = 256) -> list[dict[str, Any]]:
    """Purpose: Select length-stratified contexts for distributed calibration. Inputs: A configured canonical DuckDB ``Path`` and requested positive ``count`` (normally 256). Outputs: Equally ranked mappings with series ID, one-dimensional numeric training context, integer length, and short/medium/long label; opens the database read-only and raises when too few rows exist."""
    configuration = load_database_configuration(database_path)
    dataset_name = configuration.resolved["data"]["dataset_name"]
    connection = duckdb.connect(str(database_path.resolve()), read_only=True)
    try:
        rows = connection.execute(
            """SELECT s.series_id, s.target[:w.test_start], w.test_start
               FROM series s JOIN evaluation_windows w USING (dataset_id, series_id)
               JOIN datasets d USING (dataset_id)
               WHERE d.dataset_name=?
               ORDER BY s.observation_count, s.series_id""",
            [dataset_name],
        ).fetchall()
    finally:
        connection.close()
    if len(rows) < count:
        raise RuntimeError(
            f"distributed calibration requires {count} M4 Daily contexts; found {len(rows)}"
        )
    positions = [round(index * (len(rows) - 1) / (count - 1)) for index in range(count)]
    selected = [rows[position] for position in positions]
    third = count // 3
    return [
        {
            "series_id": row[0],
            "context": row[1],
            "length": int(row[2]),
            "length_group": "short"
            if index < third
            else "medium"
            if index < 2 * third
            else "long",
        }
        for index, row in enumerate(selected)
    ]


def _calibration_batches(
    jobs: list[dict[str, Any]], batch_size: int
) -> list[list[dict[str, Any]]]:
    """Purpose: Form length-aware Chronos calibration batches. Inputs: Job mappings with IDs and one-dimensional contexts plus a positive maximum ``batch_size``. Outputs: A list of nonempty job lists, grouped by context-length bit bucket and capped at the requested size; sorts/references jobs without mutating them."""
    grouped: dict[int, list[dict[str, Any]]] = {}
    for job in sorted(jobs, key=lambda item: (len(item["context"]), item["id"])):
        grouped.setdefault(max(1, len(job["context"])).bit_length(), []).append(job)
    return [
        batch
        for bucket in sorted(grouped)
        for offset in range(0, len(grouped[bucket]), batch_size)
        for batch in [grouped[bucket][offset : offset + batch_size]]
    ]


def _scientific_comparison(
    outputs: dict[str, dict[str, Any]], references: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    """Purpose: Evaluate distributed forecast equivalence and ID coverage. Inputs: Candidate and reference forecast mappings keyed by task ID, each containing mean/median/quantile arrays. Outputs: Equivalence, missing/unexpected counts, maximum absolute/relative differences, and fixed 1e-5 tolerances; performs no I/O or mutation."""
    missing = sorted(set(references) - set(outputs))
    unexpected = sorted(set(outputs) - set(references))
    differences = [
        _forecast_comparison(references[key], outputs[key])
        for key in sorted(set(outputs) & set(references))
    ]
    return {
        "equivalent": not missing
        and not unexpected
        and all(item["equivalent"] for item in differences),
        "missing_output_count": len(missing),
        "unexpected_output_count": len(unexpected),
        "max_abs_difference": max(
            (item["max_abs"] for item in differences), default=0.0
        ),
        "max_relative_difference": max(
            (item["max_relative"] for item in differences), default=0.0
        ),
        "relative_tolerance": 1e-5,
        "absolute_tolerance": 1e-5,
    }


def calibrate_dask_profile(
    *,
    database_path: Path,
    scheduler_address: str,
    expected_workers: int,
    profile_name: str,
    mac_cpu_workers: int,
    ubuntu_cpu_workers: int,
    chronos_batch_size: int,
    max_in_flight: int,
    output: Path,
    baseline: Path | None = None,
) -> dict[str, Any]:
    """Purpose: Calibrate an end-to-end Dask execution profile. Inputs: Canonical DuckDB path; scheduler address; positive worker, per-host CPU, Chronos batch, and in-flight counts; profile name; required output path; and optional baseline JSON path. Outputs: A report containing throughput, retries, failures, scientific equivalence, task contributions, sampled byte/percent telemetry, and safety verdict; opens canonical data read-only, creates/closes a Dask client, submits/retries pipeline tasks, launches remote R/Chronos work, creates then removes an isolated measurement DB, and creates the JSON report and parent directories."""
    from distributed import Client

    from .distributed_execution import (
        CHRONOS_GPU_RESOURCE,
        autoarima_batch,
        chronos_batch,
        clean_batch,
        run_batch_groups,
        run_batches,
        transform_batch,
    )

    root = repository_root()
    configuration = load_database_configuration(database_path)
    config = configuration.resolved
    execution = configuration.execution
    r_period = _resolved_r_period(root, configuration)
    contexts = _distributed_contexts(database_path)
    client = Client(scheduler_address, timeout="180s")
    client.wait_for_workers(expected_workers, timeout=180)
    initial_workers = set(client.scheduler_info()["workers"])
    started = time.monotonic()
    outputs: dict[str, dict[str, Any]] = {}
    model_counts = {"auto_arima": 0, "chronos_2": 0}
    host_contribution: dict[str, int] = {}
    retry_count = 0
    effective_batches: list[int] = []
    failure = None
    completed_tasks = 0
    model_elapsed = 0.0
    with tempfile.TemporaryDirectory(prefix="shapefm-dask-calibration-") as directory:
        isolated_database = Path(directory) / "calibration.duckdb"
        measurement_db = duckdb.connect(str(isolated_database))
        measurement_db.execute(
            "CREATE TABLE measurements (profile VARCHAR, measurement JSON)"
        )
        with _DistributedResourceSampler(client) as sampler:
            try:
                clean_jobs = [
                    {
                        "id": f"clean/{method}/{item['series_id']}",
                        "context": item["context"],
                        "mode": method,
                        "seasonality": r_period,
                        "official_frequency": config["data"]["benchmark"]["frequency"],
                        "series_id": item["series_id"],
                    }
                    for item in contexts
                    for method in config["pipeline"]["preprocessing"]["modes"]
                ]
                cleaned: dict[str, dict[str, Any]] = {}
                for batch, response in run_batches(
                    client,
                    clean_batch,
                    [clean_jobs[offset : offset + 8] for offset in range(0, len(clean_jobs), 8)],
                    resources={"CPU": 1},
                    max_in_flight=max_in_flight,
                    retries=2,
                    extra_arguments=(
                        paths["r_preprocess_worker"],
                        float(execution["worker_timeouts_seconds"]["r"]),
                        int(execution["thread_limits"]["r"]),
                    ),
                ):
                    expected = {job["id"] for job in batch}
                    results = {result["id"]: result for result in response["results"]}
                    if set(results) != expected or len(results) != len(response["results"]):
                        raise RuntimeError("calibration cleaning returned invalid task IDs")
                    for job in batch:
                        cleaned[job["id"]] = {
                            **job,
                            "values": results[job["id"]]["values"],
                        }
                    worker = response["worker"]
                    host_contribution[worker["hostname"]] = host_contribution.get(
                        worker["hostname"], 0
                    ) + len(batch)
                    retry_count += int(worker.get("retry_count", 0))
                    completed_tasks += len(batch)

                transform_jobs = [
                    {
                        "id": f"transform/{cleaned_job['mode']}/{method}/{cleaned_job['series_id']}",
                        "values": cleaned_job["values"],
                        "method": method,
                        "series_id": cleaned_job["series_id"],
                        "cleaning": cleaned_job["mode"],
                    }
                    for cleaned_job in cleaned.values()
                    for method in config["pipeline"]["transformations"]["methods"]
                ]
                transformed: dict[str, dict[str, Any]] = {}
                for batch, response in run_batches(
                    client,
                    transform_batch,
                    [
                        transform_jobs[offset : offset + 32]
                        for offset in range(0, len(transform_jobs), 32)
                    ],
                    resources={"CPU": 1},
                    max_in_flight=max_in_flight,
                    retries=2,
                ):
                    expected = {job["id"] for job in batch}
                    results = {result["id"]: result for result in response["results"]}
                    if set(results) != expected or len(results) != len(response["results"]):
                        raise RuntimeError("calibration transformation returned invalid task IDs")
                    for job in batch:
                        transformed[job["id"]] = {
                            **job,
                            "context": results[job["id"]]["values"],
                        }
                    worker = response["worker"]
                    host_contribution[worker["hostname"]] = host_contribution.get(
                        worker["hostname"], 0
                    ) + len(batch)
                    retry_count += int(worker.get("retry_count", 0))
                    completed_tasks += len(batch)

                auto_jobs = [
                    {
                        "id": f"model/auto_arima/{item['id']}",
                        "context": item["context"],
                        "horizon": config["data"]["benchmark"]["prediction_length"],
                        "seasonality": r_period,
                    }
                    for item in transformed.values()
                ]
                chronos_jobs = [
                    {
                        "id": f"model/chronos_2/{item['id']}",
                        "context": item["context"],
                        "horizon": config["data"]["benchmark"]["prediction_length"],
                    }
                    for item in transformed.values()
                ]
                chronos = config["models"]["chronos_2"]
                groups = {
                    "auto_arima": (
                        autoarima_batch,
                        [[job] for job in auto_jobs],
                        {"CPU": 1},
                        (
                            configuration.auto_arima_settings,
                            paths["r_auto_arima_worker"],
                            float(execution["worker_timeouts_seconds"]["r"]),
                            int(execution["thread_limits"]["r"]),
                        ),
                        max_in_flight,
                    ),
                    "chronos_2": (
                        chronos_batch,
                        _calibration_batches(chronos_jobs, chronos_batch_size),
                        {CHRONOS_GPU_RESOURCE: 1},
                        (
                            chronos["repository"],
                            chronos["revision"],
                            chronos["quantile_levels"],
                            "cuda",
                            chronos["dtype"],
                            chronos["cross_learning"],
                            chronos["predict_batches_jointly"],
                            execution["thread_limits"]["chronos"],
                            paths["chronos_environment"],
                            paths["chronos_worker"],
                            float(execution["worker_timeouts_seconds"]["chronos_startup"]),
                            float(execution["worker_timeouts_seconds"]["chronos_request"]),
                        ),
                        1,
                    ),
                }
                model_started = time.monotonic()
                for model, batch, response in run_batch_groups(
                    client, groups, retries=2
                ):
                    if isinstance(response, BaseException):
                        raise response
                    expected = {job["id"] for job in batch}
                    results = {result["id"]: result for result in response["results"]}
                    if set(results) != expected or len(results) != len(response["results"]):
                        raise RuntimeError(f"calibration {model} returned invalid task IDs")
                    outputs.update(results)
                    worker = response["worker"]
                    host_contribution[worker["hostname"]] = host_contribution.get(
                        worker["hostname"], 0
                    ) + len(batch)
                    retry_count += int(worker.get("retry_count", 0))
                    model_counts[model] += len(batch)
                    completed_tasks += len(batch)
                    if model == "chronos_2":
                        effective_batches.append(int(worker["effective_batch_size"]))
                model_elapsed = time.monotonic() - model_started
            except BaseException as error:
                failure = f"{type(error).__name__}: {error}"
        elapsed = time.monotonic() - started
        final_workers = set(client.scheduler_info()["workers"])
        telemetry = sampler.summary(initial_workers, final_workers)
        references = None
        if baseline is not None:
            references = json.loads(baseline.read_text(encoding="utf-8"))[
                "_scientific_outputs"
            ]
        comparison = (
            _scientific_comparison(outputs, references)
            if references is not None
            else {
                "equivalent": failure is None,
                "missing_output_count": 0,
                "unexpected_output_count": 0,
                "max_abs_difference": 0.0,
                "max_relative_difference": 0.0,
                "relative_tolerance": 1e-5,
                "absolute_tolerance": 1e-5,
            }
        )
        safety_reasons = []
        for hostname, values in telemetry["hosts"].items():
            minimum_gib = values["minimum_system_available_memory_bytes"] / GIB
            required = 16.0 if hostname == "WSUbuntu1" else 3.0
            if minimum_gib < required:
                safety_reasons.append(
                    f"{hostname} available memory {minimum_gib:.2f} GiB below {required:.0f} GiB"
                )
        gpu_available = telemetry["gpu"]["minimum_available_memory_bytes"]
        if gpu_available is None or gpu_available < 4 * GIB:
            safety_reasons.append("RTX 5090 available VRAM fell below 4 GiB or was unavailable")
        if telemetry["worker_restarts"]:
            safety_reasons.append("one or more Dask worker addresses changed")
        if telemetry["dask_spilling"]["persistent"]:
            safety_reasons.append("Dask spilling remained at the end of the candidate")
        if telemetry["sample_failures"]:
            safety_reasons.append("resource telemetry failed while the candidate was running")
        if (telemetry["maximum_sample_seconds"] or 0) > 5:
            safety_reasons.append("Mac coordinator telemetry response exceeded five seconds")
        if failure:
            safety_reasons.append(failure)
        if not comparison["equivalent"]:
            safety_reasons.append("scientific outputs differ from the baseline")
        report = {
            "profile_name": profile_name,
            "settings": {
                "mac_cpu_workers": mac_cpu_workers,
                "ubuntu_cpu_workers": ubuntu_cpu_workers,
                "gpu_workers": 1,
                "chronos_batch_size": chronos_batch_size,
                "maximum_in_flight_batches": max_in_flight,
            },
            "context_selection": {
                "count": len(contexts),
                "method": "256 equally spaced ranks ordered by context length then series ID",
                "length_groups": {
                    group: {
                        "count": sum(item["length_group"] == group for item in contexts),
                        "minimum": min(
                            item["length"]
                            for item in contexts
                            if item["length_group"] == group
                        ),
                        "maximum": max(
                            item["length"]
                            for item in contexts
                            if item["length_group"] == group
                        ),
                    }
                    for group in ("short", "medium", "long")
                },
            },
            "task_count": completed_tasks,
            "model_task_counts": model_counts,
            "elapsed_seconds": elapsed,
            "model_phase_elapsed_seconds": model_elapsed,
            "total_throughput_tasks_per_second": completed_tasks / elapsed,
            "per_model_throughput_tasks_per_second": {
                model: count / model_elapsed if model_elapsed else 0.0
                for model, count in model_counts.items()
            },
            "host_task_contribution": dict(sorted(host_contribution.items())),
            "telemetry": telemetry,
            "retry_count": retry_count,
            "failed_task_count": 0 if failure is None else 1,
            "failure": failure,
            "chronos_effective_batch_sizes": effective_batches,
            "scientific_equivalence": comparison,
            "safe": not safety_reasons,
            "safety_rejection_reasons": safety_reasons,
            "isolated_database": {
                "canonical_database_opened_read_only": True,
                "temporary_database_removed_after_run": True,
            },
            "_scientific_outputs": outputs,
        }
        measurement_db.execute(
            "INSERT INTO measurements VALUES (?, ?)",
            [profile_name, json.dumps({key: value for key, value in report.items() if key != "_scientific_outputs"})],
        )
        measurement_db.close()
    client.close()
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report
