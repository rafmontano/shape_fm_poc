"""Controlled local calibration using an isolated temporary DuckDB database."""

from __future__ import annotations

import json
import math
import os
import platform
import resource
import subprocess
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import duckdb

from .database import DEFAULT_DATABASE
from .execution import (
    GIB,
    ExecutionProfile,
    PersistentChronosWorker,
    system_memory,
)
from .orchestration import repository_root
from .poc1 import QUANTILES


CALIBRATION_CANDIDATES = {
    "mac_m1pro_10core_16gb": {
        "cpu_workers": [1, 2, 4],
        "chronos_batch_sizes": [1, 2, 4, 8, 16],
    },
    "ubuntu_5950x_16core_128gb_rtx5090": {
        "cpu_workers": [1, 4, 8, 12, 16],
        "chronos_batch_sizes": [8, 16, 32, 64],
    },
}


class _MemorySampler:
    def __init__(self, interval_seconds: float = 0.25):
        self.interval_seconds = interval_seconds
        self.samples: list[dict[str, int]] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _sample(self) -> None:
        self.samples.append(system_memory())

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            self._sample()

    def __enter__(self) -> "_MemorySampler":
        self._sample()
        self._thread = threading.Thread(
            target=self._run,
            name="shapefm-calibration-memory",
            daemon=True,
        )
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        self._sample()

    def summary(self) -> dict[str, Any]:
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


def representative_contexts(database_path: Path) -> list[dict[str, Any]]:
    connection = duckdb.connect(str(database_path.resolve()), read_only=True)
    try:
        rows = connection.execute(
            """WITH ranked AS (
                SELECT s.series_id, s.target, w.test_start,
                       row_number() OVER (ORDER BY s.observation_count, s.series_id) AS position,
                       count(*) OVER () AS total
                FROM series s JOIN evaluation_windows w USING (dataset_id, series_id)
                JOIN datasets d USING (dataset_id)
                WHERE d.dataset_name='m4_daily'
            )
            SELECT series_id, target[:test_start], test_start
            FROM ranked WHERE position IN (1, CAST(ceil(total / 2.0) AS INTEGER), total)
            ORDER BY position"""
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


def _r_forecast(root: Path, job: dict[str, Any]) -> dict[str, Any]:
    completed = subprocess.run(
        ["Rscript", str(root / "R/poc1_worker.R")],
        cwd=root,
        input=json.dumps({"action": "forecast", "jobs": [job]}),
        check=True,
        capture_output=True,
        text=True,
        timeout=1800,
        env={**os.environ, "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1"},
    )
    return json.loads(completed.stdout)


def _peak_children_memory_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    return int(value if platform.system() == "Darwin" else value * 1024)


def calibrate(
    profile: ExecutionProfile,
    hardware: dict[str, Any],
    database_path: Path = DEFAULT_DATABASE,
    output: Path | None = None,
) -> dict[str, Any]:
    if profile.name not in CALIBRATION_CANDIDATES:
        raise ValueError(f"no calibration grid is defined for {profile.name}")
    root = repository_root()
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
                    "horizon": 14,
                    "seasonality": 1,
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
                                executor.map(lambda job: _r_forecast(root, job), jobs)
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

        chronos = json.loads(
            (root / "config/experiments/poc1.json").read_text(encoding="utf-8")
        )["models"]["chronos_2"]
        command = [
            str(root / "environments/chronos-2/.venv/bin/python"),
            str(root / "src/shapefm/chronos_worker.py"),
            "serve",
            "--model",
            chronos["repository"],
            "--revision",
            chronos["revision"],
            "--device",
            profile.required_accelerator or "auto",
        ]
        chronos_reference: dict[str, dict[str, Any]] = {}
        with PersistentChronosWorker(command) as worker:
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
                                    "horizon": 14,
                                    "quantile_levels": list(QUANTILES),
                                    "inference_batch_size": batch_size,
                                }
                            )
                            responses.append(response)
                            if response.get("type") == "error":
                                failure = response.get("error")
                                break
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
                differences = []
                if not failure and responses:
                    for context, response in zip(contexts, responses, strict=True):
                        forecast = response["results"][0]
                        if batch_size == 1:
                            chronos_reference[context["label"]] = forecast
                        differences.append(
                            _forecast_comparison(
                                chronos_reference[context["label"]], forecast
                            )
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
