# ==============================================================================
# p04_02_forecast_provider.py
#
# Purpose: Native, storage-free providers for ordinary Gate 4 model batches.
# Inputs: Serializable forecast jobs and authoritative model/execution settings.
# Outputs: Serializable forecast responses and provenance; never DuckDB writes.
# Execution: Called by named Prefect compute tasks, locally or inside a Dask worker.
# Run from: Imported; not run directly.
# Authority: Stored science settings and explicit effective ExecutionProfile safety.
# Contract: forecast(model, batch) returns results, metadata and runtime_seconds.
# ==============================================================================

"""Scientific provider objects used by the ordinary Gate 4 Prefect flow."""

from __future__ import annotations

import time
import uuid
from functools import partial
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .shared_configuration import R_FORECAST_METHODS
from .shared_execution_profiles import GIB, PersistentChronosWorker, system_hardware, validate_system_memory


@dataclass(frozen=True)
class ForecastSafetyPolicy:
    """Serializable profile-derived controls; no processes or resource allocation.

    R controls feed the existing host reservation/continuous monitor. Accelerator
    controls feed shared startup admission and continuous owned-child monitoring;
    they are pressure limits, not an invented predictive GPU fit budget.
    """

    autoarima: dict[str, Any]
    accelerator: dict[str, Any]
    ets_fit_budget_gib: float | None = None

    @classmethod
    def from_profile(cls, profile: Any, mac_hostname: str) -> "ForecastSafetyPolicy":
        """Resolve ordinary forecast safeguards solely from an ExecutionProfile."""
        required = {
            "dask_mac_memory_min_available_gib": profile.dask_mac_memory_min_available_gib,
            "dask_ubuntu_memory_min_available_gib": profile.dask_ubuntu_memory_min_available_gib,
            "dask_autoarima_fit_budget_gib": profile.dask_autoarima_fit_budget_gib,
            "dask_memory_admission_timeout_seconds": profile.dask_memory_admission_timeout_seconds,
            "dask_memory_poll_interval_seconds": profile.dask_memory_poll_interval_seconds,
            "dask_memory_breach_grace_seconds": profile.dask_memory_breach_grace_seconds,
            "dask_swap_growth_limit_gib": profile.dask_swap_growth_limit_gib,
        }
        missing = [name for name, value in required.items() if value is None]
        if missing:
            raise ValueError("execution profile lacks forecast safety fields: " + ", ".join(missing))
        return cls(
            autoarima={
                "mac_hostname": mac_hostname,
                "mac_minimum_available_gib": required["dask_mac_memory_min_available_gib"],
                "ubuntu_minimum_available_gib": required["dask_ubuntu_memory_min_available_gib"],
                "fit_budget_gib": required["dask_autoarima_fit_budget_gib"],
                "admission_timeout_seconds": required["dask_memory_admission_timeout_seconds"],
                "poll_interval_seconds": required["dask_memory_poll_interval_seconds"],
                "breach_grace_seconds": required["dask_memory_breach_grace_seconds"],
                "swap_growth_limit_gib": required["dask_swap_growth_limit_gib"],
            },
            accelerator={
                "minimum_available_gib": profile.accelerator_memory_min_available_gib,
                "host_minimum_available_gib": required["dask_ubuntu_memory_min_available_gib"],
                "admission_timeout_seconds": required["dask_memory_admission_timeout_seconds"],
                "poll_interval_seconds": required["dask_memory_poll_interval_seconds"],
                "breach_grace_seconds": required["dask_memory_breach_grace_seconds"],
                "swap_growth_limit_gib": required["dask_swap_growth_limit_gib"],
            },
            ets_fit_budget_gib=profile.dask_ets_fit_budget_gib,
        )


@dataclass(frozen=True)
class DistributedForecastProvider:
    """Invoke existing Dask-native AutoARIMA and Chronos batch adapters."""

    autoarima_settings: dict[str, Any]
    autoarima_script: str
    r_timeout: float
    r_threads: int
    chronos_settings: dict[str, Any]
    quantiles: tuple[float, ...]
    device: str
    chronos_threads: int
    chronos_environment: str
    chronos_script: str
    chronos_startup_timeout: float
    chronos_request_timeout: float
    safety_policy: ForecastSafetyPolicy | None = None
    r_settings: dict[str, dict[str, Any]] | None = None

    def forecast(self, model: str, batch: list[dict[str, Any]]) -> dict[str, Any]:
        """Run one bounded model batch through its existing native adapter."""
        from .shared_distributed_execution import autoarima_batch, chronos_batch
        from prefect.context import get_run_context

        started = time.time()
        retry_count = get_run_context().task_run.run_count - 1
        if model in R_FORECAST_METHODS:
            safety = self.safety_policy.autoarima if self.safety_policy else None
            if model == "ets" and safety is not None:
                safety = {**safety, "fit_budget_gib": self.safety_policy.ets_fit_budget_gib}
            response = autoarima_batch(
                batch, self.r_settings[model] if self.r_settings is not None else self.autoarima_settings,
                self.autoarima_script,
                self.r_timeout, self.r_threads, retry_count=retry_count,
                memory_safety=safety,
            )
        elif model == "chronos_2":
            settings = self.chronos_settings
            response = chronos_batch(
                batch, settings["repository"], settings["revision"],
                list(self.quantiles), self.device, settings["dtype"],
                settings["cross_learning"], settings["predict_batches_jointly"],
                self.chronos_threads, self.chronos_environment, self.chronos_script,
                self.chronos_startup_timeout, self.chronos_request_timeout,
                retry_count=retry_count,
                accelerator_safety=self.safety_policy.accelerator if self.safety_policy else None,
            )
        else:
            raise ValueError(f"unsupported forecast model {model!r}")
        return {
            "results": response["results"],
            "metadata": {
                **response["worker"],
                "runtime_seconds": response["runtime_seconds"],
                "batch_task_count": len(batch),
                "started_at": started,
                "finished_at": time.time(),
            },
            "runtime_seconds": response["runtime_seconds"],
        }


@dataclass(frozen=True)
class LocalAutoArimaProvider:
    """Run configured R methods; retain the historical provider name for callers."""

    worker: Callable[[dict[str, Any]], dict[str, Any]]
    settings: dict[str, Any]
    r_settings: dict[str, dict[str, Any]] | None = None

    @classmethod
    def from_configuration(cls, configuration: Any) -> "LocalAutoArimaProvider":
        """Bind only native bridge settings, never a live coordinator or connection."""
        from .shared_distributed_execution import _run_r

        execution = configuration.execution
        paths = configuration.execution_paths
        bridge = partial(
            _run_r, script=paths.get("r_forecast_worker", paths["r_auto_arima_worker"]),
            timeout=float(execution["worker_timeouts_seconds"]["r"]),
            threads=int(execution["thread_limits"]["r"]),
        )
        settings = {model: configuration.r_model_settings(model)
                    for model in configuration.resolved["models"] if model in R_FORECAST_METHODS}
        return cls(bridge, settings.get("auto_arima", {}), settings)

    def forecast(self, model: str, batch: list[dict[str, Any]]) -> dict[str, Any]:
        """Return a storage-free R response preserving requested/executed method IDs."""
        if model not in R_FORECAST_METHODS:
            raise ValueError(f"LocalAutoArimaProvider does not support {model!r}")
        started = time.monotonic()
        settings = self.r_settings[model] if self.r_settings is not None else self.settings
        response = self.worker({
            "action": "forecast",
            "settings": settings,
            "jobs": batch,
        })
        runtime = time.monotonic() - started
        return {
            "results": response["results"],
            "metadata": {
                "packages": response["packages"], "settings": settings,
                "execution_backend": "R/CPU", "runtime_seconds": runtime,
                "batch_task_count": len(batch),
            },
            "runtime_seconds": runtime,
        }


class LocalChronosProvider:
    """Own one local persistent Chronos bridge and bounded OOM batch splitting."""

    def __init__(self, root: Path, model: dict[str, Any], quantiles: tuple[float, ...],
                 device: str, profile: Any, execution: dict[str, Any], paths: dict[str, str]):
        """Bind serializable local settings without starting a model process."""
        self.root, self.model, self.quantiles, self.device = root, model, quantiles, device
        self.profile, self.execution, self.paths = profile, execution, paths

    def forecast(self, model: str, batch: list[dict[str, Any]]) -> dict[str, Any]:
        """Return one task response, retaining bounded OOM subdivision provenance."""
        if model != "chronos_2":
            raise ValueError(f"LocalChronosProvider does not support {model!r}")
        responses = [response for _, response in self.forecast_batches([batch])]
        return {
            "results": [result for response in responses for result in response["results"]],
            "metadata": {**responses[-1]["metadata"],
                         "sub_batches": [response["metadata"] for response in responses]},
            "runtime_seconds": sum(response["runtime_seconds"] for response in responses),
        }

    def forecast_batches(self, batches: list[list[dict[str, Any]]]):
        """Yield successful responses, restarting and splitting only OOM batches."""
        pending = deque((batch, 0) for batch in batches)
        worker = None
        generation = 0
        try:
            while pending:
                validate_system_memory(self.profile, system_hardware())
                if worker is None:
                    worker = PersistentChronosWorker(self._command(), startup_timeout=float(
                        self.execution["worker_timeouts_seconds"]["chronos_startup"]))
                    ready = worker.start()
                    generation += 1
                    available = ready["accelerator_memory"].get("available_bytes")
                    if available is not None and available < int(self.profile.accelerator_memory_min_available_gib * GIB):
                        raise RuntimeError("accelerator memory safety threshold reached before inference")
                batch, retry_count = pending.popleft()
                response = worker.request(self._payload(batch), timeout=float(
                    self.execution["worker_timeouts_seconds"]["chronos_request"]))
                if response.get("type") == "error":
                    if (response.get("error_kind") != "out_of_memory" or len(batch) == 1
                            or self.model["cross_learning"] or self.model["predict_batches_jointly"]):
                        raise RuntimeError(response.get("error", "invalid Chronos response"))
                    worker.close(force=True)
                    worker = None
                    size = max(1, len(batch) // 2)
                    splits = [batch[index:index + size] for index in range(0, len(batch), size)]
                    for split in reversed(splits):
                        pending.appendleft((split, retry_count + 1))
                    continue
                if response.get("type") != "result":
                    raise RuntimeError(f"invalid Chronos worker response: {response}")
                metadata = {
                    **{key: value for key, value in ready.items() if key != "type"},
                    "execution_backend": ready["accelerator_backend"],
                    "requested_batch_size": self.profile.chronos_inference_batch_size,
                    "effective_batch_size": response["effective_batch_size"],
                    "retry_count": retry_count, "worker_generation": generation,
                    "inference_seconds": response["inference_seconds"],
                    "peak_process_memory_bytes": response["peak_process_memory_bytes"],
                    "accelerator_memory_after": response["accelerator_memory"],
                    "runtime_seconds": response["inference_seconds"],
                    "batch_task_count": len(batch),
                }
                yield batch, {"results": response["results"], "metadata": metadata,
                              "runtime_seconds": response["inference_seconds"]}
                available = response["accelerator_memory"].get("available_bytes")
                if available is not None and available < int(self.profile.accelerator_memory_min_available_gib * GIB) and pending:
                    raise RuntimeError("accelerator memory safety threshold reached after committed batch")
        finally:
            if worker is not None:
                worker.close()

    def _command(self) -> list[str]:
        """Build the established isolated Chronos serve command."""
        return [str(self.root / self.paths["chronos_environment"] / "bin/python"),
                str(self.root / self.paths["chronos_worker"]), "serve", "--model",
                self.model["repository"], "--revision", self.model["revision"],
                "--device", self.device, "--dtype", self.model["dtype"],
                "--internal-cpu-threads", str(self.execution["thread_limits"]["chronos"])]

    def _payload(self, batch: list[dict[str, Any]]) -> dict[str, Any]:
        """Build one native Chronos request without routing-only job fields."""
        return {"command": "predict", "batch_id": f"chronos-batch/{uuid.uuid4().hex}",
                "jobs": batch, "horizon": batch[0]["horizon"],
                "quantile_levels": list(self.quantiles), "inference_batch_size": len(batch),
                "cross_learning": self.model["cross_learning"],
                "predict_batches_jointly": self.model["predict_batches_jointly"]}
