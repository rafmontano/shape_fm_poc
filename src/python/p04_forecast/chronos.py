"""Process 04 offline Chronos-2 provider in its pinned native environment.

Past-only forecast-v1 requests produce same-scale point and ordered quantile
horizons. The envelope's mean is the native median, not an expectation, under
chronos-forecasting 2.2.2. A JSON-lines session reuses one model; no scheduling.
Launch with ``python -m p04_forecast.chronos serve`` and explicit model settings.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import torch

# Set before importing the HF-backed provider; execution is strictly offline.
os.environ["HF_HUB_OFFLINE"] = "1"
os.environ["TRANSFORMERS_OFFLINE"] = "1"
from chronos import BaseChronosPipeline, Chronos2Pipeline

from p04_forecast.contract import ForecastContract
from p04_forecast.checkpoint import local_chronos_snapshot


def _apple_device_name() -> str:
    """Read the Apple CPU brand string, falling back to platform identity when sysctl is unavailable."""
    try:
        return subprocess.run(
            ["/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return platform.processor() or "Apple MPS device"


def select_device(requested: str) -> str:
    """Require the explicitly assigned cpu/cuda/mps backend; never fall back."""
    if requested == "cuda":
        if not torch.cuda.is_available():
            raise RuntimeError("CUDA profile requested but torch.cuda.is_available() is false")
        return "cuda"
    if requested == "mps":
        if not torch.backends.mps.is_available():
            raise RuntimeError("MPS profile requested but torch.backends.mps.is_available() is false")
        return "mps"
    if requested == "cpu":
        return "cpu"
    raise ValueError(f"unsupported accelerator backend: {requested}")


def hardware(device: str) -> dict[str, Any]:
    """Return device/backend, host and package provenance, not qualification."""
    selected = select_device(device)
    if selected == "cuda":
        device_name = torch.cuda.get_device_name(0)
    elif selected == "mps":
        device_name = _apple_device_name()
    else:
        device_name = platform.processor() or platform.machine()
    return {
        "accelerator_backend": selected,
        "accelerator_device_name": device_name,
        "torch_version": torch.__version__,
        "chronos_forecasting_version": importlib.metadata.version("chronos-forecasting"),
        "cuda_version": torch.version.cuda,
        "python_version": platform.python_version(),
        "operating_system": platform.system(),
        "architecture": platform.machine(),
    }


def emit(value: dict[str, Any]) -> None:
    """Write one compact, finite JSON protocol response and flush stdout immediately."""
    print(json.dumps(value, separators=(",", ":"), allow_nan=False), flush=True)


def predict(
    pipeline: Chronos2Pipeline,
    request: dict[str, Any],
) -> dict[str, Any]:
    """Infer past-only jobs with the loaded model under the forecast black-box
    contract (docs/forecast-black-box-contract.md). The pinned provider's second
    predict_quantiles return, named mean upstream, is its native 0.5 quantile.
    Retain it as the paper point forecast in the common mean field; quantile
    rearrangement does not replace that native point. Acceptance is coordinator-owned.
    """
    started = time.monotonic()
    jobs = request["jobs"]
    if request["predict_batches_jointly"] is not False:
        raise ValueError("Chronos-2 batch-joint prediction must remain disabled")
    levels = request["quantile_levels"]
    inputs = [
        {"target": np.asarray(job["context"], dtype=np.float32)} for job in jobs
    ]
    quantiles, medians = pipeline.predict_quantiles(
        inputs=inputs,
        prediction_length=request["horizon"],
        batch_size=request["inference_batch_size"],
        quantile_levels=levels,
        cross_learning=request["cross_learning"],
    )
    runtime = time.monotonic() - started
    results = []
    for job, item_quantiles, item_median in zip(jobs, quantiles, medians, strict=True):
        values = item_quantiles.detach().cpu().numpy()[0].T
        point = item_median.detach().cpu().numpy()[0]
        arranged_quantiles, quantiles_rearranged = ForecastContract.noncrossing_quantiles(
            values.tolist()
        )
        results.append(
            {
                "contract_version": job["contract_version"],
                "status": "success",
                "experiment_id": job["experiment_id"],
                "task_id": job["task_id"],
                "forecast_instance_id": job["forecast_instance_id"],
                "variant_id": job["variant_id"],
                "requested_model_id": job["model_id"],
                "executed_model_id": "chronos_2",
                "forecast_capability": "probabilistic",
                "output_scale": job["input_scale"],
                "horizon": job["horizon"],
                "mean": point.tolist(),
                "median": arranged_quantiles[levels.index(0.5)],
                "quantile_levels": levels,
                "quantiles": arranged_quantiles,
                "fallback_used": False,
                "fallback_reason": None,
                "runtime_seconds": runtime,
                "provenance": {
                    "implementation_language": "Python",
                    "provider": "chronos-forecasting",
                    "provider_version": importlib.metadata.version("chronos-forecasting"),
                    "model_revision": job["model_settings"]["revision"],
                    "effective_settings": job["model_settings"],
                    "seed": job["seed"],
                    "dtype": str(item_median.dtype),
                    "quantile_rearrangement": (
                        "sorted_per_horizon" if quantiles_rearranged else "identity"
                    ),
                },
            }
        )
    return {
        "type": "result",
        "batch_id": request["batch_id"],
        "results": results,
        "inference_seconds": runtime,
        "effective_batch_size": len(jobs),
    }


def serve(args: argparse.Namespace) -> None:
    """Load one pinned offline model from explicit settings and serve stdin
    predict/shutdown requests as stdout JSON results/errors. No retry or durable
    state: accepted atomic forecast keys on the coordinator own restart.
    """
    # Bound native threading; the owning worker supplies its execution limits.
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("MKL_NUM_THREADS", "1")
    torch.set_num_threads(args.internal_cpu_threads)
    device = select_device(args.device)
    model_started = time.monotonic()
    if args.dtype != "float32":
        raise ValueError("only the configured float32 Chronos dtype is supported")
    snapshot = local_chronos_snapshot(args.model, args.revision)
    pipeline = BaseChronosPipeline.from_pretrained(
        str(snapshot),
        local_files_only=True,
        device_map=device,
        dtype=torch.float32,
    )
    if not isinstance(pipeline, Chronos2Pipeline):
        raise TypeError("the pinned model did not load as Chronos2Pipeline")
    details = hardware(device)
    emit(
        {
            "type": "ready",
            "model": args.model,
            "revision": args.revision,
            "dtype": args.dtype,
            "checkpoint_location": str(snapshot),
            "cache_location": str(
                Path(os.environ.get("HF_HOME", Path.home() / ".cache/huggingface"))
            ),
            "model_load_seconds": time.monotonic() - model_started,
            "model_load_count": 1,
            **details,
        }
    )
    for line in sys.stdin:
        try:
            request = json.loads(line)
            if request.get("command") == "shutdown":
                emit({"type": "shutdown", "status": "ok"})
                return
            if request.get("command") != "predict":
                raise ValueError("unsupported Chronos worker command")
            emit(predict(pipeline, request))
        except Exception as error:
            emit(
                {
                    "type": "error",
                    "batch_id": request.get("batch_id") if "request" in locals() else None,
                    "error_kind": "worker_error",
                    "error": f"{type(error).__name__}: {error}",
                }
            )


def main() -> None:
    """Dispatch explicit-device CLI settings to provenance JSON or model serving."""
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    hardware_parser = subparsers.add_parser("hardware")
    hardware_parser.add_argument("--device", choices=("cpu", "mps", "cuda"), required=True)
    serve_parser = subparsers.add_parser("serve")
    serve_parser.add_argument("--model", required=True)
    serve_parser.add_argument("--revision", required=True)
    serve_parser.add_argument("--device", choices=("cpu", "mps", "cuda"), required=True)
    serve_parser.add_argument("--dtype", required=True)
    serve_parser.add_argument("--internal-cpu-threads", type=int, required=True)
    args = parser.parse_args()
    if args.command == "hardware":
        emit(hardware(args.device))
    else:
        serve(args)


if __name__ == "__main__":
    main()
