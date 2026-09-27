"""Persistent Chronos-2 JSON-lines worker; this module never imports DuckDB."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import platform
import resource
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import torch
from chronos import BaseChronosPipeline, Chronos2Pipeline


def _apple_device_name() -> str:
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
    if requested != "auto":
        raise ValueError(f"unsupported accelerator backend: {requested}")
    if torch.cuda.is_available():
        return "cuda"
    if torch.backends.mps.is_available():
        return "mps"
    return "cpu"


def accelerator_memory(device: str) -> dict[str, int | None]:
    if device == "cuda":
        free, total = torch.cuda.mem_get_info()
        return {
            "total_bytes": total,
            "available_bytes": free,
            "allocated_bytes": torch.cuda.memory_allocated(),
            "reserved_bytes": torch.cuda.memory_reserved(),
        }
    if device == "mps":
        allocated = int(torch.mps.current_allocated_memory())
        driver = int(torch.mps.driver_allocated_memory())
        recommended = int(torch.mps.recommended_max_memory())
        return {
            "total_bytes": recommended,
            "available_bytes": max(0, recommended - driver),
            "allocated_bytes": allocated,
            "reserved_bytes": driver,
        }
    return {
        "total_bytes": None,
        "available_bytes": None,
        "allocated_bytes": None,
        "reserved_bytes": None,
    }


def hardware(device: str) -> dict[str, Any]:
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
        "accelerator_memory": accelerator_memory(selected),
        "torch_version": torch.__version__,
        "chronos_forecasting_version": importlib.metadata.version("chronos-forecasting"),
        "cuda_version": torch.version.cuda,
        "mps_available": torch.backends.mps.is_available(),
        "mps_backend_version": torch.__version__
        if torch.backends.mps.is_available()
        else None,
        "python_version": platform.python_version(),
        "operating_system": platform.system(),
        "architecture": platform.machine(),
    }


def is_out_of_memory(error: BaseException) -> bool:
    return isinstance(error, torch.OutOfMemoryError) or "out of memory" in str(error).lower()


def emit(value: dict[str, Any]) -> None:
    print(json.dumps(value, separators=(",", ":"), allow_nan=False), flush=True)


def predict(
    pipeline: Chronos2Pipeline,
    request: dict[str, Any],
    device: str,
) -> dict[str, Any]:
    started = time.monotonic()
    jobs = request["jobs"]
    levels = request["quantile_levels"]
    inputs = [
        {"target": np.asarray(job["context"], dtype=np.float32)} for job in jobs
    ]
    quantiles, means = pipeline.predict_quantiles(
        inputs=inputs,
        prediction_length=request["horizon"],
        batch_size=request["inference_batch_size"],
        quantile_levels=levels,
        cross_learning=False,
    )
    results = []
    for job, item_quantiles, item_mean in zip(jobs, quantiles, means, strict=True):
        values = item_quantiles.detach().cpu().numpy()[0].T
        mean = item_mean.detach().cpu().numpy()[0]
        results.append(
            {
                "id": job["id"],
                "mean": mean.tolist(),
                "median": values[levels.index(0.5)].tolist(),
                "quantiles": values.tolist(),
            }
        )
    maximum_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if platform.system() != "Darwin":
        maximum_rss *= 1024
    return {
        "type": "result",
        "batch_id": request["batch_id"],
        "results": results,
        "inference_seconds": time.monotonic() - started,
        "effective_batch_size": len(jobs),
        "peak_process_memory_bytes": maximum_rss,
        "accelerator_memory": accelerator_memory(device),
    }


def serve(args: argparse.Namespace) -> None:
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("MKL_NUM_THREADS", "1")
    torch.set_num_threads(1)
    device = select_device(args.device)
    model_started = time.monotonic()
    pipeline = BaseChronosPipeline.from_pretrained(
        args.model,
        revision=args.revision,
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
            "dtype": "float32",
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
            emit(predict(pipeline, request, device))
        except BaseException as error:
            emit(
                {
                    "type": "error",
                    "batch_id": request.get("batch_id") if "request" in locals() else None,
                    "error_kind": "out_of_memory" if is_out_of_memory(error) else "worker_error",
                    "error": f"{type(error).__name__}: {error}",
                    "accelerator_memory": accelerator_memory(device),
                }
            )


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    hardware_parser = subparsers.add_parser("hardware")
    hardware_parser.add_argument("--device", default="auto")
    serve_parser = subparsers.add_parser("serve")
    serve_parser.add_argument("--model", required=True)
    serve_parser.add_argument("--revision", required=True)
    serve_parser.add_argument("--device", default="auto")
    args = parser.parse_args()
    if args.command == "hardware":
        emit(hardware(args.device))
    else:
        serve(args)


if __name__ == "__main__":
    main()
