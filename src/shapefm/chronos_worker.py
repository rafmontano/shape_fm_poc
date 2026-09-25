"""Batch Chronos-2 worker; it never imports DuckDB or opens the project database."""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import numpy as np
import torch
from chronos import BaseChronosPipeline, Chronos2Pipeline


def select_device(requested: str) -> str:
    if requested != "auto":
        return requested
    if torch.cuda.is_available():
        return "cuda"
    if torch.backends.mps.is_available():
        return "mps"
    return "cpu"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--payload", type=Path, required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--device", default="auto")
    parser.add_argument("--batch-size", type=int, default=8)
    args = parser.parse_args()
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("MKL_NUM_THREADS", "1")
    torch.set_num_threads(1)
    payload = json.loads(args.payload.read_text(encoding="utf-8"))
    device = select_device(args.device)
    started = time.monotonic()
    pipeline = BaseChronosPipeline.from_pretrained(
        args.model,
        revision=args.revision,
        device_map=device,
        dtype=torch.float32,
    )
    if not isinstance(pipeline, Chronos2Pipeline):
        raise TypeError("the pinned model did not load as Chronos2Pipeline")
    levels = payload["quantile_levels"]
    inputs = [
        {"target": np.asarray(job["context"], dtype=np.float32)}
        for job in payload["jobs"]
    ]
    quantiles, means = pipeline.predict_quantiles(
        inputs=inputs,
        prediction_length=payload["horizon"],
        batch_size=args.batch_size,
        quantile_levels=levels,
        cross_learning=False,
    )
    results = []
    for job, item_quantiles, item_mean in zip(
        payload["jobs"], quantiles, means, strict=True
    ):
        # Chronos-2 returns [variates, horizon, quantiles] and [variates, horizon].
        q = item_quantiles.detach().cpu().numpy()[0].T
        mean = item_mean.detach().cpu().numpy()[0]
        results.append(
            {
                "id": job["id"],
                "mean": mean.tolist(),
                "median": q[levels.index(0.5)].tolist(),
                "quantiles": q.tolist(),
            }
        )
    print(
        json.dumps(
            {
                "results": results,
                "device": device,
                "dtype": "float32",
                "runtime_seconds": time.monotonic() - started,
                "model": args.model,
                "revision": args.revision,
                "cache_location": str(
                    Path(os.environ.get("HF_HOME", Path.home() / ".cache/huggingface"))
                ),
            },
            separators=(",", ":"),
            allow_nan=False,
        )
    )


if __name__ == "__main__":
    main()
