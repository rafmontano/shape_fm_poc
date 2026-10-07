# ==============================================================================
# shared_chronos_checkpoint.py
# Purpose: Resolve and validate a pinned local checkpoint at both native boundaries.
# Inputs: Repository/revision and existing Hugging Face cache environment paths.
# Outputs: Verified local snapshot path; no downloads or network I/O.
# Run from: Imported by preflight and the native Chronos worker.
# ==============================================================================
"""Resolve the exact pinned Chronos snapshot without Hugging Face or network I/O.

Bounded functional exception: stateless filesystem validation needs no object.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path


def local_chronos_snapshot(repository: str, revision: str) -> Path:
    """Require an exact commit cache entry with readable config and complete weights.

    No refs, alternate revisions, downloads or network fallbacks are permitted.
    Safetensors offsets are checked against local byte length, not deserialized.
    """
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("Chronos revision must be an exact 40-character commit hash")
    home = Path(os.environ.get("HF_HOME", Path.home() / ".cache/huggingface"))
    cache = Path(os.environ.get("HF_HUB_CACHE", home / "hub"))
    snapshot = cache / ("models--" + repository.replace("/", "--")) / "snapshots" / revision
    try:
        config = json.loads((snapshot / "config.json").read_text())
        if not isinstance(config, dict) or not config:
            raise ValueError("empty or invalid config.json")
        weights = snapshot / "model.safetensors"
        size = weights.stat().st_size
        with weights.open("rb") as stream:
            header_size = int.from_bytes(stream.read(8), "little")
            if not 0 < header_size <= min(size - 8, 100_000_000):
                raise ValueError("invalid safetensors header length")
            header = json.loads(stream.read(header_size))
        tensors = [value for name, value in header.items() if name != "__metadata__"]
        if not tensors or any(
            not 0 <= value["data_offsets"][0] < value["data_offsets"][1] <= size - 8 - header_size
            for value in tensors
        ):
            raise ValueError("missing or truncated safetensors data")
    except (OSError, ValueError, KeyError, IndexError, TypeError, AttributeError) as error:
        raise RuntimeError(f"Incomplete local pinned Chronos checkpoint at {snapshot}: {error}") from error
    return snapshot
