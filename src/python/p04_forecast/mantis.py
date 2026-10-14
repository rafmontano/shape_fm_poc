"""Process 04 frozen Mantis-8M scientific representation provider.

Label-free finite length-64 inputs are linearly resized to 512 and encoded as
float32×256 final-layer CLS vectors. The native session owns transport; this
provider owns the pinned offline network and scientific/runtime description.
Resize, frozen representation and float32 semantics follow docs/directional-models.md.
"""

from __future__ import annotations

import contextlib
import hashlib
import importlib.metadata
import json
import os
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import torch
import torch.nn.functional as functional
from mantis.architecture import Mantis8M
from mantis.trainer import MantisTrainer


INPUT_LENGTH = 64
RESIZE_LENGTH = 512
REPRESENTATION_DIMENSION = 256
REPRESENTATION_DTYPE = "float32"
CHECKPOINT_REPOSITORY = "paris-noah/Mantis-8M"
CHECKPOINT_REVISION = "bc7d5ab40c02133386a28e2c127f35c17c86901d"
CHECKPOINT_FILES = {
    "config.json": "c9e8b1e5d9b95510c7d446d1e1ef46d5d36c3eae0c7a9b409c8d3c6c7ce7837c",
    "model.safetensors": "d5077b60438c477a627feeef0e7365588bdb351eaa5b5bcfb0b3d5fc5804f8c8",
}
EXPECTED_RUNTIME = {
    "mantis-tsfm": "1.1.0",
    "numpy": "2.3.5",
    "torch": "2.12.0",
    "transformers": "4.57.6",
}


def canonical_json(value: Any) -> str:
    """Return stable JSON used at the environment-neutral worker boundary."""
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def content_fingerprint(value: Any) -> str:
    """Return the SHA-256 fingerprint of canonical JSON-compatible content."""
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _file_sha256(path: Path) -> str:
    """Hash one exact checkpoint file without loading it into memory."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def dependency_versions() -> dict[str, str]:
    """Report and validate the exact native Mantis runtime."""
    versions = {name: importlib.metadata.version(name) for name in EXPECTED_RUNTIME}
    if versions != EXPECTED_RUNTIME:
        raise RuntimeError(f"Mantis runtime differs from the locked contract: {versions!r}")
    return versions


def default_checkpoint_path() -> Path:
    """Return the setup-owned pinned snapshot without network resolution."""
    cache_root = Path(
        os.environ.get(
            "HF_HUB_CACHE",
            Path(__file__).resolve().parents[3] / ".cache/huggingface",
        )
    )
    return (
        cache_root
        / "models--paris-noah--Mantis-8M"
        / "snapshots"
        / CHECKPOINT_REVISION
    )


def checkpoint_fingerprint(checkpoint_path: Path) -> str:
    """Verify every required local checkpoint file and return a combined fingerprint."""
    observed = {}
    for name, expected in CHECKPOINT_FILES.items():
        path = checkpoint_path / name
        if not path.is_file():
            raise FileNotFoundError(f"pinned Mantis checkpoint file is missing: {path}")
        observed[name] = _file_sha256(path)
        if observed[name] != expected:
            raise RuntimeError(f"pinned Mantis checkpoint fingerprint differs for {name}")
    return content_fingerprint(observed)


def validate_device(device: str) -> torch.device:
    """Require the requested device exactly; never select a silent fallback."""
    if device == "cpu":
        return torch.device("cpu")
    if device == "cuda":
        if not torch.cuda.is_available():
            raise RuntimeError("requested CUDA device is unavailable; fallback is prohibited")
        return torch.device("cuda")
    if device == "mps":
        if not torch.backends.mps.is_available():
            raise RuntimeError("requested MPS device is unavailable; fallback is prohibited")
        return torch.device("mps")
    raise ValueError("Mantis device must be explicitly cpu, cuda, or mps")


def input_matrix(values: Sequence[Sequence[float]]) -> np.ndarray:
    """Validate a bounded batch of length-64 inputs and convert it to float32."""
    try:
        matrix = np.asarray(values, dtype=np.float32)
    except (TypeError, ValueError) as exc:
        raise ValueError("Mantis inputs must be numeric") from exc
    if (
        matrix.ndim != 2
        or matrix.shape[0] < 1
        or matrix.shape[1] != INPUT_LENGTH
        or not np.isfinite(matrix).all()
    ):
        raise ValueError("Mantis inputs must be a nonempty finite matrix of length-64 rows")
    return matrix


def resize_for_mantis(values: Sequence[Sequence[float]]) -> np.ndarray:
    """Apply accepted 64→512 linear interpolation with align_corners false."""
    matrix = input_matrix(values)
    tensor = torch.tensor(matrix[:, np.newaxis, :], dtype=torch.float32)
    resized = functional.interpolate(
        tensor,
        size=RESIZE_LENGTH,
        mode="linear",
        align_corners=False,
    )
    result = resized.cpu().numpy().astype(np.float32, copy=False)
    if result.shape != (matrix.shape[0], 1, RESIZE_LENGTH):
        raise RuntimeError("Mantis resize returned an unexpected shape")
    return result


def representation_description(
    device: torch.device,
    runtime: Mapping[str, str],
    checkpoint_content_fingerprint: str,
) -> dict[str, Any]:
    """Build the stable scientific definition and operational runtime description."""
    definition = {
        "provider": "mantis.Mantis8M",
        "package_version": runtime["mantis-tsfm"],
        "checkpoint_repository": CHECKPOINT_REPOSITORY,
        "checkpoint_revision": CHECKPOINT_REVISION,
        "checkpoint_files": CHECKPOINT_FILES,
        "checkpoint_fingerprint": checkpoint_content_fingerprint,
        "input_length": INPUT_LENGTH,
        "resize": {
            "length": RESIZE_LENGTH,
            "mode": "linear",
            "align_corners": False,
        },
        "representation": "legacy_final_transformer_layer_cls",
        "dtype": REPRESENTATION_DTYPE,
        "dimension": REPRESENTATION_DIMENSION,
        "frozen": True,
    }
    return {
        "definition_id": "representation-definition/"
        + content_fingerprint(definition)[:32],
        "definition": definition,
        "runtime": dict(runtime),
        "device": str(device),
    }


def describe_runtime(device: str, checkpoint_path: Path | None = None) -> dict[str, Any]:
    """Validate runtime, requested device and checkpoint bytes without loading the network."""
    resolved_device = validate_device(device)
    runtime = dependency_versions()
    fingerprint = checkpoint_fingerprint(checkpoint_path or default_checkpoint_path())
    return representation_description(resolved_device, runtime, fingerprint)


class MantisRepresentationProvider:
    """Own the pinned native network and calculate frozen final-layer CLS vectors."""

    def __init__(
        self,
        *,
        device: str,
        checkpoint_path: Path | None = None,
        batch_size: int = 256,
    ):
        """Validate runtime/device/checkpoint and load the offline model exactly once."""
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
            raise ValueError("Mantis batch_size must be a positive integer")
        self.device = validate_device(device)
        self.batch_size = batch_size
        self.checkpoint_path = checkpoint_path or default_checkpoint_path()
        self.runtime = dependency_versions()
        self.checkpoint_content_fingerprint = checkpoint_fingerprint(self.checkpoint_path)
        # The native loader prints an informational line to stdout. Keep stdout
        # reserved for the worker's single JSON response.
        with contextlib.redirect_stdout(sys.stderr):
            network = Mantis8M(device=str(self.device)).from_pretrained(
                self.checkpoint_path,
                local_files_only=True,
            )
        if not isinstance(network, Mantis8M) or network.hidden_dim != REPRESENTATION_DIMENSION:
            raise RuntimeError("checkpoint did not load the legacy Mantis8M architecture")
        if network.pre_training:
            raise RuntimeError("Mantis representation provider requires pre_training=false")
        network.requires_grad_(False)
        network.eval()
        self._network = network
        self._trainer = MantisTrainer(device=str(self.device), network=network)

    def describe(self) -> dict[str, Any]:
        """Return the exact scientific and runtime representation definition."""
        return representation_description(
            self.device, self.runtime, self.checkpoint_content_fingerprint
        )

    def transform(self, values: Sequence[Sequence[float]]) -> np.ndarray:
        """Resize and encode one bounded batch as finite float32×256 embeddings."""
        resized = resize_for_mantis(values)
        embeddings = np.asarray(
            self._trainer.transform(resized, batch_size=self.batch_size),
            dtype=np.float32,
        )
        if (
            embeddings.shape != (resized.shape[0], REPRESENTATION_DIMENSION)
            or not np.isfinite(embeddings).all()
        ):
            raise RuntimeError("Mantis returned an invalid representation matrix")
        return embeddings

    def close(self) -> None:
        """Release the native trainer/network and accelerator cache owned here."""
        del self._trainer
        del self._network
        if self.device.type == "cuda":
            torch.cuda.empty_cache()
        elif self.device.type == "mps":
            torch.mps.empty_cache()
