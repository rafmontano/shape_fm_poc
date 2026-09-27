# ==============================================================================
# configuration.py
#
# Purpose: Stage 1 configuration, deterministic identities, and window boundaries.
# Inputs: Stage 1 JSON configuration, source revision/file metadata, and observation counts.
# Outputs: Validated configuration, stable SHA-256 identities, and zero-based evaluation boundaries.
# Run from: Imported; not run directly.
# ==============================================================================

"""Stage 1 configuration, deterministic identities, and window boundaries."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


class ImportValidationError(ValueError):
    """Raised when source data or import state violates the Stage 1 contract."""


def canonical_json(value: Any) -> str:
    """Serialize a JSON-compatible value with sorted keys and no optional whitespace."""
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def json_fingerprint(value: Any) -> str:
    """Return the hexadecimal SHA-256 digest of ``value``'s canonical JSON."""
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def load_config(path: Path, max_series: int | None) -> dict[str, Any]:
    """Load Stage 1 JSON, set the invocation's series limit, validate it, and return it."""
    with path.open("r", encoding="utf-8") as stream:
        config = json.load(stream)
    config["max_series"] = max_series
    validate_config(config)
    return config


def validate_config(config: dict[str, Any]) -> None:
    """Require Stage 1 keys, a positive optional limit, and the exact M4 Daily benchmark."""
    required = {"schema_version", "dataset_name", "source_system", "benchmark", "max_series"}
    missing = sorted(required - config.keys())
    if missing:
        raise ImportValidationError(f"configuration is missing: {', '.join(missing)}")
    if config["max_series"] is not None and (
        not isinstance(config["max_series"], int) or config["max_series"] <= 0
    ):
        raise ImportValidationError("max_series must be a positive integer")
    expected = {
        "frequency": "D",
        "term": "short",
        "prediction_length": 14,
        "evaluation_windows": 1,
        "boundary_convention": "zero-based, end-exclusive",
    }
    if config["benchmark"] != expected:
        raise ImportValidationError(f"M4 Daily benchmark must equal {expected!r}")


def canonical_import_configuration(config: dict[str, Any]) -> dict[str, Any]:
    """Return scientific import settings, excluding the non-identity ``max_series`` limit."""
    return {key: value for key, value in config.items() if key != "max_series"}


def dataset_identity(
    config: dict[str, Any], source_revision: str, source_files: dict[str, Any]
) -> tuple[str, str]:
    """Return the content-derived dataset ID and canonical configuration SHA-256 digest."""
    canonical_config = canonical_import_configuration(config)
    config_hash = json_fingerprint(canonical_config)
    identity = {
        "logical_dataset": f"gift_eval/{config['dataset_name']}",
        "source_revision": source_revision,
        "source_files": source_files,
        "import_configuration_hash": config_hash,
    }
    return f"gift_eval/{config['dataset_name']}/{json_fingerprint(identity)[:24]}", config_hash


def evaluation_window(observation_count: int, config: dict[str, Any]) -> dict[str, Any]:
    """Return one validation/test window over the final two horizons of a series."""
    horizon = config["benchmark"]["prediction_length"]
    if observation_count < horizon * 2:
        raise ImportValidationError(
            f"series has {observation_count} observations; at least {horizon * 2} are required"
        )
    validation_start = observation_count - 2 * horizon
    test_start = observation_count - horizon
    return {
        "window_id": "short/000",
        "split_name": "validation_and_test",
        "train_start": 0,
        "train_end": validation_start,
        "validation_start": validation_start,
        "validation_end": test_start,
        "test_start": test_start,
        "test_end": observation_count,
        "horizon": horizon,
        "boundary_convention": config["benchmark"]["boundary_convention"],
    }
