# ==============================================================================
# p01_03_gift_eval_source.py
#
# Purpose: Bounded, read-only streaming adapter for GIFT-Eval Arrow source data.
# Inputs: A pinned GIFT-Eval Arrow snapshot directory, expected frequency, and optional row limit.
# Outputs: Source provenance/metadata and validated univariate series, including missingness.
# Run from: Imported; not run directly.
# ==============================================================================

"""Bounded, read-only streaming adapter for GIFT-Eval Arrow source data."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import pyarrow as pa
import pyarrow.ipc as ipc

from .shared_configuration import (
    ExperimentConfiguration,
    ImportValidationError,
    json_fingerprint,
)
from .shared_provenance import sha256_file


# Code constant: Arrow filename required by the pinned GIFT-Eval snapshot protocol.
SOURCE_ARROW_NAME = "data-00000-of-00001.arrow"
# Code constant: complete pinned-snapshot file contract used for provenance hashing.
SOURCE_METADATA_NAMES = (SOURCE_ARROW_NAME, "dataset_info.json", "state.json")


@dataclass(frozen=True)
class SourceSeries:
    """Purpose: Represent one validated source row ready for canonical import.

    Inputs: Zero-based row and item identity, first-observation timestamp, source
    frequency code, and float32-derived target observations with missing values intact.
    Outputs: Immutable series state preserving source order, values, and missingness.
    """
    source_row: int
    source_series_id: str
    start_timestamp: Any
    frequency: str
    target: tuple[float | None, ...]


class ConfiguredGiftEvalSource:
    """Own the read-only GIFT-Eval source contract for one stored experiment.

    Construction resolves no JSON and performs no I/O.  The configuration supplied
    by the DuckDB loader remains authoritative; ``records`` streams at most its
    configured selection count while preserving Arrow order and missing values.
    The official reader is also lazy, but its NumPy/GluonTS formatting loses the
    Arrow null-versus-NaN distinction. Raw import keeps that distinction and
    independently verifies byte hashes; evaluation still uses the official reader.
    """

    def __init__(self, configuration: ExperimentConfiguration, repository: Path):
        """Resolve the configured source path without I/O or acquisition."""
        self.configuration = configuration
        self.path = (
            Path(repository)
            / configuration.source_directory
            / configuration.resolved["data"]["dataset_name"]
        )

    @property
    def revision(self) -> str:
        """Return the pinned source revision from authoritative configuration."""
        return str(self.configuration.resolved["data"]["source"]["revision"])

    @property
    def settings(self) -> dict[str, Any]:
        """Return the bounded canonical import settings."""
        return self.configuration.import_settings

    def fingerprint(self) -> dict[str, Any]:
        """Return and validate the snapshot's byte identity."""
        result = source_fingerprint(self.path)
        expected = self.configuration.resolved["data"]["source"]["files"]
        observed = {name: value["sha256"] for name, value in result["files"].items()}
        if observed != expected:
            raise ImportValidationError(
                "pinned M4 Daily source hashes do not match stored configuration"
            )
        return result

    def metadata(self) -> dict[str, Any]:
        """Return pinned descriptive metadata without interpreting it as configuration."""
        return source_metadata(self.path)

    def records(self) -> Iterator[SourceSeries]:
        """Stream the configured bounded records in authoritative source order."""
        settings = self.settings
        return iter_source_series(
            self.path,
            settings["benchmark"]["frequency"],
            settings["max_series"],
        )


def source_fingerprint(source_dir: Path) -> dict[str, Any]:
    """Purpose: Establish the byte-level identity of a pinned source snapshot.

    Inputs: Directory containing all three required GIFT-Eval snapshot files.
    Outputs: Per-file byte sizes and SHA-256 values plus their aggregate JSON digest.
    """
    files: dict[str, Any] = {}
    for name in SOURCE_METADATA_NAMES:
        path = source_dir / name
        if not path.is_file():
            raise ImportValidationError(f"required source file is missing: {path}")
        files[name] = {"bytes": path.stat().st_size, "sha256": sha256_file(path)}
    return {"files": files, "fingerprint": json_fingerprint(files)}


def source_metadata(source_dir: Path) -> dict[str, Any]:
    """Purpose: Read descriptive and state metadata from a source snapshot.

    Inputs: Snapshot directory containing valid ``dataset_info.json`` and ``state.json``.
    Outputs: Decoded JSON values keyed by filename; I/O and decoding errors propagate.
    """
    metadata: dict[str, Any] = {}
    for name in ("dataset_info.json", "state.json"):
        with (source_dir / name).open("r", encoding="utf-8") as stream:
            metadata[name] = json.load(stream)
    return metadata


def _validate_schema(schema: pa.Schema) -> None:
    """Purpose: Enforce the exact scientific Arrow input schema.

    Inputs: Arrow schema expected to contain item ID, seconds timestamp, frequency,
    and list-of-float32 target columns in that order.
    Outputs: ``None`` when valid; otherwise raises ``ImportValidationError``.
    """
    expected_names = ["item_id", "start", "freq", "target"]
    if schema.names != expected_names:
        raise ImportValidationError(
            f"source columns are {schema.names!r}, expected {expected_names!r}"
        )
    if schema.field("item_id").type != pa.string():
        raise ImportValidationError("item_id must be string")
    if schema.field("start").type != pa.timestamp("s"):
        raise ImportValidationError("start must be timestamp[s]")
    if schema.field("freq").type != pa.string():
        raise ImportValidationError("freq must be string")
    target_type = schema.field("target").type
    if not pa.types.is_list(target_type) or target_type.value_type != pa.float32():
        raise ImportValidationError("target must be list<float32>")


def iter_source_series(
    source_dir: Path, expected_frequency: str, max_series: int | None
) -> Iterator[SourceSeries]:
    """Purpose: Stream validated univariate series from the pinned Arrow snapshot.

    Inputs: Source directory, required frequency code, and optional positive row limit.
    Outputs: ``SourceSeries`` values in source order without materializing the dataset.
    Notes: Rejects malformed rows, duplicate IDs, frequency mismatches, infinities,
    empty targets, and an empty source. Element-level nulls and NaNs are valid missing
    observations and their positions remain present in the imported raw target.
    """
    arrow_path = source_dir / SOURCE_ARROW_NAME
    seen: set[str] = set()
    source_row = 0
    with pa.memory_map(str(arrow_path), "r") as source:
        reader = ipc.open_stream(source)
        _validate_schema(reader.schema)
        for batch in reader:
            for batch_row in range(batch.num_rows):
                if max_series is not None and source_row >= max_series:
                    return
                values = [batch.column(index)[batch_row].as_py() for index in range(4)]
                item_id, start, frequency, target_values = values
                if any(value is None for value in values[:3]) or target_values is None:
                    raise ImportValidationError(
                        f"source row {source_row} contains null identity or target structure"
                    )
                if item_id in seen:
                    raise ImportValidationError(f"duplicate source item_id: {item_id}")
                seen.add(item_id)
                if frequency != expected_frequency:
                    raise ImportValidationError(
                        f"source row {source_row} frequency is {frequency!r}, "
                        f"expected {expected_frequency!r}"
                    )
                target = tuple(target_values)
                if not target:
                    raise ImportValidationError(
                        f"source row {source_row} target is empty"
                    )
                for position, value in enumerate(target):
                    if value is not None and (
                        not isinstance(value, (int, float)) or math.isinf(value)
                    ):
                        raise ImportValidationError(
                            f"source row {source_row} target position {position} "
                            "must be numeric, missing, or NaN and cannot be infinite"
                        )
                yield SourceSeries(source_row, item_id, start, frequency, target)
                source_row += 1
    if source_row == 0:
        raise ImportValidationError("source contains no series")
