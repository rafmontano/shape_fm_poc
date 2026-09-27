# ==============================================================================
# gift_eval_source.py
#
# Purpose: Bounded, read-only streaming adapter for GIFT-Eval Arrow source data.
# Inputs: A pinned GIFT-Eval Arrow snapshot directory, expected frequency, and optional row limit.
# Outputs: Source file provenance/metadata and validated finite univariate series streamed in source order.
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

from .configuration import ImportValidationError, json_fingerprint
from .provenance import sha256_file


# SOURCE_ARROW_NAME: pinned Arrow stream filename required from the source snapshot.
SOURCE_ARROW_NAME = "data-00000-of-00001.arrow"
# SOURCE_METADATA_NAMES: complete tuple of Arrow and metadata files required from the source snapshot.
SOURCE_METADATA_NAMES = (SOURCE_ARROW_NAME, "dataset_info.json", "state.json")


@dataclass(frozen=True)
class SourceSeries:
    """One validated Arrow row for import: zero-based source row, source item ID, first-observation timestamp, source frequency code, and finite target observations."""
    source_row: int
    source_series_id: str
    start_timestamp: Any
    frequency: str
    target: tuple[float, ...]


def source_fingerprint(source_dir: Path) -> dict[str, Any]:
    """Return byte sizes and SHA-256 digests for every required source file plus their aggregate digest."""
    files: dict[str, Any] = {}
    for name in SOURCE_METADATA_NAMES:
        path = source_dir / name
        if not path.is_file():
            raise ImportValidationError(f"required source file is missing: {path}")
        files[name] = {"bytes": path.stat().st_size, "sha256": sha256_file(path)}
    return {"files": files, "fingerprint": json_fingerprint(files)}


def source_metadata(source_dir: Path) -> dict[str, Any]:
    """Load and return the snapshot's dataset-info and state JSON keyed by filename."""
    metadata: dict[str, Any] = {}
    for name in ("dataset_info.json", "state.json"):
        with (source_dir / name).open("r", encoding="utf-8") as stream:
            metadata[name] = json.load(stream)
    return metadata


def _validate_schema(schema: pa.Schema) -> None:
    """Require the exact item ID, second timestamp, frequency, and float32-list Arrow schema."""
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
    """Yield one validated source series at a time from the memory-mapped stream."""
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
                if any(value is None for value in values):
                    raise ImportValidationError(f"source row {source_row} contains null values")
                if item_id in seen:
                    raise ImportValidationError(f"duplicate source item_id: {item_id}")
                seen.add(item_id)
                if frequency != expected_frequency:
                    raise ImportValidationError(
                        f"source row {source_row} frequency is {frequency!r}, "
                        f"expected {expected_frequency!r}"
                    )
                target = tuple(target_values)
                if not target or not all(math.isfinite(value) for value in target):
                    raise ImportValidationError(
                        f"source row {source_row} target is empty or non-finite"
                    )
                yield SourceSeries(source_row, item_id, start, frequency, target)
                source_row += 1
    if source_row == 0:
        raise ImportValidationError("source contains no series")
