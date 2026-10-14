"""Process01 canonical GIFT-Eval import application.

Inputs are the configured Arrow source and canonical population selection.
Outputs preserve source identities, float32 observations and official boundaries
in experiment.duckdb. No secondary source reconciliation or statistical repair.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.ipc as ipc

from util.runtime import DataRef, ExperimentRequest, WorkItem


def calculate_canonical_series(item: WorkItem) -> dict[str, Any]:
    """Atomic import: preserve the selected source row and official M4 boundaries.

    GIFT-Eval is the data authority (docs/data-contract.md). Targets are not
    cleaned; null/NaN observations remain missing. Boundaries are end-exclusive.
    """
    population = item.payload
    position = population["source_row"]
    with pa.memory_map(population["source_path"], "r") as source:
        for batch in ipc.open_stream(source):
            if position < batch.num_rows:
                record = batch.slice(position, 1).to_pylist()[0]
                break
            position -= batch.num_rows
        else:
            raise ValueError(f"GIFT-Eval source row is missing: {item.key}")
    target = record["target"]
    if (record["item_id"] != population["series_id"] or record["freq"] != population["frequency"]
            or not target or any(v is not None and math.isinf(v) for v in target)):
        raise ValueError(f"invalid canonical source record: {item.key}")
    horizon = population["horizon"]
    if len(target) <= 2 * horizon:
        raise ValueError(f"series has no official training history: {item.key}")
    return {**population, "start": record["start"].isoformat(), "target": target,
            "train_end": len(target) - 2 * horizon, "test_start": len(target) - horizon,
            "observation_count": len(target)}


class ImportApplication:
    """Own source population, canonical calculation and coordinator acceptance.

    The three meaningful subflows restart from population, atomic series and
    canonical dataset references respectively. Accepted series are never reread.
    """

    calculate_canonical_series = staticmethod(calculate_canonical_series)

    def __init__(self, context: Any):
        self.context = context
        self.store = context.repository

    def register_source_population(self, request: ExperimentRequest) -> DataRef:
        """Register selected identities in canonical Arrow order, without values."""
        data = self.context.configuration["data"]
        source_path = Path(data["source"]["directory"]) / data["dataset_name"] / "data-00000-of-00001.arrow"
        selected = []
        source_row = -1
        with pa.memory_map(str(source_path), "r") as source:
            reader = ipc.open_stream(source)
            if reader.schema.names != ["item_id", "start", "freq", "target"]:
                raise ValueError("unexpected canonical GIFT-Eval schema")
            for batch in reader:
                for series_id, frequency in zip(batch.column(0).to_pylist(), batch.column(2).to_pylist(), strict=True):
                    source_row += 1
                    if frequency != data["benchmark"]["frequency"]:
                        continue
                    key = f"{data['dataset_name']}@{data['source']['revision']}/{frequency}/{series_id}"
                    selected.append((key, {"dataset_id": f"{data['dataset_name']}@{data['source']['revision']}",
                        "series_id": series_id, "source_row": source_row, "frequency": frequency,
                        "source_path": str(source_path), "horizon": data["benchmark"]["prediction_length"]}))
                    if len(selected) == data["selection"]["count"]:
                        break
                if len(selected) == data["selection"]["count"]:
                    break
        if len(selected) != data["selection"]["count"] or len({k for k, _ in selected}) != len(selected):
            raise ValueError("source population is incomplete or has duplicate identities")
        self.store.accept("01.01", selected)
        return self.existing_population()

    def existing_population(self) -> DataRef:
        """Return the stored source-population reference without reopening Arrow."""
        return self.context.ref("01.01")

    def pending_canonical_series(self, population: DataRef) -> list[WorkItem]:
        """Plan only registered series absent from accepted canonical results."""
        return self.store.plan("01.02", [WorkItem(key, record) for key, record in self.store.rows("01.01").items()])

    def accept_canonical_series(self, results: list[tuple[str, dict[str, Any]]]) -> None:
        """Commit returned canonical series on the coordinator, atomically by group."""
        self.store.accept("01.02", results)

    def canonical_dataset(self) -> DataRef:
        """Return the canonical output reference consumed by Process02."""
        return self.context.ref("01.03")

    def accept_canonical_dataset(self, population: DataRef) -> DataRef:
        """Require exact registered/accepted identities, without rechecking values."""
        if self.store.keys("01.01") != self.store.keys("01.02"):
            raise RuntimeError("canonical series do not match the registered source population")
        return self.canonical_dataset()
