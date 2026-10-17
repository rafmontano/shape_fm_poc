"""Process01 canonical GIFT-Eval and configured archived-forecast ingestion.

Inputs are the configured Arrow source and canonical population selection.
Outputs preserve source identities, float32 observations and official boundaries
in experiment.duckdb. No secondary source reconciliation or statistical repair.
"""

from __future__ import annotations

import math
import time
from pathlib import Path
from typing import Any, Callable

import pyarrow as pa
import pyarrow.ipc as ipc

from util.providers import run_r
from util.runtime import DataRef, ExperimentRequest, ProgressSnapshot, WorkItem


def calculate_canonical_series(item: WorkItem) -> dict[str, Any]:
    """Atomic import: preserve the selected source row and official M4 boundaries.

    GIFT-Eval is the data authority (docs/data-contract.md). Targets are not
    cleaned; null/NaN observations remain missing. Boundaries are end-exclusive.
    """
    population, record = item.payload["population"], item.payload["record"]
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

    Canonical series and imported forecasts restart independently by accepted
    identity. Archive completion references common forecasts, never copied vectors.
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
        """Plan missing records with portable values, not coordinator-only paths.

        The source is read only by the coordinator; workers validate and return
        selected canonical records without needing a shared source filesystem.
        Accepted series are excluded before reading any observation values.
        """
        accepted = self.store.keys("01.02")
        selected = {key: record for key, record in self.store.rows("01.01").items() if key not in accepted}
        items = []
        for source_path in sorted({record["source_path"] for record in selected.values()}):
            by_position = {record["source_row"]: (key, record) for key, record in selected.items()
                           if record["source_path"] == source_path}
            offset = 0
            with pa.memory_map(source_path, "r") as source:
                for batch in ipc.open_stream(source):
                    for position, (key, record) in by_position.items():
                        if offset <= position < offset + batch.num_rows:
                            items.append(WorkItem(key, {"population": record,
                                "record": batch.slice(position - offset, 1).to_pylist()[0]}))
                    offset += batch.num_rows
        if len(items) != len(selected):
            raise ValueError("registered GIFT-Eval source rows are missing")
        return self.store.plan("01.02", sorted(items, key=lambda item: item.key))

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

    def archived_methods(self) -> list[str]:
        """Return the creation-time configured archive methods, or no import work."""
        return self.context.configuration["archived_forecasts"]["enabled"]

    def pending_archived_forecasts(self) -> list[WorkItem]:
        """Plan original competition windows and reuse already accepted forecasts.

        The official key is the same one Process03 assigns; no modelling inputs
        or M4 observations are needed. Earlier Process04 archives remain intact.
        """
        methods = self.archived_methods()
        if not methods:
            return []
        items = []
        for series_key, source in self.store.rows("01.02").items():
            input_key = f"{series_key}/official/{source['test_start']:09d}"
            for model in methods:
                items.append(WorkItem(f"{input_key}/{model}", {"input_key": input_key, "model": model,
                    "request": {"dataset_id": source["dataset_id"], "series_id": source["series_id"],
                        "official_position": source["source_row"], "horizon": source["horizon"],
                        "forecast_ids": [model]}}))
        accepted = self.store.keys("04.01")
        completed = self.store.keys("01.04")
        reused = [(item.key, {"subflow_id": "04.01", "atomic_key": item.key})
                  for item in items if item.key in accepted and item.key not in completed]
        if reused:
            self.store.accept("01.04", reused)
        self.store.plan("01.04", items)
        return self.store.plan("04.01", items)

    def import_archived_forecasts(self, items: list[WorkItem], on_progress: Callable) -> None:
        """Retrieve missing vectors once on the coordinator and commit each item.

        This is source retrieval, not distributed fitting. A single R invocation
        forces the archive and returns ordinary values; each forecast and its
        import completion commit together, allowing restart after any item.
        """
        started = time.monotonic()
        if items:
            timeout = self.context.configuration["execution"]["default"]["worker_timeouts_seconds"]["r"]
            response = run_r("src/r/p01_import/archived_forecast_worker.R", {
                "action": "reference_forecast", "jobs": [item.payload["request"] for item in items]},
                timeout=timeout, threads=1)
            records = {(row["dataset_id"], row["series_id"], row["forecast_id"]): row
                       for row in response["records"]}
            expected = {(item.payload["request"]["dataset_id"], item.payload["request"]["series_id"],
                         item.payload["model"]) for item in items}
            if len(records) != len(response["records"]) or records.keys() != expected:
                raise ValueError("01.04: archive response does not match requested forecast identities")
            for item in items:
                request = item.payload["request"]
                row = records[(request["dataset_id"], request["series_id"], item.payload["model"])]
                self.store.accept("04.01", [(item.key, {**row, "input_key": item.payload["input_key"],
                    "model": item.payload["model"], "output_scale": "original", "origin": "imported"})],
                    completion_subflow="01.04")
        self.store.require_complete("01.04")
        done, total = self.store.counts("01.04")
        on_progress(ProgressSnapshot("01.04", done, total, (time.monotonic() - started) / 60, None, None))
