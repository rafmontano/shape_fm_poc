# ==============================================================================
# window_preparation.py
#
# Purpose: Prepare legacy fixed contexts and approved complete rolling windows.
# Inputs: Stored v4/v5 settings, canonical parent series, and optional Dask client.
# Outputs: Fixed contexts or restartable S1 memberships and transformed child windows.
# Run from: Imported; not run directly.
# ==============================================================================

"""Leakage-safe rolling-window preparation and persisted S1 membership."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import math
import os
import struct
import subprocess
import time
import uuid
from contextlib import nullcontext
from dataclasses import dataclass
from decimal import Decimal, ROUND_FLOOR
from math import floor, isfinite
from pathlib import Path
from typing import Any, Iterable

import duckdb
import dask
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from prefect.context import get_run_context
from prefect.futures import as_completed
from prefect_dask import DaskTaskRunner

from .configuration import ExperimentConfiguration, canonical_json, json_fingerprint
from .database import load_database_configuration, migrate_database
from .distributed_execution import window_preparation_batch
from .execution_profiles import APPROVED_HEAVY_TUNING_PROFILE
from .import_execution import repository_root


# Code constant: child schema is independent of the parent DuckDB migration number.
WINDOW_DATABASE_SCHEMA_VERSION = 2
# Operational safety bounds: direct local preparation is for focused checks,
# never a substitute for the approved two-host profile used by heavy workflows.
MAX_LOCAL_PREPARATION_SERIES = 100
MAX_LOCAL_PREPARATION_WINDOWS = 200


@task(name="compute window-preparation batch", cache_policy=NO_CACHE, persist_result=False)
def compute_window_preparation_batch(
    batch: list[dict[str, Any]],
    script: Path,
    timeout: float,
    threads: int,
    memory_safety: dict[str, Any] | None,
) -> dict[str, Any]:
    """Compute one bounded batch without receiving coordinator or storage state."""
    return window_preparation_batch(
        batch, script, timeout, threads, memory_safety,
        retry_count=get_run_context().task_run.run_count - 1,
    )


# Code constant: normalized child schema stores definitions and membership once;
# raw observations and future values remain exclusively in the parent database.
WINDOW_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS window_schema_versions (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    description VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS preparation_metadata (
    preparation_id VARCHAR PRIMARY KEY,
    parent_scientific_hash VARCHAR NOT NULL,
    parent_configuration_hash VARCHAR NOT NULL,
    definition_hash VARCHAR NOT NULL,
    definition JSON NOT NULL,
    source_manifest_hash VARCHAR,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);
CREATE TABLE IF NOT EXISTS frequency_definitions (
    preparation_id VARCHAR NOT NULL,
    frequency_key BIGINT NOT NULL,
    input_length INTEGER NOT NULL,
    future_horizon INTEGER NOT NULL,
    stride INTEGER NOT NULL,
    PRIMARY KEY (preparation_id, frequency_key)
);
CREATE TABLE IF NOT EXISTS split_definitions (
    preparation_id VARCHAR NOT NULL,
    split_id VARCHAR NOT NULL,
    training_fraction DOUBLE NOT NULL,
    test_count_rule VARCHAR NOT NULL,
    split_seed BIGINT NOT NULL,
    generator VARCHAR NOT NULL,
    generator_version VARCHAR NOT NULL,
    source_ordering VARCHAR NOT NULL,
    cohort_fingerprint VARCHAR NOT NULL,
    membership_fingerprint VARCHAR NOT NULL,
    membership_source VARCHAR NOT NULL,
    PRIMARY KEY (preparation_id, split_id)
);
CREATE TABLE IF NOT EXISTS series_membership (
    preparation_id VARCHAR NOT NULL,
    split_id VARCHAR NOT NULL,
    series_key BIGINT NOT NULL,
    source_content_hash VARCHAR NOT NULL,
    partition VARCHAR NOT NULL CHECK (partition IN ('train', 'test')),
    usable_start INTEGER NOT NULL,
    usable_end INTEGER NOT NULL,
    usable_length INTEGER NOT NULL,
    window_count INTEGER NOT NULL,
    unused_tail INTEGER NOT NULL,
    PRIMARY KEY (preparation_id, split_id, series_key)
);
CREATE TABLE IF NOT EXISTS window_tasks (
    preparation_id VARCHAR NOT NULL,
    series_key BIGINT NOT NULL,
    status VARCHAR NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error VARCHAR,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (preparation_id, series_key)
);
CREATE TABLE IF NOT EXISTS prepared_windows (
    window_id VARCHAR PRIMARY KEY,
    preparation_id VARCHAR NOT NULL,
    series_key BIGINT NOT NULL,
    window_ordinal INTEGER NOT NULL,
    input_start INTEGER NOT NULL,
    input_end INTEGER NOT NULL,
    future_start INTEGER NOT NULL,
    future_end INTEGER NOT NULL,
    transformed_input DOUBLE[] NOT NULL,
    transformation_state JSON NOT NULL,
    preprocessing_provenance JSON NOT NULL,
    package_versions JSON NOT NULL,
    worker_provenance JSON NOT NULL,
    input_hash VARCHAR NOT NULL,
    cleaned_hash VARCHAR NOT NULL,
    transformed_hash VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    UNIQUE (preparation_id, series_key, window_ordinal)
);
"""


@dataclass(frozen=True)
class PreparedWindow:
    """One read-only model input plus its raw future resolved from the parent."""

    window_id: str
    dataset_id: str
    series_id: str
    frequency: str
    partition: str
    input_start: int
    input_end: int
    future_start: int
    future_end: int
    transformed_input: tuple[float, ...]
    transformation_state: dict[str, Any]
    future: tuple[float, ...]


def prepare_context(
    values: Iterable[float], context_length: int
) -> tuple[float, ...]:
    """Return the configured trailing context, left-padding short histories.

    The caller supplies a validated scientific context length explicitly. This
    function does not infer it from sampling frequency or forecast horizon.
    """
    if isinstance(context_length, bool) or not isinstance(context_length, int) or context_length < 1:
        raise ValueError("context_length must be a positive integer")
    try:
        source = tuple(float(value) for value in values)
    except (TypeError, ValueError) as exc:
        raise ValueError("context values must be numeric") from exc
    if not source or any(not isfinite(value) for value in source):
        raise ValueError("context values must be nonempty and finite")
    if len(source) >= context_length:
        return source[-context_length:]
    return (source[0],) * (context_length - len(source)) + source


def stable_lookup_key(namespace: str, *values: str) -> int:
    """Return a deterministic positive 60-bit lookup key for canonical strings."""
    encoded = "\0".join((namespace, *values)).encode("utf-8")
    return int(hashlib.sha256(encoded).hexdigest()[:15], 16) + 1


def complete_window_count(length: int, input_length: int, future_horizon: int) -> int:
    """Return floor(L/(W+H)) after validating nonnegative length and positive sizes."""
    if length < 0 or input_length < 1 or future_horizon < 1:
        raise ValueError("length must be nonnegative and window sizes must be positive")
    return floor(length / (input_length + future_horizon))


def rolling_window_inputs(
    values: Iterable[float | None],
    input_length: int,
    future_horizon: int,
    *,
    offset: int = 0,
) -> tuple[dict[str, Any], ...]:
    """Generate complete tsai windows with zero-based, end-exclusive positions.

    The explicit tsai settings align windows to the beginning, advance by W+H,
    and reject padding. Missing input positions are retained as ``None`` for the
    Gate 2 worker; infinity is rejected before any cleaning occurs.
    """
    import numpy as np
    from tsai.data.preparation import SlidingWindow

    source: list[float | None] = []
    for value in values:
        if value is None or (isinstance(value, float) and value != value):
            source.append(None)
        else:
            converted = float(value)
            if not isfinite(converted):
                raise ValueError("rolling-window source cannot contain infinity")
            source.append(converted)
    stride = input_length + future_horizon
    expected = complete_window_count(len(source), input_length, future_horizon)
    if expected == 0:
        return ()
    numeric = np.asarray([np.nan if value is None else value for value in source])
    inputs, futures = SlidingWindow(
        window_len=input_length,
        horizon=future_horizon,
        stride=stride,
        start=0,
        pad_remainder=False,
        padding="post",
        check_leakage=True,
    )(numeric)
    if inputs is None or futures is None or len(inputs) != expected or len(futures) != expected:
        raise RuntimeError("tsai returned an unexpected complete-window count")
    windows = []
    for ordinal in range(expected):
        start = ordinal * stride
        input_start = offset + start
        input_end = input_start + input_length
        future_start = input_end
        future_end = future_start + future_horizon
        library_input = inputs[ordinal].reshape(-1).tolist()
        input_values = [None if not isfinite(float(value)) else float(value) for value in library_input]
        windows.append(
            {
                "window_ordinal": ordinal,
                "input_start": input_start,
                "input_end": input_end,
                "future_start": future_start,
                "future_end": future_end,
                "input": input_values,
            }
        )
    return tuple(windows)


def s1_partition(
    identities: list[tuple[str, str]],
    training_fraction: float,
    seed: int,
    test_count_rule: str = "r_double_floor_v1",
) -> tuple[dict[tuple[str, str], str], dict[str, Any]]:
    """Allocate eligible namespaced series with tsai's explicit-count splitter.

    tsai names its two-way held-out output ``valid``. ShapeFM supplies the exact
    approved test count to that two-way API and records the second output as S1
    ``test``; no validation partition is created or persisted.
    """
    import numpy as np
    from tsai.data.validation import TrainValidTestSplitter

    ordered = sorted(identities)
    if len(ordered) < 2:
        raise ValueError("S1 requires at least two eligible series per frequency")
    if test_count_rule == "r_double_floor_v1":
        # Python's float is the same IEEE-754 binary64 arithmetic used by R's
        # numeric type. Preserve the historical R expression literally.
        test_count = max(1, floor((1.0 - float(training_fraction)) * len(ordered)))
    elif test_count_rule == "decimal_floor_v1":
        # Configuration v5 used this exact-decimal interpretation. It remains
        # available only so existing v5 databases resume without reinterpretation.
        test_count = max(
            1,
            int(
                ((Decimal("1") - Decimal(str(training_fraction))) * len(ordered)).to_integral_value(
                    rounding=ROUND_FLOOR
                )
            ),
        )
    else:
        raise ValueError(f"unsupported S1 test-count rule: {test_count_rule}")
    if test_count >= len(ordered):
        raise ValueError("S1 must retain at least one training series")
    train, test = TrainValidTestSplitter(
        n_splits=1,
        valid_size=test_count,
        test_size=0,
        train_only=False,
        stratify=False,
        balance=False,
        shuffle=True,
        random_state=seed,
    )(np.arange(len(ordered)))
    train_indexes = [int(value) for value in train]
    test_indexes = [int(value) for value in test]
    if set(train_indexes) & set(test_indexes) or sorted(train_indexes + test_indexes) != list(range(len(ordered))):
        raise RuntimeError("tsai returned overlapping or incomplete S1 membership")
    membership = {
        identity: "test" if index in set(test_indexes) else "train"
        for index, identity in enumerate(ordered)
    }
    return membership, {
        "test_count": test_count,
        "generator": "tsai.TrainValidTestSplitter",
        "generator_version": importlib.metadata.version("tsai"),
        "source_ordering": "lexicographic(dataset_id, series_id)",
        "test_count_rule": test_count_rule,
    }


def _target_content_hash(values: Iterable[float | None]) -> str:
    """Recompute the canonical Gate 1 content hash for lineage validation."""
    target = list(values)
    if all(value is not None and not math.isnan(float(value)) for value in target):
        packed = struct.pack(f"<{len(target)}f", *(float(value) for value in target))
    else:
        parts = []
        for value in target:
            if value is None:
                parts.append(b"N")
            elif math.isnan(float(value)):
                parts.append(b"A")
            else:
                parts.append(b"V" + struct.pack("<f", float(value)))
        packed = b"".join(parts)
    return hashlib.sha256(packed).hexdigest()


def _initialize_child_database(
    path: Path,
    preparation_id: str,
    configuration: ExperimentConfiguration,
    definition: dict[str, Any],
    source_manifest_hash: str | None,
) -> None:
    """Atomically create a child database containing only immutable definitions."""
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    connection = duckdb.connect(str(temporary))
    try:
        connection.execute("BEGIN TRANSACTION")
        connection.execute(WINDOW_SCHEMA_SQL)
        connection.execute(
            "INSERT INTO window_schema_versions VALUES (?, current_timestamp, ?)",
            [WINDOW_DATABASE_SCHEMA_VERSION, "Rolling-window preparation and S1 membership"],
        )
        connection.execute(
            "INSERT INTO preparation_metadata VALUES (?, ?, ?, ?, ?, ?, current_timestamp)",
            [
                preparation_id,
                configuration.scientific_hash,
                configuration.configuration_integrity_hash,
                json_fingerprint(definition),
                canonical_json(definition),
                source_manifest_hash,
            ],
        )
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    os.replace(temporary, path)


class WindowPreparationCoordinator:
    """Sole writer for parent identities and one normalized child windows database."""

    def __init__(self, parent_database: Path, windows_database: Path):
        """Open and validate a v5 parent; child creation remains deferred to run()."""
        self.root = repository_root()
        self.parent_path = migrate_database(parent_database)
        self.child_path = windows_database.resolve()
        if self.parent_path == self.child_path:
            raise ValueError("windows database must differ from the parent database")
        self.parent = duckdb.connect(str(self.parent_path))
        self.configuration = load_database_configuration(self.parent_path, self.parent)
        if self.configuration.version not in {5, 6, 7}:
            raise ValueError("rolling-window preparation requires configuration version 5, 6 or 7")
        self.definition = self.configuration.resolved["pipeline"]["window_preparation"]
        self.preparation_id = "window-preparation/" + json_fingerprint(
            {
                "parent": self.configuration.scientific_hash,
                "definition": self.definition,
            }
        )[:32]

    def close(self) -> None:
        """Close the parent writer and any open child writer."""
        child = getattr(self, "child", None)
        if child is not None:
            child.close()
            self.child = None
        self.parent.close()

    def __enter__(self) -> "WindowPreparationCoordinator":
        """Return this open coordinator."""
        return self

    def __exit__(self, *_: object) -> None:
        """Release owned database connections."""
        self.close()

    def _resolve_period(self, frequency: str) -> dict[str, Any]:
        """Resolve the cleaning period through the pinned GIFT-Eval environment."""
        command = [
            str(self.root / self.configuration.resolved["evaluation"]["gift_eval"]["environment"] / "bin/python"),
            str(self.root / "src/python/06_01_evaluate_gift_eval.py"),
            "resolve-period",
            "--frequency",
            frequency,
        ]
        if self.configuration.r_period_override is not None:
            command.extend(["--override", str(self.configuration.r_period_override)])
        completed = subprocess.run(
            command,
            cwd=self.root,
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
        return json.loads(completed.stdout)

    def _selected_series(self) -> list[dict[str, Any]]:
        """Read the configured official prefix and protected training boundaries."""
        state = self.parent.execute(
            "SELECT status FROM experiment_processes WHERE process_id=1"
        ).fetchone()
        if state is None or state[0] != "completed":
            raise RuntimeError("window preparation requires completed Process 01")
        rows = self.parent.execute(
            """SELECT s.dataset_id, s.series_id, s.source_row, s.frequency,
                      s.content_hash, w.train_start, w.train_end
               FROM series s JOIN evaluation_windows w USING (dataset_id, series_id)
               WHERE s.frequency IN (SELECT unnest(?))
               ORDER BY s.dataset_id, s.source_row, s.series_id LIMIT ?""",
            [
                self.definition["selected_frequencies"],
                self.configuration.series_count,
            ],
        ).fetchall()
        if len(rows) != self.configuration.series_count:
            raise RuntimeError(
                f"expected {self.configuration.series_count} selected series; found {len(rows)}"
            )
        return [
            {
                "dataset_id": row[0],
                "series_id": row[1],
                "source_row": int(row[2]),
                "frequency": row[3],
                "content_hash": row[4],
                "usable_start": int(row[5]),
                "usable_end": int(row[6]),
            }
            for row in rows
        ]

    def _register_parent_lookups(self, series: list[dict[str, Any]]) -> None:
        """Persist deterministic numeric aliases without replacing canonical keys."""
        self.parent.execute("BEGIN TRANSACTION")
        try:
            for frequency in sorted(self.definition["frequencies"]):
                self.parent.execute(
                    "INSERT INTO frequency_lookup VALUES (?, ?) ON CONFLICT DO NOTHING",
                    [stable_lookup_key("frequency", frequency), frequency],
                )
            for dataset_id in sorted({item["dataset_id"] for item in series}):
                self.parent.execute(
                    "INSERT INTO dataset_lookup VALUES (?, ?) ON CONFLICT DO NOTHING",
                    [stable_lookup_key("dataset", dataset_id), dataset_id],
                )
            for item in series:
                dataset_key = stable_lookup_key("dataset", item["dataset_id"])
                item["dataset_key"] = dataset_key
                item["frequency_key"] = stable_lookup_key("frequency", item["frequency"])
                item["series_key"] = stable_lookup_key(
                    "series", item["dataset_id"], item["series_id"]
                )
                self.parent.execute(
                    "INSERT INTO series_lookup VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                    [item["series_key"], dataset_key, item["series_id"], item["frequency_key"]],
                )
            self.parent.execute("COMMIT")
        except BaseException:
            self.parent.execute("ROLLBACK")
            raise

    def _open_child(self, source_manifest_hash: str | None) -> None:
        """Create or validate the child identity before any membership or windows."""
        if not self.child_path.exists():
            _initialize_child_database(
                self.child_path,
                self.preparation_id,
                self.configuration,
                self.definition,
                source_manifest_hash,
            )
        self.child = duckdb.connect(str(self.child_path))
        row = self.child.execute(
            """SELECT parent_scientific_hash, parent_configuration_hash,
                      definition_hash, source_manifest_hash
               FROM preparation_metadata WHERE preparation_id=?""",
            [self.preparation_id],
        ).fetchone()
        expected = (
            self.configuration.scientific_hash,
            self.configuration.configuration_integrity_hash,
            json_fingerprint(self.definition),
        )
        if row is None or row[:3] != expected:
            raise RuntimeError("windows database does not match its parent/configuration")
        if source_manifest_hash is not None and row[3] != source_manifest_hash:
            raise RuntimeError("windows database source manifest does not match this run")

    def _create_or_validate_membership(
        self, series: list[dict[str, Any]]
    ) -> tuple[str, dict[str, Any]]:
        """Persist a new allocation once or validate and reuse its exact membership."""
        existing = self.child.execute(
            """SELECT cohort_fingerprint, membership_fingerprint, generator_version,
                      membership_source FROM split_definitions
               WHERE preparation_id=? AND split_id='S1'""",
            [self.preparation_id],
        ).fetchone()
        eligible = []
        for item in series:
            if item["window_count"]:
                eligible.append(item)
        cohort = [
            [item["dataset_id"], item["series_id"], item["content_hash"]]
            for item in sorted(eligible, key=lambda value: (value["dataset_id"], value["series_id"]))
        ]
        cohort_fingerprint = json_fingerprint(cohort)
        if existing is not None:
            if existing[0] != cohort_fingerprint:
                raise RuntimeError("eligible-series cohort changed since S1 membership creation")
            membership_rows = self.child.execute(
                """SELECT series_key, partition FROM series_membership
                   WHERE preparation_id=? AND split_id='S1' ORDER BY series_key""",
                [self.preparation_id],
            ).fetchall()
            observed = json_fingerprint([[int(key), partition] for key, partition in membership_rows])
            if observed != existing[1]:
                raise RuntimeError("persisted S1 membership fingerprint is invalid")
            return existing[1], {
                "eligible": len(eligible),
                "zero_window": len(series) - len(eligible),
                "membership_source": "persisted_resume",
                "generator_version": existing[2],
            }

        memberships: dict[tuple[str, str], str] = {}
        split_provenance = None
        for frequency in self.definition["selected_frequencies"]:
            pool = [item for item in eligible if item["frequency"] == frequency]
            identities = [(item["dataset_id"], item["series_id"]) for item in pool]
            if len(identities) < 2:
                raise RuntimeError(
                    f"frequency {frequency} has {len(identities)} eligible series; S1 requires two"
                )
            allocated, provenance = s1_partition(
                identities,
                self.definition["split"]["training_fraction"],
                self.definition["split"]["seed"],
                self.definition["split"].get("test_count_rule", "decimal_floor_v1"),
            )
            memberships.update(allocated)
            split_provenance = provenance
        rows = []
        for item in eligible:
            rows.append(
                [
                    item["series_key"],
                    memberships[(item["dataset_id"], item["series_id"])],
                ]
            )
        membership_fingerprint = json_fingerprint(sorted(rows))
        self.child.execute("BEGIN TRANSACTION")
        try:
            for frequency in self.definition["selected_frequencies"]:
                settings = self.definition["frequencies"][frequency]
                self.child.execute(
                    "INSERT INTO frequency_definitions VALUES (?, ?, ?, ?, ?)",
                    [
                        self.preparation_id,
                        stable_lookup_key("frequency", frequency),
                        settings["input_length"],
                        settings["future_horizon"],
                        settings["stride"],
                    ],
                )
            self.child.execute(
                "INSERT INTO split_definitions VALUES (?, 'S1', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    self.preparation_id,
                    self.definition["split"]["training_fraction"],
                    split_provenance["test_count_rule"],
                    self.definition["split"]["seed"],
                    split_provenance["generator"],
                    split_provenance["generator_version"],
                    split_provenance["source_ordering"],
                    cohort_fingerprint,
                    membership_fingerprint,
                    "generated_new_allocation",
                ],
            )
            for item in eligible:
                self.child.execute(
                    "INSERT INTO series_membership VALUES (?, 'S1', ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        self.preparation_id,
                        item["series_key"],
                        item["content_hash"],
                        memberships[(item["dataset_id"], item["series_id"])],
                        item["usable_start"],
                        item["usable_end"],
                        item["usable_end"] - item["usable_start"],
                        item["window_count"],
                        item["unused_tail"],
                    ],
                )
                self.child.execute(
                    "INSERT INTO window_tasks VALUES (?, ?, 'pending', 0, NULL, current_timestamp)",
                    [self.preparation_id, item["series_key"]],
                )
            self.child.execute("COMMIT")
        except BaseException:
            self.child.execute("ROLLBACK")
            raise
        return membership_fingerprint, {
            "eligible": len(eligible),
            "zero_window": len(series) - len(eligible),
            "membership_source": "generated_new_allocation",
            "generator_version": split_provenance["generator_version"],
        }

    def _jobs(
        self,
        series: list[dict[str, Any]],
        periods: dict[str, dict[str, Any]],
        windows_per_job: int,
    ) -> Iterable[dict[str, Any]]:
        """Yield bounded chunks, loading only one limited target slice at a time."""
        pending = {
            int(row[0])
            for row in self.child.execute(
                """SELECT series_key FROM window_tasks
                   WHERE preparation_id=? AND status!='completed'""",
                [self.preparation_id],
            ).fetchall()
        }
        for item in series:
            if item.get("series_key") not in pending:
                continue
            settings = self.definition["frequencies"][item["frequency"]]
            existing = {
                int(row[0])
                for row in self.child.execute(
                    """SELECT window_ordinal FROM prepared_windows
                       WHERE preparation_id=? AND series_key=?""",
                    [self.preparation_id, item["series_key"]],
                ).fetchall()
            }
            for first_ordinal in range(0, item["window_count"], windows_per_job):
                stop_ordinal = min(
                    first_ordinal + windows_per_job, item["window_count"]
                )
                if all(
                    ordinal in existing
                    for ordinal in range(first_ordinal, stop_ordinal)
                ):
                    continue
                # Read a bounded set of complete W+H blocks. tsai receives the
                # transient future values needed to create each window, but only
                # its returned inputs and positions leave this coordinator.
                block_start = item["usable_start"] + first_ordinal * settings["stride"]
                block_end = item["usable_start"] + stop_ordinal * settings["stride"]
                raw_row = self.parent.execute(
                    """SELECT list_slice(target, ?, ?) FROM series
                       WHERE dataset_id=? AND series_id=?""",
                    [block_start + 1, block_end, item["dataset_id"], item["series_id"]],
                ).fetchone()
                expected_length = (stop_ordinal - first_ordinal) * settings["stride"]
                if raw_row is None or len(raw_row[0]) != expected_length:
                    raise RuntimeError("bounded source block does not match complete W+H blocks")
                generated = rolling_window_inputs(
                    raw_row[0],
                    settings["input_length"],
                    settings["future_horizon"],
                    offset=block_start,
                )
                if len(generated) != stop_ordinal - first_ordinal:
                    raise RuntimeError("tsai returned an unexpected bounded chunk")
                windows = []
                for window in generated:
                    ordinal = first_ordinal + int(window["window_ordinal"])
                    if ordinal in existing:
                        continue
                    identity = {
                        "preparation": self.preparation_id,
                        "series_key": item["series_key"],
                        "input_start": window["input_start"],
                        "input_end": window["input_end"],
                        "future_start": window["future_start"],
                        "future_end": window["future_end"],
                    }
                    windows.append({
                        **window,
                        "window_ordinal": ordinal,
                        "window_id": "prepared-window/" + json_fingerprint(identity)[:32],
                    })
                yield {
                    "id": f"{item['series_key']}/{first_ordinal}",
                    "series_key": item["series_key"],
                    "preprocessing_mode": self.definition["preprocessing_mode"],
                    "transformation": self.definition["transformation"],
                    "seasonality": periods[item["frequency"]]["r_period"],
                    "windows": windows,
                }

    @staticmethod
    def _batches(jobs: Iterable[dict[str, Any]], jobs_per_batch: int) -> Iterable[list[dict[str, Any]]]:
        """Group a lazy job stream without materializing the remaining workload."""
        batch: list[dict[str, Any]] = []
        for job in jobs:
            batch.append(job)
            if len(batch) == jobs_per_batch:
                yield batch
                batch = []
        if batch:
            yield batch

    def _commit_series_result(self, result: dict[str, Any], response: dict[str, Any]) -> None:
        """Idempotently write one series' windows and mark its child task complete."""
        series_key = int(result["series_key"])
        self.child.execute("BEGIN TRANSACTION")
        try:
            for window in result["windows"]:
                self.child.execute(
                    """INSERT INTO prepared_windows VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                    ON CONFLICT (window_id) DO NOTHING""",
                    [
                        window["window_id"],
                        self.preparation_id,
                        series_key,
                        window["window_ordinal"],
                        window["input_start"],
                        window["input_end"],
                        window["future_start"],
                        window["future_end"],
                        window["transformed_input"],
                        canonical_json(window["transformation_state"]),
                        canonical_json(window["preprocessing"]),
                        canonical_json(response["packages"]),
                        canonical_json(response["worker"]),
                        window["input_hash"],
                        window["cleaned_hash"],
                        window["transformed_hash"],
                    ],
                )
            completed, expected = self.child.execute(
                """SELECT count(*), max(m.window_count) FROM prepared_windows w
                   JOIN series_membership m USING (preparation_id, series_key)
                   WHERE w.preparation_id=? AND w.series_key=?""",
                [self.preparation_id, series_key],
            ).fetchone()
            self.child.execute(
                """UPDATE window_tasks SET status=?, attempt_count=attempt_count+1,
                   last_error=NULL, updated_at=current_timestamp
                   WHERE preparation_id=? AND series_key=?""",
                ["completed" if completed == expected else "pending", self.preparation_id, series_key],
            )
            self.child.execute("COMMIT")
        except BaseException:
            self.child.execute("ROLLBACK")
            raise

    def _run_prefect_batches(
        self,
        batches: Iterable[list[dict[str, Any]]],
        *,
        script: Path,
        timeout: float,
        threads: int,
        memory_safety: dict[str, Any] | None,
        max_in_flight: int,
        retries: int,
        distributed: bool,
    ) -> dict[str, int]:
        """Submit bounded named tasks and commit each completed result on the Mac."""
        if max_in_flight < 1:
            raise ValueError("window preparation max_in_flight must be positive")
        pending = iter(batches)
        exhausted = False
        submitted: dict[Any, list[dict[str, Any]]] = {}
        worker_counts: dict[str, int] = {}
        compute_task = compute_window_preparation_batch.with_options(retries=retries)
        failure: Exception | None = None
        while submitted or not exhausted:
            while failure is None and not exhausted and len(submitted) < max_in_flight:
                try:
                    batch = next(pending)
                except StopIteration:
                    exhausted = True
                    break
                annotation = dask.annotate(resources={"CPU": 1}, retries=0) if distributed else nullcontext()
                try:
                    with annotation:
                        future = compute_task.submit(batch, script, timeout, threads, memory_safety)
                except Exception as error:
                    failure = error
                    break
                submitted[future] = batch
            if not submitted:
                break
            future = next(as_completed(list(submitted)))
            submitted.pop(future)
            try:
                response = future.result()
                hostname = response["worker"]["hostname"]
                worker_counts[hostname] = worker_counts.get(hostname, 0) + len(response["results"])
                for result in response["results"]:
                    self._commit_series_result(result, response)
            except Exception as error:
                failure = failure or error
        if failure is not None:
            raise failure
        return worker_counts

    def run(
        self,
        *,
        dask_client: Any = None,
        source_manifest_hash: str | None = None,
        memory_safety: dict[str, Any] | None = None,
        local_limits: dict[str, int] | None = None,
        execution_profile: str | None = None,
        dask_retries: int | None = None,
        prefect_compute: bool = False,
        max_in_flight: int | None = None,
    ) -> dict[str, Any]:
        """Create/resume membership and prepare all incomplete selected series.

        ``dask_retries`` optionally transfers retry ownership to Prefect by setting
        the inner Dask attempt count to zero without changing stored configuration.
        """
        if dask_retries is not None and dask_retries < 0:
            raise ValueError("Dask retries cannot be negative")
        if dask_client is None:
            if not isinstance(local_limits, dict) or set(local_limits) != {
                "max_series",
                "max_windows",
            }:
                raise RuntimeError(
                    "direct local window preparation requires explicit max_series and max_windows"
                )
            for field in ("max_series", "max_windows"):
                value = local_limits[field]
                if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                    raise ValueError(f"local preparation {field} must be a positive integer")
            if (
                local_limits["max_series"] > MAX_LOCAL_PREPARATION_SERIES
                or local_limits["max_windows"] > MAX_LOCAL_PREPARATION_WINDOWS
            ):
                raise RuntimeError(
                    "local preparation limits exceed the focused safety policy; "
                    f"maximums are {MAX_LOCAL_PREPARATION_SERIES} series and "
                    f"{MAX_LOCAL_PREPARATION_WINDOWS} windows"
                )
            if execution_profile is not None:
                raise ValueError("a local preparation cannot claim a distributed profile")
        elif (
            local_limits is not None
            or execution_profile != APPROVED_HEAVY_TUNING_PROFILE
        ):
            raise RuntimeError(
                "distributed window preparation requires the approved execution profile "
                f"{APPROVED_HEAVY_TUNING_PROFILE} and no local limits"
            )
        started = time.monotonic()
        series = self._selected_series()
        for item in series:
            settings = self.definition["frequencies"][item["frequency"]]
            length = item["usable_end"] - item["usable_start"]
            item["window_count"] = complete_window_count(
                length, settings["input_length"], settings["future_horizon"]
            )
            item["unused_tail"] = length - item["window_count"] * settings["stride"]
        total_planned_windows = sum(int(item["window_count"]) for item in series)
        if local_limits is not None and (
            len(series) > local_limits["max_series"]
            or total_planned_windows > local_limits["max_windows"]
        ):
            raise RuntimeError(
                "local window preparation exceeds its explicit bounds before writes: "
                f"series={len(series)}/{local_limits['max_series']}, "
                f"windows={total_planned_windows}/{local_limits['max_windows']}"
            )
        self._register_parent_lookups(series)
        self._open_child(source_manifest_hash)
        membership_fingerprint, membership_summary = self._create_or_validate_membership(series)
        child_reference = os.path.relpath(self.child_path, self.parent_path.parent)
        self.parent.execute(
            """INSERT INTO window_preparation_runs
            (preparation_id, child_database, definition_hash, parent_scientific_hash,
             parent_configuration_hash, membership_fingerprint, status)
            VALUES (?, ?, ?, ?, ?, ?, 'running')
            ON CONFLICT (preparation_id) DO UPDATE SET status='running', error=NULL,
              membership_fingerprint=excluded.membership_fingerprint, completed_at=NULL""",
            [
                self.preparation_id,
                child_reference,
                json_fingerprint(self.definition),
                self.configuration.scientific_hash,
                self.configuration.configuration_integrity_hash,
                membership_fingerprint,
            ],
        )
        periods = {
            frequency: self._resolve_period(frequency)
            for frequency in self.definition["selected_frequencies"]
        }
        jobs_per_batch = int(self.configuration.execution["batch_sizes"]["window_preparation"])
        windows_per_job = int(
            self.configuration.execution.get("window_preparation_windows_per_job", jobs_per_batch)
        )
        batches = self._batches(
            self._jobs(series, periods, windows_per_job), jobs_per_batch
        )
        script = self.configuration.execution_paths["r_preprocess_worker"]
        timeout = float(self.configuration.execution["worker_timeouts_seconds"]["r"])
        threads = int(self.configuration.execution["thread_limits"]["r"])
        try:
            if prefect_compute:
                worker_counts = self._run_prefect_batches(
                    batches,
                    script=script,
                    timeout=timeout,
                    threads=threads,
                    memory_safety=memory_safety,
                    max_in_flight=(max_in_flight if max_in_flight is not None
                                   else int(self.configuration.execution["dask_max_in_flight"])),
                    retries=(int(self.configuration.execution["dask_retries"])
                             if dask_retries is None else dask_retries),
                    distributed=dask_client is not None,
                )
            elif dask_client is None:
                responses = (
                    (batch, window_preparation_batch(batch, script, timeout, threads))
                    for batch in batches
                )
            else:
                raise RuntimeError(
                    "distributed window preparation must use run_window_preparation_flow"
                )
            if not prefect_compute:
                worker_counts = {}
                for _, response in responses:
                    hostname = response["worker"]["hostname"]
                    worker_counts[hostname] = worker_counts.get(hostname, 0) + len(response["results"])
                    for result in response["results"]:
                        self._commit_series_result(result, response)
        except BaseException as exc:
            error = f"{type(exc).__name__}: {exc}"
            self.parent.execute(
                "UPDATE window_preparation_runs SET status='failed', error=? WHERE preparation_id=?",
                [error, self.preparation_id],
            )
            raise
        counts = dict(
            self.child.execute(
                """SELECT partition, count(*) FROM series_membership
                   WHERE preparation_id=? GROUP BY partition""",
                [self.preparation_id],
            ).fetchall()
        )
        window_counts = dict(
            self.child.execute(
                """SELECT m.partition, count(*) FROM prepared_windows w
                   JOIN series_membership m USING (preparation_id, series_key)
                   WHERE w.preparation_id=? GROUP BY m.partition""",
                [self.preparation_id],
            ).fetchall()
        )
        summary = {
            "preparation_id": self.preparation_id,
            "windows_database": str(self.child_path),
            "membership_fingerprint": membership_fingerprint,
            **membership_summary,
            "series_by_partition": {key: int(value) for key, value in counts.items()},
            "windows_by_partition": {key: int(value) for key, value in window_counts.items()},
            "total_windows": sum(int(value) for value in window_counts.values()),
            "selected_frequencies": self.definition["selected_frequencies"],
            "periods": periods,
            "worker_series_counts": worker_counts,
            "runtime_seconds": time.monotonic() - started,
            "memory_bounds": {
                "windows_per_job": windows_per_job,
                "jobs_per_batch": jobs_per_batch,
                "maximum_windows_per_batch": windows_per_job * jobs_per_batch,
            },
            "local_limits": local_limits,
        }
        self.parent.execute(
            """UPDATE window_preparation_runs SET status='completed', summary=?,
               error=NULL, completed_at=current_timestamp WHERE preparation_id=?""",
            [canonical_json(summary), self.preparation_id],
        )
        return summary


@flow(name="window-preparation", persist_result=False, validate_parameters=False)
def window_preparation_flow(
    parent_database: Path,
    windows_database: Path,
    *,
    source_manifest_hash: str | None,
    memory_safety: dict[str, Any] | None,
    execution_profile: str | None,
    retries: int,
    distributed: bool,
    local_limits: dict[str, int] | None,
    max_in_flight: int | None,
) -> dict[str, Any]:
    """Open coordinator-local storage, schedule compute, and commit on the Mac."""
    with WindowPreparationCoordinator(parent_database, windows_database) as coordinator:
        return coordinator.run(
            dask_client=True if distributed else None,
            source_manifest_hash=source_manifest_hash,
            memory_safety=memory_safety,
            local_limits=local_limits,
            execution_profile=execution_profile,
            dask_retries=retries,
            prefect_compute=True,
            max_in_flight=max_in_flight,
        )


def run_window_preparation_flow(
    *, scheduler_address: str | None, parent_database: Path, windows_database: Path,
    source_manifest_hash: str | None, memory_safety: dict[str, Any] | None,
    execution_profile: str | None, retries: int,
    local_limits: dict[str, int] | None = None,
    max_in_flight: int | None = None,
) -> dict[str, Any]:
    """Bind window compute explicitly to an existing Dask scheduler."""
    distributed = scheduler_address is not None
    if distributed and local_limits is not None:
        raise ValueError("distributed window preparation cannot use local limits")
    if not distributed and local_limits is None:
        raise RuntimeError("local window flow requires explicit local limits")
    selected = window_preparation_flow
    if distributed:
        selected = window_preparation_flow.with_options(
            task_runner=DaskTaskRunner(address=scheduler_address)
        )
    return selected(
        parent_database, windows_database,
        source_manifest_hash=source_manifest_hash,
        memory_safety=memory_safety,
        execution_profile=execution_profile,
        retries=retries,
        distributed=distributed,
        local_limits=local_limits,
        max_in_flight=max_in_flight,
    )


def get_prepared_window(
    parent_database: Path,
    windows_database: Path,
    *,
    dataset_id: str,
    series_id: str,
    window_ordinal: int,
) -> PreparedWindow:
    """Validate parent/child lineage, then resolve one unchanged raw future."""
    parent_path = parent_database.resolve()
    child_path = windows_database.resolve()
    parent = duckdb.connect(str(parent_path), read_only=True)
    child = duckdb.connect(str(child_path), read_only=True)
    try:
        configuration = load_database_configuration(parent_path, parent)
        metadata_rows = child.execute(
            """SELECT preparation_id, parent_scientific_hash,
                      parent_configuration_hash, definition_hash, definition
               FROM preparation_metadata"""
        ).fetchall()
        if len(metadata_rows) != 1:
            raise RuntimeError("windows database must contain exactly one preparation identity")
        preparation_id, scientific_hash, configuration_hash, definition_hash, definition_json = metadata_rows[0]
        definition = json.loads(definition_json)
        if (
            scientific_hash != configuration.scientific_hash
            or configuration_hash != configuration.configuration_integrity_hash
            or definition_hash != json_fingerprint(definition)
            or definition != configuration.resolved["pipeline"]["window_preparation"]
        ):
            raise RuntimeError("windows database does not match the parent scientific configuration")
        run_rows = parent.execute(
            """SELECT child_database, definition_hash, parent_scientific_hash,
                      parent_configuration_hash, membership_fingerprint, status
               FROM window_preparation_runs WHERE preparation_id=?""",
            [preparation_id],
        ).fetchall()
        if len(run_rows) != 1:
            raise RuntimeError("parent database has no unique preparation-run linkage")
        run = run_rows[0]
        recorded_child = (parent_path.parent / run[0]).resolve()
        if (
            recorded_child != child_path
            or run[1] != definition_hash
            or run[2] != scientific_hash
            or run[3] != configuration_hash
            or run[5] != "completed"
        ):
            raise RuntimeError("parent preparation-run linkage is incomplete or mismatched")
        split_row = child.execute(
            """SELECT cohort_fingerprint, membership_fingerprint
               FROM split_definitions WHERE preparation_id=? AND split_id='S1'""",
            [preparation_id],
        ).fetchone()
        if split_row is None or split_row[1] != run[4]:
            raise RuntimeError("parent and child membership identities do not match")
        parent_row = parent.execute(
            """SELECT l.series_key, s.frequency, s.target, s.content_hash
               FROM series s JOIN dataset_lookup d USING (dataset_id)
               JOIN series_lookup l ON l.dataset_key=d.dataset_key AND l.series_id=s.series_id
               WHERE s.dataset_id=? AND s.series_id=?""",
            [dataset_id, str(series_id)],
        ).fetchone()
        if parent_row is None:
            raise KeyError(f"series {dataset_id}/{series_id} was not found")
        if _target_content_hash(parent_row[2]) != parent_row[3]:
            raise RuntimeError("parent series observations do not match their canonical content hash")
        membership_columns = {
            row[1] for row in child.execute("PRAGMA table_info('series_membership')").fetchall()
        }
        if "source_content_hash" in membership_columns:
            membership_source = child.execute(
                """SELECT source_content_hash FROM series_membership
                   WHERE preparation_id=? AND series_key=?""",
                [preparation_id, parent_row[0]],
            ).fetchone()
            if membership_source is None or membership_source[0] != parent_row[3]:
                raise RuntimeError("prepared series source identity does not match the parent")
        else:
            # Historical v5 children predate the per-series lineage column.
            cohort = parent.execute(
                """SELECT s.dataset_id, s.series_id, s.content_hash
                   FROM series s JOIN dataset_lookup d USING (dataset_id)
                   JOIN series_lookup l ON l.dataset_key=d.dataset_key AND l.series_id=s.series_id
                   WHERE l.series_key IN (SELECT unnest(?))
                   ORDER BY s.dataset_id, s.series_id""",
                [[int(row[0]) for row in child.execute(
                    "SELECT series_key FROM series_membership WHERE preparation_id=?",
                    [preparation_id],
                ).fetchall()]],
            ).fetchall()
            if json_fingerprint([list(row) for row in cohort]) != split_row[0]:
                raise RuntimeError("historical child cohort does not match the parent source lineage")
        row = child.execute(
            """SELECT w.window_id, m.partition, w.input_start, w.input_end,
                      w.future_start, w.future_end, w.transformed_input,
                      w.transformation_state, m.usable_start, m.usable_end,
                      f.input_length, f.future_horizon, f.stride
               FROM prepared_windows w JOIN series_membership m
                 USING (preparation_id, series_key)
               JOIN frequency_definitions f USING (preparation_id)
               WHERE w.preparation_id=? AND w.series_key=? AND w.window_ordinal=?
                 AND f.frequency_key=?""",
            [
                preparation_id,
                parent_row[0],
                window_ordinal,
                stable_lookup_key("frequency", parent_row[1]),
            ],
        ).fetchall()
        if len(row) != 1:
            raise KeyError(f"prepared window {dataset_id}/{series_id}/{window_ordinal} was not found")
        row = row[0]
        expected_input_start = int(row[8]) + window_ordinal * int(row[12])
        if (
            int(row[2]) != expected_input_start
            or int(row[3]) - int(row[2]) != int(row[10])
            or int(row[4]) != int(row[3])
            or int(row[5]) - int(row[4]) != int(row[11])
            or int(row[5]) > int(row[9])
            or len(row[6]) != int(row[10])
            or int(row[5]) > len(parent_row[2])
        ):
            raise RuntimeError("prepared window boundaries are inconsistent with persisted definitions")
        future = tuple(parent_row[2][row[4] : row[5]])
        return PreparedWindow(
            window_id=row[0],
            dataset_id=dataset_id,
            series_id=str(series_id),
            frequency=parent_row[1],
            partition=row[1],
            input_start=int(row[2]),
            input_end=int(row[3]),
            future_start=int(row[4]),
            future_end=int(row[5]),
            transformed_input=tuple(row[6]),
            transformation_state=json.loads(row[7]),
            future=future,
        )
    finally:
        child.close()
        parent.close()
