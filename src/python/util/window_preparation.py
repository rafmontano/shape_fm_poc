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
import os
import subprocess
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal, ROUND_FLOOR
from math import floor, isfinite
from pathlib import Path
from typing import Any, Iterable

import duckdb

from .configuration import ExperimentConfiguration, canonical_json, json_fingerprint
from .database import load_database_configuration, migrate_database
from .distributed_execution import run_batches, window_preparation_batch
from .import_execution import repository_root


# Code constant: child schema is independent of the parent DuckDB migration number.
WINDOW_DATABASE_SCHEMA_VERSION = 1


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
    identities: list[tuple[str, str]], training_fraction: float, seed: int
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
    # Decimal preserves the approved mathematical 0.20 proportion at exact
    # boundaries (for example N=10 gives two, not a binary-float underflow to one).
    test_count = max(
        1,
        int(
            ((Decimal("1") - Decimal(str(training_fraction))) * len(ordered)).to_integral_value(
                rounding=ROUND_FLOOR
            )
        ),
    )
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
    }


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
        if self.configuration.version != 5:
            raise ValueError("rolling-window preparation requires configuration version 5")
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
            """SELECT s.dataset_id, s.series_id, s.source_row, s.frequency, s.target,
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
                "target": list(row[4]),
                "content_hash": row[5],
                "usable_start": int(row[6]),
                "usable_end": int(row[7]),
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
            settings = self.definition["frequencies"][item["frequency"]]
            length = item["usable_end"] - item["usable_start"]
            count = complete_window_count(
                length, settings["input_length"], settings["future_horizon"]
            )
            item["window_count"] = count
            item["unused_tail"] = length - count * settings["stride"]
            if count:
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
                    "max(1, floor((1 - training_fraction) * N))",
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
                    "INSERT INTO series_membership VALUES (?, 'S1', ?, ?, ?, ?, ?, ?, ?)",
                    [
                        self.preparation_id,
                        item["series_key"],
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

    def _jobs(self, series: list[dict[str, Any]], periods: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        """Build bounded serializable jobs only for incomplete eligible series."""
        pending = {
            int(row[0])
            for row in self.child.execute(
                """SELECT series_key FROM window_tasks
                   WHERE preparation_id=? AND status!='completed'""",
                [self.preparation_id],
            ).fetchall()
        }
        jobs = []
        for item in series:
            if item.get("series_key") not in pending:
                continue
            settings = self.definition["frequencies"][item["frequency"]]
            raw = item["target"][item["usable_start"] : item["usable_end"]]
            windows = []
            for window in rolling_window_inputs(
                raw,
                settings["input_length"],
                settings["future_horizon"],
                offset=item["usable_start"],
            ):
                identity = {
                    "preparation": self.preparation_id,
                    "series_key": item["series_key"],
                    "input_start": window["input_start"],
                    "input_end": window["input_end"],
                    "future_start": window["future_start"],
                    "future_end": window["future_end"],
                }
                windows.append(
                    {
                        **window,
                        "window_id": "prepared-window/" + json_fingerprint(identity)[:32],
                    }
                )
            if windows:
                jobs.append(
                    {
                        "id": str(item["series_key"]),
                        "series_key": item["series_key"],
                        "preprocessing_mode": self.definition["preprocessing_mode"],
                        "transformation": self.definition["transformation"],
                        "seasonality": periods[item["frequency"]]["r_period"],
                        "windows": windows,
                    }
                )
        return jobs

    def _commit_series_result(self, result: dict[str, Any], response: dict[str, Any]) -> None:
        """Idempotently write one series' windows and mark its child task complete."""
        series_key = int(result["id"])
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
            self.child.execute(
                """UPDATE window_tasks SET status='completed', attempt_count=attempt_count+1,
                   last_error=NULL, updated_at=current_timestamp
                   WHERE preparation_id=? AND series_key=?""",
                [self.preparation_id, series_key],
            )
            self.child.execute("COMMIT")
        except BaseException:
            self.child.execute("ROLLBACK")
            raise

    def run(
        self,
        *,
        dask_client: Any = None,
        source_manifest_hash: str | None = None,
        memory_safety: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create/resume membership and prepare all incomplete selected series."""
        started = time.monotonic()
        series = self._selected_series()
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
        jobs = self._jobs(series, periods)
        batches = [
            jobs[offset : offset + int(self.configuration.execution["batch_sizes"]["window_preparation"])]
            for offset in range(0, len(jobs), int(self.configuration.execution["batch_sizes"]["window_preparation"]))
        ]
        script = self.configuration.execution_paths["r_preprocess_worker"]
        timeout = float(self.configuration.execution["worker_timeouts_seconds"]["r"])
        threads = int(self.configuration.execution["thread_limits"]["r"])
        try:
            if dask_client is None:
                responses = (
                    (batch, window_preparation_batch(batch, script, timeout, threads))
                    for batch in batches
                )
            else:
                responses = run_batches(
                    dask_client,
                    window_preparation_batch,
                    batches,
                    resources={"CPU": 1},
                    max_in_flight=int(self.configuration.execution["dask_max_in_flight"]),
                    retries=int(self.configuration.execution["dask_retries"]),
                    extra_arguments=(script, timeout, threads, memory_safety),
                )
            worker_counts: dict[str, int] = {}
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
        }
        self.parent.execute(
            """UPDATE window_preparation_runs SET status='completed', summary=?,
               error=NULL, completed_at=current_timestamp WHERE preparation_id=?""",
            [canonical_json(summary), self.preparation_id],
        )
        return summary


def get_prepared_window(
    parent_database: Path,
    windows_database: Path,
    *,
    dataset_id: str,
    series_id: str,
    window_ordinal: int,
) -> PreparedWindow:
    """Retrieve one transformed input and resolve its unchanged future from parent."""
    parent = duckdb.connect(str(parent_database.resolve()), read_only=True)
    child = duckdb.connect(str(windows_database.resolve()), read_only=True)
    try:
        parent_row = parent.execute(
            """SELECT l.series_key, s.frequency, s.target
               FROM series s JOIN dataset_lookup d USING (dataset_id)
               JOIN series_lookup l ON l.dataset_key=d.dataset_key AND l.series_id=s.series_id
               WHERE s.dataset_id=? AND s.series_id=?""",
            [dataset_id, str(series_id)],
        ).fetchone()
        if parent_row is None:
            raise KeyError(f"series {dataset_id}/{series_id} was not found")
        row = child.execute(
            """SELECT w.window_id, m.partition, w.input_start, w.input_end,
                      w.future_start, w.future_end, w.transformed_input,
                      w.transformation_state
               FROM prepared_windows w JOIN series_membership m
                 USING (preparation_id, series_key)
               WHERE w.series_key=? AND w.window_ordinal=?""",
            [parent_row[0], window_ordinal],
        ).fetchone()
        if row is None:
            raise KeyError(f"prepared window {dataset_id}/{series_id}/{window_ordinal} was not found")
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
