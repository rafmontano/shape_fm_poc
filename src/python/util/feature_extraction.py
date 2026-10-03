# ==============================================================================
# feature_extraction.py
#
# Purpose: Optionally calculate, persist, and retrieve reusable R feature rows.
# Inputs: Validated parent/window databases, prepared inputs, periods, and workers.
# Outputs: Provenance-matched feature rows and restart/failure summaries.
# Run from: Imported; not run directly.
# ==============================================================================

"""Optional FFORMA-base feature extraction over persisted prepared windows."""

from __future__ import annotations

import json
import math
import time
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import dask
import duckdb
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from prefect.context import get_run_context
from prefect.futures import as_completed
from prefect_dask import DaskTaskRunner

from .p01_02_import_execution import repository_root
from .shared_configuration import canonical_json, json_fingerprint
from .shared_database import load_database_configuration
from .shared_distributed_execution import feature_extraction_batch
from .shared_execution_profiles import APPROVED_HEAVY_TUNING_PROFILE
from .window_preparation import resolve_r_period


# Code constants: ID 014 has one approved base set and an additive child schema.
FEATURE_SET_ID = "fforma_base_v1"
FEATURE_SCHEMA_VERSION = 4
FEATURE_PROVIDER_SCRIPT = "src/r/feature_provider.R"
MAX_LOCAL_FEATURE_WINDOWS = 200

FEATURE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS feature_set_definitions (
    preparation_id VARCHAR NOT NULL,
    feature_set_id VARCHAR NOT NULL,
    version INTEGER NOT NULL,
    provider VARCHAR NOT NULL,
    feature_names JSON NOT NULL,
    scale_policy VARCHAR NOT NULL,
    missing_policy VARCHAR NOT NULL,
    fallback_policy VARCHAR NOT NULL,
    directional_features_enabled BOOLEAN NOT NULL,
    PRIMARY KEY (preparation_id, feature_set_id)
);
CREATE TABLE IF NOT EXISTS window_features (
    window_id VARCHAR NOT NULL,
    feature_set_id VARCHAR NOT NULL,
    provenance_fingerprint VARCHAR NOT NULL,
    status VARCHAR NOT NULL CHECK (status IN ('success', 'failed')),
    feature_values DOUBLE[],
    prepared_input_hash VARCHAR NOT NULL,
    transformation_recipe VARCHAR NOT NULL,
    transformation_state_hash VARCHAR NOT NULL,
    period INTEGER NOT NULL,
    dependency_identity JSON NOT NULL,
    source_manifest_hash VARCHAR NOT NULL,
    worker_provenance JSON NOT NULL,
    attempt_count INTEGER NOT NULL,
    last_error VARCHAR,
    created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (window_id, feature_set_id, provenance_fingerprint)
);
"""


@dataclass(frozen=True)
class PreparedFeatures:
    """One accepted feature row with explicit schema and provenance."""

    window_id: str
    feature_set_id: str
    feature_names: tuple[str, ...]
    feature_values: tuple[float, ...]
    provenance_fingerprint: str
    period: int
    prepared_input_hash: str
    transformation_recipe: str
    transformation_state_hash: str
    dependency_identity: dict[str, Any]
    source_manifest_hash: str
    worker_provenance: dict[str, Any]


def feature_provenance_fingerprint(
    provider: dict[str, Any], source_manifest_hash: str
) -> str:
    """Identify one reusable provider/dependency/source implementation contract."""
    return json_fingerprint(
        {
            "feature_set_id": provider["feature_set_id"],
            "feature_set_version": provider["feature_set_version"],
            "feature_names": provider["feature_names"],
            "dependencies": provider["dependencies"],
            "source_manifest_hash": source_manifest_hash,
        }
    )


def validate_feature_provider(provider: dict[str, Any]) -> None:
    """Reject malformed, directional, non-finite, or unnamed provider contracts."""
    required = {
        "feature_set_id", "feature_set_version", "feature_names", "scale_policy",
        "missing_policy", "fallback_policy", "directional_features_enabled",
        "disabled_directional_feature_names", "directional_features_disabled_reason",
        "dependencies",
    }
    if set(provider) != required:
        raise RuntimeError("feature provider description has an incompatible schema")
    names = provider["feature_names"]
    if (
        provider["feature_set_id"] != FEATURE_SET_ID
        or provider["feature_set_version"] != 1
        or provider["directional_features_enabled"] is not False
        or not isinstance(names, list)
        or len(names) != 42
        or len(set(names)) != len(names)
        or any(not isinstance(name, str) or not name for name in names)
    ):
        raise RuntimeError("feature provider identity, names, or directional policy is incompatible")


def _provider_definition(provider: dict[str, Any]) -> dict[str, Any]:
    """Return scientific fields that must agree across eligible worker hosts."""
    return {key: value for key, value in provider.items() if key != "dependencies"}


@task(name="compute feature batch", cache_policy=NO_CACHE, persist_result=False)
def compute_feature_batch(
    batch: list[dict[str, Any]],
    script: str,
    timeout: float,
    threads: int,
    memory_safety: dict[str, Any] | None,
) -> dict[str, Any]:
    """Run one storage-free bounded R feature request on an eligible worker."""
    return feature_extraction_batch(
        batch,
        script,
        timeout,
        threads,
        retry_count=get_run_context().task_run.run_count - 1,
        memory_safety=memory_safety,
    )


class FeatureExtractionCoordinator:
    """Read prepared inputs and act as the sole writer of reusable feature rows."""

    def __init__(self, parent_database: Path, windows_database: Path):
        """Open one immutable parent and writable existing child database."""
        self.root = repository_root()
        self.parent_path = parent_database.resolve()
        self.child_path = windows_database.resolve()
        if not self.parent_path.is_file() or not self.child_path.is_file():
            raise FileNotFoundError("feature extraction requires existing parent and windows databases")
        self.parent = duckdb.connect(str(self.parent_path), read_only=True)
        self.child = duckdb.connect(str(self.child_path))
        self.configuration = load_database_configuration(self.parent_path, self.parent)
        self.preparation_id = self._validate_lineage()

    def close(self) -> None:
        """Close coordinator-owned read and write connections."""
        self.child.close()
        self.parent.close()

    def __enter__(self) -> "FeatureExtractionCoordinator":
        """Return this open coordinator."""
        return self

    def __exit__(self, *_: object) -> None:
        """Release coordinator-owned database connections."""
        self.close()

    def _validate_lineage(self) -> str:
        """Require one completed child preparation matching the parent identity."""
        rows = self.child.execute(
            """SELECT preparation_id, parent_scientific_hash,
                      parent_configuration_hash FROM preparation_metadata"""
        ).fetchall()
        if len(rows) != 1:
            raise RuntimeError("windows database must contain one preparation identity")
        preparation_id, scientific_hash, configuration_hash = rows[0]
        if (
            scientific_hash != self.configuration.scientific_hash
            or configuration_hash != self.configuration.configuration_integrity_hash
        ):
            raise RuntimeError("windows database does not match the parent configuration")
        run = self.parent.execute(
            """SELECT child_database, status FROM window_preparation_runs
               WHERE preparation_id=?""",
            [preparation_id],
        ).fetchone()
        if (
            run is None
            or (self.parent_path.parent / run[0]).resolve() != self.child_path
            or run[1] != "completed"
        ):
            raise RuntimeError("parent preparation linkage is incomplete or mismatched")
        return str(preparation_id)

    def _ensure_schema(self, provider: dict[str, Any]) -> None:
        """Add and validate the optional feature schema only after explicit request."""
        validate_feature_provider(provider)
        self.child.execute("BEGIN TRANSACTION")
        try:
            self.child.execute(FEATURE_SCHEMA_SQL)
            self.child.execute(
                """INSERT INTO window_schema_versions VALUES (?, current_timestamp, ?)
                   ON CONFLICT (version) DO NOTHING""",
                [FEATURE_SCHEMA_VERSION, "Add optional reusable FFORMA base features"],
            )
            values = [
                self.preparation_id,
                FEATURE_SET_ID,
                provider["feature_set_version"],
                "R tsfeatures with FFORMA adaptations",
                canonical_json(provider["feature_names"]),
                provider["scale_policy"],
                provider["missing_policy"],
                provider["fallback_policy"],
                provider["directional_features_enabled"],
            ]
            self.child.execute(
                """INSERT INTO feature_set_definitions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT (preparation_id, feature_set_id) DO NOTHING""",
                values,
            )
            stored = self.child.execute(
                """SELECT preparation_id, feature_set_id, version, provider,
                          feature_names, scale_policy, missing_policy,
                          fallback_policy, directional_features_enabled
                   FROM feature_set_definitions
                   WHERE preparation_id=? AND feature_set_id=?""",
                [self.preparation_id, FEATURE_SET_ID],
            ).fetchone()
            if (
                stored is None
                or list(stored[:4]) != values[:4]
                or json.loads(stored[4]) != provider["feature_names"]
                or list(stored[5:]) != values[5:]
            ):
                raise RuntimeError("persisted feature-set definition is incompatible")
            self.child.execute("COMMIT")
        except BaseException:
            self.child.execute("ROLLBACK")
            raise

    def _periods_by_series(self) -> tuple[dict[int, int], dict[str, dict[str, Any]]]:
        """Resolve each child's series key through parent frequency identities."""
        rows = self.parent.execute(
            """SELECT s.series_key, f.frequency FROM series_lookup s
               JOIN frequency_lookup f USING (frequency_key)"""
        ).fetchall()
        frequencies = sorted({str(row[1]) for row in rows})
        periods = {
            frequency: resolve_r_period(self.root, self.configuration, frequency)
            for frequency in frequencies
        }
        return (
            {int(key): int(periods[str(frequency)]["r_period"]) for key, frequency in rows},
            periods,
        )

    def _window_count(self) -> int:
        """Count the complete request without materialising prepared arrays."""
        return int(
            self.child.execute(
                "SELECT count(*) FROM prepared_windows WHERE preparation_id=?",
                [self.preparation_id],
            ).fetchone()[0]
        )

    def _configure_pending_selection(
        self,
        periods_by_series: dict[int, int],
        allowed_provenance: set[str],
    ) -> None:
        """Install bounded database-side identities used by pending-page queries."""
        self.child.execute(
            "CREATE OR REPLACE TEMP TABLE feature_series_periods "
            "(series_key BIGINT PRIMARY KEY, period INTEGER NOT NULL)"
        )
        self.child.executemany(
            "INSERT INTO feature_series_periods VALUES (?, ?)",
            sorted(periods_by_series.items()),
        )
        self.child.execute(
            "CREATE OR REPLACE TEMP TABLE allowed_feature_provenance "
            "(provenance_fingerprint VARCHAR PRIMARY KEY)"
        )
        self.child.executemany(
            "INSERT INTO allowed_feature_provenance VALUES (?)",
            [(value,) for value in sorted(allowed_provenance)],
        )

    @staticmethod
    def _pending_condition() -> str:
        """Return the exact cache-miss predicate shared by counts and pages."""
        return """
            NOT EXISTS (
                SELECT 1
                FROM window_features f
                JOIN allowed_feature_provenance a
                  ON a.provenance_fingerprint=f.provenance_fingerprint
                WHERE f.window_id=w.window_id
                  AND f.feature_set_id=?
                  AND f.status='success'
                  AND f.prepared_input_hash=w.transformed_hash
                  AND f.transformation_recipe=json_extract_string(
                      w.transformation_state, '$.recipe'
                  )
                  AND f.transformation_state_hash=sha256(
                      CAST(w.transformation_state AS VARCHAR)
                  )
                  AND f.period=p.period
            )
        """

    def _pending_count(self) -> int:
        """Count exact cache misses without reading any transformed input."""
        return int(
            self.child.execute(
                f"""SELECT count(*)
                    FROM prepared_windows w
                    JOIN feature_series_periods p USING (series_key)
                    WHERE w.preparation_id=? AND {self._pending_condition()}""",
                [self.preparation_id, FEATURE_SET_ID],
            ).fetchone()[0]
        )

    def _pending_page(
        self,
        after: tuple[int, int, str] | None,
        page_size: int,
    ) -> tuple[list[dict[str, Any]], tuple[int, int, str] | None]:
        """Read one stable keyset page containing only pending prepared arrays."""
        keyset = ""
        parameters: list[Any] = [self.preparation_id, FEATURE_SET_ID]
        if after is not None:
            keyset = "AND (w.series_key, w.window_ordinal, w.window_id) > (?, ?, ?)"
            parameters.extend(after)
        parameters.append(page_size)
        rows = self.child.execute(
            f"""SELECT w.window_id, w.series_key, w.window_ordinal,
                       w.transformed_input, w.transformed_hash,
                       w.transformation_state, p.period
                FROM prepared_windows w
                JOIN feature_series_periods p USING (series_key)
                WHERE w.preparation_id=? AND {self._pending_condition()}
                  {keyset}
                ORDER BY w.series_key, w.window_ordinal, w.window_id
                LIMIT ?""",
            parameters,
        ).fetchall()
        jobs: list[dict[str, Any]] = []
        for window_id, series_key, ordinal, values, input_hash, state_json, period in rows:
            state = json.loads(state_json)
            jobs.append(
                {
                    "window_id": str(window_id),
                    "transformed_input": list(values),
                    "prepared_input_hash": str(input_hash),
                    "transformation_recipe": str(state["recipe"]),
                    "transformation_state_hash": json_fingerprint(state),
                    "period": int(period),
                }
            )
        next_key = None
        if rows:
            next_key = (int(rows[-1][1]), int(rows[-1][2]), str(rows[-1][0]))
        return jobs, next_key

    @staticmethod
    def _batches(values: list[dict[str, Any]], size: int) -> Iterable[list[dict[str, Any]]]:
        """Yield deterministic bounded slices without a second scheduler."""
        for start in range(0, len(values), size):
            yield values[start : start + size]

    def _commit_response(
        self,
        response: dict[str, Any],
        source_manifest_hash: str,
        allowed_provenance: set[str],
    ) -> tuple[int, int]:
        """Validate and atomically persist successes and explicit failures."""
        provider = response["provider"]
        validate_feature_provider(provider)
        provenance = feature_provenance_fingerprint(provider, source_manifest_hash)
        if provenance not in allowed_provenance:
            raise RuntimeError("feature worker dependency provenance was not preflighted")
        dependency = canonical_json(provider["dependencies"])
        worker = canonical_json(response["worker"])
        succeeded = failed = 0
        self.child.execute("BEGIN TRANSACTION")
        try:
            for result in response["results"]:
                status = result["status"]
                values = result.get("feature_values")
                error = result.get("error")
                if status == "success":
                    if result.get("feature_names") != provider["feature_names"]:
                        raise RuntimeError("feature result names differ from the provider schema")
                    if (
                        not isinstance(values, list)
                        or len(values) != len(provider["feature_names"])
                        or any(not math.isfinite(float(value)) for value in values)
                    ):
                        raise RuntimeError("successful feature row is incomplete or non-finite")
                    values = [float(value) for value in values]
                    error = None
                    succeeded += 1
                elif status == "failed" and isinstance(error, str) and error:
                    values = None
                    failed += 1
                else:
                    raise RuntimeError("feature result has invalid success/failure content")
                self.child.execute(
                    """INSERT INTO window_features VALUES
                       (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, current_timestamp, current_timestamp)
                       ON CONFLICT (window_id, feature_set_id, provenance_fingerprint)
                       DO UPDATE SET status=excluded.status, feature_values=excluded.feature_values,
                         prepared_input_hash=excluded.prepared_input_hash,
                         transformation_recipe=excluded.transformation_recipe,
                         transformation_state_hash=excluded.transformation_state_hash,
                         period=excluded.period, dependency_identity=excluded.dependency_identity,
                         source_manifest_hash=excluded.source_manifest_hash,
                         worker_provenance=excluded.worker_provenance,
                         attempt_count=window_features.attempt_count+1,
                         last_error=excluded.last_error, updated_at=now()""",
                    [
                        result["id"], FEATURE_SET_ID, provenance, status, values,
                        result["prepared_input_hash"], result["transformation_recipe"],
                        result["transformation_state_hash"], result["period"],
                        dependency, source_manifest_hash, worker, error,
                    ],
                )
            self.child.execute("COMMIT")
        except BaseException:
            self.child.execute("ROLLBACK")
            raise
        return succeeded, failed

    def run(
        self,
        *,
        provider_descriptions: list[dict[str, Any]],
        source_manifest_hash: str,
        local_max_windows: int | None,
        distributed: bool,
        memory_safety: dict[str, Any] | None,
        max_in_flight: int,
        retries: int,
    ) -> dict[str, Any]:
        """Identify missing rows, compute bounded batches, and commit on the Mac."""
        started = time.monotonic()
        if not provider_descriptions:
            raise RuntimeError("feature extraction requires preflighted provider descriptions")
        if not source_manifest_hash:
            raise RuntimeError("feature extraction requires a source manifest identity")
        for provider in provider_descriptions:
            validate_feature_provider(provider)
        definition = _provider_definition(provider_descriptions[0])
        if any(
            _provider_definition(provider) != definition
            for provider in provider_descriptions[1:]
        ):
            raise RuntimeError("feature workers do not share one scientific provider contract")
        allowed = {
            feature_provenance_fingerprint(provider, source_manifest_hash)
            for provider in provider_descriptions
        }
        periods_by_series, periods = self._periods_by_series()
        total_windows = self._window_count()
        if not distributed:
            if local_max_windows is None:
                raise RuntimeError("local feature extraction requires an explicit window bound")
            if local_max_windows < 1 or local_max_windows > MAX_LOCAL_FEATURE_WINDOWS:
                raise ValueError(
                    f"local feature bound must be within 1-{MAX_LOCAL_FEATURE_WINDOWS}"
                )
            if total_windows > local_max_windows:
                raise RuntimeError(
                    f"local feature extraction exceeds its bound: {total_windows}/{local_max_windows}"
                )
        elif local_max_windows is not None:
            raise ValueError("distributed feature extraction cannot use a local bound")
        self._ensure_schema(provider_descriptions[0])
        self._configure_pending_selection(periods_by_series, allowed)
        pending_windows = self._pending_count()
        reused = total_windows - pending_windows
        batch_size = int(self.configuration.execution["batch_sizes"]["window_preparation"])
        if max_in_flight < 1:
            raise ValueError("feature extraction max_in_flight must be positive")
        page_size = batch_size * max_in_flight
        script = FEATURE_PROVIDER_SCRIPT
        timeout = float(self.configuration.execution["worker_timeouts_seconds"]["r"])
        threads = int(self.configuration.execution["thread_limits"]["r"])
        task_runner = compute_feature_batch.with_options(retries=retries)
        succeeded = failed = 0
        worker_counts: dict[str, int] = {}
        submitted_rows = 0
        array_rows_read = 0
        page_count = 0
        max_page_rows = 0
        max_live_payload_rows = 0
        max_live_payload_value_bytes = 0
        after = None
        while True:
            page, next_key = self._pending_page(after, page_size)
            if not page:
                break
            page_count += 1
            array_rows_read += len(page)
            max_page_rows = max(max_page_rows, len(page))
            max_live_payload_rows = max(max_live_payload_rows, len(page))
            max_live_payload_value_bytes = max(
                max_live_payload_value_bytes,
                sum(len(job["transformed_input"]) * 8 for job in page),
            )
            submitted: dict[Any, int] = {}
            for batch in self._batches(page, batch_size):
                annotation = (
                    dask.annotate(resources={"CPU": 1}, retries=0)
                    if distributed
                    else nullcontext()
                )
                with annotation:
                    future = task_runner.submit(batch, script, timeout, threads, memory_safety)
                submitted[future] = len(batch)
                submitted_rows += len(batch)
            page = []
            while submitted:
                future = next(as_completed(list(submitted)))
                submitted.pop(future)
                response = future.result()
                worker = response["worker"]["hostname"]
                worker_counts[worker] = worker_counts.get(worker, 0) + len(response["results"])
                accepted, rejected = self._commit_response(
                    response, source_manifest_hash, allowed
                )
                succeeded += accepted
                failed += rejected
            after = next_key
        accepted_total = total_windows - self._pending_count()
        return {
            "preparation_id": self.preparation_id,
            "feature_set_id": FEATURE_SET_ID,
            "features_requested": True,
            "total_windows": int(total_windows),
            "reused_feature_rows": reused,
            "submitted_feature_rows": submitted_rows,
            "successful_feature_rows": int(accepted_total),
            "failed_feature_rows": failed,
            "feature_preparation_complete": int(accepted_total) == int(total_windows),
            "periods": periods,
            "worker_window_counts": worker_counts,
            "batch_size": batch_size,
            "page_size": page_size,
            "page_count": page_count,
            "array_rows_read": array_rows_read,
            "max_page_rows": max_page_rows,
            "max_live_payload_rows": max_live_payload_rows,
            "max_live_payload_value_bytes": max_live_payload_value_bytes,
            "runtime_seconds": time.monotonic() - started,
        }


@flow(name="feature-extraction", persist_result=False, validate_parameters=False)
def feature_extraction_flow(
    parent_database: Path,
    windows_database: Path,
    *,
    provider_descriptions: list[dict[str, Any]],
    source_manifest_hash: str,
    local_max_windows: int | None,
    distributed: bool,
    memory_safety: dict[str, Any] | None,
    max_in_flight: int,
    retries: int,
) -> dict[str, Any]:
    """Own coordinator-local storage while Prefect/Dask execute R feature batches."""
    with FeatureExtractionCoordinator(parent_database, windows_database) as coordinator:
        return coordinator.run(
            provider_descriptions=provider_descriptions,
            source_manifest_hash=source_manifest_hash,
            local_max_windows=local_max_windows,
            distributed=distributed,
            memory_safety=memory_safety,
            max_in_flight=max_in_flight,
            retries=retries,
        )


def run_feature_extraction_flow(
    *,
    scheduler_address: str | None,
    parent_database: Path,
    windows_database: Path,
    provider_descriptions: list[dict[str, Any]],
    source_manifest_hash: str,
    local_max_windows: int | None,
    memory_safety: dict[str, Any] | None,
    execution_profile: str | None,
    max_in_flight: int,
    retries: int,
) -> dict[str, Any]:
    """Bind optional feature extraction to local Prefect or an existing scheduler."""
    distributed = scheduler_address is not None
    if distributed and execution_profile != APPROVED_HEAVY_TUNING_PROFILE:
        raise RuntimeError(
            "distributed feature extraction requires execution profile "
            + APPROVED_HEAVY_TUNING_PROFILE
        )
    selected = feature_extraction_flow
    if distributed:
        selected = feature_extraction_flow.with_options(
            task_runner=DaskTaskRunner(address=scheduler_address)
        )
    return selected(
        parent_database,
        windows_database,
        provider_descriptions=provider_descriptions,
        source_manifest_hash=source_manifest_hash,
        local_max_windows=local_max_windows,
        distributed=distributed,
        memory_safety=memory_safety,
        max_in_flight=max_in_flight,
        retries=retries,
    )


def get_prepared_features(
    windows_database: Path,
    *,
    window_id: str,
    feature_set_id: str = FEATURE_SET_ID,
) -> PreparedFeatures:
    """Retrieve the newest accepted row and validate its explicit schema."""
    child = duckdb.connect(str(windows_database.resolve()), read_only=True)
    try:
        definition = child.execute(
            """SELECT feature_names FROM feature_set_definitions
               WHERE feature_set_id=?""",
            [feature_set_id],
        ).fetchone()
        if definition is None:
            raise KeyError(f"feature set was not requested: {feature_set_id}")
        names = tuple(json.loads(definition[0]))
        rows = child.execute(
            """SELECT f.feature_values, f.provenance_fingerprint, f.period,
                      f.prepared_input_hash,
                      f.transformation_recipe,
                      f.transformation_state_hash, f.dependency_identity,
                      f.source_manifest_hash, f.worker_provenance,
                      w.transformed_hash, w.transformation_state
               FROM window_features f JOIN prepared_windows w USING (window_id)
               WHERE f.window_id=? AND feature_set_id=? AND status='success'
               ORDER BY f.updated_at DESC""",
            [window_id, feature_set_id],
        ).fetchall()
        if not rows:
            failure = child.execute(
                """SELECT last_error FROM window_features
                   WHERE window_id=? AND feature_set_id=? AND status='failed'
                   ORDER BY updated_at DESC LIMIT 1""",
                [window_id, feature_set_id],
            ).fetchone()
            reason = failure[0] if failure else "no accepted result"
            raise RuntimeError(f"features unavailable for {window_id}: {reason}")
        row = rows[0]
        values = tuple(float(value) for value in row[0])
        if len(values) != len(names) or any(not math.isfinite(value) for value in values):
            raise RuntimeError("persisted feature row is incomplete or non-finite")
        current_state = json.loads(row[10])
        if (
            str(row[3]) != str(row[9])
            or str(row[4]) != str(current_state["recipe"])
            or str(row[5]) != json_fingerprint(current_state)
        ):
            raise RuntimeError("persisted feature row does not match its prepared input provenance")
        return PreparedFeatures(
            window_id=window_id,
            feature_set_id=feature_set_id,
            feature_names=names,
            feature_values=values,
            provenance_fingerprint=str(row[1]),
            period=int(row[2]),
            prepared_input_hash=str(row[3]),
            transformation_recipe=str(row[4]),
            transformation_state_hash=str(row[5]),
            dependency_identity=json.loads(row[6]),
            source_manifest_hash=str(row[7]),
            worker_provenance=json.loads(row[8]),
        )
    finally:
        child.close()
