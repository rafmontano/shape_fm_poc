# ==============================================================================
# shared_process_storage.py
#
# Purpose: Own coordinator-local gate state and independent durable-output checks.
# Inputs: An initialized experiment DuckDB and its authoritative stored configuration.
# Outputs: Atomic gate transitions and validation evidence; never repairs research rows.
# Run from: Imported by the researcher action/workflow layer; not run directly.
# ==============================================================================

"""Coordinator-local process state and independent output validation."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import duckdb

from .shared_configuration import canonical_json, json_fingerprint
from .shared_transformations import transform


class ProcessStorage:
    """Own process SQL, transitions, and authoritative expected-output checks."""

    def __init__(self, database: Path):
        """Retain the coordinator-local path without opening a writer."""
        self.database = database.resolve()

    def transition(self, process_id: int, status: str, summary: dict[str, Any] | None = None,
                   error: str | None = None) -> None:
        """Persist one gate transition atomically through a short-lived writer."""
        connection = duckdb.connect(str(self.database))
        try:
            connection.execute("BEGIN TRANSACTION")
            if status == "running":
                connection.execute("""UPDATE experiment_processes SET status='running',
                    started_at=current_timestamp, completed_at=NULL, updated_at=current_timestamp,
                    summary=NULL, last_error=NULL WHERE process_id=?""", [process_id])
            else:
                connection.execute("""UPDATE experiment_processes SET status=?,
                    completed_at=current_timestamp, updated_at=current_timestamp,
                    summary=?, last_error=? WHERE process_id=?""",
                    [status, canonical_json(summary) if summary is not None else None, error, process_id])
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    def process_rows(self) -> list[tuple[int, str, dict[str, Any] | None]]:
        """Read ordered gate identities, state and decoded summaries."""
        connection = duckdb.connect(str(self.database), read_only=True)
        try:
            return [(int(row[0]), row[1], json.loads(row[2]) if row[2] else None)
                    for row in connection.execute(
                        "SELECT process_id, status, summary FROM experiment_processes ORDER BY process_id"
                    ).fetchall()]
        finally:
            connection.close()

    def forecast_requires_gpu(self, configured: bool) -> bool:
        """Resolve GPU need from pending durable work, or configured models before planning."""
        expected, _ = self._contracts(4)
        with duckdb.connect(str(self.database), read_only=True) as connection:
            total, pending = connection.execute(
                f"""SELECT count(*), count(*) FILTER (WHERE e.candidate='chronos_2'
                   AND (t.status IS NULL OR t.status!='completed'))
                   FROM ({expected}) e LEFT JOIN experiment_tasks t
                   ON {self._task_join(4)} AND t.stage=4"""
            ).fetchone()
        return bool(pending) if total else configured

    def validate(self, process_id: int, summary: dict[str, Any] | None = None) -> dict[str, Any]:
        """Compare authoritative planned identities with tasks and accepted outputs."""
        connection = duckdb.connect(str(self.database), read_only=True)
        try:
            if process_id == 1:
                from .shared_database import load_database_configuration

                configuration = load_database_configuration(self.database, connection)
                expected = configuration.series_count
                stored = int(connection.execute("SELECT count(*) FROM series").fetchone()[0])
                incomplete = int(connection.execute("SELECT count(*) FROM tasks WHERE status!='completed'").fetchone()[0])
                tasks = int(connection.execute("SELECT count(*) FROM tasks").fetchone()[0])
                missing = abs(stored - expected) + abs(tasks - expected)
                if not incomplete and not missing:
                    self._validate_import(connection, configuration)
            else:
                expected_sql, output_sql = self._contracts(process_id)
                expected = int(connection.execute(f"SELECT count(*) FROM ({expected_sql}) expected").fetchone()[0])
                task_count = int(connection.execute(
                    f"""SELECT count(*) FROM ({expected_sql}) e JOIN experiment_tasks t
                    ON {self._task_join(process_id)} AND t.stage=? AND t.status='completed'""",
                    [process_id],
                ).fetchone()[0])
                stored = int(connection.execute(f"SELECT count(*) FROM ({output_sql}) output").fetchone()[0])
                total_tasks = connection.execute(
                    "SELECT count(*) FROM experiment_tasks WHERE stage=?", [process_id]
                ).fetchone()[0]
                incomplete = abs(expected - task_count) + abs(expected - total_tasks)
                missing = abs(expected - stored)
                if not incomplete and not missing:
                    if process_id == 2:
                        self._validate_preprocessed(connection)
                    elif process_id == 3:
                        self._validate_transformed(connection)
                    elif process_id == 4:
                        self._validate_forecasts(connection)
        finally:
            connection.close()
        if incomplete or missing or expected <= 0:
            raise RuntimeError(
                f"completed Process {process_id} has incomplete tasks or missing stored output: "
                f"expected={expected}, incomplete={incomplete}, missing={missing}"
            )
        return {"output_validated": True, "expected_task_count": expected,
                "stored_task_count": stored}

    @staticmethod
    def _validate_import(connection: duckdb.DuckDBPyConnection, configuration: Any) -> None:
        """Validate Gate 1 identities, values, hashes and window lineage from configuration."""
        from .p01_03_gift_eval_source import ConfiguredGiftEvalSource
        from .p01_02_import_execution import SeriesTask, compute_series, repository_root
        from .shared_configuration import dataset_identity

        settings = configuration.import_settings
        horizon = int(settings["benchmark"]["prediction_length"])
        expected_window = f"{settings['benchmark']['term']}/000"
        source = ConfiguredGiftEvalSource(configuration, repository_root())
        snapshot = source.fingerprint()
        expected_dataset, _ = dataset_identity(settings, source.revision, snapshot["files"])
        authoritative = {}
        for record in source.records():
            task = SeriesTask(
                "validation", "validation", record.source_series_id,
                record.source_series_id, record.source_row, record.frequency,
                record.start_timestamp, record.target, horizon, expected_window,
                settings["benchmark"]["boundary_convention"],
            )
            authoritative[record.source_series_id] = (record, compute_series(task))
        rows = connection.execute(
            """SELECT t.dataset_id,t.series_id,s.source_series_id,s.source_row,s.frequency,
                      s.target,s.observation_count,s.content_hash,w.window_id,w.train_start,
                      w.train_end,w.validation_start,w.validation_end,w.test_start,w.test_end,
                      w.horizon,w.boundary_convention,s.start_timestamp,w.split_name
               FROM tasks t
               LEFT JOIN series s ON s.dataset_id=t.dataset_id AND s.series_id=t.series_id
               LEFT JOIN evaluation_windows w ON w.dataset_id=t.dataset_id
                 AND w.series_id=t.series_id AND w.window_id=?
               WHERE t.stage='import' AND t.status='completed'""",
            [expected_window],
        ).fetchall()
        for row in rows:
            (dataset_id, series_id, source_id, source_row, frequency, target, count,
             fingerprint, window_id, train_start, train_end, validation_start,
             validation_end, test_start, test_end, stored_horizon, boundary, start, split) = row
            length = len(target) if target is not None else -1
            expected = authoritative.get(series_id)
            values_match = (expected is not None and target is not None
                            and len(target) == len(expected[0].target)) and all(
                stored == raw or (stored is None and raw is not None and math.isnan(raw))
                for stored, raw in zip(target, expected[0].target, strict=True)
            )
            if (dataset_id != expected_dataset or expected is None or source_id != series_id
                    or source_row != expected[0].source_row or frequency != expected[0].frequency
                    or count != length or not values_match
                    or fingerprint != expected[1].content_hash
                    or start != expected[0].start_timestamp or split != "validation_and_test"
                    or window_id != expected_window or train_start != 0
                    or train_end != length - 2 * horizon
                    or validation_start != train_end
                    or validation_end != length - horizon
                    or test_start != validation_end or test_end != length
                    or stored_horizon != horizon
                    or boundary != settings["benchmark"]["boundary_convention"]):
                raise RuntimeError("stored import output failed identity, value, hash or lineage validation")
        if len(rows) != len(authoritative) or {row[1] for row in rows} != set(authoritative):
            raise RuntimeError("stored import membership differs from authoritative source")

    @staticmethod
    def _validate_preprocessed(connection: duckdb.DuckDBPyConnection) -> None:
        """Validate Gate 2 values and hashes against forecast contexts and variant contracts."""
        rows = connection.execute(
            """SELECT r.cleaning_method,v.cleaning_method,i.context_target,r.context_target,
                      r.input_hash,r.output_hash,r.preprocessing_status,
                      r.missing_count_before,r.missing_count_after,r.parent_result_id
               FROM experiment_tasks t
               JOIN preprocessed_series r ON r.experiment_id=t.experiment_id
                 AND r.forecast_instance_id=t.forecast_instance_id AND r.cleaning_method=t.candidate
               JOIN forecast_instances i ON i.forecast_instance_id=t.forecast_instance_id
               JOIN experiment_variants v ON v.experiment_id=t.experiment_id
                 AND v.cleaning_method=t.candidate
               WHERE t.stage=2"""
        ).fetchall()
        for method, expected_method, source, output, input_hash, output_hash, status, before, after, parent in rows:
            missing_before = sum(value is None or math.isnan(value) for value in source)
            missing_after = sum(value is None or math.isnan(value) for value in output)
            # R/jsonlite emits integral numbers without a decimal; historical
            # hashes precede DuckDB's DOUBLE[] conversion. Accept either existing
            # numeric encoding, never rewrite those historical fingerprints.
            native_json = [int(value) if value is not None and float(value).is_integer()
                           else value for value in output]
            if (method != expected_method or status != "success" or parent is not None
                    or input_hash != json_fingerprint(source)
                    or output_hash not in {json_fingerprint(output), json_fingerprint(native_json)}
                    or before != missing_before or after != missing_after or missing_after
                    or len(source) != len(output)
                    or any(not math.isfinite(value) for value in output)
                    or (method == "standard" and any(
                        original is not None and math.isfinite(original) and original != cleaned
                        for original, cleaned in zip(source, output)
                    ))):
                raise RuntimeError("stored preprocessing failed value, hash or lineage validation")

    @staticmethod
    def _validate_transformed(connection: duckdb.DuckDBPyConnection) -> None:
        """Recompute Gate 3 transformation values, state, hashes and parent lineage."""
        rows = connection.execute(
            """SELECT r.transformation_method,v.transformation_method,p.context_target,
                      r.transformed_target,r.parameters,r.input_hash,r.output_hash,
                      r.preprocessing_id,r.parent_result_id,p.preprocessing_id
               FROM experiment_tasks t
               JOIN transformed_series r ON r.experiment_id=t.experiment_id
                 AND r.variant_id=t.variant_id AND r.forecast_instance_id=t.forecast_instance_id
               JOIN experiment_variants v ON v.variant_id=t.variant_id
               LEFT JOIN preprocessed_series p ON p.experiment_id=t.experiment_id
                 AND p.forecast_instance_id=t.forecast_instance_id
                 AND p.cleaning_method=v.cleaning_method
               WHERE t.stage=3"""
        ).fetchall()
        for method, expected_method, source, output, parameters, input_hash, output_hash, pre_id, parent, expected_parent in rows:
            if source is None:
                raise RuntimeError("stored transformation has missing preprocessing lineage")
            calculated = transform(source, expected_method)
            stored_parameters = json.loads(parameters) if isinstance(parameters, str) else parameters
            if (method != expected_method or pre_id != expected_parent or parent != expected_parent
                    or input_hash != json_fingerprint(source)
                    or output != list(calculated.values)
                    or stored_parameters != calculated.parameters
                    or output_hash != json_fingerprint(output)):
                raise RuntimeError("stored transformation failed value, hash or lineage validation")

    @staticmethod
    def _task_join(process_id: int) -> str:
        """Return the stage-specific identity join, never a count-only match."""
        common = "t.experiment_id=e.experiment_id"
        if process_id == 6:
            return common + " AND t.variant_id=e.variant_id AND t.candidate=e.candidate"
        value = common + " AND t.forecast_instance_id=e.forecast_instance_id"
        if process_id == 2:
            return value + " AND t.candidate=e.candidate"
        value += " AND t.variant_id=e.variant_id"
        return value if process_id == 3 else value + " AND t.candidate=e.candidate"

    @staticmethod
    def _contracts(process_id: int) -> tuple[str, str]:
        """Derive required work from experiment inputs, variants and model settings."""
        instances = "experiments x JOIN forecast_instances i ON i.benchmark_configuration_id=x.benchmark_configuration_id"
        variants = instances + " JOIN experiment_variants v ON v.experiment_id=x.experiment_id"
        if process_id == 2:
            expected = f"SELECT DISTINCT x.experiment_id,i.forecast_instance_id,v.cleaning_method candidate FROM {variants}"
            output = expected + " JOIN preprocessed_series r ON r.experiment_id=x.experiment_id AND r.forecast_instance_id=i.forecast_instance_id AND r.cleaning_method=v.cleaning_method"
        elif process_id == 3:
            expected = f"SELECT x.experiment_id,i.forecast_instance_id,v.variant_id FROM {variants}"
            output = expected + " JOIN transformed_series r ON r.experiment_id=x.experiment_id AND r.forecast_instance_id=i.forecast_instance_id AND r.variant_id=v.variant_id"
        else:
            models = "json_keys(CAST(x.scientific_configuration AS JSON), '$.models')"
            combination = (
                "json_extract_string(CAST(x.scientific_configuration AS JSON), "
                "'$.pipeline.combination.method')"
            )
            candidates = models if process_id == 4 else f"list_append({models}, {combination})"
            if process_id == 6:
                expected = f"SELECT x.experiment_id,v.variant_id,c.candidate FROM experiments x JOIN experiment_variants v ON v.experiment_id=x.experiment_id, UNNEST({candidates}) c(candidate)"
                output = expected + " JOIN official_evaluations r ON r.experiment_id=x.experiment_id AND r.variant_id=v.variant_id AND r.candidate=c.candidate"
            else:
                expected = f"SELECT x.experiment_id,i.forecast_instance_id,v.variant_id,c.candidate FROM {variants}, UNNEST({candidates}) c(candidate)"
                output = expected + " JOIN forecasts r ON r.experiment_id=x.experiment_id AND r.forecast_instance_id=i.forecast_instance_id AND r.variant_id=v.variant_id AND r.candidate=c.candidate"
        return expected, output

    @staticmethod
    def _validate_forecasts(connection: duckdb.DuckDBPyConnection) -> None:
        """Check original-scale arrays, lineage and content identity independently."""
        rows = connection.execute(
            """SELECT f.mean, f.median, f.quantile_levels, f.quantiles, i.horizon,
                      f.scale, f.parent_result_id, s.transformation_id, f.content_hash,
                      f.forecast_capability
               FROM experiment_tasks t
               JOIN forecasts f ON f.experiment_id=t.experiment_id
                 AND f.variant_id=t.variant_id AND f.candidate=t.candidate
                 AND f.forecast_instance_id=t.forecast_instance_id
               JOIN forecast_instances i ON i.forecast_instance_id=t.forecast_instance_id
               LEFT JOIN transformed_series s ON s.experiment_id=t.experiment_id
                 AND s.variant_id=t.variant_id AND s.forecast_instance_id=t.forecast_instance_id
               WHERE t.stage=4"""
        ).fetchall()
        for (mean, median, levels, quantiles, horizon, scale, parent, source,
             fingerprint, capability) in rows:
            valid_shape = (
                capability == "mean_only" and median is None and levels is None
                and quantiles is None and len(mean or []) == horizon
            ) or (
                capability == "probabilistic" and median is not None and levels
                and quantiles and len(levels) == len(quantiles)
                and all(len(values) == horizon for values in [mean, median, *quantiles])
            )
            values = [mean] if capability == "mean_only" else [mean, median, *quantiles]
            current_hash = json_fingerprint({
                "capability": capability, "scale": "original", "mean": tuple(mean),
                "median": None if median is None else tuple(median),
                "quantile_levels": levels,
                "quantiles": None if quantiles is None else [tuple(value) for value in quantiles],
            })
            legacy_hash = json_fingerprint({"mean": mean, "quantiles": quantiles})
            valid_hash = fingerprint in {current_hash, legacy_hash}
            if (scale != "original" or source is None or parent != source
                    or mean is None or not valid_shape
                    or any(not math.isfinite(value) for array in values for value in array)
                    or not valid_hash):
                raise RuntimeError("stored forecast output failed value, horizon, hash or lineage validation")
