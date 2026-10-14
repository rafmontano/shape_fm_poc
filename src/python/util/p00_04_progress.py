# ==============================================================================
# p00_04_progress.py
# Purpose: Read legacy progress evidence and interpret/present human status.
# Inputs: Existing DuckDB and the status action's stored configuration snapshot.
# Outputs: Read-only evidence, interpreted progress and terminal text; no writes.
# Run from: Imported by the status action; not run directly.
# ==============================================================================

"""Presentation of recorded state, never scientific validation or live monitoring."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from .shared_configuration import PROCESS_NAMES
from .shared_active_paths import ActivePaths


class ProgressReader:
    """Read additional progress evidence without creating or repairing state."""

    def __init__(self, database: Path):
        """Retain a database path; construction has no side effects."""
        self.database = database

    def annotations(self, status: dict[str, Any]) -> dict[str, Any]:
        """Read substeps and logical database roles without revalidating science."""
        paths = ActivePaths.from_parent(self.database)
        with duckdb.connect(str(self.database), read_only=True) as connection:
            tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
            substeps = []
            if "process_substeps" in tables:
                result = connection.execute("SELECT * FROM process_substeps ORDER BY substep_id")
                names = [column[0] for column in result.description]
                substeps = [dict(zip(names, row)) for row in result.fetchall()]
            links = connection.execute(
                "SELECT preparation_id,definition_hash,status FROM window_preparation_runs"
            ).fetchall() if "window_preparation_runs" in tables else []
        required = any(row["process_id"] == 3 and row["status"] == "completed"
                       for row in status["processes"]) or any(row[2] == "completed" for row in links)
        if not paths.child.exists():
            child_state = ("recovery error: required child is missing" if required else
                           "not created (Process 03 pending)")
        else:
            try:
                with duckdb.connect(str(paths.child), read_only=True) as child:
                    metadata = child.execute(
                        """SELECT preparation_id,definition_hash,parent_scientific_hash,
                                  parent_configuration_hash FROM preparation_metadata"""
                    ).fetchall()
                compatible = bool(metadata) and all(
                    (row[0], row[1]) in {(link[0], link[1]) for link in links}
                    and row[2:] == (status["scientific_hash"], status["configuration_integrity_hash"])
                    for row in metadata
                )
                child_state = "available (stored linkage matches)" if compatible else "recovery error: incompatible child linkage"
            except duckdb.Error as error:
                child_state = f"recovery error: child unreadable ({type(error).__name__})"
        return {"substeps": substeps, "database_roles": {
            "main": {"path": "results/experiment.duckdb", "state": "available"},
            "windows": {"path": "results/windows.duckdb", "state": child_state},
        }}

    @staticmethod
    def paper_summary(connection: duckdb.DuckDBPyConnection, experiment_id: str) -> dict:
        """Preserve JSON status fields without invoking report validation."""
        tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        count = None
        report = None
        if "paper_metric_candidates" in tables:
            count = connection.execute(
                """SELECT count(*) FROM paper_metric_candidates
                   WHERE experiment_id=? AND lambda_up IS NOT NULL""",
                [experiment_id],
            ).fetchone()[0]
        if "paper_table_reports" in tables:
            row = connection.execute(
                "SELECT report FROM paper_table_reports WHERE experiment_id=?", [experiment_id]
            ).fetchone()
            report = json.loads(row[0]) if row else None
        return {"diagnostic": "m4_paper_tables_v1", "scope": "Daily-only Table2; no All",
                "ex_post": True, "deployable": False, "sensitivity_candidates": count,
                "report": report}

    def read(self, status: dict[str, Any]) -> dict[str, Any]:
        """Read counts and the latest timestamped activity/errors from legacy tables.

        Process/execution records are database-wide; downstream tasks are scoped
        to the selected experiment and import tasks to the latest import run.
        Timestamps describe persisted records, not a heartbeat or commit log.
        """
        with duckdb.connect(str(self.database), read_only=True) as connection:
            tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
            identity = status.get("experiment", {}).get("experiment_id")
            run = connection.execute(
                "SELECT run_id, dataset_id FROM runs ORDER BY started_at DESC, run_id LIMIT 1"
            ).fetchone() if "runs" in tables else None
            counts = list(status.get("experiment", {}).get("tasks", []))
            if run and "tasks" in tables:
                counts += [{"stage": 1, "status": state, "count": count}
                           for state, count in connection.execute(
                               "SELECT status, count(*) FROM tasks WHERE run_id=? GROUP BY status",
                               [run[0]],
                           ).fetchall()]
            imported = connection.execute(
                "SELECT count(*) FROM series WHERE dataset_id=?", [run[1]]
            ).fetchone()[0] if run and "series" in tables else None
            revision = connection.execute(
                """SELECT repository_revision FROM execution_events
                   ORDER BY started_at DESC, execution_id LIMIT 1"""
            ).fetchone() if "execution_events" in tables else None
            candidates = connection.execute(
                """SELECT frequency, model, count(*), count(lambda_up)
                   FROM paper_metric_candidates WHERE experiment_id=?
                   GROUP BY frequency, model ORDER BY frequency, model""", [identity]
            ).fetchall() if "paper_metric_candidates" in tables else None
            activity, error = self._latest_records(connection, tables, identity, run)
        return {"tasks": counts, "imported_series": imported,
                "revision": revision[0] if revision else None,
                "candidates": candidates, "activity": activity, "error": error}

    @staticmethod
    def _latest_records(connection, tables, identity, run) -> tuple:
        """Select timestamped state and error evidence, retaining its known context."""
        # Code constants: existing table/column contracts, not new restart state.
        sources = [
            ("process_substeps", "'Process ' || process_id || ' substep ' || substep_id || coalesce(' item ' || failure_item,'')",
             "last_progress_at", "error", "", []),
            ("experiment_processes", "'Process ' || process_id", "updated_at", "last_error", "", []),
            ("execution_events", "'execution ' || execution_id || ' processes=' || requested_processes",
             "coalesce(completed_at, started_at)", "error", "", []),
            ("experiment_tasks", "'Process ' || stage || ' task ' || task_id",
             "updated_at", "last_error", "experiment_id=?", [identity]),
            ("experiment_invocations", "'invocation ' || invocation_id || ' gate=' || requested_gate",
             "coalesce(ended_at, started_at)", "error", "experiment_id=?", [identity]),
            ("runs", "'Process 1 import run ' || run_id", "coalesce(ended_at, started_at)",
             "error_summary", "run_id=?", [run[0] if run else None]),
            ("run_invocations", "'Process 1 invocation ' || invocation_id",
             "coalesce(ended_at, started_at)", "error", "run_id=?", [run[0] if run else None]),
            ("tasks", "'Process 1 import task ' || task_id", "updated_at", "last_error",
             "run_id=?", [run[0] if run else None]),
            ("experiment_task_attempts", "'attempt ' || attempt_id || ' task=' || task_id || ' invocation=' || invocation_id",
             "coalesce(ended_at, started_at)", "error",
             "task_id IN (SELECT task_id FROM experiment_tasks WHERE experiment_id=?)", [identity]),
            ("task_attempts", "'Process 1 attempt ' || attempt_id || ' task=' || task_id",
             "coalesce(ended_at, started_at)", "error",
             "task_id IN (SELECT task_id FROM tasks WHERE run_id=?)", [run[0] if run else None]),
        ]
        records, errors = [], []
        for table, context, timestamp, error, condition, parameters in sources:
            if table not in tables:
                continue
            for error_only, destination in ((False, records), (True, errors)):
                predicates = [condition] if condition else []
                if error_only:
                    predicates.append(f"{error} IS NOT NULL AND {error} <> ''")
                where = " WHERE " + " AND ".join(predicates) if predicates else ""
                row = connection.execute(
                    f"SELECT {timestamp}, {context}, status, {error} FROM {table}{where} "
                    f"ORDER BY {timestamp} DESC NULLS LAST, {context} LIMIT 1", parameters,
                ).fetchone()
                if row:
                    destination.append(row)
        if "paper_execution_batches" in tables:
            row = connection.execute(
                """SELECT completed_at, 'Process 6 batch ' || batch_id, 'committed', NULL
                   FROM paper_execution_batches WHERE experiment_id=?
                   ORDER BY completed_at DESC, batch_id LIMIT 1""", [identity],
            ).fetchone()
            if row:
                records.append(row)
        # All timestamped records use the schema's TIMESTAMPTZ contract.
        return tuple(max(rows, key=lambda row: (row[0] is not None, row[0], row[1]))
                     if rows else None for rows in (records, errors))


class ProgressInterpretation:
    """Derive a labelled pipeline state without inventing substeps or liveness."""

    @staticmethod
    def interpret(status: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
        """Combine recorded process rows and counts; missing states remain unknown."""
        stored = {row["process_id"]: row for row in status["processes"]}
        processes = [stored.get(number, {"process_id": number, "name": name,
                                         "status": "not recorded"})
                     for number, name in PROCESS_NAMES.items()]
        states = [row["status"] for row in processes]
        state = ("failed" if "failed" in states else
                 "running (recorded)" if "running" in states else
                 "completed" if all(value == "completed" for value in states) else
                 "not recorded" if "not recorded" in states else
                 "pending" if all(value == "pending" for value in states) else "incomplete")
        return {"configuration": status, "evidence": evidence, "processes": processes,
                "state": state,
                "active": [row["process_id"] for row in processes if row["status"] == "running"]}


class HumanProgress:
    """Format an interpreted snapshot as terminal text, with no database access."""

    @staticmethod
    def _value(value: Any) -> str:
        """Make missing evidence explicit, preserving zero counts."""
        return "not recorded" if value is None else str(value)

    def render(self, progress: dict[str, Any]) -> str:
        """Present identity, ordered processes, activity and legacy limitations."""
        config, evidence = progress["configuration"], progress["evidence"]
        lines = [f"Experiment: {config['name']}", f"Description: {config['description']}",
                 f"Pipeline state (derived from recorded processes): {progress['state']}",
                 f"Experiment state (stored): {self._value(config.get('experiment', {}).get('status'))}",
                 f"Configuration: v{config['configuration_version']}; scientific hash={config['scientific_hash']}",
                 f"Configuration integrity hash: {config['configuration_integrity_hash']}",
                 f"Source revision (latest execution): {self._value(evidence['revision'])}",
                 f"Data source revision: {self._value(config['resolved_configuration'].get('data', {}).get('source', {}).get('revision'))}",
                 f"Imported series: {self._value(evidence['imported_series'])}", ""]
        for role, label in (("main", "Main"), ("windows", "Windows")):
            database = config["database_roles"][role]
            lines.append(f"{label} database: {database['path']} — {database['state']}")
        lines.append("")
        for row in progress["processes"]:
            counts = [f"{task['status']}={task['count']}" for task in evidence["tasks"]
                      if task["stage"] == row["process_id"]]
            substeps = [step for step in config["substeps"] if step["process_id"] == row["process_id"]]
            name = "Transform/window preparation" if row["process_id"] == 3 else row["name"].capitalize()
            lines += [f"Process {row['process_id']:02d} {name}: {row['status']}",
                      f"  started: {self._value(row.get('started_at'))}; completed: {self._value(row.get('completed_at'))}",
                      f"  tasks: {', '.join(sorted(counts)) if counts else 'not recorded'}" +
                      ("" if substeps else "; substeps: not recorded")]
            for step in substeps:
                lines += [f"  {step['substep_id']} {step['name']}: {step['status']}",
                          f"    items planned={step['planned_items']}, completed={step['completed_items']}, failed={step['failed_items']}; attempts={step['attempt_count']}",
                          f"    started: {self._value(step['started_at'])}; last progress: {self._value(step['last_progress_at'])}; completed: {self._value(step['completed_at'])}"]
                if step["error"]:
                    lines.append(f"    failure: {step['error']}")
        active = ", ".join(f"{number:02d}" for number in progress["active"])
        lines += ["", f"Active process (recorded): {active or 'none recorded'}"]
        for key, label in (("activity", "Latest committed activity (record timestamp)"),
                           ("error", "Latest recorded error (record timestamp)")):
            row = evidence[key]
            text = "not recorded" if row is None else f"{row[0]} | {row[1]} | {row[2]}"
            if key == "error" and row is not None:
                text += f" | {row[3]}"
            lines.append(f"{label}: {text}")
        candidates = evidence["candidates"]
        lines.append("Process 06 candidates: " + ("not recorded (table absent)" if candidates is None
                                                 else str(sum(row[2] for row in candidates))))
        for frequency, model, count, sensitivity in candidates or []:
            lines.append(f"  {frequency} / {model}: {count} stored; {sensitivity} sensitivity candidates")
        lines += ["ETA: not recorded",
                  "Limitations: later processes have no durable substep progress; there is no heartbeat. Recorded running",
                  "does not prove live work. Task counts cover recorded rows, not an expected-work denominator.",
                  "Activity/error timestamps are record timestamps, not a complete commit/error history;",
                  "process/execution state is database-wide and import counts use the latest import run.",
                  "Status does not validate scientific outputs, repair interrupted state, or estimate an ETA."]
        return "\n".join(lines)
