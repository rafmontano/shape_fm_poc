# ==============================================================================
# shared_execution_event_storage.py
#
# Purpose: Persist coordinator-owned execution lifecycle and Prefect identities.
# Inputs: One research DuckDB path and JSON-ready execution evidence.
# Outputs: Durable execution events and recovered interrupted parent records.
# Run from: Imported by the researcher process action; never run directly.
# ==============================================================================

"""Coordinator-local storage for research execution lifecycle events."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb

from .shared_configuration import PROCESS_NAMES, canonical_json


class ExecutionEventStorage:
    """Own SQL for execution events and interrupted invocation recovery."""

    def __init__(self, database: Path):
        """Bind lifecycle operations to one resolved research database."""
        self.database = database.resolve()

    def start(
        self,
        identity: str,
        processes: tuple[int, ...],
        operational_configuration: dict[str, Any],
        repository_revision: str,
        machine: dict[str, str],
    ) -> None:
        """Recover stale parent rows and insert one running execution event."""
        connection = duckdb.connect(str(self.database))
        try:
            connection.execute("BEGIN TRANSACTION")
            self._recover_interrupted(connection, processes)
            connection.execute(
                """INSERT INTO execution_events
                (execution_id, requested_processes, operational_configuration,
                 repository_revision, machine, status)
                VALUES (?, ?, ?, ?, ?, 'running')""",
                [
                    identity,
                    canonical_json(list(processes)),
                    canonical_json(operational_configuration),
                    repository_revision,
                    canonical_json(machine),
                ],
            )
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    @staticmethod
    def _recover_interrupted(
        connection: duckdb.DuckDBPyConnection,
        processes: tuple[int, ...],
    ) -> None:
        """Close stale workflow and gate invocation rows for requested gates."""
        interruption = "interrupted before completion; recovered by a later run"
        connection.execute(
            """UPDATE execution_events SET status='failed',
               completed_at=current_timestamp, error=? WHERE status='running'""",
            [interruption],
        )
        gates = [PROCESS_NAMES[process_id] for process_id in processes]
        placeholders = ", ".join("?" for _ in gates)
        connection.execute(
            f"""UPDATE experiment_invocations SET status='failed',
                ended_at=current_timestamp, error=?
                WHERE status='running' AND requested_gate IN ({placeholders})""",
            [interruption, *gates],
        )

    def finish(
        self,
        identity: str,
        status: str,
        summary: dict[str, Any],
        error: str | None = None,
    ) -> None:
        """Complete one execution event with its concise summary or failure."""
        connection = duckdb.connect(str(self.database))
        try:
            connection.execute(
                """UPDATE execution_events SET status=?,
                   completed_at=current_timestamp, summary=?, error=?
                   WHERE execution_id=?""",
                [status, canonical_json(summary), error, identity],
            )
        finally:
            connection.close()

    def record_prefect_identity(self, identity: str, flow_id: str) -> None:
        """Attach a non-persisting Prefect flow identity to an execution event."""
        connection = duckdb.connect(str(self.database))
        try:
            row = connection.execute(
                "SELECT operational_configuration FROM execution_events WHERE execution_id=?",
                [identity],
            ).fetchone()
            if row is None:
                raise RuntimeError(f"execution event was not found: {identity}")
            operational = json.loads(row[0])
            operational["prefect"] = {
                "flow_run_id": flow_id,
                "result_persistence": False,
                "cache": False,
            }
            connection.execute(
                """UPDATE execution_events SET operational_configuration=?
                   WHERE execution_id=?""",
                [canonical_json(operational), identity],
            )
        finally:
            connection.close()
