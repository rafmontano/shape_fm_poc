"""Process 01's single persistence owner: atomic items and final acceptance.

Uses the existing parent schema. Experiment execution events already own host,
code and Prefect provenance; this store does not duplicate those payloads.
"""

import hashlib
import json
import math
from contextlib import contextmanager

import duckdb

from .shared_configuration import canonical_import_configuration, canonical_json, dataset_identity, json_fingerprint


class ImportStore:
    """Own one coordinator connection; no source reads or scientific conversion."""

    def __init__(self, database, configuration):
        self.connection = duckdb.connect(str(database))
        self.configuration = configuration
        self.dataset_id = self.run_id = self.task_id = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.connection.close()

    @contextmanager
    def transaction(self):
        """Commit science and its completion marker together, or neither."""
        self.connection.execute("BEGIN TRANSACTION")
        try:
            yield
            self.connection.execute("COMMIT")
        except BaseException as error:
            self.connection.execute("ROLLBACK")
            if isinstance(error, duckdb.Error):
                raise RuntimeError(f"Import storage failed: {error}") from error
            raise

    def completed_summary(self):
        """Return durable acceptance without rescanning outputs or opening source."""
        state, summary = self.connection.execute(
            "SELECT status,summary FROM experiment_processes WHERE process_id=1"
        ).fetchone()
        if state != "completed":
            return None
        summary = json.loads(summary)
        if summary["series_count"] != self.configuration.series_count:
            raise RuntimeError("Process 01 stored acceptance count differs from configuration")
        return summary

    def begin(self, snapshot, metadata):
        """Bind pinned source identity and recover interrupted item attempts."""
        settings = self.configuration.import_settings
        revision = self.configuration.resolved["data"]["source"]["revision"]
        self.dataset_id, config_hash = dataset_identity(settings, revision, snapshot["files"])
        self.run_id = f"import/{json_fingerprint({'dataset_id': self.dataset_id, 'stage': 'import'})[:24]}"
        canonical = canonical_json(canonical_import_configuration(settings))
        with self.transaction():
            self.insert_or_verify("datasets", {
                "dataset_id": self.dataset_id, "dataset_name": settings["dataset_name"],
                "source_system": settings["source_system"], "source_revision": revision,
                "source_file_hashes": canonical_json(snapshot["files"]),
                "import_configuration": canonical, "import_configuration_hash": config_hash,
                "frequency": settings["benchmark"]["frequency"], "source_metadata": canonical_json(metadata),
            }, {"dataset_id": self.dataset_id})
            self.connection.execute(
                """INSERT INTO runs (run_id,stage,dataset_id,configuration,configuration_hash,
                   environment,machine,started_at,status)
                   VALUES (?,'import',?,?,?,'{}','{}',current_timestamp,'running')
                   ON CONFLICT (run_id) DO UPDATE SET status='running',ended_at=NULL,error_summary=NULL""",
                [self.run_id, self.dataset_id, canonical, config_hash],
            )
            self.connection.execute(
                """UPDATE task_attempts SET status='failed',ended_at=current_timestamp,
                   error='interrupted before completion' WHERE status='running'"""
            )
            self.connection.execute(
                """UPDATE tasks SET status='pending',last_error='interrupted before completion',
                   updated_at=current_timestamp WHERE status='running'"""
            )
            self.connection.execute(
                """UPDATE experiment_processes SET status='running',
                   started_at=coalesce(started_at,current_timestamp),completed_at=NULL,
                   updated_at=current_timestamp,last_error=NULL WHERE process_id=1"""
            )

    def item_states(self):
        """Return recorded identities grouped by completion; unseen items come from source."""
        rows = self.connection.execute(
            "SELECT series_id,status FROM tasks WHERE run_id=?", [self.run_id]
        ).fetchall()
        return ({identity for identity, state in rows if state == "completed"},
                {identity for identity, state in rows if state != "completed"})

    def start_item(self, identity):
        """Record the current identity before conversion so interruption is recoverable."""
        self.task_id = f"task/{json_fingerprint({'run_id': self.run_id, 'series_id': identity})[:32]}"
        with self.transaction():
            self.connection.execute(
                """INSERT INTO tasks (task_id,run_id,stage,dataset_id,series_id,status)
                   VALUES (?,?,'import',?,?,'pending') ON CONFLICT DO NOTHING""",
                [self.task_id, self.run_id, self.dataset_id, identity],
            )
            self.connection.execute(
                """UPDATE tasks SET status='running',attempt_count=attempt_count+1,
                   started_at=current_timestamp,updated_at=current_timestamp,last_error=NULL
                   WHERE task_id=?""", [self.task_id],
            )
            attempt = self.connection.execute(
                "SELECT attempt_count FROM tasks WHERE task_id=?", [self.task_id]
            ).fetchone()[0]
            self.connection.execute(
                """INSERT INTO task_attempts (attempt_id,task_id,attempt_number,started_at,status)
                   VALUES (?,?,?,current_timestamp,'running')""",
                [f"{self.task_id}/attempt/{attempt}", self.task_id, attempt],
            )

    def insert_or_verify(self, table, values, key):
        """Insert absent output; refuse conflicting persisted content without overwriting."""
        where = " AND ".join(f"{column}=?" for column in key)
        columns = ",".join(values)
        existing = self.connection.execute(
            f"SELECT {columns} FROM {table} WHERE {where}", list(key.values())
        ).fetchone()
        if existing is None:
            placeholders = ",".join("?" for _ in values)
            self.connection.execute(f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", list(values.values()))
        elif existing != tuple(values.values()):
            raise RuntimeError(f"Process 01 conflicting stored {table} identity {key}")

    def accept(self, result):
        """Atomically commit series, evaluation window, item and attempt completion."""
        record = result.source
        key = {"dataset_id": self.dataset_id, "series_id": record.source_series_id}
        with self.transaction():
            self.insert_or_verify("series", {
                **key, "source_series_id": record.source_series_id, "source_row": record.source_row,
                "frequency": record.frequency, "start_timestamp": record.start_timestamp,
                "target": [None if x is not None and math.isnan(x) else x for x in record.target],
                "observation_count": len(record.target), "content_hash": result.content_hash,
                "source_metadata": canonical_json({"source_row": record.source_row}),
            }, key)
            self.insert_or_verify("evaluation_windows", {**key, **result.window},
                                  {**key, "window_id": result.window["window_id"]})
            self.connection.execute(
                """UPDATE tasks SET status='completed',completed_at=current_timestamp,
                   updated_at=current_timestamp,last_error=NULL WHERE task_id=?""", [self.task_id],
            )
            self.connection.execute(
                """UPDATE task_attempts SET status='completed',ended_at=current_timestamp
                   WHERE task_id=? AND status='running'""", [self.task_id],
            )
        self.task_id = None

    def fail(self, error):
        """Retain committed items and persist concise process/item failure context."""
        message = f"Process 01: {type(error).__name__}: {error}"
        with self.transaction():
            if self.task_id:
                self.connection.execute(
                    "UPDATE tasks SET status='failed',updated_at=current_timestamp,last_error=? WHERE task_id=?",
                    [message, self.task_id],
                )
                self.connection.execute(
                    """UPDATE task_attempts SET status='failed',ended_at=current_timestamp,error=?
                       WHERE task_id=? AND status='running'""", [message, self.task_id],
                )
            self.connection.execute(
                "UPDATE runs SET status='failed',ended_at=current_timestamp,error_summary=? WHERE run_id=?",
                [message, self.run_id],
            )
            self.connection.execute(
                """UPDATE experiment_processes SET status='failed',updated_at=current_timestamp,
                   last_error=? WHERE process_id=1""", [message],
            )

    def finish(self, skipped):
        """Confirm accepted count and finalize once; no scientific recalculation."""
        with self.transaction():
            count, observations = self.connection.execute(
                """SELECT count(*),coalesce(sum(s.observation_count),0) FROM tasks t
                   JOIN series s USING (dataset_id,series_id)
                   JOIN evaluation_windows w USING (dataset_id,series_id)
                   WHERE t.run_id=? AND t.status='completed'""", [self.run_id],
            ).fetchone()
            if count != self.configuration.series_count:
                raise RuntimeError(f"Process 01 expected {self.configuration.series_count} items, stored {count}")
            summary = {"run_id": self.run_id, "dataset_id": self.dataset_id,
                       "selected_series": count, "series_count": count, "observation_count": observations,
                       "task_counts": {"completed": count}, "completed_this_invocation": count - skipped,
                       "skipped_completed": skipped, "output_fingerprint": self.fingerprint(self.connection)}
            self.connection.execute(
                "UPDATE runs SET status='completed',ended_at=current_timestamp,summary=?,error_summary=NULL WHERE run_id=?",
                [canonical_json(summary), self.run_id],
            )
            self.connection.execute(
                """UPDATE experiment_processes SET status='completed',completed_at=current_timestamp,
                   updated_at=current_timestamp,summary=?,last_error=NULL WHERE process_id=1""",
                [canonical_json(summary)],
            )
        return summary

    @staticmethod
    def fingerprint(connection):
        """One final stored-output digest, byte-compatible with Increment 2's digest."""
        queries = [
            """SELECT dataset_id,series_id,source_series_id,source_row,frequency,start_timestamp,
                      target,observation_count,content_hash FROM series ORDER BY dataset_id,series_id""",
            """SELECT dataset_id,series_id,window_id,split_name,train_start,train_end,
                      validation_start,validation_end,test_start,test_end,horizon,boundary_convention
               FROM evaluation_windows ORDER BY dataset_id,series_id,window_id""",
        ]
        digest = hashlib.sha256()
        for query in queries:
            cursor = connection.execute(query)
            while rows := cursor.fetchmany(100):
                for row in rows:
                    digest.update(json.dumps(row, default=str, separators=(",", ":")).encode())
                    digest.update(b"\n")
            digest.update(b"\0")
        return digest.hexdigest()
