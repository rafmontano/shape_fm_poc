# ==============================================================================
# p01_04_import_state.py
# Purpose: Own concise Process 01 substep checkpoints and persisted-output integrity.
# Inputs: Coordinator-owned connection and authoritative stored configuration.
# Outputs: Substep transitions/fingerprints; no scheduling or scientific computation.
# Run from: Imported; not run directly.
# ==============================================================================

"""The import's two durable boundaries, above its existing atomic item commits."""

import hashlib
import json


class ImportState:
    """Own substep state on the caller's single-writer connection."""

    def __init__(self, connection, configuration):
        """Bind storage without mutation; counts derive from the stored configuration."""
        self.connection = connection
        self.configuration = configuration
        count = configuration.series_count
        self.definitions = {
            "01.01": ("Import canonical GIFT-Eval dataset", count),
            "01.02": ("Persist and accept canonical import", count),
        }

    def initialize(self):
        """Insert missing pending boundaries, preserving previously accepted work."""
        for identity, (name, count) in self.definitions.items():
            self.connection.execute(
                """INSERT INTO process_substeps
                   (process_id,substep_id,name,status,planned_items,input_fingerprint)
                   VALUES (1,?,?,'pending',?,?) ON CONFLICT DO NOTHING""",
                [identity, name, count, self.configuration.scientific_hash],
            )

    def accepted(self, identity):
        """Verify a completed persisted boundary before skipping it on restart."""
        row = self.connection.execute(
            """SELECT status,planned_items,completed_items,failed_items,input_fingerprint,
                      output_fingerprint FROM process_substeps WHERE substep_id=?""", [identity]
        ).fetchone()
        if row is None or row[0] != "completed":
            return False
        expected = self.definitions[identity][1]
        if (row[1:4] != (expected, expected, 0)
                or row[4] != self.configuration.scientific_hash
                or row[5] != self.fingerprint(identity)):
            raise RuntimeError(f"Process 01 substep {identity}: persisted output/identity mismatch")
        if identity == "01.01":
            counts = self.connection.execute(
                "SELECT count(*),count(*) FILTER (WHERE status='completed') FROM tasks"
            ).fetchone()
            if counts != (expected, expected):
                raise RuntimeError("Process 01 substep 01.01: persisted item counts mismatch")
        return True

    def start(self, identity):
        """Start/retry one unaccepted boundary without discarding item commits."""
        self.connection.execute(
            """UPDATE process_substeps SET status='running',attempt_count=attempt_count+1,
               started_at=coalesce(started_at,current_timestamp),last_progress_at=current_timestamp,
               completed_at=NULL,error=NULL,failure_item=NULL WHERE substep_id=?""", [identity]
        )

    def progress(self):
        """Refresh canonical counts from the already committed authoritative item rows."""
        completed, failed = self.connection.execute(
            """SELECT count(*) FILTER (WHERE status='completed'),
                      count(*) FILTER (WHERE status='failed') FROM tasks"""
        ).fetchone()
        self.connection.execute(
            """UPDATE process_substeps SET completed_items=?,failed_items=?,
               last_progress_at=current_timestamp WHERE substep_id='01.01'""", [completed, failed]
        )

    def complete(self, identity):
        """Accept a boundary with its stored output fingerprint and exact item count."""
        count = self.connection.execute("SELECT count(*) FROM series").fetchone()[0]
        if count != self.definitions[identity][1]:
            raise RuntimeError(f"Process 01 substep {identity}: accepted item count differs from plan")
        self.connection.execute(
            """UPDATE process_substeps SET status='completed',completed_items=?,failed_items=0,
               completed_at=current_timestamp,last_progress_at=current_timestamp,
               output_fingerprint=?,error=NULL,failure_item=NULL WHERE substep_id=?""",
            [count, self.fingerprint(identity), identity],
        )

    def fail(self, identity, error, item=None):
        """Record concise known failure location; never fabricate a failed item identity."""
        self.progress()
        message = f"Process 01 substep {identity}" + (f" item {item}" if item else "")
        message += f": {type(error).__name__}: {error}"
        self.connection.execute(
            """UPDATE process_substeps SET status='failed',error=?,failure_item=?,
               last_progress_at=current_timestamp WHERE substep_id=?""", [message, item, identity]
        )

    def fingerprint(self, identity):
        """Hash persisted scientific fields in deterministic order without recalculation.

        This is a storage/restart trust-boundary check, not external source rereading.
        Operational timestamps and attempt identities never enter the fingerprint.
        """
        queries = [
            """SELECT dataset_id,series_id,source_series_id,source_row,frequency,start_timestamp,
                      target,observation_count,content_hash FROM series ORDER BY dataset_id,series_id""",
            """SELECT dataset_id,series_id,window_id,split_name,train_start,train_end,
                      validation_start,validation_end,test_start,test_end,horizon,boundary_convention
               FROM evaluation_windows ORDER BY dataset_id,series_id,window_id""",
        ]
        digest = hashlib.sha256()
        for query in queries:
            cursor = self.connection.execute(query)
            while rows := cursor.fetchmany(100):
                for row in rows:
                    digest.update(json.dumps(row, default=str, separators=(",", ":")).encode())
                    digest.update(b"\n")
            digest.update(b"\0")
        return digest.hexdigest()
