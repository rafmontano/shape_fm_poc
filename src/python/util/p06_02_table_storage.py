# ==============================================================================
# p06_02_table_storage.py
# Purpose: Own coordinator-only paper diagnostic inputs, persistence and retrieval.
# Inputs: Coordinator DuckDB connection and validated scientific/operational records.
# Outputs: Aggregate candidates, selected means, common directional rows and reports.
# Run from: Imported; not run directly.
# ==============================================================================
"""Process 06 storage; never constructed in a compute worker."""

from __future__ import annotations

import json
import math
import uuid

from .shared_configuration import canonical_json, json_fingerprint
from .p06_03_table_reports import TableReports, SELECTION_REASON


class TableStorage:
    """Own a borrowed coordinator connection; construction and retrieval never write."""

    def __init__(self, connection):
        """Borrow the sole-writer connection or an existing read-only results connection."""
        self.connection = connection

    def load_inputs(self, experiment_id: str, configuration) -> dict:
        """Read complete canonical membership, original means and stored binary predictions."""
        c = self.connection
        instances = c.execute(
            """SELECT i.forecast_instance_id,i.series_id,i.dataset_id,i.context_target,
                      i.actual_target,i.horizon
               FROM forecast_instances i JOIN experiments e USING(benchmark_configuration_id)
               WHERE e.experiment_id=? ORDER BY i.official_position""", [experiment_id]
        ).fetchall()
        if len(instances) != configuration.series_count:
            raise RuntimeError("paper input membership is incomplete")
        models = {}
        for table, name in (("directional_model_definitions", "directional_dtw"),
                            ("directional_composite_model_definitions", "directional_mantis_rf")):
            values = c.execute(f"SELECT model_definition_id FROM {table} WHERE experiment_id=?",
                               [experiment_id]).fetchall()
            if len(values) != 1:
                raise RuntimeError(f"paper input requires one {name} model")
            models[name] = values[0][0]
        series = []
        for instance, sid, dataset, context, actual, horizon in instances:
            if len(actual) != horizon or not context or any(
                type(v) not in (float, int) or not math.isfinite(v) for v in [*context, *actual]
            ):
                raise RuntimeError("paper inputs require finite canonical raw history/future")
            means, sources, directions = {}, {}, {}
            for name in ("naive2", "chronos_2", "m4_smyl", "m4_fforma"):
                if name.startswith("m4_"):
                    values = c.execute(
                        """SELECT reference_forecast_id,mean,content_hash FROM reference_forecasts
                           WHERE dataset_id=? AND series_id=? AND forecast_id=?""",
                        [dataset, sid, name]).fetchall()
                else:
                    values = c.execute(
                        """SELECT forecast_id,mean,content_hash FROM forecasts
                           WHERE experiment_id=? AND forecast_instance_id=? AND candidate=?
                             AND scale='original'""", [experiment_id, instance, name]).fetchall()
                if len(values) != 1 or len(values[0][1]) != horizon or any(
                    not math.isfinite(v) for v in values[0][1]
                ):
                    raise RuntimeError(f"missing/mismatched paper mean: {instance}/{name}")
                source_id, mean, source_hash = values[0]
                means[name] = mean
                sources[name] = {"id": source_id, "content_hash": source_hash,
                                 "mean_fingerprint": json_fingerprint(mean)}
            for name, model_id in models.items():
                values = c.execute(
                    """SELECT p.horizon,p.prediction,p.content_hash
                       FROM model_directional_predictions p
                       JOIN directional_evaluation_inputs i USING(evaluation_input_id)
                       WHERE p.experiment_id=? AND p.model_definition_id=?
                         AND i.forecast_instance_id=? ORDER BY p.horizon""",
                    [experiment_id, model_id, instance]).fetchall()
                if [v[0] for v in values] != list(range(1, horizon + 1)) or any(
                    v[1] not in (0, 1) for v in values
                ):
                    raise RuntimeError(f"missing/mismatched paper directions: {instance}/{name}")
                directions[name] = [int(v[1]) for v in values]
                sources[name] = {"id": model_id, "prediction_fingerprint": json_fingerprint(values)}
            series.append({"series_id": str(sid), "forecast_instance_id": instance,
                           "context": context, "actual": actual, "means": means,
                           "directions": directions, "sources": sources})
        frequency = configuration.resolved["data"]["benchmark"]["frequency"]
        labels = {"H": "Hourly", "D": "Daily", "W": "Weekly", "M": "Monthly",
                  "Q": "Quarterly", "Y": "Yearly"}
        return {"frequency": labels[frequency], "series": series}

    def candidate(self, candidate_id: str) -> dict | None:
        """Read accepted content and reject tampered fingerprints before restart reuse."""
        row = self.connection.execute(
            "SELECT scientific_result,result_fingerprint FROM paper_metric_candidates WHERE candidate_id=?",
            [candidate_id]).fetchone()
        if row is None:
            return None
        record = json.loads(row[0])
        if json_fingerprint(record) != row[1]:
            raise RuntimeError("stored paper candidate fingerprint mismatch")
        return {**record, "result_fingerprint": row[1]}

    def save_candidate(self, record: dict) -> None:
        """Insert a validated aggregate, or verify exact content on repeat-safe reuse."""
        fingerprint = record["result_fingerprint"]
        content = {k: v for k, v in record.items() if k != "result_fingerprint"}
        if json_fingerprint(content) != fingerprint:
            raise RuntimeError("invalid paper candidate fingerprint")
        stored = self.candidate(record["candidate_id"])
        if stored is not None:
            if stored != record:
                raise RuntimeError("conflicting paper candidate")
            return
        self.connection.execute("INSERT INTO paper_metric_candidates VALUES (?,?,?,?,?,?,?,?,?)", [
            record["candidate_id"], record["experiment_id"], record["frequency"], record["model"],
            record["lambda_up"], record["lambda_down"], record["input_fingerprint"], fingerprint,
            canonical_json(content),
        ])

    def save_batch(self, records: list[dict], provenance: dict) -> None:
        """Commit completion-order candidate responses and separate operational evidence."""
        self.connection.execute("BEGIN TRANSACTION")
        try:
            for record in records:
                self.save_candidate(record)
            self.connection.execute(
                "INSERT INTO paper_execution_batches VALUES (?,?,?,?,current_timestamp)",
                [uuid.uuid4().hex, records[0]["experiment_id"],
                 canonical_json([r["candidate_id"] for r in records]), canonical_json(provenance)])
            self.connection.execute("COMMIT")
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise

    def save_selected(self, record: dict, model: str, retained: list[dict], series: list[dict]) -> None:
        """Store one selected identity and its point-only diagnostic means in the caller transaction."""
        self.connection.execute(
            "INSERT OR REPLACE INTO paper_selected_results VALUES (?,?,?,?,?)",
            [record["experiment_id"], record["frequency"], model, record["candidate_id"],
             SELECTION_REASON if record["lambda_up"] is not None else "baseline_or_oracle"])
        if not retained:
            return
        by_id = {r["series_id"]: r for r in retained}
        if len(by_id) != len(series) or set(by_id) != {s["series_id"] for s in series}:
            raise RuntimeError("selected diagnostic membership mismatch")
        source_model = "m4_smyl" if model == "m4_smyl_oracle" else record["model"]
        for s in series:
            item = by_id[s["series_id"]]
            mean = item["mean"]
            if len(mean) != len(s["actual"]) or any(not math.isfinite(v) for v in mean):
                raise RuntimeError("selected diagnostic vector is invalid")
            # DuckDB stores DOUBLE[]; fingerprint that representation, including
            # R JSON numbers emitted as integers for exactly integral means.
            mean = [float(v) for v in mean]
            metadata = {"profile": "m4_paper_tables_v1", "ex_post": True,
                        "candidate_id": record["candidate_id"],
                        "lambda_up": record["lambda_up"], "lambda_down": record["lambda_down"],
                        "oracle": item.get("oracle")}
            content = {"mean": mean, "metadata": metadata, "source": s["sources"][source_model]["id"],
                       "input_fingerprint": record["input_fingerprint"]}
            self.connection.execute("INSERT OR REPLACE INTO paper_diagnostic_means VALUES (?,?,?,?,?,?,?,?,?)", [
                record["experiment_id"], record["frequency"], model, s["forecast_instance_id"],
                content["source"], mean, canonical_json(metadata), content["input_fingerprint"],
                json_fingerprint(content)])

    def save_directional(self, record: dict, model: str) -> list[dict]:
        """Store common per-horizon counts; tables consume these same terminal rows."""
        rows = []
        for horizon, correct in enumerate(record["correct_counts"], 1):
            content = {"experiment_id": record["experiment_id"], "frequency": record["frequency"],
                       "model": model, "horizon": horizon, "correct_count": correct,
                       "evaluation_count": record["evaluation_count"],
                       "accuracy": correct / record["evaluation_count"],
                       "input_fingerprint": record["input_fingerprint"]}
            fingerprint = json_fingerprint(content)
            self.connection.execute("INSERT OR REPLACE INTO paper_directional_results VALUES (?,?,?,?,?,?,?,?,?)",
                                    [*content.values(), fingerprint])
            rows.append({**content, "result_fingerprint": fingerprint})
        return rows

    def save_report(self, experiment_id: str, input_fingerprint: str, report: dict) -> None:
        """Persist one reconciled report in the enclosing scientific completion transaction."""
        self.connection.execute("INSERT OR REPLACE INTO paper_table_reports VALUES (?,?,?,?)", [
            experiment_id, input_fingerprint, json_fingerprint(report), canonical_json(report)])

    def summary(self, experiment_id: str) -> dict:
        """Read diagnostic identity, surface progress, selected pairs and report cells."""
        count = self.connection.execute(
            "SELECT count(*) FROM paper_metric_candidates WHERE experiment_id=? AND lambda_up IS NOT NULL",
            [experiment_id]).fetchone()[0]
        row = self.connection.execute(
            "SELECT report,result_fingerprint FROM paper_table_reports WHERE experiment_id=?",
            [experiment_id]).fetchone()
        report = None if row is None else json.loads(row[0])
        if report is not None and json_fingerprint(report) != row[1]:
            raise RuntimeError("stored paper report fingerprint mismatch")
        return {"diagnostic": "m4_paper_tables_v1", "scope": "Daily-only Table2; no All",
                "ex_post": True, "deployable": False, "sensitivity_candidates": count,
                "report": report}

    def diagnostic_mean(self, experiment_id: str, model: str, instance_id: str,
                        frequency: str = "Daily") -> dict:
        """Retrieve a selected diagnostic without treating it as a deployable forecast."""
        row = self.connection.execute(
            """SELECT mean,metadata,source_forecast_id,input_fingerprint,result_fingerprint
               FROM paper_diagnostic_means WHERE experiment_id=? AND frequency=?
                 AND model=? AND forecast_instance_id=?""",
            [experiment_id, frequency, model, instance_id]).fetchone()
        if row is None:
            raise KeyError("selected diagnostic not found")
        content = {"mean": row[0], "metadata": json.loads(row[1]), "source": row[2],
                   "input_fingerprint": row[3]}
        if json_fingerprint(content) != row[4]:
            raise RuntimeError("stored diagnostic fingerprint mismatch")
        return {**content, "ex_post": True, "deployable": False}

    def directional_rows(self, experiment_id: str) -> list[dict]:
        """Expose Figure 2/5-shaped profiles without recalculation or model execution."""
        columns = ("experiment_id", "frequency", "model", "horizon", "correct_count",
                   "evaluation_count", "accuracy", "input_fingerprint", "result_fingerprint")
        rows = [dict(zip(columns, row)) for row in self.connection.execute(
            "SELECT * FROM paper_directional_results WHERE experiment_id=? ORDER BY frequency,model,horizon",
            [experiment_id]).fetchall()]
        for row in rows:
            if json_fingerprint({k: v for k, v in row.items() if k != "result_fingerprint"}) != row["result_fingerprint"]:
                raise RuntimeError("stored directional fingerprint mismatch")
        return rows

    def export_snapshot(self, database_path) -> dict:
        """Validate and read a complete presentation snapshot without executing science or writing.

        Inputs: Borrowed read-only connection to one configured experiment.
        Outputs: Stored configuration, report, rank evidence and directional rows.
        """
        from .shared_database import load_database_configuration
        from .shared_process_storage import ProcessStorage
        configuration = load_database_configuration(database_path, self.connection)
        if configuration.version != 12:
            raise RuntimeError("export requires the integrated paper-table configuration")
        experiments = self.connection.execute(
            "SELECT experiment_id,name,description FROM experiments").fetchall()
        if len(experiments) != 1:
            raise RuntimeError("export requires exactly one experiment")
        identity, name, description = experiments[0]
        process = self.connection.execute(
            "SELECT status FROM experiment_processes WHERE process_id=6").fetchone()
        if process is None or process[0] != "completed":
            raise RuntimeError("export requires completed Process 06")
        ProcessStorage(database_path).validate(6)
        report = self.summary(identity)["report"]
        profiles = [report["directional_profile"], report["figure_2"]["horizon"],
                    report["figure_2"]["cd"]]
        if (any(p["incomplete_models"] for p in profiles)
                or report["figure_2"]["cd"]["mean_ranks"] is None):
            raise RuntimeError("export rejects incomplete configured report models")
        return {"experiment_id": identity, "name": name, "description": description,
                "configuration_version": configuration.version,
                "scientific_fingerprint": configuration.scientific_hash,
                "figure_settings": configuration.table_reproduction["figure_2"],
                "report": report, "report_fingerprint": json_fingerprint(report),
                "execution_summary": self.execution_evidence(configuration),
                "directional_rows": self.directional_rows(identity)}

    def execution_evidence(self, configuration) -> dict:
        """Read stored counts, contributions and restart state without inferring scientific success."""
        c = self.connection
        processes = [{"process": p, "status": s, "summary": json.loads(v) if v else None}
                     for p, s, v in c.execute(
                         "SELECT process_id,status,summary FROM experiment_processes ORDER BY process_id").fetchall()]
        events = c.execute(
            "SELECT repository_revision,status,summary,error FROM execution_events ORDER BY started_at,execution_id").fetchall()
        counts = {table: c.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                  for table in ("series", "forecast_instances", "forecasts", "reference_forecasts",
                                "model_directional_predictions", "model_directional_evaluations",
                                "directional_model_definitions", "directional_composite_model_definitions",
                                "directional_classifier_runs", "official_evaluations", "paper_metric_candidates",
                                "paper_selected_results", "paper_directional_results", "paper_execution_batches")}
        tasks = [{"process": p, "status": s, "count": n} for p, s, n in c.execute(
            "SELECT stage,status,count(*) FROM experiment_tasks GROUP BY stage,status ORDER BY stage,status").fetchall()]
        workers = [{"hostname": h, "status": s, "attempts": n} for h, s, n in c.execute(
            """SELECT json_extract_string(resource_usage,'$.hostname'),status,count(*)
               FROM experiment_task_attempts GROUP BY 1,2 ORDER BY 1,2""").fetchall()]
        batches = [{"hostname": h, "batches": n, "candidates": size} for h, n, size in c.execute(
            """SELECT json_extract_string(provenance,'$.worker.hostname'),count(*),
                      sum(json_array_length(candidate_ids)) FROM paper_execution_batches
               GROUP BY 1 ORDER BY 1""").fetchall()]
        restarts = []
        for _, state, value, _ in events[1:]:
            summary = json.loads(value) if value else {}
            steps = summary.get("processes", [])
            restarts.append({"status": state, "all_completed_work_skipped": bool(steps)
                             and all(step.get("status") == "skipped_completed" for step in steps)})
        report = c.execute("SELECT input_fingerprint,result_fingerprint FROM paper_table_reports").fetchone()
        return {"experiment": configuration.name, "configuration_version": configuration.version,
                "revision": events[0][0] if events else None,
                "scientific_fingerprint": configuration.scientific_hash,
                "configuration_integrity_fingerprint": configuration.configuration_integrity_hash,
                "processes": processes, "task_counts": tasks, "counts": counts,
                "sensitivity_counts": {"expected": 1050, "stored": c.execute(
                    "SELECT count(*) FROM paper_metric_candidates WHERE lambda_up IS NOT NULL").fetchone()[0]},
                "worker_contribution": workers, "sensitivity_contribution": batches,
                "restart": restarts or [{"status": "not_yet_demonstrated"}],
                "report_fingerprints": None if report is None else {"input": report[0], "result": report[1]},
                "state": events[-1][1] if events else "synthetic_no_execution_event",
                "failure": events[-1][3] if events else None}

    def validate(self, experiment_id: str) -> dict:
        """Independently check the complete Process 06 surface, selection, vectors and reports."""
        # Read authoritative configuration via this already-open connection.
        from .shared_configuration import ExperimentConfiguration
        row = self.connection.execute(
            "SELECT original_configuration,resolved_configuration FROM experiment_configuration").fetchone()
        configuration = ExperimentConfiguration(json.loads(row[0]), json.loads(row[1]))
        inputs = self.load_inputs(experiment_id, configuration)
        from .p06_01_table_flow import TableEvaluation
        evaluation = TableEvaluation(experiment_id, inputs, configuration.resolved["evaluation"]["table_reproduction"])
        records = []
        for job in evaluation.jobs():
            record = self.candidate(evaluation.identity(job)["candidate_id"])
            if record is None:
                raise RuntimeError("incomplete paper candidate surface")
            evaluation.validate_result(record, job)
            records.append(record)
        selected = evaluation.select(records)
        stored = self.connection.execute(
            "SELECT model,candidate_id,selection_reason FROM paper_selected_results WHERE experiment_id=?",
            [experiment_id]).fetchall()
        expected = {(model, record["candidate_id"],
                     SELECTION_REASON if record["lambda_up"] is not None else "baseline_or_oracle")
                    for model, record in selected.items()}
        if set(stored) != expected or len(stored) != len(expected):
            raise RuntimeError("paper deterministic selection mismatch")
        directions = self.directional_rows(experiment_id)
        if len(directions) != len(selected) * len(inputs["series"][0]["actual"]):
            raise RuntimeError("incomplete common directional result matrix")
        for model, record in selected.items():
            observed = [r for r in directions if r["model"] == model]
            if [r["correct_count"] for r in observed] != record["correct_counts"] or any(
                r["evaluation_count"] != len(inputs["series"]) or r["input_fingerprint"] != record["input_fingerprint"]
                for r in observed
            ):
                raise RuntimeError("common directional results disagree with selection")
        for model in ("m4_smyl_mantis", "chronos_2_mantis", "m4_smyl_oracle"):
            for s in inputs["series"]:
                diagnostic = self.diagnostic_mean(experiment_id, model, s["forecast_instance_id"],
                                                  inputs["frequency"])
                if diagnostic["metadata"]["candidate_id"] != selected[model]["candidate_id"]:
                    raise RuntimeError("diagnostic selection lineage mismatch")
        diagnostic_count = self.connection.execute(
            "SELECT count(*) FROM paper_diagnostic_means WHERE experiment_id=?", [experiment_id]
        ).fetchone()[0]
        if diagnostic_count != 3 * len(inputs["series"]):
            raise RuntimeError("unexpected selected diagnostic membership")
        summary = self.summary(experiment_id)
        expected_report = evaluation.report(selected, directions)
        if summary["report"] != expected_report:
            raise RuntimeError("paper table/rank reconciliation mismatch")
        tasks = self.connection.execute(
            """SELECT candidate,status FROM experiment_tasks WHERE experiment_id=? AND stage=6
               AND (candidate='paper_tables' OR starts_with(candidate,'directional_'))""",
            [experiment_id]).fetchall()
        expected_tasks = {"paper_tables"} | {f"{m}:h{h:02d}" for m in
                         ("directional_dtw", "directional_mantis_rf")
                         for h in range(1, len(inputs["series"][0]["actual"]) + 1)}
        if (len(tasks) != len(expected_tasks) or {r[0] for r in tasks} != expected_tasks
                or any(r[1] != "completed" for r in tasks)):
            raise RuntimeError("incomplete Process 06 task identities")
        return {"output_validated": True, "paper_candidates": len(records),
                "selected_models": len(selected), "directional_rows": len(directions)}
