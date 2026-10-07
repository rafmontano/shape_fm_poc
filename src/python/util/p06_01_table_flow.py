# ==============================================================================
# p06_01_table_flow.py
# Purpose: Process 06 bounded CPU sensitivity and selected diagnostic completion.
# Inputs: Canonical stored inputs, scientific configuration and existing execution settings.
# Outputs: Coordinator-committed candidates, selections, diagnostics and reconciled reports.
# Run from: Imported; not run directly.
# ==============================================================================
"""Prefect/Dask own sequencing and scheduling; one R kernel owns the science."""

from __future__ import annotations

import hashlib
import json
import math
import os
import subprocess
import tempfile
import uuid
from pathlib import Path

from .shared_configuration import canonical_json, json_fingerprint
from .p06_02_table_storage import TableStorage
from .p06_03_table_reports import TableReports, M4_FREQUENCIES

# Code constants: versioned source calculation boundary and immutable worker input cache.
ROOT = Path(__file__).resolve().parents[3]
SCIENCE_FILES = ("src/r/util/forecast_adjustments.R", "src/r/util/paper_table_metrics.R",
                 "src/r/06_02_evaluate_paper_tables.R")


def install_table_inputs(fingerprint: str, payload: dict, dask_worker=None) -> dict:
    """Install one verified read-only input per worker, not one cohort copy per candidate."""
    from .shared_distributed_execution import _immutable_cache_directory
    if json_fingerprint(payload) != fingerprint:
        raise RuntimeError("paper input cache fingerprint mismatch")
    directory = _immutable_cache_directory(dask_worker) / "paper-inputs"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{fingerprint}.json"
    if path.exists():
        if json.loads(path.read_text()) != payload:
            raise RuntimeError("conflicting immutable paper inputs")
    else:
        with tempfile.NamedTemporaryFile("w", dir=directory, delete=False) as stream:
            stream.write(canonical_json(payload))
            temporary = Path(stream.name)
        try:
            os.link(temporary, path)
        except FileExistsError:
            if json.loads(path.read_text()) != payload:
                raise RuntimeError("conflicting concurrent paper inputs")
        finally:
            temporary.unlink(missing_ok=True)
    return {"fingerprint": fingerprint, "path": str(path)}


def evaluate_table_batch(batch: list[dict], options: dict) -> dict:
    """Call the same single-threaded R calculation locally or inside a CPU Dask task."""
    from .shared_distributed_execution import _immutable_cache_directory, _worker_provenance
    fingerprint = options["dataset_fingerprint"]
    path = _immutable_cache_directory() / "paper-inputs" / f"{fingerprint}.json"
    payload = json.loads(path.read_text())
    if json_fingerprint(payload) != fingerprint:
        raise RuntimeError("worker paper input fingerprint mismatch")
    payload["jobs"] = batch
    environment = {**os.environ, "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1",
                   "MKL_NUM_THREADS": "1", "VECLIB_MAXIMUM_THREADS": "1"}
    result = subprocess.run(["Rscript", "--vanilla", SCIENCE_FILES[-1]],
                            input=canonical_json(payload), cwd=ROOT, env=environment,
                            capture_output=True, text=True, timeout=options["timeout"])
    if result.returncode:
        raise RuntimeError(f"paper scientific worker failed: {result.stderr.strip()}")
    response = json.loads(result.stdout)
    response["worker"] = _worker_provenance()
    return response


class TableEvaluation:
    """Own scientific jobs, dependency identities and report construction, not scheduling."""

    def __init__(self, experiment_id: str, inputs: dict, science: dict):
        """Bind one frequency/cohort and centrally validated profile/grid definitions."""
        self.experiment_id, self.inputs, self.science = experiment_id, inputs, science
        self.source_fingerprint = json_fingerprint({p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest()
                                                   for p in SCIENCE_FILES})

    def jobs(self) -> list[dict]:
        """Derive seven baselines/direct diagnostics and 525 jobs per adjusted base."""
        jobs = [{"model": m, "lambda_up": None, "lambda_down": None, "retain_means": False}
                for m in ("naive2", "m4_fforma", "m4_smyl", "chronos_2", "smyl_oracle",
                          "directional_dtw", "directional_mantis_rf")]
        grid = self.science["grid"]
        axes = []
        for name in ("lambda_up", "lambda_down"):
            axis = grid[name]
            count = round((axis["stop"] - axis["start"]) / axis["step"]) + 1
            axes.append([round(axis["start"] + i * axis["step"], 3) for i in range(count)])
        for model in self.science["adjusted_base_models"]:
            jobs.extend({"model": model, "lambda_up": up, "lambda_down": down, "retain_means": False}
                        for up in axes[0] for down in axes[1])
        return jobs

    def identity(self, job: dict) -> dict:
        """Fingerprint only this job's scientific inputs and dependency lineage, never topology."""
        model = "m4_smyl" if job["model"] == "smyl_oracle" else job["model"]
        adjusted = job["lambda_up"] is not None
        dependencies = {model} if model.startswith("directional_") else {model, "naive2"}
        if adjusted:
            dependencies.add(self.science["direction_model"])
        items = []
        for s in self.inputs["series"]:
            items.append({"instance": s["forecast_instance_id"], "context": s["context"],
                          "actual": s["actual"],
                          "means": {m: s["means"][m] for m in sorted(dependencies) if m in s["means"]},
                          "directions": {m: s["directions"][m] for m in sorted(dependencies) if m in s["directions"]},
                          "sources": {m: s["sources"][m] for m in sorted(dependencies)}})
        content = {"experiment_id": self.experiment_id, "frequency": self.inputs["frequency"],
                   "model": job["model"], "lambda_up": job["lambda_up"], "lambda_down": job["lambda_down"],
                   "profile": self.science["metric_profile"],
                   "grid_fingerprint": json_fingerprint(self.science["grid"]) if adjusted else None,
                   "adjustment": (self.science["adjustment"] if adjusted else
                                  "smyl_scalar_smape_oracle_v1" if job["model"] == "smyl_oracle" else None),
                   "source_fingerprint": self.source_fingerprint,
                   "input_fingerprint": json_fingerprint(items)}
        return {"candidate_id": "paper-candidate/" + json_fingerprint(content), **content}

    def validate_result(self, result: dict, job: dict) -> None:
        """Reject wrong-job responses, nonfinite metrics and incomplete/wrong-horizon counts."""
        if any(result[k] != job[k] for k in ("model", "lambda_up", "lambda_down")):
            raise RuntimeError("mismatched paper worker job")
        n, counts = result["evaluation_count"], result["correct_counts"]
        h = len(self.inputs["series"][0]["actual"])
        if type(n) is not int or n != len(self.inputs["series"]) or len(counts) != h or any(
            type(v) is not int or not 0 <= v <= n for v in counts
        ):
            raise RuntimeError("invalid paper worker counts/horizon")
        direct = job["model"].startswith("directional_")
        metrics = result["metrics"]
        if set(metrics) != {"smape", "mase", "owa", "da"} or metrics["da"] != counts[-1] / n:
            raise RuntimeError("invalid paper worker metric profile")
        for field in ("smape", "mase", "owa"):
            v = metrics[field]
            if direct and v is not None or not direct and (type(v) not in (int, float) or not math.isfinite(v) or v < 0):
                raise RuntimeError("nonfinite/inapplicable paper worker metric")

    def record(self, result: dict, job: dict) -> dict:
        """Return scientific aggregate content independent of runtime, host and retained vectors."""
        self.validate_result(result, job)
        content = {**self.identity(job), **{k: result[k] for k in
                   ("metrics", "correct_counts", "evaluation_count")}}
        return {**content, "result_fingerprint": json_fingerprint(content)}

    def batches(self, jobs: list[dict], size: int) -> list[list[dict]]:
        """Keep bounded scheduling units within one base model; no per-series grid tasks."""
        if type(size) is not int or size <= 0:
            raise ValueError("positive table batch size required")
        batches = []
        for model in dict.fromkeys(j["model"] for j in jobs):
            group = [j for j in jobs if j["model"] == model]
            batches.extend(group[i:i + size] for i in range(0, len(group), size))
        return batches

    def validate_retained(self, result: dict, job: dict, stored: dict) -> None:
        """Validate vector-only recalculation against the authoritative first surface.

        Identity, membership, parameters and counts remain exact. Measured Mac/R
        versus Ubuntu/R reductions differed by at most 8.53e-14 absolute and
        4.29e-15 relative; only aggregate floats allow 1e-13/5e-15 respectively.
        Never replace the stored values, selection or fingerprint with this record.
        """
        current = self.record(result, job)
        exact = set(stored) - {"metrics", "result_fingerprint"}
        if set(current) != set(stored) or any(current[k] != stored[k] for k in exact):
            raise RuntimeError("selected retention identity or discrete counts mismatch")
        if current["metrics"]["da"] != stored["metrics"]["da"] or any(
            not math.isclose(current["metrics"][k], stored["metrics"][k],
                             abs_tol=1e-13, rel_tol=5e-15)
            for k in ("smape", "mase", "owa")
        ):
            raise RuntimeError("selected retention metrics exceed numerical tolerance")

    def select(self, records: list[dict]) -> dict[str, dict]:
        """Select independent complete surfaces and retain seven baseline/direct identities."""
        selected = {}
        for r in records:
            if r["lambda_up"] is None:
                selected["m4_smyl_oracle" if r["model"] == "smyl_oracle" else r["model"]] = r
        for model in self.science["adjusted_base_models"]:
            surface = [r for r in records if r["model"] == model and r["lambda_up"] is not None]
            if len(surface) != 525 or len({(r["lambda_up"], r["lambda_down"]) for r in surface}) != 525:
                raise RuntimeError("incomplete or duplicate lambda surface")
            selected[model + "_mantis"] = TableReports.select(surface)
        return selected

    def report(self, selected: dict[str, dict], directional: list[dict]) -> dict:
        """Use one selection/common directional matrix for both tables and descriptive ranks."""
        rows = [{**r, "model": m} for m, r in selected.items()]
        table_rows = rows
        if {r["frequency"] for r in rows} == set(M4_FREQUENCIES):
            table_rows = rows + TableReports.aggregate_all(rows)
        horizons = {self.inputs["frequency"]: list(range(1, len(self.inputs["series"][0]["actual"]) + 1))}
        return {**TableReports.tables(table_rows, directional),
                "selected_pairs": {m: {"lambda_up": r["lambda_up"], "lambda_down": r["lambda_down"],
                                       "candidate_id": r["candidate_id"]}
                                   for m, r in selected.items() if r["lambda_up"] is not None},
                "directional_profile": TableReports.directional_profile(
                    directional, self.science["directional_report_models"], horizons, rank=False),
                "figure_2": {"horizon": TableReports.directional_profile(
                    directional, self.science["figure_2"]["horizon_models"], horizons, rank=False),
                    "cd": TableReports.directional_profile(
                        directional, self.science["figure_2"]["cd_models"], horizons)},
                "profile": self.science["metric_profile"], "ex_post": True,
                "scope": "Daily-only validation; not complete published Table2"}


def run_table_evaluation(*, coordinator, experiment_id: str, rows: list,
                         attempts: dict, workers: int, settings=None) -> None:
    """Run Process 06 barriers on the coordinator with bounded Prefect CPU submissions."""
    from .shared_workflow_orchestration import run_gate_compute_flow
    if not rows:
        return
    storage = TableStorage(coordinator.connection)
    inputs = storage.load_inputs(experiment_id, coordinator.configuration)
    science = coordinator.configuration.resolved["evaluation"]["table_reproduction"]
    evaluation = TableEvaluation(experiment_id, inputs, science)
    records = []
    pending = []
    for job in evaluation.jobs():
        record = storage.candidate(evaluation.identity(job)["candidate_id"])
        if record is None:
            pending.append(job)
        else:
            evaluation.validate_result(record, job)
            records.append(record)
    payload = {"frequency": inputs["frequency"], "series": [
        {k: s[k] for k in ("series_id", "context", "actual", "means", "directions")}
        for s in inputs["series"]]}
    fingerprint = json_fingerprint(payload)
    client = coordinator._active_dask_client
    install_table_inputs(fingerprint, payload)
    if client is not None:
        client.run(install_table_inputs, fingerprint, payload)
    options = {"paper_tables": True, "dataset_fingerprint": fingerprint,
               "timeout": coordinator.configuration.execution["worker_timeouts_seconds"]["r"]}

    def compute(jobs):
        """Submit only bounded jobs to the existing runner and yield checked completion-order responses."""
        if not jobs:
            return
        for outcome in run_gate_compute_flow(
            process_id=6, batches=evaluation.batches(jobs, coordinator.table_sensitivity_batch_size),
            options=options, scheduler_address=client.scheduler.address if client else None,
            retries=settings.dask_retries if settings else 0,
            max_in_flight=settings.dask_max_in_flight if settings else workers,
            local_workers=workers,
        ):
            if "error" in outcome:
                raise RuntimeError(outcome["error"])
            response = outcome["response"]
            batch = outcome["batch"]
            if len(response["results"]) != len(batch):
                raise RuntimeError("paper response batch cardinality mismatch")
            yield batch, response

    for batch, response in compute(pending):
        completed = [evaluation.record(result, job) for result, job in zip(response["results"], batch, strict=True)]
        storage.save_batch(completed, {"worker": response["worker"], "runtime_seconds": response["runtime_seconds"],
                                       "batch_size": len(batch), "max_in_flight": settings.dask_max_in_flight if settings else workers})
        records.extend(completed)
    selected = evaluation.select(records)
    # Recalculate only the selected two adjusted collections and Oracle for retention.
    retained = {}
    table_row = next((r for r in rows if r[3] == "paper_tables"), None)
    retain_models = ("m4_smyl_mantis", "chronos_2_mantis", "m4_smyl_oracle")
    retain_jobs = [{"model": selected[m]["model"], "lambda_up": selected[m]["lambda_up"],
                    "lambda_down": selected[m]["lambda_down"], "retain_means": True}
                   for m in retain_models] if table_row is not None else []
    for batch, response in compute(retain_jobs):
        for job, result in zip(batch, response["results"], strict=True):
            model = "m4_smyl_oracle" if job["model"] == "smyl_oracle" else job["model"] + "_mantis"
            evaluation.validate_retained(result, job, selected[model])
            retained[model] = result["adjusted_means"]

    def finish():
        """Persist selections, point-only diagnostics and common report under one completion transaction."""
        directional = []
        for model, record in selected.items():
            storage.save_selected(record, model, retained.get(model, []), inputs["series"])
            directional.extend(storage.save_directional(record, model))
        storage.save_report(experiment_id, fingerprint, evaluation.report(selected, directional))

    if table_row is not None:
        coordinator._commit_task(table_row[0], attempts[table_row[0]], 0.0, finish,
                                 {"candidate_count": len(records), "ex_post": True})
    # Preserve the accepted cleaned-origin directional evaluation separately from
    # the historical raw-origin paper profile; neither label definition is replaced.
    from .shared_labels import directional_accuracy
    for row in rows:
        if row[3] == "paper_tables":
            continue
        model, htext = row[3].rsplit(":h", 1)
        horizon = int(htext)
        table = "directional_model_definitions" if model == "directional_dtw" else "directional_composite_model_definitions"
        model_id = coordinator.connection.execute(
            f"SELECT model_definition_id FROM {table} WHERE experiment_id=?", [experiment_id]).fetchone()[0]
        values = coordinator.connection.execute(
            """SELECT p.evaluation_input_id,p.prediction,a.labels FROM model_directional_predictions p
               JOIN directional_actual_labels a USING(evaluation_input_id)
               WHERE p.experiment_id=? AND p.model_definition_id=? AND p.horizon=?
               ORDER BY p.evaluation_input_id""", [experiment_id, model_id, horizon]).fetchall()
        score = directional_accuracy([v[1] for v in values], [v[2][horizon - 1] for v in values])
        prediction_hash = json_fingerprint([{"evaluation_input_id": v[0], "prediction": int(v[1]),
                                             "actual": int(v[2][horizon - 1])} for v in values])
        content = {"experiment_id": experiment_id, "model_definition_id": model_id,
                   "horizon": horizon, **score, "prediction_fingerprint": prediction_hash}
        record = {"directional_evaluation_id": "directional-evaluation/" +
                  json_fingerprint({"model": model_id, "horizon": horizon})[:32],
                  **content, "content_hash": json_fingerprint(content)}
        coordinator._commit_task(row[0], attempts[row[0]], 0.0,
            lambda record=record: coordinator._insert_or_verify("model_directional_evaluations", record),
            {"execution_backend": "coordinator; accepted directional labels"})
