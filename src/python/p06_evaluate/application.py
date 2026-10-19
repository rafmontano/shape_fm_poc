"""Process 06 evaluation, diagnostic selection and stored research publication.

Accepted forecasts and canonical actuals produce per-series scores, a complete
configured surface, selected-only paths and table/figure data in the parent DB.
No model is fitted and no hardware or historical comparison is executed.
"""

from __future__ import annotations

import json
import math
import os
import subprocess
from typing import Any

from p06_evaluate.reports import TableReports
from util.providers import run_r
from util.runtime import CODE_ROOT, DataRef, FinalResearchSummary, WorkItem


def calculate_forecast_evaluation(item: WorkItem) -> dict[str, Any]:
    """Evaluate one accepted series/candidate through the pinned GIFT-Eval scorer."""
    return calculate_forecast_batch([item])[0][1]


def calculate_forecast_batch(items: list[WorkItem]) -> list[tuple[str, dict[str, Any]]]:
    """Return capability-appropriate native metrics; reject unusable provider output."""
    settings = items[0].payload["execution"]
    response = subprocess.run([str(CODE_ROOT / settings["environment"] / "bin/python"),
        "-X", "faulthandler", "-m", "p06_evaluate.gift"],
        input=json.dumps({"jobs": [item.payload["job"] for item in items]}),
        cwd=CODE_ROOT, env={**os.environ, "PYTHONPATH": str(CODE_ROOT / "src/python")},
        capture_output=True, text=True, timeout=settings["timeout"])
    try:
        response.check_returncode()
    except subprocess.CalledProcessError as error:
        error.add_note(f"GIFT-Eval batch: {[item.key for item in items]}")
        error.add_note(response.stderr)
        raise
    rows = json.loads(response.stdout)["results"]
    if {row["key"] for row in rows} != {item.key for item in items}:
        raise ValueError("GIFT-Eval returned unexpected atomic identities")
    by_key = {row["key"]: row for row in rows}
    results = []
    for item in items:
        metrics = by_key[item.key]["metrics"]
        if not metrics or not all(math.isfinite(value) for value in metrics.values()):
            raise ValueError(f"unusable GIFT-Eval metrics: {item.key}")
        results.append((item.key, {"input_key": item.payload["input_key"],
            "model": item.payload["model"], "metrics": metrics,
            "profile": "mean_based_v1" if item.payload["job"]["forecast"].get("quantile_levels") is None
                       else "gift_eval_probabilistic_v1"}))
    return results


def calculate_directional_evaluation(item: WorkItem) -> dict[str, Any]:
    """Score stored predictions against the explicitly selected evaluation labels.

    docs/evaluation.md and the paper-table kernel use future > last observation;
    equality belongs to down and no numerical dead zone exists.
    """
    payload = item.payload
    predicted = payload["labels"]
    actual = payload["actual_labels"]
    return {"input_key": payload["input_key"], "model": payload["model"],
            "evaluation_basis": payload["evaluation_basis"],
            "correct_counts": [int(left == right) for left, right in zip(predicted, actual, strict=True)],
            "evaluation_count": 1}


def calculate_diagnostic_candidate(item: WorkItem, *, series: list[dict[str, Any]]) -> dict[str, Any]:
    """Return one aggregate sensitivity/baseline record, without persistence.

    Stored raw cohort and candidate settings follow docs/evaluation.md; realized
    actuals are allowed here because adjustment selection is ex-post diagnostic.
    """
    return calculate_diagnostic_batch([item], series=series)[0][1]


def calculate_diagnostic_batch(items: list[WorkItem], *, series: list[dict[str, Any]]) -> list[tuple[str, dict[str, Any]]]:
    """Call the accepted R paper-table/Oracle kernel for bounded candidate groups."""
    response = run_r("src/r/p06_evaluate/evaluation_worker.R", {
        "frequency": "Daily", "series": series,
        "jobs": [item.payload["job"] for item in items]}, timeout=1800, threads=1)
    rows = response["results"]
    if len(rows) != len(items):
        raise ValueError("paper evaluator returned an incomplete batch")
    results = []
    for item, row in zip(items, rows, strict=True):
        job = item.payload["job"]
        if any(row[field] != job[field] for field in ("model", "lambda_up", "lambda_down")):
            raise ValueError("paper evaluator returned a different candidate")
        if (len(row["correct_counts"]) != 14 or row["evaluation_count"] != len(series)
                or any(not 0 <= count <= row["evaluation_count"] for count in row["correct_counts"])
                or any(value is not None and not math.isfinite(value) for value in row["metrics"].values())):
            raise ValueError(f"unusable diagnostic output: {item.key}")
        results.append((item.key, {**row, "frequency": "Daily", "candidate_id": item.key,
                                   "evaluation_basis": "raw_origin"}))
    return results


def calculate_selected_diagnostic(item: WorkItem, *, series: list[dict[str, Any]]) -> dict[str, Any]:
    """Retain selected-only adjusted paths, without rescoring the accepted surface."""
    response = run_r("src/r/p06_evaluate/diagnostic_worker.R", {"jobs": [
        {"key": item.key, "series": series, **item.payload["job"]}]},
        timeout=1800, threads=1)["results"][0]
    rows = response["adjusted_means"]
    if (response["key"] != item.key or len(rows) != len(series)
            or any(len(row["mean"]) != 14 or not all(math.isfinite(v) for v in row["mean"]) for row in rows)):
        raise ValueError(f"unusable selected diagnostic: {item.key}")
    return {"candidate_id": item.payload["candidate_id"], "model": item.key, "adjusted_means": rows}


class Application:
    """Own seven Evaluate subflows and coordinator-only accepted atomic results."""

    calculate_forecast_evaluation = staticmethod(calculate_forecast_evaluation)
    calculate_forecast_batch = staticmethod(calculate_forecast_batch)
    calculate_directional_evaluation = staticmethod(calculate_directional_evaluation)
    calculate_diagnostic_candidate = staticmethod(calculate_diagnostic_candidate)
    calculate_diagnostic_batch = staticmethod(calculate_diagnostic_batch)
    calculate_selected_diagnostic = staticmethod(calculate_selected_diagnostic)

    def __init__(self, context: Any):
        """Bind accepted-result storage and the stored diagnostic/publication settings."""
        self.context, self.store = context, context.repository
        self.science = context.configuration["evaluation"]["table_reproduction"]

    def forecast_candidates(self) -> DataRef:
        """Return accepted individual/combined/deployable candidates."""
        return self.context.ref("05.03")

    def selected_diagnostics(self) -> DataRef:
        """Return the retained selected diagnostic reference."""
        return self.context.ref("06.04")

    def research_results(self) -> DataRef:
        """Return assembled stored tables and configured figure matrices."""
        return self.context.ref("06.05")

    def _series(self) -> list[dict[str, Any]]:
        canonical = self.store.rows("01.02")
        forecasts = self.store.rows("04.01") | self.store.rows("05.01") | self.store.rows("05.02")
        directions = self.store.rows("04.04")
        output = []
        for key, row in self.context.windows().rows("03.02", kind="official",
                fields=("series_key", "series_id", "frequency", "labels")).items():
            source = canonical[row["series_key"]]
            context = source["target"][:source["test_start"]]
            output.append({"input_key": key, "series_id": row["series_id"], "start": source["start"],
                "frequency": row["frequency"], "context": context,
                "actual": source["target"][source["test_start"]:],
                "prepared_labels": row["labels"],
                "forecasts": {r["model"]: r for r in forecasts.values() if r["input_key"] == key},
                "directions": {r["model"]: r["labels"] for r in directions.values() if r["input_key"] == key}})
        return output

    def diagnostic_series(self) -> list[dict[str, Any]]:
        """Read the unchanged raw cohort once for shared diagnostic transport."""
        return [{"series_id": s["series_id"], "context": s["context"], "actual": s["actual"],
                 "means": {m: r["mean"] for m, r in s["forecasts"].items()}, "directions": s["directions"]}
                for s in self._series()]

    def pending_forecast_evaluations(self, candidates: DataRef) -> list[WorkItem]:
        """Plan one capability-aware GIFT-Eval score per stored series/candidate."""
        config = self.context.configuration
        execution = {"environment": config["evaluation"]["gift_eval"]["environment"],
                     "timeout": config["execution"]["default"]["worker_timeouts_seconds"]["gift_eval"]}
        items = []
        for series in self._series():
            for model, forecast in series["forecasts"].items():
                key = f"{series['input_key']}/{model}"
                items.append(WorkItem(key, {"input_key": series["input_key"], "model": model,
                    "execution": execution, "job": {k: series[k] for k in
                        ("series_id", "start", "frequency", "context", "actual")} | {
                        "key": key, "forecast": forecast, "options": config["evaluation"]["gift_eval_options"]}}))
        return self.store.plan("06.01", items)

    def accept_forecast_evaluations(self, results: list[tuple[str, dict[str, Any]]]) -> None:
        """Commit complete per-series native score records in the parent."""
        self.store.accept("06.01", results)

    def pending_directional_evaluations(self, candidates: DataRef) -> list[WorkItem]:
        """Use official prepared labels for direct classifiers, raw labels for points."""
        items = []
        for series in self._series():
            origin = series["context"][-1]
            labels = {m: [int(v > origin) for v in r["mean"]] for m, r in series["forecasts"].items()}
            for model, values in (labels | series["directions"]).items():
                direct = model in series["directions"]
                items.append(WorkItem(f"{series['input_key']}/{model}", {"input_key": series["input_key"],
                    "model": model, "labels": values,
                    "actual_labels": series["prepared_labels"] if direct else
                        [int(value > origin) for value in series["actual"]],
                    "evaluation_basis": "prepared_origin" if direct else "raw_origin"}))
        return self.store.plan("06.02", items)

    def accept_directional_evaluations(self, results: list[tuple[str, dict[str, Any]]]) -> None:
        """Commit complete per-origin directional count vectors."""
        self.store.accept("06.02", results)

    def pending_diagnostic_candidates(self, candidates: DataRef, series: list[dict[str, Any]]) -> list[WorkItem]:
        """Plan the declared seven baselines and 1,050 fixed diagnostic candidates."""
        jobs = [{"model": model, "lambda_up": None, "lambda_down": None, "retain_means": False}
                for model in ("naive2", "m4_fforma", "m4_smyl", "chronos_2", "smyl_oracle",
                              "directional_dtw", "directional_mantis_rf")]
        axes = []
        # Inclusive configured axes define the ex-post surface in docs/evaluation.md.
        for name in ("lambda_up", "lambda_down"):
            axis = self.science["grid"][name]
            count = round((axis["stop"] - axis["start"]) / axis["step"]) + 1
            axes.append([round(axis["start"] + i * axis["step"], 3) for i in range(count)])
        for model in self.science["adjusted_base_models"]:
            jobs.extend({"model": model, "lambda_up": up, "lambda_down": down, "retain_means": False}
                        for up in axes[0] for down in axes[1])
        return self.store.plan("06.03", [WorkItem(
            f"Daily/{job['model']}/{job['lambda_up']}/{job['lambda_down']}",
            {"job": job}) for job in jobs])

    def accept_diagnostic_candidates(self, results: list[tuple[str, dict[str, Any]]]) -> None:
        """Accept each aggregate surface candidate independently for restart."""
        self.store.accept("06.03", results)

    def select_diagnostic_settings(self, candidates: DataRef) -> dict[str, dict[str, Any]]:
        """Select stored OWA minima using the tie order in docs/evaluation.md.

        Require the full accepted surface; return settings, not deployable models.
        """
        self.store.require_complete("06.03")
        rows = list(self.store.rows("06.03").values())
        selected = {"m4_smyl_oracle" if r["model"] == "smyl_oracle" else r["model"]: r
                    for r in rows if r["lambda_up"] is None}
        for model in self.science["adjusted_base_models"]:
            selected[model + "_mantis"] = TableReports.select([
                r for r in rows if r["model"] == model and r["lambda_up"] is not None])
        return selected

    def pending_selected_diagnostics(self, selection: dict[str, dict[str, Any]]) -> list[WorkItem]:
        """Plan only selected adjusted paths and the declared ex-post Oracle output."""
        return self.store.plan("06.04", [WorkItem(model, {
            "candidate_id": row["candidate_id"], "job": {k: row[k] for k in ("model", "lambda_up", "lambda_down")}})
            for model, row in selection.items() if row["lambda_up"] is not None or model == "m4_smyl_oracle"])

    def accept_selected_diagnostics(self, results: list[tuple[str, dict[str, Any]]]) -> None:
        """Store only selected adjusted vectors, preserving surface selection lineage."""
        self.store.accept("06.04", results)

    def finalise_selected_diagnostics(self, selection: dict[str, dict[str, Any]]) -> DataRef:
        """Persist selected identities after the selected output keys are accepted."""
        self.store.require_complete("06.04")
        if not self.store.keys("selected_settings"):
            self.store.accept("selected_settings", [("selection", selection)])
        return self.selected_diagnostics()

    def assemble_research_results(self, candidates: DataRef) -> DataRef:
        """Assemble tables/ranks from accepted scores and selected paths, never refit."""
        selected = self.store.rows("selected_settings")["selection"]
        directional = []
        for model, row in selected.items():
            if row["evaluation_basis"] != "raw_origin":
                raise ValueError("paper tables require raw-origin sensitivity scores")
            for index, correct in enumerate(row["correct_counts"], 1):
                directional.append({"frequency": "Daily", "model": model, "horizon": index,
                    "evaluation_basis": row["evaluation_basis"],
                    "correct_count": correct, "evaluation_count": row["evaluation_count"]})
        rows = [{**row, "model": model} for model, row in selected.items()]
        direct_models = {"directional_dtw", "directional_mantis_rf"}
        figure_directional = [row for row in directional if row["model"] not in direct_models]
        prepared = {}
        for row in self.store.rows("06.02").values():
            if row["model"] not in direct_models:
                continue
            if row["evaluation_basis"] != "prepared_origin":
                raise ValueError("direct classifier figures require prepared-origin scores")
            for index, correct in enumerate(row["correct_counts"], 1):
                key = (row["model"], index)
                aggregate = prepared.setdefault(key, {"frequency": "Daily", "model": row["model"],
                    "horizon": index, "evaluation_basis": "prepared_origin",
                    "correct_count": 0, "evaluation_count": 0})
                aggregate["correct_count"] += correct
                aggregate["evaluation_count"] += row["evaluation_count"]
        figure_directional.extend(prepared.values())
        horizons = {"Daily": list(range(1, 15))}
        figure = self.science["figure_2"]
        report = {**TableReports.tables(rows, directional), "directional_rows": directional,
            "figure_directional_rows": figure_directional,
            "evaluation_provenance": {"tables": "raw_origin",
                "figure_2": {model: "prepared_origin" if model in direct_models else "raw_origin"
                             for model in set(figure["horizon_models"] + figure["cd_models"])}},
            "figure_2": {"horizon": TableReports.directional_profile(figure_directional, figure["horizon_models"], horizons, rank=False),
                         "cd": TableReports.directional_profile(figure_directional, figure["cd_models"], horizons)},
            "figure_settings": figure}
        self.store.accept("06.05", [("research_results", report)])
        return self.research_results()

    def publish_outputs(self, results: DataRef) -> FinalResearchSummary:
        """Publish only declared CSV/PNG/PDF files from the stored assembled report."""
        from p06_evaluate.publication import publish_stored_outputs
        summary = publish_stored_outputs(self.context.request.root, self.store.rows("06.05")["research_results"])
        self.store.accept("06.06", [("published", {"metrics": summary.metrics, "artifacts": list(summary.artifacts)})])
        return summary

    def published_summary(self) -> FinalResearchSummary:
        """Return registered final outputs without rendering or recalculation."""
        row = self.store.rows("06.06")["published"]
        return FinalResearchSummary(row["metrics"], tuple(row["artifacts"]))

    def accept_experiment(self, results: DataRef) -> None:
        """Require six evaluation predecessors and registered approved output files."""
        from pathlib import Path
        for subflow in ("06.01", "06.02", "06.03", "06.04", "06.05", "06.06"):
            if not self.context.state.is_subflow_accepted(subflow):
                raise RuntimeError(f"missing prerequisite {subflow}")
        if not all(Path(path).is_file() for path in self.published_summary().artifacts):
            raise RuntimeError("registered final output is missing")
