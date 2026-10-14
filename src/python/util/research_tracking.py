"""Coordinator-only MLflow projection for the six-process research workflow.

Inputs are experiment identity, milestone events, DuckDB progress and curated
final outputs. One resumable run provides visibility; tracking failure warns
without rejecting accepted scientific work. No nested or worker runs exist.
"""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Any, Iterator

from mlflow import MlflowClient
from prefect import runtime as prefect_runtime

from util.runtime import ExperimentRef, FinalResearchSummary, ProgressSnapshot

LOGGER = logging.getLogger(__name__)


class MLflowResearchTracking:
    """Project research milestones into the experiment's single MLflow run.

    The supported SQLite backend is operational state under .runtime/mlflow;
    experiment.duckdb remains the sole scientific restart authority.
    """

    def __init__(self, context: Any):
        """Bind coordinator state; defer tracking connection until experiment entry."""
        self.context = context
        self.client = None
        self.run_id = None
        self.last_progress = {}

    def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        if self.client is None:
            return None
        try:
            return getattr(self.client, method)(*args, **kwargs)
        except Exception as error:
            LOGGER.warning("MLflow update unavailable: %s", error)
            return None

    def _tags(self, values: dict[str, str]) -> None:
        for key, value in values.items():
            self._call("set_tag", self.run_id, f"shapefm.{key}", value)

    @contextmanager
    def experiment(self, experiment: ExperimentRef) -> Iterator[None]:
        """Experiment-level visibility; resume the run ID stored in DuckDB.

        Scientific acceptance is established by Process06, never by run status.
        A process-bounded invocation ends ready_to_resume rather than accepted.
        """
        try:
            runtime = self.context.request.root / ".runtime/mlflow"
            runtime.mkdir(parents=True, exist_ok=True)
            self.client = MlflowClient(tracking_uri=f"sqlite:///{runtime / 'tracking.sqlite'}")
            ml_experiment = self.client.get_experiment_by_name("ShapeFM")
            experiment_id = (ml_experiment.experiment_id if ml_experiment else
                             self.client.create_experiment("ShapeFM", artifact_location=(runtime / "artifacts").as_uri()))
            self.run_id = self.context.state.mlflow_run_id()
            if self.run_id is None:
                self.run_id = self.client.create_run(experiment_id).info.run_id
                self.context.state.set_mlflow_run_id(self.run_id)
                config = self.context.configuration
                for key, value in {"dataset": config["data"]["dataset_name"],
                                   "frequency": config["data"]["benchmark"]["frequency"],
                                   "series_count": config["data"]["selection"]["count"],
                                   "seed": config["reproducibility"]["seed"],
                                   "preprocessing": config["pipeline"]["preprocessing"]["default"]}.items():
                    self._call("log_param", self.run_id, key, str(value))
            self._call("update_run", self.run_id, status="RUNNING")
            self._tags({"experiment_id": experiment.experiment_id, "state": "running"})
            self._call("set_tag", self.run_id, "prefect.flow_run_id", str(prefect_runtime.flow_run.id or ""))
        except Exception as error:
            LOGGER.warning("MLflow tracking unavailable: %s", error)
            self.client = None
        try:
            yield
        except BaseException as error:
            self._tags({"state": "failed", "last_error": str(error)[:1000]})
            self._call("set_terminated", self.run_id, status="FAILED")
            raise
        else:
            self._tags({"state": "completed" if self.context.state.is_experiment_accepted() else "ready_to_resume"})
            self._call("set_terminated", self.run_id, status="FINISHED")

    @contextmanager
    def process(self, process_id: str, name: str) -> Iterator[None]:
        """Process-level milestone; the accepted process row is its restart boundary."""
        self._tags({"current_process": process_id, "current_process_name": name,
                    "current_subflow": "", "process_state": "running"})
        try:
            yield
        except BaseException:
            self._tags({"process_state": "failed"})
            raise
        else:
            self._tags({"process_state": "completed", "last_completed_process": process_id})

    @contextmanager
    def subflow(self, subflow_id: str, name: str) -> Iterator[None]:
        """Meaningful-subflow milestone; accepted atomic keys survive failed attempts."""
        self._tags({"current_subflow": subflow_id, "current_subflow_name": name, "subflow_state": "running"})
        try:
            yield
        except BaseException:
            self._tags({"subflow_state": "failed"})
            raise
        else:
            self._tags({"subflow_state": "completed", "last_completed_subflow": subflow_id})

    def progress(self, snapshot: ProgressSnapshot) -> None:
        """Log completion, or explicitly enabled periodic aggregate progress only."""
        now = time.monotonic()
        interval = self.context.configuration.get("research_tracking", {}).get("progress", {}).get(snapshot.subflow_id)
        if snapshot.completed != snapshot.total and (interval is None or
                now - self.last_progress.get(snapshot.subflow_id, 0) < interval):
            return
        self.last_progress[snapshot.subflow_id] = now
        metrics = {"completed_items": snapshot.completed, "total_items": snapshot.total,
                   "progress_percent": 100 * snapshot.completed / snapshot.total if snapshot.total else 100,
                   "elapsed_minutes": snapshot.elapsed_minutes,
                   "items_per_minute": snapshot.items_per_minute or 0,
                   "approximate_eta_minutes": snapshot.eta_minutes or 0}
        for key, value in metrics.items():
            self._call("log_metric", self.run_id, f"{snapshot.subflow_id}.{key}", float(value))

    def publish_final(self, summary: FinalResearchSummary) -> None:
        """Publish only curated final metrics and declared CSV/PNG/PDF outputs."""
        for key, value in summary.metrics.items():
            self._call("log_metric", self.run_id, key, value)
        for artifact in summary.artifacts:
            self._call("log_artifact", self.run_id, artifact)
