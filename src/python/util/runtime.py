"""Shared application runtime for Processes 01–06.

Inputs are one creation-time experiment document or an existing experiment ID.
Outputs are process services backed by the two fixed scientific DuckDB stores.
DuckDB, not operational orchestration history, owns accepted atomic identities.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

import duckdb

CODE_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class ExperimentRequest:
    """Experiment-level request; a resume uses stored scientific configuration.

    `root` selects the experiment package, not scientific identity. A process
    range stops at its last process and never implicitly runs later processes.
    """

    processes: tuple[int, ...] = (1, 2, 3, 4, 5, 6)
    root: Path = CODE_ROOT
    experiment_id: str | None = None
    execution_profile: str = "sequential_safe"

    def includes(self, process: int) -> bool:
        """Return whether this invocation requests the numbered process."""
        return process in self.processes


@dataclass(frozen=True)
class DataRef:
    """Scientific output reference: the experiment and owning accepted subflow."""

    experiment_id: str
    subflow_id: str


@dataclass(frozen=True)
class ExperimentRef:
    """Identity of the single scientific experiment stored in the parent."""

    experiment_id: str


@dataclass(frozen=True)
class WorkItem:
    """Atomic calculation request with a scientific key and pure worker inputs.

    Resource labels affect placement only. No database writer or tracking client
    is carried to a worker. The complete result is accepted by its coordinator.
    """

    key: str
    payload: dict[str, Any]
    resources: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class ProgressSnapshot:
    """DuckDB aggregate progress for one meaningful subflow, not worker telemetry."""

    subflow_id: str
    completed: int
    total: int
    elapsed_minutes: float
    items_per_minute: float | None
    eta_minutes: float | None


@dataclass(frozen=True)
class FinalResearchSummary:
    """Curated final comparison metrics and declared CSV/PNG/PDF artifact paths."""

    metrics: dict[str, float]
    artifacts: tuple[str, ...]


class Repository:
    """Coordinator-owned scientific storage shared by all six process services.

    Plans and accepted results are keyed by subflow and atomic scientific key.
    Each acceptance call commits its complete results and keys in one transaction.
    Process 03 creates the child; downstream services reuse its recorded ownership.
    """

    def __init__(self, path: Path, experiment_id: str):
        """Open a coordinator writer and establish store ownership/result tables."""
        self.path = path
        self.experiment_id = experiment_id
        self.connection = duckdb.connect(str(path))
        self.connection.execute("CREATE TABLE IF NOT EXISTS ownership (experiment_id VARCHAR PRIMARY KEY)")
        owners = self.connection.execute("SELECT experiment_id FROM ownership").fetchall()
        if owners and owners != [(experiment_id,)]:
            raise ValueError("scientific database belongs to another experiment")
        self.connection.execute("INSERT INTO ownership VALUES (?) ON CONFLICT DO NOTHING", [experiment_id])
        self.connection.execute("""CREATE TABLE IF NOT EXISTS atomic_results (
            subflow_id VARCHAR, atomic_key VARCHAR, result JSON NOT NULL,
            accepted_at TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (subflow_id, atomic_key))""")
        self.connection.execute("""CREATE TABLE IF NOT EXISTS atomic_plan (
            subflow_id VARCHAR, atomic_key VARCHAR,
            PRIMARY KEY (subflow_id, atomic_key))""")

    def plan(self, subflow: str, items: Iterable[WorkItem]) -> list[WorkItem]:
        """Persist required identities and return only those not already accepted."""
        items = list(items)
        accepted = self.keys(subflow)
        if items:
            self.connection.executemany("INSERT INTO atomic_plan VALUES (?,?) ON CONFLICT DO NOTHING",
                                        [(subflow, item.key) for item in items])
        return [item for item in items if item.key not in accepted]

    def keys(self, subflow: str) -> set[str]:
        """Read accepted atomic keys without recalculating scientific results."""
        return {r[0] for r in self.connection.execute(
            "SELECT atomic_key FROM atomic_results WHERE subflow_id=?", [subflow]).fetchall()}

    def rows(self, subflow: str, *, keys: Iterable[str] | None = None,
             kind: str | None = None, fields: tuple[str, ...] | None = None) -> dict[str, dict[str, Any]]:
        """Read selected trusted outputs in stable atomic-key order.

        Filter and project inside DuckDB, before materialising numerical JSON
        arrays in Python. Downstream consumers need not load unrelated windows.
        """
        parameters: list[Any] = []
        projection = "result"
        if fields is not None:
            projection = "json_object(" + ",".join("?,json_extract(result,?)" for _ in fields) + ")"
            for field in fields:
                parameters.extend((field, f"$.{field}"))
        query = f"SELECT atomic_key,{projection} FROM atomic_results WHERE subflow_id=?"
        parameters.append(subflow)
        if keys is not None:
            selected = list(keys)
            if not selected:
                return {}
            query += " AND atomic_key IN (SELECT unnest(?))"
            parameters.append(selected)
        if kind is not None:
            query += " AND json_extract_string(result,'$.kind')=?"
            parameters.append(kind)
        return {key: json.loads(result) for key, result in self.connection.execute(
            query + " ORDER BY atomic_key", parameters).fetchall()}

    def accept(self, subflow: str, results: list[tuple[str, dict[str, Any]]], *,
               completion_subflow: str | None = None) -> None:
        """Commit results and optional cross-process completion references together.

        Imported forecasts live only in the common forecast rows; their import
        completion references that same accepted identity without copying values.
        """
        self.connection.begin()
        try:
            for key, result in results:
                self.connection.execute("INSERT INTO atomic_results (subflow_id,atomic_key,result) VALUES (?,?,?)",
                                        [subflow, key, json.dumps(result)])
                if completion_subflow is not None:
                    self.connection.execute("INSERT INTO atomic_results (subflow_id,atomic_key,result) VALUES (?,?,?)",
                        [completion_subflow, key, json.dumps({"subflow_id": subflow, "atomic_key": key})])
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

    def counts(self, subflow: str) -> tuple[int, int]:
        """Return durable completed/planned counts for acceptance and tracking."""
        return tuple(self.connection.execute("""SELECT
            (SELECT count(*) FROM atomic_results WHERE subflow_id=?),
            (SELECT count(*) FROM atomic_plan WHERE subflow_id=?)""", [subflow, subflow]).fetchone())

    def require_complete(self, subflow: str) -> None:
        """Require every planned key, without revalidating accepted content."""
        missing = self.connection.execute("""SELECT atomic_key FROM atomic_plan WHERE subflow_id=?
            EXCEPT SELECT atomic_key FROM atomic_results WHERE subflow_id=?""", [subflow, subflow]).fetchall()
        if missing:
            raise RuntimeError(f"{subflow}: {len(missing)} planned atomic items are missing")

    def close(self) -> None:
        """Release the coordinator's database writer connection."""
        self.connection.close()


class ExperimentState:
    """Scientific process/subflow acceptance and resumable MLflow identity.

    This is persisted completion state, not a scheduler or operational retry
    engine. Prefect owns attempts; a restart plans only unaccepted atomic keys.
    """

    def __init__(self, repository: Repository):
        """Bind parent storage and establish durable process/subflow milestones."""
        self.repository = repository
        repository.connection.execute("""CREATE TABLE IF NOT EXISTS milestones (
            milestone_id VARCHAR PRIMARY KEY, accepted_at TIMESTAMP DEFAULT current_timestamp)""")

    def is_subflow_accepted(self, subflow: str) -> bool:
        """Read the meaningful-subflow restart boundary."""
        return bool(self.repository.connection.execute(
            "SELECT 1 FROM milestones WHERE milestone_id=?", [subflow]).fetchone())

    def is_process_accepted(self, process: str) -> bool:
        """Read process acceptance; it never implies recomputation or validation."""
        return self.is_subflow_accepted(process)

    def accept_subflow(self, subflow: str) -> None:
        """Record acceptance after the owning subflow establishes its output."""
        self.repository.connection.execute("INSERT INTO milestones (milestone_id) VALUES (?) ON CONFLICT DO NOTHING",
                                           [subflow])

    def accept_process(self, process: str) -> None:
        """Record the process-level restart boundary after its subflows finish."""
        self.accept_subflow(process)

    def require_prerequisites(self, process: str) -> None:
        """Require the accepted preceding process before scientific calculation."""
        predecessor = f"{int(process) - 1:02d}"
        if not self.is_process_accepted(predecessor):
            raise RuntimeError(f"Process {process} requires accepted Process {predecessor}")

    def mlflow_run_id(self) -> str | None:
        """Read the experiment's single operational tracking-run reference."""
        return self.repository.connection.execute("SELECT mlflow_run_id FROM experiment").fetchone()[0]

    def set_mlflow_run_id(self, run_id: str) -> None:
        """Persist one tracking identity so later invocations resume that run."""
        self.repository.connection.execute("UPDATE experiment SET mlflow_run_id=?", [run_id])

    def is_experiment_accepted(self) -> bool:
        """Read final scientific acceptance independently of MLflow availability."""
        return self.is_process_accepted("06")


class ProcessContext:
    """Shared application services, coordinator state and execution for the flows.

    Flows see scientific actions and data references, not database SQL, worker
    placement or tracking APIs. Calculation callables receive pure WorkItems.
    """

    def __init__(self, request: ExperimentRequest, repository: Repository, configuration: dict[str, Any]):
        """Bind stored configuration and requested process services to one writer."""
        self.request, self.repository, self.configuration = request, repository, configuration
        self.experiment_ref = ExperimentRef(repository.experiment_id)
        self.state = ExperimentState(repository)
        self.tracking = None
        self.child = None
        from util.distributed_execution import WorkGroupExecutor
        from p01_import.application import ImportApplication
        self.execution = WorkGroupExecutor(self)
        self.imports = ImportApplication(self)
        for number, package, name in ((2, "p02_preprocess", "preprocess"), (3, "p03_transform", "transform"),
                                      (4, "p04_forecast", "forecast"), (5, "p05_combine", "combine"),
                                      (6, "p06_evaluate", "evaluate")):
            if number in request.processes:
                from importlib import import_module
                service = import_module(f"{package}.application").Application
                setattr(self, name, service(self))

    def with_tracking(self, tracking: Any) -> ProcessContext:
        """Bind coordinator-only research tracking before any process begins."""
        self.tracking = tracking
        return self

    def __getstate__(self) -> dict[str, Any]:
        """Expose identity only in Prefect's serialized flow-parameter context.

        The supported Dask integration carries parent flow parameters to task
        runs. Coordinator writers/services/tracking clients must not cross that
        transport boundary; scientific tasks use only their explicit WorkItems.
        """
        return {"experiment_ref": self.experiment_ref}

    def windows(self) -> Repository:
        """Lazily open the child writer for Process 03 outputs/downstream reads.

        Process 03 first creates it; later process services reuse the same stored
        experiment identity. Merely importing runtime never opens the child.
        """
        if self.child is None:
            self.child = Repository(self.request.root / "results/windows.duckdb", self.experiment_ref.experiment_id)
        return self.child

    def ref(self, subflow: str) -> DataRef:
        """Return a stable scientific reference owned by the named subflow."""
        return DataRef(self.experiment_ref.experiment_id, subflow)

    def close(self) -> None:
        """Release scientific writers after completion or failure."""
        try:
            self.execution.close()
        finally:
            if self.child is not None:
                self.child.close()
            self.repository.close()


def open_experiment(request: ExperimentRequest) -> ProcessContext:
    """Open/resume the experiment-level DuckDB boundary using configuration once.

    A new experiment reads config/experiment.json. Subsequent requests read only
    its stored document. This opener assumes the current schema; it performs no
    migration or deletion of an existing experiment.
    """
    path = request.root / "results/experiment.duckdb"
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    if exists:
        with duckdb.connect(str(path), read_only=True) as connection:
            experiment_id, encoded = connection.execute("SELECT experiment_id,configuration FROM experiment").fetchone()
        configuration = json.loads(encoded)
        if request.experiment_id is not None and request.experiment_id != experiment_id:
            raise ValueError("requested experiment ID does not match the stored experiment")
    else:
        configuration = json.loads((request.root / "config/experiment.json").read_text())
        configuration["data"]["source"]["directory"] = str((CODE_ROOT / configuration["data"]["source"]["directory"]).resolve())
        if request.experiment_id is not None:
            raise ValueError("cannot resume an experiment without its scientific database")
        experiment_id = str(uuid.uuid4())
    repository = Repository(path, experiment_id)
    if not exists:
        repository.connection.execute("""CREATE TABLE experiment (
            experiment_id VARCHAR PRIMARY KEY, configuration JSON NOT NULL,
            mlflow_run_id VARCHAR, created_at TIMESTAMP DEFAULT current_timestamp)""")
        repository.connection.execute("INSERT INTO experiment (experiment_id,configuration) VALUES (?,?)",
                                      [experiment_id, json.dumps(configuration)])
    return ProcessContext(request, repository, configuration)
