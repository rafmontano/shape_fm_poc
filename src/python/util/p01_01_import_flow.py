# ==============================================================================
# p01_01_import_flow.py
#
# Purpose: Readable Gate 1 Prefect composition over configuration, source, and storage.
# Inputs: An initialized experiment DuckDB with authoritative stored configuration.
# Outputs: A verified restartable import summary; all DuckDB writes remain local.
# Execution: Prefect coordinates local pure computation; ImportCoordinator is sole writer.
# Contracts: Tasks carry SeriesTask and return SeriesResult; failures remain durable/failed.
# Restart: Completed records are independently checked against the pinned configured source.
# Authority: Stored ExperimentConfiguration defines source identity, scope, and execution.
# Run from: Imported by the Process 01 worker interface; not run directly.
# ==============================================================================

"""Gate 1 Prefect composition for configured GIFT-Eval imports."""

from pathlib import Path
from typing import Any

from prefect import flow, task
from prefect.cache_policies import NO_CACHE

from .shared_database import load_database_configuration
from .p01_03_gift_eval_source import ConfiguredGiftEvalSource
from .p01_02_import_execution import ImportCoordinator, SeriesTask, SeriesResult, compute_series


@task(name="compute-import-series", cache_policy=NO_CACHE, persist_result=False)
def compute_import_series(task_input: SeriesTask) -> SeriesResult:
    """Compute one bounded series result without storage or writable connections."""
    return compute_series(task_input)


@flow(name="gate-1-configured-import", persist_result=False)
def gate1_import_flow(database: Path) -> dict[str, Any]:
    """Load stored configuration, bind source/storage, import, then verify completion."""
    configuration = load_database_configuration(database)
    source = ConfiguredGiftEvalSource(configuration, Path(__file__).resolve().parents[3])
    with ImportCoordinator(database, configuration) as storage:
        summary = storage.begin_configured_import(source)
        try:
            for record in source.records():
                job = storage.prepare_source_record(record, summary)
                if job is None:
                    continue
                attempt = storage.start_import_task(job)
                try:
                    result = compute_import_series(job)
                    storage.accept_import_result(result, attempt, summary)
                except Exception as error:
                    storage.fail_import_task(job, attempt, error, summary)
                    raise
            return storage.finish_configured_import(source, summary)
        except BaseException as error:
            storage.record_import_outcome(summary, f"{type(error).__name__}: {error}")
            raise
