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
from .p01_04_import_state import ImportState


@task(name="compute-import-series", cache_policy=NO_CACHE, persist_result=False)
def compute_import_series(task_input: SeriesTask) -> SeriesResult:
    """Compute one bounded series result without storage or writable connections."""
    return compute_series(task_input)


@flow(name="01.01 Import canonical GIFT-Eval dataset", persist_result=False)
def canonical_import(storage, source, state, summary):
    """Import external series into atomic item commits, then checkpoint the cohort."""
    item = None
    try:
        if state.accepted("01.01"):
            storage.summarize_import(summary)
            summary["skipped_completed"] = summary["series_count"]
            return
        state.start("01.01")
        for record in source.records():
            item = record.source_series_id
            job = storage.prepare_source_record(record, summary)
            if job is None:
                continue
            attempt = storage.start_import_task(job)
            try:
                result = compute_import_series(job)
                storage.accept_import_result(result, attempt, summary)
                state.progress()
            except Exception as error:
                storage.fail_import_task(job, attempt, error, summary)
                raise
        item = None
        storage.finish_canonical_import(source, summary)
        state.complete("01.01")
    except BaseException as error:
        state.fail("01.01", error, item)
        raise


@flow(name="01.02 Persist and accept canonical import", persist_result=False)
def import_acceptance(storage, source, state, summary):
    """Persist canonical import acceptance without consulting another dataset."""
    try:
        if state.accepted("01.02"):
            return
        state.start("01.02")
        storage.finish_configured_import(source, summary)
        state.complete("01.02")
    except BaseException as error:
        state.fail("01.02", error)
        raise


@flow(name="gate-1-configured-import", persist_result=False)
def gate1_import_flow(database: Path) -> dict[str, Any]:
    """Compose two durable import boundaries; Prefect owns their dependency."""
    configuration = load_database_configuration(database)
    with ImportCoordinator(database, configuration) as storage:
        state = ImportState(storage.connection, configuration)
        state.initialize()
        summary = None
        try:
            source = ConfiguredGiftEvalSource(configuration, Path(__file__).resolve().parents[3])
            summary = storage.begin_configured_import(source)
            canonical_import(storage, source, state, summary)
            import_acceptance(storage, source, state, summary)
            return summary
        except BaseException as error:
            if summary is None:
                state.fail("01.01", error)
            else:
                storage.record_import_outcome(summary, f"{type(error).__name__}: {error}")
            raise
