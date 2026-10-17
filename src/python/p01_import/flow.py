"""Process01 orchestration: configured source population to accepted canonical data.

The meaningful subflows own canonical data and configured archive restart
boundaries in experiment.duckdb. Scientific calculations live outside flows.
"""

from __future__ import annotations

from prefect import flow, task
from prefect.cache_policies import NO_CACHE

from util.runtime import DataRef, ProcessContext, WorkItem


@flow(name="01.01 Register source population")
def register_source_population(context: ProcessContext) -> DataRef:
    """Meaningful subflow: register the ordered population once in DuckDB."""
    with context.tracking.subflow("01.01", "Register source population"):
        if context.state.is_subflow_accepted("01.01"):
            return context.imports.existing_population()

        population = context.imports.register_source_population(context.request)
        context.state.accept_subflow("01.01")
        return population


@flow(name="01.02 Materialise canonical series")
def materialise_canonical_series(
    context: ProcessContext,
    population: DataRef,
) -> DataRef:
    """Meaningful subflow: accept missing canonical series at atomic-key boundaries."""
    with context.tracking.subflow("01.02", "Materialise canonical series"):
        if not context.state.is_subflow_accepted("01.02"):
            pending = context.imports.pending_canonical_series(population)
            context.execution.run(
                subflow_id="01.02",
                items=pending,
                calculate=context.imports.calculate_canonical_series,
                accept=context.imports.accept_canonical_series,
                on_progress=context.tracking.progress,
            )
            context.state.accept_subflow("01.02")

        return context.imports.canonical_dataset()


@flow(name="01.03 Accept canonical dataset")
def accept_canonical_dataset(
    context: ProcessContext,
    population: DataRef,
) -> DataRef:
    """Meaningful subflow: accept matching population/output identities, without refit."""
    with context.tracking.subflow("01.03", "Accept canonical dataset"):
        if context.state.is_subflow_accepted("01.03"):
            return context.imports.canonical_dataset()

        canonical = context.imports.accept_canonical_dataset(population)
        context.state.accept_subflow("01.03")
        return canonical


@task(name="Import archived forecast group", cache_policy=NO_CACHE, persist_result=False)
def import_archived_group(context: ProcessContext, items: list[WorkItem]) -> None:
    """Coordinator source-retrieval group; each forecast has its own atomic commit."""
    context.imports.import_archived_forecasts(items, context.tracking.progress)


@flow(name="01.04 Import archived forecasts")
def import_archived_forecasts(context: ProcessContext) -> None:
    """Meaningful subflow: accept missing archives in the common forecast pool."""
    with context.tracking.subflow("01.04", "Import archived forecasts"):
        pending = context.imports.pending_archived_forecasts()
        if pending:
            import_archived_group(context, pending)
        context.repository.require_complete("01.04")
        context.state.accept_subflow("01.04")


@flow(name="01 Import")
def run_import(context: ProcessContext) -> DataRef:
    """Process flow: accepted canonical data may still need configured archives."""
    with context.tracking.process("01", "Import"):
        if not context.state.is_process_accepted("01"):
            population = register_source_population(context)
            materialise_canonical_series(context, population)
            accept_canonical_dataset(context, population)
        if context.imports.archived_methods():
            import_archived_forecasts(context)
        context.state.accept_process("01")
        return context.imports.canonical_dataset()
