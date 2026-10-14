"""Process 02 orchestration: canonical histories to the prepared dataset.

Inputs are Process 01's accepted series. The two approved meaningful subflows
restart from parent DuckDB prepared-series keys and the dataset milestone.
"""

from __future__ import annotations

from prefect import flow

from util.runtime import DataRef, ProcessContext


@flow(name="02.01 Materialise prepared histories")
def materialise_prepared_histories(context: ProcessContext, canonical: DataRef) -> DataRef:
    """Meaningful-subflow level: accept missing prepared series into the parent."""
    with context.tracking.subflow("02.01", "Materialise prepared histories"):
        if not context.state.is_subflow_accepted("02.01"):
            pending = context.preprocess.pending_prepared_histories(canonical)
            context.execution.run(
                subflow_id="02.01", items=pending,
                calculate=context.preprocess.calculate_prepared_history,
                calculate_batch=context.preprocess.calculate_prepared_histories,
                accept=context.preprocess.accept_prepared_histories,
                on_progress=context.tracking.progress,
            )
            context.state.accept_subflow("02.01")
        return context.preprocess.prepared_dataset()


@flow(name="02.02 Accept prepared dataset")
def accept_prepared_dataset(context: ProcessContext, canonical: DataRef) -> DataRef:
    """Meaningful-subflow level: require complete histories and store acceptance."""
    with context.tracking.subflow("02.02", "Accept prepared dataset"):
        if context.state.is_subflow_accepted("02.02"):
            return context.preprocess.prepared_dataset()
        prepared = context.preprocess.accept_prepared_dataset(canonical)
        context.state.accept_subflow("02.02")
        return prepared


@flow(name="02 Preprocess")
def run_preprocess(context: ProcessContext) -> DataRef:
    """Process level: order 02.01–02.02; restart returns the accepted dataset."""
    with context.tracking.process("02", "Preprocess"):
        context.state.require_prerequisites("02")
        if context.state.is_process_accepted("02"):
            return context.preprocess.prepared_dataset()
        canonical = context.preprocess.canonical_dataset()
        materialise_prepared_histories(context, canonical)
        prepared = accept_prepared_dataset(context, canonical)
        context.state.accept_process("02")
        return prepared
