"""Process 03 orchestration: accepted histories to the modelling dataset.

The four approved meaningful subflows restart from parent membership/features
and child origin keys; no forecasts or decision-model fitting occur here.
"""

from __future__ import annotations

from prefect import flow

from util.runtime import DataRef, ProcessContext


@flow(name="03.01 Assign research membership")
def assign_research_membership(context: ProcessContext, prepared: DataRef) -> DataRef:
    """Meaningful-subflow level: persist seeded series membership in the parent."""
    with context.tracking.subflow("03.01", "Assign research membership"):
        if context.state.is_subflow_accepted("03.01"):
            return context.transform.existing_membership()
        membership = context.transform.assign_research_membership(prepared)
        context.state.accept_subflow("03.01")
        return membership


@flow(name="03.02 Materialise modelling inputs")
def materialise_modelling_inputs(context: ProcessContext, membership: DataRef) -> None:
    """Meaningful-subflow level: accept missing origins at child atomic-key boundaries."""
    with context.tracking.subflow("03.02", "Materialise modelling inputs"):
        if context.state.is_subflow_accepted("03.02"):
            return
        pending = context.transform.pending_modelling_inputs(membership)
        context.execution.run(subflow_id="03.02", items=pending,
            calculate=context.transform.calculate_modelling_inputs,
            calculate_batch=context.transform.calculate_modelling_batch,
            accept=context.transform.accept_modelling_inputs, on_progress=context.tracking.progress)
        context.state.accept_subflow("03.02")


@flow(name="03.03 Materialise reusable features")
def materialise_reusable_features(context: ProcessContext, membership: DataRef) -> None:
    """Meaningful-subflow level: accept missing feature vectors in the parent."""
    with context.tracking.subflow("03.03", "Materialise reusable features"):
        if context.state.is_subflow_accepted("03.03"):
            return
        pending = context.transform.pending_reusable_features(membership)
        context.execution.run(subflow_id="03.03", items=pending,
            calculate=context.transform.calculate_reusable_features,
            calculate_batch=context.transform.calculate_feature_batch,
            accept=context.transform.accept_reusable_features, on_progress=context.tracking.progress)
        context.state.accept_subflow("03.03")


@flow(name="03.04 Accept modelling dataset")
def accept_modelling_dataset(context: ProcessContext, membership: DataRef) -> DataRef:
    """Meaningful-subflow level: require planned inputs/features and the shared cohort."""
    with context.tracking.subflow("03.04", "Accept modelling dataset"):
        if context.state.is_subflow_accepted("03.04"):
            return context.transform.modelling_dataset()
        modelling = context.transform.accept_modelling_dataset(membership)
        context.state.accept_subflow("03.04")
        return modelling


@flow(name="03 Transform")
def run_transform(context: ProcessContext) -> DataRef:
    """Process level: order 03.01–03.04; return accepted modelling data on restart."""
    with context.tracking.process("03", "Transform"):
        context.state.require_prerequisites("03")
        if context.state.is_process_accepted("03"):
            return context.transform.modelling_dataset()
        prepared = context.transform.prepared_dataset()
        membership = assign_research_membership(context, prepared)
        materialise_modelling_inputs(context, membership)
        materialise_reusable_features(context, membership)
        modelling = accept_modelling_dataset(context, membership)
        context.state.accept_process("03")
        return modelling
