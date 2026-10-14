"""Process 04 orchestration from modelling inputs to the stored forecast pool.

The five approved subflows restart from parent atomic forecast, representation,
model and prediction keys. Only the coordinator accepts scientific outputs.
"""

from __future__ import annotations

from typing import Callable

from prefect import flow

from util.runtime import DataRef, ProcessContext


def _execute(context: ProcessContext, subflow: str, name: str, modelling: DataRef,
             pending: Callable, calculate: Callable, accept: Callable,
             calculate_batch: Callable | None = None) -> None:
    with context.tracking.subflow(subflow, name):
        if context.state.is_subflow_accepted(subflow):
            return
        context.execution.run(subflow_id=subflow, items=pending(modelling),
            calculate=calculate, calculate_batch=calculate_batch,
            accept=accept, on_progress=context.tracking.progress)
        context.state.accept_subflow(subflow)


@flow(name="04.01 Materialise individual forecasts")
def materialise_individual_forecasts(context: ProcessContext, modelling: DataRef) -> None:
    """Meaningful-subflow level: accept complete horizons at forecast-key boundaries."""
    service = context.forecast
    _execute(context, "04.01", "Materialise individual forecasts", modelling,
        service.pending_individual_forecasts, service.calculate_individual_forecast,
        service.accept_individual_forecasts, service.calculate_forecast_batch)


@flow(name="04.02 Materialise representations")
def materialise_representations(context: ProcessContext, modelling: DataRef) -> None:
    """Meaningful-subflow level: persist reusable embeddings by modelling-input key."""
    service = context.forecast
    _execute(context, "04.02", "Materialise representations", modelling,
        service.pending_representations, service.calculate_representation,
        service.accept_representations, service.calculate_representation_batch)


@flow(name="04.03 Fit decision models")
def fit_decision_models(context: ProcessContext, modelling: DataRef) -> None:
    """Meaningful-subflow level: accept reusable fitted artifacts by training unit."""
    service = context.forecast
    _execute(context, "04.03", "Fit decision models", modelling,
        service.pending_decision_models, service.calculate_decision_model,
        service.accept_decision_models)


@flow(name="04.04 Materialise directional predictions")
def materialise_directional_predictions(context: ProcessContext, modelling: DataRef) -> None:
    """Meaningful-subflow level: accept complete label vectors by model/input key."""
    service = context.forecast
    _execute(context, "04.04", "Materialise directional predictions", modelling,
        service.pending_directional_predictions, service.calculate_directional_prediction,
        service.accept_directional_predictions)


@flow(name="04.05 Accept forecast pool")
def accept_forecast_pool(context: ProcessContext, modelling: DataRef) -> DataRef:
    """Meaningful-subflow level: require every planned pool item before acceptance."""
    with context.tracking.subflow("04.05", "Accept forecast pool"):
        if context.state.is_subflow_accepted("04.05"):
            return context.forecast.forecast_pool()
        pool = context.forecast.accept_forecast_pool(modelling)
        context.state.accept_subflow("04.05")
        return pool


@flow(name="04 Forecast")
def run_forecast(context: ProcessContext) -> DataRef:
    """Process level: order 04.01–04.05; accepted DuckDB keys own restart."""
    with context.tracking.process("04", "Forecast"):
        context.state.require_prerequisites("04")
        context.forecast.reconcile_fitted_models()
        if context.state.is_process_accepted("04"):
            return context.forecast.forecast_pool()
        modelling = context.forecast.modelling_dataset()
        materialise_individual_forecasts(context, modelling)
        materialise_representations(context, modelling)
        fit_decision_models(context, modelling)
        materialise_directional_predictions(context, modelling)
        pool = accept_forecast_pool(context, modelling)
        context.state.accept_process("04")
        return pool
