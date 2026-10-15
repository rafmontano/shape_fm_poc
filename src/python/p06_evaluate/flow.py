"""Process 06 Prefect hierarchy for evaluation, diagnostics and final publication.

Seven approved subflows restart on parent metric/candidate/selected-output keys
and durable assembled/published references. Evaluation never refits forecasts.
ProcessContext and accepted candidate references enter; the process returns the
stored research-results reference after publication and experiment acceptance.
"""

from typing import Callable

from prefect import flow

from util.runtime import DataRef, FinalResearchSummary, ProcessContext


def _execute(context: ProcessContext, subflow: str, name: str, pending: Callable,
             calculate: Callable, accept: Callable, calculate_batch: Callable | None = None) -> None:
    with context.tracking.subflow(subflow, name):
        if context.state.is_subflow_accepted(subflow):
            return
        context.execution.run(subflow_id=subflow, items=pending(), calculate=calculate,
            calculate_batch=calculate_batch, accept=accept, on_progress=context.tracking.progress)
        context.state.accept_subflow(subflow)


@flow(name="06.01 Evaluate forecast accuracy")
def evaluate_forecast_accuracy(context: ProcessContext, candidates: DataRef) -> None:
    """Subflow level: accept capability-aware GIFT-Eval scores by series/candidate."""
    service = context.evaluate
    _execute(context, "06.01", "Evaluate forecast accuracy", lambda: service.pending_forecast_evaluations(candidates),
        service.calculate_forecast_evaluation, service.accept_forecast_evaluations, service.calculate_forecast_batch)


@flow(name="06.02 Evaluate directional performance")
def evaluate_directional_performance(context: ProcessContext, candidates: DataRef) -> None:
    """Subflow level: accept complete directional counts by series/method key."""
    service = context.evaluate
    _execute(context, "06.02", "Evaluate directional performance", lambda: service.pending_directional_evaluations(candidates),
        service.calculate_directional_evaluation, service.accept_directional_evaluations)


@flow(name="06.03 Materialise diagnostic surfaces")
def materialise_diagnostic_surfaces(context: ProcessContext, candidates: DataRef) -> None:
    """Subflow level: accept each declared sensitivity/baseline candidate independently."""
    service = context.evaluate
    with context.tracking.subflow("06.03", "Materialise diagnostic surfaces"):
        if context.state.is_subflow_accepted("06.03"):
            return
        series = service.diagnostic_series()
        context.execution.run(subflow_id="06.03", items=service.pending_diagnostic_candidates(candidates, series),
            calculate=service.calculate_diagnostic_candidate, calculate_batch=service.calculate_diagnostic_batch,
            shared_inputs={"series": series}, accept=service.accept_diagnostic_candidates,
            on_progress=context.tracking.progress)
        context.state.accept_subflow("06.03")


@flow(name="06.04 Select and retain diagnostics")
def select_and_retain_diagnostics(context: ProcessContext, candidates: DataRef) -> DataRef:
    """Subflow level: select stored settings and accept only selected output keys."""
    with context.tracking.subflow("06.04", "Select and retain diagnostics"):
        service = context.evaluate
        if context.state.is_subflow_accepted("06.04"):
            return service.selected_diagnostics()
        selection = service.select_diagnostic_settings(candidates)
        context.execution.run(subflow_id="06.04", items=service.pending_selected_diagnostics(selection),
            calculate=service.calculate_selected_diagnostic, accept=service.accept_selected_diagnostics,
            shared_inputs={"series": service.diagnostic_series()},
            on_progress=context.tracking.progress)
        selected = service.finalise_selected_diagnostics(selection)
        context.state.accept_subflow("06.04")
        return selected


@flow(name="06.05 Assemble research results")
def assemble_research_results(context: ProcessContext, candidates: DataRef) -> DataRef:
    """Subflow level: persist assembled tables and ranks from accepted results."""
    with context.tracking.subflow("06.05", "Assemble research results"):
        if context.state.is_subflow_accepted("06.05"):
            return context.evaluate.research_results()
        results = context.evaluate.assemble_research_results(candidates)
        context.state.accept_subflow("06.05")
        return results


@flow(name="06.06 Publish approved outputs")
def publish_approved_outputs(context: ProcessContext, results: DataRef) -> FinalResearchSummary:
    """Subflow level: publish stored results and return registered file references.

    Accepted 06.06 resumes the stored summary without rendering; before milestone
    acceptance, files may be rendered again. MLflow does not own this boundary.
    """
    with context.tracking.subflow("06.06", "Publish approved outputs"):
        if context.state.is_subflow_accepted("06.06"):
            return context.evaluate.published_summary()
        summary = context.evaluate.publish_outputs(results)
        context.tracking.publish_final(summary)
        context.state.accept_subflow("06.06")
        return summary


@flow(name="06.07 Accept experiment")
def accept_experiment(context: ProcessContext, results: DataRef) -> None:
    """Subflow level: require accepted predecessors and registered output references."""
    with context.tracking.subflow("06.07", "Accept experiment"):
        if context.state.is_subflow_accepted("06.07"):
            return
        context.evaluate.accept_experiment(results)
        context.state.accept_subflow("06.07")


@flow(name="06 Evaluate")
def run_evaluate(context: ProcessContext) -> DataRef:
    """Process level: order 06.01–06.07; accepted atomic keys govern restart."""
    with context.tracking.process("06", "Evaluate"):
        context.state.require_prerequisites("06")
        if context.state.is_process_accepted("06"):
            return context.evaluate.research_results()
        candidates = context.evaluate.forecast_candidates()
        evaluate_forecast_accuracy(context, candidates)
        evaluate_directional_performance(context, candidates)
        materialise_diagnostic_surfaces(context, candidates)
        selected = select_and_retain_diagnostics(context, candidates)
        results = assemble_research_results(context, selected)
        publish_approved_outputs(context, results)
        accept_experiment(context, results)
        context.state.accept_process("06")
        return results
