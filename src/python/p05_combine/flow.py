"""Process 05 Prefect hierarchy from stored pool to accepted forecast candidates.

Three ordered subflows restart on complete combined/adjusted horizon keys in
the parent DuckDB; the shared executor returns pure worker calculations.
"""

from prefect import flow

from util.runtime import DataRef, ProcessContext


@flow(name="05.01 Materialise forecast combinations")
def materialise_forecast_combinations(context: ProcessContext, pool: DataRef) -> None:
    """Subflow level: accept complete combined horizons with component lineage."""
    with context.tracking.subflow("05.01", "Materialise forecast combinations"):
        if context.state.is_subflow_accepted("05.01"):
            return
        context.execution.run(subflow_id="05.01", items=context.combine.pending_combinations(pool),
            calculate=context.combine.calculate_combination, accept=context.combine.accept_combinations,
            on_progress=context.tracking.progress)
        context.state.accept_subflow("05.01")


@flow(name="05.02 Materialise approved deployable adjustments")
def materialise_deployable_adjustments(context: ProcessContext, pool: DataRef) -> None:
    """Subflow level: accept only explicitly enabled deployable adjustment keys."""
    with context.tracking.subflow("05.02", "Materialise approved deployable adjustments"):
        if context.state.is_subflow_accepted("05.02"):
            return
        context.execution.run(subflow_id="05.02", items=context.combine.pending_deployable_adjustments(pool),
            calculate=context.combine.calculate_deployable_adjustment,
            accept=context.combine.accept_deployable_adjustments, on_progress=context.tracking.progress)
        context.state.accept_subflow("05.02")


@flow(name="05.03 Accept forecast candidates")
def accept_forecast_candidates(context: ProcessContext, pool: DataRef) -> DataRef:
    """Subflow level: require all planned candidates before exposing their reference."""
    with context.tracking.subflow("05.03", "Accept forecast candidates"):
        if context.state.is_subflow_accepted("05.03"):
            return context.combine.forecast_candidates()
        candidates = context.combine.accept_forecast_candidates(pool)
        context.state.accept_subflow("05.03")
        return candidates


@flow(name="05 Combine")
def run_combine(context: ProcessContext) -> DataRef:
    """Process level: order 05.01–05.03; accepted DuckDB keys govern restart."""
    with context.tracking.process("05", "Combine"):
        context.state.require_prerequisites("05")
        if context.state.is_process_accepted("05"):
            return context.combine.forecast_candidates()
        pool = context.combine.forecast_pool()
        materialise_forecast_combinations(context, pool)
        materialise_deployable_adjustments(context, pool)
        candidates = accept_forecast_candidates(context, pool)
        context.state.accept_process("05")
        return candidates
