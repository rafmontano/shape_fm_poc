"""Process 05 stored-component combination and deployable candidate acceptance.

Accepted individual forecasts produce combined horizons with component lineage.
No forecast fitting, Oracle or sensitivity-grid calculation belongs here.
"""

from __future__ import annotations

from typing import Any

from p05_combine.science import combine_m4_point
from util.runtime import DataRef, WorkItem


def calculate_combination(item: WorkItem) -> dict[str, Any]:
    """Combine stored SES/Holt/Damped means at equal thirds; preserve their keys.

    The M4 Comb formula and mean-only capability follow the forecast contract.
    Workers consume accepted components, never invoke a forecast provider.
    """
    payload = item.payload
    return {**combine_m4_point(payload["components"], payload["weights"]),
            "input_key": payload["input_key"], "model": "m4_comb",
            "forecast_capability": "mean_only", "output_scale": "original",
            "component_keys": payload["component_keys"], "weights": payload["weights"]}


class Application:
    """Own Process 05 plans and complete-horizon restart keys in the parent DB."""

    calculate_combination = staticmethod(calculate_combination)

    def __init__(self, context: Any):
        self.context, self.store = context, context.repository

    def forecast_pool(self) -> DataRef:
        """Return the accepted individual/directional Process 04 pool."""
        return self.context.ref("04.05")

    def forecast_candidates(self) -> DataRef:
        """Return stored individual, combined and enabled adjusted candidates."""
        return self.context.ref("05.03")

    def pending_combinations(self, pool: DataRef) -> list[WorkItem]:
        """Plan one M4 Comb horizon per origin using all accepted component keys."""
        configuration = self.context.configuration["pipeline"]["combination"]
        if configuration["method"] != "m4_comb":
            raise ValueError("unsupported configured combination")
        forecasts = self.store.rows("04.01")
        inputs = sorted({row["input_key"] for row in forecasts.values()})
        items = []
        for input_key in inputs:
            keys = {model: f"{input_key}/{model}" for model in ("ses", "holt", "damped")}
            components = {model: {**forecasts[key], "capability": forecasts[key]["forecast_capability"]}
                          for model, key in keys.items()}
            items.append(WorkItem(f"{input_key}/m4_comb", {
                "input_key": input_key, "components": components,
                "component_keys": keys, "weights": configuration["weights"]}))
        return self.store.plan("05.01", items)

    def accept_combinations(self, results: list[tuple[str, dict[str, Any]]]) -> None:
        """Accept combined horizons with unchanged component identities and weights."""
        self.store.accept("05.01", results)

    def pending_deployable_adjustments(self, pool: DataRef) -> list[WorkItem]:
        """Identity enables no extra candidate; diagnostics stay in Process 06."""
        if self.context.configuration["pipeline"]["adjustment"] != "identity":
            raise ValueError("no non-identity deployable adjustment is approved")
        return self.store.plan("05.02", [])

    def calculate_deployable_adjustment(self, item: WorkItem) -> dict[str, Any]:
        """Fail for an unconfigured adjustment rather than inventing a candidate."""
        raise ValueError("no deployable adjustment is configured")

    def accept_deployable_adjustments(self, results: list[tuple[str, dict[str, Any]]]) -> None:
        """Accept only explicitly planned deployable adjustment results."""
        self.store.accept("05.02", results)

    def accept_forecast_candidates(self, pool: DataRef) -> DataRef:
        """Require the planned combinations and enabled adjustments, without refits."""
        self.store.require_complete("05.01")
        self.store.require_complete("05.02")
        return self.forecast_candidates()
