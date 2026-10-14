"""Process 05 combination arithmetic under the forecast black-box contract.

Stored components and configured weights produce combined horizon arrays,
without fitting, scheduling, persistence or diagnostic adjustment.
"""

from __future__ import annotations

import math
from typing import Any


def combine_m4_point(
    components: dict[str, dict[str, Any]], weights: dict[str, float]
) -> dict[str, Any]:
    """Return mean-only M4 Comb from stored SES, Holt and Damped horizons.

    At each step, mean = (SES + Holt + Damped) / 3, as specified in
    docs/forecast-black-box-contract.md. Require ordered, finite, equal-length
    mean-only components and exact one-third weights; invalid inputs raise.
    No fitting or persistence occurs and probabilistic fields remain absent.
    """
    names = ("ses", "holt", "damped")
    expected = {name: 1.0 / 3.0 for name in names}
    if tuple(components) != names or weights != expected:
        raise ValueError("m4_comb requires ordered SES, Holt, and Damped one-third components")
    horizons = {len(components[name].get("mean", [])) for name in names}
    if horizons == {0} or len(horizons) != 1:
        raise ValueError("m4_comb components must have one equal positive horizon")
    for name in names:
        component = components[name]
        if component.get("capability") != "mean_only":
            raise ValueError("m4_comb components must be mean-only forecasts")
        if any(not isinstance(value, (int, float)) or not math.isfinite(value)
               for value in component["mean"]):
            raise ValueError("m4_comb components must contain finite means")
    return {
        "mean": [
            sum(components[name]["mean"][step] * weights[name] for name in names)
            for step in range(horizons.pop())
        ],
        "median": None,
        "quantiles": None,
        "quantiles_rearranged": False,
    }
