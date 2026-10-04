# ==============================================================================
# p05_01_forecast_combination.py
#
# Purpose: Shared deterministic forecast calculations for coordinator and workers.
# Inputs: Configured forecast mappings with equally shaped mean, median, and quantile arrays.
# Outputs: Elementwise equal-weight forecast with noncrossing quantiles and a rearrangement flag.
# Run from: Imported; not run directly.
# ==============================================================================

"""Shared deterministic forecast calculations for coordinator and workers."""

from __future__ import annotations

import math
from typing import Any


def combine_equal_weight(
    components: dict[str, dict[str, Any]], weights: dict[str, float]
) -> dict[str, Any]:
    """Combine configured distributions with equal weights and strict array shapes.

    Arrays use levels-by-horizon quantiles. Corresponding means, medians and
    quantiles are averaged; sorting removes crossings independently per horizon.
    Two-component arithmetic retains the historical (left + right) / 2 result.
    """
    if not components or weights != {name: 1 / len(components) for name in components}:
        raise ValueError("equal-weight combination requires every configured component")

    def average(arrays: list[list[float]]) -> list[float]:
        """Average corresponding values, rejecting unequal lengths."""
        return [sum(values) / len(arrays) for values in zip(*arrays, strict=True)]

    quantiles = [
        average(list(level))
        for level in zip(*(item["quantiles"] for item in components.values()), strict=True)
    ]
    points = list(zip(*quantiles, strict=True))
    rearranged = any(tuple(point) != tuple(sorted(point)) for point in points)
    if rearranged:
        quantiles = [
            list(values)
            for values in zip(*(sorted(point) for point in points), strict=True)
        ]
    return {
        "mean": average([item["mean"] for item in components.values()]),
        "median": average([item["median"] for item in components.values()]),
        "quantiles": quantiles,
        "quantiles_rearranged": rearranged,
    }


def combine_m4_point(
    components: dict[str, dict[str, Any]], weights: dict[str, float]
) -> dict[str, Any]:
    """Calculate official M4 Comb from stored SES, Holt, and Damped means only."""
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
