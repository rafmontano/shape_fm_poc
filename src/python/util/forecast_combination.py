# ==============================================================================
# forecast_combination.py
#
# Purpose: Shared deterministic forecast calculations for coordinator and workers.
# Inputs: Configured forecast mappings with equally shaped mean, median, and quantile arrays.
# Outputs: Elementwise equal-weight forecast with noncrossing quantiles and a rearrangement flag.
# Run from: Imported; not run directly.
# ==============================================================================

"""Shared deterministic forecast calculations for coordinator and workers."""

from __future__ import annotations

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
