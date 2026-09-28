# ==============================================================================
# forecast_combination.py
#
# Purpose: Shared deterministic forecast calculations for coordinator and workers.
# Inputs: Two forecast mappings with equally shaped mean, median, and quantile arrays.
# Outputs: Elementwise equal-weight forecast with noncrossing quantiles and a rearrangement flag.
# Run from: Imported; not run directly.
# ==============================================================================

"""Shared deterministic forecast calculations for coordinator and workers."""

from __future__ import annotations

from typing import Any


def combine_equal_weight(
    left: dict[str, Any], right: dict[str, Any], weights: dict[str, float]
) -> dict[str, Any]:
    """Apply configured equal model weights and rearrange crossed output quantiles."""
    if weights != {"auto_arima": 0.5, "chronos_2": 0.5}:
        raise ValueError("equal-weight combination requires two weights of 0.5")

    def average(a: list[float], b: list[float]) -> list[float]:
        """Return strict pairwise arithmetic means, rejecting unequal lengths."""
        return [(x + y) / 2.0 for x, y in zip(a, b, strict=True)]

    quantiles = [
        average(left_values, right_values)
        for left_values, right_values in zip(
            left["quantiles"], right["quantiles"], strict=True
        )
    ]
    points = list(zip(*quantiles, strict=True))
    rearranged = any(tuple(point) != tuple(sorted(point)) for point in points)
    if rearranged:
        quantiles = [
            list(values)
            for values in zip(*(sorted(point) for point in points), strict=True)
        ]
    return {
        "mean": average(left["mean"], right["mean"]),
        "median": average(left["median"], right["median"]),
        "quantiles": quantiles,
        "quantiles_rearranged": rearranged,
    }
