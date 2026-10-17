"""Process 04 scientific forecast boundary shared by R and Python providers.

Check usable inputs and finite horizon outputs, not internal envelope metadata.
Providers own their approved settings and fallback calculations; this module
has no storage or execution side effects (docs/forecast-black-box-contract.md).
"""

from __future__ import annotations

import math
from typing import Any, Sequence


class ForecastContract:
    """Check usable scientific outputs of the common forecast protocol."""

    @staticmethod
    def noncrossing_quantiles(
        quantiles: list[list[float]],
    ) -> tuple[list[list[float]], bool]:
        """Sort quantiles per horizon step and report whether rearrangement occurred."""
        points = list(zip(*quantiles, strict=True))
        rearranged = any(tuple(point) != tuple(sorted(point)) for point in points)
        if not rearranged:
            return quantiles, False
        return [
            list(values)
            for values in zip(*(sorted(point) for point in points), strict=True)
        ], True

    def validate_result(
        self, request: dict[str, Any], result: dict[str, Any]
    ) -> dict[str, Any]:
        """Check native output dimensions/values without revalidating the request."""
        if result["status"] == "error":
            return result
        if result["status"] != "success":
            raise ValueError("forecast result status must be success or error")
        horizon = request["horizon"]
        self._horizon(result["mean"], horizon, "mean")
        capability = request["required_capability"]
        levels = result["quantile_levels"]
        self._validate_levels(levels, capability)
        if capability == "mean_only":
            if result["median"] is not None or result["quantiles"] is not None:
                raise ValueError("mean-only result must use null probabilistic fields")
        else:
            if levels != request["quantile_levels"]:
                raise ValueError("forecast result quantile levels do not match request")
            self._horizon(result["median"], horizon, "median")
            quantiles = result["quantiles"]
            if len(quantiles) != len(levels):
                raise ValueError("forecast result quantiles must be levels by horizon")
            for row in quantiles:
                self._horizon(row, horizon, "quantile")
            if any(any(left > right for left, right in zip(column, column[1:]))
                   for column in zip(*quantiles, strict=True)):
                raise ValueError("forecast result quantiles cross")
            if any(abs(left - right) > 1e-10 for left, right in zip(
                result["median"], quantiles[levels.index(0.5)], strict=True
            )):
                raise ValueError("forecast result median does not equal q0.5")
        return result

    @staticmethod
    def _finite(values: Sequence[float]) -> bool:
        return all(math.isfinite(value) for value in values)

    @classmethod
    def _horizon(cls, values: Sequence[float], horizon: int, label: str) -> None:
        """Require a provider vector with one finite value per forecast step."""
        if len(values) != horizon or not cls._finite(values):
            raise ValueError(f"forecast result {label} must be one finite horizon vector")

    @staticmethod
    def _validate_levels(levels: Sequence[float] | None, capability: str) -> None:
        """Preserve point-only nulls and ordered probabilistic levels including q0.5."""
        if capability == "mean_only":
            if levels is not None:
                raise ValueError("mean-only quantile levels must be null")
            return
        if (
            not levels or 0.5 not in levels
            or not ForecastContract._finite(levels)
            or any(level <= 0 or level >= 1 for level in levels)
            or any(left >= right for left, right in zip(levels, levels[1:]))
        ):
            raise ValueError("probabilistic quantile levels must be ordered and include q0.5")
