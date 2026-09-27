# ==============================================================================
# transformations.py
#
# Purpose: Leakage-safe POC 1 transformations and inverse transformations.
# Inputs: Numeric context/forecast values, a supported method name, and fitted parameters for inversion.
# Outputs: Immutable transformed values and parameters, or values restored to the original scale.
# Run from: Imported; not run directly.
# ==============================================================================

"""Leakage-safe POC 1 transformations and inverse transformations."""

from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import Iterable


@dataclass(frozen=True)
class TransformationResult:
    """Transformation output consumed by workers: immutable scaled values and fitted extrema/center/scale with constant-series flags; identity uses an empty parameter mapping."""
    values: tuple[float, ...]
    parameters: dict[str, float | bool]


def transform(values: Iterable[float], method: str) -> TransformationResult:
    """Apply identity or context-fitted min-max then population-standardization and return fitted parameters."""
    source = tuple(float(value) for value in values)
    if not source:
        raise ValueError("cannot transform an empty context")
    if method == "identity":
        return TransformationResult(source, {})
    if method != "minmax_then_standardize":
        raise ValueError(f"unsupported transformation: {method}")
    minimum = min(source)
    maximum = max(source)
    span = maximum - minimum
    constant = span == 0.0
    minmax = tuple(0.0 if constant else (value - minimum) / span for value in source)
    center = sum(minmax) / len(minmax)
    variance = sum((value - center) ** 2 for value in minmax) / len(minmax)
    scale = sqrt(variance)
    zero_scale = scale == 0.0
    output = tuple(0.0 if zero_scale else (value - center) / scale for value in minmax)
    return TransformationResult(
        output,
        {
            "minimum": minimum,
            "maximum": maximum,
            "span": span,
            "minmax_constant": constant,
            "standardization_center": center,
            "standardization_scale": scale,
            "standardization_constant": zero_scale,
        },
    )


def inverse(values: Iterable[float], method: str, parameters: dict) -> tuple[float, ...]:
    """Restore forecast values from identity or fitted standardized/min-max scale to the original units."""
    source = tuple(float(value) for value in values)
    if method == "identity":
        return source
    if method != "minmax_then_standardize":
        raise ValueError(f"unsupported transformation: {method}")
    if parameters["standardization_constant"]:
        minmax = (parameters["standardization_center"],) * len(source)
    else:
        minmax = tuple(
            value * parameters["standardization_scale"]
            + parameters["standardization_center"]
            for value in source
        )
    if parameters["minmax_constant"]:
        return (parameters["minimum"],) * len(source)
    return tuple(
        value * parameters["span"] + parameters["minimum"] for value in minmax
    )
