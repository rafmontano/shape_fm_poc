# ==============================================================================
# shared_transformations.py
#
# Purpose: Fit, apply, compose, and invert versioned leakage-safe transformations.
# Inputs: Finite numeric values, registered recipe names, and portable fitted state.
# Outputs: Immutable transformed values and state, or values restored to original scale.
# Run from: Imported; not run directly.
# ==============================================================================

"""Versioned leakage-safe transformations shared by Gate 3 and Gate 4."""

from __future__ import annotations

from dataclasses import dataclass
from math import fsum, isfinite, sqrt
from typing import Any, Iterable, Mapping, Sequence


# Code constants: the new ordered interface contains only identity and the
# approved sample-SD recipe. The legacy name remains accepted solely so stored
# configuration-v1 through v3 experiments keep their historical interpretation.
REGISTERED_TRANSFORMATION_STEPS = ("identity", "standardise_sample_v1")
STANDARDISATION_RECIPE = "standardise_sample_v1"
STANDARDISATION_VERSION = 1
STANDARDISATION_STATE_KEYS = frozenset(
    {"recipe", "version", "centre", "scale", "count", "constant"}
)
LEGACY_TRANSFORMATION = "minmax_then_standardize"


@dataclass(frozen=True)
class TransformationResult:
    """Purpose: Carry transformed observations and the state required for inversion.

    Inputs: ``values`` are immutable transformed observations; ``parameters`` holds
    fitted portable state, legacy parameters, or an empty mapping for identity.
    Outputs: Worker-owned, immutable transformation state on the transformed scale.
    """
    values: tuple[float, ...]
    parameters: dict[str, Any]


def _finite_values(values: Iterable[float], purpose: str) -> tuple[float, ...]:
    """Return one nonempty finite double-precision sequence or fail clearly."""
    try:
        source = tuple(float(value) for value in values)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{purpose} values must be numeric") from exc
    if not source:
        raise ValueError(f"{purpose} values cannot be empty")
    if any(not isfinite(value) for value in source):
        raise ValueError(f"{purpose} values must be finite")
    return source


def _validated_state(state: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and normalize the exact portable standardisation-state schema."""
    if not isinstance(state, Mapping) or set(state) != STANDARDISATION_STATE_KEYS:
        raise ValueError(
            "standardisation state must contain exactly recipe, version, centre, "
            "scale, count, and constant"
        )
    if state["recipe"] != STANDARDISATION_RECIPE:
        raise ValueError(f"unsupported transformation state recipe: {state['recipe']!r}")
    if (
        isinstance(state["version"], bool)
        or not isinstance(state["version"], int)
        or state["version"] != STANDARDISATION_VERSION
    ):
        raise ValueError(f"unsupported {STANDARDISATION_RECIPE} state version")
    count = state["count"]
    if isinstance(count, bool) or not isinstance(count, int) or count < 1:
        raise ValueError("standardisation state count must be a positive integer")
    if not isinstance(state["constant"], bool):
        raise ValueError("standardisation state constant must be boolean")
    try:
        centre = float(state["centre"])
        scale = float(state["scale"])
    except (TypeError, ValueError) as exc:
        raise ValueError("standardisation state centre and scale must be numeric") from exc
    if not isfinite(centre) or not isfinite(scale) or scale <= 0.0:
        raise ValueError("standardisation state centre must be finite and scale positive")
    if state["constant"] and scale != 1.0:
        raise ValueError("constant standardisation state must use effective scale 1")
    if not state["constant"] and count < 2:
        raise ValueError("nonconstant standardisation state requires at least two observations")
    return {
        "recipe": STANDARDISATION_RECIPE,
        "version": STANDARDISATION_VERSION,
        "centre": centre,
        "scale": scale,
        "count": count,
        "constant": state["constant"],
    }


def fit_transformation(values: Iterable[float], recipe: str) -> dict[str, Any]:
    """Fit one registered step using only the supplied historical observations.

    Exactly constant histories, including one observation, use centre=x[0] and
    effective scale 1. Nonconstant histories use sample SD with denominator n-1.
    """
    source = _finite_values(values, "transformation fit")
    if recipe == "identity":
        return {}
    if recipe != STANDARDISATION_RECIPE:
        raise ValueError(f"unsupported transformation recipe: {recipe}")
    constant = all(value == source[0] for value in source[1:])
    if constant:
        centre = source[0]
        scale = 1.0
    else:
        centre = fsum(source) / len(source)
        variance = fsum((value - centre) ** 2 for value in source) / (len(source) - 1)
        scale = sqrt(variance)
        if not isfinite(scale) or scale <= 0.0:
            raise ValueError("nonconstant standardisation produced an invalid sample scale")
    return {
        "recipe": STANDARDISATION_RECIPE,
        "version": STANDARDISATION_VERSION,
        "centre": centre,
        "scale": scale,
        "count": len(source),
        "constant": constant,
    }


def apply_transformation(
    values: Iterable[float], state: Mapping[str, Any]
) -> tuple[float, ...]:
    """Apply one already-fitted state without inspecting or refitting other values."""
    source = _finite_values(values, "transformation apply")
    if not state:
        return source
    fitted = _validated_state(state)
    output = tuple(
        (value - fitted["centre"]) / fitted["scale"] for value in source
    )
    if any(not isfinite(value) for value in output):
        raise ValueError("standardisation produced a non-finite result")
    return output


def inverse_transformation(
    values: Iterable[float], state: Mapping[str, Any]
) -> tuple[float, ...]:
    """Invert one already-fitted positive affine state without refitting it."""
    source = _finite_values(values, "transformation inverse")
    if not state:
        return source
    fitted = _validated_state(state)
    output = tuple(
        fitted["centre"] + fitted["scale"] * value for value in source
    )
    if any(not isfinite(value) for value in output):
        raise ValueError("inverse standardisation produced a non-finite result")
    return output


def fit_apply_steps(
    values: Iterable[float], recipes: Sequence[str]
) -> tuple[tuple[float, ...], tuple[dict[str, Any], ...]]:
    """Fit and apply registered steps in declared forward order."""
    current = _finite_values(values, "transformation pipeline")
    states: list[dict[str, Any]] = []
    for recipe in recipes:
        if recipe not in REGISTERED_TRANSFORMATION_STEPS:
            raise ValueError(f"unsupported ordered transformation step: {recipe}")
        state = fit_transformation(current, recipe)
        current = apply_transformation(current, state)
        states.append(state)
    return current, tuple(states)


def apply_steps(
    values: Iterable[float], states: Sequence[Mapping[str, Any]]
) -> tuple[float, ...]:
    """Apply fitted states in forward order without fitting any new parameters."""
    current = _finite_values(values, "transformation pipeline apply")
    for state in states:
        current = apply_transformation(current, state)
    return current


def inverse_steps(
    values: Iterable[float], states: Sequence[Mapping[str, Any]]
) -> tuple[float, ...]:
    """Invert fitted states in reverse order without fabricating output fields."""
    current = _finite_values(values, "transformation pipeline inverse")
    for state in reversed(states):
        current = inverse_transformation(current, state)
    return current


def transform(values: Iterable[float], method: str) -> TransformationResult:
    """Purpose: Fit and apply a supported transformation to context observations.

    Inputs: Finite numeric context ``values`` and a current or historical method.
    Outputs: Transformed values plus parameters sufficient to restore original units.
    Notes: The approved recipe uses sample SD and portable state. The historical
    min-max/population-SD branch remains unchanged for v1-v3 database compatibility.
    """
    source = _finite_values(values, "transformation")
    if method == "identity":
        return TransformationResult(source, {})
    if method == STANDARDISATION_RECIPE:
        state = fit_transformation(source, method)
        return TransformationResult(apply_transformation(source, state), state)
    if method != LEGACY_TRANSFORMATION:
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
    """Purpose: Restore transformed forecasts to the observations' original units.

    Inputs: Forecast ``values``, the fitted method name, and parameters returned by
    :func:`transform` for the corresponding context.
    Outputs: An immutable sequence on the original data scale.
    Notes: New recipe state is portable across R/Python. Historical constant
    contexts retain their old collapsed inverse solely for stored compatibility.
    """
    source = _finite_values(values, "inverse transformation")
    if method == "identity":
        return source
    if method == STANDARDISATION_RECIPE:
        return inverse_transformation(source, parameters)
    if method != LEGACY_TRANSFORMATION:
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
