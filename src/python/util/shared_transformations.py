"""Process 03/04 leakage-safe affine transformations (docs/data-contract.md).

Finite historical values produce sample-SD fitted state and transformed values;
Process 04 applies or inverts that state without refitting future observations.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import fsum, isfinite, sqrt
from typing import Any, Iterable, Mapping, Sequence


REGISTERED_TRANSFORMATION_STEPS = ("identity", "standardise_sample_v1")
STANDARDISATION_RECIPE = "standardise_sample_v1"
STANDARDISATION_VERSION = 1
STANDARDISATION_STATE_KEYS = frozenset(
    {"recipe", "version", "centre", "scale", "count", "constant"}
)


@dataclass(frozen=True)
class TransformationResult:
    """Carry transformed values and portable fitted state for later inversion.

    Values are an immutable tuple; parameters are a dictionary (empty for
    identity), not an immutable state object.
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
    The leakage-safe fit and portable state are defined in docs/data-contract.md.
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
    """Fit context-only identity or sample-SD state and return values and state."""
    source = _finite_values(values, "transformation")
    if method == "identity":
        return TransformationResult(source, {})
    if method == STANDARDISATION_RECIPE:
        state = fit_transformation(source, method)
        return TransformationResult(apply_transformation(source, state), state)
    raise ValueError(f"unsupported transformation: {method}")


def inverse(values: Iterable[float], method: str, parameters: dict) -> tuple[float, ...]:
    """Restore transformed forecasts to the observations' original units.

    Inputs: Forecast ``values``, the fitted method name, and parameters returned by
    :func:`transform` for the corresponding context.
    Outputs: An immutable sequence on the original data scale.
    Fitted sample-SD state is portable across R/Python, including constant contexts.
    """
    source = _finite_values(values, "inverse transformation")
    if method == "identity":
        return source
    if method == STANDARDISATION_RECIPE:
        return inverse_transformation(source, parameters)
    raise ValueError(f"unsupported transformation: {method}")
