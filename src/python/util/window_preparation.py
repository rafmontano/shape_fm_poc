# ==============================================================================
# window_preparation.py
#
# Purpose: Prepare explicitly configured trailing contexts without horizon rules.
# Inputs: Finite observations and a positive configured context length.
# Outputs: Fixed-length immutable context preserving the legacy padding rule.
# Run from: Imported; not run directly.
# ==============================================================================

"""Explicit context-window preparation capability for future selected consumers."""

from __future__ import annotations

from math import isfinite
from typing import Iterable


def prepare_context(
    values: Iterable[float], context_length: int
) -> tuple[float, ...]:
    """Return the configured trailing context, left-padding short histories.

    The caller supplies a validated scientific context length explicitly. This
    function does not infer it from sampling frequency or forecast horizon.
    """
    if isinstance(context_length, bool) or not isinstance(context_length, int) or context_length < 1:
        raise ValueError("context_length must be a positive integer")
    try:
        source = tuple(float(value) for value in values)
    except (TypeError, ValueError) as exc:
        raise ValueError("context values must be numeric") from exc
    if not source or any(not isfinite(value) for value in source):
        raise ValueError("context values must be nonempty and finite")
    if len(source) >= context_length:
        return source[-context_length:]
    return (source[0],) * (context_length - len(source)) + source
