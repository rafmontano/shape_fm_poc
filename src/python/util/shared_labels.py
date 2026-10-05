# ==============================================================================
# shared_labels.py
#
# Purpose: Calculate versioned binary directional labels in Python.
# Inputs: Numeric future vectors or row-major matrices and explicit references.
# Outputs: Shape-preserving arrays containing 0, 1, or unavailable values.
# Run from: Imported; not run directly.
# ==============================================================================

"""Shared strict directional-label calculation."""

from __future__ import annotations

from typing import Any

import numpy as np


# Code constants: persisted label identity and exact scientific definition.
DIRECTIONAL_LABEL_DEFINITION_ID = "directional_strict_v1"
DIRECTIONAL_LABEL_VERSION = 1
DIRECTIONAL_LABEL_RULE = "1 when value > reference; otherwise 0; missing value unavailable"


def directional_labels(values: Any, references: Any) -> np.ndarray:
    """Compare future values with explicit references without broadcasting.

    A one-dimensional future vector requires one finite reference. A matrix
    requires a one-dimensional reference vector whose length equals its row
    count. Missing future values remain ``nan``; infinite futures and missing or
    non-finite references are rejected. Comparisons are strict double-precision
    comparisons with no near-tie tolerance.
    """
    try:
        future = np.asarray(values, dtype=np.float64)
        reference = np.asarray(references, dtype=np.float64)
    except (TypeError, ValueError) as exc:
        raise ValueError("directional label values and references must be numeric") from exc
    if future.ndim not in {1, 2} or future.size == 0:
        raise ValueError("directional label values must be a nonempty vector or matrix")
    if np.isinf(future).any():
        raise ValueError("directional label values cannot contain infinity")
    if future.ndim == 1:
        if reference.ndim == 0:
            scalar_reference = float(reference)
        elif reference.ndim == 1 and reference.size == 1:
            scalar_reference = float(reference[0])
        else:
            raise ValueError("a future vector requires exactly one reference")
        if not np.isfinite(scalar_reference):
            raise ValueError("directional label references must be finite")
        compared = future > scalar_reference
    else:
        if reference.ndim != 1 or reference.size != future.shape[0]:
            raise ValueError("a future matrix requires exactly one reference per row")
        if not np.isfinite(reference).all():
            raise ValueError("directional label references must be finite")
        compared = future > reference[:, np.newaxis]
    labels = compared.astype(np.float64)
    labels[np.isnan(future)] = np.nan
    return labels


def directional_accuracy(
    predictions: Any, actual_labels: Any
) -> dict[str, float | int]:
    """Return strict binary directional accuracy for one complete horizon."""
    predicted = np.asarray(predictions)
    actual = np.asarray(actual_labels)
    if (
        predicted.ndim != 1
        or actual.ndim != 1
        or predicted.size == 0
        or predicted.shape != actual.shape
        or not np.isin(predicted, (0, 1)).all()
        or not np.isin(actual, (0, 1)).all()
    ):
        raise ValueError("directional evaluation requires equal nonempty binary vectors")
    correct = int(np.equal(predicted, actual).sum())
    count = int(predicted.size)
    return {
        "correct_count": correct,
        "evaluation_count": count,
        "accuracy": correct / count,
    }
