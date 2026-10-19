"""Process 03 numerical inputs: deterministic S1 and complete rolling boundaries.

Inputs are canonical identities and prepared training arrays. Outputs are membership
and complete input/future positions under docs/data-contract.md. tsai owns
the accepted splitting/window algorithm; no scheduler or database is imported.
"""

from __future__ import annotations

from math import floor
from typing import Any

import numpy as np


def s1_partition(identities: list[tuple[str, str]], training_fraction: float,
                 seed: int) -> dict[tuple[str, str], str]:
    """Split sorted series using the accepted R-double-floor count and tsai seed.

    The held-out count is floor((1−fraction)×N) in binary64, at least one.
    tsai's two-way valid output is the S1 test population, not a third split.
    """
    from tsai.data.validation import TrainValidTestSplitter
    ordered = sorted(identities)
    if len(ordered) < 2:
        raise ValueError("S1 requires at least two eligible series per frequency")
    test_count = max(1, floor((1.0 - float(training_fraction)) * len(ordered)))
    if test_count >= len(ordered):
        raise ValueError("S1 must retain at least one training series")
    train, test = TrainValidTestSplitter(n_splits=1, valid_size=test_count, test_size=0,
        train_only=False, stratify=False, balance=False, shuffle=True,
        random_state=seed)(np.arange(len(ordered)))
    held_out = {int(index) for index in test}
    return {identity: "test" if index in held_out else "train"
            for index, identity in enumerate(ordered)}


def rolling_window_inputs(values: list[float | None], input_length: int,
                          future_horizon: int) -> list[dict[str, Any]]:
    """Generate beginning-aligned complete W+H blocks without padding/leakage.

    Only prepared pre-test observations enter tsai. Prepared futures supply
    labels but never enter the input-only transformation fit.
    """
    from tsai.data.preparation import SlidingWindow
    stride = input_length + future_horizon
    count = floor(len(values) / stride)
    if count == 0:
        return []
    numeric = np.asarray(values, dtype=np.float64)
    inputs, futures = SlidingWindow(window_len=input_length, horizon=future_horizon,
        stride=stride, start=0, pad_remainder=False, padding="post",
        check_leakage=True)(numeric)
    if inputs is None or len(inputs) != count or len(futures) != count:
        raise RuntimeError("tsai returned an unexpected complete-window count")
    return [{"input_start": ordinal * stride, "input_end": ordinal * stride + input_length,
        "future_start": ordinal * stride + input_length, "future_end": (ordinal + 1) * stride,
        "prepared_input": [None if np.isnan(value) else float(value) for value in inputs[ordinal].reshape(-1)],
        "actual": values[ordinal * stride + input_length:(ordinal + 1) * stride]}
        for ordinal in range(count)]
