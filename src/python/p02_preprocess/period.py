"""Resolve preprocessing periods in the isolated, pinned GluonTS environment.

Run with the GIFT-Eval environment's Python and ``--frequency``; stdout is JSON.
"""

from __future__ import annotations

import argparse
import json

from gluonts.time_feature import get_seasonality


def resolve_period(frequency: str, override: int | None = None) -> dict:
    """Resolve the R period from an explicit override or pinned GluonTS.

    Purpose: Keep the reproducible frequency convention in the environment that
    pins GluonTS instead of copying a mapping into the lean coordinator or R.
    Inputs: Stored dataset frequency and an optional positive integer override.
    Outputs: JSON-ready period, source, frequency, and pinned default metadata.
    """
    if not isinstance(frequency, str) or not frequency:
        raise ValueError("stored frequency must be a non-empty string")
    if override is not None and (
        isinstance(override, bool) or not isinstance(override, int) or override < 1
    ):
        raise ValueError("R-period override must be a positive integer")
    pinned_default = int(get_seasonality(frequency))
    return {
        "frequency": frequency,
        "r_period": override if override is not None else pinned_default,
        "r_period_source": (
            "experiment_override" if override is not None else "pinned_gluonts_get_seasonality"
        ),
        "gluonts_default_seasonality": pinned_default,
    }


def main() -> None:
    """Print one period record without opening benchmark data or evaluating models."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--frequency", required=True)
    parser.add_argument("--override", type=int)
    args = parser.parse_args()
    print(json.dumps(resolve_period(args.frequency, args.override)))


if __name__ == "__main__":
    main()
