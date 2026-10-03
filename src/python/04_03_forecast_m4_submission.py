#!/usr/bin/env python3
"""JSON bridge for retrieval-only official M4 archived point forecasts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from util.p04_05_m4_submission import retrieve_m4_submission


def main() -> None:
    """Read one request from stdin and emit one 17-digit-compatible JSON result."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    arguments = parser.parse_args()
    request = json.load(sys.stdin)
    json.dump(
        retrieve_m4_submission(arguments.database, request),
        sys.stdout,
        allow_nan=False,
        separators=(",", ":"),
    )


if __name__ == "__main__":
    main()
