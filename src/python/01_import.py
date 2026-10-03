# ==============================================================================
# 01_import.py
#
# Purpose: Expose the Process 01 import boundary to the researcher entry point.
# Inputs: Path to an initialized experiment DuckDB whose stored configuration identifies the pinned source data and import execution controls.
# Outputs: Existing import summary plus canonical datasets, series, windows, and restart state written by the single ImportCoordinator writer.
# Called from: 00_main.py through its numbered-wrapper loader.
# Run from: Imported by 00_main.py; not run directly.
# ==============================================================================

"""Process 01 import wrapper over the shared single-writer implementation."""

from pathlib import Path
from typing import Any

from util.p01_01_import_flow import gate1_import_flow


# Code constant: process identity validated by the numbered-wrapper loader.
PROCESS_NUMBER = 1


def run(database: Path) -> dict[str, Any]:
    """Purpose: Run Process 01 from authoritative stored configuration.

    Inputs: Path to an initialized experiment DuckDB.
    Outputs: Existing import summary; opens one ``ImportCoordinator`` that owns all
    Process 01 DuckDB writes and delegates source handling to shared utilities.
    """
    return gate1_import_flow(database)
