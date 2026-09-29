# ==============================================================================
# 03_transform.py
#
# Purpose: Expose Process 03 transformation orchestration.
# Inputs: Path to an experiment DuckDB containing a planned experiment and completed Process 02 tasks.
# Outputs: Existing Process 03 summary plus transformed series and restart state written by ExperimentCoordinator using shared transformation and distributed-execution utilities.
# Called from: 00_main.py through its numbered-wrapper loader.
# Run from: Imported by 00_main.py; not run directly.
# ==============================================================================

"""Process 03 wrapper over shared transformation orchestration."""

from pathlib import Path
from typing import Any

from util.experiment_execution import ExperimentCoordinator, latest_experiment_id


# Code constant: process identity validated by the numbered-wrapper loader.
PROCESS_NUMBER = 3


def run(database: Path) -> dict[str, Any]:
    """Purpose: Resolve the current experiment and run Process 03 transformations.

    Inputs: Path to an authoritative experiment DuckDB after Process 02.
    Outputs: Existing Process 03 invocation/task summary; transformation science,
    distributed execution, retries, and writes remain in shared utilities.
    """
    experiment_id = latest_experiment_id(database)
    with ExperimentCoordinator(database) as coordinator:
        return coordinator.run_process(experiment_id, PROCESS_NUMBER)
