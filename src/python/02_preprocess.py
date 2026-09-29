# ==============================================================================
# 02_preprocess.py
#
# Purpose: Expose Process 02 planning and preprocessing orchestration.
# Inputs: Path to an experiment DuckDB containing a completed Process 01 import and authoritative configuration.
# Outputs: Existing Process 02 summary plus preprocessed series and restart state written by ExperimentCoordinator; invokes the 02_01 R substep through shared execution utilities.
# Called from: 00_main.py through its numbered-wrapper loader.
# Run from: Imported by 00_main.py; not run directly.
# ==============================================================================

"""Process 02 wrapper over experiment planning and shared preprocessing orchestration."""

from pathlib import Path
from typing import Any

from util.experiment_execution import ExperimentCoordinator


# Code constant: process identity validated by the numbered-wrapper loader.
PROCESS_NUMBER = 2


def run(database: Path) -> dict[str, Any]:
    """Purpose: Plan when required and run Process 02 preprocessing.

    Inputs: Path to an authoritative experiment DuckDB after Process 01.
    Outputs: Existing Process 02 invocation/task summary; the coordinator remains
    the sole writer and shared utilities own R, Dask, retry, and provenance details.
    """
    with ExperimentCoordinator(database) as coordinator:
        plan = coordinator.plan()
        return coordinator.run_process(plan.experiment_id, PROCESS_NUMBER)
