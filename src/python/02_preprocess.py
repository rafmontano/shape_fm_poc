# ==============================================================================
# 02_preprocess.py
#
# Purpose: Expose Process 02 planning and preprocessing orchestration.
# Inputs: Path to an experiment DuckDB containing a completed Process 01 import and authoritative configuration.
# Outputs: Existing Process 02 summary plus preprocessed series and restart state written by ExperimentCoordinator; invokes the 02_01 R substep through shared execution utilities.
# Run from: Legacy process API retained until Process 02 migration; not a researcher CLI.
# ==============================================================================

"""Process 02 wrapper over experiment planning and shared preprocessing orchestration."""

from pathlib import Path
from typing import Any

from util.shared_experiment_execution import ExperimentCoordinator
from util.shared_execution_profiles import ExecutionProfile, ExecutionSettings


# Code constant: legacy process identity.
PROCESS_NUMBER = 2


def run(database: Path, execution: tuple[ExecutionProfile, dict[str, Any]] | None = None,
        execution_settings: ExecutionSettings | None = None) -> dict[str, Any]:
    """Purpose: Plan when required and run Process 02 preprocessing.

    Inputs: Path to an authoritative experiment DuckDB after Process 01.
    Outputs: Existing Process 02 invocation/task summary; the coordinator remains
    the sole writer and shared utilities own R, Dask, retry, and provenance details.
    """
    with ExperimentCoordinator(database) as coordinator:
        plan = coordinator.plan()
        return coordinator.run_process(plan.experiment_id, PROCESS_NUMBER,
                                       execution=execution, execution_settings=execution_settings)
