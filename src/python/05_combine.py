# ==============================================================================
# 05_combine.py
#
# Purpose: Expose Process 05 forecast-combination orchestration.
# Inputs: Path to an experiment DuckDB containing complete Process 04 base forecasts and authoritative combination settings.
# Outputs: Existing Process 05 summary plus candidate forecasts, component lineage, and restart state written by ExperimentCoordinator using shared combination utilities.
# Run from: Legacy process API retained until Process 05 migration; not a researcher CLI.
# ==============================================================================

"""Process 05 wrapper over shared forecast-combination orchestration."""

from pathlib import Path
from typing import Any

from util.shared_experiment_execution import ExperimentCoordinator, latest_experiment_id
from util.shared_execution_profiles import ExecutionProfile, ExecutionSettings


# Code constant: legacy process identity.
PROCESS_NUMBER = 5


def run(database: Path, execution: tuple[ExecutionProfile, dict[str, Any]] | None = None,
        execution_settings: ExecutionSettings | None = None) -> dict[str, Any]:
    """Purpose: Resolve the current experiment and run Process 05 combination.

    Inputs: Path to an authoritative experiment DuckDB after Process 04.
    Outputs: Existing Process 05 invocation/task summary; shared utilities retain
    combination calculations, distributed execution, retries, lineage, and writes.
    """
    experiment_id = latest_experiment_id(database)
    with ExperimentCoordinator(database) as coordinator:
        return coordinator.run_process(experiment_id, PROCESS_NUMBER,
                                       execution=execution, execution_settings=execution_settings)
