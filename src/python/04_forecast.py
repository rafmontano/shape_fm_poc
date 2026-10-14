# ==============================================================================
# 04_forecast.py
#
# Purpose: Expose Process 04 configured forecasting and optional period tuning.
# Inputs: Path to an experiment DuckDB containing completed transformed contexts, model configuration, and execution controls.
# Outputs: Existing Process 04 summary plus original-scale base forecasts and restart state written by ExperimentCoordinator; specialised 04_01 R and 04_02 Chronos branches run through shared utilities.
# Run from: Legacy process API retained until Process 04 migration; not a researcher CLI.
# ==============================================================================

"""Process 04 wrapper over shared configured-model orchestration."""

from pathlib import Path
from typing import Any

from util.shared_experiment_execution import ExperimentCoordinator, latest_experiment_id
from util.shared_execution_profiles import ExecutionProfile, ExecutionSettings


# Code constant: legacy process identity.
PROCESS_NUMBER = 4


def run(
    database: Path,
    execution: tuple[ExecutionProfile, dict[str, Any]] | None = None,
    execution_settings: ExecutionSettings | None = None,
) -> dict[str, Any]:
    """Purpose: Resolve the current experiment and run Process 04 forecasting.

    Inputs: Path to an authoritative experiment DuckDB after Process 03.
    Outputs: Existing Process 04 invocation/task summary; the shared coordinator
    owns untuned AutoARIMA/Chronos or tuned AutoARIMA/ETS execution, inverse
    transforms, retries, and single-writer persistence.
    """
    experiment_id = latest_experiment_id(database)
    with ExperimentCoordinator(database) as coordinator:
        return coordinator.run_process(
            experiment_id,
            PROCESS_NUMBER,
            execution=execution,
            execution_settings=execution_settings,
        )
