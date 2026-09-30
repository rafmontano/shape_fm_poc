# ==============================================================================
# 04_forecast.py
#
# Purpose: Expose Process 04 configured forecasting and optional period tuning.
# Inputs: Path to an experiment DuckDB containing completed transformed contexts, model configuration, and execution controls.
# Outputs: Existing Process 04 summary plus original-scale base forecasts and restart state written by ExperimentCoordinator; specialised 04_01 R and 04_02 Chronos branches run through shared utilities.
# Called from: 00_main.py through its numbered-wrapper loader.
# Run from: Imported by 00_main.py; not run directly.
# ==============================================================================

"""Process 04 wrapper over shared configured-model orchestration."""

from pathlib import Path
from typing import Any

from util.experiment_execution import ExperimentCoordinator, latest_experiment_id
from util.execution_profiles import ExecutionProfile, ExecutionSettings


# Code constant: process identity validated by the numbered-wrapper loader.
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
