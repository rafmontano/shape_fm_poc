# ==============================================================================
# 06_evaluate.py
#
# Purpose: Expose Process 06 official GIFT-Eval orchestration and experiment completion.
# Inputs: Path to an experiment DuckDB containing complete candidate forecast matrices and authoritative evaluation settings.
# Outputs: Existing Process 06 summary plus official metrics, evaluation provenance, task state, and completed experiment state written by ExperimentCoordinator through the 06_01 isolated bridge.
# Called from: 00_main.py through its numbered-wrapper loader.
# Run from: Imported by 00_main.py; not run directly.
# ==============================================================================

"""Process 06 wrapper over shared official-evaluation orchestration."""

from pathlib import Path
from typing import Any

from util.shared_experiment_execution import ExperimentCoordinator, latest_experiment_id
from util.shared_execution_profiles import ExecutionProfile, ExecutionSettings


# Code constant: process identity validated by the numbered-wrapper loader.
PROCESS_NUMBER = 6


def run(database: Path, execution: tuple[ExecutionProfile, dict[str, Any]] | None = None,
        execution_settings: ExecutionSettings | None = None) -> dict[str, Any]:
    """Purpose: Resolve and evaluate the current experiment, then complete it.

    Inputs: Path to an authoritative experiment DuckDB after Process 05.
    Outputs: Existing Process 06 invocation/task summary; shared utilities own the
    isolated GIFT-Eval call, validation, transaction writes, and completion update.
    """
    experiment_id = latest_experiment_id(database)
    with ExperimentCoordinator(database) as coordinator:
        summary = coordinator.run_process(experiment_id, PROCESS_NUMBER,
                                          execution=execution, execution_settings=execution_settings)
        coordinator.complete_experiment(experiment_id)
        return summary
