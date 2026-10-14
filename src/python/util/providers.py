"""Native provider transport shared by Processes 02, 03, 04 and 06.

Inputs are JSON scientific requests and stored execution limits. Outputs are
native provider responses; exceptions propagate without alternate science.
Pinned GIFT-Eval supplies the R period, not statistical seasonality inference.
"""

from __future__ import annotations

import json
import os
import subprocess
from typing import Any

from util.runtime import CODE_ROOT


def run_r(script: str, payload: dict[str, Any], *, timeout: float, threads: int) -> dict[str, Any]:
    """Call a native R provider with explicit scientific input and bounded execution.

    The worker receives no storage/tracking object. Thread limits are operational;
    the caller validates the returned scientific contract before acceptance.
    """
    completed = subprocess.run(
        ["Rscript", str(CODE_ROOT / script)], input=json.dumps(payload),
        cwd=CODE_ROOT, capture_output=True, text=True, timeout=timeout,
        env={**os.environ, "OMP_NUM_THREADS": str(threads), "OPENBLAS_NUM_THREADS": str(threads),
             "RENV_CONFIG_SYNCHRONIZED_CHECK": "false"},
    )
    try:
        completed.check_returncode()
    except subprocess.CalledProcessError as error:
        error.add_note(completed.stderr)
        raise
    return json.loads(completed.stdout)


def resolve_r_period(configuration: dict[str, Any], frequency: str) -> dict[str, Any]:
    """Resolve the modelling period once from pinned GluonTS or the stored override.

    Per docs/data-contract.md, this convention is distinct from inferred
    seasonality and scoring seasonality. No copied frequency mapping is used.
    """
    environment = configuration["evaluation"]["gift_eval"]["environment"]
    command = [str(CODE_ROOT / environment / "bin/python"),
               str(CODE_ROOT / "src/python/06_01_evaluate_gift_eval.py"),
               "resolve-period", "--frequency", frequency]
    override = configuration["pipeline"]["r_period_override"]
    if override is not None:
        command.extend(["--override", str(override)])
    completed = subprocess.run(command, cwd=CODE_ROOT, check=True, capture_output=True,
                               text=True, timeout=60)
    return json.loads(completed.stdout)
