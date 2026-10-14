"""Native provider transport shared by Processes 02, 03, 04 and 06.

Inputs are JSON scientific requests and stored execution limits. Outputs are
native provider responses; exceptions propagate without alternate science.
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
