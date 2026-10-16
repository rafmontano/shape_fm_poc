"""Native provider transport shared by Processes 02, 03, 04 and 06.

Inputs are JSON scientific requests and stored execution limits. Outputs are
native provider responses; exceptions propagate without alternate science.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from typing import Any

from util.runtime import CODE_ROOT


PREPROCESS_WORKER = "src/r/util/preprocess_worker.R"


def run_r(script: str, payload: dict[str, Any], *, timeout: float, threads: int) -> dict[str, Any]:
    """Call a native R provider with explicit scientific input and bounded execution.

    The worker receives no storage/tracking object. Thread limits are operational;
    the caller validates the returned scientific contract before acceptance.
    """
    try:
        encoded = json.dumps(payload)
    except ValueError as error:
        if "integer string conversion" in str(error):
            # Inspect only failed requests; never decimal-format the suspect integer.
            pending = [("$", payload)]
            seen = set()
            while pending:
                path, value = pending.pop()
                if isinstance(value, int) and value.bit_length() > sys.get_int_max_str_digits() * 3:
                    error.add_note(f"JSON field {path}: type={type(value).__name__}, size_bits={value.bit_length()}")
                elif isinstance(value, (dict, list)) and id(value) not in seen:
                    seen.add(id(value))
                    if isinstance(value, dict):
                        pending.extend((f"{path}[{key!r}]", child) for key, child in value.items())
                    else:
                        pending.extend((f"{path}[{index}]", child) for index, child in enumerate(value))
        raise
    completed = subprocess.run(
        ["Rscript", str(CODE_ROOT / script)], input=encoded,
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
