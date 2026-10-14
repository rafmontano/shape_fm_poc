"""Read stored cohort metadata for the legacy shared completion checker.

Inputs are a read-only child connection, preparation identity and cohort settings;
output is stored membership metadata. No selection, fitting or writes occur here.
Active Process 04 reads accepted Process 03 atomic inputs in its application;
this accessor has no role in fitting or selecting its scientific cohort.
"""

from __future__ import annotations

import json
from typing import Any


def load_training_cohort(
    child_connection: Any, preparation_id: str, settings: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Read the required stored cohort; an unset cap means uncapped inputs.

    Missing membership is an error, never an invitation to recreate predecessors.
    This accessor serves the legacy shared storage completion boundary only.
    """
    if settings is None:
        return None
    row = child_connection.execute(
        "SELECT metadata FROM training_cohorts WHERE preparation_id=?",
        [preparation_id],
    ).fetchone()
    if row is None:
        raise RuntimeError("training cohort was not created during preparation")
    return json.loads(row[0])
