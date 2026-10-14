"""Process 04 reads persisted training membership for its unmigrated consumers.

Inputs are a read-only child connection, preparation identity and cohort settings;
output is stored membership metadata. No selection, fitting or writes occur here.
Active Process 03 science belongs to :mod:`p03_transform.inputs`.
"""

from __future__ import annotations

import json
from typing import Any


def load_training_cohort(
    child_connection: Any, preparation_id: str, settings: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Read the required stored cohort; an unset cap means uncapped inputs.

    Missing membership is an error, never an invitation to recreate predecessors.
    This accessor reads the schema still consumed by the unmigrated Process 04.
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
