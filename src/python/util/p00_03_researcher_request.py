# ==============================================================================
# p00_03_researcher_request.py
# Purpose: Carry typed requests from the researcher CLI to action objects.
# Inputs: One validated public command name and its command-specific values.
# Outputs: Immutable ResearcherRequest values and convenience accessors.
# Run from: Imported by the researcher CLI/action layer; not run directly.
# ==============================================================================

"""Typed requests passed from the researcher CLI to action objects."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ResearcherRequest:
    """Carry one validated command name and its command-specific values."""

    action: str | None
    values: dict[str, Any]

    def value(self, name: str, default: Any = None) -> Any:
        """Return one command value without exposing an argparse namespace."""
        return self.values.get(name, default)

    @property
    def database(self) -> Path:
        """Return the resolved database selected by this request."""
        return Path(self.values["database"]).resolve()
