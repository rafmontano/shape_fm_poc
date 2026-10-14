# ==============================================================================
# shared_active_paths.py
# Purpose: Own the fixed paths of the single active experiment package.
# Inputs: Repository root, or an isolated temporary root for automated tests.
# Outputs: Paths only; no filesystem creation or experiment reset.
# Run from: Imported; not run directly.
# ==============================================================================

"""Operational locations are not scientific experiment identity."""

from dataclasses import dataclass
from pathlib import Path
import tempfile

# Code constant: production repository root; never an experiment setting.
ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class ActivePaths:
    """Fixed parent/child/bootstrap paths; alternate roots are test-only."""

    root: Path = ROOT

    def __post_init__(self):
        """Reject non-repository roots unless inside an isolated temporary directory."""
        root = self.root.resolve()
        temporary = Path(tempfile.gettempdir()).resolve()
        if root != ROOT and (root == temporary or not root.is_relative_to(temporary)):
            raise ValueError("path overrides are test-only and must be inside a temporary directory")
        object.__setattr__(self, "root", root)

    @property
    def configuration(self) -> Path:
        """Return the creation-only researcher configuration."""
        return self.root / "config/experiment.json"

    @property
    def parent(self) -> Path:
        """Return the parent authority created by Process 01."""
        return self.root / "results/experiment.duckdb"

    @property
    def child(self) -> Path:
        """Return the required child created by Process 03, never Process 01."""
        return self.root / "results/windows.duckdb"

    @classmethod
    def from_parent(cls, parent: Path) -> "ActivePaths":
        """Accept only the fixed production filename or the same layout in tests."""
        parent = Path(parent).absolute()
        paths = cls(parent.parent.parent)
        if parent.name != "experiment.duckdb" or parent.parent.name != "results" or parent.resolve() != paths.parent:
            raise ValueError("parent database must use the fixed results/experiment.duckdb path")
        if paths.child.resolve() != paths.child:
            raise ValueError("windows database path must not escape through a symlink")
        return paths
