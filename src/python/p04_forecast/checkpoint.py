"""Process 04: resolve configured repository/revision to a local Chronos snapshot.

The native provider owns asset loading and validation; no downloads or network I/O.
"""

from __future__ import annotations

import os
from pathlib import Path


def local_chronos_snapshot(repository: str, revision: str) -> Path:
    """Resolve the configured offline snapshot; the native loader validates assets."""
    home = Path(os.environ.get("HF_HOME", Path.home() / ".cache/huggingface"))
    cache = Path(os.environ.get("HF_HUB_CACHE", home / "hub"))
    snapshot = cache / ("models--" + repository.replace("/", "--")) / "snapshots" / revision
    if not snapshot.is_dir():
        raise FileNotFoundError(f"Local pinned Chronos snapshot is missing: {snapshot}")
    return snapshot
