# ==============================================================================
# provenance.py
#
# Purpose: Small shared helpers for deterministic provenance and atomic metadata.
# Inputs: Filesystem paths and JSON-serializable metadata values.
# Outputs: UTC timestamps, file SHA-256 digests, and atomically replaced JSON files.
# Run from: Imported; not run directly.
# ==============================================================================

"""Small shared helpers for deterministic provenance and atomic metadata."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    """Return the current UTC time as a seconds-precision ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_file(path: Path) -> str:
    """Stream ``path`` in 1 MiB blocks and return its hexadecimal SHA-256 digest."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_write_json(path: Path, value: Any) -> None:
    """Write sorted, indented JSON durably to a sibling temporary file, then atomically replace ``path``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{uuid.uuid4().hex}")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
