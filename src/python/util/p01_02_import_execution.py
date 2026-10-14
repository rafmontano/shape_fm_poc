"""Pure canonical conversion and the small synchronous execution interface.

Inputs are parsed GIFT-Eval records and trusted import settings. No persistence,
workflow state or machine placement belongs here. An executor implements
execute(function, *arguments) -> result; a future distributed adapter can resolve
its future internally without exposing scheduler details to ImportProcess.
"""

import hashlib
import math
import struct
from dataclasses import dataclass

from .shared_configuration import evaluation_window
from .p01_03_gift_eval_source import SourceSeries


@dataclass(frozen=True)
class CanonicalSeries:
    """One converted input with its unchanged scientific identity and window."""

    source: SourceSeries
    content_hash: str
    window: dict


def convert_series(record, settings):
    """Preserve the accepted float32/null/NaN hash and end-exclusive window."""
    return CanonicalSeries(record, target_content_hash(record.target),
                           evaluation_window(len(record.target), settings))


def target_content_hash(target):
    """Hash raw values with the established distinct Arrow-null/NaN encoding."""
    values = tuple(target)
    if all(value is not None and not math.isnan(value) for value in values):
        packed = struct.pack(f"<{len(values)}f", *values)
    else:
        parts = []
        for value in values:
            if value is None:
                parts.append(b"N")
            elif math.isnan(value):
                parts.append(b"A")
            else:
                parts.append(b"V" + struct.pack("<f", value))
        packed = b"".join(parts)
    return hashlib.sha256(packed).hexdigest()


def execute_local(function, *arguments):
    """Execute one bounded operation locally; no pool or framework is needed."""
    return function(*arguments)
