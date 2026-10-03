#!/usr/bin/env python3
"""Single researcher entry point for ShapeFM."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/python"))

from util.p00_01_researcher_actions import ResearcherActions
from util.p00_02_researcher_cli import InvocationProvenance, ResearcherCLI


def main(argv: Sequence[str] | None = None) -> int:
    """Obtain one request, dispatch its action, and present its result or error."""
    cli = ResearcherCLI()
    request = cli.request(argv)
    if request.action is None:
        cli.help()
        return 0
    try:
        invocation = InvocationProvenance().record(request)
        cli.present(ResearcherActions().dispatch(request, invocation))
        return 0
    except (OSError, RuntimeError, ValueError, KeyError, subprocess.SubprocessError) as exc:
        cli.present_error(exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
