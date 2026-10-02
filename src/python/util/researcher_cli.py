"""Researcher command parsing, provenance, and JSON/error presentation."""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
from pathlib import Path
from typing import Any, Sequence

from .configuration import PROCESS_NAMES
from .researcher_request import ResearcherRequest

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATABASE = ROOT / "results/poc2_acceptance.duckdb"
DEFAULT_PLAN_DATABASE = ROOT / "results/poc2_local_plan.duckdb"
DEFAULT_REPORT = ROOT / "results/poc2_acceptance_report.json"
DEFAULT_CONFIGURATION = ROOT / "config/experiments/poc2_m4_daily_100_resolved_period.json"


class ResearcherCLI:
    """Own command syntax and researcher-facing presentation contracts."""

    def __init__(self) -> None:
        """Build the parser once for request and help operations."""
        self.parser = self._build_parser()

    @staticmethod
    def positive_integer(value: str) -> int:
        """Parse and require one positive integer option value."""
        try:
            parsed = int(value)
        except ValueError as exc:
            raise argparse.ArgumentTypeError("must be an integer") from exc
        if parsed <= 0:
            raise argparse.ArgumentTypeError("must be positive")
        return parsed

    @staticmethod
    def process_selection(value: str) -> tuple[int, ...]:
        """Parse one process or inclusive range within Processes 01 through 06."""
        try:
            if "-" in value:
                start, end = (int(item) for item in value.split("-", 1))
                selected = tuple(range(start, end + 1))
            else:
                selected = (int(value),)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                "must be one process or an inclusive range, such as 1-3"
            ) from exc
        if not selected or any(item not in PROCESS_NAMES for item in selected):
            raise argparse.ArgumentTypeError("process selection must be within 1-6")
        return selected

    def _build_parser(self) -> argparse.ArgumentParser:
        """Define the preserved researcher command and option contract."""
        parser = argparse.ArgumentParser(description="Single researcher entry point for ShapeFM.")
        commands = parser.add_subparsers(dest="action")
        plan = commands.add_parser("plan", help="dry-plan a deterministic M4 Daily subset")
        plan.add_argument("--configuration", type=Path, default=DEFAULT_CONFIGURATION)
        run = commands.add_parser("run", help="create or resume configured Processes 01-06")
        run.add_argument("--database", type=Path, required=True)
        run.add_argument("--configuration", type=Path)
        run.add_argument("--processes", type=self.process_selection, default=tuple(PROCESS_NAMES))
        run.add_argument("--execution-profile")
        run.add_argument("--local-heavy-exception", metavar="APPROVAL_REFERENCE")
        windows = commands.add_parser("prepare-windows", help="create or resume rolling windows")
        windows.add_argument("--database", type=Path, required=True)
        windows.add_argument("--windows-database", type=Path, required=True)
        windows.add_argument("--execution-profile")
        windows.add_argument("--local-max-series", type=self.positive_integer)
        windows.add_argument("--local-max-windows", type=self.positive_integer)
        status = commands.add_parser("status", help="read acceptance experiment status")
        status.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
        status.add_argument("--experiment-id")
        results = commands.add_parser("results", help="read evaluations or one stored forecast")
        results.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
        results.add_argument("--experiment-id")
        results.add_argument("--variant-id")
        results.add_argument("--series-id")
        results.add_argument("--candidate")
        results.add_argument("--windows-database", type=Path)
        results.add_argument("--dataset-id")
        results.add_argument("--window-ordinal", type=int)
        test = commands.add_parser("test", help="run or restart the 100-series acceptance case")
        test.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
        test.add_argument("--report", type=Path, default=DEFAULT_REPORT)
        return parser

    def request(self, argv: Sequence[str] | None = None) -> ResearcherRequest:
        """Parse arguments into a command-neutral immutable request."""
        namespace = self.parser.parse_args(argv)
        values = vars(namespace).copy()
        action = values.pop("action")
        if action == "plan":
            values["database"] = DEFAULT_PLAN_DATABASE
        return ResearcherRequest(action, values)

    def help(self) -> None:
        """Print top-level command help without constructing action services."""
        self.parser.print_help()

    @staticmethod
    def present(result: dict[str, Any]) -> None:
        """Render one successful result as stable readable JSON."""
        print(json.dumps(result, indent=2, sort_keys=True, default=str))

    @staticmethod
    def present_error(error: BaseException) -> None:
        """Render one expected command failure to stderr."""
        print(f"error: {error}", file=sys.stderr)


class InvocationProvenance:
    """Build read-only invocation evidence for one parsed request."""

    def __init__(self, root: Path = ROOT):
        """Bind provenance collection to the repository root."""
        self.root = root

    def _git(self, *arguments: str) -> str:
        """Return stripped output from one checked read-only Git command."""
        return subprocess.run(
            ["git", *arguments], cwd=self.root, check=True, capture_output=True,
            text=True, timeout=30,
        ).stdout.strip()

    def record(self, request: ResearcherRequest) -> dict[str, Any]:
        """Collect arguments, repository state, environment, and database identity."""
        return {
            "action": request.action,
            "arguments": {
                key: str(value) if isinstance(value, Path) else value
                for key, value in request.values.items()
            },
            "command": [sys.executable, str(self.root / "src/python/00_main.py"), *sys.argv[1:]],
            "repository_root": str(self.root),
            "repository_revision": self._git("rev-parse", "HEAD"),
            "working_tree": self._git("status", "--short", "--untracked-files=all").splitlines(),
            "environment": {
                "python": platform.python_version(), "python_executable": sys.executable,
                "system": platform.system(), "machine": platform.machine(), "node": platform.node(),
            },
            "database": str(request.database),
        }
