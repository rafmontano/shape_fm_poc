"""Command-line interface for the canonical GIFT-Eval Stage 1 database."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Sequence

from .config import ImportValidationError, load_config
from .database import DEFAULT_DATABASE, ShapeFMDatabase, migrate_database
from .orchestration import ImportCoordinator, repository_root


def defaults() -> tuple[Path, Path, Path, str]:
    root = repository_root()
    dependency_path = root / "config/dependencies/gift_eval.json"
    with dependency_path.open("r", encoding="utf-8") as stream:
        dependency = json.load(stream)
    return (
        root / DEFAULT_DATABASE,
        root / dependency["dataset"]["default_local_source_directory"] / "m4_daily",
        root / "config/imports/m4_daily.json",
        dependency["dataset"]["revision"],
    )


def build_parser() -> argparse.ArgumentParser:
    database, source_dir, config, _ = defaults()
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    migrate = subparsers.add_parser("migrate", help="create or migrate the database")
    migrate.add_argument("--database", type=Path, default=database)

    import_parser = subparsers.add_parser("import", help="import M4 Daily")
    import_parser.add_argument("--database", type=Path, default=database)
    import_parser.add_argument("--source-dir", type=Path, default=source_dir)
    import_parser.add_argument("--config", type=Path, default=config)
    import_parser.add_argument("--max-series", type=int)
    import_parser.add_argument("--workers", type=int, default=1)

    status = subparsers.add_parser("status", help="inspect Stage 1 status")
    status.add_argument("--database", type=Path, default=database)
    status.add_argument("--dataset", default="m4_daily")

    get = subparsers.add_parser("get", help="retrieve one canonical series")
    get.add_argument("--database", type=Path, default=database)
    get.add_argument("--dataset", default="m4_daily")
    get.add_argument("--series-id", required=True)
    get.add_argument("--include-target", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "migrate":
            print(json.dumps({"database": str(migrate_database(args.database)), "schema": 1}, indent=2))
            return 0
        if args.command == "import":
            _, _, _, revision = defaults()
            config = load_config(args.config, args.max_series)
            with ImportCoordinator(args.database) as coordinator:
                result = coordinator.import_m4_daily(
                    args.source_dir, config, revision, workers=args.workers
                )
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0
        with ShapeFMDatabase.open(args.database) as database:
            if args.command == "status":
                value = asdict(database.stage_status("import", args.dataset))
            else:
                series = database.get_series(args.dataset, args.series_id)
                value = asdict(series)
                if not args.include_target:
                    value["target_preview"] = [*series.target[:3], *series.target[-3:]]
                    del value["target"]
            print(json.dumps(value, indent=2, sort_keys=True, default=str))
            return 0
    except (ImportValidationError, RuntimeError, OSError, ValueError, KeyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
