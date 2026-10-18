"""Process 06 read-only publication of approved stored tables and figures.

The assembled DuckDB report supplies five CSVs and three PNG/PDF figure pairs
under results/tables and results/figures. Rendering never evaluates or fits models.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import duckdb

from util.providers import run_r
from util.runtime import CODE_ROOT, FinalResearchSummary, ProcessContext

CSV_NAMES = ("table_1", "table_2_daily", "directional_accuracy_by_horizon",
             "cd_input_daily", "cd_mean_ranks_daily")
FIGURE_NAMES = ("figure_2_accuracy_by_horizon", "figure_2_cd_daily", "figure_2_cd_daily_paper")
LOGGER = logging.getLogger(__name__)


def deliver_published_outputs(context: ProcessContext, summary: FinalResearchSummary) -> None:
    """Coordinator-only delivery of declared files; failed hosts remain pending.

    Delivery is operational, not scientific acceptance. Each normal publication
    resume retries the stored paths without rendering or reopening accepted work.
    """
    settings = context.execution.settings
    hosts = {worker["machine"] for worker in settings.get("workers", [])
             if worker["machine"] != settings.get("coordinator")}
    if not hosts or not summary.artifacts:
        return
    root = context.request.root
    paths = [str(Path(path).relative_to(root)) for path in summary.artifacts]
    machines = json.loads((CODE_ROOT / "config/machines.json").read_text())["machines"]
    for host in sorted(hosts):
        machine = machines[host]
        target = f"{machine['ssh_user']}@{machine['hostname']}:{machine['project_root']}/"
        try:
            subprocess.run(["rsync", "-rtR", "-e", "ssh -o BatchMode=yes", *paths, target],
                           cwd=root, check=True, timeout=120)
        except (OSError, subprocess.SubprocessError) as error:
            LOGGER.warning("Process 06 output delivery pending for %s: %s", host, error)


def _csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _matrix(profile: dict[str, Any]) -> dict[str, Any]:
    models = profile["models"]
    cells = {(r["frequency"], r["horizon"], r["model"]): r["accuracy"] for r in profile["matrix"]}
    blocks = sorted({(r["frequency"], r["horizon"]) for r in profile["matrix"]})
    return {"models": models, "blocks": [f"{f}_{h}" for f, h in blocks],
            "frequencies": [f for f, h in blocks], "horizons": [h for f, h in blocks],
            "values": [[cells[(f, h, m)] for m in models] for f, h in blocks]}


def publish_stored_outputs(root: Path, report: dict[str, Any] | None = None) -> FinalResearchSummary:
    """Render the accepted report with approved names/styles, without rescoring.

    A standalone export opens the parent read-only. Process 06 supplies its
    already-read assembled record; only the coordinator publishes declared files.
    Return curated metrics and file paths; docs/evaluation.md owns the descriptive
    interpretation of ranks across dependent horizons.
    """
    if report is None:
        with duckdb.connect(str(root / "results/experiment.duckdb"), read_only=True) as connection:
            report = json.loads(connection.execute(
                "SELECT result FROM atomic_results WHERE subflow_id='06.05' AND atomic_key='research_results'").fetchone()[0])
    results = root / "results"
    with tempfile.TemporaryDirectory(prefix=".publish-", dir=results) as temporary:
        staging = Path(temporary)
        tables, figures = staging / "tables", staging / "figures"
        tables.mkdir()
        figures.mkdir()
        table1 = [{**{k: row[k] for k in ("frequency", "model", "candidate_id", "lambda_up", "lambda_down",
                    "evaluation_count")},
                   **row["metrics"], **{f"{k}_improvement_vs_smyl_pct": v
                    for k, v in row["improvement_vs_smyl_pct"].items()}} for row in report["Table1"]]
        _csv(tables / "table_1.csv", list(table1[0]), table1)
        table2 = [{"panel": panel, **row} for panel, rows in report["Table2"].items() for row in rows]
        _csv(tables / "table_2_daily.csv", ["panel", "frequency", "model", "value"], table2)
        rows = [{**row, "accuracy": row["correct_count"] / row["evaluation_count"]}
                for row in report["directional_rows"]]
        _csv(tables / "directional_accuracy_by_horizon.csv", list(rows[0]), rows)
        profile = report["figure_2"]["cd"]
        matrix = _matrix(profile)
        wide = [{"dataset": block, **dict(zip(profile["models"], values, strict=True))}
                for block, values in zip(matrix["blocks"], matrix["values"], strict=True)]
        _csv(tables / "cd_input_daily.csv", ["dataset", *profile["models"]], wide)
        ranks = [{"model": model, "mean_rank": profile["mean_ranks"][model]} for model in profile["models"]]
        _csv(tables / "cd_mean_ranks_daily.csv", ["model", "mean_rank"], ranks)
        lock = json.loads((CODE_ROOT / "renv.lock").read_text())["Packages"]
        packages = {name: {k: lock[name][k] for k in ("Version", "RemoteSha") if k in lock[name]}
                    for name in ("scmamp", "jsonlite")}
        run_r("src/r/p06_evaluate/publication_worker.R", {"output": str(figures),
            "horizon": _matrix(report["figure_2"]["horizon"]), "cd": matrix,
            "settings": report["figure_settings"]["cd_settings"], "packages": packages}, timeout=120, threads=1)
        relative = [*(f"tables/{name}.csv" for name in CSV_NAMES),
                    *(f"figures/{name}.{extension}" for name in FIGURE_NAMES for extension in ("png", "pdf"))]
        for name in relative:
            if not (staging / name).is_file() or (staging / name).stat().st_size == 0:
                raise RuntimeError(f"publisher omitted {name}")
        (results / "tables").mkdir(exist_ok=True)
        (results / "figures").mkdir(exist_ok=True)
        for name in relative:
            os.replace(staging / name, results / name)
    metrics = {f"{row['model']}.{metric}": value for row in report["Table1"]
               for metric, value in row["metrics"].items() if value is not None}
    return FinalResearchSummary(metrics, tuple(str(results / name) for name in relative))
