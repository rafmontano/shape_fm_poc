# ==============================================================================
# p06_04_result_export.py
# Purpose: Export validated stored Process 06 results as derived human-facing files.
# Inputs: Read-only DuckDB snapshot and explicit/default output directory.
# Outputs: CSVs, PDF/PNG figures and hashed manifest; never research database writes.
# Run from: Imported by the export researcher action; not run directly.
# ==============================================================================
"""Presentation only: DuckDB remains authoritative; R renders explicit stored inputs."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import platform
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .shared_configuration import canonical_json
from .p06_02_table_storage import TableStorage
from .p00_02_researcher_cli import ResearcherCLI

# Code constants: declared derived files, renderer boundary and historical exclusions.
ROOT = Path(__file__).resolve().parents[3]
CSV_NAMES = ("table_1", "table_2_daily", "directional_accuracy_by_horizon",
             "cd_input_daily", "cd_mean_ranks_daily")
FIGURE_NAMES = ("figure_2_accuracy_by_horizon", "figure_2_cd_daily", "figure_2_cd_daily_paper")
DERIVED_FILES = ("manifest.json", *(f"tables/{n}.csv" for n in CSV_NAMES),
                 *(f"figures/{n}.{ext}" for n in FIGURE_NAMES for ext in ("pdf", "png")),
                 "evidence/execution-summary.json")
EXCLUDED_MODELS = ("XGBoost", "1-NN Euclidean", "Rotation Forest", "ROCKET", "InceptionTime")
CD_INTERPRETATION = ("Applicable-model benchmark reproduction, not the original ten-model diagram. "
                     "Daily horizons share the same series; CD bars/grouping are descriptive, "
                     "not independent-sample statistical evidence.")


class ResultExport:
    """Own read-only snapshot export and publication of declared presentation files only."""

    def __init__(self, database: Path, output: Path | None = None):
        """Bind paths without opening a database, creating files or running workers."""
        self.database = ResearcherCLI.result_path(database, database=True)
        self.output = ResearcherCLI.result_path(output if output is not None else self.database.with_suffix(""))

    @staticmethod
    def _csv(path: Path, columns: list[str], rows: list[dict]) -> None:
        """Write deterministic UTF-8 rows without rounding stored scientific values."""
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)

    def _tables(self, directory: Path, snapshot: dict) -> None:
        """Flatten stored report cells and counts; consume stored ranks without recalculation."""
        report = snapshot["report"]
        table1 = [{**{k: r[k] for k in ("frequency", "model", "candidate_id", "lambda_up", "lambda_down",
                                       "evaluation_count", "input_fingerprint", "result_fingerprint")},
                   **r["metrics"], **{f"{k}_improvement_vs_smyl_pct": v
                                     for k, v in r["improvement_vs_smyl_pct"].items()}}
                  for r in report["Table1"]]
        self._csv(directory / "table_1.csv", list(table1[0]), table1)
        table2 = [{"panel": panel, **r} for panel, rows in report["Table2"].items() for r in rows]
        self._csv(directory / "table_2_daily.csv", ["panel", "frequency", "model", "value"], table2)
        rows = snapshot["directional_rows"]
        self._csv(directory / "directional_accuracy_by_horizon.csv", list(rows[0]), rows)
        profile = report["figure_2"]["cd"]
        matrix = self._matrix(profile)
        wide = [{"dataset": block, **dict(zip(profile["models"], values, strict=True))}
                for block, values in zip(matrix["blocks"], matrix["values"], strict=True)]
        self._csv(directory / "cd_input_daily.csv", ["dataset", *profile["models"]], wide)
        ranks = [{"model": m, "mean_rank": profile["mean_ranks"][m]} for m in profile["models"]]
        self._csv(directory / "cd_mean_ranks_daily.csv", ["model", "mean_rank"], ranks)

    @staticmethod
    def _matrix(profile: dict) -> dict:
        """Reshape the stored ordered matrix, rejecting incomplete/duplicate configured cells."""
        if profile["incomplete_models"]:
            raise RuntimeError("incomplete configured figure models")
        models = profile["models"]
        cells, blocks = {}, []
        for r in profile["matrix"]:
            block = (r["frequency"], r["horizon"])
            key = (*block, r["model"])
            if key in cells:
                raise RuntimeError("duplicate configured figure cell")
            cells[key] = r["accuracy"]
            if block not in blocks:
                blocks.append(block)
        blocks.sort()
        if not blocks or len(cells) != len(blocks) * len(models):
            raise RuntimeError("incomplete configured figure matrix")
        return {"models": models, "blocks": [f"{f}_{h}" for f, h in blocks],
                "frequencies": [f for f, h in blocks], "horizons": [h for f, h in blocks],
                "values": [[cells[(f, h, m)] for m in models] for f, h in blocks]}

    def _render(self, directory: Path, snapshot: dict) -> dict:
        """Invoke only the stateless renderer with explicit stored matrices and locked packages."""
        lock = json.loads((ROOT / "renv.lock").read_text())["Packages"]
        packages = {name: {k: lock[name][k] for k in ("Version", "RemoteSha") if k in lock[name]}
                    for name in ("scmamp", "jsonlite")}
        figure = snapshot["report"]["figure_2"]
        request = {"output": str(directory), "horizon": self._matrix(figure["horizon"]),
                   "cd": self._matrix(figure["cd"]),
                   "settings": snapshot["figure_settings"]["cd_settings"], "packages": packages,
                   "interpretation": CD_INTERPRETATION}
        result = subprocess.run(["Rscript", "--vanilla", "src/r/06_03_plot_directional_results.R"],
                                cwd=ROOT, input=canonical_json(request), text=True,
                                capture_output=True, check=True, timeout=120)
        return json.loads(result.stdout)

    def run(self) -> dict:
        """Validate first, render in staging, then replace only declared derived files.

        Inputs: Bound database and output paths. Outputs: Summary and presentation files.
        Incomplete or invalid science fails before any final file is published.
        """
        if not self.database.is_file():
            raise FileNotFoundError(f"database does not exist: {self.database}")
        with duckdb.connect(str(self.database), read_only=True) as connection:
            snapshot = TableStorage(connection).export_snapshot(self.database)
        if self.database.stem != snapshot["name"]:
            raise ValueError("database filename must match the stored experiment name")
        for relative in ("tables", "figures", "evidence", *DERIVED_FILES):
            if (self.output / relative).is_symlink():
                raise ValueError("export refuses symlinked derived destinations")
        self.output.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".shapefm-export-", dir=self.output.parent) as temporary:
            staging = Path(temporary)
            (staging / "tables").mkdir()
            (staging / "figures").mkdir()
            (staging / "evidence").mkdir()
            self._tables(staging / "tables", snapshot)
            runtime = self._render(staging / "figures", snapshot)
            evidence = {**snapshot["execution_summary"], "experiment_id": snapshot["experiment_id"],
                        "exported_result_paths": list(DERIVED_FILES),
                        "matrix_fingerprint": snapshot["report"]["figure_2"]["cd"]["matrix_fingerprint"]}
            (staging / "evidence/execution-summary.json").write_text(canonical_json(evidence) + "\n")
            hashes = {}
            for relative in DERIVED_FILES[1:]:
                path = staging / relative
                if not path.is_file() or path.stat().st_size == 0:
                    raise RuntimeError(f"renderer omitted derived file: {relative}")
                hashes[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
            manifest = {k: snapshot[k] for k in ("experiment_id", "name", "description",
                        "configuration_version", "scientific_fingerprint", "report_fingerprint")}
            manifest.update({"source_database": self.database.name,
                "matrix_fingerprint": snapshot["report"]["directional_profile"]["matrix_fingerprint"],
                "figure_matrix_fingerprints": {"horizon": snapshot["report"]["figure_2"]["horizon"]["matrix_fingerprint"],
                    "cd": snapshot["report"]["figure_2"]["cd"]["matrix_fingerprint"]},
                "figure_2_models": snapshot["figure_settings"], "excluded_historical_models": EXCLUDED_MODELS,
                "cd_interpretation": CD_INTERPRETATION, "generated_paths": list(DERIVED_FILES),
                "generated_files": hashes,
                "exported_at": datetime.now(timezone.utc).isoformat(),
                "runtime": {"python": platform.python_version(), "duckdb": duckdb.__version__, **runtime}})
            (staging / "manifest.json").write_text(canonical_json(manifest) + "\n", encoding="utf-8")
            for relative in ("tables", "figures", "evidence", *DERIVED_FILES):
                if (self.output / relative).is_symlink():
                    raise RuntimeError("export refuses symlinked derived destinations")
            self.output.mkdir(exist_ok=True)
            (self.output / "tables").mkdir(exist_ok=True)
            (self.output / "figures").mkdir(exist_ok=True)
            (self.output / "evidence").mkdir(exist_ok=True)
            for relative in (*DERIVED_FILES[1:], "manifest.json"):
                os.replace(staging / relative, self.output / relative)
        return {"experiment_id": snapshot["experiment_id"], "output": str(self.output),
                "files": list(DERIVED_FILES), "report_fingerprint": snapshot["report_fingerprint"],
                "database_access": "read-only", "scientific_execution": False}
