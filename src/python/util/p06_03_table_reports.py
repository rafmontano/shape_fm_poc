# ==============================================================================
# p06_03_table_reports.py
# Purpose: Select historical diagnostics and expose tables and directional profiles.
# Inputs: Validated aggregate candidates and common frequency/model/horizon rows.
# Outputs: Deterministic in-memory tables, descriptive ranks and fingerprints.
# Run from: Imported; not run directly.
# ==============================================================================
"""Process 06 reporting; no model execution, scheduling or persistence."""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Any

from .shared_configuration import json_fingerprint

# Code constants: historical point-row order and complete M4 aggregation boundary.
POINT_MODELS = ("naive2", "chronos_2", "m4_fforma", "m4_smyl",
                "m4_smyl_mantis", "chronos_2_mantis", "m4_smyl_oracle")
M4_FREQUENCIES = ("Hourly", "Daily", "Weekly", "Monthly", "Quarterly", "Yearly")
SELECTION_REASON = "owa,total_departure,up_departure,down_departure,lambda_up,lambda_down"


class TableReports:
    """Own deterministic selection and views of one selected scientific result set."""

    @staticmethod
    def select(candidates: list[dict[str, Any]]) -> dict[str, Any]:
        """Select the first candidate under all approved tie levels, never tolerance ties."""
        if not candidates or any(not math.isfinite(r["metrics"]["owa"]) for r in candidates):
            raise ValueError("selection requires finite complete candidates")
        return min(candidates, key=lambda r: (
            r["metrics"]["owa"], abs(r["lambda_up"] - 1) + abs(r["lambda_down"] - 1),
            abs(r["lambda_up"] - 1), abs(r["lambda_down"] - 1),
            r["lambda_up"], r["lambda_down"],
        ))

    @staticmethod
    def aggregate_all(selected: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Weight components by series count and recalculate OWA only for complete M4."""
        frequencies = {r["frequency"] for r in selected}
        if frequencies != set(M4_FREQUENCIES):
            raise ValueError("All requires the complete six-frequency M4 result")
        by_model = defaultdict(list)
        for row in selected:
            by_model[row["model"]].append(row)
        aggregate = []
        for model, rows in sorted(by_model.items()):
            if len(rows) != 6 or {r["frequency"] for r in rows} != frequencies:
                raise ValueError(f"incomplete or duplicate All model: {model}")
            weights = [r["evaluation_count"] for r in rows]
            if any(type(n) is not int or n <= 0 for n in weights):
                raise ValueError("All requires positive series counts")
            metrics = {}
            for field in ("smape", "mase", "da"):
                values = [r["metrics"][field] for r in rows]
                if all(v is None for v in values) and field != "da":
                    metrics[field] = None
                elif any(v is None or not math.isfinite(v) for v in values):
                    raise ValueError("incomplete All components")
                else:
                    metrics[field] = sum(v * n for v, n in zip(values, weights)) / sum(weights)
            aggregate.append({"frequency": "All", "model": model,
                              "evaluation_count": sum(weights), "metrics": metrics})
        reference = next((r for r in aggregate if r["model"] == "naive2"), None)
        if reference is None or any(reference["metrics"][f] <= 0 for f in ("smape", "mase")):
            raise ValueError("All requires positive Naive2 components")
        for row in aggregate:
            m = row["metrics"]
            m["owa"] = (None if m["smape"] is None else
                        0.5 * (m["smape"] / reference["metrics"]["smape"] +
                               m["mase"] / reference["metrics"]["mase"]))
        return aggregate

    @staticmethod
    def tables(selected: list[dict[str, Any]], directional: list[dict[str, Any]]) -> dict:
        """Build both tables from selected rows using only common terminal DA records."""
        cells = {}
        terminal = {}
        for row in directional:
            key = (row["frequency"], row["model"])
            if key not in terminal or row["horizon"] > terminal[key]["horizon"]:
                terminal[key] = row
        for row in selected:
            key = (row["frequency"], row["model"])
            if key in cells:
                raise ValueError("duplicate selected table row")
            record = {**row, "metrics": dict(row["metrics"])}
            if row["frequency"] != "All":
                if key not in terminal:
                    raise ValueError("missing common terminal directional result")
                record["metrics"]["da"] = terminal[key]["accuracy"]
            cells[key] = record
        output = {"Table1": [], "Table2": {"A": [], "B": [], "C": []}}
        for frequency in sorted({r["frequency"] for r in selected}):
            reference = cells.get((frequency, "m4_smyl"))
            if reference is None:
                raise ValueError("SMYL table reference missing")
            smyl = reference["metrics"]
            if any(smyl[k] is None or smyl[k] <= 0 for k in ("smape", "mase", "owa")):
                raise ValueError("SMYL improvement requires positive components")
            for model in POINT_MODELS:
                row = cells.get((frequency, model))
                if row is None:
                    raise ValueError(f"incomplete table model: {frequency}/{model}")
                m = row["metrics"]
                improvement = {field: 100 * (1 - m[field] / smyl[field])
                               for field in ("smape", "mase", "owa")}
                if frequency == "Daily":
                    output["Table1"].append({**row, "improvement_vs_smyl_pct": improvement})
                for panel, value in (("A", m["da"]), ("B", m["owa"]), ("C", improvement["owa"])):
                    output["Table2"][panel].append({"frequency": frequency, "model": model, "value": value})
            mantis = cells.get((frequency, "directional_mantis_rf"))
            if mantis is None:
                raise ValueError("standalone Mantis table result missing")
            output["Table2"]["A"].append({"frequency": frequency,
                                           "model": "directional_mantis_rf",
                                           "value": mantis["metrics"]["da"]})
        return output

    @staticmethod
    def directional_profile(rows: list[dict[str, Any]], models: list[str],
                            expected_horizons: dict[str, list[int]], *, rank: bool = True) -> dict:
        """Expose the fingerprinted matrix and tied average ranks; report incompleteness."""
        if not models or len(set(models)) != len(models):
            raise ValueError("configured report models must be non-empty and unique")
        values = {}
        for row in rows:
            key = (row["frequency"], row["model"], row["horizon"])
            if key in values:
                raise ValueError("duplicate directional result")
            n, correct = row["evaluation_count"], row["correct_count"]
            if type(n) is not int or n <= 0 or type(correct) is not int or not 0 <= correct <= n:
                raise ValueError("invalid directional counts")
            if row["accuracy"] != correct / n:
                raise ValueError("directional accuracy disagrees with counts")
            values[key] = row
        blocks = [(f, h) for f in sorted(expected_horizons) for h in expected_horizons[f]]
        missing = {model: [(f, h) for f, h in blocks if (f, model, h) not in values]
                   for model in models}
        incomplete = {m: gaps for m, gaps in missing.items() if gaps}
        matrix = [{"frequency": f, "horizon": h, "model": m,
                   "accuracy": values[(f, m, h)]["accuracy"],
                   "result_fingerprint": values[(f, m, h)]["result_fingerprint"]}
                  for f, h in blocks for m in models if (f, m, h) in values]
        ranks = None
        if rank and not incomplete:
            totals = dict.fromkeys(models, 0.0)
            for f, h in blocks:
                ordered = sorted(models, key=lambda m: -values[(f, m, h)]["accuracy"])
                start = 0
                while start < len(ordered):
                    end = start + 1
                    while end < len(ordered) and values[(f, ordered[end], h)]["accuracy"] == values[(f, ordered[start], h)]["accuracy"]:
                        end += 1
                    rank = (start + 1 + end) / 2
                    for m in ordered[start:end]:
                        totals[m] += rank
                    start = end
            ranks = {m: totals[m] / len(blocks) for m in models}
        return {"matrix": matrix, "matrix_fingerprint": json_fingerprint(matrix),
                "models": models, "incomplete_models": incomplete,
                **({"mean_ranks": ranks} if rank else {}),
                "interpretation": "descriptive dependent horizons; no significance or CD claim"}
