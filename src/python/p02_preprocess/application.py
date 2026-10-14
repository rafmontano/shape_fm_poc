"""Process 02 preparation of canonical historical observations.

Inputs are accepted series and the stored cleaning policy. Outputs are finite,
same-scale prepared histories and native method/period/package provenance in the
parent DuckDB. Future test observations never enter the R cleaning request.
"""

from __future__ import annotations

import math
from typing import Any

from util.providers import resolve_r_period, run_r
from util.runtime import DataRef, WorkItem


def calculate_prepared_history(item: WorkItem) -> dict[str, Any]:
    """Prepare one history with the configured native function, without fallback.

    Per docs/data-contract.md, standard uses forecast::na.interp and robust uses
    forecast::tsclean. The historical end is the official test start. Cleaning
    preserves scale/length; the canonical observations remain unchanged.
    """
    return calculate_prepared_histories([item])[0][1]


def calculate_prepared_histories(items: list[WorkItem]) -> list[tuple[str, dict[str, Any]]]:
    """Prepare a bounded native batch without repeating R startup for each series.

    Each result retains its series key and is independently restartable after
    coordinator acceptance. Batching cannot alter the native cleaning formula.
    """
    settings = items[0].payload
    jobs = [{"id": item.key,
        "context": [None if value is None or math.isnan(value) else value for value in item.payload["history"]],
        "mode": item.payload["method"], "seasonality": item.payload["period"]["r_period"]}
        for item in items]
    response = run_r(settings["script"], {"action": "preprocess", "jobs": jobs},
                     timeout=settings["timeout"], threads=settings["threads"])
    results = response["results"]
    if len(results) != len(items) or {result["id"] for result in results} != {item.key for item in items}:
        raise ValueError("preprocessing provider returned unexpected atomic identities")
    by_key = {result["id"]: result for result in results}
    return [(item.key, _prepared_result(item, by_key[item.key], response["packages"])) for item in items]


def _prepared_result(item: WorkItem, result: dict[str, Any], packages: dict[str, str]) -> dict[str, Any]:
    payload = item.payload
    history = payload["history"]
    values = result["values"]
    if (result["status"] != "success" or result["preprocessing_mode"] != payload["method"]
            or len(values) != len(history) or not all(math.isfinite(value) for value in values)
            or result["missing_count_after"] != 0):
        raise ValueError("preprocessing provider returned invalid values or method provenance")
    if payload["method"] == "standard" and any(
        original is not None and math.isfinite(original) and original != prepared
        for original, prepared in zip(history, values, strict=True)
    ):
        raise ValueError("standard preprocessing changed a finite observation")
    return {"series_key": payload["series_key"], "method": payload["method"],
            "history_end": payload["history_end"], "values": values,
            "parameters": payload["period"], "provider": packages,
            "missing_count_before": result["missing_count_before"],
            "values_changed": result["values_changed"]}


class Application:
    """Own prepared-series planning, coordinator acceptance and dataset reference.

    Accepted series keys are the atomic restart boundary of subflow 02.01;
    subflow 02.02 requires one result for every canonical series in scope.
    """

    calculate_prepared_history = staticmethod(calculate_prepared_history)
    calculate_prepared_histories = staticmethod(calculate_prepared_histories)

    def __init__(self, context: Any):
        self.context = context
        self.store = context.repository

    def canonical_dataset(self) -> DataRef:
        """Read Process 01's accepted canonical dataset reference."""
        return self.context.ref("01.03")

    def prepared_dataset(self) -> DataRef:
        """Return the prepared dataset consumed by modelling-input construction."""
        return self.context.ref("02.02")

    def pending_prepared_histories(self, canonical: DataRef) -> list[WorkItem]:
        """Plan only missing series and resolve each required R period once."""
        configuration = self.context.configuration
        execution = configuration["execution"]
        method = configuration["pipeline"]["preprocessing"]["default"]
        accepted = self.store.keys("02.01")
        periods = {}
        items = []
        for key, series in self.store.rows("01.02").items():
            if key in accepted:
                continue
            frequency = series["frequency"]
            if frequency not in periods:
                periods[frequency] = resolve_r_period(configuration, frequency)
            items.append(WorkItem(key, {"series_key": key, "method": method,
                "history": series["target"][:series["test_start"]], "history_end": series["test_start"],
                "period": periods[frequency], "script": execution["paths"]["r_preprocess_worker"],
                "timeout": execution["default"]["worker_timeouts_seconds"]["r"],
                "threads": execution["default"]["thread_limits"]["r"]}))
        return self.store.plan("02.01", items)

    def accept_prepared_histories(self, results: list[tuple[str, dict[str, Any]]]) -> None:
        """Commit complete prepared histories at coordinator atomic-key boundaries."""
        self.store.accept("02.01", results)

    def accept_prepared_dataset(self, canonical: DataRef) -> DataRef:
        """Accept matching canonical/prepared identities without repeating cleaning."""
        if self.store.keys("01.02") != self.store.keys("02.01"):
            raise RuntimeError("prepared histories do not match the canonical population")
        return self.prepared_dataset()
