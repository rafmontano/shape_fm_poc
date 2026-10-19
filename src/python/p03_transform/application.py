"""Process 03 modelling inputs, research membership and reusable features.

Inputs are accepted prepared histories and canonical training/test boundaries.
Outputs are parent membership/features/cohort references and child rolling plus
official modelling inputs sliced from the once-prepared historical series.
Sample-SD fitting, strict direction labels and window boundaries follow
docs/data-contract.md; feature definitions follow R p03_transform/features.R.
"""

from __future__ import annotations

import math
from typing import Any

from p03_transform.inputs import rolling_window_inputs, s1_partition
from p03_transform.labels import directional_labels
from util.providers import run_r
from util.runtime import DataRef, WorkItem
from util.transformations import transform


def calculate_modelling_input(item: WorkItem) -> dict[str, Any]:
    """Transform one prepared origin and label its associated future block."""
    return calculate_modelling_batch([item])[0][1]


def calculate_modelling_batch(items: list[WorkItem]) -> list[tuple[str, dict[str, Any]]]:
    """Fit input-only sample-SD state without another cleaning/interpolation pass.

    Rolling inputs/targets are prepared; official actuals are raw. Labels compare
    those targets with the prepared endpoint under docs/data-contract.md.
    """
    outputs = []
    for item in items:
        payload = item.payload
        cleaned = payload["prepared_input"]
        transformed = transform(cleaned, payload["transformation"])
        reference = cleaned[-1]
        labels = directional_labels(payload["actual"], reference).tolist()
        output = {key: value for key, value in payload.items() if key not in {"execution", "point_history"}}
        output.update(cleaned_input=cleaned, values=list(transformed.values), state=transformed.parameters,
            label_reference=reference, labels=[None if math.isnan(value) else int(value) for value in labels],
            label_definition="directional_strict_v1", evaluation_basis="prepared_endpoint",
            target_basis="raw_official" if payload["kind"] == "official" else "prepared_history")
        if payload["kind"] == "official":
            point = transform(payload["point_history"], payload["transformation"])
            output.update(point_context=list(point.values), point_state=point.parameters,
                          prepared_history=payload["point_history"])
        outputs.append((item.key, output))
    return outputs


def calculate_reusable_feature(item: WorkItem) -> dict[str, Any]:
    """Calculate one approved FFORMA feature vector from a prepared input."""
    return calculate_feature_batch([item])[0][1]


def calculate_feature_batch(items: list[WorkItem]) -> list[tuple[str, dict[str, Any]]]:
    """Extract the accepted 42-feature contract in independent native R jobs."""
    settings = items[0].payload["execution"]
    response = run_r("src/r/p03_transform/feature_worker.R", {"action": "extract", "jobs": [
        {"id": item.key, "context": item.payload["values"], "seasonality": item.payload["r_period"]}
        for item in items]}, timeout=settings["timeout"], threads=settings["threads"])
    results = response["results"]
    provider = response["provider"]
    if len(results) != len(items) or {row["id"] for row in results} != {item.key for item in items}:
        raise ValueError("feature provider returned unexpected identities")
    output = []
    for result in results:
        if result["status"] != "success":
            raise RuntimeError(f"feature item {result['id']}: {result['error']}")
        if (result["feature_names"] != provider["feature_names"] or len(result["feature_values"]) != 42
                or not all(math.isfinite(value) for value in result["feature_values"])):
            raise ValueError(f"feature contract mismatch: {result['id']}")
        output.append((result["id"], {"input_key": result["id"], "feature_set_id": provider["feature_set_id"],
            "feature_set_version": provider["feature_set_version"], "names": result["feature_names"],
            "values": result["feature_values"], "provider": provider["dependencies"]}))
    return output


class Application:
    """Own Process 03 references and atomic input/feature coordinator acceptance.

    Membership is parent-owned. Each modelling origin is child-owned. Features
    and model-specific ordered training populations remain in the parent.
    Mantis uses all eligible S1 windows; only DTW uses the historical cap.
    """

    calculate_modelling_inputs = staticmethod(calculate_modelling_input)
    calculate_modelling_batch = staticmethod(calculate_modelling_batch)
    calculate_reusable_features = staticmethod(calculate_reusable_feature)
    calculate_feature_batch = staticmethod(calculate_feature_batch)

    def __init__(self, context: Any):
        self.context = context
        self.store = context.repository
        self.configuration = context.configuration["pipeline"]["window_preparation"]

    def prepared_dataset(self) -> DataRef:
        """Return the accepted Process 02 dataset reference."""
        return self.context.ref("02.02")

    def existing_membership(self) -> DataRef:
        """Read the accepted series-level research membership reference."""
        return self.context.ref("03.01")

    def modelling_dataset(self) -> DataRef:
        """Return the complete Process 03 input/features/cohort reference."""
        return self.context.ref("03.04")

    def assign_research_membership(self, prepared: DataRef) -> DataRef:
        """Assign seeded S1 only to series with a complete training block."""
        series = self.store.rows("01.02")
        partitions = {}
        split = self.configuration["split"]
        for frequency in self.configuration["selected_frequencies"]:
            sizes = self.configuration["frequencies"][frequency]
            eligible = [(row["dataset_id"], row["series_id"]) for row in series.values()
                if row["frequency"] == frequency and row["train_end"] >= sizes["input_length"] + sizes["future_horizon"]]
            partitions.update(s1_partition(eligible, split["training_fraction"], split["seed"]))
        self.store.accept("03.01", [(key, {"series_key": key, "split_id": split["split_id"],
            "partition": partitions.get((row["dataset_id"], row["series_id"])),
            "research_role": "reference_and_evaluation" if (row["dataset_id"], row["series_id"]) in partitions else "evaluation_only"})
            for key, row in series.items()])
        return self.existing_membership()

    def _execution(self) -> dict[str, Any]:
        settings = self.context.configuration["execution"]
        return {"timeout": settings["default"]["worker_timeouts_seconds"]["r"],
                "threads": settings["default"]["thread_limits"]["r"]}

    def pending_modelling_inputs(self, membership: DataRef) -> list[WorkItem]:
        """Plan missing rolling origins and uncapped official evaluation inputs.

        Rolling inputs and targets slice prepared pre-test history; official
        actuals remain raw. Neither the DTW cap nor cleaning changes this plan.
        """
        child = self.context.windows()
        accepted = child.keys("03.02")
        series = self.store.rows("01.02")
        histories = self.store.rows("02.01")
        members = self.store.rows("03.01")
        items = []
        for series_key, row in series.items():
            settings = self.configuration["frequencies"][row["frequency"]]
            length = settings["input_length"]
            common = {"series_key": series_key, "series_id": row["series_id"], "frequency": row["frequency"],
                "partition": members[series_key]["partition"],
                "transformation": self.configuration["transformation"], "execution": self._execution(),
                "r_period": histories[series_key]["parameters"]["r_period"]}
            for window in rolling_window_inputs(histories[series_key]["values"][:row["train_end"]], length, settings["future_horizon"]):
                key = f"{series_key}/rolling/{window['input_start']:09d}"
                if key not in accepted:
                    items.append(WorkItem(key, {**common, **window, "kind": "rolling"}))
            key = f"{series_key}/official/{row['test_start']:09d}"
            if key not in accepted:
                prepared = histories[series_key]["values"][-length:]
                prepared = [prepared[0]] * (length - len(prepared)) + prepared
                items.append(WorkItem(key, {**common, "kind": "official", "prepared_input": prepared,
                    "input_start": max(0, row["test_start"] - length), "input_end": row["test_start"],
                    "future_start": row["test_start"], "future_end": row["observation_count"],
                    "actual": row["target"][row["test_start"]:], "point_history": histories[series_key]["values"]}))
        return child.plan("03.02", items)

    def accept_modelling_inputs(self, results: list[tuple[str, dict[str, Any]]]) -> None:
        """Commit complete origin inputs and labels only in the fixed child store."""
        self.context.windows().accept("03.02", results)

    def _reference_cohort(self, inputs: dict[str, dict[str, Any]]) -> list[str]:
        existing = self.store.rows("reference_cohort")
        if existing:
            return existing["directional_dtw"]["input_keys"]
        population = sorted(key for key, row in inputs.items()
            if row["kind"] == "rolling" and row["partition"] == "train")
        settings = self.configuration["dtw_training_cohort"]
        selection = run_r("src/r/p03_transform/training_cohort.R", {
            "labels": [inputs[key]["labels"][0] for key in population],
            "cap": settings["cap"], "seed": settings["seed"]}, timeout=60, threads=1)
        indexes = selection["selected_indices"]
        if isinstance(indexes, int):
            indexes = [indexes]
        selected = [population[index - 1] for index in indexes]
        self.store.accept("reference_cohort", [("directional_dtw", {"input_keys": selected,
            "eligible_count": len(population), "configured_cap": settings["cap"],
            "seed": settings["seed"], "method": settings["method"],
            "r_version": selection["r_version"], "rng_kind": selection["rng_kind"]}),
            ("directional_mantis_rf", {"input_keys": population, "eligible_count": len(population),
                "method": "all_eligible_s1_training_windows"})])
        return selected

    def pending_reusable_features(self, membership: DataRef) -> list[WorkItem]:
        """Plan reusable features for DTW references and all official inputs."""
        inputs = self.context.windows().rows("03.02")
        required = set(self._reference_cohort(inputs)) | {key for key, row in inputs.items() if row["kind"] == "official"}
        return self.store.plan("03.03", [WorkItem(key, {"values": inputs[key]["cleaned_input"],
            "r_period": inputs[key]["r_period"], "execution": self._execution()}) for key in sorted(required)])

    def accept_reusable_features(self, results: list[tuple[str, dict[str, Any]]]) -> None:
        """Commit approved reusable feature vectors in the parent database."""
        self.store.accept("03.03", results)

    def accept_modelling_dataset(self, membership: DataRef) -> DataRef:
        """Accept complete planned inputs/features and model-specific populations."""
        self.context.windows().require_complete("03.02")
        self.store.require_complete("03.03")
        if not self.store.rows("reference_cohort"):
            raise RuntimeError("modelling dataset has no training populations")
        return self.modelling_dataset()
