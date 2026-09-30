# ==============================================================================
# seasonal_period_tuning.py
#
# Purpose: Execute leakage-safe Gate 4 period-policy validation for AutoARIMA/ETS.
# Inputs: Pending Gate 4 tasks, raw official-history contexts, fixed preparation
#   variants, baseline period, and the versioned tuning configuration.
# Outputs: Inspectable fold/candidate/validation/selection rows and normal final forecasts.
# Run from: ExperimentCoordinator._run_04_forecast for opt-in v3 experiments.
# ==============================================================================

"""Leakage-safe seasonal-period policy tuning on historical validation folds."""

from __future__ import annotations

import json
import math
import platform
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any

from .configuration import canonical_json, json_fingerprint
from .transformations import inverse, transform


MODEL_METHODS = {
    "auto_arima": "auto_arima_forec",
    "ets": "ets_forec",
}


@dataclass(frozen=True)
class HistoricalFold:
    """One expanding-origin fold wholly contained in official model history."""

    number: int
    train_start: int
    train_end: int
    validation_start: int
    validation_end: int


def historical_folds(length: int, horizon: int, count: int = 3) -> tuple[HistoricalFold, ...]:
    """Return oldest-to-newest expanding folds ending at the official test boundary."""
    if horizon < 1 or count < 1 or length <= count * horizon:
        return ()
    return tuple(
        HistoricalFold(
            number=index + 1,
            train_start=0,
            train_end=length - (count - index) * horizon,
            validation_start=length - (count - index) * horizon,
            validation_end=length - (count - index - 1) * horizon,
        )
        for index in range(count)
    )


def original_scale_mae(
    prediction: list[float], actual: list[float | None]
) -> tuple[float | None, int]:
    """Score finite labels only; missing labels are benchmark-masked, never imputed."""
    if len(prediction) != len(actual) or any(not math.isfinite(value) for value in prediction):
        raise ValueError("validation prediction must be finite and match the horizon")
    errors = [
        abs(predicted - float(observed))
        for predicted, observed in zip(prediction, actual, strict=True)
        if observed is not None and math.isfinite(float(observed))
    ]
    return (sum(errors) / len(errors), len(errors)) if errors else (None, 0)


def select_period_policy(
    baseline_scores: list[float | None],
    estimated_scores: list[float | None],
    baseline_valid: list[bool],
    estimated_valid: list[bool],
) -> dict[str, Any]:
    """Select estimated only after three complete requested-model comparisons.

    Ties and every incomplete, fallback, failed, or all-missing comparison retain
    baseline. The function is pure so sequential and parallel callers agree.
    """
    if not (
        len(baseline_scores)
        == len(estimated_scores)
        == len(baseline_valid)
        == len(estimated_valid)
        == 3
    ):
        return {
            "selected_policy": "baseline",
            "baseline_mean_mae": None,
            "estimated_mean_mae": None,
            "status": "skipped",
            "reason": "three complete historical validation windows are required",
        }
    if not all(baseline_valid) or not all(estimated_valid) or any(
        value is None for value in (*baseline_scores, *estimated_scores)
    ):
        return {
            "selected_policy": "baseline",
            "baseline_mean_mae": None,
            "estimated_mean_mae": None,
            "status": "inconclusive",
            "reason": "one or more policy windows were incomplete, substituted, fallback, failed, or all-missing",
        }
    baseline_mean = sum(float(value) for value in baseline_scores) / 3
    estimated_mean = sum(float(value) for value in estimated_scores) / 3
    if estimated_mean < baseline_mean and not math.isclose(
        estimated_mean, baseline_mean, rel_tol=1e-12, abs_tol=1e-12
    ):
        return {
            "selected_policy": "estimated",
            "baseline_mean_mae": baseline_mean,
            "estimated_mean_mae": estimated_mean,
            "status": "selected",
            "reason": "estimated policy has lower mean MAE across all three folds",
        }
    return {
        "selected_policy": "baseline",
        "baseline_mean_mae": baseline_mean,
        "estimated_mean_mae": estimated_mean,
        "status": "selected",
        "reason": "baseline retained because it tied or outperformed the estimated policy",
    }


def _finite_or_none(values: list[Any]) -> list[float | None]:
    """Normalize DuckDB float values while retaining missing labels as missing."""
    return [
        None if value is None or (isinstance(value, float) and math.isnan(value)) else float(value)
        for value in values
    ]


def _worker_results(response: dict[str, Any], expected: set[str]) -> dict[str, dict[str, Any]]:
    """Validate one bounded worker response and return its unique results by ID."""
    results = response.get("results")
    if not isinstance(results, list):
        raise RuntimeError("R tuning worker returned no result list")
    identifiers = [result.get("id") for result in results]
    if len(identifiers) != len(set(identifiers)) or set(identifiers) != expected:
        raise RuntimeError("R tuning worker returned missing, duplicate, or unexpected IDs")
    return {result["id"]: result for result in results}


def _forecast_job(
    identifier: str,
    dataset_id: str,
    series_id: str,
    model: str,
    context: list[float],
    horizon: int,
    period: int,
    settings: dict[str, Any],
) -> dict[str, Any]:
    """Build the actual-free registered R forecast request."""
    return {
        "id": identifier,
        "dataset_id": dataset_id,
        "series_id": series_id,
        "model": model,
        "context": context,
        "horizon": horizon,
        "model_period": period,
        "settings": settings,
    }


def _run_tuning_r(
    payload: dict[str, Any],
    script: str,
    timeout: float,
    threads: int,
    memory_monitor: Any = None,
) -> dict[str, Any]:
    """Run one bounded R tuning request from a local or Dask worker process."""
    from .distributed_execution import _run_r

    return _run_r(payload, script, timeout, threads, memory_monitor)


def _compute_tuning_group(
    payload: dict[str, Any],
    preprocess_script: str,
    forecast_script: str,
    timeout: float,
    threads: int,
    memory_monitor: Any = None,
) -> dict[str, Any]:
    """Compute one variant/series group's pending tuning tasks without DuckDB.

    The serializable payload may contain persisted fold/candidate/validation rows;
    those rows are reused rather than refitted. Returned records are written only
    by the Mac coordinator after identity validation.
    """
    started = time.monotonic()
    tuning = payload["tuning"]
    raw_context = _finite_or_none(payload["raw_context"])
    horizon = int(payload["horizon"])
    baseline_period = int(payload["baseline_period"])
    folds = historical_folds(
        len(raw_context), horizon, int(tuning["validation_windows"])
    )
    prepared_folds = list(payload.get("folds", []))
    new_folds: list[dict[str, Any]] = []
    if folds and len(prepared_folds) != len(folds):
        jobs = [
            {
                "id": f"{payload['instance_id']}/{payload['variant_id']}/fold/{fold.number}",
                "context": raw_context[fold.train_start : fold.train_end],
                "mode": payload["cleaning"],
                "seasonality": baseline_period,
            }
            for fold in folds
        ]
        response = _run_tuning_r(
            {"action": "preprocess", "jobs": jobs},
            preprocess_script,
            timeout,
            threads,
            memory_monitor,
        )
        cleaned = _worker_results(response, {job["id"] for job in jobs})
        prepared_folds = []
        for fold, job in zip(folds, jobs, strict=True):
            transformed = transform(
                [float(value) for value in cleaned[job["id"]]["values"]],
                payload["transformation_method"],
            )
            fold_id = f"tuning-fold/{json_fingerprint({'experiment': payload['experiment_id'], 'variant': payload['variant_id'], 'instance': payload['instance_id'], 'fold': fold.number, 'tuning': tuning})[:32]}"
            preparation_id = f"fold-preparation/{json_fingerprint({'fold': fold_id, 'raw': raw_context[fold.train_start:fold.train_end]})[:32]}"
            record = {
                "fold_id": fold_id,
                "fold_number": fold.number,
                "train_start": fold.train_start,
                "train_end": fold.train_end,
                "validation_start": fold.validation_start,
                "validation_end": fold.validation_end,
                "horizon": horizon,
                "raw_training_hash": json_fingerprint(
                    raw_context[fold.train_start : fold.train_end]
                ),
                "prepared_training_hash": json_fingerprint(transformed.values),
                "context": list(transformed.values),
                "preprocessing_id": preparation_id,
                "transformation_method": payload["transformation_method"],
                "parameters": transformed.parameters,
                "actual": raw_context[fold.validation_start : fold.validation_end],
                "preparation_metadata": {
                    "cleaning": payload["cleaning"],
                    "cleaning_period": baseline_period,
                    "cleaning_packages": response["packages"],
                    "transformation_parameters": transformed.parameters,
                    "validation_actual_role": "QA scoring data; never model input",
                },
            }
            prepared_folds.append(record)
            new_folds.append(record)

    cached_candidates = payload.get("candidates", {})
    cached_validations = payload.get("validations", {})
    shared_diagnostics: dict[int, tuple[dict[str, Any], dict[str, Any]]] = {}
    for prepared in prepared_folds:
        fold_number = int(prepared["fold_number"])
        stored = next(
            (
                cached_candidates[model][str(fold_number)]
                for model in cached_candidates
                if str(fold_number) in cached_candidates[model]
            ),
            None,
        )
        if stored is not None:
            estimated = stored.get("estimated_period")
            eligible = (
                estimated is not None
                and len(prepared["context"]) >= tuning["minimum_cycles"] * int(estimated)
                and int(estimated) <= 350
            )
            shared_diagnostics[fold_number] = (
                {
                    "estimated_period": estimated,
                    "seasonal_strength": stored.get("seasonal_strength"),
                    "eligible": eligible,
                    "reason": None if eligible else "stored estimate is ineligible",
                },
                stored.get("package_versions", {}),
            )
        else:
            identifier = f"{prepared['fold_id']}/shared-diagnostic"
            response = _run_tuning_r(
                {
                    "action": "diagnose_period",
                    "jobs": [
                        {
                            "id": identifier,
                            "context": prepared["context"],
                            "baseline_period": baseline_period,
                            "model": "auto_arima",
                            "minimum_cycles": tuning["minimum_cycles"],
                        }
                    ],
                },
                forecast_script,
                timeout,
                threads,
                memory_monitor,
            )
            shared_diagnostics[fold_number] = (
                _worker_results(response, {identifier})[identifier],
                response["packages"],
            )

    model_outputs = []
    for task in payload["tasks"]:
        model_started = time.monotonic()
        model = task["model"]
        if model not in MODEL_METHODS:
            raise RuntimeError(f"unsupported tuned model: {model}")
        baseline_scores: list[float | None] = []
        estimated_scores: list[float | None] = []
        baseline_valid: list[bool] = []
        estimated_valid: list[bool] = []
        candidates: list[dict[str, Any]] = []
        validations: list[dict[str, Any]] = []
        model_candidate_cache = cached_candidates.get(model, {})
        model_validation_cache = cached_validations.get(model, {})

        for prepared in prepared_folds:
            fold_number = int(prepared["fold_number"])
            candidate_id = f"period-candidate/{json_fingerprint({'fold': prepared['fold_id'], 'model': model})[:32]}"
            stored_candidate = model_candidate_cache.get(str(fold_number))
            if stored_candidate is None:
                diagnostic, packages = shared_diagnostics[fold_number]
                estimated_period = diagnostic.get("estimated_period")
                eligible = bool(diagnostic["eligible"])
                reason = diagnostic.get("reason")
                if eligible and model == "ets" and int(estimated_period) > 24:
                    eligible = False
                    reason = f"model ets does not support period {estimated_period}"
                candidate = {
                    "candidate_id": candidate_id,
                    "fold_id": prepared["fold_id"],
                    "model": model,
                    "baseline_period": baseline_period,
                    "estimated_period": estimated_period,
                    "seasonal_strength": diagnostic.get("seasonal_strength"),
                    "eligible": eligible,
                    "reason": reason,
                    "package_versions": packages,
                }
                candidates.append(candidate)
            else:
                candidate = stored_candidate
                estimated_period = candidate.get("estimated_period")
                eligible = bool(candidate["eligible"])
                reason = candidate.get("reason")

            policies = {
                "baseline": (baseline_period, baseline_period, True, None),
                "estimated": (
                    estimated_period,
                    int(estimated_period) if eligible else baseline_period,
                    eligible,
                    reason,
                ),
            }
            computed: dict[int, dict[str, Any]] = {}
            for policy, values in policies.items():
                requested_period, executed_period, policy_eligible, policy_reason = values
                cached = model_validation_cache.get(str(fold_number), {}).get(policy)
                if cached is None:
                    if executed_period not in computed:
                        forecast_identifier = f"{candidate_id}/{policy}/forecast"
                        try:
                            response = _run_tuning_r(
                                {
                                    "action": "forecast",
                                    "jobs": [
                                        _forecast_job(
                                            forecast_identifier,
                                            payload["dataset_id"],
                                            payload["series_id"],
                                            model,
                                            prepared["context"],
                                            horizon,
                                            executed_period,
                                            task["settings"],
                                        )
                                    ],
                                },
                                forecast_script,
                                timeout,
                                threads,
                                memory_monitor,
                            )
                            result = _worker_results(response, {forecast_identifier})[
                                forecast_identifier
                            ]
                            result["packages"] = response["packages"]
                        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
                            result = {
                                "worker_error": f"{type(error).__name__}: {error}",
                                "requested_method_id": MODEL_METHODS[model],
                                "executed_method_id": MODEL_METHODS[model],
                                "fallback_used": False,
                                "fallback_reason": None,
                                "packages": {},
                                "provenance": {},
                            }
                        computed[executed_period] = result
                    result = computed[executed_period]
                    if "worker_error" in result:
                        prediction: list[float] = []
                        mae = None
                        valid_count = sum(
                            value is not None and math.isfinite(float(value))
                            for value in prepared["actual"]
                        )
                        status = "failed"
                        status_reason = result["worker_error"]
                    else:
                        prediction = list(
                            inverse(
                                result["mean"],
                                payload["transformation_method"],
                                prepared["parameters"],
                            )
                        )
                        mae, valid_count = original_scale_mae(
                            prediction, prepared["actual"]
                        )
                        if valid_count == 0:
                            status = "all_missing"
                            status_reason = "validation window has no finite labels"
                        elif result["fallback_used"]:
                            status = "fallback"
                            status_reason = result["fallback_reason"]
                        elif policy == "estimated" and not policy_eligible:
                            status = "substituted"
                            status_reason = policy_reason
                        else:
                            status = "success"
                            status_reason = None
                    validation_id = f"tuning-validation/{json_fingerprint({'candidate': candidate_id, 'policy': policy, 'settings': task['settings']})[:32]}"
                    cached = {
                        "validation_id": validation_id,
                        "candidate_id": candidate_id,
                        "model": model,
                        "policy": policy,
                        "requested_period": requested_period,
                        "executed_period": executed_period,
                        "prediction": prediction,
                        "actual": prepared["actual"],
                        "mae": mae,
                        "valid_label_count": valid_count,
                        "requested_method_id": result["requested_method_id"],
                        "executed_method_id": result["executed_method_id"],
                        "fallback_used": result["fallback_used"],
                        "status": status,
                        "reason": status_reason,
                        "execution_metadata": {
                            "packages": result["packages"],
                            "provenance": result["provenance"],
                            "deduplicated_equal_period": policy == "estimated"
                            and executed_period == baseline_period,
                        },
                        "content_hash": json_fingerprint(
                            {
                                "prediction": prediction,
                                "actual": prepared["actual"],
                                "period": executed_period,
                            }
                        ),
                    }
                    validations.append(cached)
                score = None if cached.get("mae") is None else float(cached["mae"])
                (baseline_scores if policy == "baseline" else estimated_scores).append(score)
                (baseline_valid if policy == "baseline" else estimated_valid).append(
                    cached["status"] == "success"
                )

        selection = select_period_policy(
            baseline_scores, estimated_scores, baseline_valid, estimated_valid
        )
        final_estimated_period = None
        final_period = baseline_period
        final_substitution_reason = None
        final_diagnostic = None
        if selection["selected_policy"] == "estimated":
            diagnostic_id = f"{task['task_id']}/final-period"
            response = _run_tuning_r(
                {
                    "action": "diagnose_period",
                    "jobs": [
                        {
                            "id": diagnostic_id,
                            "context": payload["full_prepared"],
                            "baseline_period": baseline_period,
                            "model": model,
                            "minimum_cycles": tuning["minimum_cycles"],
                        }
                    ],
                },
                forecast_script,
                timeout,
                threads,
                memory_monitor,
            )
            final_diagnostic = _worker_results(response, {diagnostic_id})[diagnostic_id]
            final_estimated_period = final_diagnostic.get("estimated_period")
            if final_diagnostic["eligible"]:
                final_period = int(final_estimated_period)
            else:
                final_substitution_reason = final_diagnostic.get("reason")
                selection = {
                    **selection,
                    "selected_policy": "baseline",
                    "status": "inconclusive",
                    "reason": "estimated policy won validation but its final-history period was ineligible",
                }

        final_identifier = f"{task['task_id']}/final"
        final_job = _forecast_job(
            final_identifier,
            payload["dataset_id"],
            payload["series_id"],
            model,
            payload["full_prepared"],
            horizon,
            final_period,
            task["settings"],
        )
        try:
            final_response = _run_tuning_r(
                {"action": "forecast", "jobs": [final_job]},
                forecast_script,
                timeout,
                threads,
                memory_monitor,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
            if final_period == baseline_period:
                raise
            final_substitution_reason = (
                "estimated final-period forecast failed; baseline used: "
                f"{type(error).__name__}: {error}"
            )
            final_period = baseline_period
            selection = {
                **selection,
                "selected_policy": "baseline",
                "status": "inconclusive",
                "reason": final_substitution_reason,
            }
            final_job["model_period"] = baseline_period
            final_response = _run_tuning_r(
                {"action": "forecast", "jobs": [final_job]},
                forecast_script,
                timeout,
                threads,
                memory_monitor,
            )
        final = _worker_results(final_response, {final_identifier})[final_identifier]
        full_parameters = payload["transformation_parameters"]
        mean = list(inverse(final["mean"], payload["transformation_method"], full_parameters))
        median = list(inverse(final["median"], payload["transformation_method"], full_parameters))
        quantiles = [
            list(inverse(values, payload["transformation_method"], full_parameters))
            for values in final["quantiles"]
        ]
        selection_id = f"period-selection/{json_fingerprint({'experiment': payload['experiment_id'], 'variant': payload['variant_id'], 'instance': payload['instance_id'], 'model': model, 'tuning': tuning})[:32]}"
        forecast_id = f"forecast/{json_fingerprint({'experiment': payload['experiment_id'], 'variant': payload['variant_id'], 'instance': payload['instance_id'], 'candidate': model})[:32]}"
        model_outputs.append(
            {
                "task_id": task["task_id"],
                "model": model,
                "candidates": candidates,
                "validations": validations,
                "selection": {
                    "selection_id": selection_id,
                    **selection,
                    "baseline_period": baseline_period,
                    "final_estimated_period": final_estimated_period,
                    "final_period": final_period,
                    "final_substitution_reason": final_substitution_reason,
                },
                "forecast": {
                    "forecast_id": forecast_id,
                    "mean": mean,
                    "median": median,
                    "quantiles": quantiles,
                    "packages": final_response["packages"],
                    "forecast_method": {
                        "requested_method_id": final["requested_method_id"],
                        "executed_method_id": final["executed_method_id"],
                        "fallback_used": final["fallback_used"],
                        "fallback_reason": final["fallback_reason"],
                        "provenance": final["provenance"],
                    },
                    "final_diagnostic": final_diagnostic,
                    "runtime_seconds": time.monotonic() - model_started,
                },
            }
        )
    return {
        "group_id": payload["group_id"],
        "new_folds": new_folds,
        "models": model_outputs,
        "runtime_seconds": time.monotonic() - started,
    }


def seasonal_tuning_batch(
    batch: list[dict[str, Any]],
    preprocess_script: str,
    forecast_script: str,
    timeout: float,
    threads: int,
    memory_floors_gib: dict[str, float],
    fit_budgets_gib: dict[str, float],
    memory_controls: dict[str, float],
    retry_count: int = 0,
) -> dict[str, Any]:
    """Compute tuning groups under admission and continuous memory protection."""
    from .distributed_execution import _worker_provenance, tuning_memory_reservation

    results = []
    admissions = []
    system = platform.system()
    for payload in batch:
        models = {task["model"] for task in payload["tasks"]}
        if len(models) != 1:
            raise RuntimeError("one distributed tuning payload must contain one model")
        fit_budget_gib = float(fit_budgets_gib[models.pop()])
        with tuning_memory_reservation(
            float(memory_floors_gib[system]),
            fit_budget_gib,
            timeout_seconds=float(memory_controls["admission_timeout_seconds"]),
            poll_interval_seconds=float(memory_controls["poll_interval_seconds"]),
            breach_grace_seconds=float(memory_controls["breach_grace_seconds"]),
            swap_growth_limit_gib=float(memory_controls["swap_growth_limit_gib"]),
        ) as monitor:
            results.append(
                _compute_tuning_group(
                    payload,
                    preprocess_script,
                    forecast_script,
                    timeout,
                    threads,
                    monitor,
                )
            )
            monitor.raise_if_unsafe()
            admissions.append(monitor.evidence())
    return {
        "results": results,
        "worker": {
            **_worker_provenance(retry_count),
            "memory_admissions": admissions,
        },
    }


def distributed_tuning_queue_groups(
    payloads: list[dict[str, Any]],
    profile: Any,
    worker_arguments: tuple[Any, ...],
) -> dict[str, tuple[Any, list[list[dict[str, Any]]], dict[str, float], tuple[Any, ...], int]]:
    """Build profile-bounded model queues without host sampling quotas.

    AutoARIMA requests the Ubuntu-only large-fit capability. ETS requests the
    shared tuning capability advertised by every Mac and Ubuntu CPU worker, so
    Dask may continuously use any safe eligible worker.
    """
    from .distributed_execution import AUTOARIMA_R_RESOURCE, TUNING_R_RESOURCE

    by_model = {
        model: [
            [payload]
            for payload in payloads
            if payload["tasks"][0]["model"] == model
        ]
        for model in ("auto_arima", "ets")
    }
    return {
        "auto_arima": (
            seasonal_tuning_batch,
            by_model["auto_arima"],
            {AUTOARIMA_R_RESOURCE: 1},
            worker_arguments,
            int(profile.dask_autoarima_max_in_flight),
        ),
        "ets": (
            seasonal_tuning_batch,
            by_model["ets"],
            {TUNING_R_RESOURCE: 1},
            worker_arguments,
            int(profile.dask_ets_max_in_flight),
        ),
    }


def run_distributed_tuned_forecasts(
    coordinator: Any,
    experiment_id: str,
    rows: list[tuple],
    attempts: dict[str, int],
    dask_client: Any,
    settings: Any,
    profile: Any,
) -> None:
    """Dispatch resumable tuning groups while retaining sole-writer persistence.

    Workers receive only serializable series/configuration/evidence snapshots.
    This Mac coordinator validates returned task IDs and transactionally writes all
    folds, candidates, validations, selections, forecasts, and task completions.
    """
    from .distributed_execution import run_batch_groups

    tuning = coordinator.configuration.seasonal_period_tuning
    if tuning is None or not tuning["enabled"]:
        raise RuntimeError("seasonal period tuning is not enabled")
    grouped: dict[tuple[str, str], list[tuple]] = {}
    for row in rows:
        grouped.setdefault((row[1], row[2]), []).append(row)
    payloads = []
    for (instance_id, variant_id), model_rows in grouped.items():
        source = coordinator.connection.execute(
            """SELECT i.context_target, i.horizon, i.dataset_id, i.series_id,
                      b.metadata, v.cleaning_method, v.transformation_method,
                      t.transformed_target, t.parameters, t.transformation_id
               FROM forecast_instances i
               JOIN benchmark_configurations b USING (benchmark_configuration_id)
               JOIN experiment_variants v ON v.variant_id=?
               JOIN transformed_series t ON t.experiment_id=v.experiment_id
                    AND t.variant_id=v.variant_id
                    AND t.forecast_instance_id=i.forecast_instance_id
               WHERE i.forecast_instance_id=?""",
            [variant_id, instance_id],
        ).fetchone()
        if source is None:
            raise RuntimeError(f"missing tuning input for {instance_id}/{variant_id}")
        raw_context = _finite_or_none(source[0])
        benchmark_metadata = json.loads(source[4])
        baseline_period = int(
            benchmark_metadata.get(
                "r_period", benchmark_metadata.get("official_seasonality")
            )
        )
        fold_records = []
        for fold in coordinator.connection.execute(
            """SELECT fold_id, fold_number, train_start, train_end,
                      validation_start, validation_end, horizon,
                      raw_training_hash, prepared_training_hash,
                      prepared_training, preprocessing_id,
                      transformation_method, preparation_metadata
               FROM seasonal_tuning_folds
               WHERE experiment_id=? AND variant_id=? AND forecast_instance_id=?
               ORDER BY fold_number""",
            [experiment_id, variant_id, instance_id],
        ).fetchall():
            metadata = json.loads(fold[12])
            fold_records.append(
                {
                    "fold_id": fold[0],
                    "fold_number": fold[1],
                    "train_start": fold[2],
                    "train_end": fold[3],
                    "validation_start": fold[4],
                    "validation_end": fold[5],
                    "horizon": fold[6],
                    "raw_training_hash": fold[7],
                    "prepared_training_hash": fold[8],
                    "context": list(fold[9]),
                    "preprocessing_id": fold[10],
                    "transformation_method": fold[11],
                    "parameters": metadata["transformation_parameters"],
                    "actual": raw_context[fold[4] : fold[5]],
                    "preparation_metadata": metadata,
                }
            )
        candidates: dict[str, dict[str, dict[str, Any]]] = {}
        for candidate in coordinator.connection.execute(
            """SELECT c.candidate_id, f.fold_id, f.fold_number, c.model,
                      c.baseline_period, c.estimated_period, c.seasonal_strength,
                      c.eligible, c.reason, c.package_versions
               FROM seasonal_period_candidates c
               JOIN seasonal_tuning_folds f USING (fold_id)
               WHERE f.experiment_id=? AND f.variant_id=?
                 AND f.forecast_instance_id=?""",
            [experiment_id, variant_id, instance_id],
        ).fetchall():
            candidates.setdefault(candidate[3], {})[str(candidate[2])] = {
                "candidate_id": candidate[0],
                "fold_id": candidate[1],
                "model": candidate[3],
                "baseline_period": candidate[4],
                "estimated_period": candidate[5],
                "seasonal_strength": candidate[6],
                "eligible": candidate[7],
                "reason": candidate[8],
                "package_versions": json.loads(candidate[9]),
            }
        validations: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}
        for validation in coordinator.connection.execute(
            """SELECT v.validation_id, v.candidate_id, f.fold_number, v.model,
                      v.policy, v.requested_period, v.executed_period, v.prediction,
                      v.validation_actual, v.mae, v.valid_label_count,
                      v.requested_method_id, v.executed_method_id, v.fallback_used,
                      v.status, v.reason, v.execution_metadata, v.content_hash
               FROM seasonal_tuning_validations v
               JOIN seasonal_period_candidates c USING (candidate_id)
               JOIN seasonal_tuning_folds f USING (fold_id)
               WHERE f.experiment_id=? AND f.variant_id=?
                 AND f.forecast_instance_id=?""",
            [experiment_id, variant_id, instance_id],
        ).fetchall():
            validations.setdefault(validation[3], {}).setdefault(
                str(validation[2]), {}
            )[validation[4]] = {
                "validation_id": validation[0],
                "candidate_id": validation[1],
                "model": validation[3],
                "policy": validation[4],
                "requested_period": validation[5],
                "executed_period": validation[6],
                "prediction": list(validation[7]),
                "actual": _finite_or_none(validation[8]),
                "mae": validation[9],
                "valid_label_count": validation[10],
                "requested_method_id": validation[11],
                "executed_method_id": validation[12],
                "fallback_used": validation[13],
                "status": validation[14],
                "reason": validation[15],
                "execution_metadata": json.loads(validation[16]),
                "content_hash": validation[17],
            }
        for task_row in model_rows:
            group_id = f"{instance_id}/{variant_id}/{task_row[3]}"
            payloads.append(
                {
                    "group_id": group_id,
                    "id": group_id,
                    "experiment_id": experiment_id,
                    "instance_id": instance_id,
                    "variant_id": variant_id,
                    "raw_context": raw_context,
                    "horizon": int(source[1]),
                    "dataset_id": source[2],
                    "series_id": source[3],
                    "cleaning": source[5],
                    "transformation_method": source[6],
                    "full_prepared": list(source[7]),
                    "transformation_parameters": json.loads(source[8]),
                    "transformation_id": source[9],
                    "baseline_period": baseline_period,
                    "tuning": tuning,
                    "folds": fold_records,
                    "candidates": candidates,
                    "validations": validations,
                    "tasks": [
                        {
                            "task_id": task_row[0],
                            "model": task_row[3],
                            "settings": coordinator.configuration.r_model_settings(
                                task_row[3]
                            ),
                        }
                    ],
                }
            )

    expected_tasks = {row[0] for row in rows}
    completed_tasks: set[str] = set()
    paths = coordinator.configuration.execution_paths
    timeout = float(
        coordinator.configuration.execution["worker_timeouts_seconds"]["r"]
    )
    threads = int(coordinator.configuration.execution["thread_limits"]["r"])
    worker_arguments = (
        paths["r_preprocess_worker"],
        paths["r_forecast_worker"],
        timeout,
        threads,
        {
            "Darwin": float(profile.dask_mac_memory_min_available_gib),
            "Linux": float(profile.dask_ubuntu_memory_min_available_gib),
        },
        {
            "auto_arima": float(profile.dask_autoarima_fit_budget_gib),
            "ets": float(profile.dask_ets_fit_budget_gib),
        },
        {
            "admission_timeout_seconds": float(
                profile.dask_memory_admission_timeout_seconds
            ),
            "poll_interval_seconds": float(profile.dask_memory_poll_interval_seconds),
            "breach_grace_seconds": float(profile.dask_memory_breach_grace_seconds),
            "swap_growth_limit_gib": float(profile.dask_swap_growth_limit_gib),
        },
    )
    responses = run_batch_groups(
        dask_client,
        distributed_tuning_queue_groups(payloads, profile, worker_arguments),
        retries=settings.dask_retries,
    )
    group_count = len(payloads)
    finished_groups = 0
    for host_group, batch, response in responses:
        if isinstance(response, BaseException):
            raise response
        if len(batch) != 1 or len(response.get("results", [])) != 1:
            raise RuntimeError("seasonal tuning worker returned an invalid group response")
        payload = batch[0]
        result = response["results"][0]
        if result.get("group_id") != payload["group_id"]:
            raise RuntimeError("seasonal tuning worker returned a mismatched group ID")
        returned_tasks = {model["task_id"] for model in result["models"]}
        submitted_tasks = {task["task_id"] for task in payload["tasks"]}
        if returned_tasks != submitted_tasks or completed_tasks.intersection(returned_tasks):
            raise RuntimeError("seasonal tuning worker returned unexpected or duplicate task IDs")
        for model_result in result["models"]:
            task_id = model_result["task_id"]
            forecast = model_result["forecast"]
            selection = model_result["selection"]
            execution_metadata = {
                "packages": forecast["packages"],
                "forecast_method": forecast["forecast_method"],
                "seasonal_period_tuning": {
                    "selection_id": selection["selection_id"],
                    "selected_policy": selection["selected_policy"],
                    "preprocessing_period": selection["baseline_period"],
                    "final_model_period": selection["final_period"],
                    "final_diagnostic": forecast["final_diagnostic"],
                },
                "runtime_seconds": forecast["runtime_seconds"],
                "execution_backend": "Dask Distributed bounded R/CPU seasonal tuning",
                "worker": response["worker"],
            }

            def insert(
                payload=payload,
                result=result,
                model_result=model_result,
                forecast=forecast,
                selection=selection,
                execution_metadata=execution_metadata,
            ) -> None:
                for fold in result["new_folds"]:
                    coordinator.connection.execute(
                        """INSERT INTO seasonal_tuning_folds VALUES
                           (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                           ON CONFLICT (fold_id) DO NOTHING""",
                        [
                            fold["fold_id"], experiment_id, payload["variant_id"],
                            payload["instance_id"], fold["fold_number"],
                            fold["train_start"], fold["train_end"],
                            fold["validation_start"], fold["validation_end"],
                            fold["horizon"], fold["raw_training_hash"],
                            fold["prepared_training_hash"], fold["context"],
                            fold["preprocessing_id"], fold["transformation_method"],
                            canonical_json(fold["preparation_metadata"]),
                        ],
                    )
                for candidate in model_result["candidates"]:
                    coordinator.connection.execute(
                        """INSERT INTO seasonal_period_candidates VALUES
                           (?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                           ON CONFLICT (candidate_id) DO NOTHING""",
                        [
                            candidate["candidate_id"], candidate["fold_id"],
                            candidate["model"], candidate["baseline_period"],
                            candidate["estimated_period"], candidate["seasonal_strength"],
                            candidate["eligible"], candidate["reason"],
                            canonical_json(candidate["package_versions"]),
                        ],
                    )
                for validation in model_result["validations"]:
                    coordinator.connection.execute(
                        """INSERT INTO seasonal_tuning_validations VALUES
                           (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                           ON CONFLICT (validation_id) DO NOTHING""",
                        [
                            validation["validation_id"], validation["candidate_id"],
                            validation["model"], validation["policy"],
                            validation["requested_period"], validation["executed_period"],
                            validation["prediction"], validation["actual"], validation["mae"],
                            validation["valid_label_count"], validation["requested_method_id"],
                            validation["executed_method_id"], validation["fallback_used"],
                            validation["status"], validation["reason"],
                            canonical_json(validation["execution_metadata"]),
                            validation["content_hash"],
                        ],
                    )
                coordinator.connection.execute(
                    """INSERT INTO seasonal_period_selections VALUES
                       (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                       ON CONFLICT (selection_id) DO NOTHING""",
                    [
                        selection["selection_id"], experiment_id, payload["variant_id"],
                        payload["instance_id"], model_result["model"],
                        selection["selected_policy"], selection["baseline_mean_mae"],
                        selection["estimated_mean_mae"], selection["status"],
                        selection["reason"], selection["baseline_period"],
                        selection["final_estimated_period"], selection["final_period"],
                        selection["final_substitution_reason"], canonical_json(tuning),
                    ],
                )
                coordinator.connection.execute(
                    """INSERT INTO forecasts
                       (forecast_id, experiment_id, variant_id, forecast_instance_id,
                        candidate, model_revision, parent_result_id, scale, mean,
                        median, quantile_levels, quantiles, runtime_seconds,
                        execution_metadata, content_hash, created_at,
                        forecast_capability)
                       VALUES (?, ?, ?, ?, ?, ?, ?, 'original', ?, ?, ?, ?, ?, ?, ?,
                               current_timestamp, 'probabilistic')
                       ON CONFLICT (forecast_id) DO NOTHING""",
                    [
                        forecast["forecast_id"], experiment_id, payload["variant_id"],
                        payload["instance_id"], model_result["model"],
                        forecast["packages"]["forecast"], payload["transformation_id"],
                        forecast["mean"], forecast["median"], list(coordinator.quantiles),
                        forecast["quantiles"], forecast["runtime_seconds"],
                        canonical_json(execution_metadata),
                        json_fingerprint(
                            {"mean": forecast["mean"], "quantiles": forecast["quantiles"]}
                        ),
                    ],
                )

            coordinator._commit_task(
                task_id,
                attempts[task_id],
                forecast["runtime_seconds"],
                insert,
                execution_metadata,
            )
            completed_tasks.add(task_id)
        finished_groups += 1
        print(
            f"seasonal tuning progress: {len(completed_tasks)}/{len(expected_tasks)} tasks "
            f"({finished_groups}/{group_count} groups), placement={host_group}, "
            f"latest host={response['worker']['hostname']}",
            file=sys.stderr,
            flush=True,
        )
    if completed_tasks != expected_tasks:
        raise RuntimeError(
            f"seasonal tuning completed {len(completed_tasks)} of {len(expected_tasks)} tasks"
        )


def run_tuned_forecasts(
    coordinator: Any,
    experiment_id: str,
    rows: list[tuple],
    attempts: dict[str, int],
) -> None:
    """Tune and commit all pending v3 Gate 4 AutoARIMA/ETS tasks.

    Python remains the sole DuckDB writer. Preparation is refitted from each raw
    training slice, validation actuals never enter R, and final rows use the normal
    ``forecasts`` contract so retrieval and Gate 6 remain unchanged.
    """
    tuning = coordinator.configuration.seasonal_period_tuning
    if tuning is None or not tuning["enabled"]:
        raise RuntimeError("seasonal period tuning is not enabled")
    grouped: dict[tuple[str, str], list[tuple]] = {}
    for row in rows:
        grouped.setdefault((row[1], row[2]), []).append(row)

    for (instance_id, variant_id), model_rows in grouped.items():
        (
            raw_context,
            horizon,
            dataset_id,
            series_id,
            benchmark_metadata,
            cleaning,
            transformation_method,
            full_prepared,
            transformation_parameters,
            transformation_id,
        ) = coordinator.connection.execute(
            """SELECT i.context_target, i.horizon, i.dataset_id, i.series_id,
                      b.metadata, v.cleaning_method, v.transformation_method,
                      t.transformed_target, t.parameters, t.transformation_id
               FROM forecast_instances i
               JOIN benchmark_configurations b USING (benchmark_configuration_id)
               JOIN experiment_variants v ON v.variant_id=?
               JOIN transformed_series t ON t.experiment_id=v.experiment_id
                    AND t.variant_id=v.variant_id
                    AND t.forecast_instance_id=i.forecast_instance_id
               WHERE i.forecast_instance_id=?""",
            [variant_id, instance_id],
        ).fetchone()
        raw_context = _finite_or_none(raw_context)
        metadata = json.loads(benchmark_metadata)
        baseline_period = int(metadata.get("r_period", metadata.get("official_seasonality")))
        folds = historical_folds(len(raw_context), int(horizon), tuning["validation_windows"])

        prepared_folds: list[dict[str, Any]] = []
        stored_folds = coordinator.connection.execute(
            """SELECT fold_id, fold_number, train_start, train_end,
                      validation_start, validation_end, prepared_training,
                      preparation_metadata
               FROM seasonal_tuning_folds
               WHERE experiment_id=? AND variant_id=? AND forecast_instance_id=?
               ORDER BY fold_number""",
            [experiment_id, variant_id, instance_id],
        ).fetchall()
        if folds and len(stored_folds) == len(folds):
            for row in stored_folds:
                preparation_metadata = json.loads(row[7])
                prepared_folds.append(
                    {
                        "fold": HistoricalFold(row[1], row[2], row[3], row[4], row[5]),
                        "fold_id": row[0],
                        "context": list(row[6]),
                        "parameters": preparation_metadata["transformation_parameters"],
                        "actual": raw_context[row[4] : row[5]],
                    }
                )
        elif folds:
            preprocess_jobs = [
                {
                    "id": f"{instance_id}/{variant_id}/fold/{fold.number}",
                    "context": raw_context[fold.train_start : fold.train_end],
                    "mode": cleaning,
                    "seasonality": baseline_period,
                }
                for fold in folds
            ]
            preprocess_response = coordinator._r_worker(
                {"action": "preprocess", "jobs": preprocess_jobs}
            )
            cleaned = _worker_results(
                preprocess_response, {job["id"] for job in preprocess_jobs}
            )
            for fold, job in zip(folds, preprocess_jobs, strict=True):
                cleaned_values = [float(value) for value in cleaned[job["id"]]["values"]]
                transformed = transform(cleaned_values, transformation_method)
                fold_id = f"tuning-fold/{json_fingerprint({'experiment': experiment_id, 'variant': variant_id, 'instance': instance_id, 'fold': fold.number, 'tuning': tuning})[:32]}"
                preparation_id = f"fold-preparation/{json_fingerprint({'fold': fold_id, 'raw': raw_context[fold.train_start:fold.train_end]})[:32]}"
                coordinator.connection.execute(
                    """INSERT INTO seasonal_tuning_folds VALUES
                       (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                       ON CONFLICT (fold_id) DO NOTHING""",
                    [
                        fold_id,
                        experiment_id,
                        variant_id,
                        instance_id,
                        fold.number,
                        fold.train_start,
                        fold.train_end,
                        fold.validation_start,
                        fold.validation_end,
                        horizon,
                        json_fingerprint(raw_context[fold.train_start : fold.train_end]),
                        json_fingerprint(transformed.values),
                        list(transformed.values),
                        preparation_id,
                        transformation_method,
                        canonical_json(
                            {
                                "cleaning": cleaning,
                                "cleaning_period": baseline_period,
                                "cleaning_packages": preprocess_response["packages"],
                                "transformation_parameters": transformed.parameters,
                                "validation_actual_role": "QA scoring data; never model input",
                            }
                        ),
                    ],
                )
                prepared_folds.append(
                    {
                        "fold": fold,
                        "fold_id": fold_id,
                        "context": list(transformed.values),
                        "parameters": transformed.parameters,
                        "actual": raw_context[fold.validation_start : fold.validation_end],
                    }
                )

            # ``findfrequency`` and seasonal strength depend only on the prepared
            # fold, so compute them once and reuse them across AutoARIMA and ETS.
            # Model-specific support is applied below without repeating analysis.
            for prepared in prepared_folds:
                stored_diagnostic = coordinator.connection.execute(
                    """SELECT estimated_period, seasonal_strength, package_versions
                       FROM seasonal_period_candidates WHERE fold_id=?
                       ORDER BY model LIMIT 1""",
                    [prepared["fold_id"]],
                ).fetchone()
                if stored_diagnostic is not None:
                    estimated, strength, packages = stored_diagnostic
                    eligible = (
                        estimated is not None
                        and len(prepared["context"])
                        >= tuning["minimum_cycles"] * int(estimated)
                        and int(estimated) <= 350
                    )
                    prepared["shared_diagnostic"] = {
                        "estimated_period": estimated,
                        "seasonal_strength": strength,
                        "eligible": eligible,
                        "reason": None if eligible else "stored estimate is ineligible",
                    }
                    prepared["diagnostic_packages"] = json.loads(packages)
                else:
                    diagnostic_id = f"{prepared['fold_id']}/shared-diagnostic"
                    diagnostic_response = coordinator._r_worker(
                        {
                            "action": "diagnose_period",
                            "jobs": [
                                {
                                    "id": diagnostic_id,
                                    "context": prepared["context"],
                                    "baseline_period": baseline_period,
                                    "model": "auto_arima",
                                    "minimum_cycles": tuning["minimum_cycles"],
                                }
                            ],
                        }
                    )
                    prepared["shared_diagnostic"] = _worker_results(
                        diagnostic_response, {diagnostic_id}
                    )[diagnostic_id]
                    prepared["diagnostic_packages"] = diagnostic_response["packages"]

        for task_id, _, _, model in model_rows:
            if model not in MODEL_METHODS:
                raise RuntimeError(f"unsupported tuned model: {model}")
            model_started = time.monotonic()
            stored_validation_count = coordinator.connection.execute(
                """SELECT count(*) FROM seasonal_tuning_validations validation
                   JOIN seasonal_period_candidates candidate USING (candidate_id)
                   JOIN seasonal_tuning_folds fold USING (fold_id)
                   WHERE fold.experiment_id=? AND fold.variant_id=?
                     AND fold.forecast_instance_id=? AND candidate.model=?""",
                [experiment_id, variant_id, instance_id, model],
            ).fetchone()[0]
            previous_attempt_error = None
            if attempts[task_id] > 1:
                previous_attempt_error = coordinator.connection.execute(
                    """SELECT error FROM experiment_task_attempts
                       WHERE task_id=? AND attempt_number=?""",
                    [task_id, attempts[task_id] - 1],
                ).fetchone()[0]
            resumed_after_final_timeout = (
                attempts[task_id] > 1
                and stored_validation_count == 2 * len(prepared_folds)
                and previous_attempt_error is not None
                and "TimeoutExpired" in previous_attempt_error
            )
            baseline_scores: list[float | None] = []
            estimated_scores: list[float | None] = []
            baseline_valid: list[bool] = []
            estimated_valid: list[bool] = []

            for prepared in prepared_folds:
                fold = prepared["fold"]
                candidate_id = f"period-candidate/{json_fingerprint({'fold': prepared['fold_id'], 'model': model})[:32]}"
                stored = coordinator.connection.execute(
                    """SELECT estimated_period, seasonal_strength, eligible, reason
                       FROM seasonal_period_candidates WHERE candidate_id=?""",
                    [candidate_id],
                ).fetchone()
                if stored is None:
                    diagnostic = prepared["shared_diagnostic"]
                    estimated_period = diagnostic.get("estimated_period")
                    seasonal_strength = diagnostic.get("seasonal_strength")
                    eligible = bool(diagnostic["eligible"])
                    reason = diagnostic.get("reason")
                    if (
                        eligible
                        and model == "ets"
                        and estimated_period is not None
                        and int(estimated_period) > 24
                    ):
                        eligible = False
                        reason = f"model ets does not support period {estimated_period}"
                    coordinator.connection.execute(
                        """INSERT INTO seasonal_period_candidates VALUES
                           (?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                           ON CONFLICT (candidate_id) DO NOTHING""",
                        [
                            candidate_id,
                            prepared["fold_id"],
                            model,
                            baseline_period,
                            estimated_period,
                            seasonal_strength,
                            eligible,
                            reason,
                            canonical_json(prepared["diagnostic_packages"]),
                        ],
                    )
                else:
                    estimated_period, seasonal_strength, eligible, reason = stored

                policies = {
                    "baseline": (baseline_period, baseline_period, True, None),
                    "estimated": (
                        estimated_period,
                        int(estimated_period) if eligible else baseline_period,
                        bool(eligible),
                        reason,
                    ),
                }
                computed: dict[int, dict[str, Any]] = {}
                for policy, (requested_period, executed_period, policy_eligible, policy_reason) in policies.items():
                    validation_id = f"tuning-validation/{json_fingerprint({'candidate': candidate_id, 'policy': policy, 'settings': coordinator.configuration.r_model_settings(model)})[:32]}"
                    existing = coordinator.connection.execute(
                        """SELECT mae, valid_label_count, status FROM seasonal_tuning_validations
                           WHERE validation_id=?""",
                        [validation_id],
                    ).fetchone()
                    if existing is None:
                        if executed_period not in computed:
                            forecast_identifier = f"{validation_id}/forecast"
                            try:
                                response = coordinator._r_worker(
                                    {
                                        "action": "forecast",
                                        "jobs": [
                                            _forecast_job(
                                                forecast_identifier,
                                                dataset_id,
                                                series_id,
                                                model,
                                                prepared["context"],
                                                horizon,
                                                executed_period,
                                                coordinator.configuration.r_model_settings(model),
                                            )
                                        ],
                                    }
                                )
                                result = _worker_results(response, {forecast_identifier})[
                                    forecast_identifier
                                ]
                                result["packages"] = response["packages"]
                            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
                                # A bounded external-worker failure is scientific
                                # evidence for this policy, not a reason to discard
                                # every other completed series in the invocation.
                                result = {
                                    "worker_error": f"{type(error).__name__}: {error}",
                                    "requested_method_id": MODEL_METHODS[model],
                                    "executed_method_id": MODEL_METHODS[model],
                                    "fallback_used": False,
                                    "fallback_reason": None,
                                    "packages": {},
                                    "provenance": {},
                                }
                            computed[executed_period] = result
                        result = computed[executed_period]
                        if "worker_error" in result:
                            prediction = []
                            mae = None
                            valid_count = sum(
                                value is not None and math.isfinite(float(value))
                                for value in prepared["actual"]
                            )
                            status = "failed"
                            status_reason = result["worker_error"]
                        else:
                            prediction = list(
                                inverse(
                                    result["mean"],
                                    transformation_method,
                                    prepared["parameters"],
                                )
                            )
                            mae, valid_count = original_scale_mae(
                                prediction, prepared["actual"]
                            )
                        if "worker_error" in result:
                            pass
                        elif valid_count == 0:
                            status = "all_missing"
                            status_reason = "validation window has no finite labels"
                        elif result["fallback_used"]:
                            status = "fallback"
                            status_reason = result["fallback_reason"]
                        elif policy == "estimated" and not policy_eligible:
                            status = "substituted"
                            status_reason = policy_reason
                        else:
                            status = "success"
                            status_reason = None
                        coordinator.connection.execute(
                            """INSERT INTO seasonal_tuning_validations VALUES
                               (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                               ON CONFLICT (validation_id) DO NOTHING""",
                            [
                                validation_id,
                                candidate_id,
                                model,
                                policy,
                                requested_period,
                                executed_period,
                                prediction,
                                prepared["actual"],
                                mae,
                                valid_count,
                                result["requested_method_id"],
                                result["executed_method_id"],
                                result["fallback_used"],
                                status,
                                status_reason,
                                canonical_json(
                                    {
                                        "packages": result["packages"],
                                        "provenance": result["provenance"],
                                        "deduplicated_equal_period": policy == "estimated"
                                        and executed_period == baseline_period,
                                    }
                                ),
                                json_fingerprint(
                                    {
                                        "prediction": prediction,
                                        "actual": prepared["actual"],
                                        "period": executed_period,
                                    }
                                ),
                            ],
                        )
                    else:
                        mae, valid_count, status = existing
                    score_list = baseline_scores if policy == "baseline" else estimated_scores
                    valid_list = baseline_valid if policy == "baseline" else estimated_valid
                    score_list.append(None if mae is None else float(mae))
                    valid_list.append(status == "success")

            selection = select_period_policy(
                baseline_scores, estimated_scores, baseline_valid, estimated_valid
            )
            final_estimated_period = None
            final_period = baseline_period
            final_substitution_reason = None
            final_diagnostic = None
            if selection["selected_policy"] == "estimated":
                diagnostic_id = f"{task_id}/final-period"
                response = coordinator._r_worker(
                    {
                        "action": "diagnose_period",
                        "jobs": [
                            {
                                "id": diagnostic_id,
                                "context": full_prepared,
                                "baseline_period": baseline_period,
                                "model": model,
                                "minimum_cycles": tuning["minimum_cycles"],
                            }
                        ],
                    }
                )
                final_diagnostic = _worker_results(response, {diagnostic_id})[diagnostic_id]
                final_estimated_period = final_diagnostic.get("estimated_period")
                if final_diagnostic["eligible"]:
                    final_period = int(final_estimated_period)
                else:
                    final_substitution_reason = final_diagnostic.get("reason")
                    selection = {
                        **selection,
                        "selected_policy": "baseline",
                        "status": "inconclusive",
                        "reason": "estimated policy won validation but its final-history period was ineligible",
                    }
                if resumed_after_final_timeout and final_period != baseline_period:
                    final_substitution_reason = (
                        "estimated final-period forecast exceeded the configured R worker "
                        "timeout on the previous attempt"
                    )
                    final_period = baseline_period
                    selection = {
                        **selection,
                        "selected_policy": "baseline",
                        "status": "inconclusive",
                        "reason": final_substitution_reason,
                    }

            final_identifier = f"{task_id}/final"
            final_payload = {
                "action": "forecast",
                "jobs": [
                    _forecast_job(
                        final_identifier,
                        dataset_id,
                        series_id,
                        model,
                        list(full_prepared),
                        horizon,
                        final_period,
                        coordinator.configuration.r_model_settings(model),
                    )
                ],
            }
            try:
                final_response = coordinator._r_worker(final_payload)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
                if final_period == baseline_period:
                    raise
                final_substitution_reason = (
                    f"estimated final-period forecast failed; baseline used: "
                    f"{type(error).__name__}: {error}"
                )
                final_period = baseline_period
                selection = {
                    **selection,
                    "selected_policy": "baseline",
                    "status": "inconclusive",
                    "reason": final_substitution_reason,
                }
                final_payload["jobs"][0]["model_period"] = baseline_period
                final_response = coordinator._r_worker(final_payload)
            final = _worker_results(final_response, {final_identifier})[final_identifier]
            parameters = json.loads(transformation_parameters)
            mean = list(inverse(final["mean"], transformation_method, parameters))
            median = list(inverse(final["median"], transformation_method, parameters))
            quantiles = [
                list(inverse(values, transformation_method, parameters))
                for values in final["quantiles"]
            ]
            selection_id = f"period-selection/{json_fingerprint({'experiment': experiment_id, 'variant': variant_id, 'instance': instance_id, 'model': model, 'tuning': tuning})[:32]}"
            forecast_id = f"forecast/{json_fingerprint({'experiment': experiment_id, 'variant': variant_id, 'instance': instance_id, 'candidate': model})[:32]}"
            execution_metadata = {
                "packages": final_response["packages"],
                "forecast_method": {
                    "requested_method_id": final["requested_method_id"],
                    "executed_method_id": final["executed_method_id"],
                    "fallback_used": final["fallback_used"],
                    "fallback_reason": final["fallback_reason"],
                    "provenance": final["provenance"],
                },
                "seasonal_period_tuning": {
                    "selection_id": selection_id,
                    "selected_policy": selection["selected_policy"],
                    "preprocessing_period": baseline_period,
                    "final_model_period": final_period,
                    "final_diagnostic": final_diagnostic,
                },
                "runtime_seconds": time.monotonic() - model_started,
                "execution_backend": "bounded R/CPU seasonal tuning",
            }

            def insert() -> None:
                coordinator.connection.execute(
                    """INSERT INTO seasonal_period_selections VALUES
                       (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                       ON CONFLICT (selection_id) DO NOTHING""",
                    [
                        selection_id,
                        experiment_id,
                        variant_id,
                        instance_id,
                        model,
                        selection["selected_policy"],
                        selection["baseline_mean_mae"],
                        selection["estimated_mean_mae"],
                        selection["status"],
                        selection["reason"],
                        baseline_period,
                        final_estimated_period,
                        final_period,
                        final_substitution_reason,
                        canonical_json(tuning),
                    ],
                )
                coordinator.connection.execute(
                    """INSERT INTO forecasts
                       (forecast_id, experiment_id, variant_id, forecast_instance_id,
                        candidate, model_revision, parent_result_id, scale, mean,
                        median, quantile_levels, quantiles, runtime_seconds,
                        execution_metadata, content_hash, created_at,
                        forecast_capability)
                       VALUES (?, ?, ?, ?, ?, ?, ?, 'original', ?, ?, ?, ?, ?, ?, ?,
                               current_timestamp, 'probabilistic')
                       ON CONFLICT (forecast_id) DO NOTHING""",
                    [
                        forecast_id,
                        experiment_id,
                        variant_id,
                        instance_id,
                        model,
                        final_response["packages"]["forecast"],
                        transformation_id,
                        mean,
                        median,
                        list(coordinator.quantiles),
                        quantiles,
                        execution_metadata["runtime_seconds"],
                        canonical_json(execution_metadata),
                        json_fingerprint({"mean": mean, "quantiles": quantiles}),
                    ],
                )

            coordinator._commit_task(
                task_id,
                attempts[task_id],
                execution_metadata["runtime_seconds"],
                insert,
                execution_metadata,
            )
