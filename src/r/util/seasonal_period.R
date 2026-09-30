# ==============================================================================
# seasonal_period.R
#
# Purpose: Estimate and diagnose one forecasting period without changing values.
# Inputs: A finite prepared numeric series, baseline period, model ID, and minimum cycles.
# Outputs: JSON-safe period, seasonal-strength, eligibility, and reason fields.
# Run from: Sourced by the bounded generic R forecast worker.
# ==============================================================================

# Purpose: State whether a registered model can represent a proposed period.
# Inputs: Internal model ID and positive integer period.
# Outputs: TRUE/FALSE. Limits mirror forecast-package model constraints.
model_supports_period <- function(model_id, period) {
  if (identical(model_id, "auto_arima")) {
    return(period <= 350L)
  }
  if (identical(model_id, "ets")) {
    return(period <= 24L)
  }
  stop("Unsupported period-diagnostic model: ", model_id, call. = FALSE)
}

# Purpose: Estimate one candidate policy and attach diagnostic evidence.
# Inputs: Finite prepared values, baseline period, model, and minimum cycle count.
# Outputs: Estimated period and eligibility. Seasonal strength is diagnostic only;
#   period 1 deliberately has no fabricated seasonal-strength value.
diagnose_seasonal_period <- function(values, baseline_period, model_id, minimum_cycles = 3L) {
  values <- suppressWarnings(as.numeric(values))
  if (length(values) < 1L || any(!is.finite(values))) {
    stop("period diagnostics require a non-empty finite prepared series", call. = FALSE)
  }
  baseline_period <- .positive_integer(baseline_period, "baseline_period")
  minimum_cycles <- .positive_integer(minimum_cycles, "minimum_cycles")
  estimated <- tryCatch(
    suppressWarnings(as.integer(round(forecast::findfrequency(values)))),
    error = function(error) NA_integer_
  )
  if (length(estimated) != 1L || is.na(estimated) || estimated < 1L) {
    return(list(
      baseline_period = baseline_period,
      estimated_period = NULL,
      seasonal_strength = NULL,
      eligible = FALSE,
      reason = "findfrequency did not return a positive integer"
    ))
  }
  reason <- NULL
  if (length(values) < minimum_cycles * estimated) {
    reason <- sprintf(
      "history has %d observations; period %d requires at least %d complete cycles (%d observations)",
      length(values), estimated, minimum_cycles, minimum_cycles * estimated
    )
  } else if (!model_supports_period(model_id, estimated)) {
    reason <- sprintf("model %s does not support period %d", model_id, estimated)
  }
  strength <- NULL
  if (estimated > 1L && is.null(reason)) {
    feature <- tryCatch(
      tsfeatures::tsfeatures(
        stats::ts(values, start = 1L, frequency = estimated),
        features = "stl_features"
      ),
      error = function(error) NULL
    )
    if (!is.null(feature)) {
      strength <- as.numeric(feature$seasonal_strength[[1L]])
    }
    if (is.null(strength) || length(strength) != 1L || !is.finite(strength)) {
      strength <- NULL
    }
  }
  list(
    baseline_period = baseline_period,
    estimated_period = estimated,
    seasonal_strength = strength,
    eligible = is.null(reason),
    reason = reason
  )
}
