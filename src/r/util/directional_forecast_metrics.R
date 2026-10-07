# ==============================================================================
# directional_forecast_metrics.R
#
# Purpose: Preserve five historical within-future-path directional metrics as a
#   dormant, model-neutral research capability, not canonical PT or live features.
# Inputs: Realized actuals and already calculated point means on a common scale.
# Outputs: Versioned in-memory R list; unavailable values include explicit reasons.
# Run from: Imported; not run directly.
# ==============================================================================

# Code constants: version the imported mathematics and its h - 1 comparisons.
.DIRECTIONAL_METRIC_SET_ID <- "legacy_directional_forecast_v1"
.DIRECTIONAL_COMPARISON_DEFINITION <- "within_future_path_changes_v1"

# Bounded functional exception: these pure scientific kernels have no durable
# state or side effects. The aggregator owns validation; kernels consume its contract.

# Purpose: Preserve historical sign accuracy, including matching zero changes.
# Inputs: Equal-length finite numeric paths; finite reward and penalty scalars.
# Outputs: Scalar (reward - penalty) * matching fraction + penalty.
legacy_mean_directional_accuracy <- function(actual, forecast, reward = 1, penalty = 0) {
  directional_hit <- sign(diff(actual)) == sign(diff(forecast))
  (reward - penalty) * mean(directional_hit) + penalty
}

# Purpose: Preserve signed absolute realized changes in original series units.
# Inputs: Equal-length finite numeric actual and point-forecast paths.
# Outputs: Mean magnitude, positive for sign matches and negative otherwise.
legacy_mean_directional_value <- function(actual, forecast) {
  actual_change <- diff(actual)
  directional_value <- ifelse(sign(actual_change) == sign(diff(forecast)), 1, -1)
  mean(abs(actual_change) * directional_value)
}

# Purpose: Preserve signed percentage changes using the destination actual.
# Inputs: Equal-length finite numeric actual and point-forecast paths.
# Outputs: Scalar percentage; NA_real_ when any destination actual is zero.
legacy_mean_directional_percentage_value <- function(actual, forecast) {
  if (any(actual[-1L] == 0)) return(NA_real_)
  actual_change <- diff(actual)
  directional_value <- ifelse(sign(actual_change) == sign(diff(forecast)), 1, -1)
  mean(abs(actual_change / actual[-1L]) * directional_value) * 100
}

# Purpose: Preserve the simplified historical PT statistic, not canonical PT.
# Inputs: Equal-length finite numeric paths; zero-direction pairs are removed.
# Outputs: Scalar z-style statistic; NA_real_ for empty or degenerate comparisons.
legacy_pt_statistic <- function(actual, forecast) {
  a <- sign(diff(actual))
  f <- sign(diff(forecast))
  keep <- is.finite(a) & is.finite(f) & a != 0 & f != 0
  a <- as.integer(a[keep] > 0)
  f <- as.integer(f[keep] > 0)
  if (length(a) == 0L) return(NA_real_)
  p <- mean(a)
  q <- mean(f)
  p_hat <- mean(a == f)
  p_0 <- p * q + (1 - p) * (1 - q)
  denominator <- sqrt(p * q * (1 - p) * (1 - q) / length(a))
  if (!is.finite(denominator) || denominator <= 0) return(NA_real_)
  (p_hat - p_0) / denominator
}

# Purpose: Preserve the historical two-sided normal p-value for legacy PT.
# Inputs: Equal-length finite numeric actual and point-forecast paths.
# Outputs: Scalar p-value; NA_real_ when the legacy statistic is unavailable.
legacy_pt_p_value <- function(actual, forecast) {
  z <- legacy_pt_statistic(actual, forecast)
  if (!is.finite(z)) return(NA_real_)
  2 * (1 - stats::pnorm(abs(z)))
}

# Validate an original-scale finite numeric vector of at least two observations.
.validate_directional_path <- function(path, name) {
  if (!is.numeric(path) || !is.null(dim(path)) || length(path) < 2L ||
      any(!is.finite(path))) {
    stop(sprintf("%s must be a finite numeric vector of length at least two", name),
         call. = FALSE)
  }
}

# Purpose: Calculate isolated retrospective metrics for arbitrary forecast candidates.
# Inputs: Finite actual path of length h >= 2, non-empty uniquely named list of
#   equal-length finite forecast means on the same caller-provided scale, and
#   finite scalar reward/penalty for MDA only.
# Outputs: Ordinary list with version IDs, h, h - 1, and named model results.
#   Each result has named values and unavailable reasons; NA_real_ must become
#   JSON null in any future adapter. Malformed inputs raise errors; writes nothing.
calculate_directional_forecast_metrics <- function(actual, forecasts, reward = 1, penalty = 0) {
  .validate_directional_path(actual, "actual")
  if (!is.list(forecasts) || length(forecasts) == 0L ||
      is.null(names(forecasts)) || anyNA(names(forecasts)) ||
      any(!nzchar(trimws(names(forecasts)))) || anyDuplicated(names(forecasts))) {
    stop("forecasts must be a non-empty list with non-empty unique names", call. = FALSE)
  }
  for (name in c("reward", "penalty")) {
    value <- if (name == "reward") reward else penalty
    if (!is.numeric(value) || length(value) != 1L || !is.null(dim(value)) ||
        !is.finite(value)) {
      stop(sprintf("%s must be a finite numeric scalar", name), call. = FALSE)
    }
  }
  models <- lapply(names(forecasts), function(name) {
    forecast <- forecasts[[name]]
    .validate_directional_path(forecast, sprintf("forecast '%s'", name))
    if (length(forecast) != length(actual)) {
      stop(sprintf("forecast '%s' must have the same length as actual", name), call. = FALSE)
    }
    values <- list(
      mda = legacy_mean_directional_accuracy(actual, forecast, reward, penalty),
      mdv = legacy_mean_directional_value(actual, forecast),
      mdpv = legacy_mean_directional_percentage_value(actual, forecast),
      legacy_pt_statistic = legacy_pt_statistic(actual, forecast),
      legacy_pt_p_value = legacy_pt_p_value(actual, forecast)
    )
    unavailable <- list()
    if (any(actual[-1L] == 0)) unavailable$mdpv <- "zero_destination_actual"
    if (is.na(values$legacy_pt_statistic)) {
      a <- sign(diff(actual))
      f <- sign(diff(forecast))
      reason <- if (!any(is.finite(a) & is.finite(f) & a != 0 & f != 0)) {
        "no_nonzero_direction_comparisons"
      } else {
        "non_positive_or_non_finite_pt_denominator"
      }
      unavailable$legacy_pt_statistic <- reason
      unavailable$legacy_pt_p_value <- reason
    }
    for (metric in names(values)) {
      if (!is.finite(values[[metric]])) {
        values[[metric]] <- NA_real_
        if (is.null(unavailable[[metric]])) unavailable[[metric]] <- "non_finite_result"
      }
    }
    list(values = values, unavailable = unavailable)
  })
  list(
    metric_set_id = .DIRECTIONAL_METRIC_SET_ID,
    comparison_definition = .DIRECTIONAL_COMPARISON_DEFINITION,
    actual_length = length(actual),
    comparison_count = length(actual) - 1L,
    models = stats::setNames(models, names(forecasts))
  )
}
