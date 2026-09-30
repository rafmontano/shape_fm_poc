# ==============================================================================
# time_series_input.R
#
# Purpose: Construct the shared numeric vector and stats::ts input for R workers.
# Inputs: Worker job with a univariate numeric/missing context and positive seasonality.
# Outputs: List containing the length-n numeric values and same-scale, frequency-tagged
#   stats::ts series; infinities and malformed values fail with a clear error.
# Run from: Imported; not run directly.
# ==============================================================================

# Purpose: Construct the shared R model object without changing its values.
# Inputs: Numeric values and a positive integer period resolved by the coordinator.
# Outputs: A stats::ts with start 1 and the requested frequency; malformed periods,
#   infinities, or (when disallowed) missing values raise a clear error.
time_series_from_values <- function(values, period, allow_missing = TRUE) {
  values <- suppressWarnings(as.numeric(values))
  if (length(values) < 1L || any(is.infinite(values))) {
    stop("values must be a non-empty numeric vector without infinity", call. = FALSE)
  }
  if (!isTRUE(allow_missing) && any(is.na(values))) {
    stop("values must be finite for forecasting", call. = FALSE)
  }
  period <- suppressWarnings(as.integer(period))
  if (length(period) != 1L || is.na(period) || period < 1L) {
    stop("period must be a positive integer", call. = FALSE)
  }
  stats::ts(values, start = 1L, frequency = period)
}

# Purpose: Normalize one worker context and attach its resolved R period.
# Inputs: job$context is a length-n univariate numeric/missing sequence in the source
#   measurement scale; job$seasonality is its positive integer observations-per-cycle.
# Outputs: List with a length-n numeric vector and same-scale stats::ts (start 1,
#   frequency seasonality); writes nothing and conversion/invalid frequency errors propagate.
time_series_input <- function(job) {
  if (!is.list(job$context) || length(job$context) < 1L) {
    stop("context must be a non-empty JSON array", call. = FALSE)
  }
  values <- vapply(job$context, function(value) {
    if (is.null(value) || (length(value) == 1L && is.na(value))) {
      return(NA_real_)
    }
    converted <- suppressWarnings(as.numeric(value))
    if (length(converted) != 1L || is.na(converted)) {
      stop("context values must be numeric or missing", call. = FALSE)
    }
    converted
  }, numeric(1L))
  if (any(is.infinite(values))) {
    stop("context values cannot contain positive or negative infinity", call. = FALSE)
  }
  series <- time_series_from_values(values, job$seasonality, allow_missing = TRUE)
  list(
    values = values,
    series = series
  )
}
