# Shared Process 02–04 provider input: convert univariate historical values and
# the coordinator-resolved R period into same-scale numeric/stats::ts objects.
# Conversion does not infer seasonality, interpolate or rescale observations.

# Construct stats::ts(values, start=1, frequency=period), preserving values.
# Reject empty/infinite inputs, invalid periods and disallowed missing values.
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

# Convert job$context (a JSON numeric/missing array) and job$seasonality (the
# resolved R period) to a list of numeric values and stats::ts; null becomes NA.
# Malformed input fails at this provider boundary; nothing is written.
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
