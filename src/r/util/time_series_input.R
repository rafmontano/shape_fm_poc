# ==============================================================================
# time_series_input.R
#
# Purpose: Construct the shared numeric vector and stats::ts input for R workers.
# Inputs: Worker job with a univariate numeric/missing context and positive seasonality.
# Outputs: List containing the length-n numeric values and same-scale, frequency-tagged
#   stats::ts series; infinities and malformed values fail with a clear error.
# Run from: Imported; not run directly.
# ==============================================================================

# Purpose: Normalize one worker context and attach its seasonal frequency.
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
  seasonality <- suppressWarnings(as.integer(job$seasonality))
  if (length(seasonality) != 1L || is.na(seasonality) || seasonality < 1L) {
    stop("seasonality must be a positive integer", call. = FALSE)
  }
  list(
    values = values,
    series = stats::ts(values, frequency = seasonality)
  )
}
