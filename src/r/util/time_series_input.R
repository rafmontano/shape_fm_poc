# ==============================================================================
# time_series_input.R
#
# Purpose: Construct the shared numeric vector and stats::ts input for R workers.
# Inputs: Worker job with a univariate numeric context and positive integer seasonality.
# Outputs: List containing the length-n numeric values and same-scale, frequency-tagged
#   stats::ts series; malformed values or frequency can raise an R conversion error.
# Run from: Imported; not run directly.
# ==============================================================================

# Purpose: Normalize one worker context and attach its seasonal frequency.
# Inputs: job$context is a length-n univariate numeric sequence in the source
#   measurement scale; job$seasonality is its positive integer observations-per-cycle.
# Outputs: List with a length-n numeric vector and same-scale stats::ts (start 1,
#   frequency seasonality); writes nothing and conversion/invalid frequency errors propagate.
time_series_input <- function(job) {
  values <- as.numeric(unlist(job$context))
  list(
    values = values,
    series = stats::ts(values, frequency = as.integer(job$seasonality))
  )
}
