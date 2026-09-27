# ==============================================================================
# time_series_input.R
#
# Purpose: Construct the shared numeric vector and stats::ts input used by R workers.
# Inputs: A worker job containing numeric context and integer seasonality.
# Outputs: A list with numeric values and a frequency-tagged stats::ts series.
# Run from: Imported; not run directly.
# ==============================================================================

# Build the shared R-worker input; job supplies numeric context and seasonality, and cleaning and AutoARIMA callers receive raw values plus a frequency-tagged stats::ts series.
time_series_input <- function(job) {
  values <- as.numeric(unlist(job$context))
  list(
    values = values,
    series = stats::ts(values, frequency = as.integer(job$seasonality))
  )
}
