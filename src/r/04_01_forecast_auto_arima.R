#!/usr/bin/env Rscript
# ==============================================================================
# 04_01_forecast_auto_arima.R
#
# Purpose: Serve the bounded R subprocess that produces AutoARIMA forecasts.
# Inputs: JSON on stdin: action="forecast", authoritative AutoARIMA settings,
#   and jobs with id, numeric context, seasonal frequency, and horizon.
# Outputs: JSON on stdout with same-scale forecasts, fallback provenance, and package versions;
#   invalid settings, actions, or series terminate the subprocess with an R error.
# Run from: printf '%s' '{"action":"forecast","settings":{},"jobs":[]}' | Rscript src/r/04_01_forecast_auto_arima.R
# ==============================================================================

suppressPackageStartupMessages({
  library(forecast)
  library(jsonlite)
})

source("src/r/util/forecast_methods.R")

# Execution global: payload is the coordinator-authored request for this subprocess;
# scientific and execution values remain authoritative from DuckDB and have no local override.
payload <- jsonlite::fromJSON(file("stdin"), simplifyVector = FALSE)

# Purpose: Fit AutoARIMA to one univariate context and forecast its horizon.
# Inputs: job has id, finite numeric context of length n, positive integer
#   seasonality/frequency, and positive integer horizon h; settings supplies
#   coordinator-authoritative AutoARIMA booleans, core count, and interval levels.
# Outputs: List with id; h same-scale mean and median values; and nine h-value
#   quantile vectors ordered 0.1 through 0.9, plus explicit fallback provenance.
#   Writes nothing to stdout; malformed input or terminal model/fallback failure raises.
forecast_one <- function(job, settings) {
  result <- run_forecast_method(list(
    task_id = job$id,
    dataset_id = job$dataset_id %||% job$id,
    series_id = job$series_id %||% job$id,
    context = job$context,
    horizon = job$horizon,
    frequency = job$seasonality,
    method_id = "auto_arima_forec",
    settings = settings,
    quantile_levels = seq(0.1, 0.9, by = 0.1)
  ))
  quantiles <- lapply(
    seq_len(nrow(result$quantiles)),
    function(row) as.numeric(result$quantiles[row, ])
  )
  list(
    id = job$id,
    mean = result$mean,
    median = result$median,
    quantiles = quantiles,
    requested_method_id = result$requested_method_id,
    executed_method_id = result$executed_method_id,
    fallback_used = result$fallback_used,
    fallback_reason = result$fallback_reason,
    provenance = result$provenance
  )
}

# Execution global: results preserves coordinator job order for this invocation;
# it is derived from stdin and has no independent override.
if (identical(payload$action, "forecast")) {
  if (is.null(payload$settings)) {
    stop("AutoARIMA settings are required")
  }
  results <- lapply(payload$jobs, forecast_one, settings = payload$settings)
} else {
  stop("Unsupported POC 1 worker action")
}

# Code constant: JSON encoding is the worker protocol—scalar unboxing, 17-digit
# numeric precision, and JSON null spelling are fixed by this implementation.
cat(jsonlite::toJSON(
  list(
    results = results,
    packages = list(
      R = R.version.string,
      forecast = as.character(utils::packageVersion("forecast")),
      jsonlite = as.character(utils::packageVersion("jsonlite"))
    )
  ),
  auto_unbox = TRUE,
  digits = 17,
  null = "null"
))
