#!/usr/bin/env Rscript
# Process 04 AutoARIMA provider (docs/forecast-black-box-contract.md).
# Input: stdin JSON action="forecast", approved settings and past-only jobs
# with context, period and horizon. Output: same-scale probabilistic forecasts
# and package provenance on stdout; invalid inputs/fitting failures propagate.

suppressPackageStartupMessages({
  library(forecast)
  library(jsonlite)
})

source("src/r/util/forecast_methods.R")

payload <- jsonlite::fromJSON(file("stdin"), simplifyVector = FALSE)

# Purpose: Fit AutoARIMA to one univariate context and forecast its horizon.
# Inputs: job has id, finite numeric context of length n, positive integer
#   seasonality/frequency, and positive integer horizon h; settings supplies
#   coordinator-authoritative AutoARIMA booleans, core count, and interval levels.
# Outputs: List with id; h same-scale mean and median values; and nine h-value
#   quantile vectors ordered 0.1 through 0.9, plus explicit fallback provenance.
#   Writes nothing to stdout; malformed input or fitting failure raises. AutoARIMA
#   has no scientific fallback under the forecast black-box contract.
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
    r_period = result$r_period,
    requested_method_id = result$requested_method_id,
    executed_method_id = result$executed_method_id,
    fallback_used = result$fallback_used,
    fallback_reason = result$fallback_reason,
    provenance = result$provenance
  )
}

if (identical(payload$action, "forecast")) {
  if (is.null(payload$settings)) {
    stop("AutoARIMA settings are required")
  }
  results <- lapply(payload$jobs, forecast_one, settings = payload$settings)
} else {
  stop("Unsupported POC 1 worker action")
}

# Preserve forecast precision and the JSON protocol's scalar/null representation.
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
