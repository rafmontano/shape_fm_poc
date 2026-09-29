#!/usr/bin/env Rscript
# ==============================================================================
# 04_01_forecast_auto_arima.R
#
# Purpose: Serve the bounded R subprocess that produces AutoARIMA forecasts.
# Inputs: JSON on stdin: action="forecast", authoritative AutoARIMA settings,
#   and jobs with id, numeric context, seasonal frequency, and horizon.
# Outputs: JSON on stdout with same-scale forecasts and package versions;
#   invalid settings, actions, or series terminate the subprocess with an R error.
# Run from: printf '%s' '{"action":"forecast","settings":{},"jobs":[]}' | Rscript src/r/04_01_forecast_auto_arima.R
# ==============================================================================

suppressPackageStartupMessages({
  library(forecast)
  library(jsonlite)
})

source("src/r/util/time_series_input.R")

# Execution global: payload is the coordinator-authored request for this subprocess;
# scientific and execution values remain authoritative from DuckDB and have no local override.
payload <- jsonlite::fromJSON(file("stdin"), simplifyVector = FALSE)

# Purpose: Fit AutoARIMA to one univariate context and forecast its horizon.
# Inputs: job has id, finite numeric context of length n, positive integer
#   seasonality/frequency, and positive integer horizon h; settings supplies
#   coordinator-authoritative AutoARIMA booleans, core count, and interval levels.
# Outputs: List with id; h same-scale mean and median values; and nine h-value
#   quantile vectors ordered 0.1 through 0.9. Writes nothing to stdout; fitting,
#   malformed input, or unavailable expected 20/40/60/80% intervals raises an R error.
forecast_one <- function(job, settings) {
  input <- time_series_input(job)
  fit <- forecast::auto.arima(
    input$series,
    stepwise = isTRUE(settings$stepwise),
    approximation = isTRUE(settings$approximation),
    allowdrift = isTRUE(settings$allowdrift),
    allowmean = isTRUE(settings$allowmean),
    parallel = isTRUE(settings$parallel),
    num.cores = as.integer(settings$num_cores)
  )
  predicted <- forecast::forecast(
    fit,
    h = as.integer(job$horizon),
    level = as.numeric(unlist(settings$interval_levels))
  )
  mean <- as.numeric(predicted$mean)
  quantiles <- list(
    as.numeric(predicted$lower[, "80%"]),
    as.numeric(predicted$lower[, "60%"]),
    as.numeric(predicted$lower[, "40%"]),
    as.numeric(predicted$lower[, "20%"]),
    mean,
    as.numeric(predicted$upper[, "20%"]),
    as.numeric(predicted$upper[, "40%"]),
    as.numeric(predicted$upper[, "60%"]),
    as.numeric(predicted$upper[, "80%"])
  )
  list(id = job$id, mean = mean, median = mean, quantiles = quantiles)
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
