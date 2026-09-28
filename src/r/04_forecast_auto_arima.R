#!/usr/bin/env Rscript
# ==============================================================================
# 04_forecast_auto_arima.R
#
# Purpose: Fit AutoARIMA and return point and interval-derived quantile forecasts for worker jobs.
# Inputs: One JSON object on stdin with action, AutoARIMA settings, and forecast jobs.
# Outputs: One JSON object on stdout containing forecasts and R package versions.
# Run from: printf '%s' '<payload>' | Rscript src/r/04_forecast_auto_arima.R
# ==============================================================================

suppressPackageStartupMessages({
  library(forecast)
  library(jsonlite)
})

source("src/r/util/time_series_input.R")

# payload: decoded stdin request consumed by the action dispatcher below.
payload <- jsonlite::fromJSON(file("stdin"), simplifyVector = FALSE)

# Forecast one worker job with explicit settings; the dispatcher receives mean, median, and nine ordered quantile vectors.
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

# results: ordered forecast responses serialized to stdout.
if (identical(payload$action, "forecast")) {
  if (is.null(payload$settings)) {
    stop("AutoARIMA settings are required")
  }
  results <- lapply(payload$jobs, forecast_one, settings = payload$settings)
} else {
  stop("Unsupported POC 1 worker action")
}

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
