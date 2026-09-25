#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(forecast)
  library(jsonlite)
})

payload <- jsonlite::fromJSON(file("stdin"), simplifyVector = FALSE)

clean_one <- function(job) {
  values <- as.numeric(unlist(job$context))
  series <- stats::ts(values, frequency = as.integer(job$seasonality))
  if (identical(job$method, "identity")) {
    cleaned <- values
  } else if (identical(job$method, "tsclean")) {
    cleaned <- as.numeric(forecast::tsclean(series))
  } else {
    stop("Unsupported cleaning method: ", job$method)
  }
  list(id = job$id, values = cleaned)
}

forecast_one <- function(job) {
  values <- as.numeric(unlist(job$context))
  series <- stats::ts(values, frequency = as.integer(job$seasonality))
  fit <- forecast::auto.arima(
    series,
    stepwise = TRUE,
    approximation = FALSE,
    allowdrift = TRUE,
    allowmean = TRUE,
    parallel = FALSE
  )
  predicted <- forecast::forecast(fit, h = as.integer(job$horizon), level = c(20, 40, 60, 80))
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

if (identical(payload$action, "clean")) {
  results <- lapply(payload$jobs, clean_one)
} else if (identical(payload$action, "forecast")) {
  results <- lapply(payload$jobs, forecast_one)
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
