#!/usr/bin/env Rscript
# ==============================================================================
# 02_preprocess_series.R
#
# Purpose: Clean batches of time-series contexts with identity or forecast::tsclean.
# Inputs: One JSON object on stdin with action="clean" and jobs containing id, context, method, and seasonality.
# Outputs: One JSON object on stdout containing cleaned values and R package versions.
# Run from: printf '%s' '<payload>' | Rscript src/r/02_preprocess_series.R
# ==============================================================================

suppressPackageStartupMessages({
  library(forecast)
  library(jsonlite)
})

source("src/r/util/time_series_input.R")

# payload: decoded stdin request consumed by the action dispatcher below.
payload <- jsonlite::fromJSON(file("stdin"), simplifyVector = FALSE)

# Clean one worker job; job supplies id, numeric context, seasonality, and method, and the action dispatcher receives the id with cleaned values.
clean_one <- function(job) {
  input <- time_series_input(job)
  if (identical(job$method, "identity")) {
    cleaned <- input$values
  } else if (identical(job$method, "tsclean")) {
    cleaned <- as.numeric(forecast::tsclean(input$series))
  } else {
    stop("Unsupported cleaning method: ", job$method)
  }
  list(id = job$id, values = cleaned)
}

# results: ordered cleaning responses serialized to stdout.
if (identical(payload$action, "clean")) {
  results <- lapply(payload$jobs, clean_one)
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
