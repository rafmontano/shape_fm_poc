#!/usr/bin/env Rscript
# Process 02 provider: read full historical jobs from stdin JSON and return
# same-scale prepared vectors, missing/change metadata and package versions.
# Process 03 slices these persisted histories without another cleaning pass.
# Each job supplies id, context, mode and the coordinator-resolved R period
# (seasonality); future observations are excluded. Invalid requests fail.

suppressPackageStartupMessages({
  library(forecast)
  library(jsonlite)
})

source("src/r/util/time_series_input.R")

payload <- jsonlite::fromJSON(file("stdin"), simplifyVector = FALSE)

# Prepare one historical job under docs/data-contract.md: standard uses
# forecast::na.interp; robust uses forecast::tsclean without altered settings.
# Return finite length-n values and provenance; no rescaling or future input.
preprocess_one <- function(job) {
  input <- time_series_input(job)
  mode <- job$mode
  missing_before <- sum(is.na(input$values))
  if (identical(mode, "standard")) {
    processed <- as.numeric(forecast::na.interp(input$series))
  } else if (identical(mode, "robust")) {
    processed <- as.numeric(forecast::tsclean(input$series))
  } else {
    stop("Unsupported preprocessing mode: ", mode, call. = FALSE)
  }
  if (length(processed) != length(input$values) || any(!is.finite(processed))) {
    stop("Preprocessing did not produce a finite vector of the original length", call. = FALSE)
  }
  finite_positions <- !is.na(input$values)
  changed <- missing_before > 0L || any(
    processed[finite_positions] != input$values[finite_positions]
  )
  list(
    id = job$id,
    values = processed,
    preprocessing_mode = mode,
    r_period = as.integer(stats::frequency(input$series)),
    status = "success",
    missing_count_before = missing_before,
    missing_count_after = sum(is.na(processed)),
    values_changed = changed
  )
}

if (identical(payload$action, "preprocess")) {
  results <- lapply(payload$jobs, preprocess_one)
} else {
  stop("Unsupported preprocessing worker action", call. = FALSE)
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
