#!/usr/bin/env Rscript
# ==============================================================================
# 02_01_preprocess_series.R
#
# Purpose: Serve the bounded R subprocess that preprocesses model-training contexts.
# Inputs: JSON on stdin: action="preprocess" and jobs with id, numeric/missing
#   context, mode, and positive official seasonality; Python supplies the request.
# Outputs: JSON with finite same-scale vectors, change/missingness metadata, and versions;
#   invalid actions, methods, or series terminate the subprocess with an R error.
# Run from: printf '%s' '{"action":"preprocess","jobs":[]}' | Rscript src/r/02_01_preprocess_series.R
# ==============================================================================

suppressPackageStartupMessages({
  library(forecast)
  library(jsonlite)
})

source("src/r/util/time_series_input.R")

# Execution global: payload is the coordinator-authored request for this subprocess;
# scientific and execution values remain authoritative from DuckDB and have no local override.
payload <- jsonlite::fromJSON(file("stdin"), simplifyVector = FALSE)

# Purpose: Prepare one context using the approved standard or robust mode.
# Inputs: job with id, numeric/missing context of length n, official seasonality,
#   and mode "standard" or "robust". No future observations are accepted.
# Outputs: Finite length-n values plus missingness/change provenance. Standard uses
#   na.interp only; robust intentionally reproduces forecast::tsclean exactly.
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
    status = "success",
    missing_count_before = missing_before,
    missing_count_after = sum(is.na(processed)),
    values_changed = changed
  )
}

# Execution global: results preserves coordinator job order for this invocation;
# it is derived from stdin and has no independent override.
if (identical(payload$action, "preprocess")) {
  results <- lapply(payload$jobs, preprocess_one)
} else {
  stop("Unsupported preprocessing worker action", call. = FALSE)
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
