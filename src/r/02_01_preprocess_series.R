#!/usr/bin/env Rscript
# ==============================================================================
# 02_01_preprocess_series.R
#
# Purpose: Serve the bounded R subprocess that cleans batches of time-series contexts.
# Inputs: JSON on stdin: action="clean" and jobs with id, numeric context, method,
#   and positive integer seasonal frequency; Python supplies it to the subprocess.
# Outputs: JSON on stdout with same-scale cleaned vectors and package versions;
#   invalid actions, methods, or series terminate the subprocess with an R error.
# Run from: printf '%s' '{"action":"clean","jobs":[]}' | Rscript src/r/02_01_preprocess_series.R
# ==============================================================================

suppressPackageStartupMessages({
  library(forecast)
  library(jsonlite)
})

source("src/r/util/time_series_input.R")

# Execution global: payload is the coordinator-authored request for this subprocess;
# scientific and execution values remain authoritative from DuckDB and have no local override.
payload <- jsonlite::fromJSON(file("stdin"), simplifyVector = FALSE)

# Purpose: Clean one univariate context using identity or forecast::tsclean.
# Inputs: job with id, finite numeric context of length n, positive integer
#   seasonality (the stats::ts frequency), and method "identity" or "tsclean".
# Outputs: List with the unchanged id and n same-scale numeric values; writes
#   nothing to stdout and raises an R error for an unsupported method/input.
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

# Execution global: results preserves coordinator job order for this invocation;
# it is derived from stdin and has no independent override.
if (identical(payload$action, "clean")) {
  results <- lapply(payload$jobs, clean_one)
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
