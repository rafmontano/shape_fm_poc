#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(forecast)
  library(jsonlite)
})

source("src/r/util/time_series_input.R")

payload <- jsonlite::fromJSON(file("stdin"), simplifyVector = FALSE)

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
