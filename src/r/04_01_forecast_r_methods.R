#!/usr/bin/env Rscript
# ==============================================================================
# 04_01_forecast_r_methods.R
#
# Purpose: Serve bounded AutoARIMA/ETS forecasts and shared period diagnostics.
# Inputs: JSON stdin with action forecast or diagnose_period and an ordered jobs list.
# Outputs: JSON results and exact R package versions; never reads or writes DuckDB.
# Run from: Invoked by the Python Gate 4 coordinator.
# ==============================================================================

suppressPackageStartupMessages({
  library(forecast)
  library(jsonlite)
  library(tsfeatures)
})

source("src/r/util/forecast_methods.R")
source("src/r/util/seasonal_period.R")

payload <- jsonlite::fromJSON(file("stdin"), simplifyVector = FALSE)

# Purpose: Map production model IDs to the existing registered R method IDs.
# Inputs: auto_arima or ets.
# Outputs: One allowlisted forecast-pool method ID.
registered_method_id <- function(model_id) {
  methods <- c(auto_arima = "auto_arima_forec", ets = "ets_forec")
  result <- unname(methods[[as.character(model_id)]])
  if (is.null(result)) {
    stop("Unsupported R model: ", model_id, call. = FALSE)
  }
  result
}

# Purpose: Execute one production forecast through the registered pool.
# Inputs: Job with identities, prepared context, model period, horizon and settings.
# Outputs: Common probabilistic arrays plus requested/executed/fallback provenance.
forecast_one <- function(job) {
  method_id <- registered_method_id(job$model)
  result <- run_forecast_method(list(
    task_id = job$id,
    dataset_id = job$dataset_id %||% job$id,
    series_id = job$series_id %||% job$id,
    context = job$context,
    horizon = job$horizon,
    frequency = job$model_period,
    method_id = method_id,
    settings = job$settings %||% list(),
    quantile_levels = seq(0.1, 0.9, by = 0.1)
  ))
  list(
    id = job$id,
    mean = result$mean,
    median = result$median,
    quantiles = lapply(
      seq_len(nrow(result$quantiles)),
      function(row) as.numeric(result$quantiles[row, ])
    ),
    model_period = result$r_period,
    requested_method_id = result$requested_method_id,
    executed_method_id = result$executed_method_id,
    fallback_used = result$fallback_used,
    fallback_reason = result$fallback_reason,
    provenance = result$provenance
  )
}

# Purpose: Diagnose one model-specific estimated-period candidate.
# Inputs: Job with prepared context, baseline period, model, and minimum cycles.
# Outputs: Candidate evidence carrying the original job identity.
diagnose_one <- function(job) {
  c(
    list(id = job$id, model = job$model),
    diagnose_seasonal_period(
      job$context,
      job$baseline_period,
      job$model,
      job$minimum_cycles
    )
  )
}

if (identical(payload$action, "forecast")) {
  results <- lapply(payload$jobs, forecast_one)
} else if (identical(payload$action, "diagnose_period")) {
  results <- lapply(payload$jobs, diagnose_one)
} else {
  stop("Unsupported R methods worker action", call. = FALSE)
}

cat(jsonlite::toJSON(
  list(
    results = results,
    packages = list(
      R = R.version.string,
      forecast = as.character(utils::packageVersion("forecast")),
      jsonlite = as.character(utils::packageVersion("jsonlite")),
      tsfeatures = as.character(utils::packageVersion("tsfeatures"))
    )
  ),
  auto_unbox = TRUE,
  digits = 17,
  null = "null"
))
