#!/usr/bin/env Rscript
# ==============================================================================
# 04_01_forecast_r_methods.R
#
# Purpose: Serve the capability-aware R pool and shared period diagnostics.
# Inputs: JSON stdin with action forecast or diagnose_period and versioned jobs.
# Outputs: Common forecast result/error envelopes and package versions; no DuckDB.
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

# Code constants: exact cross-language fields and point-only M4 allowlist.
.FORECAST_REQUEST_FIELDS <- c(
  "contract_version", "experiment_id", "task_id", "forecast_instance_id",
  "variant_id", "dataset_id", "series_id", "model_id", "required_capability",
  "context", "horizon", "frequency", "seasonal_period", "input_scale",
  "quantile_levels", "seed", "model_settings"
)
.M4_POINT_METHODS <- c(
  naive2 = "naive2_forec", ses = "ses_forec", holt = "holt_forec",
  damped = "damped_forec"
)

# Purpose: Map production model IDs to one capability-aware R method ID.
# Inputs: Stable production model ID.
# Outputs: One allowlisted registered or M4 point method ID.
r_method_id <- function(model_id) {
  if (model_id %in% names(.M4_POINT_METHODS)) {
    return(unname(.M4_POINT_METHODS[[model_id]]))
  }
  result <- paste0(model_id, "_forec")
  if (!result %in% names(forecast_method_registry())) {
    stop("Unsupported R model: ", model_id, call. = FALSE)
  }
  result
}

# Purpose: Execute one common request through the registered or M4 point pool.
# Inputs: Exact language-neutral forecast request.
# Outputs: Exact language-neutral successful forecast result.
forecast_one <- function(job) {
  if (!identical(sort(names(job)), sort(.FORECAST_REQUEST_FIELDS))) {
    stop("forecast request must contain exactly the common contract fields", call. = FALSE)
  }
  method_id <- r_method_id(job$model_id)
  started <- proc.time()[["elapsed"]]
  if (identical(job$required_capability, "mean_only")) {
    if (!job$model_id %in% names(.M4_POINT_METHODS) || !is.null(job$quantile_levels)) {
      stop("invalid mean-only R forecast request", call. = FALSE)
    }
    series <- time_series_from_values(
      as.numeric(unlist(job$context)), as.integer(job$seasonal_period), allow_missing = FALSE
    )
    mean <- get(method_id, mode = "function")(series, as.integer(job$horizon))
    result <- list(
      mean = as.numeric(mean), median = NULL, quantile_levels = NULL,
      quantiles = NULL, requested_method_id = method_id,
      executed_method_id = method_id, fallback_used = FALSE,
      fallback_reason = NULL,
      provenance = .forecast_provenance(method_id, "official M4 point forecast", list())
    )
  } else {
    if (!identical(job$required_capability, "probabilistic")) {
      stop("unsupported R forecast capability", call. = FALSE)
    }
    settings <- job$model_settings
    if (identical(method_id, "nnetar_forec")) {
      settings$seed <- as.integer(job$seed)
    }
    result <- run_forecast_method(list(
      task_id = job$task_id,
      dataset_id = job$dataset_id,
      series_id = job$series_id,
      context = job$context,
      horizon = job$horizon,
      frequency = job$seasonal_period,
      method_id = method_id,
      settings = settings,
      quantile_levels = job$quantile_levels
    ))
  }
  provenance <- result$provenance
  provenance$implementation_language <- "R"
  provenance$seed <- job$seed
  list(
    contract_version = job$contract_version,
    status = "success",
    experiment_id = job$experiment_id,
    task_id = job$task_id,
    forecast_instance_id = job$forecast_instance_id,
    variant_id = job$variant_id,
    requested_model_id = job$model_id,
    executed_model_id = sub("_forec$", "", result$executed_method_id),
    forecast_capability = job$required_capability,
    output_scale = job$input_scale,
    horizon = job$horizon,
    mean = result$mean,
    median = result$median,
    quantile_levels = if (is.null(result$quantile_levels)) NULL else as.numeric(result$quantile_levels),
    quantiles = if (is.null(result$quantiles)) NULL else lapply(
      seq_len(nrow(result$quantiles)), function(row) as.numeric(result$quantiles[row, ])
    ),
    fallback_used = result$fallback_used,
    fallback_reason = result$fallback_reason,
    runtime_seconds = max(0, proc.time()[["elapsed"]] - started),
    provenance = provenance
  )
}

# Purpose: Convert a provider failure to the common error envelope.
# Inputs: Original request and caught condition.
# Outputs: Exact common error mapping with no partial forecast arrays.
forecast_error <- function(job, error) {
  list(
    contract_version = job$contract_version %||% "forecast-v1",
    status = "error",
    experiment_id = job$experiment_id %||% "unknown",
    task_id = job$task_id %||% "unknown",
    forecast_instance_id = job$forecast_instance_id %||% "unknown",
    variant_id = job$variant_id %||% "unknown",
    requested_model_id = job$model_id %||% "unknown",
    error_type = class(error)[[1L]],
    error_message = conditionMessage(error),
    provenance = list(
      implementation_language = "R",
      provider = "forecast",
      package_version = as.character(utils::packageVersion("forecast"))
    )
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
  results <- lapply(payload$jobs, function(job) {
    tryCatch(forecast_one(job), error = function(error) forecast_error(job, error))
  })
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
