#!/usr/bin/env Rscript
# ==============================================================================
# inspect_seasonal_period_tuning.R
#
# Purpose: Inspect one series' persisted Gate 4 period-tuning evidence read-only.
# Inputs: Optional TUNING_QA_* variables before source(), or CLI database/series/model.
# Outputs: selected_tuning_series remains in the caller's R session after source().
# Run from: source("src/r/qa/inspect_seasonal_period_tuning.R") in repository-root R.
# ==============================================================================

.tuning_scalar <- function(value, name) {
  value <- as.character(value)
  if (length(value) != 1L || is.na(value) || !nzchar(value)) {
    stop(name, " must be one nonempty value", call. = FALSE)
  }
  value
}

# Purpose: Read one series/model/variant tuning trail without modifying DuckDB.
# Inputs: Database, series, model, preprocessing, transformation and optional experiment.
# Outputs: Named list of folds, candidates, validation QA rows, selection, and final forecast.
get_seasonal_period_tuning <- function(
    database_path = "results/poc2_m4_daily_100_period_tuning.duckdb",
    series_id = "0",
    model = "auto_arima",
    preprocessing = "robust",
    transformation = "identity",
    experiment_id = NULL) {
  if (!requireNamespace("DBI", quietly = TRUE) ||
      !requireNamespace("duckdb", quietly = TRUE)) {
    stop("R packages 'DBI' and 'duckdb' are required", call. = FALSE)
  }
  database_path <- normalizePath(.tuning_scalar(database_path, "database_path"), mustWork = TRUE)
  series_id <- .tuning_scalar(series_id, "series_id")
  model <- .tuning_scalar(model, "model")
  if (!model %in% c("auto_arima", "ets")) {
    stop("model must be auto_arima or ets", call. = FALSE)
  }
  connection <- DBI::dbConnect(
    duckdb::duckdb(shared_home = FALSE), dbdir = database_path, read_only = TRUE
  )
  on.exit(DBI::dbDisconnect(connection, shutdown = TRUE), add = TRUE)
  if (is.null(experiment_id)) {
    experiment <- DBI::dbGetQuery(
      connection,
      "SELECT experiment_id FROM experiments ORDER BY updated_at DESC LIMIT 1"
    )
    if (nrow(experiment) != 1L) stop("No experiment was found", call. = FALSE)
    experiment_id <- experiment$experiment_id[[1L]]
  }
  identity <- DBI::dbGetQuery(
    connection,
    paste(
      "SELECT i.forecast_instance_id, v.variant_id",
      "FROM forecast_instances i",
      "JOIN experiment_variants v ON v.experiment_id = ?",
      "WHERE i.series_id = ? AND v.cleaning_method = ?",
      "AND v.transformation_method = ?"
    ),
    params = list(experiment_id, series_id, preprocessing, transformation)
  )
  if (nrow(identity) != 1L) {
    stop("Expected exactly one matching series and preparation variant", call. = FALSE)
  }
  instance_id <- identity$forecast_instance_id[[1L]]
  variant_id <- identity$variant_id[[1L]]
  folds <- DBI::dbGetQuery(
    connection,
    paste(
      "SELECT * FROM seasonal_tuning_folds",
      "WHERE experiment_id = ? AND variant_id = ? AND forecast_instance_id = ?",
      "ORDER BY fold_number"
    ),
    params = list(experiment_id, variant_id, instance_id)
  )
  candidates <- DBI::dbGetQuery(
    connection,
    paste(
      "SELECT c.* FROM seasonal_period_candidates c",
      "JOIN seasonal_tuning_folds f USING (fold_id)",
      "WHERE f.experiment_id = ? AND f.variant_id = ?",
      "AND f.forecast_instance_id = ? AND c.model = ? ORDER BY f.fold_number"
    ),
    params = list(experiment_id, variant_id, instance_id, model)
  )
  validations <- DBI::dbGetQuery(
    connection,
    paste(
      "SELECT f.fold_number, v.* FROM seasonal_tuning_validations v",
      "JOIN seasonal_period_candidates c USING (candidate_id)",
      "JOIN seasonal_tuning_folds f USING (fold_id)",
      "WHERE f.experiment_id = ? AND f.variant_id = ?",
      "AND f.forecast_instance_id = ? AND v.model = ?",
      "ORDER BY f.fold_number, v.policy"
    ),
    params = list(experiment_id, variant_id, instance_id, model)
  )
  selection <- DBI::dbGetQuery(
    connection,
    paste(
      "SELECT * FROM seasonal_period_selections",
      "WHERE experiment_id = ? AND variant_id = ?",
      "AND forecast_instance_id = ? AND model = ?"
    ),
    params = list(experiment_id, variant_id, instance_id, model)
  )
  final_forecast <- DBI::dbGetQuery(
    connection,
    paste(
      "SELECT forecast_id, candidate, mean, execution_metadata FROM forecasts",
      "WHERE experiment_id = ? AND variant_id = ?",
      "AND forecast_instance_id = ? AND candidate = ?"
    ),
    params = list(experiment_id, variant_id, instance_id, model)
  )
  list(
    experiment_id = experiment_id,
    series_id = series_id,
    model = model,
    preprocessing = preprocessing,
    transformation = transformation,
    folds = folds,
    candidates = candidates,
    validation = validations,
    validation_actuals_note = "QA scoring data only; never supplied as model inputs",
    selection = selection,
    final_forecast = final_forecast
  )
}

arguments <- commandArgs(trailingOnly = TRUE)
if (sys.nframe() == 0L) {
  selected_tuning_series <- get_seasonal_period_tuning(
    database_path = if (length(arguments) >= 1L) arguments[[1L]] else "results/poc2_m4_daily_100_period_tuning.duckdb",
    series_id = if (length(arguments) >= 2L) arguments[[2L]] else "0",
    model = if (length(arguments) >= 3L) arguments[[3L]] else "auto_arima"
  )
  print(selected_tuning_series)
} else {
  selected_tuning_series <- get_seasonal_period_tuning(
    database_path = if (exists("TUNING_QA_DATABASE", inherits = TRUE)) get("TUNING_QA_DATABASE", inherits = TRUE) else "results/poc2_m4_daily_100_period_tuning.duckdb",
    series_id = if (exists("TUNING_QA_SERIES_ID", inherits = TRUE)) get("TUNING_QA_SERIES_ID", inherits = TRUE) else "0",
    model = if (exists("TUNING_QA_MODEL", inherits = TRUE)) get("TUNING_QA_MODEL", inherits = TRUE) else "auto_arima",
    preprocessing = if (exists("TUNING_QA_PREPROCESSING", inherits = TRUE)) get("TUNING_QA_PREPROCESSING", inherits = TRUE) else "robust",
    transformation = if (exists("TUNING_QA_TRANSFORMATION", inherits = TRUE)) get("TUNING_QA_TRANSFORMATION", inherits = TRUE) else "identity",
    experiment_id = if (exists("TUNING_QA_EXPERIMENT_ID", inherits = TRUE)) get("TUNING_QA_EXPERIMENT_ID", inherits = TRUE) else NULL
  )
  message("Created selected_tuning_series. Inspect its folds, candidates, validation, selection, and final_forecast components.")
}
