#!/usr/bin/env Rscript
# ==============================================================================
# inspect_standardisation.R
#
# Purpose: Inspect one series' raw, preprocessed, transformed, and restored values.
# Inputs: Read-only experiment DuckDB plus optional STANDARDISATION_QA_* selectors.
# Outputs: Inspectable selected_standardisation and named vectors/state in R session.
# Run from: source("src/r/qa/inspect_standardisation.R") in repository-root R.
# ==============================================================================

source("src/r/util/transformations.R")

# Purpose: Validate one nonempty selector used in read-only QA queries.
# Inputs: Candidate scalar and human-readable name.
# Outputs: Character scalar or a clear terminating error.
.standardisation_scalar <- function(value, name) {
  value <- as.character(value)
  if (length(value) != 1L || is.na(value) || !nzchar(value)) {
    stop(name, " must be one nonempty value", call. = FALSE)
  }
  value
}

# Purpose: Read and locally verify one Gate 3 standardisation without writes.
# Inputs: Database, series, preprocessing mode, and optional experiment identity.
# Outputs: Raw/model histories, stored state when available, local state, and checks.
get_standardisation_qa <- function(
    database_path = "results/poc2_m4_daily_100_period_tuning.duckdb",
    series_id = "0",
    preprocessing = "robust",
    experiment_id = NULL) {
  if (!requireNamespace("DBI", quietly = TRUE) ||
      !requireNamespace("duckdb", quietly = TRUE) ||
      !requireNamespace("jsonlite", quietly = TRUE)) {
    stop("R packages 'DBI', 'duckdb', and 'jsonlite' are required", call. = FALSE)
  }
  database_path <- normalizePath(
    .standardisation_scalar(database_path, "database_path"), mustWork = TRUE
  )
  series_id <- .standardisation_scalar(series_id, "series_id")
  preprocessing <- .standardisation_scalar(preprocessing, "preprocessing")
  if (!preprocessing %in% c("standard", "robust")) {
    stop("preprocessing must be standard or robust", call. = FALSE)
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
  experiment_id <- .standardisation_scalar(experiment_id, "experiment_id")

  instance <- DBI::dbGetQuery(
    connection,
    paste(
      "SELECT forecast_instance_id, context_target, actual_target, horizon",
      "FROM forecast_instances WHERE series_id = ? ORDER BY window_id"
    ),
    params = list(series_id)
  )
  if (nrow(instance) != 1L) {
    stop("Expected exactly one matching M4 Daily forecast instance", call. = FALSE)
  }
  instance_id <- instance$forecast_instance_id[[1L]]
  preprocessed <- DBI::dbGetQuery(
    connection,
    paste(
      "SELECT preprocessing_id, context_target, method_configuration, package_versions",
      "FROM preprocessed_series WHERE experiment_id = ?",
      "AND forecast_instance_id = ? AND cleaning_method = ?"
    ),
    params = list(experiment_id, instance_id, preprocessing)
  )
  if (nrow(preprocessed) != 1L) {
    stop("Expected exactly one matching preprocessed history", call. = FALSE)
  }

  stored <- DBI::dbGetQuery(
    connection,
    paste(
      "SELECT t.transformation_id, t.transformed_target, t.parameters",
      "FROM experiment_variants v JOIN transformed_series t USING (variant_id)",
      "WHERE v.experiment_id = ? AND t.forecast_instance_id = ?",
      "AND v.cleaning_method = ?",
      "AND v.transformation_method = 'standardise_sample_v1'"
    ),
    params = list(experiment_id, instance_id, preprocessing)
  )
  if (nrow(stored) > 1L) {
    stop("More than one stored standardisation matched the requested identity", call. = FALSE)
  }

  raw_history <- as.numeric(instance$context_target[[1L]])
  model_history <- as.numeric(preprocessed$context_target[[1L]])
  local_state <- fit_transformation(model_history, STANDARDISATION_RECIPE)
  local_transformed <- apply_transformation(model_history, local_state)
  local_restored <- inverse_transformation(local_transformed, local_state)
  stored_available <- nrow(stored) == 1L
  stored_state <- if (stored_available) {
    jsonlite::fromJSON(stored$parameters[[1L]], simplifyVector = FALSE)
  } else {
    NULL
  }
  stored_transformed <- if (stored_available) {
    as.numeric(stored$transformed_target[[1L]])
  } else {
    NULL
  }
  # Python uses math.fsum while R uses mean(), so portable state can differ by
  # a few 1e-11 on long histories despite equivalent double-precision formulas.
  tolerance <- 1e-10
  list(
    experiment_id = experiment_id,
    forecast_instance_id = instance_id,
    series_id = series_id,
    preprocessing = preprocessing,
    recipe = STANDARDISATION_RECIPE,
    raw_history = raw_history,
    future_actuals_qa_only = as.numeric(instance$actual_target[[1L]]),
    model_history = model_history,
    local_state = local_state,
    local_transformed = local_transformed,
    local_restored = local_restored,
    stored_available = stored_available,
    stored_transformation_id = if (stored_available) stored$transformation_id[[1L]] else NULL,
    stored_state = stored_state,
    stored_transformed = stored_transformed,
    checks = list(
      raw_history_unchanged = identical(raw_history, as.numeric(instance$context_target[[1L]])),
      local_inverse_max_abs_error = max(abs(local_restored - model_history)),
      stored_state_matches_local = if (stored_available) {
        isTRUE(all.equal(
          validate_transformation_state(stored_state),
          local_state,
          tolerance = tolerance
        ))
      } else {
        NA
      },
      stored_values_match_local = if (stored_available) {
        isTRUE(all.equal(stored_transformed, local_transformed, tolerance = tolerance))
      } else {
        NA
      }
    ),
    provenance = list(
      database_path = database_path,
      preprocessing_configuration = jsonlite::fromJSON(
        preprocessed$method_configuration[[1L]], simplifyVector = FALSE
      ),
      preprocessing_packages = jsonlite::fromJSON(
        preprocessed$package_versions[[1L]], simplifyVector = FALSE
      ),
      state_source = if (stored_available) {
        "stored Gate 3 result plus independent local recomputation"
      } else {
        "local recomputation only; this database has no standardise_sample_v1 row"
      },
      future_actuals_role = "QA display only; never used to fit preprocessing or transformation"
    )
  )
}

# Purpose: Leave the main QA components as convenient named objects in a session.
# Inputs: Result returned by get_standardisation_qa().
# Outputs: Named assignments in the caller environment; returns result invisibly.
expose_standardisation_qa <- function(result, environment = parent.frame()) {
  assign("selected_standardisation", result, envir = environment)
  assign("original_history", result$raw_history, envir = environment)
  assign("model_history", result$model_history, envir = environment)
  assign("fitted_standardisation_state", result$local_state, envir = environment)
  assign("standardised_history", result$local_transformed, envir = environment)
  assign("restored_history", result$local_restored, envir = environment)
  assign("stored_standardisation_state", result$stored_state, envir = environment)
  assign("standardisation_checks", result$checks, envir = environment)
  invisible(result)
}

arguments <- commandArgs(trailingOnly = TRUE)
if (sys.nframe() == 0L) {
  result <- get_standardisation_qa(
    database_path = if (length(arguments) >= 1L) arguments[[1L]] else "results/poc2_m4_daily_100_period_tuning.duckdb",
    series_id = if (length(arguments) >= 2L) arguments[[2L]] else "0",
    preprocessing = if (length(arguments) >= 3L) arguments[[3L]] else "robust"
  )
  expose_standardisation_qa(result, globalenv())
  print(selected_standardisation)
} else {
  result <- get_standardisation_qa(
    database_path = if (exists("STANDARDISATION_QA_DATABASE", inherits = TRUE)) get("STANDARDISATION_QA_DATABASE", inherits = TRUE) else "results/poc2_m4_daily_100_period_tuning.duckdb",
    series_id = if (exists("STANDARDISATION_QA_SERIES_ID", inherits = TRUE)) get("STANDARDISATION_QA_SERIES_ID", inherits = TRUE) else "0",
    preprocessing = if (exists("STANDARDISATION_QA_PREPROCESSING", inherits = TRUE)) get("STANDARDISATION_QA_PREPROCESSING", inherits = TRUE) else "robust",
    experiment_id = if (exists("STANDARDISATION_QA_EXPERIMENT_ID", inherits = TRUE)) get("STANDARDISATION_QA_EXPERIMENT_ID", inherits = TRUE) else NULL
  )
  # source() evaluates this branch through an intermediate frame in some
  # frontends. The global session is the stable researcher workspace promised
  # by this QA script, including RStudio and Rscript -e source() calls.
  expose_standardisation_qa(result, globalenv())
  message(
    "Created selected_standardisation and inspectable history/state vectors. ",
    result$provenance$state_source, "."
  )
}
