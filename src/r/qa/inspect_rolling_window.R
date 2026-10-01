#!/usr/bin/env Rscript
# ==============================================================================
# inspect_rolling_window.R
#
# Purpose: Inspect one persisted S1 rolling window without modifying either database.
# Inputs: Parent/child DuckDB paths, dataset ID, series ID and zero-based ordinal.
# Outputs: selected_rolling_window plus convenient raw/transformed/state objects in R.
# Run from: source("src/r/qa/inspect_rolling_window.R") in repository-root R.
# ==============================================================================

source("src/r/util/transformations.R")

# Purpose: Require one nonempty character selector for a read-only QA query.
# Inputs: Candidate value and field name.
# Outputs: Character scalar or a clear error.
.rolling_scalar <- function(value, name) {
  value <- as.character(value)
  if (length(value) != 1L || is.na(value) || !nzchar(value)) {
    stop(name, " must be one nonempty value", call. = FALSE)
  }
  value
}

# Purpose: Retrieve one prepared window and resolve raw slices from the parent.
# Inputs: Existing parent/child databases and canonical dataset/series/window selectors.
# Outputs: Mapped partition, boundaries, raw input/future, transformed input, state and provenance.
get_rolling_window_qa <- function(
    parent_database = "results/poc2_m4_daily_100_rolling_windows.duckdb",
    windows_database = "results/poc2_m4_daily_100_rolling_windows.windows.duckdb",
    dataset_id,
    series_id = "0",
    window_ordinal = 0L) {
  if (!requireNamespace("DBI", quietly = TRUE) ||
      !requireNamespace("duckdb", quietly = TRUE) ||
      !requireNamespace("jsonlite", quietly = TRUE)) {
    stop("R packages 'DBI', 'duckdb', and 'jsonlite' are required", call. = FALSE)
  }
  parent_database <- normalizePath(
    .rolling_scalar(parent_database, "parent_database"), mustWork = TRUE
  )
  windows_database <- normalizePath(
    .rolling_scalar(windows_database, "windows_database"), mustWork = TRUE
  )
  dataset_id <- .rolling_scalar(dataset_id, "dataset_id")
  series_id <- .rolling_scalar(series_id, "series_id")
  ordinal_value <- suppressWarnings(as.numeric(window_ordinal))
  window_ordinal <- suppressWarnings(as.integer(ordinal_value))
  if (length(ordinal_value) != 1L || is.na(ordinal_value) ||
      ordinal_value < 0L || ordinal_value != window_ordinal) {
    stop("window_ordinal must be one non-negative integer", call. = FALSE)
  }

  parent <- DBI::dbConnect(
    duckdb::duckdb(shared_home = FALSE), dbdir = parent_database, read_only = TRUE
  )
  on.exit(DBI::dbDisconnect(parent, shutdown = TRUE), add = TRUE)
  child <- DBI::dbConnect(
    duckdb::duckdb(shared_home = FALSE), dbdir = windows_database, read_only = TRUE
  )
  on.exit(DBI::dbDisconnect(child, shutdown = TRUE), add = TRUE)

  parent_identity <- DBI::dbGetQuery(
    parent,
    paste(
      "SELECT scientific_hash, configuration_integrity_hash",
      "FROM experiment_configuration WHERE configuration_key='experiment'"
    )
  )
  child_identity <- DBI::dbGetQuery(
    child,
    paste(
      "SELECT preparation_id, parent_scientific_hash, parent_configuration_hash,",
      "definition_hash FROM preparation_metadata"
    )
  )
  if (nrow(parent_identity) != 1L || nrow(child_identity) != 1L ||
      parent_identity$scientific_hash[[1L]] != child_identity$parent_scientific_hash[[1L]] ||
      parent_identity$configuration_integrity_hash[[1L]] !=
        child_identity$parent_configuration_hash[[1L]]) {
    stop("Parent and child scientific/configuration identities do not match", call. = FALSE)
  }
  preparation_id <- child_identity$preparation_id[[1L]]
  run <- DBI::dbGetQuery(
    parent,
    paste(
      "SELECT child_database, definition_hash, parent_scientific_hash,",
      "parent_configuration_hash, membership_fingerprint, status",
      "FROM window_preparation_runs WHERE preparation_id=?"
    ),
    params = list(preparation_id)
  )
  split <- DBI::dbGetQuery(
    child,
    paste(
      "SELECT membership_fingerprint FROM split_definitions",
      "WHERE preparation_id=? AND split_id='S1'"
    ),
    params = list(preparation_id)
  )
  recorded_child <- if (nrow(run) == 1L) {
    normalizePath(file.path(dirname(parent_database), run$child_database[[1L]]), mustWork = TRUE)
  } else {
    NA_character_
  }
  if (nrow(run) != 1L || nrow(split) != 1L || recorded_child != windows_database ||
      run$definition_hash[[1L]] != child_identity$definition_hash[[1L]] ||
      run$parent_scientific_hash[[1L]] != child_identity$parent_scientific_hash[[1L]] ||
      run$parent_configuration_hash[[1L]] != child_identity$parent_configuration_hash[[1L]] ||
      run$membership_fingerprint[[1L]] != split$membership_fingerprint[[1L]] ||
      run$status[[1L]] != "completed") {
    stop("Parent/child preparation lineage is incomplete or mismatched", call. = FALSE)
  }
  if (!("source_content_hash" %in% DBI::dbListFields(child, "series_membership"))) {
    stop(
      "This historical child lacks per-series source lineage; use a corrected v6 child",
      call. = FALSE
    )
  }

  series <- DBI::dbGetQuery(
    parent,
    paste(
      "SELECT l.series_key, l.frequency_key, s.frequency, s.target,",
      "s.content_hash, s.observation_count FROM series s",
      "JOIN dataset_lookup d USING (dataset_id)",
      "JOIN series_lookup l ON l.dataset_key=d.dataset_key",
      "AND l.series_id=s.series_id WHERE s.dataset_id=? AND s.series_id=?"
    ),
    params = list(dataset_id, series_id)
  )
  if (nrow(series) != 1L) stop("Expected exactly one canonical series", call. = FALSE)
  prepared <- DBI::dbGetQuery(
    child,
    paste(
      "SELECT w.window_id, m.partition, w.input_start, w.input_end,",
      "w.future_start, w.future_end, w.transformed_input,",
      "w.transformation_state, w.preprocessing_provenance,",
      "w.package_versions, w.worker_provenance, m.source_content_hash,",
      "m.usable_start, m.usable_end, f.input_length, f.future_horizon, f.stride",
      "FROM prepared_windows w JOIN series_membership m",
      "USING (preparation_id, series_key)",
      "JOIN frequency_definitions f USING (preparation_id)",
      "WHERE w.preparation_id=? AND w.series_key=? AND w.window_ordinal=?",
      "AND f.frequency_key=?"
    ),
    params = list(
      preparation_id, series$series_key[[1L]], window_ordinal,
      series$frequency_key[[1L]]
    )
  )
  if (nrow(prepared) != 1L) stop("Expected exactly one prepared window", call. = FALSE)
  if (prepared$source_content_hash[[1L]] != series$content_hash[[1L]]) {
    stop("Prepared series source identity does not match the parent", call. = FALSE)
  }

  target <- as.numeric(series$target[[1L]])
  expected_input_start <- as.integer(prepared$usable_start[[1L]]) +
    window_ordinal * as.integer(prepared$stride[[1L]])
  boundaries_valid <-
    length(target) == as.integer(series$observation_count[[1L]]) &&
    as.integer(prepared$input_start[[1L]]) == expected_input_start &&
    as.integer(prepared$input_end[[1L]]) - as.integer(prepared$input_start[[1L]]) ==
      as.integer(prepared$input_length[[1L]]) &&
    as.integer(prepared$future_start[[1L]]) == as.integer(prepared$input_end[[1L]]) &&
    as.integer(prepared$future_end[[1L]]) - as.integer(prepared$future_start[[1L]]) ==
      as.integer(prepared$future_horizon[[1L]]) &&
    as.integer(prepared$future_end[[1L]]) <= as.integer(prepared$usable_end[[1L]]) &&
    as.integer(prepared$future_end[[1L]]) <= length(target) &&
    length(as.numeric(prepared$transformed_input[[1L]])) ==
      as.integer(prepared$input_length[[1L]])
  if (!boundaries_valid) {
    stop("Prepared window boundaries are inconsistent with persisted definitions", call. = FALSE)
  }
  input_positions <- seq.int(
    as.integer(prepared$input_start[[1L]]) + 1L,
    as.integer(prepared$input_end[[1L]])
  )
  future_positions <- seq.int(
    as.integer(prepared$future_start[[1L]]) + 1L,
    as.integer(prepared$future_end[[1L]])
  )
  state <- jsonlite::fromJSON(
    prepared$transformation_state[[1L]], simplifyVector = FALSE
  )
  transformed <- as.numeric(prepared$transformed_input[[1L]])
  list(
    dataset_id = dataset_id,
    series_id = series_id,
    frequency = series$frequency[[1L]],
    window_id = prepared$window_id[[1L]],
    window_ordinal = window_ordinal,
    partition = prepared$partition[[1L]],
    boundaries = list(
      input_start = as.integer(prepared$input_start[[1L]]),
      input_end = as.integer(prepared$input_end[[1L]]),
      future_start = as.integer(prepared$future_start[[1L]]),
      future_end = as.integer(prepared$future_end[[1L]])
    ),
    raw_input = target[input_positions],
    raw_future = target[future_positions],
    transformed_input = transformed,
    transformation_state = state,
    restored_cleaned_input = inverse_transformation(transformed, state),
    preprocessing_provenance = jsonlite::fromJSON(
      prepared$preprocessing_provenance[[1L]], simplifyVector = FALSE
    ),
    package_versions = jsonlite::fromJSON(
      prepared$package_versions[[1L]], simplifyVector = FALSE
    ),
    worker_provenance = jsonlite::fromJSON(
      prepared$worker_provenance[[1L]], simplifyVector = FALSE
    ),
    database_paths = list(parent = parent_database, windows = windows_database)
  )
}

# Purpose: Leave the selected window and main components in the R global workspace.
# Inputs: Result from get_rolling_window_qa().
# Outputs: Global assignments for interactive QA; returns result invisibly.
expose_rolling_window_qa <- function(result, environment = globalenv()) {
  assign("selected_rolling_window", result, envir = environment)
  assign("rolling_raw_input", result$raw_input, envir = environment)
  assign("rolling_raw_future", result$raw_future, envir = environment)
  assign("rolling_transformed_input", result$transformed_input, envir = environment)
  assign("rolling_transformation_state", result$transformation_state, envir = environment)
  assign("rolling_restored_cleaned_input", result$restored_cleaned_input, envir = environment)
  invisible(result)
}

arguments <- commandArgs(trailingOnly = TRUE)
if (sys.nframe() == 0L) {
  if (length(arguments) < 3L || length(arguments) > 5L) {
    stop(
      "Usage: Rscript src/r/qa/inspect_rolling_window.R parent.duckdb windows.duckdb dataset_id [series_id] [window_ordinal]",
      call. = FALSE
    )
  }
  result <- get_rolling_window_qa(
    parent_database = arguments[[1L]],
    windows_database = arguments[[2L]],
    dataset_id = arguments[[3L]],
    series_id = if (length(arguments) >= 4L) arguments[[4L]] else "0",
    window_ordinal = if (length(arguments) >= 5L) arguments[[5L]] else 0L
  )
  expose_rolling_window_qa(result)
  print(selected_rolling_window)
} else {
  required <- c(
    "ROLLING_QA_PARENT_DATABASE",
    "ROLLING_QA_WINDOWS_DATABASE",
    "ROLLING_QA_DATASET_ID"
  )
  missing <- required[!vapply(required, exists, logical(1L), inherits = TRUE)]
  if (length(missing)) {
    stop("Set before source(): ", paste(missing, collapse = ", "), call. = FALSE)
  }
  result <- get_rolling_window_qa(
    parent_database = get("ROLLING_QA_PARENT_DATABASE", inherits = TRUE),
    windows_database = get("ROLLING_QA_WINDOWS_DATABASE", inherits = TRUE),
    dataset_id = get("ROLLING_QA_DATASET_ID", inherits = TRUE),
    series_id = if (exists("ROLLING_QA_SERIES_ID", inherits = TRUE)) {
      get("ROLLING_QA_SERIES_ID", inherits = TRUE)
    } else {
      "0"
    },
    window_ordinal = if (exists("ROLLING_QA_WINDOW_ORDINAL", inherits = TRUE)) {
      get("ROLLING_QA_WINDOW_ORDINAL", inherits = TRUE)
    } else {
      0L
    }
  )
  expose_rolling_window_qa(result)
  message(
    "Created selected_rolling_window and raw/transformed/state objects for ",
    result$dataset_id, "/", result$series_id, " window ", result$window_ordinal, "."
  )
}
