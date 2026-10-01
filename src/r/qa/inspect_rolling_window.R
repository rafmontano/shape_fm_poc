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

  series <- DBI::dbGetQuery(
    parent,
    paste(
      "SELECT l.series_key, s.frequency, s.target FROM series s",
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
      "w.package_versions, w.worker_provenance",
      "FROM prepared_windows w JOIN series_membership m",
      "USING (preparation_id, series_key)",
      "WHERE w.series_key=? AND w.window_ordinal=?"
    ),
    params = list(series$series_key[[1L]], window_ordinal)
  )
  if (nrow(prepared) != 1L) stop("Expected exactly one prepared window", call. = FALSE)

  target <- as.numeric(series$target[[1L]])
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
