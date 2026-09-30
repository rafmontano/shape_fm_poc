#!/usr/bin/env Rscript
# ==============================================================================
# get_m4_daily_series.R
#
# Purpose: Retrieve one canonical M4 Daily time series from ShapeFM's DuckDB.
# Inputs: A ShapeFM series ID, DuckDB path, and optional exact dataset ID.
# Outputs: selected_m4_series with the exact ten M4comp2018 element components.
#   M4 metadata/forecasts come from the package; x and xx come from DuckDB.
# Run from the repository root:
#   Rscript src/r/qa/get_m4_daily_series.R 0 data/shapefm.duckdb
# Or source this file in R/RStudio; selected_m4_series remains in the workspace.
# ==============================================================================

# Purpose: Validate a required nonempty scalar character argument.
# Inputs: One value and its human-readable argument name.
# Outputs: The value as character; raises a clear error for malformed input.
.qa_scalar_character <- function(value, name) {
  value <- as.character(value)
  if (length(value) != 1L || is.na(value) || !nzchar(value)) {
    stop(name, " must be one nonempty value", call. = FALSE)
  }
  value
}

# Purpose: Compare DuckDB float32 observations with M4comp2018 double values.
# Inputs: Two numeric vectors expected to represent the same M4 observations.
# Outputs: One logical using strict float32 representation tolerance.
.qa_float32_equal <- function(observed, expected) {
  if (length(observed) != length(expected) ||
      any(is.na(observed)) || any(is.na(expected)) ||
      any(!is.finite(observed)) || any(!is.finite(expected))) {
    return(FALSE)
  }
  tolerance <- pmax(1e-6, abs(expected) * 2^-23)
  all(abs(observed - expected) <= tolerance)
}

# Purpose: Retrieve one imported canonical M4 Daily series without modifying DuckDB.
# Inputs: ShapeFM's zero-based series ID, database path, and optionally an exact
#   dataset ID when the database contains more than one M4 Daily import.
# Outputs: One exact M4comp2018 element structure. Package metadata and submitted
#   forecasts are retained; package x/xx are replaced by the matching canonical
#   observations read from DuckDB. ShapeFM provenance is attached as an attribute.
# Notes: Gate 1 imported the canonical series through GIFT-Eval. This QA function
#   independently validates its identity and values against M4comp2018 before use.
get_m4_daily_series <- function(
    series_id = "0",
    database_path = "data/shapefm.duckdb",
    dataset_id = NULL) {
  if (!requireNamespace("DBI", quietly = TRUE) ||
      !requireNamespace("duckdb", quietly = TRUE) ||
      !requireNamespace("M4comp2018", quietly = TRUE)) {
    stop("R packages 'DBI', 'duckdb', and 'M4comp2018' are required", call. = FALSE)
  }

  series_id <- .qa_scalar_character(series_id, "series_id")
  database_path <- .qa_scalar_character(database_path, "database_path")
  if (!is.null(dataset_id)) {
    dataset_id <- .qa_scalar_character(dataset_id, "dataset_id")
  }
  if (!file.exists(database_path)) {
    stop("DuckDB file does not exist: ", database_path, call. = FALSE)
  }

  database_path <- normalizePath(database_path, mustWork = TRUE)
  connection <- DBI::dbConnect(
    duckdb::duckdb(shared_home = FALSE),
    dbdir = database_path,
    read_only = TRUE
  )
  on.exit(DBI::dbDisconnect(connection, shutdown = TRUE), add = TRUE)

  required_tables <- c("datasets", "series", "evaluation_windows")
  missing_tables <- required_tables[!vapply(
    required_tables,
    DBI::dbExistsTable,
    logical(1L),
    conn = connection
  )]
  if (length(missing_tables)) {
    stop(
      "DuckDB is missing required ShapeFM tables: ",
      paste(missing_tables, collapse = ", "),
      call. = FALSE
    )
  }

  if (is.null(dataset_id)) {
    datasets <- DBI::dbGetQuery(
      connection,
      paste(
        "SELECT dataset_id, dataset_name, source_system, source_revision, frequency",
        "FROM datasets WHERE dataset_name = ? AND frequency = 'D'",
        "ORDER BY created_at DESC"
      ),
      params = list("m4_daily")
    )
    if (nrow(datasets) == 0L) {
      stop("No imported M4 Daily dataset was found", call. = FALSE)
    }
    if (nrow(datasets) > 1L) {
      stop(
        "More than one M4 Daily dataset was found; pass an exact dataset_id. Candidates: ",
        paste(datasets$dataset_id, collapse = ", "),
        call. = FALSE
      )
    }
  } else {
    datasets <- DBI::dbGetQuery(
      connection,
      paste(
        "SELECT dataset_id, dataset_name, source_system, source_revision, frequency",
        "FROM datasets",
        "WHERE dataset_id = ? AND dataset_name = ? AND frequency = 'D'"
      ),
      params = list(dataset_id, "m4_daily")
    )
    if (nrow(datasets) != 1L) {
      stop("dataset_id is not an imported M4 Daily dataset: ", dataset_id, call. = FALSE)
    }
  }

  dataset_id <- datasets$dataset_id[[1L]]
  series <- DBI::dbGetQuery(
    connection,
    paste(
      "SELECT series_id, source_series_id, source_row, frequency,",
      "start_timestamp, target, observation_count, content_hash",
      "FROM series WHERE dataset_id = ? AND series_id = ?"
    ),
    params = list(dataset_id, series_id)
  )
  if (nrow(series) != 1L) {
    stop(
      "Series ID ", series_id, " was not found in M4 Daily dataset ", dataset_id,
      call. = FALSE
    )
  }

  target <- as.numeric(series$target[[1L]])
  if (length(target) != series$observation_count[[1L]]) {
    stop("Stored target length does not match observation_count", call. = FALSE)
  }
  source_row <- as.integer(series$source_row[[1L]])
  if (is.na(source_row) || source_row < 0L || !identical(series_id, as.character(source_row))) {
    stop("Stored M4 Daily series identity is inconsistent with source_row", call. = FALSE)
  }

  windows <- DBI::dbGetQuery(
    connection,
    paste(
      "SELECT window_id, split_name, train_start, train_end,",
      "validation_start, validation_end, test_start, test_end, horizon,",
      "boundary_convention FROM evaluation_windows",
      "WHERE dataset_id = ? AND series_id = ? ORDER BY window_id"
    ),
    params = list(dataset_id, series_id)
  )
  if (nrow(windows) != 1L) {
    stop("Expected exactly one official M4 Daily evaluation window", call. = FALSE)
  }
  test_start <- as.integer(windows$test_start[[1L]])
  test_end <- as.integer(windows$test_end[[1L]])
  horizon <- as.integer(windows$horizon[[1L]])
  if (test_start < 1L || test_end != length(target) ||
      test_end - test_start != horizon) {
    stop("Stored M4 Daily evaluation boundary is inconsistent", call. = FALSE)
  }
  history <- target[seq_len(test_start)]
  future <- target[seq.int(test_start + 1L, test_end)]

  # Load the package object once, select the same zero-based Daily position, and
  # preserve its metadata and all submitted point/interval forecast matrices.
  package_objects <- new.env(parent = emptyenv())
  utils::data("M4", package = "M4comp2018", envir = package_objects)
  if (!exists("M4", package_objects, inherits = FALSE)) {
    stop("M4comp2018::M4 is unavailable", call. = FALSE)
  }
  M4 <- package_objects$M4
  daily_positions <- which(vapply(
    M4,
    function(value) identical(as.character(value$period), "Daily"),
    logical(1L)
  ))
  if (source_row >= length(daily_positions)) {
    stop("Stored source position is outside M4comp2018 Daily series", call. = FALSE)
  }
  package_series <- M4[[daily_positions[[source_row + 1L]]]]
  expected_names <- c(
    "st", "x", "n", "type", "h", "period", "xx", "pt_ff", "up_ff", "low_ff"
  )
  if (!identical(names(package_series), expected_names) ||
      !identical(as.character(package_series$st), paste0("D", source_row + 1L)) ||
      !identical(as.character(package_series$period), "Daily") ||
      as.integer(package_series$n) != length(history) ||
      as.integer(package_series$h) != horizon ||
      !.qa_float32_equal(history, as.numeric(package_series$x)) ||
      !.qa_float32_equal(future, as.numeric(package_series$xx))) {
    stop(
      "DuckDB series does not match the selected M4comp2018 Daily element",
      call. = FALSE
    )
  }

  # Keep the package's original time bases so the documented M4 plotting example
  # works unchanged, while the scientific x/xx values come from DuckDB.
  result <- package_series
  result$x <- stats::ts(
    history,
    start = stats::start(package_series$x),
    frequency = stats::frequency(package_series$x)
  )
  result$xx <- stats::ts(
    future,
    start = stats::start(package_series$xx),
    frequency = stats::frequency(package_series$xx)
  )
  attr(result, "shape_fm_provenance") <- list(
    dataset_id = dataset_id,
    dataset_name = datasets$dataset_name[[1L]],
    source_system = datasets$source_system[[1L]],
    source_revision = datasets$source_revision[[1L]],
    series_id = series_id,
    source_series_id = series$source_series_id[[1L]],
    source_position = source_row,
    official_m4_series_id = paste0("D", source_row + 1L),
    frequency = series$frequency[[1L]],
    start_timestamp = series$start_timestamp[[1L]],
    observation_count = series$observation_count[[1L]],
    missing_count = sum(is.na(target)),
    content_hash = series$content_hash[[1L]],
    evaluation_windows = windows
  )
  result
}

# Purpose: Print concise QA metadata without dumping every M4 forecast matrix.
# Inputs: A value returned by get_m4_daily_series().
# Outputs: Visible identity/shape/source details; returns x invisibly.
print_m4_qa_summary <- function(x) {
  provenance <- attr(x, "shape_fm_provenance", exact = TRUE)
  if (is.null(provenance)) {
    stop("Object has no ShapeFM provenance", call. = FALSE)
  }
  cat("ShapeFM M4 Daily series\n")
  cat("  dataset_id: ", provenance$dataset_id, "\n", sep = "")
  cat("  ShapeFM series_id: ", provenance$series_id, "\n", sep = "")
  cat("  M4 series ID (st): ", x$st, "\n", sep = "")
  cat("  type: ", as.character(x$type), "\n", sep = "")
  cat("  period: ", as.character(x$period), "\n", sep = "")
  cat("  historical observations (n): ", x$n, "\n", sep = "")
  cat("  future observations (h): ", x$h, "\n", sep = "")
  cat("  submitted forecasts: ", nrow(x$pt_ff), "\n", sep = "")
  cat("  content_hash: ", provenance$content_hash, "\n", sep = "")
  invisible(x)
}

# Execution boundary: direct Rscript execution prints a temporary object because
# that R process necessarily exits. source() creates selected_m4_series in the
# caller's workspace; optional M4_QA_* variables select a non-default series.
arguments <- commandArgs(trailingOnly = TRUE)
if (sys.nframe() == 0L) {
  if (length(arguments) > 3L || any(arguments %in% c("-h", "--help"))) {
    cat(
      "Usage: Rscript src/r/qa/get_m4_daily_series.R ",
      "[series_id] [database_path] [dataset_id]\n",
      "Defaults: series_id=0, database_path=data/shapefm.duckdb\n",
      sep = ""
    )
    quit(status = if (length(arguments) > 3L) 2L else 0L)
  }
  selected_m4_series <- get_m4_daily_series(
    series_id = if (length(arguments) >= 1L) arguments[[1L]] else "0",
    database_path = if (length(arguments) >= 2L) arguments[[2L]] else "data/shapefm.duckdb",
    dataset_id = if (length(arguments) >= 3L) arguments[[3L]] else NULL
  )
  print_m4_qa_summary(selected_m4_series)
} else {
  selected_m4_series <- get_m4_daily_series(
    series_id = if (exists("M4_QA_SERIES_ID", inherits = TRUE)) {
      get("M4_QA_SERIES_ID", inherits = TRUE)
    } else {
      "0"
    },
    database_path = if (exists("M4_QA_DATABASE_PATH", inherits = TRUE)) {
      get("M4_QA_DATABASE_PATH", inherits = TRUE)
    } else {
      "data/shapefm.duckdb"
    },
    dataset_id = if (exists("M4_QA_DATASET_ID", inherits = TRUE)) {
      get("M4_QA_DATASET_ID", inherits = TRUE)
    } else {
      NULL
    }
  )
  message(
    "Created selected_m4_series for ", selected_m4_series$st,
    " with M4comp2018 metadata/forecasts and DuckDB x/xx values."
  )
}
