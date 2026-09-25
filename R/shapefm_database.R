# Read-only researcher interface to the canonical ShapeFM DuckDB database.

shapefm_database_path <- function() {
  root <- shapefm_project_root()
  value <- Sys.getenv("SHAPEFM_DATABASE", unset = "")
  normalizePath(
    if (nzchar(value)) value else file.path(root, "data", "shapefm.duckdb"),
    mustWork = FALSE
  )
}

shapefm_open <- function(path = shapefm_database_path()) {
  if (!file.exists(path)) stop("ShapeFM database does not exist: ", path)
  DBI::dbConnect(
    duckdb::duckdb(shared_home = FALSE),
    dbdir = path,
    read_only = TRUE
  )
}

shapefm_close <- function(db) {
  DBI::dbDisconnect(db, shutdown = TRUE)
  invisible(NULL)
}

shapefm_resolve_dataset <- function(db, dataset, stage = "import") {
  row <- DBI::dbGetQuery(
    db,
    paste(
      "SELECT d.dataset_id, d.dataset_name, r.run_id",
      "FROM datasets d JOIN runs r ON r.dataset_id = d.dataset_id",
      "WHERE (d.dataset_name = ? OR d.dataset_id = ?) AND r.stage = ?",
      "ORDER BY r.ended_at DESC NULLS LAST, r.started_at DESC LIMIT 1"
    ),
    params = list(dataset, dataset, stage)
  )
  if (nrow(row) != 1L) stop("No ", stage, " dataset found for: ", dataset)
  row
}

shapefm_get_series <- function(db, dataset, series_id) {
  selected <- shapefm_resolve_dataset(db, dataset)
  row <- DBI::dbGetQuery(
    db,
    paste(
      "SELECT series_id, source_series_id, frequency, start_timestamp, target,",
      "observation_count, content_hash FROM series",
      "WHERE dataset_id = ? AND series_id = ?"
    ),
    params = list(selected$dataset_id[[1L]], as.character(series_id))
  )
  if (nrow(row) != 1L) stop("Series was not found: ", series_id)
  windows <- DBI::dbGetQuery(
    db,
    paste(
      "SELECT window_id, split_name, train_start, train_end,",
      "validation_start, validation_end, test_start, test_end,",
      "horizon, boundary_convention FROM evaluation_windows",
      "WHERE dataset_id = ? AND series_id = ? ORDER BY window_id"
    ),
    params = list(selected$dataset_id[[1L]], as.character(series_id))
  )
  result <- list(
    dataset_id = selected$dataset_id[[1L]],
    dataset_name = selected$dataset_name[[1L]],
    series_id = row$series_id[[1L]],
    source_series_id = row$source_series_id[[1L]],
    frequency = row$frequency[[1L]],
    start_timestamp = row$start_timestamp[[1L]],
    target = row$target[[1L]],
    observation_count = row$observation_count[[1L]],
    content_hash = row$content_hash[[1L]],
    evaluation_windows = windows
  )
  class(result) <- "shapefm_series"
  result
}

shapefm_stage_status <- function(db, stage = "import", dataset = "m4_daily") {
  selected <- shapefm_resolve_dataset(db, dataset, stage)
  tasks <- DBI::dbGetQuery(
    db,
    "SELECT status, count(*) AS task_count FROM tasks WHERE run_id = ? GROUP BY status",
    params = list(selected$run_id[[1L]])
  )
  totals <- DBI::dbGetQuery(
    db,
    paste(
      "SELECT count(*) AS series_count,",
      "coalesce(sum(observation_count), 0) AS observation_count",
      "FROM series WHERE dataset_id = ?"
    ),
    params = list(selected$dataset_id[[1L]])
  )
  run_status <- DBI::dbGetQuery(
    db, "SELECT status FROM runs WHERE run_id = ?", params = list(selected$run_id[[1L]])
  )$status[[1L]]
  invocation_count <- DBI::dbGetQuery(
    db,
    "SELECT count(*) AS invocation_count FROM run_invocations WHERE run_id = ?",
    params = list(selected$run_id[[1L]])
  )$invocation_count[[1L]]
  result <- list(
    stage = stage,
    dataset_id = selected$dataset_id[[1L]],
    dataset_name = selected$dataset_name[[1L]],
    run_id = selected$run_id[[1L]],
    run_status = run_status,
    invocation_count = invocation_count,
    tasks = tasks,
    series_count = totals$series_count[[1L]],
    observation_count = totals$observation_count[[1L]]
  )
  class(result) <- "shapefm_stage_status"
  result
}

print.shapefm_series <- function(x, ...) {
  window <- x$evaluation_windows[1L, , drop = FALSE]
  cat("<shapefm_series>\n")
  cat("  dataset:     ", x$dataset_name, " (", x$dataset_id, ")\n", sep = "")
  cat("  series:      ", x$series_id, "\n", sep = "")
  cat("  frequency:   ", x$frequency, "\n", sep = "")
  cat("  start:       ", format(x$start_timestamp), "\n", sep = "")
  cat("  observations:", x$observation_count, "\n")
  cat(
    "  training:     [", window$train_start, ", ", window$train_end, ")\n",
    sep = ""
  )
  cat(
    "  validation:   [", window$validation_start, ", ", window$validation_end, ")\n",
    sep = ""
  )
  cat(
    "  test:         [", window$test_start, ", ", window$test_end, ")\n",
    sep = ""
  )
  cat("  target:      ", paste(utils::head(x$target, 5L), collapse = ", "), " ... ",
      paste(utils::tail(x$target, 5L), collapse = ", "), "\n", sep = "")
  invisible(x)
}

print.shapefm_stage_status <- function(x, ...) {
  cat("<shapefm_stage_status>\n")
  cat("  dataset:     ", x$dataset_name, " (", x$dataset_id, ")\n", sep = "")
  cat("  run:         ", x$run_id, " [", x$run_status, "]\n", sep = "")
  cat("  invocations: ", x$invocation_count, "\n", sep = "")
  cat("  series:      ", x$series_count, "\n", sep = "")
  cat("  observations:", x$observation_count, "\n")
  print(x$tasks, row.names = FALSE)
  invisible(x)
}
