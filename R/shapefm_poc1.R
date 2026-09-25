# Researcher-facing POC 1 functions. Python owns orchestration and all writes.

shapefm_poc1_cli <- function(args) {
  root <- shapefm_project_root()
  uv <- file.path(root, ".tools", "uv", "uv")
  output <- system2(
    uv,
    args = c("run", "--locked", "shapefm-poc1", args),
    stdout = TRUE,
    stderr = TRUE,
    env = shapefm_uv_environment(root)
  )
  status <- attr(output, "status")
  if (!is.null(status) && status != 0L) {
    stop("POC 1 command failed:\n", paste(output, collapse = "\n"))
  }
  jsonlite::fromJSON(tail(output, 1L), simplifyVector = FALSE)
}

shapefm_plan_poc1 <- function(scope = "smoke") {
  plan <- shapefm_poc1_cli(c("plan", "--scope", scope))
  class(plan) <- c("shapefm_poc1_plan", "list")
  plan
}

shapefm_run_poc1 <- function(
    plan, profile = "sequential_safe", workers = NULL,
    device = NULL, batch_size = NULL) {
  args <- c("run", "--experiment-id", plan$experiment_id, "--profile", profile)
  if (!is.null(workers)) args <- c(args, "--workers", as.character(workers))
  if (!is.null(device)) args <- c(args, "--device", device)
  if (!is.null(batch_size)) args <- c(args, "--batch-size", as.character(batch_size))
  shapefm_poc1_cli(args)
}

shapefm_run_gate <- function(
    plan, stage, profile = "sequential_safe", workers = NULL,
    device = NULL, batch_size = NULL) {
  args <- c(
    "run", "--experiment-id", plan$experiment_id,
    "--stage", as.character(stage),
    "--profile", profile
  )
  if (!is.null(workers)) args <- c(args, "--workers", as.character(workers))
  if (!is.null(device)) args <- c(args, "--device", device)
  if (!is.null(batch_size)) args <- c(args, "--batch-size", as.character(batch_size))
  shapefm_poc1_cli(args)
}

shapefm_calibrate_poc1 <- function(
    profile = "mac_m1pro_10core_16gb",
    output = "docs/calibration/mac_m1pro_calibration.json") {
  shapefm_poc1_cli(c(
    "calibrate", "--profile", profile, "--output", output
  ))
}

shapefm_experiment_status <- function(db, plan) {
  experiment <- DBI::dbGetQuery(
    db,
    paste(
      "SELECT experiment_id, name, scope, status, created_at, updated_at",
      "FROM experiments WHERE experiment_id = ?"
    ),
    params = list(plan$experiment_id)
  )
  if (nrow(experiment) != 1L) stop("Experiment was not found: ", plan$experiment_id)
  tasks <- DBI::dbGetQuery(
    db,
    paste(
      "SELECT stage, status, count(*) AS task_count FROM experiment_tasks",
      "WHERE experiment_id = ? GROUP BY stage, status ORDER BY stage, status"
    ),
    params = list(plan$experiment_id)
  )
  invocations <- DBI::dbGetQuery(
    db,
    paste(
      "SELECT requested_gate, status, count(*) AS invocation_count",
      "FROM experiment_invocations WHERE experiment_id = ?",
      "GROUP BY requested_gate, status ORDER BY requested_gate, status"
    ),
    params = list(plan$experiment_id)
  )
  result <- list(
    experiment_id = experiment$experiment_id[[1L]],
    name = experiment$name[[1L]],
    scope = experiment$scope[[1L]],
    status = experiment$status[[1L]],
    created_at = experiment$created_at[[1L]],
    updated_at = experiment$updated_at[[1L]],
    tasks = tasks,
    invocations = invocations
  )
  class(result) <- c("shapefm_experiment_status", "list")
  result
}

shapefm_get_official_results <- function(db, plan) {
  rows <- DBI::dbGetQuery(
    db,
    paste(
      "SELECT e.evaluation_id, e.variant_id, v.cleaning_method,",
      "v.transformation_method, e.candidate, b.configuration_name,",
      "e.evaluator, e.evaluator_revision, e.options, e.metrics,",
      "e.is_complete_manifest, e.is_submittable, e.created_at",
      "FROM official_evaluations e",
      "JOIN experiment_variants v USING (variant_id)",
      "JOIN benchmark_configurations b USING (benchmark_configuration_id)",
      "WHERE e.experiment_id = ?",
      "ORDER BY v.cleaning_method, v.transformation_method, e.candidate"
    ),
    params = list(plan$experiment_id)
  )
  evaluations <- lapply(seq_len(nrow(rows)), function(index) {
    list(
      evaluation_id = rows$evaluation_id[[index]],
      variant_id = rows$variant_id[[index]],
      cleaning = rows$cleaning_method[[index]],
      transformation = rows$transformation_method[[index]],
      candidate = rows$candidate[[index]],
      benchmark_configuration = rows$configuration_name[[index]],
      evaluator = rows$evaluator[[index]],
      evaluator_revision = rows$evaluator_revision[[index]],
      options = jsonlite::fromJSON(as.character(rows$options[[index]]), simplifyVector = FALSE),
      metrics = jsonlite::fromJSON(as.character(rows$metrics[[index]]), simplifyVector = FALSE),
      is_complete_manifest = rows$is_complete_manifest[[index]],
      is_submittable = rows$is_submittable[[index]],
      created_at = rows$created_at[[index]]
    )
  })
  result <- list(
    experiment_id = plan$experiment_id,
    evaluation_count = length(evaluations),
    evaluations = evaluations
  )
  class(result) <- c("shapefm_official_results", "list")
  result
}

shapefm_export_candidate <- function(plan, model_name = "ShapeFM-POC1-provisional") {
  shapefm_poc1_cli(c(
    "export", "--experiment-id", plan$experiment_id, "--model-name", model_name
  ))
}

shapefm_get_forecast <- function(
    db, plan, series_id, candidate = "equal_weight",
    cleaning = "identity", transformation = "minmax_then_standardize") {
  variant <- DBI::dbGetQuery(
    db,
    paste(
      "SELECT variant_id FROM experiment_variants WHERE experiment_id=?",
      "AND cleaning_method=? AND transformation_method=?"
    ),
    params = list(plan$experiment_id, cleaning, transformation)
  )
  if (nrow(variant) != 1L) stop("Experiment variant was not found")
  row <- DBI::dbGetQuery(
    db,
    paste(
      "SELECT f.forecast_id, f.forecast_instance_id, f.mean, f.median,",
      "f.quantile_levels, f.quantiles, i.actual_target FROM forecasts f",
      "JOIN forecast_instances i USING (forecast_instance_id)",
      "WHERE f.experiment_id=? AND f.variant_id=? AND i.series_id=? AND f.candidate=?"
    ),
    params = list(plan$experiment_id, variant$variant_id[[1L]], as.character(series_id), candidate)
  )
  if (nrow(row) != 1L) stop("Forecast was not found")
  result <- list(
    forecast_id = row$forecast_id[[1L]],
    experiment_id = plan$experiment_id,
    variant_id = variant$variant_id[[1L]],
    forecast_instance_id = row$forecast_instance_id[[1L]],
    series_id = as.character(series_id),
    candidate = candidate,
    mean = row$mean[[1L]],
    median = row$median[[1L]],
    quantile_levels = row$quantile_levels[[1L]],
    quantiles = row$quantiles[[1L]],
    actual = row$actual_target[[1L]]
  )
  class(result) <- c("shapefm_forecast", "list")
  result
}

print.shapefm_poc1_plan <- function(x, ...) {
  cat("<shapefm_poc1_plan>\n")
  cat("  experiment: ", x$experiment_id, "\n", sep = "")
  cat("  scope:      ", x$scope, "\n", sep = "")
  cat("  instances:  ", x$instance_count, "\n", sep = "")
  invisible(x)
}

print.shapefm_forecast <- function(x, ...) {
  cat("<shapefm_forecast>\n")
  cat("  series:    ", x$series_id, "\n", sep = "")
  cat("  candidate: ", x$candidate, "\n", sep = "")
  cat("  horizon:   ", length(x$mean), "\n", sep = "")
  invisible(x)
}

print.shapefm_experiment_status <- function(x, ...) {
  cat("<shapefm_experiment_status>\n")
  cat("  experiment:  ", x$experiment_id, "\n", sep = "")
  cat("  status:      ", x$status, "\n", sep = "")
  cat("  task states:\n")
  print(x$tasks, row.names = FALSE)
  cat("  invocations:\n")
  print(x$invocations, row.names = FALSE)
  invisible(x)
}

print.shapefm_official_results <- function(x, ...) {
  old_options <- options(width = max(getOption("width"), 120L))
  on.exit(options(old_options), add = TRUE)
  cat("<shapefm_official_results>\n")
  cat("  experiment:  ", x$experiment_id, "\n", sep = "")
  cat("  evaluations: ", x$evaluation_count, "\n", sep = "")
  if (x$evaluation_count > 0L) {
    summary <- do.call(rbind, lapply(x$evaluations, function(value) {
      data.frame(
        cleaning = value$cleaning,
        transformation = value$transformation,
        candidate = value$candidate,
        MASE = value$metrics[["MASE[0.5]"]],
        sMAPE = value$metrics[["sMAPE[0.5]"]],
        CRPS = value$metrics[["mean_weighted_sum_quantile_loss"]],
        RMSE = value$metrics[["RMSE[mean]"]],
        check.names = FALSE
      )
    }))
    print(summary, row.names = FALSE)
  }
  invisible(x)
}
