# ==============================================================================
# paper_table_metrics.R
# Purpose: Single m4_paper_tables_v1 scientific kernel for bounded Process 06 jobs.
# Inputs: Raw context, future actuals, stored means/directions and explicit jobs.
# Outputs: Aggregate metrics, per-horizon counts and optional diagnostic means.
# Run from: Imported; not run directly.
# ==============================================================================
# Bounded functional exception: stateless arithmetic/validation needs no classes.
# Adjustment functions are supplied by forecast_adjustments.R; no I/O or scheduler.

# Require a finite real path, optionally with an exact horizon.
.paper_path <- function(x, name, horizon = NULL) {
  if (!is.numeric(x) || is.complex(x) || !is.null(dim(x)) || !length(x) ||
      any(!is.finite(x)) || (!is.null(horizon) && length(x) != horizon)) {
    stop(paste(name, "must be a finite numeric vector of the required horizon"), call. = FALSE)
  }
}

# Return the accepted M4 seasonal denominator lag; Daily deliberately uses 1.
paper_mase_lag <- function(frequency) {
  if (!is.character(frequency) || length(frequency) != 1L || is.na(frequency) ||
      !frequency %in% c("Hourly", "Daily", "Weekly", "Monthly", "Quarterly", "Yearly")) {
    stop("unsupported frequency", call. = FALSE)
  }
  switch(frequency, Hourly = 24L, Monthly = 12L, Quarterly = 4L, 1L)
}

# Calculate full-horizon historical component errors; omit only zero/zero sMAPE.
calculate_paper_point_metrics <- function(context, actual, mean, frequency) {
  .paper_path(context, "context")
  .paper_path(actual, "actual")
  .paper_path(mean, "mean", length(actual))
  lag <- paper_mase_lag(frequency)
  if (length(context) <= lag) stop("context too short for MASE lag", call. = FALSE)
  differences <- abs(context[(lag + 1L):length(context)] -
                       context[seq_len(length(context) - lag)])
  denominator <- mean(differences)
  errors <- abs(actual - mean)
  smape_denominators <- abs(actual) + abs(mean)
  smape_terms <- 200 * errors / smape_denominators
  zerozero <- actual == 0 & mean == 0
  if (any(!is.finite(differences)) || !is.finite(denominator) || denominator <= 0 ||
      any(!is.finite(errors)) || any(!is.finite(smape_denominators)) ||
      any(!is.finite(smape_terms[!zerozero]))) {
    stop("nonfinite error or zero/nonfinite MASE denominator", call. = FALSE)
  }
  result <- c(smape = mean(smape_terms, na.rm = TRUE), mase = mean(errors / denominator))
  if (any(!is.finite(result))) stop("nonfinite point metrics", call. = FALSE)
  result
}

# Validate an entire shared cohort once, preserving raw origins and model identities.
.validate_paper_request <- function(request) {
  paper_mase_lag(request$frequency)
  if (!is.list(request$series) || !length(request$series) ||
      !is.list(request$jobs) || !length(request$jobs)) stop("series and jobs required", call. = FALSE)
  horizon <- length(request$series[[1]]$actual)
  ids <- character(length(request$series))
  for (i in seq_along(request$series)) {
    s <- request$series[[i]]
    if (!is.character(s$series_id) || length(s$series_id) != 1L ||
        is.na(s$series_id) || !nzchar(s$series_id)) stop("series_id required", call. = FALSE)
    ids[i] <- s$series_id
    .paper_path(s$context, "context")
    .paper_path(s$actual, "actual", horizon)
    for (model in c("naive2", "m4_smyl", "m4_fforma", "chronos_2")) {
      .paper_path(s$means[[model]], paste("means", model), horizon)
    }
    for (model in c("directional_mantis_rf", "directional_dtw")) {
      d <- s$directions[[model]]
      .paper_path(d, paste("directions", model), horizon)
      if (any(!d %in% c(0, 1))) stop("directions must be binary", call. = FALSE)
    }
  }
  if (anyDuplicated(ids)) stop("duplicate series_id", call. = FALSE)
  for (job in request$jobs) {
    if (!is.character(job$model) || length(job$model) != 1L || is.na(job$model) ||
        !job$model %in% c("naive2", "m4_smyl", "m4_fforma", "chronos_2",
                          "smyl_oracle", "directional_mantis_rf", "directional_dtw")) {
      stop("unsupported model", call. = FALSE)
    }
    if (!is.logical(job$retain_means) || length(job$retain_means) != 1L ||
        is.na(job$retain_means)) stop("retain_means must be boolean", call. = FALSE)
    adjusted <- !is.null(job$lambda_up) || !is.null(job$lambda_down)
    if (adjusted) {
      if (!job$model %in% c("m4_smyl", "chronos_2")) stop("model cannot be adjusted", call. = FALSE)
      for (field in c("lambda_up", "lambda_down")) {
        .paper_path(job[[field]], field, 1L)
      }
    }
  }
  horizon
}

# Evaluate one job in one series pass; derive all direction counts from raw origin.
.evaluate_paper_job <- function(series, job, frequency, horizon, reference) {
  direct <- job$model %in% c("directional_mantis_rf", "directional_dtw")
  sums <- c(smape = 0, mase = 0)
  counts <- integer(horizon)
  retained <- list()
  for (s in series) {
    origin <- tail(s$context, 1)
    oracle <- NULL
    if (direct) {
      predicted <- s$directions[[job$model]]
    } else {
      path <- if (job$model == "smyl_oracle") {
        oracle <- calculate_smyl_oracle(s$actual, s$means$m4_smyl)
        oracle$adjusted_mean
      } else s$means[[job$model]]
      if (!is.null(job$lambda_up)) {
        path <- calculate_mantis_adjustment(path, origin,
          tail(s$directions$directional_mantis_rf, 1), job$lambda_up, job$lambda_down)
      }
      sums <- sums + calculate_paper_point_metrics(s$context, s$actual, path, frequency)
      predicted <- as.integer(path > origin)
      if (job$retain_means) {
        item <- list(series_id = s$series_id, mean = path)
        if (!is.null(oracle)) item$oracle <- oracle[names(oracle) != "adjusted_mean"]
        retained[[length(retained) + 1L]] <- item
      }
    }
    counts <- counts + as.integer(predicted == as.integer(s$actual > origin))
  }
  components <- sums / length(series)
  owa <- if (direct) NULL else 0.5 * sum(components / reference)
  if (!direct && any(!is.finite(c(components, owa)))) stop("nonfinite aggregate", call. = FALSE)
  result <- list(model = job$model, lambda_up = job$lambda_up, lambda_down = job$lambda_down,
    metrics = list(smape = if (direct) NULL else unname(components["smape"]),
                   mase = if (direct) NULL else unname(components["mase"]),
                   owa = owa, da = tail(counts, 1) / length(series)),
    correct_counts = counts, evaluation_count = length(series))
  if (job$retain_means) result$adjusted_means <- retained
  result
}

# Purpose: Evaluate bounded jobs through one common sequential/distributed kernel.
# Inputs: Validated JSON-shaped request; all models share the same raw cohort.
# Outputs: Results only (operational evidence belongs to the JSON worker).
evaluate_paper_tables <- function(request) {
  horizon <- .validate_paper_request(request)
  reference <- Reduce(`+`, lapply(request$series, function(s) {
    calculate_paper_point_metrics(s$context, s$actual, s$means$naive2, request$frequency)
  })) / length(request$series)
  if (any(!is.finite(reference)) || any(reference <= 0)) {
    stop("zero/nonfinite Naive2 OWA denominator", call. = FALSE)
  }
  list(results = lapply(request$jobs, function(job) {
    .evaluate_paper_job(request$series, job, request$frequency, horizon, reference)
  }))
}
