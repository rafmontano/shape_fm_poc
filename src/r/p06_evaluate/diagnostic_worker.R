#!/usr/bin/env Rscript
# Process 06 selected-output worker (docs/evaluation.md).
# Inputs: stored canonical origins, accepted forecasts/directions and selected jobs.
# Outputs: selected-only adjusted vectors; no metric rescoring, fitting or storage.
source("src/r/p06_evaluate/adjustments.R")

# Retain the selected diagnostic path without recomputing its stored surface metrics.
retain_selected <- function(job) {
  outputs <- lapply(job$series, function(series) {
    mean <- if (identical(job$model, "smyl_oracle")) {
      calculate_smyl_oracle(unlist(series$actual), unlist(series$means$m4_smyl))$adjusted_mean
    } else {
      calculate_mantis_adjustment(unlist(series$means[[job$model]]),
        tail(unlist(series$context), 1), tail(unlist(series$directions$directional_mantis_rf), 1),
        job$lambda_up, job$lambda_down)
    }
    list(series_id = series$series_id, mean = I(mean))
  })
  list(key = job$key, adjusted_means = outputs)
}

request <- jsonlite::fromJSON(file("stdin"), simplifyVector = FALSE)
cat(jsonlite::toJSON(list(results = lapply(request$jobs, retain_selected)),
                    auto_unbox = TRUE, null = "null", digits = 17))
