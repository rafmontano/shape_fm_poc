#!/usr/bin/env Rscript
# ==============================================================================
# test_paper_table_metrics.R
# Purpose: Frozen arithmetic and contract tests for m4_paper_tables_v1.
# Inputs: Synthetic raw-history fixtures; no historical runner or dependencies.
# Outputs: Assertions on stdout; no files or scientific store changes.
# Run from: Rscript --vanilla src/r/tests/test_paper_table_metrics.R
# ==============================================================================
# Bounded functional exception: pure assertions do not need classes.
before <- loadedNamespaces()
source("src/r/util/forecast_adjustments.R")
source("src/r/util/paper_table_metrics.R")
stopifnot(identical(before, loadedNamespaces()))

# Assert numerical parity with independently derived rational fixture values.
near <- function(x, y) stopifnot(isTRUE(all.equal(unname(x), unname(y), tolerance = 1e-12)))
# Require failure without relying on incidental R error text.
fails <- function(expr) stopifnot(inherits(tryCatch({force(expr); NULL}, error = identity), "error"))
# Build a canonical stored record; each model has an explicitly supplied path.
record <- function(id, context, actual, naive, base, mantis, dtw) {
  list(series_id = id, context = context, actual = actual,
    means = list(naive2 = naive, m4_smyl = base, m4_fforma = base, chronos_2 = base),
    directions = list(directional_mantis_rf = mantis, directional_dtw = dtw))
}
# Build an explicit job (absent lambdas mean baseline, not implicit adjustment).
job <- function(model, retain = FALSE, up = NULL, down = NULL) {
  list(model = model, retain_means = retain, lambda_up = up, lambda_down = down)
}

# Independent hand arithmetic: first lag1 denominator 15/7, second 4.
# First naive errors (4,2), base (2,2); second naive (2,4), base (4,4).
# First base sMAPE = (400/22+400/18)/2 = 2000/99;
# second base = (800/8+800/8)/2 = 100.
request <- list(frequency = "Daily", series = list(
  record("a", c(1, 3, 2, 6, 5, 9, 10, 12), c(12, 8), c(8, 10), c(10, 10), c(0, 1), c(0, 0)),
  record("b", c(2, 6), c(6, 2), c(4, 6), c(2, 6), c(0, 0), c(1, 0))),
  jobs = list(job("naive2"), job("m4_smyl", TRUE), job("directional_mantis_rf"),
              job("directional_dtw"), job("smyl_oracle", TRUE), job("chronos_2", TRUE, 1.1, 0.9)))
results <- evaluate_paper_tables(request)$results
base_smape <- (2000/99 + 100)/2
base_mase <- (14/15 + 1)/2
naive_smape <- ((40 + 200/9)/2 + (40 + 100)/2)/2
naive_mase <- (7/5 + 3/4)/2
near(results[[2]]$metrics$smape, base_smape)
near(results[[2]]$metrics$mase, base_mase)
near(results[[2]]$metrics$owa, (base_smape/naive_smape + base_mase/naive_mase)/2)
near(results[[1]]$metrics$owa, 1)
stopifnot(identical(results[[2]]$correct_counts, c(2L, 2L)),
          identical(results[[3]]$correct_counts, c(2L, 1L)),
          identical(results[[4]]$correct_counts, c(1L, 2L)),
          results[[3]]$metrics$da == 0.5, results[[3]]$evaluation_count == 2L,
          is.null(results[[3]]$metrics$smape), is.null(results[[3]]$metrics$mase),
          is.null(results[[3]]$metrics$owa), !"adjusted_means" %in% names(results[[1]]))
near(results[[6]]$adjusted_means[[1]]$mean, c(11, 11))
near(results[[6]]$adjusted_means[[2]]$mean, c(2, 6))
# Adjusted first errors (1,3): frozen sMAPE (200/23+600/19)/2; same MASE.
adjusted_smape <- ((200/23 + 600/19)/2 + 100)/2
near(results[[6]]$metrics$smape, adjusted_smape)
near(results[[6]]$metrics$mase, base_mase)
near(results[[6]]$metrics$owa, (adjusted_smape/naive_smape + base_mase/naive_mase)/2)
stopifnot(results[[5]]$adjusted_means[[1]]$oracle$method_id == .SMYL_ORACLE_METHOD_ID)
for (i in 1:2) near(results[[5]]$adjusted_means[[i]]$mean,
  calculate_smyl_oracle(request$series[[i]]$actual, request$series[[i]]$means$m4_smyl)$adjusted_mean)
cat("ok - frozen asymmetric component means, ratio-of-means OWA and horizon counts\n")

near(calculate_mantis_adjustment(c(8, 12), 10, 1, 1.1, .9), c(8, 12))
near(calculate_mantis_adjustment(c(8, 10), 10, 0, 1.1, .9), c(8, 10))
near(calculate_mantis_adjustment(c(8, 10), 10, 1, 1.1, .9), c(8.8, 11))
near(calculate_mantis_adjustment(c(8, 12), 10, 0, 1.1, .9), c(7.2, 10.8))
up <- seq(1, 1.12, .005)
down <- seq(1, .9, -.005)
stopifnot(length(up) == 25L, length(down) == 21L, length(up)*length(down) == 525L)
near(range(up), c(1, 1.12)); near(range(down), c(.9, 1))
for (u in up) for (d in down) {
  near(calculate_mantis_adjustment(c(8, 10), 10, 1, u, d), u*c(8, 10))
  near(calculate_mantis_adjustment(c(8, 12), 10, 0, u, d), d*c(8, 12))
}
cat("ok - agreement, both disagreements, strict terminal ties and full grid\n")

near(calculate_paper_point_metrics(c(1,3), c(0,2), c(0,1), "Daily"), c(200/3, 1/4))
near(calculate_paper_point_metrics(request$series[[1]]$context, c(12,8), c(10,10), "Daily")[2], 14/15)
# Lag7 denominator would be 11 and MASE 2/11, intentionally not the accepted value.
stopifnot(abs(14/15 - 2/11) > .5)
near(vapply(c("Hourly","Daily","Weekly","Monthly","Quarterly","Yearly"),
            paper_mase_lag, integer(1)), c(24,1,1,12,4,1))
cat("ok - zero/zero omission, Daily lag1 not lag7 and frequency mapping\n")

for (bad in list(NULL, numeric(), TRUE, "1", NA_real_, Inf, NaN, matrix(1), 1+1i)) {
  fails(calculate_mantis_adjustment(bad, 1, 1, 1.1, .9))
  fails(calculate_mantis_adjustment(c(1,2), bad, 1, 1.1, .9))
  fails(calculate_mantis_adjustment(c(1,2), 1, 1, bad, .9))
}
fails(calculate_mantis_adjustment(c(1,2), 1, 2, 1.1, .9))
fails(calculate_mantis_adjustment(c(1,2), c(1,2), 1, 1.1, .9))
fails(calculate_mantis_adjustment(1e308, 0, 0, 1, 10))
fails(calculate_paper_point_metrics(c(1,1), 1, 2, "Daily"))
fails(calculate_paper_point_metrics(1, 1, 2, "Daily"))
fails(calculate_paper_point_metrics(c(1,2), 0, 0, "Daily"))
fails(calculate_paper_point_metrics(c(1,2), -1e308, 1e308, "Daily"))
fails(calculate_paper_point_metrics(c(1,2), 1e308, 1e308, "Daily"))
fails(calculate_paper_point_metrics(c(1,2), 1, c(1,2), "Daily"))
for (mutation in c("mean", "direction", "horizon", "nonfinite", "binary", "duplicate",
                   "model", "lambda", "retain", "reference", "frequency")) {
  invalid <- request
  if (mutation == "mean") invalid$series[[1]]$means$m4_fforma <- NULL
  if (mutation == "direction") invalid$series[[1]]$directions$directional_dtw <- NULL
  if (mutation == "horizon") invalid$series[[2]]$actual <- 1
  if (mutation == "nonfinite") invalid$series[[1]]$actual[1] <- Inf
  if (mutation == "binary") invalid$series[[1]]$directions$directional_dtw[1] <- 2
  if (mutation == "duplicate") invalid$series[[2]]$series_id <- "a"
  if (mutation == "model") invalid$jobs[[1]]$model <- "unknown"
  if (mutation == "lambda") invalid$jobs[[6]]$lambda_down <- NULL
  if (mutation == "retain") invalid$jobs[[1]]$retain_means <- 1
  if (mutation == "reference") for (i in 1:2) invalid$series[[i]]$means$naive2 <- invalid$series[[i]]$actual
  if (mutation == "frequency") invalid$frequency <- "unknown"
  fails(evaluate_paper_tables(invalid))
}
# Batch partition/reordering are operational only; results retain input job order.
for (i in seq_along(request$jobs)) {
  single <- request; single$jobs <- request$jobs[i]
  stopifnot(identical(evaluate_paper_tables(single)$results[[1]], results[[i]]))
}
cat("ok - invalid inputs rejected and job partition invariance\n")
cat("All 4 paper metric test groups passed.\n")
