#!/usr/bin/env Rscript
# Focused fixture tests for the bounded M4comp2018 read adapter.

source("src/r/01_02_import_m4comp2018.R")

assert <- function(value, message) if (!isTRUE(value)) stop(message, call. = FALSE)
fails <- function(expression, pattern) {
  error <- tryCatch(force(expression), error = identity)
  assert(inherits(error, "error") && grepl(pattern, conditionMessage(error)), pattern)
}

series <- function(id, offset = 0, period = "Daily") list(
  st = id, x = stats::ts(c(1, 2, 3) + offset, frequency = 1), n = 3L,
  type = "Other", h = 2L, period = period, xx = c(4, 5) + offset,
  pt_ff = rbind(c(10, 11) + offset, c(20, 21) + offset, c(30, 31) + offset),
  up_ff = matrix(999, nrow = 3L, ncol = 2L),
  low_ff = matrix(-999, nrow = 3L, ncol = 2L)
)
# A non-Daily prefix proves source_position is relative to the official Daily subset.
M4 <- c(
  list(series("Y1", period = "Yearly")),
  lapply(seq_len(100L), function(index) {
    series(paste0("D", index), (index - 1L) * 100)
  })
)
info <- data.frame(
  ID = c(118L, 245L, 237L), Type = c("Hybrid", "Combination", "Combination"),
  `Author(s)` = c("Smyl", "Montero-Manso, et al.", "Other"),
  Affiliation = c("Uber Technologies", "University", "Other"),
  `Rank(OWA)` = 1:3, check.names = FALSE
)
request <- function(position = 0L, id = "D1", offset = 0) list(
  series_id = as.character(position), source_position = position,
  official_m4_series_id = id, history = as.list(c(1, 2, 3) + offset),
  future = as.list(c(4, 5) + offset), horizon = 2L
)
payload <- list(
  action = "read", dataset_id = "dataset/1", dataset_name = "m4_daily",
  forecast_ids = c("m4_smyl", "m4_fforma"), expected_frequency = "D",
  expected_horizon = 2L, series = list(request())
)

result <- import_m4comp2018(payload, M4, info)
assert(length(result$records) == 2L, "expected two approved records")
assert(identical(vapply(result$records, `[[`, "", "forecast_id"), c("m4_smyl", "m4_fforma")), "providers/order")
assert(identical(vapply(result$records, `[[`, 0L, "submission_id"), c(118L, 245L)), "submission IDs")
assert(identical(vapply(result$records, `[[`, 0L, "submission_rank"), 1:2), "ranks")
assert(identical(vapply(result$records, `[[`, "", "submission_author"), c("Smyl", "Montero-Manso, et al.")), "authors")
assert(all(vapply(result$records, `[[`, "", "forecast_capability") == "mean_only"), "mean-only records")
assert(identical(result$records[[1]]$mean, c(10, 11)), "Smyl pt_ff row")
assert(identical(result$records[[2]]$mean, c(20, 21)), "FFORMA pt_ff row")
assert(identical(result$records[[1]]$official_m4_series_id, "D1"), "D1 mapping")
assert(identical(result$records[[1]]$series_id, "0"), "zero-based mapping")
assert(grepl("^[0-9a-f]{64}$", result$records[[1]]$content_hash), "content hash")

only_smyl <- payload; only_smyl$forecast_ids <- "m4_smyl"
assert(length(import_m4comp2018(only_smyl, M4, info)$records) == 1L, "provider selection")
two <- payload; two$series <- list(request(), request(99L, "D100", 9900))
mapped <- import_m4comp2018(two, M4, info)$records
assert(length(mapped) == 4L, "multiple series")
assert(identical(mapped[[3]]$official_m4_series_id, "D100"), "D100 mapping")
float32 <- payload; float32$series[[1]]$history[[2]] <- 2 + 1e-7
assert(length(import_m4comp2018(float32, M4, info)$records) == 2L, "float32 tolerance")

bad <- payload; bad$action <- "write"; fails(import_m4comp2018(bad, M4, info), "action")
bad <- payload; bad$dataset_name <- "other"; fails(import_m4comp2018(bad, M4, info), "m4_daily")
bad <- payload; bad$expected_frequency <- "M"; fails(import_m4comp2018(bad, M4, info), "frequency")
bad <- payload; bad$forecast_ids <- "other"; fails(import_m4comp2018(bad, M4, info), "approved")
bad <- payload; bad$series[[1]]$official_m4_series_id <- "D2"; fails(import_m4comp2018(bad, M4, info), "official_id")
bad <- payload; bad$series[[1]]$source_position <- 100L; fails(import_m4comp2018(bad, M4, info), "outside")
bad <- payload; bad$series <- list(request(), request()); fails(import_m4comp2018(bad, M4, info), "unique")
bad <- payload; bad$series[[1]]$history[[1]] <- 1.1; fails(import_m4comp2018(bad, M4, info), "history")
bad <- payload; bad$series[[1]]$future[[1]] <- Inf; fails(import_m4comp2018(bad, M4, info), "future")
bad <- payload; bad$series[[1]]$horizon <- 3L; fails(import_m4comp2018(bad, M4, info), "horizon")
bad_M4 <- M4; bad_M4[[2]]$period <- "Monthly"; fails(import_m4comp2018(payload, bad_M4, info), "official_id|outside|Daily")
bad_M4 <- M4; bad_M4[[2]]$pt_ff <- bad_M4[[2]]$pt_ff[, 1, drop = FALSE]
fails(import_m4comp2018(payload, bad_M4, info), "pt_ff")
bad_info <- info; bad_info[1, "ID"] <- 999L; fails(import_m4comp2018(payload, M4, bad_info), "ID/rank")
bad_info <- info[c(2, 1, 3), ]; fails(import_m4comp2018(payload, M4, bad_info), "row order")
bad_info <- info; bad_info[1, "Author(s)"] <- "Wrong"; fails(import_m4comp2018(payload, M4, bad_info), "author")

encoded <- jsonlite::toJSON(result, auto_unbox = TRUE, digits = 17, null = "null")
decoded <- jsonlite::fromJSON(encoded, simplifyVector = FALSE)
assert(identical(decoded$records[[1]]$dataset_id, "dataset/1"), "JSON identity")
assert(isTRUE(all.equal(unlist(decoded$records[[1]]$mean), c(10, 11), tolerance = 0)), "JSON mean")
forbidden <- c("actual", "actuals", "future", "up_ff", "low_ff", "median", "quantiles")
assert(!any(forbidden %in% names(result$records[[1]])), "forbidden values returned")
source_text <- paste(readLines("src/r/01_02_import_m4comp2018.R", warn = FALSE), collapse = "\n")
matches <- function(pattern) sum(gregexpr(pattern, source_text, perl = TRUE)[[1]] > 0L)
assert(matches('utils::data\\("M4"') == 1L, "M4 must load once")
assert(matches('utils::data\\("submission_info"') == 1L, "metadata must load once")
core_text <- sub("# CLI boundary:[\\s\\S]*$", "", source_text, perl = TRUE)
assert(!grepl("utils::data", core_text), "core must only use injected objects")
assert(!grepl("DBI|duckdb|INSERT|UPDATE|DELETE", source_text), "adapter must not write databases")
assert(grepl("digits = 17", source_text, fixed = TRUE), "CLI must encode 17 digits")

policy <- list(version = "cross_source_v1", series_warning_pct = .01,
  population_review_pct = 1, single_series_material_pct = 1,
  directional_impact_review_pct = .1)
reconciled <- payload; reconciled$source_reconciliation <- policy
unchanged <- reconciled
r <- import_m4comp2018(reconciled, M4, info)$reconciliation
assert(r$summary$status == "informational" && r$summary$differing_observations == 0, "identical reconciliation")
reconciled$series[[1]]$history[[1]] <- 1 + 1e-7
r <- import_m4comp2018(reconciled, M4, info)$reconciliation
assert(r$summary$status == "informational" && r$summary$differing_observations == 1, "representation differences")
assert(reconciled$series[[1]]$history[[1]] == 1 + 1e-7, "GIFT values unchanged")
reconciled <- unchanged; reconciled$series[[1]]$history[[1]] <- 1.001
r <- import_m4comp2018(reconciled, M4, info)$reconciliation
assert(r$summary$status == "researcher_review_required" && r$summary$flagged_series == 1, "population breach")
# One warning in 101 series is below 1%; one warning in 100 is exactly 1%.
population <- rep(r$series, 101); population[-1] <- rep(import_m4comp2018(unchanged, M4, info)$reconciliation$series, 100)
assert(.m4_reconciliation_summary(population, policy, list())$status == "warning", "below population threshold")
assert(.m4_reconciliation_summary(population[1:100], policy, list())$status == "warning", "population boundary")
assert(.m4_reconciliation_summary(population[1:99], policy, list())$status == "researcher_review_required", "above population boundary")
boundary <- r$series[[1]]; boundary$maximum_normalized_difference_pct <- .01
assert(.m4_reconciliation_summary(list(boundary), policy, list())$status == "informational", "warning equality")
population[[1]]$maximum_normalized_difference_pct <- 1
assert(.m4_reconciliation_summary(population, policy, list())$status == "warning", "material equality")
population[[1]]$maximum_normalized_difference_pct <- 1 + 1e-8
assert(.m4_reconciliation_summary(population, policy, list())$materially_different_series == 1, "above material boundary")
boundary$maximum_normalized_difference_pct <- 0
boundary$direction_comparisons <- 1000; boundary$directional_label_changes <- 1
assert(.m4_reconciliation_summary(list(boundary), policy, list())$status == "informational", "directional equality")
boundary$directional_label_changes <- 2
assert(.m4_reconciliation_summary(list(boundary), policy, list())$status == "researcher_review_required", "above directional boundary")
reconciled$series[[1]]$history[[1]] <- 2
assert(import_m4comp2018(reconciled, M4, info)$reconciliation$summary$materially_different_series == 1, "material breach")
constant <- series("D1"); constant$x[] <- 0; constant$xx[] <- 0
zero <- request(); zero$history <- as.list(c(0,0,0)); zero$future <- as.list(c(0,0))
assert(.m4_reconcile_series(zero, constant)$series_scale == 1e-12, "zero scale floor")
zero$future[[1]] <- 1e-15
z <- .m4_reconcile_series(zero, constant)
assert(abs(z$maximum_normalized_difference_pct - .1) < 1e-12, "near-zero normalized discrepancy")
assert(z$directional_label_changes == 1 && z$changed_direction_horizons == "1", "strict tie direction impact")
assert(.m4_reconciliation_summary(list(z), policy, list())$status == "researcher_review_required", "direction review")
assert(.m4_difference(c(2,4),c(1,2),10)$rms_normalized_difference_pct == 100 * sqrt(2.5)/10, "independent RMS fixture")
bad <- unchanged; bad$series[[1]]$history <- list(1,2)
fails(import_m4comp2018(bad, M4, info), "structural")
bad <- unchanged; bad$series[[1]]$history[1] <- list(NULL)
fails(import_m4comp2018(bad, M4, info), "structural")
bad <- unchanged; bad$series[[1]]$future[[1]] <- Inf
fails(import_m4comp2018(bad, M4, info), "structural")
bad <- unchanged; bad$series <- list(request(1L, "D2",100),request())
fails(import_m4comp2018(bad, M4, info), "ordering")
bad <- unchanged; bad$series[[1]]$official_m4_series_id <- "D2"
fails(import_m4comp2018(bad, M4, info), "official_id")
assert(identical(unchanged$series, payload$series), "reconciliation never mutates input")

cat("ok - M4comp2018 adapter contract\n")
