#!/usr/bin/env Rscript
# ==============================================================================
# test_directional_forecast_metrics.R
#
# Purpose: Prove historical arithmetic, unavailable contracts and dormant isolation.
# Inputs: Fixed finite fixtures and current repository source; no historical runtime.
# Outputs: Named assertions on stdout; failures exit nonzero; writes nothing.
# Run from: Rscript src/r/tests/test_directional_forecast_metrics.R
# ==============================================================================

# Bounded functional exception: deterministic assertions need no stateful classes.
packages_before <- loadedNamespaces()
source("src/r/util/directional_forecast_metrics.R")
stopifnot(identical(loadedNamespaces(), packages_before))

# Run a named assertion group and report success only after all assertions pass.
check <- function(name, assertion) {
  assertion()
  cat(sprintf("ok - %s\n", name))
}

# Compare numeric results against independently frozen values at tight tolerance.
expect_numeric <- function(observed, expected) {
  stopifnot(isTRUE(all.equal(observed, expected, tolerance = 1e-12)))
}

# Require a malformed public request to fail with the expected contract message.
expect_error <- function(expression, message) {
  error <- tryCatch({ force(expression); NULL }, error = identity)
  stopifnot(inherits(error, "error"), grepl(message, conditionMessage(error), fixed = TRUE))
}

# Test fixtures: asymmetric actual changes +4,-2,+6,-3,+5; first forecast misses
# only -2, while the second misses every change. Frozen historical results below
# distinguish hit fraction, signed magnitudes, destination denominators and PT.
actual <- c(10, 14, 12, 18, 15, 20)
partial <- c(8, 11, 13, 17, 14, 16)
opposite <- c(20, 16, 18, 12, 15, 10)
expected_partial <- c(
  mda = 0.8, mdv = 3.2, mdpv = 18.047619047619047,
  legacy_pt_statistic = 2.7386127875258306,
  legacy_pt_p_value = 0.0061698993205441255
)
expected_opposite <- c(
  mda = 0, mdv = -4, mdpv = -24.714285714285715,
  legacy_pt_statistic = -4.47213595499958,
  legacy_pt_p_value = 0.0000077442164310159711
)

check("five kernels preserve frozen historical finite-input arithmetic", function() {
  for (fixture in list(list(partial, expected_partial), list(opposite, expected_opposite))) {
    forecast <- fixture[[1L]]
    observed <- c(
      mda = legacy_mean_directional_accuracy(actual, forecast),
      mdv = legacy_mean_directional_value(actual, forecast),
      mdpv = legacy_mean_directional_percentage_value(actual, forecast),
      legacy_pt_statistic = legacy_pt_statistic(actual, forecast),
      legacy_pt_p_value = legacy_pt_p_value(actual, forecast)
    )
    expect_numeric(observed, fixture[[2L]])
  }
})

check("one or many arbitrary candidates retain isolated named results", function() {
  single <- calculate_directional_forecast_metrics(actual, list(candidate_a = partial))
  multiple <- calculate_directional_forecast_metrics(
    actual, list(candidate_b = opposite, candidate_a = partial)
  )
  stopifnot(identical(names(multiple$models), c("candidate_b", "candidate_a")),
            identical(single$models$candidate_a, multiple$models$candidate_a))
  expect_numeric(unlist(single$models$candidate_a$values), expected_partial)
  expect_numeric(unlist(multiple$models$candidate_b$values), expected_opposite)
  stopifnot(identical(single$models$candidate_a$unavailable, list()))
})

check("stable envelope and reward/penalty remapping affect only MDA", function() {
  result <- calculate_directional_forecast_metrics(actual, list(custom = partial), 3, -2)
  stopifnot(identical(names(result), c(
    "metric_set_id", "comparison_definition", "actual_length", "comparison_count", "models"
  )), identical(result$metric_set_id, "legacy_directional_forecast_v1"),
  identical(result$comparison_definition, "within_future_path_changes_v1"),
  identical(result$actual_length, 6L), identical(result$comparison_count, 5L),
  identical(names(result$models$custom), c("values", "unavailable")),
  identical(names(result$models$custom$values), names(expected_partial)))
  expect_numeric(result$models$custom$values$mda, 2)
  expect_numeric(unlist(result$models$custom$values)[-1L], expected_partial[-1L])
})

check("ties match for MDA, absolute negative destinations and PT filtering persist", function() {
  result <- calculate_directional_forecast_metrics(
    c(-4, -2, -2, -6), list(ties = c(3, 5, 5, 7))
  )$models$ties
  expect_numeric(result$values$mda, 2 / 3)
  expect_numeric(result$values$mdv, -2 / 3)
  expect_numeric(result$values$mdpv, 100 / 9)
  # An actual-only tie with a nonzero forecast direction must also be removed.
  expect_numeric(legacy_pt_statistic(
    c(10, 14, 14, 12, 18, 15, 20), c(8, 11, 12, 14, 18, 15, 17)
  ), 2.7386127875258306)
})

check("minimum length uses only its one within-path change", function() {
  result <- calculate_directional_forecast_metrics(c(100, 101), list(one = c(-5, -4)))
  stopifnot(identical(result$comparison_count, 1L))
  expect_numeric(result$models$one$values$mda, 1)
  expect_numeric(result$models$one$values$mdv, 1)
})

check("invalid actuals and forecast paths fail at the public boundary", function() {
  for (bad in list(numeric(), 1, c(1, NA), c(1, NaN), c(1, Inf),
                   c(1, -Inf), c("1", "2"), c(TRUE, FALSE), matrix(1:4, 2))) {
    expect_error(calculate_directional_forecast_metrics(bad, list(valid = partial)), "actual must")
    expect_error(calculate_directional_forecast_metrics(actual, list(bad = bad)), "forecast 'bad' must")
  }
  expect_error(calculate_directional_forecast_metrics(actual, list(short = c(1, 2))), "same length")
  expect_error(calculate_directional_forecast_metrics(actual, list(long = 1:7)), "same length")
})

check("empty, unnamed, duplicate and invalid candidate names fail", function() {
  for (bad in list(list(), list(partial), partial,
                   stats::setNames(list(partial), ""),
                   stats::setNames(list(partial), NA_character_),
                   stats::setNames(list(partial), " "),
                   stats::setNames(list(partial, opposite), c("same", "same")))) {
    expect_error(calculate_directional_forecast_metrics(actual, bad), "non-empty unique names")
  }
})

check("reward and penalty must be finite numeric scalars", function() {
  for (bad in list(NULL, numeric(), c(1, 2), NA_real_, NaN, Inf, "1", TRUE)) {
    expect_error(calculate_directional_forecast_metrics(actual, list(a = partial), reward = bad),
                 "reward must")
    expect_error(calculate_directional_forecast_metrics(actual, list(a = partial), penalty = bad),
                 "penalty must")
  }
})

check("zero destination makes MDPV explicitly unavailable, not infinite or zero", function() {
  result <- calculate_directional_forecast_metrics(c(2, 0, 3), list(a = c(4, 1, 2)))$models$a
  stopifnot(identical(result$values$mdpv, NA_real_),
            identical(result$unavailable, list(mdpv = "zero_destination_actual")))
  expect_numeric(result$values$mda, 1)
  expect_numeric(result$values$mdv, 2.5)
  # A zero starting value is not a destination and must not invalidate MDPV.
  expect_numeric(legacy_mean_directional_percentage_value(c(0, 2, 4), c(1, 3, 5)), 75)
})

check("empty and degenerate PT have stable reasons for both values", function() {
  for (fixture in list(
    list(c(2, 2, 2), c(3, 4, 2), "no_nonzero_direction_comparisons"),
    list(c(2, 3, 1), c(4, 4, 4), "no_nonzero_direction_comparisons"),
    list(c(2, 3, 4), c(3, 2, 4), "non_positive_or_non_finite_pt_denominator"),
    list(c(2, 3, 1), c(3, 4, 5), "non_positive_or_non_finite_pt_denominator")
  )) {
    result <- calculate_directional_forecast_metrics(fixture[[1L]], list(a = fixture[[2L]]))$models$a
    stopifnot(identical(result$values$legacy_pt_statistic, NA_real_),
              identical(result$values$legacy_pt_p_value, NA_real_),
              identical(result$unavailable, list(
                legacy_pt_statistic = fixture[[3L]], legacy_pt_p_value = fixture[[3L]]
              )))
  }
})

check("finite-input arithmetic overflow never leaks non-standard numeric results", function() {
  result <- calculate_directional_forecast_metrics(c(-1e308, 1e308), list(a = c(1, 2)))$models$a
  stopifnot(identical(result$values$mdv, NA_real_),
            identical(result$unavailable$mdv, "non_finite_result"),
            identical(result$values$mdpv, NA_real_),
            identical(result$unavailable$mdpv, "non_finite_result"))
})

check("no dependencies, model fitting, workflow calls or active source references", function() {
  text <- readLines("src/r/util/directional_forecast_metrics.R")
  code <- text[!grepl("^\\s*#", text)]
  stopifnot(!any(grepl(
    "forecast::|library\\(|require\\(|requireNamespace\\(|source\\(|auto\\.arima|ets\\(|duckdb|Prefect|Dask|stats::ts\\(",
    code
  )))
  files <- list.files("src", pattern = "\\.(R|py)$", recursive = TRUE, full.names = TRUE)
  files <- files[!grepl("/tests/", files) & files != "src/r/util/directional_forecast_metrics.R"]
  for (file in files) {
    stopifnot(!any(grepl("directional_forecast_metrics|legacy_mean_directional_|legacy_pt_",
                        readLines(file, warn = FALSE))))
  }
  features <- new.env(parent = baseenv())
  sys.source("src/r/util/features.R", features)
  stopifnot(length(features$FEATURE_SCHEMA) == 42L,
            identical(features$DISABLED_DIRECTIONAL_FEATURES, c(
              "da_mda_arima", "da_mdv_arima", "da_mdpv_arima", "da_pt_pvalue_arima",
              "da_mda_ets", "da_mdv_ets", "da_mdpv_ets", "da_pt_pvalue_ets"
            )), !any(features$DISABLED_DIRECTIONAL_FEATURES %in% features$FEATURE_SCHEMA))
})

cat("All 12 directional forecast metric test groups passed.\n")
