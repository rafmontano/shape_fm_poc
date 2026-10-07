#!/usr/bin/env Rscript
# ==============================================================================
# test_forecast_adjustments.R
#
# Purpose: Verify historical SMYL Oracle parity, invalid inputs and dormant isolation.
# Inputs: Frozen historical fixtures and repository source; no legacy runtime.
# Outputs: Named assertions on stdout; failures exit nonzero; writes nothing.
# Run from: Rscript --vanilla src/r/tests/test_forecast_adjustments.R
# ==============================================================================

# Bounded functional exception: deterministic assertions need no stateful classes.
packages_before <- loadedNamespaces()
source("src/r/util/forecast_adjustments.R")
stopifnot(identical(loadedNamespaces(), packages_before))

# Run a named assertion group and report success only after all assertions pass.
check <- function(name, assertion) {
  assertion()
  cat(sprintf("ok - %s\n", name))
}

# Compare all numeric values against independently frozen historical results.
expect_numeric <- function(observed, expected) {
  stopifnot(isTRUE(all.equal(observed, expected, tolerance = 1e-12)))
}

# Require an invalid request to fail with the expected contract message.
expect_error <- function(expression, message) {
  error <- tryCatch({ force(expression); NULL }, error = identity)
  stopifnot(inherits(error, "error"),
            grepl(message, conditionMessage(error), fixed = TRUE))
}

# Purpose: Check the complete public result against a frozen scientific fixture.
# Inputs: Actual and SMYL vectors plus independent expected scale, sMAPE and ties.
# Outputs: Assertions on every field and the complete adjusted vector; no writes.
expect_result <- function(actual, forecast, multiplier, objective, ties, adjusted) {
  result <- calculate_smyl_oracle(actual, forecast)
  stopifnot(is.list(result), identical(names(result), c(
    "method_id", "multiplier", "objective_smape", "near_tie_count", "adjusted_mean"
  )), identical(result$method_id, "smyl_scalar_smape_oracle_v1"),
  identical(result$near_tie_count, ties), length(result$adjusted_mean) == length(actual))
  expect_numeric(result$multiplier, multiplier)
  expect_numeric(result$objective_smape, objective)
  expect_numeric(result$adjusted_mean, adjusted)
}

# Frozen test values obtained by parsing the unchanged historical
# m4_tsc_fmts_2026/src/r/sensitivity/04a_add_smyl_oracle.R and evaluating only
# oracle_grid, calc_smape_oracle and add_smyl_oracle_one in an isolated environment.
# Neither the historical common script nor its top-level runner was sourced.
check("complete asymmetric historical parity", function() {
  expect_result(c(9, 21, 8, 37), c(8, 17, 11, 29),
                1.235, 16.0873924561231, 1L, c(9.88, 20.995, 13.585, 35.815))
  expect_result(c(-8, 13, 0, 27), c(-11, 10, 0, 22),
                1.227, 18.9806154692789, 1L, c(-13.497, 12.27, 0, 26.994))
})

check("fixed 1001-value grid and both endpoints", function() {
  stopifnot(identical(.SMYL_ORACLE_GRID, seq(0.500, 1.500, by = 0.001)),
            length(.SMYL_ORACLE_GRID) == 1001L)
  expect_result(c(2, 5, 9), c(4, 10, 18), 0.5, 0, 1L, c(2, 5, 9))
  expect_result(c(6, 15, 27), c(4, 10, 18), 1.5, 0, 1L, c(6, 15, 27))
})

check("interior optimum scales every horizon", function() {
  expect_result(c(2.5, 6.25, 11.25), c(2, 5, 9),
                1.25, 0, 1L, c(2.5, 6.25, 11.25))
})

check("exact ties select the first grid value", function() {
  expect_result(c(1, -3, 7), c(0, 0, 0),
                0.5, 200, 1001L, c(0, 0, 0))
})

check("near ties include unequal objectives within historical tolerance", function() {
  # Frozen historical result: first minimum at 0.5, but 297 near minima.
  # This distinguishes tolerance counting from exact equality or grid size.
  expect_result(c(1, 1e-10), c(-1, 1),
                0.5, 199.99999996, 297L, c(-0.5, 0.5))
})

check("zero-over-zero terms are omitted, not treated as zero error", function() {
  expect_result(c(0, 9, 21, 8, 37), c(0, 8, 17, 11, 29),
                1.235, 16.0873924561231, 1L, c(0, 9.88, 20.995, 13.585, 35.815))
  expect_result(c(0, 3), c(0, 0), 0.5, 200, 1001L, c(0, 0))
})

check("all-unavailable and candidate overflow fail explicitly", function() {
  expect_error(calculate_smyl_oracle(c(0, 0), c(0, 0)), "finite objective")
  # Early candidates are finite; later numerator multiplication overflows. Do not ignore
  # the unavailable candidates and select from a scientifically different grid.
  expect_error(calculate_smyl_oracle(0, 1e306), "finite objective")
})

check("invalid types, dimensions, lengths and non-finite inputs fail", function() {
  for (bad in list(NULL, numeric(), "1", TRUE, list(1), 1 + 1i,
                   matrix(1:4, 2), array(1, c(1, 1, 1)),
                   NA_real_, NaN, Inf, -Inf, c(1, NA_real_))) {
    expect_error(calculate_smyl_oracle(bad, c(1, 2)), "actual must")
    expect_error(calculate_smyl_oracle(c(1, 2), bad), "smyl_forecast must")
  }
  expect_error(calculate_smyl_oracle(c(1, 2), 1), "same length")
  expect_error(calculate_smyl_oracle(1, c(1, 2)), "same length")
  expect_result(1L, 2L, 0.5, 0, 1L, 1)
})

check("dependency-free source and only approved paper-profile activation", function() {
  code <- readLines("src/r/util/forecast_adjustments.R")
  code <- code[!grepl("^\\s*#", code)]
  stopifnot(!any(grepl(
    "library\\(|require\\(|requireNamespace\\(|source\\(|::|read[A-Z]|write[A-Z]|save\\(|load\\(|file\\(|dir\\.create|system\\(|<<-|auto\\.arima|ets\\(|duckdb|Prefect|Dask",
    code
  )))
  files <- c(
    list.files("src", pattern = "\\.(R|py)$", recursive = TRUE, full.names = TRUE),
    list.files("config", recursive = TRUE, full.names = TRUE),
    list.files("workflows", pattern = "\\.(R|py|sh|json|ya?ml)$",
               recursive = TRUE, full.names = TRUE),
    list.files("scripts", pattern = "\\.(R|py|sh)$", recursive = TRUE, full.names = TRUE)
  )
  files <- files[!grepl("/tests/", files) &
                   !files %in% c("src/r/util/forecast_adjustments.R",
                                 "src/r/util/paper_table_metrics.R",
                                 "src/r/06_02_evaluate_paper_tables.R",
                                 "src/python/util/p06_01_table_flow.py") & !dir.exists(files)]
  for (file in files) {
    stopifnot(!any(grepl(
      "forecast_adjustments\\.R|calculate_smyl_oracle|smyl_scalar_smape_oracle_v1",
      readLines(file, warn = FALSE)
    )))
  }
})

cat("All 9 forecast adjustment test groups passed.\n")
