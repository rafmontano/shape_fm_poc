#!/usr/bin/env Rscript
# ==============================================================================
# test_features.R
#
# Purpose: Verify the isolated ID 014 FFORMA feature calculation and worker contract.
# Inputs: Current R sources and the unchanged legacy feature source, read-only.
# Outputs: Named test progress on stdout and nonzero exit on assertion failure.
# Run from: Rscript src/r/tests/test_features.R
# ==============================================================================

LEGACY_FEATURE_SOURCE <- Sys.getenv(
  "SHAPEFM_LEGACY_FEATURE_SOURCE",
  unset = "/Users/monta/Documents/Projects/m4_tsc_fmts_2026/src/r/features.R"
)

# The small assertion-oriented test script uses the code standard's practical
# functional exception: test classes would add structure without reusable state.
legacy_environment <- new.env(parent = globalenv())
legacy_calc_features <- NULL
if (file.exists(LEGACY_FEATURE_SOURCE)) {
  sys.source(LEGACY_FEATURE_SOURCE, envir = legacy_environment)
  legacy_calc_features <- legacy_environment$calc_features
}
source("src/r/feature_provider.R")

EXPECTED_SCHEMA <- c(
  "x_acf1", "x_acf10", "diff1_acf1", "diff1_acf10", "diff2_acf1",
  "diff2_acf10", "seas_acf1", "ARCH.LM", "crossing_points", "entropy",
  "flat_spots", "arch_acf", "garch_acf", "arch_r2", "garch_r2", "alpha",
  "beta", "hurst", "lumpiness", "nonlinearity", "x_pacf5", "diff1x_pacf5",
  "diff2x_pacf5", "seas_pacf", "nperiods", "seasonal_period", "trend",
  "spike", "linearity", "curvature", "e_acf1", "e_acf10",
  "seasonal_strength", "peak", "trough", "stability", "hw_alpha",
  "hw_beta", "hw_gamma", "unitroot_kpss", "unitroot_pp", "series_length"
)

check <- function(name, assertion) {
  assertion()
  cat(sprintf("ok - %s\n", name))
}

assert_true <- function(condition, message) {
  if (length(condition) != 1L || is.na(condition) || !condition) stop(message, call. = FALSE)
}

assert_equal_numeric <- function(observed, expected, message, tolerance = 1e-10) {
  if (!isTRUE(all.equal(as.numeric(observed), as.numeric(expected), tolerance = tolerance))) {
    stop(message, call. = FALSE)
  }
}

nonseasonal <- stats::ts(20 + seq_len(80) / 7 + sin(seq_len(80) / 3), frequency = 1)
seasonal <- stats::ts(
  50 + seq_len(120) / 20 + rep(c(-3, -1, 2, 4, 1, -2, 0, 3, -1, 2, 0, -2), 10),
  frequency = 12
)

check("exact 42 feature names and order", function() {
  observed <- calculate_fforma_features(nonseasonal)
  assert_true(identical(names(observed), EXPECTED_SCHEMA), "feature schema differs")
  assert_true(length(observed) == 42L, "feature count differs")
})

check("period-1 parity with unchanged legacy calculation", function() {
  if (is.null(legacy_calc_features)) {
    cat("skip - set SHAPEFM_LEGACY_FEATURE_SOURCE for legacy parity\n")
    return(invisible(NULL))
  }
  expected <- unlist(legacy_calc_features(list(x = nonseasonal))$features[1L, ])
  observed <- calculate_fforma_features(nonseasonal)
  assert_true(identical(names(expected), names(observed)), "period-1 legacy names differ")
  assert_equal_numeric(observed, expected, "period-1 values differ from legacy")
})

check("seasonal parity with unchanged legacy calculation", function() {
  if (is.null(legacy_calc_features)) {
    cat("skip - set SHAPEFM_LEGACY_FEATURE_SOURCE for legacy parity\n")
    return(invisible(NULL))
  }
  expected <- unlist(legacy_calc_features(list(x = seasonal))$features[1L, ])
  observed <- calculate_fforma_features(seasonal)
  assert_true(identical(names(expected), names(observed)), "seasonal legacy names differ")
  assert_equal_numeric(observed, expected, "seasonal values differ from legacy")
})

check("constant and short inputs produce finite vectors", function() {
  constant <- calculate_fforma_features(stats::ts(rep(7, 30), frequency = 1))
  short <- calculate_fforma_features(stats::ts(c(1, 2, 3), frequency = 1))
  assert_true(all(is.finite(constant)) && all(is.finite(short)), "edge output is non-finite")
  assert_true(constant[["entropy"]] == 0 && short[["entropy"]] == 0, "entropy fallback differs")
})

check("individual and batch extraction agree beside a constant", function() {
  varying_job <- list(id = "varying", context = as.list(as.numeric(nonseasonal)), seasonality = 1L)
  constant_job <- list(id = "constant", context = as.list(rep(4, 30)), seasonality = 1L)
  individual <- extract_feature_job(varying_job)
  batch <- dispatch_feature_request(list(action = "extract", jobs = list(constant_job, varying_job)))
  assert_true(all(vapply(batch$results, function(x) x$status == "success", logical(1L))),
              "valid batch job failed")
  assert_equal_numeric(batch$results[[2L]]$feature_values, individual$feature_values,
                       "constant neighbor changed varying-series scaling")
})

check("resolved periods are reflected in seasonal_period", function() {
  period_one <- extract_feature_job(list(id = "p1", context = as.list(as.numeric(seasonal)), seasonality = 1L))
  period_twelve <- extract_feature_job(list(id = "p12", context = as.list(as.numeric(seasonal)), seasonality = 12L))
  position <- match("seasonal_period", EXPECTED_SCHEMA)
  assert_true(period_one$feature_values[[position]] == 1, "period 1 was not retained")
  assert_true(period_twelve$feature_values[[position]] == 12, "period 12 was not retained")
})

check("one invalid job fails without stopping its batch", function() {
  jobs <- list(
    list(id = "bad", context = list(1, NA, 3), seasonality = 1L),
    list(id = "good", context = as.list(as.numeric(nonseasonal)), seasonality = 1L)
  )
  results <- dispatch_feature_request(list(action = "extract", jobs = jobs))$results
  assert_true(identical(vapply(results, `[[`, character(1L), "status"), c("failed", "success")),
              "per-job failure did not preserve batch progress")
  assert_true(is.null(results[[1L]]$feature_values), "failure was converted to feature values")
})

check("describe reports identity, policies, dependencies, and disabled directions", function() {
  description <- describe_feature_provider()
  assert_true(identical(description$feature_names, EXPECTED_SCHEMA), "describe schema differs")
  assert_true(identical(description$directional_features_enabled, FALSE), "directional flag differs")
  assert_true(identical(description$disabled_directional_feature_names,
                        DISABLED_DIRECTIONAL_FEATURES), "disabled names differ")
  assert_true(grepl("window futures", description$directional_features_disabled_reason),
              "disabled rationale is absent")
  assert_true(all(c("R", "platform", "tsfeatures", "forecast", "jsonlite", "tibble") %in%
                    names(description$dependencies)), "dependency identity is incomplete")
})

check("successful extraction excludes disabled directional fields", function() {
  result <- extract_feature_job(list(
    id = "plain", context = as.list(as.numeric(nonseasonal)), seasonality = 1L
  ))
  assert_true(result$status == "success", "valid extraction failed")
  assert_true(!any(DISABLED_DIRECTIONAL_FEATURES %in% result$feature_names),
              "disabled directional field was emitted")
  assert_true(length(result$feature_values) == 42L && all(is.finite(result$feature_values)),
              "success output is incomplete or non-finite")
})

cat("All feature tests passed.\n")
