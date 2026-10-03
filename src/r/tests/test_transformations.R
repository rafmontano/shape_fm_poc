#!/usr/bin/env Rscript
# ==============================================================================
# test_transformations.R
#
# Purpose: Verify R sample standardisation, selected labels, and context windows.
# Inputs: Shared R utilities and deterministic hand-calculated fixtures.
# Outputs: Concise test progress; exits nonzero on any contract violation.
# Run from: Rscript --vanilla src/r/tests/test_transformations.R
# ==============================================================================

source("src/r/util/transformations.R")
source("src/r/util/labels.R")
source("src/r/util/window_preparation.R")

# Purpose: Require a true scalar condition.
# Inputs: Logical condition and diagnostic text.
# Outputs: TRUE invisibly or a terminating assertion error.
assert_true <- function(condition, message) {
  if (length(condition) != 1L || is.na(condition) || !condition) {
    stop(message, call. = FALSE)
  }
  invisible(TRUE)
}

# Purpose: Require an expression to fail with matching text.
# Inputs: Zero-argument function and expected regular expression.
# Outputs: Captured error invisibly or a terminating assertion error.
assert_error <- function(operation, pattern) {
  error <- tryCatch(operation(), error = identity)
  assert_true(
    inherits(error, "error") && grepl(pattern, conditionMessage(error)),
    paste("Expected error matching", pattern)
  )
  invisible(error)
}

# Purpose: Run and report one named test.
# Inputs: Name and zero-argument assertion function.
# Outputs: One progress line; assertion errors stop the script.
check <- function(name, assertion) {
  assertion()
  cat("ok -", name, "\n")
}

check("hand fixture uses sample standard deviation", function() {
  state <- fit_transformation(c(10, 20, 30), STANDARDISATION_RECIPE)
  assert_true(identical(state$centre, 20), "centre changed")
  assert_true(identical(state$scale, 10), "sample scale changed")
  assert_true(identical(apply_transformation(c(10, 20, 30), state), c(-1, 0, 1)), "hand values changed")
})

check("constant and singleton histories retain affine additional values", function() {
  state <- fit_transformation(c(5, 5, 5), STANDARDISATION_RECIPE)
  assert_true(isTRUE(state$constant) && state$scale == 1, "constant state changed")
  assert_true(identical(apply_transformation(7, state), 2), "constant apply collapsed")
  assert_true(identical(inverse_transformation(c(2, 1.5), state), c(7, 6.5)), "constant inverse collapsed")
  singleton <- fit_transformation(4, STANDARDISATION_RECIPE)
  assert_true(isTRUE(singleton$constant) && singleton$count == 1L, "singleton policy changed")
})

check("negative and nearly constant histories remain valid", function() {
  negative <- fit_transformation(c(-4, -2, -3), STANDARDISATION_RECIPE)
  assert_true(identical(apply_transformation(c(-4, -2, -3), negative), c(-1, 1, 0)), "negative fixture changed")
  near <- fit_transformation(c(1, 1 + 1e-12, 1 - 1e-12), STANDARDISATION_RECIPE)
  assert_true(!near$constant && near$scale > 0, "near constant was clipped")
})

check("pair operation reuses fitted state", function() {
  pair <- scale_pair_std(c(10, 20, 30), c(0, 40))
  assert_true(identical(pair$x_std, c(-1, 0, 1)), "history mismatch")
  assert_true(identical(pair$xx_std, c(-2, 2)), "future was refitted")
  assert_true(identical(standardise_vec(c(10, 20, 30)), pair$x_std), "standardise_vec diverged")
})

check("new nonconstant result matches previous R direct standardisation", function() {
  source <- c(-7.5, 2, 11.25, 4.5)
  old_expected <- (source - mean(source)) / stats::sd(source)
  assert_true(
    isTRUE(all.equal(standardise_vec(source), old_expected, tolerance = 1e-14)),
    "new R formula differs from previous nonconstant R behavior"
  )
})

check("ordered interface applies forward and inverts reverse", function() {
  fitted <- fit_apply_steps(c(10, 20, 30), c("identity", STANDARDISATION_RECIPE))
  assert_true(identical(fitted$values, c(-1, 0, 1)), "ordered forward changed")
  assert_true(identical(apply_steps(40, fitted$states), 2), "ordered held-out apply changed")
  assert_true(identical(inverse_steps(2, fitted$states), 40), "ordered inverse changed")
  assert_error(function() fit_apply_steps(c(1, 2), "minmax_then_standardize"), "unsupported")
})

check("invalid inputs and malformed state fail", function() {
  assert_error(function() fit_transformation(numeric(), STANDARDISATION_RECIPE), "nonempty")
  assert_error(function() fit_transformation(c(1, Inf), STANDARDISATION_RECIPE), "finite")
  state <- fit_transformation(c(1, 2), STANDARDISATION_RECIPE)
  state$version <- 2L
  assert_error(function() apply_transformation(3, state), "version")
  state <- fit_transformation(c(1, 2), STANDARDISATION_RECIPE)
  state$extra <- TRUE
  assert_error(function() apply_transformation(3, state), "exactly")
})

check("strict labels preserve upward changes and ties", function() {
  assert_true(identical(compute_label_vector(c(5, 5, 5), c(7, 5, 4)), c(1L, 0L, 0L)), "label rule changed")
  negative <- compute_label_vector(c(-2, -2), c(-1, -2, -3))
  assert_true(identical(negative, c(1L, 0L, 0L)), "negative label rule changed")
  matrix_labels <- directional_labels(
    matrix(c(2, 1, NA, -1, -2, -3), nrow = 2L, byrow = TRUE),
    c(1, -2)
  )
  assert_true(
    identical(matrix_labels, matrix(c(1L, 0L, NA_integer_, 1L, 0L, 0L), nrow = 2L, byrow = TRUE)),
    "row-specific or missing matrix labels changed"
  )
  assert_error(function() directional_labels(matrix(1:4, nrow = 2L), 1), "one reference per row")
  assert_error(function() directional_labels(c(1, Inf), 1), "infinity")
  assert_error(function() directional_labels(c(1, 2), NA_real_), "references must be finite")
})

check("explicit context length preserves legacy M4 Daily value 64", function() {
  short <- prepare_context(c(1, 2, 3), 5L)
  assert_true(identical(short, c(1, 1, 1, 2, 3)), "short context padding changed")
  full <- prepare_context(seq_len(100), 64L)
  assert_true(identical(full, as.numeric(37:100)), "configured trailing context changed")
  assert_error(function() prepare_context(1:3, 0L), "positive integer")
  assert_error(function() prepare_context(1:3, 1.5), "positive integer")
})

cat("All R standardisation tests passed.\n")
