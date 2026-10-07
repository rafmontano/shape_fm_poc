# ==============================================================================
# forecast_adjustments.R
#
# Purpose: Preserve the dormant ex-post SMYL scaling Oracle for future Process 06
#   diagnostics; this is not a deployable forecast or combination input.
# Inputs: Canonical realised future and stored m4_smyl mean for the same horizon.
# Outputs: Versioned in-memory diagnostic list; no persistent or global writes.
# Run from: Imported; not run directly.
# ==============================================================================

# Code constants: fixed scientific identity and grid, not experiment settings.
.SMYL_ORACLE_METHOD_ID <- "smyl_scalar_smape_oracle_v1"
.SMYL_ORACLE_GRID <- seq(0.500, 1.500, by = 0.001)
stopifnot(length(.SMYL_ORACLE_GRID) == 1001L)

# Bounded functional exception: this small stateless kernel needs no class.

# Require a non-empty finite numeric vector without dimensional attributes.
.validate_oracle_path <- function(path, name) {
  if (!is.numeric(path) || !is.null(dim(path)) || length(path) == 0L ||
      any(!is.finite(path))) {
    stop(sprintf("%s must be a non-empty finite numeric vector", name), call. = FALSE)
  }
}

# Purpose: Select the historical full-horizon, first-minimum ex-post SMYL scale.
# Inputs: Equal-length, non-empty finite numeric actual and stored SMYL vectors.
# Outputs: Named list of method_id, multiplier, objective_smape, near_tie_count,
#   and adjusted_mean. Zero-over-zero terms are omitted; any non-finite grid
#   objective raises an error. Invalid inputs fail clearly; writes nothing.
calculate_smyl_oracle <- function(actual, smyl_forecast) {
  .validate_oracle_path(actual, "actual")
  .validate_oracle_path(smyl_forecast, "smyl_forecast")
  if (length(actual) != length(smyl_forecast)) {
    stop("actual and smyl_forecast must have the same length", call. = FALSE)
  }

  # Historical Oracle-specific objective, including na.rm zero/zero omission.
  oracle_errors <- vapply(.SMYL_ORACLE_GRID, function(multiplier) {
    mean(
      200 * abs(actual - multiplier * smyl_forecast) /
        (abs(actual) + abs(multiplier * smyl_forecast)),
      na.rm = TRUE
    )
  }, numeric(1))
  if (any(!is.finite(oracle_errors))) {
    stop("SMYL Oracle requires a finite objective for every grid multiplier",
         call. = FALSE)
  }

  best_idx <- which.min(oracle_errors)
  best_multiplier <- .SMYL_ORACLE_GRID[best_idx]
  best_error <- oracle_errors[best_idx]
  list(
    method_id = .SMYL_ORACLE_METHOD_ID,
    multiplier = best_multiplier,
    objective_smape = best_error,
    near_tie_count = sum(abs(oracle_errors - best_error) <= sqrt(.Machine$double.eps)),
    adjusted_mean = best_multiplier * smyl_forecast
  )
}
