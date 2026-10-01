# ==============================================================================
# window_preparation.R
#
# Purpose: Prepare explicitly configured trailing contexts without horizon rules.
# Inputs: Finite observations and a positive configured context length.
# Outputs: Fixed-length numeric context preserving the legacy padding rule.
# Run from: Imported with source(); not run directly.
# ==============================================================================

# Purpose: Select a configured trailing context and left-pad short histories.
# Inputs: Nonempty finite values and a positive integer context_length.
# Outputs: Numeric vector of exactly context_length; no frequency/horizon inference.
prepare_context <- function(values, context_length) {
  values <- suppressWarnings(as.numeric(values))
  supplied_context_length <- suppressWarnings(as.numeric(context_length))
  context_length <- suppressWarnings(as.integer(supplied_context_length))
  if (length(supplied_context_length) != 1L || is.na(supplied_context_length) ||
      !is.finite(supplied_context_length) || supplied_context_length < 1 ||
      is.na(context_length) ||
      supplied_context_length != context_length) {
    stop("context_length must be a positive integer", call. = FALSE)
  }
  if (length(values) < 1L || anyNA(values) || any(!is.finite(values))) {
    stop("context values must be nonempty and finite", call. = FALSE)
  }
  if (length(values) >= context_length) {
    return(as.numeric(utils::tail(values, context_length)))
  }
  c(rep(values[[1L]], context_length - length(values)), values)
}
