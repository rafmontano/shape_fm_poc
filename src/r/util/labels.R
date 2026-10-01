# ==============================================================================
# labels.R
#
# Purpose: Compute selected direction labels independently of transformations.
# Inputs: Finite historical and future numeric observations.
# Outputs: Integer labels using the strict future-greater-than-last-history rule.
# Run from: Imported with source(); not run directly.
# ==============================================================================

# Purpose: Label each future value as upward only when it exceeds final history.
# Inputs: Nonempty finite history x and finite future vector xx.
# Outputs: Integer vector; ties and downward moves are zero, upward moves are one.
compute_label_vector <- function(x, xx) {
  x <- suppressWarnings(as.numeric(x))
  xx <- suppressWarnings(as.numeric(xx))
  if (length(x) < 1L || length(xx) < 1L ||
      anyNA(c(x, xx)) || any(!is.finite(c(x, xx)))) {
    stop("label inputs must be nonempty and finite", call. = FALSE)
  }
  as.integer(xx > x[[length(x)]])
}
