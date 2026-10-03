# ==============================================================================
# labels.R
#
# Purpose: Compute versioned direction labels independently of transformations.
# Inputs: Numeric future vectors/matrices and explicit finite references.
# Outputs: Shape-preserving integer labels with unavailable targets retained as NA.
# Run from: Imported with source(); not run directly.
# ==============================================================================

# Code constants: persisted label identity and exact scientific definition.
DIRECTIONAL_LABEL_DEFINITION_ID <- "directional_strict_v1"
DIRECTIONAL_LABEL_VERSION <- 1L
DIRECTIONAL_LABEL_RULE <- paste(
  "1 when value > reference; otherwise 0;",
  "missing value unavailable"
)

# Purpose: Compare future values with explicit references without recycling.
# Inputs: Nonempty numeric vector plus one reference, or numeric matrix plus one
#   finite reference per row. Missing future values are allowed; infinity is not.
# Outputs: Integer vector/matrix of identical dimensions; missing targets are NA.
directional_labels <- function(values, references) {
  if (!is.atomic(values) || !(is.null(dim(values)) || is.matrix(values))) {
    stop("directional label values must be a nonempty vector or matrix", call. = FALSE)
  }
  converted <- suppressWarnings(as.numeric(values))
  if (length(converted) < 1L || any(is.infinite(converted))) {
    stop("directional label values must be nonempty and cannot contain infinity", call. = FALSE)
  }
  if (any(!is.na(values) & is.na(converted))) {
    stop("directional label values and references must be numeric", call. = FALSE)
  }
  reference <- suppressWarnings(as.numeric(references))
  if (anyNA(reference) || any(!is.finite(reference))) {
    stop("directional label references must be finite", call. = FALSE)
  }
  if (is.matrix(values)) {
    if (is.matrix(references) || length(reference) != nrow(values)) {
      stop("a future matrix requires exactly one reference per row", call. = FALSE)
    }
    numeric_values <- matrix(
      converted,
      nrow = nrow(values),
      ncol = ncol(values),
      dimnames = dimnames(values)
    )
    labels <- numeric_values > reference[row(numeric_values)]
  } else {
    if (length(reference) != 1L) {
      stop("a future vector requires exactly one reference", call. = FALSE)
    }
    labels <- converted > reference[[1L]]
    names(labels) <- names(values)
  }
  storage.mode(labels) <- "integer"
  labels
}

# Purpose: Preserve the previous history/future interface as a delegating adapter.
# Inputs: Nonempty finite history x and numeric future vector xx.
# Outputs: directional_labels(xx, final history value).
compute_label_vector <- function(x, xx) {
  history <- suppressWarnings(as.numeric(x))
  if (length(history) < 1L || anyNA(history) || any(!is.finite(history))) {
    stop("label history must be nonempty and finite", call. = FALSE)
  }
  directional_labels(xx, history[[length(history)]])
}
