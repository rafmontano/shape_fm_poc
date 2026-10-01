# ==============================================================================
# transformations.R
#
# Purpose: Fit, apply, compose, and invert the approved portable transformation.
# Inputs: Finite numeric values, registered recipe names, and fitted state lists.
# Outputs: Transformed/restored numeric vectors and JSON-serialisable fitted state.
# Run from: Imported with source(); not run directly.
# ==============================================================================

# Code constants shared with the Python implementation and persisted state.
REGISTERED_TRANSFORMATION_STEPS <- c("identity", "standardise_sample_v1")
STANDARDISATION_RECIPE <- "standardise_sample_v1"
STANDARDISATION_VERSION <- 1L
STANDARDISATION_STATE_KEYS <- c(
  "recipe", "version", "centre", "scale", "count", "constant"
)

# Purpose: Validate one nonempty finite numeric vector for a named operation.
# Inputs: Coercible numeric values and human-readable operation name.
# Outputs: Double vector; malformed, empty, or nonfinite input raises an error.
.finite_transformation_values <- function(values, purpose) {
  source <- suppressWarnings(as.numeric(values))
  if (length(source) < 1L || anyNA(source) || any(!is.finite(source))) {
    stop(purpose, " values must be nonempty and finite", call. = FALSE)
  }
  source
}

# Purpose: Validate the exact six-field portable state contract.
# Inputs: State produced in R or decoded from Python JSON.
# Outputs: Normalised state list; aliases, malformed fields, and versions fail.
validate_transformation_state <- function(state) {
  if (!is.list(state) || !identical(sort(names(state)), sort(STANDARDISATION_STATE_KEYS))) {
    stop(
      "standardisation state must contain exactly recipe, version, centre, ",
      "scale, count, and constant",
      call. = FALSE
    )
  }
  if (!identical(as.character(state$recipe), STANDARDISATION_RECIPE)) {
    stop("unsupported transformation state recipe", call. = FALSE)
  }
  version <- suppressWarnings(as.integer(state$version))
  if (length(version) != 1L || is.na(version) ||
      as.numeric(state$version) != version || version != STANDARDISATION_VERSION) {
    stop("unsupported standardise_sample_v1 state version", call. = FALSE)
  }
  count <- suppressWarnings(as.integer(state$count))
  if (length(count) != 1L || is.na(count) ||
      as.numeric(state$count) != count || count < 1L) {
    stop("standardisation state count must be a positive integer", call. = FALSE)
  }
  if (!is.logical(state$constant) || length(state$constant) != 1L || is.na(state$constant)) {
    stop("standardisation state constant must be boolean", call. = FALSE)
  }
  centre <- suppressWarnings(as.numeric(state$centre))
  scale <- suppressWarnings(as.numeric(state$scale))
  if (length(centre) != 1L || length(scale) != 1L ||
      !is.finite(centre) || !is.finite(scale) || scale <= 0) {
    stop("standardisation state centre must be finite and scale positive", call. = FALSE)
  }
  if (isTRUE(state$constant) && scale != 1) {
    stop("constant standardisation state must use effective scale 1", call. = FALSE)
  }
  if (!isTRUE(state$constant) && count < 2L) {
    stop("nonconstant standardisation state requires at least two observations", call. = FALSE)
  }
  list(
    recipe = STANDARDISATION_RECIPE,
    version = STANDARDISATION_VERSION,
    centre = centre,
    scale = scale,
    count = count,
    constant = state$constant
  )
}

# Purpose: Fit one registered step from historical observations only.
# Inputs: Finite history and identity or standardise_sample_v1 recipe.
# Outputs: Empty identity state or exact six-field sample-standardisation state.
fit_transformation <- function(values, recipe) {
  source <- .finite_transformation_values(values, "transformation fit")
  recipe <- as.character(recipe)
  if (length(recipe) != 1L || is.na(recipe)) {
    stop("transformation recipe must be one value", call. = FALSE)
  }
  if (identical(recipe, "identity")) return(list())
  if (!identical(recipe, STANDARDISATION_RECIPE)) {
    stop("unsupported transformation recipe: ", recipe, call. = FALSE)
  }
  constant <- all(source == source[[1L]])
  centre <- if (constant) source[[1L]] else mean(source)
  scale <- if (constant) 1 else stats::sd(source)
  if (!is.finite(scale) || scale <= 0) {
    stop("nonconstant standardisation produced an invalid sample scale", call. = FALSE)
  }
  list(
    recipe = STANDARDISATION_RECIPE,
    version = STANDARDISATION_VERSION,
    centre = centre,
    scale = scale,
    count = length(source),
    constant = constant
  )
}

# Purpose: Apply one fitted state without refitting on supplied values.
# Inputs: Finite values and an identity or exact portable fitted state.
# Outputs: Finite transformed double vector.
apply_transformation <- function(values, state) {
  source <- .finite_transformation_values(values, "transformation apply")
  if (length(state) == 0L) return(source)
  fitted <- validate_transformation_state(state)
  output <- (source - fitted$centre) / fitted$scale
  if (any(!is.finite(output))) {
    stop("standardisation produced a non-finite result", call. = FALSE)
  }
  as.numeric(output)
}

# Purpose: Restore values with one fitted positive affine state without refitting.
# Inputs: Finite transformed values and an identity or portable fitted state.
# Outputs: Finite double vector on the original scale.
inverse_transformation <- function(values, state) {
  source <- .finite_transformation_values(values, "transformation inverse")
  if (length(state) == 0L) return(source)
  fitted <- validate_transformation_state(state)
  output <- fitted$centre + fitted$scale * source
  if (any(!is.finite(output))) {
    stop("inverse standardisation produced a non-finite result", call. = FALSE)
  }
  as.numeric(output)
}

# Purpose: Fit and apply registered steps in declared forward order.
# Inputs: Finite historical values and ordered recipe identifiers.
# Outputs: List containing transformed values and one fitted state per step.
fit_apply_steps <- function(values, recipes) {
  current <- .finite_transformation_values(values, "transformation pipeline")
  recipes <- as.character(recipes)
  if (any(!recipes %in% REGISTERED_TRANSFORMATION_STEPS)) {
    stop("ordered transformations contain an unsupported step", call. = FALSE)
  }
  states <- vector("list", length(recipes))
  for (index in seq_along(recipes)) {
    states[[index]] <- fit_transformation(current, recipes[[index]])
    current <- apply_transformation(current, states[[index]])
  }
  list(values = current, states = states)
}

# Purpose: Apply fitted states in forward order without fitting new parameters.
# Inputs: Finite values and ordered fitted states.
# Outputs: Finite transformed vector.
apply_steps <- function(values, states) {
  current <- .finite_transformation_values(values, "transformation pipeline apply")
  for (state in states) current <- apply_transformation(current, state)
  current
}

# Purpose: Invert fitted states in reverse order.
# Inputs: Finite transformed values and ordered fitted states.
# Outputs: Finite vector on the original scale.
inverse_steps <- function(values, states) {
  current <- .finite_transformation_values(values, "transformation pipeline inverse")
  for (state in rev(states)) current <- inverse_transformation(current, state)
  current
}

# Purpose: Standardise one vector through the shared fitted implementation.
# Inputs: Finite history.
# Outputs: Standardised numeric vector; no duplicate scaling formula.
standardise_vec <- function(values) {
  state <- fit_transformation(values, STANDARDISATION_RECIPE)
  apply_transformation(values, state)
}

# Purpose: Fit on history and apply the same state to optional additional values.
# Inputs: Finite historical x and optional finite xx values.
# Outputs: Standardised pair plus portable fitted state.
scale_pair_std <- function(x, xx = NULL) {
  state <- fit_transformation(x, STANDARDISATION_RECIPE)
  list(
    x_std = apply_transformation(x, state),
    xx_std = if (is.null(xx)) NULL else apply_transformation(xx, state),
    state = state
  )
}
