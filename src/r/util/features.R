# ==============================================================================
# features.R
#
# Purpose: Calculate the fixed FFORMA-compatible 42-feature vector for one series.
# Inputs: A finite stats::ts whose frequency is the centrally resolved seasonality.
# Outputs: One named, finite numeric vector in FEATURE_SCHEMA order; no forecasts,
#   future observations, labels, persistence, or frequency mapping are performed.
# Run from: Imported by src/r/feature_provider.R and focused R tests.
# ==============================================================================

# Scientific constants: the schema and pool reproduce the unchanged legacy
# calculation while naming composition explicitly rather than relying on positions.
FEATURE_SCHEMA <- c(
  "x_acf1", "x_acf10", "diff1_acf1", "diff1_acf10", "diff2_acf1",
  "diff2_acf10", "seas_acf1", "ARCH.LM", "crossing_points", "entropy",
  "flat_spots", "arch_acf", "garch_acf", "arch_r2", "garch_r2", "alpha",
  "beta", "hurst", "lumpiness", "nonlinearity", "x_pacf5", "diff1x_pacf5",
  "diff2x_pacf5", "seas_pacf", "nperiods", "seasonal_period", "trend",
  "spike", "linearity", "curvature", "e_acf1", "e_acf10",
  "seasonal_strength", "peak", "trough", "stability", "hw_alpha",
  "hw_beta", "hw_gamma", "unitroot_kpss", "unitroot_pp", "series_length"
)

NONSEASONAL_OPTIONAL_FEATURES <- c(
  "seas_acf1", "seas_pacf", "seasonal_strength", "peak", "trough"
)

# Future capability record: these fields are intentionally outside FEATURE_SCHEMA.
# Legacy training calculated them using window futures, while real prediction used
# a historical holdout; that mismatch is not a valid deployable feature contract.
DISABLED_DIRECTIONAL_FEATURES <- c(
  "da_mda_arima", "da_mdv_arima", "da_mdpv_arima", "da_pt_pvalue_arima",
  "da_mda_ets", "da_mdv_ets", "da_mdpv_ets", "da_pt_pvalue_ets"
)
DIRECTIONAL_FEATURES_DISABLED_REASON <- paste(
  "Legacy training used window futures while real prediction used a historical",
  "holdout; forecasts and directional features are therefore disabled."
)

# Purpose: Preserve FFORMA's four-zero fallback when heterogeneity cannot be fit.
# Inputs: One stats::ts accepted by tsfeatures.
# Outputs: Four named heterogeneity values, or four zeros after any error.
heterogeneity_feature_fallback <- function(x) {
  fallback <- c(arch_acf = 0, garch_acf = 0, arch_r2 = 0, garch_r2 = 0)
  value <- try(tsfeatures::heterogeneity(x), silent = TRUE)
  if (inherits(value, "try-error")) fallback else value
}

# Purpose: Preserve a finite zero entropy for constant, short, or failing series.
# Inputs: One stats::ts accepted by tsfeatures.
# Outputs: Named scalar entropy, using zero when entropy is not finite or available.
entropy_feature_fallback <- function(x) {
  series_sd <- stats::sd(as.numeric(x), na.rm = TRUE)
  if (length(x) < 2L || !is.finite(series_sd) || series_sd == 0) {
    return(c(entropy = 0))
  }
  value <- try(tsfeatures::entropy(x), silent = TRUE)
  if (inherits(value, "try-error") || length(value) < 1L || !is.finite(value[[1L]])) {
    return(c(entropy = 0))
  }
  c(entropy = as.numeric(value[[1L]]))
}

# Purpose: Fit the legacy Holt-Winters AAA parameters with its NA fallback.
# Inputs: One stats::ts accepted by forecast::ets.
# Outputs: Named alpha, beta, gamma parameters; unavailable positions are NA and
#   are converted to zero only by the final feature-vector missing-value policy.
hw_parameters_feature_fallback <- function(x) {
  parameters <- c(NA_real_, NA_real_, NA_real_)
  fit <- try(forecast::ets(x, model = "AAA"), silent = TRUE)
  if (!inherits(fit, "try-error")) {
    count <- min(3L, length(fit$par))
    if (count > 0L) parameters[seq_len(count)] <- as.numeric(fit$par[seq_len(count)])
  }
  stats::setNames(parameters, c("hw_alpha", "hw_beta", "hw_gamma"))
}

# Purpose: Calculate exactly one FFORMA-style feature row independently.
# Inputs: A finite, non-empty stats::ts with its authoritative resolved frequency.
# Outputs: Named finite numeric vector in FEATURE_SCHEMA order. Unexpected,
#   duplicate, or nonseasonal-unrelated missing names raise a schema error.
# Design: This bounded stateless scientific kernel uses the code standard's
#   practical functional exception; an object would add no state or cohesion.
calculate_fforma_features <- function(series) {
  if (!inherits(series, "ts") || length(series) < 1L || any(!is.finite(series))) {
    stop("feature input must be a non-empty finite stats::ts", call. = FALSE)
  }

  row <- tsfeatures::tsfeatures(
    series,
    features = c(
      "acf_features", "arch_stat", "crossing_points", entropy_feature_fallback,
      "flat_spots", heterogeneity_feature_fallback, "holt_parameters", "hurst",
      "lumpiness", "nonlinearity", "pacf_features", "stl_features", "stability",
      hw_parameters_feature_fallback, "unitroot_kpss", "unitroot_pp"
    ),
    scale = TRUE
  )
  values <- unlist(row[1L, ], use.names = TRUE)
  values <- c(values, series_length = length(series))

  if (anyDuplicated(names(values))) {
    stop("feature calculation returned duplicate names", call. = FALSE)
  }
  unexpected <- setdiff(names(values), FEATURE_SCHEMA)
  missing <- setdiff(FEATURE_SCHEMA, names(values))
  allowed_missing <- if (stats::frequency(series) == 1) {
    NONSEASONAL_OPTIONAL_FEATURES
  } else {
    character()
  }
  if (length(unexpected) > 0L || length(setdiff(missing, allowed_missing)) > 0L) {
    stop(sprintf(
      "feature schema mismatch (unexpected: %s; missing: %s)",
      paste(unexpected, collapse = ","), paste(missing, collapse = ",")
    ), call. = FALSE)
  }
  values[intersect(missing, allowed_missing)] <- 0
  values[is.na(values)] <- 0
  values <- as.numeric(values[FEATURE_SCHEMA]) |> stats::setNames(FEATURE_SCHEMA)
  if (length(values) != length(FEATURE_SCHEMA) || any(!is.finite(values))) {
    stop("feature calculation produced non-finite or incomplete output", call. = FALSE)
  }
  values
}
