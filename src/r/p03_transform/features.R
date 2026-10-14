# Process 03 reusable FFORMA features: finite prepared stats::ts -> 42 named values.
# Native tsfeatures formulas and scaling follow the FFORMA feature pool
# (Montero-Manso et al., 2020); approved missing/fallback policies are below.
# No future observations, forecasting, membership selection or storage occurs here.

# Fixed feature order shared by provider output and downstream learners.
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

# Future-dependent directional features are excluded from the deployable schema.
DISABLED_DIRECTIONAL_FEATURES <- c(
  "da_mda_arima", "da_mdv_arima", "da_mdpv_arima", "da_pt_pvalue_arima",
  "da_mda_ets", "da_mdv_ets", "da_mdpv_ets", "da_pt_pvalue_ets"
)
DIRECTIONAL_FEATURES_DISABLED_REASON <- paste(
  "Legacy training used window futures while real prediction used a historical",
  "holdout; forecasts and directional features are therefore disabled."
)

# stats::ts -> four heterogeneity values; fitting errors return four zeros.
heterogeneity_feature_fallback <- function(x) {
  fallback <- c(arch_acf = 0, garch_acf = 0, arch_r2 = 0, garch_r2 = 0)
  value <- try(tsfeatures::heterogeneity(x), silent = TRUE)
  if (inherits(value, "try-error")) fallback else value
}

# stats::ts -> entropy; constant/short inputs and unavailable entropy return zero.
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

# stats::ts -> ETS(AAA) alpha/beta/gamma; fit failure returns NA, converted to zero
# only by the final feature-vector policy.
hw_parameters_feature_fallback <- function(x) {
  parameters <- c(NA_real_, NA_real_, NA_real_)
  fit <- try(forecast::ets(x, model = "AAA"), silent = TRUE)
  if (!inherits(fit, "try-error")) {
    count <- min(3L, length(fit$par))
    if (count > 0L) parameters[seq_len(count)] <- as.numeric(fit$par[seq_len(count)])
  }
  stats::setNames(parameters, c("hw_alpha", "hw_beta", "hw_gamma"))
}

# Finite nonempty stats::ts -> FEATURE_SCHEMA order. tsfeatures scales each series
# independently; only absent nonseasonal fields and final NA values become zero.
# Duplicate/unexpected names, other missing fields and nonfinite output fail.
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
