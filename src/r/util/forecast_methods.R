# ==============================================================================
# forecast_methods.R
#
# Purpose: Define the registered probabilistic R forecast pool and unregistered M4
#   point methods with their shared native scientific calculations.
# Inputs: Validated request lists for registered methods, or one finite stats::ts
#   and positive horizon for unregistered M4 point methods.
# Outputs: JSON-serialisable registered results or numeric point vectors; performs
#   no file or database writes.
# Run from: Imported; not run directly.
# ==============================================================================

# Code constant: the approved initial FFORMA-derived method identifiers and order.
.M4_FORECAST_METHODS <- c(
  "auto_arima_forec",
  "ets_forec",
  "nnetar_forec",
  "tbats_forec",
  "stlm_ar_forec",
  "rw_drift_forec",
  "thetaf_forec",
  "naive_forec",
  "snaive_forec"
)

# Code constant: deterministic defaults for NNETAR fitting and predictive simulation.
.NNETAR_DEFAULT_SEED <- 1234L
.NNETAR_DEFAULT_PATHS <- 1000L

# Shared object adapter: preprocessing and every R forecasting method construct
# stats::ts through this one utility; methods receive an already adapted object.
source("src/r/util/time_series_input.R")

#' Return the approved M4 forecasting method identifiers.
#'
#' Purpose: Expose the initial forecast pool in the original FFORMA order.
#' Inputs: None.
#' Outputs: Character vector containing the nine registered method identifiers.
M4_forec_methods <- function() {
  .M4_FORECAST_METHODS
}

# Purpose: Return a default when one list element is absent or NULL.
# Inputs: Candidate value and its default.
# Outputs: Candidate when non-NULL, otherwise the default; has no side effects.
`%||%` <- function(value, default) {
  if (is.null(value)) default else value
}

# Purpose: Validate a scalar identity and normalise it for JSON output.
# Inputs: Candidate value and field name used in errors.
# Outputs: One non-empty character value; invalid identities raise an error.
.identity_value <- function(value, field) {
  if (is.null(value) || length(value) != 1L || is.list(value) || is.na(value)) {
    stop(sprintf("%s must be one non-empty identifier", field), call. = FALSE)
  }
  value <- as.character(value)
  if (!nzchar(value)) {
    stop(sprintf("%s must be one non-empty identifier", field), call. = FALSE)
  }
  value
}

# Purpose: Validate a positive whole-number request field.
# Inputs: Candidate value and field name used in errors.
# Outputs: Positive integer scalar; malformed values raise an error.
.positive_integer <- function(value, field) {
  value <- suppressWarnings(as.numeric(unlist(value)))
  if (
    length(value) != 1L || !is.finite(value) || value <= 0 ||
      value != floor(value) || value > .Machine$integer.max
  ) {
    stop(sprintf("%s must be a positive integer", field), call. = FALSE)
  }
  as.integer(value)
}

# Purpose: Read and validate one logical method setting.
# Inputs: Settings list, setting name, and documented default.
# Outputs: One TRUE/FALSE value; malformed configured values raise an error.
.logical_setting <- function(settings, name, default) {
  value <- settings[[name]] %||% default
  if (length(value) != 1L || !is.logical(value) || is.na(value)) {
    stop(sprintf("settings.%s must be true or false", name), call. = FALSE)
  }
  value
}

# Purpose: Read and validate one positive integer method setting.
# Inputs: Settings list, setting name, and documented default.
# Outputs: Positive integer value; malformed configured values raise an error.
.integer_setting <- function(settings, name, default) {
  .positive_integer(settings[[name]] %||% default, sprintf("settings.%s", name))
}

#' Validate and normalise one forecast-method request.
#'
#' Purpose: Enforce the common Gate 4 input contract before model execution.
#' Inputs: Request list with dataset_id, series_id, finite numeric context,
#'   positive horizon and frequency/seasonality, registered method_id, settings,
#'   ordered unique quantile_levels, and optional task_id/run_id.
#' Outputs: Normalised request list. Future actual observations are neither accepted
#'   nor returned; malformed inputs raise an error before fallback is considered.
validate_forecast_request <- function(request, expected_method_id = NULL) {
  if (!is.list(request)) {
    stop("forecast request must be a list", call. = FALSE)
  }
  forbidden_actuals <- intersect(
    names(request), c("actual", "actuals", "future", "future_actuals", "labels")
  )
  if (length(forbidden_actuals) > 0L) {
    stop("forecast request must not contain future actual observations", call. = FALSE)
  }

  method_id <- .identity_value(request$method_id, "method_id")
  if (!method_id %in% M4_forec_methods()) {
    stop(sprintf("unknown forecast method_id: %s", method_id), call. = FALSE)
  }
  if (!is.null(expected_method_id) && !identical(method_id, expected_method_id)) {
    stop(
      sprintf("%s adapter cannot execute method_id %s", expected_method_id, method_id),
      call. = FALSE
    )
  }

  context <- suppressWarnings(as.numeric(unlist(request$context)))
  if (length(context) < 1L || any(!is.finite(context))) {
    stop("context must be a non-empty finite numeric series", call. = FALSE)
  }
  frequency <- request$frequency %||% request$seasonality
  frequency <- .positive_integer(frequency, "frequency")
  horizon <- .positive_integer(request$horizon, "horizon")

  quantile_levels <- suppressWarnings(as.numeric(unlist(request$quantile_levels)))
  if (
    length(quantile_levels) < 1L || any(!is.finite(quantile_levels)) ||
      any(quantile_levels <= 0 | quantile_levels >= 1) ||
      is.unsorted(quantile_levels, strictly = TRUE)
  ) {
    stop(
      "quantile_levels must be ordered, unique numeric values strictly between zero and one",
      call. = FALSE
    )
  }

  settings <- request$settings %||% list()
  if (!is.list(settings)) {
    stop("settings must be a list", call. = FALSE)
  }

  list(
    task_id = if (is.null(request$task_id)) NULL else .identity_value(request$task_id, "task_id"),
    run_id = if (is.null(request$run_id)) NULL else .identity_value(request$run_id, "run_id"),
    dataset_id = .identity_value(request$dataset_id, "dataset_id"),
    series_id = .identity_value(request$series_id, "series_id"),
    context = context,
    horizon = horizon,
    frequency = frequency,
    method_id = method_id,
    settings = settings,
    quantile_levels = quantile_levels
  )
}

# Purpose: Derive forecast-package central interval levels for requested quantiles.
# Inputs: Ordered quantile probabilities strictly between zero and one.
# Outputs: Increasing unique central percentages, excluding the median.
.central_interval_levels <- function(quantile_levels) {
  is_median <- abs(quantile_levels - 0.5) < 1e-12
  levels <- round(100 * abs(2 * quantile_levels[!is_median] - 1), digits = 10L)
  sort(unique(levels))
}

# Purpose: Confirm configured interval levels match the requested quantile contract.
# Inputs: Method settings and levels derived from request quantiles.
# Outputs: Derived levels unchanged; inconsistent explicit settings raise an error.
.validated_interval_levels <- function(settings, derived_levels) {
  configured <- settings$interval_levels
  if (!is.null(configured)) {
    configured <- sort(unique(round(as.numeric(unlist(configured)), digits = 10L)))
    if (
      length(configured) != length(derived_levels) ||
        any(abs(configured - derived_levels) > 1e-10)
    ) {
      stop(
        "settings.interval_levels do not match the requested quantile levels",
        call. = FALSE
      )
    }
  }
  derived_levels
}

# Purpose: Map central forecast intervals to ordered quantile rows.
# Inputs: A forecast object, requested levels, and expected horizon.
# Outputs: Mean, equal median, and quantile matrix. These adapters do not request
#   internal Box-Cox transforms, so forecast-package intervals are symmetric on
#   their input scale and the package point forecast is also q0.5.
.distribution_from_intervals <- function(predicted, quantile_levels, horizon) {
  mean <- as.numeric(predicted$mean)
  interval_levels <- as.numeric(predicted$level)
  lower <- as.matrix(predicted$lower)
  upper <- as.matrix(predicted$upper)
  quantiles <- matrix(
    NA_real_, nrow = length(quantile_levels), ncol = horizon
  )

  for (row in seq_along(quantile_levels)) {
    quantile <- quantile_levels[[row]]
    if (abs(quantile - 0.5) < 1e-12) {
      quantiles[row, ] <- mean
      next
    }
    central_level <- round(100 * abs(2 * quantile - 1), digits = 10L)
    column <- which(abs(interval_levels - central_level) < 1e-8)
    if (length(column) != 1L) {
      stop(
        sprintf("forecast output is missing the %.10g%% central interval", central_level),
        call. = FALSE
      )
    }
    quantiles[row, ] <- if (quantile < 0.5) {
      as.numeric(lower[, column])
    } else {
      as.numeric(upper[, column])
    }
  }

  list(mean = mean, median = mean, quantiles = quantiles)
}

# Purpose: Build concise package and distribution provenance for one adapter.
# Inputs: Method identifier, scientific output policy, and selected method settings.
# Outputs: JSON-safe provenance list with R and forecast-package versions.
.forecast_provenance <- function(method_id, distribution, settings = list()) {
  list(
    method_id = method_id,
    package = "forecast",
    package_version = as.character(utils::packageVersion("forecast")),
    r_version = R.version.string,
    distribution = distribution,
    settings = settings
  )
}

#' Validate one successful common forecast result.
#'
#' Purpose: Prevent malformed or scientifically invalid output from leaving Gate 4.
#' Inputs: Result list containing identities, method/fallback fields, horizon vectors,
#'   ordered quantile levels, quantile matrix, status, and provenance.
#' Outputs: Result unchanged; invalid dimensions, non-finite values, crossing
#'   quantiles, inconsistent fallback metadata, or actual observations raise an error.
validate_forecast_result <- function(result) {
  required <- c(
    "dataset_id", "series_id", "requested_method_id", "executed_method_id",
    "horizon", "mean", "median", "quantile_levels", "quantiles",
    "fallback_used", "fallback_reason", "provenance", "status"
  )
  missing <- setdiff(required, names(result))
  if (length(missing) > 0L) {
    stop(sprintf("forecast result is missing fields: %s", paste(missing, collapse = ", ")), call. = FALSE)
  }
  forbidden_actuals <- intersect(
    names(result), c("actual", "actuals", "future", "future_actuals", "labels")
  )
  if (length(forbidden_actuals) > 0L) {
    stop("forecast result must not contain actual observations", call. = FALSE)
  }

  .identity_value(result$dataset_id, "result dataset_id")
  .identity_value(result$series_id, "result series_id")
  requested_method_id <- .identity_value(
    result$requested_method_id, "result requested_method_id"
  )
  executed_method_id <- .identity_value(
    result$executed_method_id, "result executed_method_id"
  )
  horizon <- .positive_integer(result$horizon, "result horizon")
  if (!is.numeric(result$mean) || !is.numeric(result$median)) {
    stop("forecast result mean and median must be numeric", call. = FALSE)
  }
  levels <- as.numeric(result$quantile_levels)
  if (
    length(levels) < 1L || any(!is.finite(levels)) ||
      any(levels <= 0 | levels >= 1) || is.unsorted(levels, strictly = TRUE)
  ) {
    stop("forecast result quantile levels are invalid", call. = FALSE)
  }
  if (
    !requested_method_id %in% M4_forec_methods() ||
      !executed_method_id %in% M4_forec_methods()
  ) {
    stop("forecast result method identifiers are not registered", call. = FALSE)
  }
  quantiles <- result$quantiles
  if (!is.matrix(quantiles) || !is.numeric(quantiles)) {
    stop("forecast result quantiles must be a numeric matrix", call. = FALSE)
  }
  if (!identical(dim(quantiles), c(length(levels), horizon))) {
    stop("forecast result quantile dimensions must be levels by horizon", call. = FALSE)
  }
  if (length(result$mean) != horizon || length(result$median) != horizon) {
    stop("forecast result mean and median lengths must equal horizon", call. = FALSE)
  }
  values <- c(result$mean, result$median, as.numeric(quantiles))
  if (any(!is.finite(values))) {
    stop("forecast result contains non-finite forecast values", call. = FALSE)
  }
  if (any(apply(quantiles, 2L, function(values) any(diff(values) < 0)))) {
    stop("forecast result contains crossing quantiles", call. = FALSE)
  }
  median_row <- which(abs(levels - 0.5) < 1e-12)
  if (
    length(median_row) == 1L &&
      any(abs(as.numeric(quantiles[median_row, ]) - as.numeric(result$median)) > 1e-10)
  ) {
    stop("forecast result median does not equal its q0.5 row", call. = FALSE)
  }
  if (length(result$status) != 1L || !identical(result$status, "success")) {
    stop("successful forecast result status must be 'success'", call. = FALSE)
  }
  if (
    !is.logical(result$fallback_used) || length(result$fallback_used) != 1L ||
      is.na(result$fallback_used)
  ) {
    stop("forecast result fallback_used must be one logical value", call. = FALSE)
  }
  if (!is.list(result$provenance)) {
    stop("forecast result provenance must be a list", call. = FALSE)
  }
  if (isTRUE(result$fallback_used)) {
    if (
      !identical(result$executed_method_id, "snaive_forec") ||
        identical(result$requested_method_id, result$executed_method_id) ||
        !is.character(result$fallback_reason) || !nzchar(result$fallback_reason)
    ) {
      stop("fallback forecast result has inconsistent provenance", call. = FALSE)
    }
  } else if (
    !identical(result$requested_method_id, result$executed_method_id) ||
      !is.null(result$fallback_reason)
  ) {
    stop("normal forecast result has inconsistent fallback provenance", call. = FALSE)
  }
  result
}

# Purpose: Assemble one normal successful result under the common contract.
# Inputs: Validated request, executed method identifier, distribution vectors/matrix,
#   and concise provenance.
# Outputs: Validated JSON-safe result with no fallback and no actual observations.
.normal_result <- function(request, method_id, distribution, provenance) {
  provenance$r_period <- request$frequency
  result <- list(
    task_id = request$task_id,
    run_id = request$run_id,
    dataset_id = request$dataset_id,
    series_id = request$series_id,
    requested_method_id = method_id,
    executed_method_id = method_id,
    horizon = request$horizon,
    r_period = request$frequency,
    mean = as.numeric(distribution$mean),
    median = as.numeric(distribution$median),
    quantile_levels = as.numeric(request$quantile_levels),
    quantiles = unname(as.matrix(distribution$quantiles)),
    fallback_used = FALSE,
    fallback_reason = NULL,
    provenance = provenance,
    status = "success"
  )
  validate_forecast_result(result)
}

# Purpose: Execute one central-interval adapter under the common result contract.
# Inputs: Request, expected method ID, model-specific forecast function, provenance
#   description, and selected settings for provenance.
# Outputs: Validated successful result; model or output errors propagate to the runner.
.run_interval_adapter <- function(
  request, method_id, forecast_function, distribution_description, provenance_settings
) {
  request <- validate_forecast_request(request, method_id)
  if (!requireNamespace("forecast", quietly = TRUE)) {
    stop("R package 'forecast' is required", call. = FALSE)
  }
  levels <- .validated_interval_levels(
    request$settings,
    .central_interval_levels(request$quantile_levels)
  )
  series <- time_series_from_values(
    request$context, request$frequency, allow_missing = FALSE
  )
  predicted <- forecast_function(series, request$horizon, levels, request$settings)
  distribution <- .distribution_from_intervals(
    predicted, request$quantile_levels, request$horizon
  )
  .normal_result(
    request,
    method_id,
    distribution,
    .forecast_provenance(method_id, distribution_description, provenance_settings)
  )
}

# Purpose: Apply the official M4 90% autocorrelation seasonality test.
# Inputs: One finite numeric series and its positive integer period.
# Outputs: TRUE only when the official 1.645 critical-value rule identifies
#   seasonality; fewer than three cycles and indeterminate results return FALSE.
.m4_seasonality_test <- function(input, period) {
  if (!is.numeric(input) || length(input) < 1L || any(!is.finite(input))) {
    stop("M4 seasonality input must be a non-empty finite numeric series", call. = FALSE)
  }
  period <- .positive_integer(period, "M4 series frequency")
  if (length(input) < 3L * period) {
    return(FALSE)
  }

  autocorrelations <- stats::acf(input, plot = FALSE)$acf[-1L, 1L, 1L]
  critical_limit <- 1.645 / sqrt(length(input)) *
    sqrt(cumsum(c(1, 2 * autocorrelations^2)))
  isTRUE(abs(autocorrelations[period]) > critical_limit[period])
}

# Purpose: Prepare, forecast, and restore one official M4 point forecast.
# Inputs: One finite univariate stats::ts, positive integer horizon, and a function
#   that returns a point forecast from the optionally seasonally adjusted series.
# Outputs: Finite numeric vector of exactly horizon length. Seasonal inputs are
#   adjusted once by classical multiplicative decomposition and restored once.
.m4_point_forecast <- function(series, horizon, forecast_function) {
  if (
    !stats::is.ts(series) || !is.numeric(series) || !is.null(dim(series)) ||
      length(series) < 1L || any(!is.finite(series))
  ) {
    stop("M4 point methods require one finite univariate stats::ts", call. = FALSE)
  }
  horizon <- .positive_integer(horizon, "horizon")
  period <- .positive_integer(stats::frequency(series), "M4 series frequency")
  if (!requireNamespace("forecast", quietly = TRUE)) {
    stop("R package 'forecast' is required", call. = FALSE)
  }

  adjusted <- series
  seasonal_factors <- rep(1, horizon)
  if (period > 1L && .m4_seasonality_test(series, period)) {
    decomposition <- stats::decompose(series, type = "multiplicative")
    adjusted <- series / decomposition$seasonal
    seasonal_factors <- rep(
      utils::tail(decomposition$seasonal, period),
      length.out = horizon
    )
  }

  point_forecast <- as.numeric(forecast_function(adjusted, horizon))
  if (length(point_forecast) != horizon) {
    stop("M4 point forecast output length must equal horizon", call. = FALSE)
  }
  if (any(!is.finite(point_forecast))) {
    stop("M4 point forecast output must contain only finite values", call. = FALSE)
  }
  output <- point_forecast * seasonal_factors
  if (any(!is.finite(output))) {
    stop("M4 seasonal restoration produced non-finite values", call. = FALSE)
  }
  output
}

#' Forecast with the official M4 Naive2 benchmark.
#'
#' Inputs: One finite univariate stats::ts and a positive integer horizon.
#' Outputs: Numeric mean forecast after shared optional M4 seasonal adjustment.
naive2_forec <- function(x, h) {
  .m4_point_forecast(x, h, function(series, horizon) {
    forecast::naive(series, h = horizon)$mean
  })
}

#' Forecast with the official M4 simple exponential smoothing benchmark.
#'
#' Inputs: One finite univariate stats::ts and a positive integer horizon.
#' Outputs: Numeric mean forecast after shared optional M4 seasonal adjustment.
ses_forec <- function(x, h) {
  .m4_point_forecast(x, h, function(series, horizon) {
    forecast::ses(series, h = horizon)$mean
  })
}

#' Forecast with the official undamped M4 Holt benchmark.
#'
#' Inputs: One finite univariate stats::ts and a positive integer horizon.
#' Outputs: Numeric mean forecast after shared optional M4 seasonal adjustment.
holt_forec <- function(x, h) {
  .m4_point_forecast(x, h, function(series, horizon) {
    forecast::holt(series, h = horizon, damped = FALSE)$mean
  })
}

#' Forecast with the official damped M4 Holt benchmark.
#'
#' Inputs: One finite univariate stats::ts and a positive integer horizon.
#' Outputs: Numeric mean forecast after shared optional M4 seasonal adjustment.
damped_forec <- function(x, h) {
  .m4_point_forecast(x, h, function(series, horizon) {
    forecast::holt(series, h = horizon, damped = TRUE)$mean
  })
}

#' Forecast with automatic ARIMA selection.
#'
#' Inputs: One common forecast request whose method_id is auto_arima_forec.
#' Outputs: Common result using Gaussian central intervals; transformed-scale mean
#'   equals median because no model-internal transformation is requested.
auto_arima_forec <- function(request) {
  settings <- request$settings %||% list()
  selected <- list(
    stepwise = .logical_setting(settings, "stepwise", FALSE),
    approximation = .logical_setting(settings, "approximation", FALSE),
    allowdrift = .logical_setting(settings, "allowdrift", TRUE),
    allowmean = .logical_setting(settings, "allowmean", TRUE),
    parallel = .logical_setting(settings, "parallel", FALSE),
    num_cores = .integer_setting(settings, "num_cores", 1L)
  )
  .run_interval_adapter(
    request,
    "auto_arima_forec",
    function(series, horizon, levels, unused_settings) {
      fit <- forecast::auto.arima(
        series,
        stepwise = selected$stepwise,
        approximation = selected$approximation,
        allowdrift = selected$allowdrift,
        allowmean = selected$allowmean,
        parallel = selected$parallel,
        num.cores = selected$num_cores
      )
      forecast::forecast(fit, h = horizon, level = levels)
    },
    "forecast central intervals; symmetric Gaussian q0.5 equals point mean",
    selected
  )
}

#' Forecast with exponential smoothing state-space model selection.
#'
#' Inputs: One common forecast request whose method_id is ets_forec.
#' Outputs: Common result using forecast-package central intervals and their
#'   symmetric transformed-scale mean/median, without an internal transformation.
ets_forec <- function(request) {
  settings <- request$settings %||% list()
  opt_crit <- settings$opt_crit %||% "mae"
  if (length(opt_crit) != 1L || !identical(as.character(opt_crit), "mae")) {
    stop("settings.opt_crit must be 'mae' for ets_forec", call. = FALSE)
  }
  .run_interval_adapter(
    request,
    "ets_forec",
    function(series, horizon, levels, unused_settings) {
      fit <- forecast::ets(series, opt.crit = "mae", lambda = NULL)
      forecast::forecast(fit, h = horizon, level = levels, lambda = NULL)
    },
    "forecast central intervals without Box-Cox; symmetric q0.5 equals point mean",
    list(opt_crit = "mae", lambda = NULL)
  )
}

# Purpose: Evaluate an expression-producing function under a local deterministic seed.
# Inputs: Integer seed and zero-argument function containing stochastic work.
# Outputs: Function result while restoring the caller's prior random-number state.
.with_seed <- function(seed, operation) {
  had_seed <- exists(".Random.seed", envir = .GlobalEnv, inherits = FALSE)
  if (had_seed) {
    old_seed <- .GlobalEnv$.Random.seed
  }
  on.exit({
    if (had_seed) {
      assign(".Random.seed", old_seed, envir = .GlobalEnv)
    } else if (exists(".Random.seed", envir = .GlobalEnv, inherits = FALSE)) {
      rm(".Random.seed", envir = .GlobalEnv)
    }
  })
  set.seed(seed)
  operation()
}

#' Forecast with a neural-network autoregression and predictive simulation.
#'
#' Inputs: One common request whose settings may specify seed, repeats, npaths,
#'   and bootstrap; deterministic defaults are 1234, 20, 1000, and false.
#' Outputs: Common result preserving the fitted model's standard FFORMA point
#'   forecast as mean; median and q0.1-q0.9 are empirical summaries of predictive
#'   paths simulated from that same, single fit.
nnetar_forec <- function(request) {
  request <- validate_forecast_request(request, "nnetar_forec")
  if (!requireNamespace("forecast", quietly = TRUE)) {
    stop("R package 'forecast' is required", call. = FALSE)
  }
  seed <- .integer_setting(request$settings, "seed", .NNETAR_DEFAULT_SEED)
  repeats <- .integer_setting(request$settings, "repeats", 20L)
  npaths <- .integer_setting(request$settings, "npaths", .NNETAR_DEFAULT_PATHS)
  bootstrap <- .logical_setting(request$settings, "bootstrap", FALSE)
  series <- time_series_from_values(
    request$context, request$frequency, allow_missing = FALSE
  )

  distribution <- .with_seed(seed, function() {
    fit <- forecast::nnetar(series, repeats = repeats, lambda = NULL)
    point_mean <- as.numeric(forecast::forecast(fit, h = request$horizon)$mean)
    simulations <- matrix(NA_real_, nrow = npaths, ncol = request$horizon)
    for (path in seq_len(npaths)) {
      simulations[path, ] <- as.numeric(stats::simulate(
        fit,
        nsim = request$horizon,
        future = TRUE,
        bootstrap = bootstrap,
        lambda = NULL
      ))
    }
    quantiles <- vapply(
      seq_len(request$horizon),
      function(step) stats::quantile(
        simulations[, step], probs = request$quantile_levels,
        type = 8L, names = FALSE
      ),
      numeric(length(request$quantile_levels))
    )
    if (!is.matrix(quantiles)) {
      quantiles <- matrix(quantiles, ncol = request$horizon)
    }
    median <- vapply(
      seq_len(request$horizon),
      function(step) stats::quantile(
        simulations[, step], probs = 0.5, type = 8L, names = FALSE
      ),
      numeric(1L)
    )
    list(
      mean = point_mean,
      median = median,
      quantiles = quantiles
    )
  })

  .normal_result(
    request,
    "nnetar_forec",
    distribution,
    .forecast_provenance(
      "nnetar_forec",
      paste(
        "standard fitted-model point mean; deterministic predictive simulation",
        "for empirical median and type-8 quantiles"
      ),
      list(seed = seed, repeats = repeats, npaths = npaths, bootstrap = bootstrap)
    )
  )
}

#' Forecast with TBATS.
#'
#' Inputs: One common forecast request whose method_id is tbats_forec.
#' Outputs: Common result using central intervals from the same fitted model. TBATS
#'   retains its original automatic internal component selection, including Box-Cox;
#'   this is part of the model and is distinct from Gate 3 transformations.
tbats_forec <- function(request) {
  .run_interval_adapter(
    request,
    "tbats_forec",
    function(series, horizon, levels, unused_settings) {
      fit <- forecast::tbats(series, use.parallel = FALSE)
      forecast::forecast(fit, h = horizon, level = levels)
    },
    paste(
      "forecast central intervals from the FFORMA-compatible TBATS fit;",
      "package point forecast supplies q0.5 and median"
    ),
    list(use_parallel = FALSE, automatic_component_selection = TRUE)
  )
}

#' Forecast an STL decomposition with an autoregressive remainder model.
#'
#' Inputs: One common forecast request whose method_id is stlm_ar_forec.
#' Outputs: Common result using symmetric central intervals. Unlike the old FFORMA
#'   function, fitting errors remain visible to the pool-level seasonal-naive fallback.
stlm_ar_forec <- function(request) {
  .run_interval_adapter(
    request,
    "stlm_ar_forec",
    function(series, horizon, levels, unused_settings) {
      fit <- forecast::stlm(series, modelfunction = stats::ar, lambda = NULL)
      forecast::forecast(fit, h = horizon, level = levels, lambda = NULL)
    },
    "forecast central intervals without Box-Cox; symmetric q0.5 equals point mean",
    list(model_function = "stats::ar", lambda = NULL)
  )
}

#' Forecast with a random walk including drift.
#'
#' Inputs: One common forecast request whose method_id is rw_drift_forec.
#' Outputs: Common result using the model's symmetric Gaussian central intervals.
rw_drift_forec <- function(request) {
  .run_interval_adapter(
    request,
    "rw_drift_forec",
    function(series, horizon, levels, unused_settings) {
      forecast::rwf(series, drift = TRUE, h = horizon, level = levels, lambda = NULL)
    },
    "Gaussian random-walk central intervals; q0.5 equals point mean",
    list(drift = TRUE, lambda = NULL)
  )
}

#' Forecast with the Theta method.
#'
#' Inputs: One common forecast request whose method_id is thetaf_forec.
#' Outputs: Common result using the method's symmetric central intervals.
thetaf_forec <- function(request) {
  .run_interval_adapter(
    request,
    "thetaf_forec",
    function(series, horizon, levels, unused_settings) {
      forecast::thetaf(series, h = horizon, level = levels)
    },
    "Theta-method central intervals; q0.5 equals point mean",
    list()
  )
}

#' Forecast with the last-observation naïve method.
#'
#' Inputs: One common forecast request whose method_id is naive_forec.
#' Outputs: Common result using the model's symmetric Gaussian central intervals.
naive_forec <- function(request) {
  .run_interval_adapter(
    request,
    "naive_forec",
    function(series, horizon, levels, unused_settings) {
      forecast::naive(series, h = horizon, level = levels, lambda = NULL)
    },
    "Gaussian naive central intervals; q0.5 equals point mean",
    list(lambda = NULL)
  )
}

#' Forecast with the seasonal-naïve method.
#'
#' Inputs: One common forecast request whose method_id is snaive_forec.
#' Outputs: Common result using symmetric Gaussian central intervals. This callable
#'   is also the one explicit, non-recursive pool fallback.
snaive_forec <- function(request) {
  .run_interval_adapter(
    request,
    "snaive_forec",
    function(series, horizon, levels, unused_settings) {
      forecast::snaive(series, h = horizon, level = levels, lambda = NULL)
    },
    "Gaussian seasonal-naive central intervals; q0.5 equals point mean",
    list(lambda = NULL)
  )
}

#' Return the named allowlist of forecast callables.
#'
#' Purpose: Provide explicit dispatch without get(), eval(), parse(), or JSON code.
#' Inputs: None.
#' Outputs: Named list whose names and callable values exactly match M4_forec_methods().
forecast_method_registry <- function() {
  list(
    auto_arima_forec = auto_arima_forec,
    ets_forec = ets_forec,
    nnetar_forec = nnetar_forec,
    tbats_forec = tbats_forec,
    stlm_ar_forec = stlm_ar_forec,
    rw_drift_forec = rw_drift_forec,
    thetaf_forec = thetaf_forec,
    naive_forec = naive_forec,
    snaive_forec = snaive_forec
  )
}

# Purpose: Validate a supplied registry while preserving its explicit allowlist.
# Inputs: Candidate named list, including injectable registered functions for tests.
# Outputs: Registry unchanged; missing, extra, reordered, or non-callable entries fail.
.validated_registry <- function(registry) {
  if (
    !is.list(registry) || !identical(names(registry), M4_forec_methods()) ||
      any(!vapply(registry, is.function, logical(1L)))
  ) {
    stop(
      "forecast registry must contain the nine approved callable methods in order",
      call. = FALSE
    )
  }
  registry
}

#' Execute one registered forecast method with visible seasonal-naive fallback.
#'
#' Purpose: Provide generic allowlisted Gate 4 dispatch and validate every result.
#' Inputs: Common request and, for controlled tests, a complete approved registry.
#' Outputs: One successful common result. Requested-model failures attempt snaive once,
#'   emit a warning, and retain the original error; direct or failed fallback errors
#'   terminate clearly and never recurse.
run_forecast_method <- function(request, registry = forecast_method_registry()) {
  registry <- .validated_registry(registry)
  request <- validate_forecast_request(request)
  method_id <- request$method_id

  result <- tryCatch(
    validate_forecast_result(registry[[method_id]](request)),
    error = function(error) error
  )
  if (!inherits(result, "error")) {
    return(result)
  }

  original_error <- conditionMessage(result)
  if (identical(method_id, "snaive_forec")) {
    stop(
      sprintf(
        "forecast method 'snaive_forec' failed; no recursive fallback attempted: %s",
        original_error
      ),
      call. = FALSE
    )
  }

  fallback_request <- request
  fallback_request$method_id <- "snaive_forec"
  fallback <- tryCatch(
    validate_forecast_result(registry[["snaive_forec"]](fallback_request)),
    error = function(error) error
  )
  if (inherits(fallback, "error")) {
    stop(
      sprintf(
        "forecast method '%s' failed: %s; seasonal-naive fallback also failed: %s",
        method_id, original_error, conditionMessage(fallback)
      ),
      call. = FALSE
    )
  }

  fallback$requested_method_id <- method_id
  fallback$executed_method_id <- "snaive_forec"
  fallback$fallback_used <- TRUE
  fallback$fallback_reason <- original_error
  warning(
    sprintf(
      "forecast method '%s' failed; executed snaive_forec fallback: %s",
      method_id, original_error
    ),
    call. = FALSE
  )
  validate_forecast_result(fallback)
}

#' Execute selected registered methods in deterministic caller order.
#'
#' Purpose: Demonstrate generic pool execution without coordinator model branches.
#' Inputs: Common base request, ordered registered method IDs, and optional test registry.
#' Outputs: One ordered result entry per requested method, including one entry when
#'   fallback executes; terminal method/fallback failures stop the call.
run_forecast_methods <- function(
  request, method_ids = request$method_id, registry = forecast_method_registry()
) {
  registry <- .validated_registry(registry)
  method_ids <- as.character(unlist(method_ids))
  if (length(method_ids) < 1L || any(!method_ids %in% M4_forec_methods())) {
    stop("method_ids must contain only registered forecast methods", call. = FALSE)
  }
  lapply(method_ids, function(method_id) {
    selected_request <- request
    selected_request$method_id <- method_id
    run_forecast_method(selected_request, registry = registry)
  })
}

#' Encode one validated forecast result as JSON.
#'
#' Purpose: Preserve scientific precision and quantile row orientation at the R boundary.
#' Inputs: One common successful forecast result.
#' Outputs: JSON string with row-major quantile arrays; performs no file writes.
forecast_result_to_json <- function(result) {
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("R package 'jsonlite' is required", call. = FALSE)
  }
  result <- validate_forecast_result(result)
  jsonlite::toJSON(
    result,
    auto_unbox = TRUE,
    digits = 17,
    null = "null",
    matrix = "rowmajor"
  )
}

# Gate boundary: Gate 3 supplies an already prepared numeric context and retains
# fitted transformation state. This Gate 4 library neither interprets transformation
# expressions nor inverts forecasts; the caller restores every output to the approved
# scale before persistence. Gate 5, not this library, selects or combines forecasts.
