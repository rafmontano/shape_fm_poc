#!/usr/bin/env Rscript
# ==============================================================================
# test_forecast_methods.R
#
# Purpose: Verify the registered R forecast pool, unregistered M4 point methods,
#   scientific parity, and fallback contract.
# Inputs: Project R library plus src/r/util/forecast_methods.R.
# Outputs: Test progress on stdout and nonzero exit on assertion failure; writes nothing.
# Run from: Rscript src/r/tests/test_forecast_methods.R
# ==============================================================================

source("src/r/util/forecast_methods.R")
source("src/r/util/seasonal_period.R")

# Test constant: exact approved identifiers and FFORMA order.
APPROVED_METHODS <- c(
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
# Test constant: available M4 point methods that must remain outside the registry.
AVAILABLE_M4_POINT_METHODS <- c(
  "naive2_forec",
  "ses_forec",
  "holt_forec",
  "damped_forec"
)
# Test constant: current GIFT-Eval probabilistic levels.
GIFT_EVAL_QUANTILES <- seq(0.1, 0.9, by = 0.1)
# Test constant: tight tolerance for official-versus-historical R calculations.
M4_R_TOLERANCE <- 1e-10
# Test constant: deterministic positive seasonal series long enough for STL and TBATS.
TEST_CONTEXT <- as.numeric(
  50 + seq_len(84) * 0.2 +
    rep(c(-3, -1, 0, 2, 4, 1, -2), 12) + sin(seq_len(84))
)
# Test fixtures: asymmetric short, seasonal, and nonseasonal M4 cases.
M4_FIXTURES <- list(
  short = list(values = c(12, 18, 9, 21, 13, 17, 10, 20), period = 4L, seasonal = FALSE),
  seasonal = list(values = rep(c(10, 20, 35, 55), 8L), period = 4L, seasonal = TRUE),
  nonseasonal = list(
    values = c(
      10, 11, 9, 12, 10, 13, 8, 14, 11, 10, 12, 9,
      13, 10, 11, 12, 9, 10, 11, 13, 8, 12, 10, 11
    ),
    period = 4L,
    seasonal = FALSE
  )
)
# Test fixture: approved period-2 boundary case from the historical R implementation.
M4_BOUNDARY_FIXTURE <- stats::ts(c(
  1.3987711506, 0.8172526965, 0.6705580809,
  1.6200446061, 1.6536169158, 0.8855726992
), frequency = 2L)
# Test constant: fixed historical Naive2 output for M4_BOUNDARY_FIXTURE at horizon five.
M4_BOUNDARY_NAIVE2 <- c(
  0.79930807821704164, 0.88557269920000004, 0.79930807821704164,
  0.88557269920000004, 0.79930807821704164
)

# Purpose: Run one named assertion and print concise progress.
# Inputs: Human-readable name and zero-argument assertion function.
# Outputs: TRUE invisibly after success; assertion errors stop the test script.
check <- function(name, assertion) {
  assertion()
  cat(sprintf("ok - %s\n", name))
  invisible(TRUE)
}

# Purpose: Require one logical condition to be true.
# Inputs: Condition and failure message.
# Outputs: TRUE invisibly; false or missing conditions raise a test error.
assert_true <- function(condition, message) {
  if (length(condition) != 1L || is.na(condition) || !condition) {
    stop(message, call. = FALSE)
  }
  invisible(TRUE)
}

# Purpose: Require exact object identity.
# Inputs: Observed value, expected value, and failure message.
# Outputs: TRUE invisibly; non-identical values raise a test error.
assert_identical <- function(observed, expected, message) {
  if (!identical(observed, expected)) {
    stop(message, call. = FALSE)
  }
  invisible(TRUE)
}

# Purpose: Require an operation to fail with matching diagnostic text.
# Inputs: Zero-argument operation, regular expression, and failure message.
# Outputs: Captured error invisibly; missing or mismatched errors raise a test error.
assert_error <- function(operation, pattern, message) {
  error <- tryCatch(operation(), error = function(error) error)
  if (!inherits(error, "error") || !grepl(pattern, conditionMessage(error))) {
    stop(message, call. = FALSE)
  }
  invisible(error)
}

# Purpose: Construct one deterministic common request for a selected method.
# Inputs: Registered method identifier and optional method settings.
# Outputs: Complete request with no future actual observations.
test_request <- function(method_id = "naive_forec", settings = list()) {
  list(
    task_id = "task-1",
    run_id = "run-1",
    dataset_id = "m4_daily",
    series_id = "D1",
    context = TEST_CONTEXT,
    horizon = 3L,
    frequency = 7L,
    method_id = method_id,
    settings = settings,
    quantile_levels = GIFT_EVAL_QUANTILES
  )
}

# Purpose: Select practical deterministic settings for real-adapter tests.
# Inputs: Registered method identifier.
# Outputs: Settings preserving model behavior while bounding test runtime.
test_settings <- function(method_id) {
  if (identical(method_id, "auto_arima_forec")) {
    return(list(
      stepwise = FALSE,
      approximation = FALSE,
      allowdrift = TRUE,
      allowmean = TRUE,
      parallel = FALSE,
      num_cores = 1L,
      interval_levels = c(20, 40, 60, 80)
    ))
  }
  if (identical(method_id, "nnetar_forec")) {
    return(list(seed = 1234L))
  }
  list()
}

# Purpose: Reproduce the original FFORMA point-forecast implementation directly.
# Inputs: Approved method identifier and the same ts/horizon used by the adapter.
# Outputs: Original numeric point forecast, with the NNETAR fit under the same seed.
original_fforma_mean <- function(method_id, series, horizon) {
  switch(
    method_id,
    auto_arima_forec = as.numeric(forecast::forecast(
      forecast::auto.arima(series, stepwise = FALSE, approximation = FALSE), h = horizon
    )$mean),
    ets_forec = as.numeric(forecast::forecast(
      forecast::ets(series, opt.crit = "mae"), h = horizon
    )$mean),
    nnetar_forec = .with_seed(1234L, function() {
      as.numeric(forecast::forecast(forecast::nnetar(series), h = horizon)$mean)
    }),
    tbats_forec = as.numeric(forecast::forecast(
      forecast::tbats(series, use.parallel = FALSE), h = horizon
    )$mean),
    stlm_ar_forec = {
      fit <- tryCatch(
        forecast::stlm(series, modelfunction = stats::ar),
        error = function(error) forecast::auto.arima(series, d = 0, D = 0)
      )
      as.numeric(forecast::forecast(fit, h = horizon)$mean)
    },
    rw_drift_forec = as.numeric(forecast::forecast(
      forecast::rwf(series, drift = TRUE, h = length(series)), h = horizon
    )$mean),
    thetaf_forec = as.numeric(forecast::thetaf(series, h = horizon)$mean),
    naive_forec = as.numeric(forecast::forecast(
      forecast::naive(series, h = length(series)), h = horizon
    )$mean),
    snaive_forec = {
      frequency <- stats::frequency(series)
      as.numeric(utils::tail(series, frequency)[((seq_len(horizon) - 1L) %% frequency) + 1L])
    },
    stop("unsupported original FFORMA method", call. = FALSE)
  )
}

# Purpose: Independently reproduce the official and historical M4 point calculation.
# Inputs: Finite stats::ts, positive horizon, and one available M4 point method ID.
# Outputs: Official numeric point forecast and the independent seasonality decision.
official_m4_reference <- function(series, horizon, method_id) {
  period <- as.integer(stats::frequency(series))
  seasonal <- FALSE
  if (period > 1L && length(series) >= 3L * period) {
    autocorrelations <- stats::acf(series, plot = FALSE)$acf[-1L, 1L, 1L]
    critical_limit <- 1.645 / sqrt(length(series)) *
      sqrt(cumsum(c(1, 2 * autocorrelations^2)))
    seasonal <- isTRUE(abs(autocorrelations[period]) > critical_limit[period])
  }

  adjusted <- series
  seasonal_factors <- rep(1, horizon)
  if (seasonal) {
    decomposition <- stats::decompose(series, type = "multiplicative")
    adjusted <- series / decomposition$seasonal
    seasonal_factors <- rep(
      utils::tail(decomposition$seasonal, period), length.out = horizon
    )
  }
  point <- switch(
    method_id,
    naive2_forec = forecast::naive(adjusted, h = horizon)$mean,
    ses_forec = forecast::ses(adjusted, h = horizon)$mean,
    holt_forec = forecast::holt(adjusted, h = horizon, damped = FALSE)$mean,
    damped_forec = forecast::holt(adjusted, h = horizon, damped = TRUE)$mean,
    stop("unsupported M4 reference method", call. = FALSE)
  )
  list(seasonal = seasonal, forecast = as.numeric(point) * seasonal_factors)
}

# Purpose: Construct a valid synthetic result for controlled registry tests.
# Inputs: Validated request and executed method identifier.
# Outputs: Common result with deterministic finite non-crossing arrays.
synthetic_result <- function(request, method_id = request$method_id) {
  request$method_id <- method_id
  request <- validate_forecast_request(request, method_id)
  quantiles <- outer(
    request$quantile_levels,
    seq_len(request$horizon),
    function(level, step) 10 + level + step
  )
  median_row <- which(abs(request$quantile_levels - 0.5) < 1e-12)
  .normal_result(
    request,
    method_id,
    list(
      mean = as.numeric(quantiles[median_row, ]),
      median = as.numeric(quantiles[median_row, ]),
      quantiles = quantiles
    ),
    list(method_id = method_id, package = "test", package_version = "1")
  )
}

check("M4 method names preserve the approved order", function() {
  assert_identical(M4_forec_methods(), APPROVED_METHODS, "approved method order changed")
})

check("registry names and callables match the allowlist", function() {
  registry <- forecast_method_registry()
  assert_identical(names(registry), APPROVED_METHODS, "registry names or order changed")
  assert_true(all(vapply(registry, is.function, logical(1L))), "registry contains a non-function")
  expected <- list(
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
  assert_true(
    all(mapply(identical, registry, expected)),
    "one or more registered callables changed"
  )
})

check("four M4 point methods exist but remain unregistered", function() {
  assert_true(
    all(vapply(AVAILABLE_M4_POINT_METHODS, exists, logical(1L), mode = "function")),
    "an available M4 point function is missing"
  )
  assert_true(
    !any(AVAILABLE_M4_POINT_METHODS %in% M4_forec_methods()),
    "an M4 point method entered M4_forec_methods"
  )
  assert_true(
    !any(AVAILABLE_M4_POINT_METHODS %in% names(forecast_method_registry())),
    "an M4 point method entered the active registry"
  )
})

check("period-one M4 methods equal their ordinary forecast-package definitions", function() {
  series <- stats::ts(c(3, 5, 8, 13, 12, 16, 19, 23, 21, 25, 29, 34), frequency = 1L)
  horizon <- 5L
  expected <- list(
    naive2_forec = as.numeric(forecast::naive(series, h = horizon)$mean),
    ses_forec = as.numeric(forecast::ses(series, h = horizon)$mean),
    holt_forec = as.numeric(forecast::holt(series, h = horizon, damped = FALSE)$mean),
    damped_forec = as.numeric(forecast::holt(series, h = horizon, damped = TRUE)$mean)
  )
  observed <- list(
    naive2_forec = naive2_forec(series, horizon),
    ses_forec = ses_forec(series, horizon),
    holt_forec = holt_forec(series, horizon),
    damped_forec = damped_forec(series, horizon)
  )
  for (method_id in AVAILABLE_M4_POINT_METHODS) {
    assert_true(
      isTRUE(all.equal(observed[[method_id]], expected[[method_id]], tolerance = 1e-12)),
      sprintf("%s changed its period-one package definition", method_id)
    )
  }
})

check("M4 seasonality rejects fewer than three complete cycles", function() {
  fixture <- M4_FIXTURES$short
  assert_identical(
    .m4_seasonality_test(fixture$values, fixture$period),
    FALSE,
    "short M4 series was classified as seasonal"
  )
  assert_true(
    isTRUE(all.equal(
      naive2_forec(stats::ts(fixture$values, frequency = fixture$period), 7L),
      rep(20, 7L),
      tolerance = M4_R_TOLERANCE
    )),
    "short nonseasonal Naive2 output changed from the official R result"
  )
})

check("M4 seasonality treats an indeterminate result as nonseasonal", function() {
  assert_identical(
    suppressWarnings(.m4_seasonality_test(rep(5, 12L), 4L)),
    FALSE,
    "indeterminate M4 seasonality was not classified as nonseasonal"
  )
})

check("M4 period-2 boundary reproduces historical R seasonality and Naive2", function() {
  assert_identical(
    .m4_seasonality_test(M4_BOUNDARY_FIXTURE, 2L),
    TRUE,
    "period-2 historical boundary was not classified as seasonal"
  )
  assert_true(
    isTRUE(all.equal(
      naive2_forec(M4_BOUNDARY_FIXTURE, 5L),
      M4_BOUNDARY_NAIVE2,
      tolerance = M4_R_TOLERANCE
    )),
    "period-2 Naive2 output changed from the historical R result"
  )
})

check("M4 point methods match official and historical deterministic calculations", function() {
  methods <- list(
    naive2_forec = naive2_forec,
    ses_forec = ses_forec,
    holt_forec = holt_forec,
    damped_forec = damped_forec
  )
  for (fixture_name in c("seasonal", "nonseasonal")) {
    fixture <- M4_FIXTURES[[fixture_name]]
    series <- stats::ts(fixture$values, frequency = fixture$period)
    for (method_id in names(methods)) {
      expected <- official_m4_reference(series, 7L, method_id)
      observed <- methods[[method_id]](series, 7L)
      assert_identical(
        expected$seasonal,
        fixture$seasonal,
        sprintf("%s independent seasonality expectation changed", fixture_name)
      )
      assert_true(
        isTRUE(all.equal(observed, expected$forecast, tolerance = M4_R_TOLERANCE)),
        sprintf("%s disagrees with official M4 on %s", method_id, fixture_name)
      )
    }
  }
})

check("M4 point outputs are finite horizon vectors and preserve every input", function() {
  methods <- list(
    naive2_forec = naive2_forec,
    ses_forec = ses_forec,
    holt_forec = holt_forec,
    damped_forec = damped_forec
  )
  series <- stats::ts(
    M4_FIXTURES$seasonal$values,
    start = c(2001L, 2L),
    frequency = M4_FIXTURES$seasonal$period
  )
  for (method_id in names(methods)) {
    before <- series
    observed <- methods[[method_id]](series, 7L)
    assert_true(is.numeric(observed), sprintf("%s output is not numeric", method_id))
    assert_identical(length(observed), 7L, sprintf("%s output length changed", method_id))
    assert_true(all(is.finite(observed)), sprintf("%s output is non-finite", method_id))
    assert_identical(as.numeric(series), as.numeric(before), sprintf("%s changed values", method_id))
    assert_identical(stats::frequency(series), stats::frequency(before), sprintf("%s changed frequency", method_id))
    assert_identical(stats::start(series), stats::start(before), sprintf("%s changed start", method_id))
    assert_identical(stats::end(series), stats::end(before), sprintf("%s changed end", method_id))
  }
})

check("M4 point methods reject invalid inputs, horizons, and outputs", function() {
  valid <- stats::ts(seq_len(12L), frequency = 1L)
  assert_error(function() naive2_forec(seq_len(12L), 3L), "stats::ts", "plain vector accepted")
  assert_error(function() ses_forec(stats::ts(c(1, Inf)), 3L), "finite", "Inf accepted")
  assert_error(
    function() holt_forec(stats::ts(cbind(seq_len(12L), seq_len(12L))), 3L),
    "univariate",
    "multivariate ts accepted"
  )
  for (horizon in list(0L, -1L, 1.5, NA_real_)) {
    assert_error(
      function() damped_forec(valid, horizon),
      "horizon must be a positive integer",
      "invalid horizon accepted"
    )
  }
  assert_error(
    function() .m4_point_forecast(valid, 3L, function(series, horizon) c(1, 2)),
    "length must equal horizon",
    "short internal point output accepted"
  )
  assert_error(
    function() .m4_point_forecast(valid, 3L, function(series, horizon) rep(Inf, horizon)),
    "only finite values",
    "non-finite internal point output accepted"
  )
})

check("M4 preparation is shared and no refitting comb function exists", function() {
  source_lines <- readLines("src/r/util/forecast_methods.R", warn = FALSE)
  code_lines <- source_lines[!grepl("^\\s*#", source_lines)]
  assert_identical(
    sum(grepl("stats::decompose\\(", code_lines)),
    1L,
    "M4 seasonal decomposition has more than one implementation"
  )
  for (method_id in AVAILABLE_M4_POINT_METHODS) {
    method_text <- paste(deparse(body(get(method_id, mode = "function"))), collapse = "\n")
    assert_true(
      grepl(".m4_point_forecast", method_text, fixed = TRUE),
      sprintf("%s bypasses shared M4 preparation", method_id)
    )
  }
  assert_true(
    !any(grepl("^\\s*comb_forec\\s*<-\\s*function", code_lines)),
    "comb_forec model refitting function was added"
  )
})

check("shared R adapter preserves values and applies the resolved period", function() {
  adapted <- time_series_from_values(TEST_CONTEXT, 7L, allow_missing = FALSE)
  assert_identical(as.numeric(adapted), TEST_CONTEXT, "shared adapter changed input values")
  assert_identical(as.integer(stats::frequency(adapted)), 7L, "shared adapter changed period")
  source_lines <- readLines("src/r/util/forecast_methods.R", warn = FALSE)
  code_lines <- source_lines[!grepl("^\\s*#", source_lines)]
  assert_identical(
    sum(grepl("stats::ts\\(", code_lines)),
    0L,
    "a forecast method independently constructs stats::ts"
  )
})

check("unknown method identifiers are rejected", function() {
  assert_error(
    function() run_forecast_method(test_request("arbitrary_code")),
    "unknown forecast method_id",
    "unknown method was not rejected"
  )
})

check("generic dispatch uses the selected callable", function() {
  registry <- forecast_method_registry()
  called <- FALSE
  registry[["naive_forec"]] <- function(request) {
    called <<- TRUE
    synthetic_result(request)
  }
  result <- run_forecast_method(test_request("naive_forec"), registry)
  assert_true(called, "selected registry callable did not execute")
  assert_identical(result$executed_method_id, "naive_forec", "wrong callable result")
})

# Real model execution demonstrates that every registered adapter can provide the
# common probabilistic output; failure mechanics are tested separately and deterministically.
real_results <- list()
for (method_id in APPROVED_METHODS) {
  check(sprintf("%s returns valid finite probabilistic output", method_id), function() {
    result <- run_forecast_method(test_request(method_id, test_settings(method_id)))
    assert_identical(result$requested_method_id, method_id, "requested method changed")
    assert_identical(result$executed_method_id, method_id, "unexpected real-model fallback")
    assert_identical(result$fallback_used, FALSE, "normal result recorded fallback")
    assert_identical(result$r_period, 7L, "resolved R period was not recorded")
    assert_identical(result$provenance$r_period, 7L, "provenance lost R period")
    assert_true(is.null(result$fallback_reason), "normal result recorded fallback reason")
    assert_identical(length(result$mean), 3L, "mean length is not horizon")
    assert_identical(length(result$median), 3L, "median length is not horizon")
    assert_identical(dim(result$quantiles), c(9L, 3L), "quantile shape is not levels by horizon")
    assert_true(all(is.finite(c(result$mean, result$median, result$quantiles))), "non-finite forecast")
    assert_true(
      all(apply(result$quantiles, 2L, function(values) all(diff(values) >= 0))),
      "forecast contains crossing quantiles"
    )
    assert_true(!any(c("actual", "actuals", "future_actuals") %in% names(result)), "actuals leaked")
    real_results[[method_id]] <<- result
  })
}

for (method_id in APPROVED_METHODS) {
  check(sprintf("%s mean matches original FFORMA", method_id), function() {
    series <- stats::ts(TEST_CONTEXT, frequency = 7L)
    expected <- original_fforma_mean(method_id, series, 3L)
    observed <- real_results[[method_id]]$mean
    assert_true(
      isTRUE(all.equal(observed, expected, tolerance = 1e-10)),
      sprintf("%s adapter changed the original FFORMA point forecast", method_id)
    )
  })
}

check("every registered method matches FFORMA on a short period-one history", function() {
  values <- c(8, 11, 9, 15, 12, 18, 14, 21, 17, 24, 19, 27, 23, 30)
  series <- stats::ts(values, frequency = 1L)
  for (method_id in APPROVED_METHODS) {
    request <- test_request(method_id, test_settings(method_id))
    request$context <- values
    request$frequency <- 1L
    observed <- run_forecast_method(request)
    expected <- original_fforma_mean(method_id, series, request$horizon)
    assert_true(
      isTRUE(all.equal(observed$mean, expected, tolerance = M4_R_TOLERANCE)),
      sprintf("%s disagrees with FFORMA on the short period-one fixture", method_id)
    )
  }
})

check("central interval bounds map to the approved quantile rows", function() {
  request <- test_request("naive_forec")
  expected <- forecast::naive(
    stats::ts(request$context, frequency = request$frequency),
    h = request$horizon,
    level = c(20, 40, 60, 80),
    lambda = NULL
  )
  observed <- real_results[["naive_forec"]]$quantiles
  lower_80 <- as.numeric(expected$lower[, "80%"])
  lower_20 <- as.numeric(expected$lower[, "20%"])
  upper_20 <- as.numeric(expected$upper[, "20%"])
  upper_80 <- as.numeric(expected$upper[, "80%"])
  assert_true(isTRUE(all.equal(observed[1L, ], lower_80)), "q0.1 mapping")
  assert_true(isTRUE(all.equal(observed[4L, ], lower_20)), "q0.4 mapping")
  assert_true(isTRUE(all.equal(observed[5L, ], as.numeric(expected$mean))), "q0.5 mapping")
  assert_true(isTRUE(all.equal(observed[6L, ], upper_20)), "q0.6 mapping")
  assert_true(isTRUE(all.equal(observed[9L, ], upper_80)), "q0.9 mapping")
})

check("NNETAR simulation is deterministic and records its method", function() {
  request <- test_request("nnetar_forec", test_settings("nnetar_forec"))
  repeated <- run_forecast_method(request)
  assert_true(
    isTRUE(all.equal(repeated$mean, real_results$nnetar_forec$mean, tolerance = 0)),
    "NNETAR mean changed under the same seed"
  )
  assert_true(
    isTRUE(all.equal(repeated$quantiles, real_results$nnetar_forec$quantiles, tolerance = 0)),
    "NNETAR quantiles changed under the same seed"
  )
  assert_true(grepl("predictive simulation", repeated$provenance$distribution), "simulation not recorded")
  expected <- original_fforma_mean(
    "nnetar_forec", stats::ts(TEST_CONTEXT, frequency = 7L), request$horizon
  )
  assert_true(
    isTRUE(all.equal(repeated$mean, expected, tolerance = 1e-10)),
    "NNETAR simulation replaced the original FFORMA point mean"
  )
})

check("method failure remains visible and never executes seasonal naive", function() {
  registry <- forecast_method_registry()
  registry[["auto_arima_forec"]] <- function(request) stop("controlled model failure")
  seasonal_naive_called <- FALSE
  registry[["snaive_forec"]] <- function(request) {
    seasonal_naive_called <<- TRUE
    synthetic_result(request)
  }
  assert_error(
    function() run_forecast_methods(test_request("auto_arima_forec"), registry = registry),
    "controlled model failure",
    "method failure was hidden by another model"
  )
  assert_identical(seasonal_naive_called, FALSE, "failure executed seasonal naive")
})

check("invalid model output remains failed and never executes another model", function() {
  registry <- forecast_method_registry()
  seasonal_naive_called <- FALSE
  registry[["ets_forec"]] <- function(request) {
    result <- synthetic_result(request)
    result$quantiles <- result$quantiles[, -1L, drop = FALSE]
    result
  }
  registry[["snaive_forec"]] <- function(request) {
    seasonal_naive_called <<- TRUE
    synthetic_result(request)
  }
  assert_error(
    function() run_forecast_method(test_request("ets_forec"), registry),
    "quantile dimensions",
    "invalid output was replaced by another model"
  )
  assert_identical(seasonal_naive_called, FALSE, "invalid output executed seasonal naive")
})

check("period-one STL uses only the fixed AutoARIMA fitting fallback", function() {
  request <- test_request("stlm_ar_forec")
  request$frequency <- 1L
  result <- run_forecast_method(request)
  series <- stats::ts(request$context, frequency = 1L)
  fit <- forecast::auto.arima(series, d = 0, D = 0)
  expected <- forecast::forecast(fit, h = request$horizon)$mean
  assert_true(
    isTRUE(all.equal(result$mean, as.numeric(expected), tolerance = M4_R_TOLERANCE)),
    "STL fallback changed the fixed AutoARIMA point mean"
  )
  assert_identical(result$requested_method_id, "stlm_ar_forec", "STL request identity changed")
  assert_identical(result$executed_method_id, "auto_arima_forec", "wrong STL fallback executed")
  assert_identical(result$fallback_used, TRUE, "STL fitting fallback was not recorded")
  assert_true(grepl("seasonal", result$fallback_reason), "original STL error was not retained")
  assert_identical(
    result$provenance$settings,
    list(selected_branch = "auto_arima_d0_D0", d = 0L, D = 0L),
    "fixed STL fallback settings changed"
  )
  assert_identical(
    result$provenance$original_stl_error,
    result$fallback_reason,
    "fallback provenance lost the original STL error"
  )
  assert_true(
    identical(result$provenance$package_version, as.character(utils::packageVersion("forecast"))),
    "STL fallback provenance lost the forecast package version"
  )
  assert_true(
    isTRUE(all.equal(result$quantiles[5L, ], result$median, tolerance = 0)),
    "fallback q0.5 and median differ"
  )
})

check("successful period-seven STL records the STL branch without fallback", function() {
  result <- run_forecast_method(test_request("stlm_ar_forec"))
  assert_identical(result$executed_method_id, "stlm_ar_forec", "STL success changed identity")
  assert_identical(result$fallback_used, FALSE, "STL success recorded a fallback")
  assert_identical(result$provenance$settings$selected_branch, "stlm_ar", "STL branch missing")
})

check("JSON round trip preserves fixed STL fallback identity and reason", function() {
  request <- test_request("stlm_ar_forec")
  request$frequency <- 1L
  result <- run_forecast_method(request)
  decoded <- jsonlite::fromJSON(forecast_result_to_json(result), simplifyVector = TRUE)
  assert_identical(dim(decoded$quantiles), c(9L, 3L), "JSON changed quantile orientation")
  assert_true(isTRUE(all.equal(decoded$mean, result$mean, tolerance = 0)), "JSON changed means")
  assert_true(isTRUE(all.equal(decoded$quantiles, result$quantiles, tolerance = 0)), "JSON changed quantiles")
  assert_identical(decoded$requested_method_id, "stlm_ar_forec", "JSON lost requested method")
  assert_identical(decoded$executed_method_id, "auto_arima_forec", "JSON lost executed method")
  assert_identical(decoded$fallback_used, TRUE, "JSON lost fallback flag")
  assert_identical(decoded$fallback_reason, result$fallback_reason, "JSON lost fallback reason")
  assert_identical(
    decoded$provenance$settings$selected_branch,
    "auto_arima_d0_D0",
    "JSON lost fallback settings"
  )
})

check("FFORMA baseline rejects incompatible scientific overrides", function() {
  cases <- list(
    list(method = "auto_arima_forec", settings = list(stepwise = TRUE)),
    list(method = "ets_forec", settings = list(opt_crit = "lik")),
    list(method = "nnetar_forec", settings = list(repeats = 1L)),
    list(method = "tbats_forec", settings = list(use_parallel = TRUE)),
    list(method = "stlm_ar_forec", settings = list(d = 1L))
  )
  for (case in cases) {
    assert_error(
      function() run_forecast_method(test_request(case$method, case$settings)),
      "approved|separately approved",
      sprintf("%s accepted an incompatible baseline override", case$method)
    )
  }
})

check("invalid request fields fail before model fallback", function() {
  cases <- list(
    list(field = "context", value = numeric(), pattern = "context"),
    list(field = "context", value = c(1, Inf), pattern = "context"),
    list(field = "horizon", value = 0, pattern = "horizon"),
    list(field = "frequency", value = 1.5, pattern = "frequency"),
    list(field = "quantile_levels", value = c(0.2, 0.1), pattern = "quantile_levels"),
    list(field = "quantile_levels", value = c(0.1, 0.1), pattern = "quantile_levels"),
    list(field = "quantile_levels", value = c(0, 0.5), pattern = "quantile_levels")
  )
  for (case in cases) {
    request <- test_request()
    request[[case$field]] <- case$value
    assert_error(
      function() run_forecast_method(request),
      case$pattern,
      sprintf("invalid %s did not fail clearly", case$field)
    )
  }
  request <- test_request()
  request$actuals <- c(1, 2, 3)
  assert_error(
    function() run_forecast_method(request),
    "future actual observations",
    "request accepted actual observations"
  )
})

check("output validator rejects non-finite, crossing, and malformed arrays", function() {
  valid <- synthetic_result(test_request())
  non_finite <- valid
  non_finite$mean[[1L]] <- Inf
  assert_error(function() validate_forecast_result(non_finite), "non-finite", "non-finite output accepted")
  crossing <- valid
  crossing$quantiles[1L, 1L] <- crossing$quantiles[9L, 1L] + 1
  assert_error(function() validate_forecast_result(crossing), "crossing", "crossing output accepted")
  malformed <- valid
  malformed$median <- malformed$median[-1L]
  assert_error(function() validate_forecast_result(malformed), "lengths", "bad median shape accepted")
})

check("forecast library has no database writer or arbitrary expression execution", function() {
  source_lines <- readLines("src/r/util/forecast_methods.R", warn = FALSE)
  code_lines <- source_lines[!grepl("^\\s*#", source_lines)]
  code <- paste(code_lines, collapse = "\n")
  assert_true(!grepl("DBI::|duckdb::|dbConnect|dbExecute", code), "database-writing code introduced")
  assert_true(!grepl("\\b(get|eval|parse)\\s*\\(", code), "arbitrary expression primitive introduced")
  assert_true(!grepl("transform_registry|compose_transform|inverse_pipeline", code), "transformation engine introduced")
})

check("existing AutoARIMA linear-series output remains compatible", function() {
  request <- test_request("auto_arima_forec", test_settings("auto_arima_forec"))
  request$context <- as.numeric(seq_len(12L))
  request$frequency <- 1L
  expected <- c(13, 14, 15)
  result <- run_forecast_method(request)
  assert_true(isTRUE(all.equal(result$mean, expected)), "AutoARIMA means changed")
  assert_true(isTRUE(all.equal(result$median, expected)), "AutoARIMA medians changed")
  assert_true(all(result$quantiles == rep(expected, each = 9L)), "AutoARIMA quantiles changed")
})

check("period diagnostics enforce cycles and model support without a strength cutoff", function() {
  periodic <- rep(c(1, 3, 2, 4), 20L)
  diagnostic <- diagnose_seasonal_period(periodic, 1L, "ets", 3L)
  assert_true(diagnostic$estimated_period >= 1L, "findfrequency returned no period")
  if (diagnostic$estimated_period == 1L) {
    assert_identical(diagnostic$seasonal_strength, NULL, "period one fabricated strength")
  }
  assert_identical(model_supports_period("ets", 24L), TRUE, "ETS rejected period 24")
  assert_identical(model_supports_period("ets", 25L), FALSE, "ETS accepted period 25")
  assert_identical(model_supports_period("auto_arima", 350L), TRUE, "ARIMA rejected 350")
  assert_identical(model_supports_period("auto_arima", 351L), FALSE, "ARIMA accepted 351")
})

cat(sprintf("All forecast-method tests passed for %d registered methods.\n", length(APPROVED_METHODS)))
