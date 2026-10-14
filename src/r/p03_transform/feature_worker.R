#!/usr/bin/env Rscript
# Process 03 FFORMA provider: one JSON request on stdin -> one JSON response.
# describe returns schema/dependency identity; extract accepts ordered jobs with
# id, finite prepared context and resolved seasonality and returns per-job results.
# Formulas, schema and approved fallbacks live in features.R; no storage.

source("src/r/util/time_series_input.R")
source("src/r/p03_transform/features.R")

# Return the fixed feature contract and native dependency identity.
describe_feature_provider <- function() {
  list(
    feature_set_id = "fforma_base_v1",
    feature_set_version = 1L,
    feature_names = FEATURE_SCHEMA,
    scale_policy = "tsfeatures scale=TRUE independently for each series",
    missing_policy = paste(
      "Only absent nonseasonal seas_acf1, seas_pacf, seasonal_strength, peak,",
      "and trough are inserted as zero; final NA values become zero."
    ),
    fallback_policy = paste(
      "Heterogeneity errors become four zeros; entropy constant/short/errors become",
      "zero; Holt-Winters AAA errors become NA then zero; schema errors fail the job."
    ),
    directional_features_enabled = FALSE,
    disabled_directional_feature_names = DISABLED_DIRECTIONAL_FEATURES,
    directional_features_disabled_reason = DIRECTIONAL_FEATURES_DISABLED_REASON,
    dependencies = list(
      R = R.version.string,
      platform = R.version$platform,
      tsfeatures = as.character(utils::packageVersion("tsfeatures")),
      forecast = as.character(utils::packageVersion("forecast")),
      jsonlite = as.character(utils::packageVersion("jsonlite")),
      tibble = as.character(utils::packageVersion("tibble"))
    )
  )
}

# Job (id, context, seasonality) -> ordered feature values or explicit job failure.
extract_feature_job <- function(job) {
  id <- job$id
  tryCatch({
    if (is.null(id) || length(id) != 1L) stop("job id must be one value", call. = FALSE)
    input <- time_series_input(job)
    if (any(!is.finite(input$values))) {
      stop("context must contain only finite prepared values", call. = FALSE)
    }
    values <- calculate_fforma_features(input$series)
    list(
      id = id,
      status = "success",
      feature_names = FEATURE_SCHEMA,
      feature_values = unname(values)
    )
  }, error = function(error) {
    list(id = id, status = "failed", error = conditionMessage(error))
  })
}

# Decoded request -> description or extraction results; malformed protocol fails.
dispatch_feature_request <- function(payload) {
  if (!is.list(payload) || is.null(payload$action) || length(payload$action) != 1L) {
    stop("top-level action is required", call. = FALSE)
  }
  if (identical(payload$action, "describe")) return(describe_feature_provider())
  if (!identical(payload$action, "extract")) stop("unsupported feature action", call. = FALSE)
  if (is.null(payload$jobs) || !is.list(payload$jobs)) {
    stop("extract jobs must be a JSON array", call. = FALSE)
  }
  list(
    provider = describe_feature_provider(),
    results = lapply(payload$jobs, extract_feature_job)
  )
}

# Emit one response with binary64 round-trip precision; protocol errors terminate.
feature_provider_main <- function() {
  if (!requireNamespace("jsonlite", quietly = TRUE)) stop("jsonlite is required", call. = FALSE)
  payload <- jsonlite::fromJSON(file("stdin"), simplifyVector = FALSE)
  response <- dispatch_feature_request(payload)
  cat(jsonlite::toJSON(response, auto_unbox = TRUE, digits = 17, null = "null"))
}

if (sys.nframe() == 0L) feature_provider_main()
