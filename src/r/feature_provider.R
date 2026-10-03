#!/usr/bin/env Rscript
# ==============================================================================
# feature_provider.R
#
# Purpose: Serve the JSON-stdin boundary for the fixed FFORMA 42-feature provider.
# Inputs: action="describe", or action="extract" with ordered jobs containing id,
#   finite prepared context values and centrally resolved positive seasonality.
# Outputs: JSON description or ordered per-job success/failure results on stdout.
# Run from: printf '%s' '{"action":"describe"}' | Rscript --vanilla src/r/feature_provider.R
# ==============================================================================

source("src/r/util/time_series_input.R")
source("src/r/util/features.R")

# Purpose: Report stable scientific contract and runtime dependency identity.
# Inputs: None.
# Outputs: JSON-ready feature provider description; writes nothing.
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

# Purpose: Extract one job while converting scientific/input errors to job failure.
# Inputs: Job with id, finite context array, and centrally resolved seasonality.
# Outputs: JSON-ready success with ordered names/values, or failure with error text.
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

# Purpose: Validate and dispatch one top-level worker request.
# Inputs: Decoded JSON list with action and, for extraction, a jobs array.
# Outputs: JSON-ready response; malformed top-level protocol raises a fatal error.
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

# Purpose: Read one stdin request and emit its response with protocol precision.
# Inputs: JSON from stdin.
# Outputs: Exactly one JSON document on stdout; fatal protocol errors terminate.
feature_provider_main <- function() {
  if (!requireNamespace("jsonlite", quietly = TRUE)) stop("jsonlite is required", call. = FALSE)
  payload <- jsonlite::fromJSON(file("stdin"), simplifyVector = FALSE)
  response <- dispatch_feature_request(payload)
  cat(jsonlite::toJSON(response, auto_unbox = TRUE, digits = 17, null = "null"))
}

if (sys.nframe() == 0L) feature_provider_main()
