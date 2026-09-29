#!/usr/bin/env Rscript
# ==============================================================================
# 01_02_import_m4comp2018.R
#
# Purpose: Read and validate the two approved M4 Daily competition forecasts.
# Inputs: JSON on stdin with action="read", dataset identity, approved forecast
#   IDs, expected Daily frequency/horizon, and selected canonical series entries.
#   source_position is zero-based within the official M4 Daily subset.
# Outputs: JSON records for m4_smyl and m4_fforma; this adapter writes no files
#   or databases. Run with: Rscript src/r/01_02_import_m4comp2018.R < request.json
# ==============================================================================

# Code constant: the only submissions approved for import, in output order.
M4_APPROVED_SUBMISSIONS <- data.frame(
  forecast_id = c("m4_smyl", "m4_fforma"),
  submission_id = c(118L, 245L),
  rank = c(1L, 2L),
  author = c("Smyl", "Montero-Manso, et al."),
  stringsAsFactors = FALSE
)

# Code constants: immutable package release and source revision used for provenance.
.M4_SOURCE_PACKAGE <- "M4comp2018"
.M4_SOURCE_REVISION <- "3c75dcd25c72c631f04bff1a017d9917d0e7251c"

# Purpose: Compare source numbers after the same precision loss as an Arrow
# float32 target. Inputs: two finite numeric vectors. Output: one logical.
.m4_float32_equal <- function(observed, expected) {
  if (length(observed) != length(expected)) return(FALSE)
  if (!is.numeric(observed) || !is.numeric(expected) ||
      any(!is.finite(observed)) || any(!is.finite(expected))) return(FALSE)
  tolerance <- pmax(1e-6, abs(expected) * 2^-23)
  all(abs(observed - expected) <= tolerance)
}

# Purpose: Extract a required scalar integer without permissive coercion.
.m4_integer <- function(value, name, minimum = 0L) {
  if (!is.numeric(value) || length(value) != 1L || !is.finite(value) ||
      value != floor(value) || value < minimum) {
    stop(name, " must be a scalar integer >= ", minimum, call. = FALSE)
  }
  as.integer(value)
}

# Purpose: Validate stable package metadata before interpreting pt_ff row order.
.m4_validate_submission_info <- function(submission_info) {
  if (!is.data.frame(submission_info)) stop("submission_info must be a data.frame", call. = FALSE)
  id_column <- match("ID", names(submission_info))
  rank_column <- match("Rank(OWA)", names(submission_info))
  author_column <- match("Author(s)", names(submission_info))
  if (is.na(id_column) || is.na(rank_column) || is.na(author_column)) {
    stop("submission_info must contain stable ID, Author(s), and Rank(OWA) columns", call. = FALSE)
  }
  ids <- suppressWarnings(as.integer(as.character(submission_info[[id_column]])))
  ranks <- suppressWarnings(as.integer(as.character(submission_info[[rank_column]])))
  for (row in seq_len(nrow(M4_APPROVED_SUBMISSIONS))) {
    expected <- M4_APPROVED_SUBMISSIONS[row, ]
    matches <- which(ids == expected$submission_id)
    if (length(matches) != 1L || ranks[matches] != expected$rank) {
      stop("submission_info does not match approved ID/rank metadata", call. = FALSE)
    }
    if (matches != expected$rank) {
      stop("submission_info rank order does not match pt_ff row order", call. = FALSE)
    }
    if (!identical(as.character(submission_info[[author_column]][matches]), expected$author)) {
      stop("submission_info does not match approved submission author", call. = FALSE)
    }
  }
  invisible(TRUE)
}

# Purpose: Validate one requested ShapeFM series against its official M4 entry.
.m4_validate_series <- function(request, official) {
  required <- c(
    "series_id", "source_position", "official_m4_series_id", "history", "future"
  )
  missing <- setdiff(required, names(request))
  if (length(missing)) stop("series request is missing: ", paste(missing, collapse = ", "), call. = FALSE)
  position <- .m4_integer(request$source_position, "source_position")
  series_id <- as.character(request$series_id)
  if (length(series_id) != 1L || is.na(series_id) || !nzchar(series_id)) {
    stop("series_id must be one nonempty identifier", call. = FALSE)
  }
  if (!identical(series_id, as.character(position))) {
    stop("series_id does not match zero-based Daily source_position", call. = FALSE)
  }
  official_id <- as.character(request$official_m4_series_id)
  if (length(official_id) != 1L || is.na(official_id) || !nzchar(official_id) ||
      !identical(official_id, as.character(official$st))) {
    stop("official_id does not match M4 source_position", call. = FALSE)
  }
  if (!identical(as.character(official$period), "Daily")) stop("M4 source_position is not Daily", call. = FALSE)
  horizon <- .m4_integer(request$horizon, "horizon", 1L)
  if (horizon != as.integer(official$h) || horizon != length(official$xx)) {
    stop("horizon does not match official M4 series", call. = FALSE)
  }
  if (!.m4_float32_equal(unlist(request$history, use.names = FALSE), as.numeric(official$x))) {
    stop("history does not match official M4 series at float32 precision", call. = FALSE)
  }
  if (!.m4_float32_equal(unlist(request$future, use.names = FALSE), as.numeric(official$xx))) {
    stop("future does not match official M4 series at float32 precision", call. = FALSE)
  }
  point <- official$pt_ff
  if (!is.matrix(point) || nrow(point) < 2L || ncol(point) != horizon || any(!is.finite(point[1:2, ]))) {
    stop("pt_ff must contain finite approved forecasts with the official horizon", call. = FALSE)
  }
  list(
    series_id = series_id, position = position,
    official_m4_series_id = official_id, point = point
  )
}

# Purpose: Create a stable content identity for one imported point forecast.
# Inputs: Record identity and finite mean vector.
# Outputs: Lowercase SHA-256 over R's version-2 canonical serialization.
.m4_content_hash <- function(record) {
  if (!requireNamespace("digest", quietly = TRUE)) {
    stop("R package 'digest' is required", call. = FALSE)
  }
  digest::digest(record, algo = "sha256", serialize = TRUE, serializeVersion = 2L)
}

# Purpose: Execute the bounded adapter with injectable package fixtures.
# Inputs: parsed request, complete M4 list, and submission_info data.frame.
# Outputs: mean-only records; does not load packages or perform any writes.
import_m4comp2018 <- function(
  payload, M4, submission_info, package_version = "0.2.0",
  source_revision = .M4_SOURCE_REVISION
) {
  if (!is.list(payload) || !identical(payload$action, "read")) stop("unsupported adapter action", call. = FALSE)
  dataset_id <- as.character(payload$dataset_id)
  if (length(dataset_id) != 1L || is.na(dataset_id) || !nzchar(dataset_id)) {
    stop("dataset_id must be one nonempty identifier", call. = FALSE)
  }
  if (!identical(payload$dataset_name, "m4_daily")) stop("only dataset m4_daily is supported", call. = FALSE)
  if (!identical(payload$expected_frequency, "D")) stop("expected_frequency must be D", call. = FALSE)
  expected_horizon <- .m4_integer(payload$expected_horizon, "expected_horizon", 1L)
  forecast_ids <- as.character(unlist(payload$forecast_ids, use.names = FALSE))
  if (!length(forecast_ids) || anyDuplicated(forecast_ids) ||
      any(!forecast_ids %in% M4_APPROVED_SUBMISSIONS$forecast_id)) {
    stop("forecast_ids must select unique approved M4 providers", call. = FALSE)
  }
  if (!is.list(payload$series)) stop("series must be a JSON array", call. = FALSE)
  if (!is.list(M4) || !length(M4)) stop("M4 must be a nonempty list", call. = FALSE)
  .m4_validate_submission_info(submission_info)
  daily_positions <- which(vapply(
    M4, function(value) identical(as.character(value$period), "Daily"), logical(1L)
  ))
  if (!length(daily_positions)) stop("M4 contains no Daily series", call. = FALSE)
  records <- list()
  seen <- integer()
  for (request in payload$series) {
    position <- .m4_integer(request$source_position, "source_position")
    if (position >= length(daily_positions)) stop("source_position is outside M4 Daily", call. = FALSE)
    if (position %in% seen) stop("source_position must be unique", call. = FALSE)
    seen <- c(seen, position)
    if (.m4_integer(request$horizon, "horizon", 1L) != expected_horizon) {
      stop("series horizon does not match expected_horizon", call. = FALSE)
    }
    checked <- .m4_validate_series(request, M4[[daily_positions[[position + 1L]]]])
    for (forecast_id in forecast_ids) {
      approved <- M4_APPROVED_SUBMISSIONS[
        M4_APPROVED_SUBMISSIONS$forecast_id == forecast_id, , drop = FALSE
      ]
      identity <- list(
        dataset_id = dataset_id,
        series_id = checked$series_id,
        official_m4_series_id = checked$official_m4_series_id,
        forecast_id = approved$forecast_id,
        submission_id = approved$submission_id,
        submission_rank = approved$rank,
        submission_author = approved$author,
        horizon = expected_horizon,
        mean = as.numeric(checked$point[approved$rank, ])
      )
      records[[length(records) + 1L]] <- c(identity, list(
        point_semantics = "M4 competition point forecast",
        forecast_capability = "mean_only",
        source_package = .M4_SOURCE_PACKAGE,
        source_package_version = as.character(package_version),
        source_revision = as.character(source_revision),
        content_hash = .m4_content_hash(identity)
      ))
    }
  }
  list(records = records)
}

# CLI boundary: package datasets are loaded exactly once per invocation and are
# injected into the core; neither package object is fetched in the series loop.
if (sys.nframe() == 0L) {
  if (!requireNamespace("jsonlite", quietly = TRUE)) stop("jsonlite is required", call. = FALSE)
  if (!requireNamespace("M4comp2018", quietly = TRUE)) stop("M4comp2018 is required", call. = FALSE)
  package_objects <- new.env(parent = emptyenv())
  utils::data("M4", package = "M4comp2018", envir = package_objects)
  utils::data("submission_info", package = "M4comp2018", envir = package_objects)
  if (!exists("M4", package_objects, inherits = FALSE) ||
      !exists("submission_info", package_objects, inherits = FALSE)) {
    stop("M4comp2018 package datasets are unavailable", call. = FALSE)
  }
  payload <- jsonlite::fromJSON(file("stdin"), simplifyVector = FALSE)
  result <- import_m4comp2018(
    payload,
    package_objects$M4,
    package_objects$submission_info,
    package_version = as.character(utils::packageVersion("M4comp2018"))
  )
  cat(jsonlite::toJSON(result, auto_unbox = TRUE, digits = 17, null = "null"))
}
