#!/usr/bin/env Rscript
# Process 04 point-only provider. M4comp2018 supplies archived vectors, never data.
# Input: jobs with dataset/series identity, zero-based official_position, horizon
# and forecast_ids. Output: standard mean-only records; no fitting or writes.

# Resolve approved Daily archived vectors by canonical source position and horizon.
# Inputs are request jobs and native package objects; outputs are original-scale
# mean-only records with submission provenance, never canonical observations.
forecast_m4_reference <- function(jobs, M4, submission_info, package_version) {
  approved <- list(
    m4_smyl = list(id = 118L, rank = 1L, author = "Smyl"),
    m4_fforma = list(id = 245L, rank = 2L, author = "Montero-Manso, et al.")
  )
  rows <- match(c(118L, 245L), submission_info$ID)
  if (anyNA(rows) || !identical(as.integer(submission_info[["Rank(OWA)"]][rows]), c(1L, 2L)) ||
      !identical(as.character(submission_info[["Author(s)"]][rows]),
                 c("Smyl", "Montero-Manso, et al."))) {
    stop("approved M4 submission identities unavailable", call. = FALSE)
  }
  daily <- which(vapply(M4, function(entry) identical(as.character(entry$period), "Daily"), logical(1L)))
  records <- list()
  for (job in jobs) {
    position <- job$official_position
    if (length(position) != 1L || !is.numeric(position) || !is.finite(position) ||
        position != floor(position) || position < 0 || position >= length(daily)) {
      stop("invalid official Daily position", call. = FALSE)
    }
    entry <- M4[[daily[[position + 1L]]]]
    if (!identical(entry$st, paste0("D", position + 1L))) {
      stop("official Daily identity does not match position", call. = FALSE)
    }
    ids <- unlist(job$forecast_ids, use.names = FALSE)
    if (!length(ids) || anyDuplicated(ids) || any(!ids %in% names(approved))) {
      stop("only Smyl 118 and FFORMA 245 are approved", call. = FALSE)
    }
    point <- entry$pt_ff
    if (!is.matrix(point) || ncol(point) != job$horizon || nrow(point) < max(rows)) {
      stop("archived forecast horizon or submission rows unavailable", call. = FALSE)
    }
    for (id in ids) {
      provider <- approved[[id]]
      mean <- as.numeric(point[rows[[match(id, names(approved))]], ])
      if (any(!is.finite(mean))) stop("non-finite archived mean", call. = FALSE)
      records[[length(records) + 1L]] <- list(
        status = "success", dataset_id = job$dataset_id, series_id = job$series_id,
        official_m4_series_id = entry$st, forecast_id = id,
        requested_provider = id, executed_provider = id,
        submission_id = provider$id, submission_rank = provider$rank,
        submission_author = provider$author, horizon = job$horizon,
        mean = mean, median = NULL, quantile_levels = NULL, quantiles = NULL,
        scale = "original", forecast_capability = "mean_only",
        point_semantics = "M4 competition point forecast",
        source_metadata = list(source_package = "M4comp2018",
                               source_package_version = package_version, scale = "original")
      )
    }
  }
  list(records = records)
}

if (sys.nframe() == 0L) {
  objects <- new.env(parent = emptyenv())
  utils::data("M4", package = "M4comp2018", envir = objects)
  utils::data("submission_info", package = "M4comp2018", envir = objects)
  payload <- jsonlite::fromJSON(file("stdin"), simplifyVector = FALSE)
  if (!identical(payload$action, "reference_forecast")) stop("unsupported provider action")
  result <- forecast_m4_reference(payload$jobs, objects$M4, objects$submission_info,
                                  as.character(utils::packageVersion("M4comp2018")))
  cat(jsonlite::toJSON(result, auto_unbox = TRUE, digits = 17, null = "null"))
}
