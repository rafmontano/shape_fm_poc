#!/usr/bin/env Rscript
# Process 06 bounded paper-table worker under docs/evaluation.md.
# One stdin JSON cohort/candidate request -> stdout aggregate metrics, directional
# counts, optional paths and operational provenance; errors exit nonzero.
# The coordinator accepts candidate keys; this worker has no storage or fitting.
source("src/r/p06_evaluate/adjustments.R")
source("src/r/p06_evaluate/metrics.R")

# Decode cohort JSON into the paper kernel's request list without data frames;
# only numeric arrays become vectors, preserving identities and candidate settings.
decode_paper_request <- function(input) {
  request <- jsonlite::fromJSON(input, simplifyVector = FALSE)
  for (i in seq_along(request$series)) {
    s <- request$series[[i]]
    for (field in c("context", "actual")) s[[field]] <- .decode_numeric_array(s[[field]])
    for (field in c("means", "directions")) {
      s[[field]] <- lapply(s[[field]], .decode_numeric_array)
    }
    request$series[[i]] <- s
  }
  request
}

# Preserve type/null failures instead of coercing strings, booleans or null elements.
.decode_numeric_array <- function(x) {
  if (!is.list(x) || !length(x) ||
      !all(vapply(x, function(v) is.numeric(v) && length(v) == 1L, logical(1)))) {
    stop("expected numeric JSON array", call. = FALSE)
  }
  unlist(x, use.names = FALSE)
}

started <- proc.time()[["elapsed"]]
request <- decode_paper_request(paste(readLines(file("stdin"), warn = FALSE), collapse = "\n"))
response <- evaluate_paper_tables(request)
# Explicit arrays retain horizon-one vector shape under auto_unbox.
for (i in seq_along(response$results)) {
  response$results[[i]]$correct_counts <- I(response$results[[i]]$correct_counts)
  for (j in seq_along(response$results[[i]]$adjusted_means)) {
    response$results[[i]]$adjusted_means[[j]]$mean <- I(response$results[[i]]$adjusted_means[[j]]$mean)
  }
}
response$worker <- list(hostname = unname(Sys.info()[["nodename"]]))
response$runtime_seconds <- proc.time()[["elapsed"]] - started
cat(jsonlite::toJSON(response, auto_unbox = TRUE, null = "null", digits = 17), "\n")
