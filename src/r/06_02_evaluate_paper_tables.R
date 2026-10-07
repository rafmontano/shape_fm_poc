#!/usr/bin/env Rscript
# ==============================================================================
# 06_02_evaluate_paper_tables.R
# Purpose: Internal JSON boundary for bounded historical Process 06 calculations.
# Inputs: One request JSON on stdin; repository utilities and existing jsonlite.
# Outputs: One response JSON on stdout; errors exit nonzero; no storage or fitting.
# Run from: Rscript --vanilla src/r/06_02_evaluate_paper_tables.R < request.json
# ==============================================================================
source("src/r/util/forecast_adjustments.R")
source("src/r/util/paper_table_metrics.R")

# Decode without data-frame simplification; only numeric arrays become vectors.
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
