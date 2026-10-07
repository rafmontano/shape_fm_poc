#!/usr/bin/env Rscript
# ==============================================================================
# test_paper_table_worker.R
# Purpose: Verify the real stdin/stdout JSON worker, including horizon-one arrays.
# Inputs: Frozen kernel fixtures and existing jsonlite; no legacy runtime.
# Outputs: Assertions on stdout; no retained files or database writes.
# Run from: Rscript --vanilla src/r/tests/test_paper_table_worker.R
# ==============================================================================
# Bounded functional exception: subprocess contract assertions need no classes.
source("src/r/tests/test_paper_table_metrics.R")

# Invoke the real worker with one JSON request; collect exit status and stdout.
invoke <- function(request_json) {
  suppressWarnings(system2(file.path(R.home("bin"), "Rscript"),
    c("--vanilla", "src/r/06_02_evaluate_paper_tables.R"),
    input = request_json, stdout = TRUE, stderr = TRUE))
}

wire <- jsonlite::toJSON(request, auto_unbox = TRUE, null = "null", digits = 17)
output <- invoke(wire)
stopifnot(is.null(attr(output, "status")))
decoded <- jsonlite::fromJSON(paste(output, collapse = "\n"), simplifyVector = FALSE)
stopifnot(length(decoded$results) == length(request$jobs),
          is.character(decoded$worker$hostname), nzchar(decoded$worker$hostname),
          is.finite(decoded$runtime_seconds), decoded$runtime_seconds >= 0)
for (i in seq_along(results)) {
  observed <- decoded$results[[i]]
  expected <- results[[i]]
  stopifnot(observed$model == expected$model,
            identical(observed$lambda_up, expected$lambda_up),
            identical(observed$lambda_down, expected$lambda_down),
            observed$evaluation_count == expected$evaluation_count)
  near(unlist(observed$correct_counts), expected$correct_counts)
  near(unlist(observed$metrics), unlist(expected$metrics))
  if (!request$jobs[[i]]$retain_means) stopifnot(!"adjusted_means" %in% names(observed))
}
stopifnot(is.null(decoded$results[[3]]$metrics$smape),
          is.null(decoded$results[[3]]$metrics$mase), is.null(decoded$results[[3]]$metrics$owa))
cat("ok - real JSON boundary matches kernel and separates operational provenance\n")

one <- request
one$series <- one$series[1]
one$series[[1]]$actual <- I(12)
one$series[[1]]$means <- lapply(one$series[[1]]$means, function(x) I(x[1]))
one$series[[1]]$directions <- lapply(one$series[[1]]$directions, function(x) I(x[1]))
one$jobs <- list(job("m4_smyl", TRUE), job("directional_dtw", TRUE))
output <- invoke(jsonlite::toJSON(one, auto_unbox = TRUE, null = "null"))
stopifnot(is.null(attr(output, "status")))
decoded <- jsonlite::fromJSON(paste(output, collapse = "\n"), simplifyVector = FALSE)
stopifnot(is.list(decoded$results[[1]]$correct_counts),
          length(decoded$results[[1]]$correct_counts) == 1L,
          is.list(decoded$results[[1]]$adjusted_means[[1]]$mean),
          length(decoded$results[[1]]$adjusted_means[[1]]$mean) == 1L,
          is.list(decoded$results[[2]]$adjusted_means),
          length(decoded$results[[2]]$adjusted_means) == 0L)
cat("ok - horizon-one arrays, null lambda fields and direct empty retention\n")

for (bad in c(sub('"actual":\\[12,8\\]', '"actual":[12,null]', wire),
              sub('"actual":\\[12,8\\]', '"actual":[12,true]', wire),
              sub('"actual":\\[12,8\\]', '"actual":[12,"8"]', wire),
              sub('"model":"naive2"', '"model":"missing"', wire))) {
  stopifnot(!is.null(attr(invoke(bad), "status")))
}
cat("ok - invalid numeric JSON elements and unknown model exit nonzero\n")
cat("All 3 JSON worker test groups passed.\n")
