#!/usr/bin/env Rscript
# ==============================================================================
# test_directional_result_plots.R
# Purpose: Verify locked selective renderer settings, actual devices and rejection.
# Inputs: Temporary explicit synthetic matrices; existing renv-locked packages.
# Outputs: Assertions; all temporary rendering files removed.
# Run from: Rscript --vanilla src/r/tests/test_directional_result_plots.R
# ==============================================================================
# Bounded functional exception: subprocess/graphics assertions need no class.

# Invoke the real worker; only its structured runtime response is accepted.
invoke <- function(request) {
  suppressWarnings(system2(file.path(R.home("bin"), "Rscript"),
    c("--vanilla", "src/r/06_03_plot_directional_results.R"),
    input = jsonlite::toJSON(request, auto_unbox = TRUE, null = "null", digits = 17),
    stdout = TRUE, stderr = TRUE))
}

# Build ordered complete rows with both distinct scores and ties.
matrix_input <- function(models) {
  list(models = I(models), blocks = I(paste0("Daily_", 1:14)),
       frequencies = I(rep("Daily", 14)), horizons = I(1:14),
       values = lapply(1:14, function(h) I(c(.7, .6, .5, .7, .8)[seq_along(models)])))
}

# Snapshot root output files so checks never remove or overwrite unrelated work.
root_outputs <- function() {
  files <- sort(list.files(".", pattern = "\\.(csv|pdf|png)$", ignore.case = TRUE))
  tools::md5sum(files)
}

# Exercise owned PDF/PNG devices with successful and failing draw callbacks.
check_devices <- function(directory, settings) {
  renderer <- new.env(parent = globalenv())
  expressions <- parse("src/r/06_03_plot_directional_results.R")
  for (expression in expressions) {
    if (is.call(expression) && identical(expression[[1]], as.name("<-")) &&
        identical(expression[[2]], as.name("request"))) break
    eval(expression, renderer)
  }
  before <- grDevices::dev.list()
  for (format in c("pdf", "png")) {
    for (fail in c(FALSE, TRUE)) {
      path <- file.path(directory, paste0("device-", format, "-", fail, ".", format))
      result <- tryCatch(renderer$render_device(path, format, function() {
        stopifnot(grDevices::dev.cur() != 1L,
                  !grDevices::dev.cur() %in% before)
        graphics::plot(1:3)
        if (fail) stop("deliberate draw failure", call. = FALSE)
      }, 8, 4.8, settings), error = identity)
      stopifnot(identical(grDevices::dev.list(), before), file.exists(path))
      if (fail) stopifnot(inherits(result, "error"),
                          conditionMessage(result) == "deliberate draw failure")
      else stopifnot(!inherits(result, "error"))
      unlink(path)
    }
  }
  cat("ok - explicit PDF/PNG devices close on success and draw errors\n")
}

# Test actual PDF/PNG rendering and failure before rendering with invalid boundaries.
run_checks <- function() {
  root_before <- root_outputs()
  on.exit(stopifnot(identical(root_outputs(), root_before)), add = TRUE)
  directory <- tempfile("directional-plots-")
  dir.create(directory)
  on.exit(unlink(directory, recursive = TRUE), add = TRUE)
  lock <- jsonlite::fromJSON("renv.lock", simplifyVector = FALSE)$Packages
  packages <- lapply(lock[c("scmamp", "jsonlite")], function(p) p[intersect(names(p), c("Version", "RemoteSha"))])
  focused <- c("directional_mantis_rf", "chronos_2", "directional_dtw", "m4_smyl")
  applicable <- c("directional_mantis_rf", "directional_dtw", "m4_fforma", "chronos_2", "m4_smyl")
  settings <- list(alpha = .05, reverse = TRUE, cex = .75, useDingbats = FALSE)
  request <- list(output = directory, packages = packages, settings = settings,
                  horizon = matrix_input(focused), cd = matrix_input(applicable))
  response <- invoke(request)
  stopifnot(is.null(attr(response, "status")))
  response <- jsonlite::fromJSON(paste(response, collapse = "\n"), simplifyVector = FALSE)
  stopifnot(identical(response$cd_settings, settings),
            response$packages$scmamp$version == lock$scmamp$Version,
            response$packages$scmamp$revision == lock$scmamp$RemoteSha)
  files <- file.path(directory, c("figure_2_accuracy_by_horizon.pdf", "figure_2_accuracy_by_horizon.png",
                                 "figure_2_cd_daily.pdf", "figure_2_cd_daily.png",
                                 "figure_2_cd_daily_paper.pdf", "figure_2_cd_daily_paper.png"))
  hashes <- unname(tools::md5sum(files))
  stopifnot(all(file.exists(files)), all(file.info(files)$size > 1000),
            setequal(list.files(directory), basename(files)),
            identical(hashes[3:4], hashes[5:6]))
  cat("ok - six files, locked settings and byte-identical standalone CD versions\n")
  for (mutation in c("version", "revision", "alpha", "reverse", "cex", "dingbats", "model", "missing", "null",
                     "cd_missing", "cd_null", "legacy_panels", "output")) {
    invalid <- request
    invalid$output <- file.path(directory, mutation)
    dir.create(invalid$output)
    if (mutation == "version") invalid$packages$scmamp$Version <- "0.0.0"
    if (mutation == "revision") invalid$packages$scmamp$RemoteSha <- "wrong"
    if (mutation == "alpha") invalid$settings$alpha <- .1
    if (mutation == "reverse") invalid$settings$reverse <- FALSE
    if (mutation == "cex") invalid$settings$cex <- 1
    if (mutation == "dingbats") invalid$settings$useDingbats <- TRUE
    if (mutation == "model") invalid$horizon$models[1] <- "xgboost"
    if (mutation == "missing") invalid$horizon$values <- invalid$horizon$values[-1]
    if (mutation == "null") invalid$horizon$values[[1]] <- list(NULL, .6, .5, .7)
    if (mutation == "cd_missing") invalid$cd$values <- invalid$cd$values[-1]
    if (mutation == "cd_null") invalid$cd$values[[1]] <- list(NULL, .6, .5, .7, .8)
    if (mutation == "legacy_panels") invalid$cd <- list(focused = matrix_input(focused),
                                                       applicable = matrix_input(applicable))
    if (mutation == "output") unlink(invalid$output, recursive = TRUE)
    stopifnot(!is.null(attr(invoke(invalid), "status")), !length(list.files(invalid$output)))
  }
  cat("ok - locked revision, scientific settings, excluded/missing model and invalid values reject\n")
  check_devices(directory, settings)
  code <- readLines("src/r/06_03_plot_directional_results.R")
  stopifnot(any(grepl("scmamp::plotCD", code, fixed = TRUE)),
    !any(grepl("readRDS|saveRDS|source\\(|duckdb|evaluate_paper_tables|calculate_smyl_oracle|auto.arima|future::", code)))
  cat("ok - selective renderer isolation: no legacy readers, evaluation or model execution\n")
  stopifnot(identical(root_outputs(), root_before))
  cat("ok - no repository-root CSV/PDF/PNG created or changed\n")
}

run_checks()
cat("All 5 directional result plot test groups passed.\n")
