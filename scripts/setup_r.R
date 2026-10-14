# Restore the locked R library or check a few imports without autoload/bootstrap.
arguments <- commandArgs(trailingOnly = TRUE)
if (length(arguments) != 1L || !arguments[[1L]] %in% c("restore", "verify")) {
  stop("setup_r.R requires exactly one operation: restore or verify", call. = FALSE)
}
root <- normalizePath(".", winslash = "/", mustWork = TRUE)
if (arguments[[1L]] == "restore") {
  options(
    repos = c(CRAN = "https://cloud.r-project.org"),
    timeout = max(as.integer(Sys.getenv("SHAPEFM_DOWNLOAD_TIMEOUT", "7200")), 60L),
    renv.config.cache.symlinks = FALSE
  )
  if (!requireNamespace("renv", quietly = TRUE)) {
    stop("renv is unavailable; restore the project renv bootstrap first", call. = FALSE)
  }
  renv::restore(project = root, lockfile = file.path(root, "renv.lock"), prompt = FALSE)
} else {
  # Never source renv/activate.R here: it can bootstrap a missing installation.
  minor <- paste(R.version$major, sub("\\..*$", "", R.version$minor), sep = ".")
  markers <- Sys.glob(file.path(
    root, "renv", "library", "*", paste0("R-", minor),
    R.version$platform, "renv", "DESCRIPTION"
  ))
  if (length(markers) != 1L) {
    stop("one matching project-local R library is required; run scripts/setup.sh", call. = FALSE)
  }
  .libPaths(c(dirname(dirname(markers[[1L]])), .Library))
}
for (package in c("renv", "jsonlite", "M4comp2018", "forecast", "tsfeatures", "dtw", "DBI", "duckdb")) {
  if (!requireNamespace(package, quietly = TRUE)) {
    stop("R import unavailable: ", package, call. = FALSE)
  }
}
cat("R imports ready\n")
