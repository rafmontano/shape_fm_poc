# ==============================================================================
# setup_r.R
#
# Purpose: Restore or read-only verify the complete locked ShapeFM R package superset.
# Inputs: Operation `restore` or `verify`, renv.lock, and the project-local renv library.
# Outputs: Restored packages or a concise version summary; verification changes no files.
# Run from: Invoked by scripts/setup.sh; not run directly by researchers.
# ==============================================================================

# Approved package contract ---------------------------------------------------

required_packages <- c(
  "M4comp2018", "forecast", "tsfeatures", "dtw", "xgboost",
  "rBayesianOptimization", "caret", "RANN", "factoextra", "scmamp",
  "future", "future.apply", "foreach", "doParallel", "reticulate",
  "cachem", "memoise", "dplyr", "tidyr", "purrr", "readr", "tibble",
  "stringr", "ggplot2", "ggpubr", "ragg", "svglite", "scales", "DBI",
  "duckdb", "jsonlite"
)

arguments <- commandArgs(trailingOnly = TRUE)
if (length(arguments) != 1L || !arguments[[1L]] %in% c("restore", "verify")) {
  stop("setup_r.R requires exactly one operation: restore or verify", call. = FALSE)
}
operation <- arguments[[1L]]
root <- normalizePath(".", winslash = "/", mustWork = TRUE)
lockfile <- file.path(root, "renv.lock")

# Verification library selection --------------------------------------------

if (operation == "verify") {
  # Verification cannot source the renv autoloader because a missing renv could
  # bootstrap itself. Locate only the library matching this R minor version and
  # platform; an archive or another machine's library must not satisfy checks.
  library_root <- file.path(root, "renv", "library")
  r_minor_version <- paste(
    R.version$major,
    sub("\\..*$", "", R.version$minor),
    sep = "."
  )
  package_markers <- Sys.glob(file.path(
    library_root,
    "*",
    paste0("R-", r_minor_version),
    R.version$platform,
    "renv",
    "DESCRIPTION"
  ))
  if (length(package_markers) != 1L) {
    stop(
      "one matching project-local R library is required; run scripts/setup.sh",
      call. = FALSE
    )
  }
  project_library <- dirname(dirname(package_markers[[1L]]))
  .libPaths(c(project_library, .libPaths()))
}

# Lockfile validation ---------------------------------------------------------

if (!file.exists(lockfile)) {
  stop("renv.lock is missing", call. = FALSE)
}

# Restore ---------------------------------------------------------------------

if (operation == "restore") {
  download_timeout <- as.integer(Sys.getenv("SHAPEFM_DOWNLOAD_TIMEOUT", "7200"))
  options(
    repos = c(CRAN = "https://cloud.r-project.org"),
    timeout = max(download_timeout, getOption("timeout", 60L)),
    renv.config.cache.symlinks = FALSE
  )
  if (!requireNamespace("renv", quietly = TRUE)) {
    stop("renv 1.2.4 is unavailable; install it before rerunning setup", call. = FALSE)
  }
  renv::restore(project = root, lockfile = lockfile, prompt = FALSE)
}

# Lock and installed-package verification ------------------------------------

# jsonlite is itself restored from the lock. Deferring lock parsing until this
# point lets a genuinely fresh project restore without a global jsonlite copy.
if (!requireNamespace("jsonlite", quietly = TRUE)) {
  stop("project R library is missing jsonlite", call. = FALSE)
}
lock <- jsonlite::fromJSON(lockfile, simplifyVector = FALSE)
locked_packages <- names(lock$Packages)
missing_from_lock <- setdiff(required_packages, locked_packages)
if (length(missing_from_lock) > 0L) {
  stop(
    "renv.lock is missing required packages: ",
    paste(missing_from_lock, collapse = ", "),
    call. = FALSE
  )
}

# Emit tool versions even when a later package check fails, so verification
# reports retain enough information to diagnose an incomplete installation.
cat(
  paste0(
    "R=", getRversion(),
    ", renv=", utils::packageVersion("renv")
  ),
  "\n"
)

missing_installed <- required_packages[
  !vapply(required_packages, requireNamespace, logical(1), quietly = TRUE)
]
if (length(missing_installed) > 0L) {
  stop(
    "project R library is missing: ",
    paste(missing_installed, collapse = ", "),
    call. = FALSE
  )
}

version_mismatches <- vapply(required_packages, function(package) {
  installed <- as.character(utils::packageVersion(package))
  expected <- as.character(lock$Packages[[package]]$Version)
  if (utils::compareVersion(installed, expected) == 0L) {
    ""
  } else {
    paste0(package, "=", installed, " expected ", expected)
  }
}, character(1))
version_mismatches <- version_mismatches[nzchar(version_mismatches)]
if (length(version_mismatches) > 0L) {
  stop("R package version mismatch: ", paste(version_mismatches, collapse = "; "), call. = FALSE)
}

data("M4", package = "M4comp2018", envir = environment())
data("submission_info", package = "M4comp2018", envir = environment())
if (!exists("M4", inherits = FALSE) || !is.list(M4) || length(M4) == 0L) {
  stop("M4comp2018 did not provide the expected nonempty M4 object", call. = FALSE)
}
if (!exists("submission_info", inherits = FALSE)) {
  stop("M4comp2018 did not provide submission_info", call. = FALSE)
}

cat(
  paste0(
    "R=", getRversion(),
    ", renv=", utils::packageVersion("renv"),
    ", M4comp2018=", utils::packageVersion("M4comp2018"),
    ", packages=", length(required_packages)
  ),
  "\n"
)
