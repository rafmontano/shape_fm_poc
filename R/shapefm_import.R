# Friendly process boundary between R workflows and the locked Stage 1 importer.

shapefm_project_root <- function() {
  root <- normalizePath(getwd(), mustWork = TRUE)
  if (!file.exists(file.path(root, "pyproject.toml"))) {
    stop("Run ShapeFM workflows from the repository root (or open shape_fm_poc.Rproj).")
  }
  root
}

shapefm_source_root <- function(must_exist = TRUE) {
  root <- shapefm_project_root()
  value <- Sys.getenv("SHAPEFM_GIFT_EVAL_ROOT", unset = "")
  path <- normalizePath(
    if (nzchar(value)) value else file.path(root, "data", "source", "gift_eval"),
    mustWork = FALSE
  )
  if (must_exist && !dir.exists(path)) stop("GIFT-Eval source is missing: ", path)
  path
}

shapefm_uv_environment <- function(root) {
  c(
    paste0("UV_PYTHON_INSTALL_DIR=", shQuote(file.path(root, ".tools", "python"))),
    paste0("UV_CACHE_DIR=", shQuote(file.path(root, ".tools", "cache"))),
    "UV_PYTHON_PREFERENCE=only-managed"
  )
}

shapefm_run_cli <- function(args) {
  root <- shapefm_project_root()
  uv <- file.path(root, ".tools", "uv", "uv")
  if (!file.exists(uv)) stop("Repository-local uv is missing. Follow docs/environment.md first.")
  status <- system2(
    uv,
    args = c("run", "--locked", "shapefm-import", args),
    stdout = "",
    stderr = "",
    env = shapefm_uv_environment(root)
  )
  if (!identical(status, 0L)) stop("ShapeFM command failed with exit status ", status)
  invisible(status)
}

shapefm_migrate <- function(database = NULL) {
  args <- "migrate"
  if (!is.null(database)) args <- c(args, "--database", shQuote(database))
  shapefm_run_cli(args)
}

shapefm_import_m4_daily <- function(max_series = NULL, workers = 1L, database = NULL) {
  source_dir <- file.path(shapefm_source_root(TRUE), "m4_daily")
  if (!dir.exists(source_dir)) stop("M4 Daily source directory is missing: ", source_dir)
  args <- c(
    "import", "--source-dir", shQuote(source_dir),
    "--workers", as.character(workers)
  )
  if (!is.null(max_series)) args <- c(args, "--max-series", as.character(max_series))
  if (!is.null(database)) args <- c(args, "--database", shQuote(database))
  shapefm_run_cli(args)
}
