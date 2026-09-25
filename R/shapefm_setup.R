# Reusable Gate 00 setup and verification operations.

source("R/shapefm_import.R")

shapefm_system <- function(command, args, env = character()) {
  output <- system2(command, args = args, stdout = TRUE, stderr = TRUE, env = env)
  status <- attr(output, "status")
  if (is.null(status)) status <- 0L
  if (!identical(status, 0L)) {
    stop(paste(c("Command failed:", command, args, output), collapse = " "))
  }
  output
}

shapefm_verify_gift_eval <- function() {
  root <- shapefm_project_root()
  gift_python <- file.path(root, "environments", "gift-eval", ".venv", "bin", "python")
  package_version <- shapefm_system(
    gift_python,
    c("-c", shQuote("import importlib.metadata; print(importlib.metadata.version('salesforce-gift-eval'))"))
  )[[1L]]

  source_root <- shapefm_source_root(TRUE)
  uv <- file.path(root, ".tools", "uv", "uv")
  verification <- shapefm_system(
    uv,
    c(
      "run", "--locked", "python", "-m", "shapefm.acquire_gift_eval", "verify",
      "--source-root", shQuote(source_root)
    ),
    shapefm_uv_environment(root)
  )
  cat(paste(verification, collapse = "\n"), "\n")
  cat("Gate 00 ready:\n")
  cat("  salesforce-gift-eval: ", package_version, "\n", sep = "")
  cat("  source directory: ", source_root, "\n", sep = "")
  cat("  database: ", file.path(root, "data", "shapefm.duckdb"), "\n", sep = "")
  invisible(TRUE)
}

shapefm_prepare_gift_eval <- function() {
  root <- shapefm_project_root()
  uv <- file.path(root, ".tools", "uv", "uv")
  if (!file.exists(uv)) stop("Repository-local uv is missing. Follow docs/environment.md first.")
  submodule <- file.path(root, "external", "gift-eval")
  if (!file.exists(file.path(submodule, "pyproject.toml"))) {
    shapefm_system("git", c("submodule", "update", "--init", "--recursive", "external/gift-eval"))
  }
  env <- shapefm_uv_environment(root)
  shapefm_system(uv, c("sync", "--locked"), env)
  shapefm_system(
    uv,
    c("sync", "--project", "environments/gift-eval", "--locked"),
    env
  )
  source_root <- shapefm_source_root(FALSE)
  shapefm_system(
    uv,
    c(
      "run", "--locked", "python", "-m", "shapefm.acquire_gift_eval", "m4_daily",
      "--source-root", shQuote(source_root)
    ),
    env
  )
  shapefm_verify_gift_eval()
}
