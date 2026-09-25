"""Regression test for R reads while one read-only DuckDB connection stays open."""

import subprocess
import unittest
from pathlib import Path


class RReadWorkflowTests(unittest.TestCase):
    def test_forecast_results_and_status_share_open_read_only_connection(self):
        root = Path(__file__).resolve().parents[2]
        script = r'''
source("R/shapefm_import.R")
source("R/shapefm_database.R")
source("R/shapefm_poc1.R")

plan <- shapefm_plan_poc1(scope = "smoke")
db <- shapefm_open()

# Any accidental CLI use after the connection opens makes the regression fail.
shapefm_poc1_cli <- function(...) stop("read operation invoked the Python CLI")

forecast <- shapefm_get_forecast(db, plan, series_id = "0")
results <- shapefm_get_official_results(db, plan)
status <- shapefm_experiment_status(db, plan)
printed_results <- paste(capture.output(print(results)), collapse = "\n")

# The Python status command is also a true read path and can share the lock.
root <- shapefm_project_root()
cli_status <- system2(
  file.path(root, ".tools", "uv", "uv"),
  args = c(
    "run", "--locked", "shapefm-poc1", "status",
    "--experiment-id", plan$experiment_id
  ),
  stdout = TRUE,
  stderr = TRUE,
  env = shapefm_uv_environment(root)
)

stopifnot(inherits(forecast, "shapefm_forecast"))
stopifnot(inherits(results, "shapefm_official_results"))
stopifnot(results$evaluation_count == 12L)
stopifnot(grepl("cleaning", printed_results, fixed = TRUE))
stopifnot(grepl("transformation", printed_results, fixed = TRUE))
stopifnot(grepl("candidate", printed_results, fixed = TRUE))
stopifnot(grepl("MASE", printed_results, fixed = TRUE))
stopifnot(grepl("sMAPE", printed_results, fixed = TRUE))
stopifnot(grepl("CRPS", printed_results, fixed = TRUE))
stopifnot(grepl("RMSE", printed_results, fixed = TRUE))
stopifnot(inherits(status, "shapefm_experiment_status"))
stopifnot(nrow(status$tasks) == 5L)
stopifnot(is.null(attr(cli_status, "status")))

shapefm_close(db)
cat("R read workflow completed without a lock conflict\n")
'''
        completed = subprocess.run(
            ["Rscript", "-e", script],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("without a lock conflict", completed.stdout)


if __name__ == "__main__":
    unittest.main()
