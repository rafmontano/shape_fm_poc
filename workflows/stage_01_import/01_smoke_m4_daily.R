# Import the first ten M4 Daily series through the normal sequential path.
# This creates or migrates data/shapefm.duckdb automatically.

source("R/shapefm_import.R")
shapefm_import_m4_daily(max_series = 10L, workers = 1L)
