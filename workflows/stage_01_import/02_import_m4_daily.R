# Import all 4,227 M4 Daily series. Run only after inspecting the smoke import.
# Increase workers for local parallel computation; one coordinator still writes.

source("R/shapefm_import.R")
shapefm_import_m4_daily(workers = 1L)
