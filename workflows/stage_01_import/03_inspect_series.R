# Inspect Stage 1 status and retrieve the first canonical M4 Daily series.

source("R/shapefm_import.R")
source("R/shapefm_database.R")

db <- shapefm_open()
on.exit(shapefm_close(db))

status <- shapefm_stage_status(db, stage = "import", dataset = "m4_daily")
series <- shapefm_get_series(db, dataset = "m4_daily", series_id = "0")

print(status)
print(series)
