source("R/shapefm_import.R")
source("R/shapefm_database.R")
source("R/shapefm_poc1.R")

plan <- shapefm_plan_poc1(scope = "smoke")
db <- shapefm_open()

forecast <- shapefm_get_forecast(db, plan, series_id = "0")
print(forecast)
print(shapefm_get_official_results(db, plan))
print(shapefm_experiment_status(db, plan))

shapefm_close(db)
