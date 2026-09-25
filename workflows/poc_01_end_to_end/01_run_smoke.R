source("R/shapefm_import.R")
source("R/shapefm_database.R")
source("R/shapefm_poc1.R")

plan <- shapefm_plan_poc1(scope = "smoke")
print(plan)
shapefm_run_poc1(plan, profile = "sequential_safe")

db <- shapefm_open()
print(shapefm_experiment_status(db, plan))
shapefm_close(db)
