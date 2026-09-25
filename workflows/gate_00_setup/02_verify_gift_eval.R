# Purpose: Verify and report the pinned GIFT-Eval code, package, and M4 Daily data.
# Writes data: No.
# Rscript: Rscript workflows/gate_00_setup/02_verify_gift_eval.R

source("R/shapefm_setup.R")
shapefm_verify_gift_eval()
