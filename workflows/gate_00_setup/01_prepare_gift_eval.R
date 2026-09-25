# Purpose: Reconstruct pinned GIFT-Eval code, software, and M4 Daily source data.
# Writes data: Yes, only generated environments, caches, and immutable source data.
# Rscript: Rscript workflows/gate_00_setup/01_prepare_gift_eval.R

source("R/shapefm_setup.R")
shapefm_prepare_gift_eval()
