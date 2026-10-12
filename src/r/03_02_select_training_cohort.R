#!/usr/bin/env Rscript
# Purpose: Stateless historical first-horizon stratified training selection.
# Input/output: JSON labels, cap and seed on stdin; one-based indexes and R provenance.
# Source: rafmontano/m4_tsc_fmts_2026@839b5b6dc4334b5f088adce610f6d35935cbbca1,
# src/r/08_split_index.R cap_train_indices_stratified. Seed is caller-owned.
# Bounded OOP exception: a stateless scientific kernel needs no object lifecycle.
select_training_cohort <- function(labels, cap, seed) {
  stopifnot(length(cap) == 1L, cap >= 1L, cap == as.integer(cap),
            length(seed) == 1L, seed >= 0, seed <= .Machine$integer.max,
            all(!is.na(labels)), all(labels %in% c(-1L, 0L, 1L)))
  provenance <- list(r_version = as.character(getRversion()), rng_kind = RNGkind())
  if (length(labels) <= cap) {
    return(c(list(selected_indices = seq_along(labels)), provenance))
  }
  classes <- sort(unique(labels))
  counts <- vapply(classes, function(k) sum(labels == k), integer(1))
  if (cap < length(classes)) stop("cap must retain every class")
  quotas <- pmax(floor(counts / sum(counts) * cap), 1L)
  adjustment_order <- order(-counts, seq_along(classes))
  remainder <- cap - sum(quotas)
  while (remainder != 0L) {
    for (i in adjustment_order) {
      if (remainder > 0L) {
        quotas[i] <- quotas[i] + 1L
        remainder <- remainder - 1L
      } else if (quotas[i] > 1L) {
        quotas[i] <- quotas[i] - 1L
        remainder <- remainder + 1L
      }
      if (remainder == 0L) break
    }
  }
  set.seed(seed)
  chosen <- unlist(lapply(seq_along(classes), function(i) {
    local <- which(labels == classes[i])
    if (length(local) > quotas[i]) sample(local, quotas[i], replace = FALSE) else local
  }), use.names = FALSE)
  c(list(selected_indices = sample(chosen, length(chosen), replace = FALSE)), provenance)
}

if (sys.nframe() == 0L) {
  payload <- jsonlite::fromJSON(file("stdin"))
  result <- select_training_cohort(payload$labels, payload$cap, payload$seed)
  cat(jsonlite::toJSON(result, auto_unbox = TRUE))
}
