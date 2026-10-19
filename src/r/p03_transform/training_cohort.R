#!/usr/bin/env Rscript
# Process 03: first-horizon labels, DTW-only cap and seed -> selected one-based
# indexes and R RNG provenance (JSON stdin/stdout). No persistence or forecasting.
# Quotas start at max(floor(class_count / total * cap), 1); adjust the remainder
# in descending class-count order, sample within classes, then shuffle membership.
# Uncapped inputs retain their order without consuming RNG; docs/directional-models.md.
# Seed is caller-owned.
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
