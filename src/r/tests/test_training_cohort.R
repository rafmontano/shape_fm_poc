# Process 03 cohort contract tests: fixed labels, cap and seed -> assertions
# on selected membership, quotas and RNG preservation; no database or outputs.
# Run: Rscript src/r/tests/test_training_cohort.R
source("src/r/03_02_select_training_cohort.R")

labels <- rep(c(-1L, 0L, 1L), c(11000L, 5000L, 2001L))
selected <- select_training_cohort(labels, 10000L, 1234L)
stopifnot(length(selected$selected_indices) == 10000L,
          !anyDuplicated(selected$selected_indices),
          identical(as.integer(table(labels[selected$selected_indices])), c(6111L, 2778L, 1111L)),
          identical(selected, select_training_cohort(labels, 10000L, 1234L)))
# Independent native-R reconstruction with externally calculated quotas.
set.seed(1234L)
expected <- c(sample(seq_len(11000L), 6111L),
              sample(11001L:16000L, 2778L), sample(16001L:18001L, 1111L))
expected <- sample(expected, length(expected))
stopifnot(identical(selected$selected_indices, expected))

set.seed(42L)
before <- .Random.seed
noop <- select_training_cohort(c(1L, -1L, 0L), 10000L, 1234L)
stopifnot(identical(noop$selected_indices, 1L:3L), identical(before, .Random.seed),
          identical(select_training_cohort(integer(0), 10000L, 1234L)$selected_indices, integer(0)))
# Rare classes force a negative remainder; only the majority quota may shrink.
rare <- c(rep(-1L, 100L), 0L, 1L)
stopifnot(identical(as.integer(table(rare[select_training_cohort(rare, 3L, 1234L)$selected_indices])), c(1L, 1L, 1L)))
cat("Training cohort R tests passed\n")
