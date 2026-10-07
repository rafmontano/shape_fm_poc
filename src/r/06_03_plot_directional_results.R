#!/usr/bin/env Rscript
# ==============================================================================
# 06_03_plot_directional_results.R
# Purpose: Render selective IDs 058/062 figures from explicit stored result matrices.
# Inputs: JSON stdin, locked installed scmamp/jsonlite and a staging output directory.
# Outputs: Three PDF/PNG figure pairs and runtime JSON; no scientific evaluation.
# Run from: Internal Rscript --vanilla subprocess of the export action.
# ==============================================================================
# Bounded functional exception: stateless rendering uses native graphics objects.

# Code constants: stable historical presentation, not model selection or calculations.
labels <- c(directional_mantis_rf = "Mantis", chronos_2 = "Chronos-2",
            directional_dtw = "1-NN DTW", m4_smyl = "SMYL", m4_fforma = "FFORMA")
colours <- c(directional_mantis_rf = "#D7191C", chronos_2 = "#7B61FF",
             directional_dtw = "#2C7BB6", m4_smyl = "#333333", m4_fforma = "black")
line_types <- c(directional_mantis_rf = "solid", chronos_2 = "longdash",
                directional_dtw = "dashed", m4_smyl = "dotdash", m4_fforma = "dotted")

# Verify installed package versions/revision against the existing supplied lock.
verify_packages <- function(packages) {
  versions <- list()
  for (name in c("scmamp", "jsonlite")) {
    description <- utils::packageDescription(name)
    expected <- packages[[name]]
    if (is.null(expected$Version) || !identical(description$Version, expected$Version) ||
        (!is.null(expected$RemoteSha) && !identical(description$RemoteSha, expected$RemoteSha))) {
      stop(paste("locked package mismatch:", name), call. = FALSE)
    }
    versions[[name]] <- list(version = description$Version, revision = description$RemoteSha)
  }
  versions
}

# Decode a complete ordered matrix without coercing invalid JSON types to numbers.
decode_matrix <- function(input) {
  models <- unlist(input$models, use.names = FALSE)
  blocks <- unlist(input$blocks, use.names = FALSE)
  horizons <- unlist(input$horizons, use.names = FALSE)
  frequencies <- unlist(input$frequencies, use.names = FALSE)
  if (length(models) < 2L || anyDuplicated(models) || any(!models %in% names(labels)) ||
      !length(blocks) || anyDuplicated(blocks) || length(horizons) != length(blocks) ||
      length(frequencies) != length(blocks) || length(input$values) != length(blocks) ||
      !is.numeric(horizons) || any(!is.finite(horizons) | horizons < 1 | horizons != floor(horizons))) {
    stop("incomplete or invalid configured matrix", call. = FALSE)
  }
  rows <- lapply(input$values, function(row) {
    if (length(row) != length(models) || !all(vapply(row, function(x)
        is.numeric(x) && length(x) == 1L && is.finite(x) && x >= 0 && x <= 1, logical(1)))) {
      stop("matrix requires finite stored directional accuracies", call. = FALSE)
    }
    unlist(row, use.names = FALSE)
  })
  matrix <- do.call(rbind, rows)
  dimnames(matrix) <- list(blocks, unname(labels[models]))
  list(matrix = matrix, models = models, horizons = horizons, frequencies = frequencies)
}

# Draw explicit stored accuracies with fixed scientific scale and historical styles.
plot_horizons <- function(input) {
  frequencies <- unique(input$frequencies)
  graphics::par(mfrow = c(length(frequencies), 1), mar = c(7.5, 4.5, 3.2, 1))
  for (frequency in frequencies) {
    selected <- which(input$frequencies == frequency)
    h <- input$horizons[selected]
    graphics::matplot(h, input$matrix[selected, , drop = FALSE], type = "l",
      lty = unname(line_types[input$models]), col = unname(colours[input$models]), lwd = 2,
      ylim = c(0, 1), xlim = range(h), xaxt = "n", xlab = "Horizon",
      ylab = "Directional accuracy", main = paste(frequency, "- focused applicable models"))
    graphics::axis(1, at = h)
    graphics::legend(x = mean(range(h)), y = -.45, xjust = .5, yjust = .5, xpd = NA,
      legend = unname(labels[input$models]),
      col = unname(colours[input$models]), lty = unname(line_types[input$models]),
      lwd = 2, bty = "n", cex = .8, ncol = length(input$models))
  }
}

# Delegate CD mathematics and visual structure to the locked historical scmamp API.
plot_cd <- function(input, settings) {
  graphics::par(oma = c(2, 0, 0, 0), xpd = NA)
  scmamp::plotCD(results.matrix = input$matrix, alpha = settings$alpha,
                 reverse = settings$reverse, cex = settings$cex)
  graphics::mtext("Descriptive: dependent horizons; not independent-sample evidence", side = 1,
                  outer = TRUE, line = .5, cex = .6)
}

# Open an owned device before drawing and close that device even when drawing fails.
render_device <- function(path, format, draw, width, height, settings) {
  if (format == "pdf") {
    grDevices::pdf(path, width = width, height = height,
                   useDingbats = settings$useDingbats, timestamp = FALSE)
  } else {
    grDevices::png(path, width = width * 150, height = height * 150, res = 150)
  }
  device <- grDevices::dev.cur()
  on.exit(grDevices::dev.off(device), add = TRUE)
  draw()
}

# Render both formats; disabling PDF timestamps makes regeneration byte-reproducible.
render_pair <- function(output, name, draw, width, height, settings) {
  for (format in c("pdf", "png")) {
    render_device(file.path(output, paste0(name, ".", format)), format,
                  draw, width, height, settings)
  }
}

request <- jsonlite::fromJSON(paste(readLines(file("stdin"), warn = FALSE), collapse = "\n"),
                             simplifyVector = FALSE)
versions <- verify_packages(request$packages)
settings <- request$settings
if (!identical(settings$alpha, .05) || !identical(settings$reverse, TRUE) ||
    !identical(settings$cex, .75) || !identical(settings$useDingbats, FALSE)) {
  stop("historical CD settings required", call. = FALSE)
}
if (!dir.exists(request$output)) stop("explicit staging directory required", call. = FALSE)
horizon <- decode_matrix(request$horizon)
cd <- decode_matrix(request$cd)
render_pair(request$output, "figure_2_accuracy_by_horizon", function() plot_horizons(horizon),
            8, 4.8 * length(unique(horizon$frequencies)), settings)
for (name in c("figure_2_cd_daily", "figure_2_cd_daily_paper")) {
  render_pair(request$output, name, function() plot_cd(cd, settings), 8, 4.8, settings)
}
cat(jsonlite::toJSON(list(R = R.version.string, packages = versions, cd_settings = settings),
                    auto_unbox = TRUE, null = "null"), "\n")
